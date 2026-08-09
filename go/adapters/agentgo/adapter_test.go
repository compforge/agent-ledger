package agentgoadapter

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentgo"
)

func TestNativeMessagesRestoreIntoAgentGo(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	adapter := newTestAdapter(t, ctx, store)
	if err := adapter.commitMessage(agentgo.UserMsg("hello")); err != nil {
		t.Fatalf("commit message: %v", err)
	}

	agent := agentgo.NewAgent()
	if err := adapter.Restore(ctx, agent); err != nil {
		t.Fatalf("restore: %v", err)
	}
	messages := agent.Messages()
	if len(messages) != 1 || messages[0].TextContent() != "hello" {
		t.Fatalf("restored messages = %#v", messages)
	}
}

func TestWrappedModelFailsClosedWhenPrewriteFails(t *testing.T) {
	ctx := context.Background()
	base := agentledger.NewMemoryEventStore()
	store := failingStore{EventStore: base, eventType: "model.requested"}
	adapter := newTestAdapter(t, ctx, store)
	inner := &countingModel{}
	_, err := adapter.WrapModel(inner).Generate(ctx, []agentgo.Message{agentgo.UserMsg("hello")}, nil)
	if err == nil {
		t.Fatal("generate succeeded despite prewrite failure")
	}
	if inner.called {
		t.Fatal("inner model was called before durable prewrite")
	}
}

func TestToolMiddlewareWritesBeforeExecute(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	adapter := newTestAdapter(t, ctx, store)
	executed := false
	call := agentgo.ToolCall{ID: "tool-1", Name: "read", Args: json.RawMessage(`{"path":"README.md"}`)}
	_, err := adapter.ToolMiddleware()(ctx, call, func(context.Context, json.RawMessage) (json.RawMessage, error) {
		executed = true
		return json.RawMessage(`{"ok":true}`), nil
	})
	if err != nil {
		t.Fatalf("execute middleware: %v", err)
	}
	if !executed {
		t.Fatal("tool was not executed")
	}

	var types []string
	for event, loadErr := range store.Load(ctx, agentledger.EventStream{SessionID: "session", StreamID: "run"}, -1) {
		if loadErr != nil {
			t.Fatalf("load events: %v", loadErr)
		}
		types = append(types, event.EventType)
	}
	if len(types) != 2 || types[0] != "tool.requested" || types[1] != "tool.completed" {
		t.Fatalf("event types = %v", types)
	}
}

func TestWrappedModelRecordsPhysicalAttempt(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	adapter := newTestAdapter(t, ctx, store)
	model := adapter.WrapModel(fakeModel{})
	if _, err := model.Generate(ctx, []agentgo.Message{agentgo.UserMsg("hello")}, nil); err != nil {
		t.Fatalf("generate: %v", err)
	}

	var types []string
	for event, loadErr := range store.Load(ctx, agentledger.EventStream{SessionID: "session", StreamID: "run"}, -1) {
		if loadErr != nil {
			t.Fatalf("load events: %v", loadErr)
		}
		types = append(types, event.EventType)
	}
	if len(types) != 2 || types[0] != "model.requested" || types[1] != "model.completed" {
		t.Fatalf("event types = %v", types)
	}
}

func TestWrappedModelEarlyCloseHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := agentledger.NewMemoryEventStore()
	adapter := newTestAdapter(t, ctx, store)
	stream, err := adapter.WrapModel(nonTerminalStreamModel{}).GenerateStream(ctx, nil, nil)
	if err != nil {
		t.Fatalf("generate stream: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for len(stream) < 16 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(stream) != 16 {
		t.Fatalf("buffered events = %d, want 16", len(stream))
	}
	cancel()

	var events []agentgo.StreamEvent
	closeDeadline := time.After(time.Second)
collect:
	for {
		select {
		case event, ok := <-stream:
			if !ok {
				break collect
			}
			events = append(events, event)
		case <-closeDeadline:
			t.Fatal("wrapped stream did not close after cancellation")
		}
	}
	if len(events) != 16 {
		t.Fatalf("forwarded events = %d, want 16 without a post-cancel terminal event", len(events))
	}
}

func newTestAdapter(t *testing.T, ctx context.Context, store agentledger.EventStore) *Adapter {
	t.Helper()
	adapter, err := New(ctx, Config{
		Store: store, SessionID: "session", RunID: "run", NativeSessionID: "native",
		Actor:            agentledger.Actor{Type: "agent", ID: "agentgo", Framework: "agentgo"},
		OperationTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	return adapter
}

type fakeModel struct{}

func (fakeModel) Generate(context.Context, []agentgo.Message, []agentgo.ToolSpec, ...agentgo.CallOption) (*agentgo.LLMResponse, error) {
	return &agentgo.LLMResponse{Message: agentgo.Message{Role: agentgo.RoleAssistant, Content: []agentgo.ContentBlock{agentgo.TextBlock("done")}, Timestamp: time.Now()}}, nil
}

func (fakeModel) GenerateStream(context.Context, []agentgo.Message, []agentgo.ToolSpec, ...agentgo.CallOption) (<-chan agentgo.StreamEvent, error) {
	stream := make(chan agentgo.StreamEvent)
	close(stream)
	return stream, nil
}

func (fakeModel) SupportsTools() bool { return true }

type countingModel struct{ called bool }

func (m *countingModel) Generate(context.Context, []agentgo.Message, []agentgo.ToolSpec, ...agentgo.CallOption) (*agentgo.LLMResponse, error) {
	m.called = true
	return &agentgo.LLMResponse{}, nil
}

func (*countingModel) GenerateStream(context.Context, []agentgo.Message, []agentgo.ToolSpec, ...agentgo.CallOption) (<-chan agentgo.StreamEvent, error) {
	return nil, errors.New("not implemented")
}

func (*countingModel) SupportsTools() bool { return true }

type nonTerminalStreamModel struct{}

func (nonTerminalStreamModel) Generate(context.Context, []agentgo.Message, []agentgo.ToolSpec, ...agentgo.CallOption) (*agentgo.LLMResponse, error) {
	return nil, errors.New("not implemented")
}

func (nonTerminalStreamModel) GenerateStream(context.Context, []agentgo.Message, []agentgo.ToolSpec, ...agentgo.CallOption) (<-chan agentgo.StreamEvent, error) {
	stream := make(chan agentgo.StreamEvent, 16)
	for range 16 {
		stream <- agentgo.StreamEvent{}
	}
	close(stream)
	return stream, nil
}

func (nonTerminalStreamModel) SupportsTools() bool { return true }

type failingStore struct {
	agentledger.EventStore
	eventType string
}

func (s failingStore) Append(ctx context.Context, stream agentledger.EventStream, expectedVersion int64, appendID string, events ...agentledger.ProposedEvent) (agentledger.CommitReceipt, error) {
	for _, event := range events {
		if event.EventType == s.eventType {
			return agentledger.CommitReceipt{}, errors.New("prewrite failed")
		}
	}
	return s.EventStore.Append(ctx, stream, expectedVersion, appendID, events...)
}
