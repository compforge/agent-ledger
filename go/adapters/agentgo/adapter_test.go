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
	store := failingStore{EventStore: base, eventType: "attempt.requested"}
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

func TestToolMiddlewareRecordsActionAndAttempt(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	adapter := newTestAdapter(t, ctx, store)
	call := agentgo.ToolCall{ID: "tool-1", Name: "read", Args: json.RawMessage(`{"path":"README.md"}`)}
	_, err := adapter.ToolMiddleware()(ctx, call, func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	})
	if err != nil {
		t.Fatalf("execute middleware: %v", err)
	}
	view, err := store.LoadSession(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Turns) != 1 || len(view.Actions) != 1 || view.Actions[0].Type != "tool_call" || len(view.Attempts) != 1 {
		t.Fatalf("session view = %#v", view)
	}
	assertAttemptLifecycle(t, view.Events)
}

func TestWrappedModelRecordsPhysicalAttempt(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	adapter := newTestAdapter(t, ctx, store)
	if _, err := adapter.WrapModel(fakeModel{}).Generate(ctx, []agentgo.Message{agentgo.UserMsg("hello")}, nil); err != nil {
		t.Fatalf("generate: %v", err)
	}
	view, err := store.LoadSession(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Actions) != 1 || view.Actions[0].Type != "model_call" || len(view.Attempts) != 1 {
		t.Fatalf("session view = %#v", view)
	}
	assertAttemptLifecycle(t, view.Events)
}

func assertAttemptLifecycle(t *testing.T, events []agentledger.StoredEvent) {
	t.Helper()
	var types []string
	for _, event := range events {
		if event.EventType == "attempt.requested" || event.EventType == "attempt.completed" {
			types = append(types, event.EventType)
		}
	}
	if len(types) != 2 || types[0] != "attempt.requested" || types[1] != "attempt.completed" {
		t.Fatalf("attempt event types = %v", types)
	}
}

func newTestAdapter(t *testing.T, ctx context.Context, store agentledger.EventStore) *Adapter {
	t.Helper()
	adapter, err := New(ctx, Config{
		Store: store, SessionID: "session", RunID: "run", NativeSessionID: "native",
		Actor: agentledger.NewActor("agent", "agentgo"), OperationTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	return adapter
}

type fakeModel struct{}

func (fakeModel) Generate(context.Context, []agentgo.Message, []agentgo.ToolSpec, ...agentgo.CallOption) (*agentgo.LLMResponse, error) {
	return &agentgo.LLMResponse{Message: agentgo.Message{
		Role: agentgo.RoleAssistant, Content: []agentgo.ContentBlock{agentgo.TextBlock("done")}, Timestamp: time.Now(),
	}}, nil
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

type failingStore struct {
	agentledger.EventStore
	eventType string
}

func (s failingStore) Append(ctx context.Context, laneID string, expectedLastSeq int64, appendID string, events ...agentledger.ProposedEvent) (agentledger.AppendReceipt, error) {
	for _, event := range events {
		if event.EventType == s.eventType {
			return agentledger.AppendReceipt{}, errors.New("prewrite failed")
		}
	}
	return s.EventStore.Append(ctx, laneID, expectedLastSeq, appendID, events...)
}
