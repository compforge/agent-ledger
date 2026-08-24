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
	if len(view.Turns) != 1 || len(view.Actions) != 1 || view.Actions[0].Type != "tool_call" || view.Actions[0].Key != call.ID || len(view.Attempts) != 1 {
		t.Fatalf("session view = %#v", view)
	}
	if view.Actions[0].Effect != agentledger.UnknownEffect() {
		t.Fatalf("tool effect = %#v", view.Actions[0].Effect)
	}
	for _, event := range view.Events {
		switch event.EventType {
		case agentledger.EventTypeAttemptRequested:
			if _, ok := event.Payload["input"]; !ok || event.Payload["tool_name"] != call.Name {
				t.Fatalf("tool request payload = %#v", event.Payload)
			}
		case agentledger.EventTypeAttemptCompleted:
			if _, ok := event.Payload["output"]; !ok {
				t.Fatalf("tool result payload = %#v", event.Payload)
			}
		}
	}
	assertAttemptLifecycle(t, view.Events)
}

func TestToolMiddlewareRetriesOnlyWhenPolicyApprovesFixedEffect(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	readEffect := agentledger.Effect{Kind: agentledger.EffectKindRead, Idempotency: agentledger.IdempotencyNotApplicable}
	adapter, err := New(ctx, Config{
		Store: store, SessionID: "session", RunID: "run", NativeSessionID: "native",
		Actor: agentledger.NewActor("agent", "agentgo"), OperationTimeout: time.Second,
		ToolSemantics: func(agentgo.ToolCall) ToolSemantics {
			return ToolSemantics{Effect: readEffect}
		},
		CanRetryTool: func(action agentledger.Action, attempt agentledger.Attempt, call agentgo.ToolCall) ToolRetryDecision {
			return ToolRetryDecision{
				Approved:           action.Effect == readEffect && attempt.AttemptNo == 1 && call.ID == "tool-1",
				RecoveryDecisionID: "confirmation-1",
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	call := agentgo.ToolCall{ID: "tool-1", Name: "read", Args: json.RawMessage(`{"path":"README.md"}`)}
	turnID, err := adapter.turnID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.beforeToolCall(ctx, turnID, call, readEffect, map[string]any{
		"tool_name": call.Name, "input": call.Args,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.ToolMiddleware()(ctx, call, func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	}); err != nil {
		t.Fatal(err)
	}
	view, err := store.LoadSession(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Actions) != 1 || len(view.Attempts) != 2 || view.Attempts[1].AttemptNo != 2 {
		t.Fatalf("actions=%#v attempts=%#v", view.Actions, view.Attempts)
	}
	if view.Actions[0].Effect != readEffect {
		t.Fatalf("effect = %#v, want %#v", view.Actions[0].Effect, readEffect)
	}
	var oldResolved, retryRequested bool
	for _, event := range view.Events {
		switch {
		case event.EventType == agentledger.EventTypeAttemptOutcomeUnknown && event.SubjectID == view.Attempts[0].ID:
			oldResolved = event.Payload["superseded_by_attempt_id"] == view.Attempts[1].ID
		case event.EventType == agentledger.EventTypeAttemptRequested && event.SubjectID == view.Attempts[1].ID:
			retryRequested = event.Payload["recovery_decision_id"] == "confirmation-1"
		}
	}
	if !oldResolved || !retryRequested {
		t.Fatalf("old_resolved=%t retry_requested=%t events=%#v", oldResolved, retryRequested, view.Events)
	}
}

func TestToolMiddlewareDoesNotRetryUnresolvedActionByDefault(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	adapter := newTestAdapter(t, ctx, store)
	call := agentgo.ToolCall{ID: "tool-1", Name: "write", Args: json.RawMessage(`{}`)}
	turnID, err := adapter.turnID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.beforeToolCall(ctx, turnID, call, agentledger.UnknownEffect(), map[string]any{
		"tool_name": call.Name, "input": call.Args,
	}); err != nil {
		t.Fatal(err)
	}
	called := false
	_, err = adapter.ToolMiddleware()(ctx, call, func(context.Context, json.RawMessage) (json.RawMessage, error) {
		called = true
		return nil, nil
	})
	if err == nil || called {
		t.Fatalf("err=%v called=%t", err, called)
	}
}

func TestToolMiddlewareRejectsKeyedEffectWithoutEffectiveKey(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	adapter, err := New(ctx, Config{
		Store: store, SessionID: "session", RunID: "run", NativeSessionID: "native",
		Actor: agentledger.NewActor("agent", "agentgo"), OperationTimeout: time.Second,
		ToolSemantics: func(agentgo.ToolCall) ToolSemantics {
			return ToolSemantics{Effect: agentledger.Effect{
				Kind: agentledger.EffectKindWrite, Idempotency: agentledger.IdempotencyKeyed,
			}}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	called := false
	_, err = adapter.ToolMiddleware()(ctx,
		agentgo.ToolCall{ID: "tool-1", Name: "write", Args: json.RawMessage(`{}`)},
		func(context.Context, json.RawMessage) (json.RawMessage, error) {
			called = true
			return nil, nil
		},
	)
	if err == nil || called {
		t.Fatalf("err=%v called=%t", err, called)
	}
}

func TestToolMiddlewareRejectsChangedIdempotencyKeyOnRetry(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	key := "key-1"
	effect := agentledger.Effect{
		Kind: agentledger.EffectKindWrite, Idempotency: agentledger.IdempotencyKeyed,
	}
	adapter, err := New(ctx, Config{
		Store: store, SessionID: "session", RunID: "run", NativeSessionID: "native",
		Actor: agentledger.NewActor("agent", "agentgo"), OperationTimeout: time.Second,
		ToolSemantics: func(agentgo.ToolCall) ToolSemantics {
			return ToolSemantics{Effect: effect, IdempotencyKey: key}
		},
		CanRetryTool: func(agentledger.Action, agentledger.Attempt, agentgo.ToolCall) ToolRetryDecision {
			return ToolRetryDecision{Approved: true}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	call := agentgo.ToolCall{ID: "tool-1", Name: "write", Args: json.RawMessage(`{}`)}
	turnID, err := adapter.turnID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.beforeToolCall(ctx, turnID, call, effect, map[string]any{
		"tool_name": call.Name, "input": call.Args, "idempotency_key": key,
	}); err != nil {
		t.Fatal(err)
	}
	key = "key-2"
	called := false
	_, err = adapter.ToolMiddleware()(ctx, call, func(context.Context, json.RawMessage) (json.RawMessage, error) {
		called = true
		return nil, nil
	})
	if err == nil || called {
		t.Fatalf("err=%v called=%t", err, called)
	}
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
	wantEffect := agentledger.Effect{Kind: agentledger.EffectKindNone, Idempotency: agentledger.IdempotencyNotApplicable}
	if view.Actions[0].Effect != wantEffect {
		t.Fatalf("model effect = %#v, want %#v", view.Actions[0].Effect, wantEffect)
	}
	for _, event := range view.Events {
		switch event.EventType {
		case agentledger.EventTypeAttemptRequested:
			model, _ := event.Payload["model"].(map[string]any)
			if model["id"] != "test-model" || model["provider"] != "test-provider" {
				t.Fatalf("model request payload = %#v", event.Payload)
			}
		case agentledger.EventTypeAttemptCompleted:
			usage, _ := event.Payload["usage"].(map[string]any)
			if event.Payload["finish_reason"] != "stop" || usage["input_tokens"] != float64(10) {
				t.Fatalf("model result payload = %#v", event.Payload)
			}
		}
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
		Role: agentgo.RoleAssistant, Content: []agentgo.ContentBlock{agentgo.TextBlock("done")},
		StopReason: agentgo.StopReasonStop, Usage: &agentgo.Usage{
			Provider: "actual-provider", Model: "actual-model", Input: 10, Output: 4, TotalTokens: 14,
		}, Timestamp: time.Now(),
	}}, nil
}
func (fakeModel) GenerateStream(context.Context, []agentgo.Message, []agentgo.ToolSpec, ...agentgo.CallOption) (<-chan agentgo.StreamEvent, error) {
	stream := make(chan agentgo.StreamEvent)
	close(stream)
	return stream, nil
}
func (fakeModel) SupportsTools() bool  { return true }
func (fakeModel) ProviderName() string { return "test-provider" }
func (fakeModel) ModelName() string    { return "test-model" }

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
