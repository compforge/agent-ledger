package agentgoadapter

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentgo"
)

func TestCheckpointFailureStopsBeforeModel(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	checkpoints := &failingCheckpointStore{CheckpointStore: store, failAt: 1}
	adapter := newTestAdapter(t, ctx, store, checkpoints, Config{})
	model := &scriptedModel{responses: []agentgo.Message{assistantText("should not run")}}
	agent := newTestAgent(model, nil, adapter)

	if err := agent.Prompt(ctx, "hello"); err == nil {
		t.Fatal("prompt succeeded despite baseline checkpoint failure")
	}
	if model.callCount() != 0 {
		t.Fatalf("model calls = %d, want 0", model.callCount())
	}
}

func TestRecoveryReplaysCompletedModelThroughNativeLoop(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	checkpoints := &failingCheckpointStore{CheckpointStore: store, failAt: 2}
	firstAdapter := newTestAdapter(t, ctx, store, checkpoints, Config{})
	firstModel := &scriptedModel{responses: []agentgo.Message{assistantText("done")}}
	firstAgent := newTestAgent(firstModel, nil, firstAdapter)

	if err := firstAgent.Prompt(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	firstAgent.WaitForIdle()
	if firstModel.callCount() != 1 {
		t.Fatalf("first model calls = %d, want 1", firstModel.callCount())
	}

	secondAdapter := newTestAdapter(t, ctx, store, nil, Config{})
	secondModel := &scriptedModel{}
	secondAgent := newTestAgent(secondModel, nil, secondAdapter)
	if err := secondAgent.Prompt(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	secondAgent.WaitForIdle()

	if secondModel.callCount() != 0 {
		t.Fatalf("recovery model calls = %d, want 0", secondModel.callCount())
	}
	messages := secondAgent.Messages()
	if len(messages) != 2 || messages[0].TextContent() != "hello" || messages[1].TextContent() != "done" {
		t.Fatalf("recovered messages = %#v", messages)
	}
}

func TestRecoveryRejectsDivergedModelInput(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	checkpoints := &failingCheckpointStore{CheckpointStore: store, failAt: 2}
	firstAdapter := newTestAdapter(t, ctx, store, checkpoints, Config{})
	firstAgent := newTestAgent(
		&scriptedModel{responses: []agentgo.Message{assistantText("done")}}, nil, firstAdapter,
	)
	if err := firstAgent.Prompt(ctx, "original"); err != nil {
		t.Fatal(err)
	}
	firstAgent.WaitForIdle()

	secondAdapter := newTestAdapter(t, ctx, store, nil, Config{})
	secondModel := &scriptedModel{responses: []agentgo.Message{assistantText("must not run")}}
	secondAgent := newTestAgent(secondModel, nil, secondAdapter)
	if err := secondAgent.Prompt(ctx, "different"); err != nil {
		t.Fatal(err)
	}
	secondAgent.WaitForIdle()
	if secondModel.callCount() != 0 {
		t.Fatalf("model calls = %d, want 0", secondModel.callCount())
	}
	if got := secondAgent.State().Error; !strings.Contains(got, "input diverged") {
		t.Fatalf("agent error = %q", got)
	}
}

func TestRecoveryReplaysCompletedToolWithoutSideEffect(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	checkpoints := &failingCheckpointStore{CheckpointStore: store, failAt: 2}
	firstAdapter := newTestAdapter(t, ctx, store, checkpoints, Config{})
	call := agentgo.ToolCall{ID: "tool-1", Name: "read", Args: json.RawMessage(`{"path":"README.md"}`)}
	firstModel := &scriptedModel{responses: []agentgo.Message{assistantToolCall(call), assistantText("done")}}
	firstTool := &countingTool{name: "read", result: json.RawMessage(`{"ok":true}`)}
	firstAgent := newTestAgent(firstModel, []agentgo.Tool{firstTool}, firstAdapter)

	if err := firstAgent.Prompt(ctx, "inspect"); err != nil {
		t.Fatal(err)
	}
	firstAgent.WaitForIdle()
	if firstModel.callCount() != 2 || firstTool.callCount() != 1 {
		t.Fatalf("first model calls=%d tool calls=%d", firstModel.callCount(), firstTool.callCount())
	}

	secondAdapter := newTestAdapter(t, ctx, store, nil, Config{})
	secondModel := &scriptedModel{}
	secondTool := &countingTool{name: "read", result: json.RawMessage(`{"unexpected":true}`)}
	secondAgent := newTestAgent(secondModel, []agentgo.Tool{secondTool}, secondAdapter)
	if err := secondAgent.Prompt(ctx, "inspect"); err != nil {
		t.Fatal(err)
	}
	secondAgent.WaitForIdle()

	if secondModel.callCount() != 0 || secondTool.callCount() != 0 {
		t.Fatalf("recovery model calls=%d tool calls=%d, want both 0", secondModel.callCount(), secondTool.callCount())
	}
	if got := secondAgent.Messages()[len(secondAgent.Messages())-1].TextContent(); got != "done" {
		t.Fatalf("final message = %q", got)
	}
}

func TestRecoveryPreservesRichToolResult(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	first := newTestAdapter(t, ctx, store, nil, Config{})
	if _, err := first.handleBeforeRun(ctx, agentgo.BeforeRunContext{Kind: agentgo.RunKindPrompt}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.handleBeforeTurn(ctx, agentgo.BeforeTurnContext{TurnIndex: 1}); err != nil {
		t.Fatal(err)
	}
	call := agentgo.ToolCall{ID: "tool-1", Name: "inspect", Args: json.RawMessage(`{}`)}
	execution := agentgo.ToolExecution{
		Execution: agentgo.Execution{ID: call.ID, Kind: agentgo.ExecutionKindTool, TurnIndex: 1, Attempt: 1},
		Call:      call,
	}
	want := agentgo.ToolResult{
		ToolCallID: call.ID,
		Content:    json.RawMessage(`"screenshot"`),
		ContentBlocks: []agentgo.ContentBlock{
			agentgo.TextBlock("screenshot"), agentgo.ImageBlock("aW1hZ2U=", "image/png"),
		},
	}
	if _, err := first.ToolMiddleware()(ctx, execution, func(context.Context, agentgo.ToolExecution) (agentgo.ToolResult, error) {
		return want, nil
	}); err != nil {
		t.Fatal(err)
	}

	second := newTestAdapter(t, ctx, store, nil, Config{})
	if _, err := second.handleBeforeRun(ctx, agentgo.BeforeRunContext{Kind: agentgo.RunKindPrompt}); err != nil {
		t.Fatal(err)
	}
	if _, err := second.handleBeforeTurn(ctx, agentgo.BeforeTurnContext{TurnIndex: 1}); err != nil {
		t.Fatal(err)
	}
	called := false
	got, err := second.ToolMiddleware()(ctx, execution, func(context.Context, agentgo.ToolExecution) (agentgo.ToolResult, error) {
		called = true
		return agentgo.ToolResult{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if called || len(got.ContentBlocks) != 2 || got.ContentBlocks[1].Image == nil || got.ContentBlocks[1].Image.MimeType != "image/png" {
		t.Fatalf("called=%t replayed result=%#v", called, got)
	}
}

func TestRecoveryBlocksUnresolvedToolUntilExplicitDecision(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	call := agentgo.ToolCall{ID: "tool-1", Name: "write", Args: json.RawMessage(`{"value":1}`)}
	seed := newTestAdapter(t, ctx, store, nil, Config{})
	seedUnresolvedTool(t, ctx, seed, call)

	blockedAdapter := newTestAdapter(t, ctx, store, nil, Config{})
	blockedAgent := newTestAgent(&scriptedModel{}, nil, blockedAdapter)
	err := blockedAgent.Prompt(ctx, "change it")
	var blocked *RecoveryBlockedError
	if !errors.As(err, &blocked) || len(blocked.Tools) != 1 {
		t.Fatalf("prompt error = %v, want one RecoveryBlockedError tool", err)
	}

	approvedAdapter := newTestAdapter(t, ctx, store, nil, Config{
		CanRetryTool: func(agentledger.Action, agentledger.Attempt, agentgo.ToolCall) ToolRetryDecision {
			return ToolRetryDecision{Approved: true, RecoveryDecisionID: "operator-approval-1"}
		},
	})
	model := &scriptedModel{responses: []agentgo.Message{assistantText("recovered")}}
	tool := &countingTool{name: "write", result: json.RawMessage(`{"ok":true}`)}
	agent := newTestAgent(model, []agentgo.Tool{tool}, approvedAdapter)
	if err := agent.Prompt(ctx, "change it"); err != nil {
		t.Fatal(err)
	}
	agent.WaitForIdle()
	if model.callCount() != 1 || tool.callCount() != 1 {
		t.Fatalf("model calls=%d tool calls=%d state=%#v, want one new call each", model.callCount(), tool.callCount(), agent.State())
	}

	view, err := store.LoadSession(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	var toolAttempts []agentledger.Attempt
	for _, action := range view.Actions {
		if action.Type != agentledger.ActionTypeToolCall {
			continue
		}
		for _, attempt := range view.Attempts {
			if attempt.ActionID == action.ID {
				toolAttempts = append(toolAttempts, attempt)
			}
		}
	}
	hasSecondAttempt := false
	for _, attempt := range toolAttempts {
		hasSecondAttempt = hasSecondAttempt || attempt.AttemptNo == 2
	}
	if len(toolAttempts) != 2 || !hasSecondAttempt {
		t.Fatalf("tool attempts = %#v", toolAttempts)
	}
	var unknown, approved bool
	for _, event := range view.Events {
		switch event.EventType {
		case agentledger.EventTypeAttemptOutcomeUnknown:
			unknown = true
		case agentledger.EventTypeAttemptRequested:
			if event.Payload["recovery_decision_id"] == "operator-approval-1" {
				approved = true
			}
		}
	}
	if !unknown || !approved {
		t.Fatalf("unknown=%t approved=%t", unknown, approved)
	}
}

func TestModelFailureRetriesSameLogicalAction(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	adapter := newTestAdapter(t, ctx, store, nil, Config{})
	if _, err := adapter.handleBeforeRun(ctx, agentgo.BeforeRunContext{Kind: agentgo.RunKindPrompt}); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.handleBeforeTurn(ctx, agentgo.BeforeTurnContext{TurnIndex: 1}); err != nil {
		t.Fatal(err)
	}
	middleware := adapter.ModelMiddleware()
	execution := agentgo.ModelExecution{Execution: agentgo.Execution{
		ID: "model-1", Kind: agentgo.ExecutionKindModel, TurnIndex: 1, Attempt: 1,
	}}
	if _, err := middleware(ctx, execution, func(context.Context, agentgo.ModelExecution) (agentgo.ModelResult, error) {
		return agentgo.ModelResult{}, errors.New("provider unavailable")
	}); err == nil {
		t.Fatal("first model call succeeded")
	}
	execution.Attempt = 2
	if _, err := middleware(ctx, execution, func(context.Context, agentgo.ModelExecution) (agentgo.ModelResult, error) {
		return agentgo.ModelResult{Message: assistantText("done")}, nil
	}); err != nil {
		t.Fatal(err)
	}

	view, err := store.LoadSession(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	hasSecondAttempt := false
	for _, attempt := range view.Attempts {
		hasSecondAttempt = hasSecondAttempt || attempt.AttemptNo == 2
	}
	if len(view.Actions) != 1 || len(view.Attempts) != 2 || !hasSecondAttempt {
		t.Fatalf("actions=%#v attempts=%#v", view.Actions, view.Attempts)
	}
}

func TestModelPrewriteFailurePreventsProviderCall(t *testing.T) {
	ctx := context.Background()
	base := agentledger.NewMemoryEventStore()
	store := failingStore{EventStore: base, eventType: agentledger.EventTypeAttemptRequested}
	adapter := newTestAdapter(t, ctx, store, base, Config{})
	model := &scriptedModel{responses: []agentgo.Message{assistantText("should not run")}}
	agent := newTestAgent(model, nil, adapter)
	if err := agent.Prompt(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	agent.WaitForIdle()
	if model.callCount() != 0 {
		t.Fatalf("model calls = %d, want 0", model.callCount())
	}

	recoveryAdapter := newTestAdapter(t, ctx, base, nil, Config{})
	recoveryModel := &scriptedModel{responses: []agentgo.Message{assistantText("recovered")}}
	recoveryAgent := newTestAgent(recoveryModel, nil, recoveryAdapter)
	if err := recoveryAgent.Prompt(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	recoveryAgent.WaitForIdle()
	if recoveryModel.callCount() != 1 {
		t.Fatalf("recovery model calls = %d, want 1", recoveryModel.callCount())
	}
}

func TestToolOutcomeWriteFailureStopsBeforeNextModel(t *testing.T) {
	ctx := context.Background()
	base := agentledger.NewMemoryEventStore()
	store := &failingNthEventStore{
		EventStore: base, eventType: agentledger.EventTypeAttemptCompleted, failAt: 2,
	}
	adapter := newTestAdapter(t, ctx, store, base, Config{})
	call := agentgo.ToolCall{ID: "tool-1", Name: "write", Args: json.RawMessage(`{"value":1}`)}
	model := &scriptedModel{responses: []agentgo.Message{
		assistantToolCall(call), assistantText("must not run"),
	}}
	tool := &countingTool{name: "write", result: json.RawMessage(`{"ok":true}`)}
	agent := newTestAgent(model, []agentgo.Tool{tool}, adapter)
	if err := agent.Prompt(ctx, "change it"); err != nil {
		t.Fatal(err)
	}
	agent.WaitForIdle()
	if model.callCount() != 1 || tool.callCount() != 1 {
		t.Fatalf("model calls=%d tool calls=%d, want 1 each", model.callCount(), tool.callCount())
	}

	view, err := base.LoadRun(ctx, "session", "run")
	if err != nil {
		t.Fatal(err)
	}
	inspection := agentledger.InspectRun(view)
	if len(inspection.UnresolvedAttempts) != 1 || inspection.UnresolvedAttempts[0].ActionType != agentledger.ActionTypeToolCall {
		t.Fatalf("unresolved attempts = %#v", inspection.UnresolvedAttempts)
	}

	recoveryAdapter := newTestAdapter(t, ctx, base, nil, Config{})
	recoveryAgent := newTestAgent(&scriptedModel{}, []agentgo.Tool{tool}, recoveryAdapter)
	err = recoveryAgent.Prompt(ctx, "change it")
	var blocked *RecoveryBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("recovery error = %v, want RecoveryBlockedError", err)
	}
}

func seedUnresolvedTool(t *testing.T, ctx context.Context, adapter *Adapter, call agentgo.ToolCall) {
	t.Helper()
	if _, err := adapter.handleBeforeRun(ctx, agentgo.BeforeRunContext{Kind: agentgo.RunKindPrompt}); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.handleBeforeTurn(ctx, agentgo.BeforeTurnContext{TurnIndex: 1}); err != nil {
		t.Fatal(err)
	}
	modelExecution := agentgo.ModelExecution{Execution: agentgo.Execution{
		ID: "model-1", Kind: agentgo.ExecutionKindModel, TurnIndex: 1, Attempt: 1,
	}, Request: agentgo.LLMRequest{
		Messages: []agentgo.Message{agentgo.UserMsg("change it")},
		Tools: []agentgo.ToolSpec{{
			Name: call.Name, Description: "test tool", Parameters: map[string]any{"type": "object"},
		}},
	}}
	if _, err := adapter.ModelMiddleware()(ctx, modelExecution, func(context.Context, agentgo.ModelExecution) (agentgo.ModelResult, error) {
		return agentgo.ModelResult{Message: assistantToolCall(call), HasCompletedToolCalls: true}, nil
	}); err != nil {
		t.Fatal(err)
	}
	toolExecution := agentgo.ToolExecution{
		Execution: agentgo.Execution{ID: call.ID, Kind: agentgo.ExecutionKindTool, TurnIndex: 1, Attempt: 1},
		Call:      call,
	}
	semantics, err := validateToolSemantics(adapter.toolSemantics(call))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := toolRequestPayload(toolExecution, semantics)
	if err != nil {
		t.Fatal(err)
	}
	opCtx, cancel := adapter.operationContext(ctx)
	defer cancel()
	if _, err := adapter.beginToolAttempt(opCtx, toolExecution, semantics, payload); err != nil {
		t.Fatal(err)
	}
}

func newTestAdapter(
	t *testing.T,
	ctx context.Context,
	store agentledger.EventStore,
	checkpointStore agentledger.CheckpointStore,
	overrides Config,
) *Adapter {
	t.Helper()
	config := overrides
	config.Store = store
	config.CheckpointStore = checkpointStore
	config.CheckpointKey = "agentgo:session"
	config.SessionID = "session"
	config.RunID = "run"
	config.Actor = agentledger.NewActorWithKey("agent:session", "agent", "agentgo")
	config.OperationTimeout = time.Second
	adapter, err := New(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func newTestAgent(model agentgo.ChatModel, tools []agentgo.Tool, adapter *Adapter) *agentgo.Agent {
	options := []agentgo.AgentOption{agentgo.WithModel(model), agentgo.WithTools(tools...)}
	options = append(options, adapter.Options()...)
	return agentgo.NewAgent(options...)
}

func assistantText(text string) agentgo.Message {
	return agentgo.Message{
		Role:       agentgo.RoleAssistant,
		Content:    []agentgo.ContentBlock{agentgo.TextBlock(text)},
		StopReason: agentgo.StopReasonStop,
		Timestamp:  time.Now(),
	}
}

func assistantToolCall(call agentgo.ToolCall) agentgo.Message {
	return agentgo.Message{
		Role:       agentgo.RoleAssistant,
		Content:    []agentgo.ContentBlock{agentgo.ToolCallBlock(call)},
		StopReason: agentgo.StopReasonToolUse,
		Timestamp:  time.Now(),
	}
}

type scriptedModel struct {
	mu        sync.Mutex
	responses []agentgo.Message
	calls     int
}

func (m *scriptedModel) Generate(
	context.Context,
	[]agentgo.Message,
	[]agentgo.ToolSpec,
	...agentgo.CallOption,
) (*agentgo.LLMResponse, error) {
	return nil, errors.New("streaming is required")
}

func (m *scriptedModel) GenerateStream(
	context.Context,
	[]agentgo.Message,
	[]agentgo.ToolSpec,
	...agentgo.CallOption,
) (<-chan agentgo.StreamEvent, error) {
	m.mu.Lock()
	index := m.calls
	m.calls++
	var response agentgo.Message
	if index < len(m.responses) {
		response = m.responses[index]
	}
	m.mu.Unlock()
	stream := make(chan agentgo.StreamEvent, 1)
	if response.Role == "" {
		stream <- agentgo.StreamEvent{Type: agentgo.StreamEventError, Err: errors.New("unexpected model call")}
	} else {
		stream <- agentgo.StreamEvent{Type: agentgo.StreamEventDone, Message: response}
	}
	close(stream)
	return stream, nil
}

func (*scriptedModel) SupportsTools() bool { return true }

func (m *scriptedModel) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

type countingTool struct {
	mu     sync.Mutex
	name   string
	result json.RawMessage
	calls  int
}

func (t *countingTool) Name() string         { return t.name }
func (*countingTool) Description() string    { return "test tool" }
func (*countingTool) Schema() map[string]any { return map[string]any{"type": "object"} }
func (t *countingTool) Execute(context.Context, json.RawMessage) (json.RawMessage, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls++
	return t.result, nil
}
func (t *countingTool) callCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls
}

type failingCheckpointStore struct {
	agentledger.CheckpointStore
	mu     sync.Mutex
	failAt int
	saves  int
}

func (s *failingCheckpointStore) SaveCheckpoint(
	ctx context.Context,
	expectedRevision int64,
	checkpoint agentledger.ProposedCheckpoint,
) (agentledger.Checkpoint, error) {
	s.mu.Lock()
	s.saves++
	shouldFail := s.saves == s.failAt
	s.mu.Unlock()
	if shouldFail {
		return agentledger.Checkpoint{}, errors.New("checkpoint unavailable")
	}
	return s.CheckpointStore.SaveCheckpoint(ctx, expectedRevision, checkpoint)
}

type failingStore struct {
	agentledger.EventStore
	eventType string
}

type failingNthEventStore struct {
	agentledger.EventStore
	mu        sync.Mutex
	eventType string
	failAt    int
	seen      int
}

func (s *failingNthEventStore) Append(
	ctx context.Context,
	laneID string,
	expectedLastSeq int64,
	appendID string,
	events ...agentledger.ProposedEvent,
) (agentledger.AppendReceipt, error) {
	s.mu.Lock()
	shouldFail := false
	for _, event := range events {
		if event.EventType != s.eventType {
			continue
		}
		s.seen++
		shouldFail = shouldFail || s.seen == s.failAt
	}
	s.mu.Unlock()
	if shouldFail {
		return agentledger.AppendReceipt{}, errors.New("event write failed")
	}
	return s.EventStore.Append(ctx, laneID, expectedLastSeq, appendID, events...)
}

func (s failingStore) Append(
	ctx context.Context,
	laneID string,
	expectedLastSeq int64,
	appendID string,
	events ...agentledger.ProposedEvent,
) (agentledger.AppendReceipt, error) {
	for _, event := range events {
		if event.EventType == s.eventType {
			return agentledger.AppendReceipt{}, errors.New("prewrite failed")
		}
	}
	return s.EventStore.Append(ctx, laneID, expectedLastSeq, appendID, events...)
}
