package agentgoadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentgo"
)

var Descriptor = agentledger.AdapterDescriptor{
	SchemaVersion: "1.0", AdapterID: "agentgo", AdapterVersion: "1",
	Framework: "agentgo", FrameworkVersion: ">=0.0.1 <1",
	Capabilities: agentledger.AdapterCapabilities{
		ModelPrewrite: "strict", ToolPrewrite: "strict", OutcomeGate: "strict",
		Recovery: "native_store", PreservesNativeState: true,
	},
}

type MessageCodec interface {
	Encode(agentgo.AgentMessage) (kind string, data json.RawMessage, err error)
	Decode(kind string, data json.RawMessage) (agentgo.AgentMessage, error)
}

type ToolSemantics struct {
	Effect         agentledger.Effect
	IdempotencyKey string
}

type ToolSemanticsResolver func(agentgo.ToolCall) ToolSemantics

type ToolRetryDecision struct {
	Approved           bool
	RecoveryDecisionID string
}

// ToolRetryPolicy authorizes one physical retry of one unresolved Attempt.
// RecoveryDecisionID proves which one-shot authorization the retry consumed.
type ToolRetryPolicy func(agentledger.Action, agentledger.Attempt, agentgo.ToolCall) ToolRetryDecision

type Config struct {
	Store           agentledger.EventStore
	SessionID       string
	RunID           string
	Actor           agentledger.Actor
	NativeSessionID string
	MessageCodec    MessageCodec
	BeforeTurn      agentgo.BeforeTurnHook
	AfterTurn       agentgo.AfterTurnHook
	// ToolSemantics fixes the logical Action's Effect and effective idempotency key before execution.
	ToolSemantics ToolSemanticsResolver
	// CanRetryTool is intentionally fail-closed by default; recovery policy belongs to the caller.
	CanRetryTool     ToolRetryPolicy
	OperationTimeout time.Duration
}

type Adapter struct {
	runtimeRecorder *agentledger.LaneRecorder
	stateRecorder   *agentledger.LaneRecorder
	codec           MessageCodec
	timeout         time.Duration
	beforeTurn      agentgo.BeforeTurnHook
	afterTurn       agentgo.AfterTurnHook
	toolSemantics   ToolSemanticsResolver
	canRetryTool    ToolRetryPolicy

	mu          sync.Mutex
	currentTurn string
}

func New(ctx context.Context, config Config) (*Adapter, error) {
	if config.Store == nil {
		return nil, errors.New("agentgo adapter requires an event store")
	}
	if config.OperationTimeout <= 0 {
		return nil, errors.New("agentgo adapter operation timeout must be positive")
	}
	codec := config.MessageCodec
	if codec == nil {
		codec = ConcreteMessageCodec{}
	}
	nativeSessionID := config.NativeSessionID
	if nativeSessionID == "" {
		nativeSessionID = config.SessionID
	}
	toolSemantics := config.ToolSemantics
	if toolSemantics == nil {
		toolSemantics = func(agentgo.ToolCall) ToolSemantics {
			return ToolSemantics{Effect: agentledger.UnknownEffect()}
		}
	}
	canRetryTool := config.CanRetryTool
	if canRetryTool == nil {
		canRetryTool = func(agentledger.Action, agentledger.Attempt, agentgo.ToolCall) ToolRetryDecision {
			return ToolRetryDecision{}
		}
	}
	runtimeRecorder, err := agentledger.OpenRecorder(ctx, agentledger.RecorderOptions{
		Store: config.Store, SessionID: config.SessionID, RunID: config.RunID,
		LaneName: "main", Actor: config.Actor,
	})
	if err != nil {
		return nil, fmt.Errorf("open agentgo runtime recorder: %w", err)
	}
	stateRecorder, err := agentledger.OpenRecorder(ctx, agentledger.RecorderOptions{
		Store: config.Store, SessionID: config.SessionID, RunID: config.RunID,
		LaneName: "framework/agentgo/" + nativeSessionID, Actor: config.Actor,
	})
	if err != nil {
		return nil, fmt.Errorf("open agentgo state recorder: %w", err)
	}
	return &Adapter{
		runtimeRecorder: runtimeRecorder, stateRecorder: stateRecorder, codec: codec,
		timeout: config.OperationTimeout, beforeTurn: config.BeforeTurn, afterTurn: config.AfterTurn,
		toolSemantics: toolSemantics, canRetryTool: canRetryTool,
	}, nil
}

// Options must be installed after any competing message committer or middleware options.
func (a *Adapter) Options(existing ...agentgo.ToolMiddleware) []agentgo.AgentOption {
	middlewares := append([]agentgo.ToolMiddleware{a.ToolMiddleware()}, existing...)
	return []agentgo.AgentOption{
		agentgo.WithBeforeTurn(a.handleBeforeTurn),
		agentgo.WithAfterTurn(a.handleAfterTurn),
		agentgo.WithMessageCommitter(a.commitMessage),
		agentgo.WithMiddlewares(middlewares...),
	}
}

// WrapModel records every physical provider attempt, including AgentGo retries.
func (a *Adapter) WrapModel(model agentgo.ChatModel) agentgo.ChatModel {
	return &recordingModel{inner: model, adapter: a}
}

func (a *Adapter) ToolMiddleware() agentgo.ToolMiddleware {
	return func(ctx context.Context, call agentgo.ToolCall, next agentgo.ToolExecuteFunc) (json.RawMessage, error) {
		turnID, err := a.turnID(ctx)
		if err != nil {
			return nil, err
		}
		semantics := a.toolSemantics(call)
		if semantics.Effect.Idempotency == agentledger.IdempotencyKeyed && semantics.IdempotencyKey == "" {
			return nil, fmt.Errorf("tool %s declares keyed idempotency without an idempotency key", call.Name)
		}
		payload := map[string]any{"tool_name": call.Name, "input": call.Args}
		if semantics.IdempotencyKey != "" {
			payload["idempotency_key"] = semantics.IdempotencyKey
		}
		attempt, err := a.beforeToolCall(ctx, turnID, call, semantics.Effect, payload)
		if err != nil {
			return nil, fmt.Errorf("record agentgo tool request: %w", err)
		}
		result, callErr := next(ctx, call.Args)
		if callErr != nil {
			if err := a.runtimeRecorder.ToolFailed(ctx, attempt, callErr); err != nil {
				return nil, fmt.Errorf("record agentgo tool failure after execution: %w", err)
			}
			return result, callErr
		}
		if err := a.runtimeRecorder.ToolCompleted(ctx, attempt, map[string]any{"output": result}); err != nil {
			return nil, fmt.Errorf("record agentgo tool result after execution: %w", err)
		}
		return result, nil
	}
}

func (a *Adapter) beforeToolCall(
	ctx context.Context,
	turnID string,
	call agentgo.ToolCall,
	effect agentledger.Effect,
	payload map[string]any,
) (agentledger.AttemptHandle, error) {
	view, err := a.runtimeRecorder.Store().LoadSession(ctx, a.runtimeRecorder.SessionID())
	if err != nil {
		return agentledger.AttemptHandle{}, err
	}
	unresolved, exists := unresolvedToolAction(agentledger.SelectRun(view, a.runtimeRecorder.RunID()), call.ID)
	if !exists {
		return a.runtimeRecorder.BeforeToolCallWithEffect(ctx, turnID, call.ID, payload, effect)
	}
	if unresolved.Action.Effect.Idempotency == agentledger.IdempotencyKeyed {
		key, _ := payload["idempotency_key"].(string)
		if key == "" || key != unresolved.IdempotencyKey {
			return agentledger.AttemptHandle{}, fmt.Errorf(
				"tool action %s retry changed its effective idempotency key", unresolved.Action.ID,
			)
		}
	}
	// A restored AgentGo transcript re-emits the same ToolCall ID. Keep that
	// physical retry under the original logical Action and its fixed Effect.
	decision := a.canRetryTool(unresolved.Action, unresolved.Attempt, call)
	if !decision.Approved {
		return agentledger.AttemptHandle{}, fmt.Errorf("unresolved tool action %s attempt %s is not approved for retry", unresolved.Action.ID, unresolved.Attempt.ID)
	}
	if decision.RecoveryDecisionID != "" {
		payload["recovery_decision_id"] = decision.RecoveryDecisionID
	}
	retry, err := a.runtimeRecorder.Retry(ctx, unresolved.Action.ID, unresolved.Attempt.AttemptNo+1, payload)
	if err != nil {
		return agentledger.AttemptHandle{}, err
	}
	// The old Attempt remains an immutable record of an unknown outcome. Close
	// its execution lifecycle before invoking the external tool again; recovery
	// policy can still inspect the structured reason and the superseding Attempt.
	for _, prior := range unresolved.All {
		handle := agentledger.AttemptHandle{
			ActionType: agentledger.ActionTypeToolCall, ActionID: unresolved.Action.ID,
			AttemptID: prior.Attempt.ID, AttemptNo: prior.Attempt.AttemptNo,
			RequestedEventID: prior.RequestedEventID,
		}
		if _, err := a.runtimeRecorder.MarkAttemptOutcomeUnknown(
			ctx, handle, "superseded by an authorized retry", retry.AttemptID,
		); err != nil {
			return agentledger.AttemptHandle{}, fmt.Errorf("close superseded tool attempt %s: %w", prior.Attempt.ID, err)
		}
	}
	return retry, nil
}

type unresolvedAttempt struct {
	Attempt          agentledger.Attempt
	RequestedEventID string
}

type unresolvedTool struct {
	Action         agentledger.Action
	Attempt        agentledger.Attempt
	IdempotencyKey string
	All            []unresolvedAttempt
}

func unresolvedToolAction(view agentledger.RunView, toolCallID string) (unresolvedTool, bool) {
	actions := make(map[string]agentledger.Action, len(view.Actions))
	for _, action := range view.Actions {
		if action.Type == agentledger.ActionTypeToolCall && action.Key == toolCallID {
			actions[action.ID] = action
		}
	}
	attempts := make(map[string]agentledger.Attempt, len(view.Attempts))
	for _, attempt := range view.Attempts {
		if _, ok := actions[attempt.ActionID]; ok {
			attempts[attempt.ID] = attempt
		}
	}
	terminal := make(map[string]bool, len(attempts))
	requested := make(map[string]agentledger.StoredEvent, len(attempts))
	for _, event := range view.Events {
		if _, ok := attempts[event.SubjectID]; !ok {
			continue
		}
		switch event.EventType {
		case agentledger.EventTypeAttemptRequested:
			requested[event.SubjectID] = event
		case agentledger.EventTypeAttemptCompleted, agentledger.EventTypeAttemptFailed,
			agentledger.EventTypeAttemptCancelled, agentledger.EventTypeAttemptOutcomeUnknown:
			terminal[event.SubjectID] = true
		}
	}
	var selected agentledger.Attempt
	for attemptID := range requested {
		attempt := attempts[attemptID]
		if !terminal[attemptID] && attempt.AttemptNo > selected.AttemptNo {
			selected = attempt
		}
	}
	if selected.ID == "" {
		return unresolvedTool{}, false
	}
	selectedRequest := requested[selected.ID]
	idempotencyKey, _ := selectedRequest.Payload["idempotency_key"].(string)
	result := unresolvedTool{
		Action: actions[selected.ActionID], Attempt: selected, IdempotencyKey: idempotencyKey,
	}
	for attemptID, event := range requested {
		attempt := attempts[attemptID]
		if attempt.ActionID == selected.ActionID && !terminal[attemptID] {
			result.All = append(result.All, unresolvedAttempt{Attempt: attempt, RequestedEventID: event.ID})
		}
	}
	return result, true
}

// Restore replaces an idle AgentGo transcript with its last durably committed prefix.
func (a *Adapter) Restore(ctx context.Context, agent *agentgo.Agent) error {
	release := agent.HoldRuns()
	defer release()
	if err := ctx.Err(); err != nil {
		return err
	}
	messages, err := a.loadMessages(ctx)
	if err != nil {
		return err
	}
	if err := agent.SetMessages(messages); err != nil {
		return fmt.Errorf("restore agentgo messages: %w", err)
	}
	return nil
}

func (a *Adapter) RestoreAndContinue(ctx context.Context, agent *agentgo.Agent) error {
	if err := a.Restore(ctx, agent); err != nil {
		return err
	}
	return agent.Continue(ctx)
}

func (a *Adapter) commitMessage(message agentgo.AgentMessage) error {
	kind, data, err := a.codec.Encode(message)
	if err != nil {
		return fmt.Errorf("encode agentgo message: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
	defer cancel()
	_, err = a.stateRecorder.Record(ctx, "lane.framework.agentgo.message.committed", a.stateRecorder.Lane().ID, map[string]any{
		"message_type": kind, "message_json": string(data),
	}, "")
	if err != nil {
		return fmt.Errorf("persist agentgo message: %w", err)
	}
	return nil
}

func (a *Adapter) handleBeforeTurn(ctx context.Context, turn agentgo.BeforeTurnContext) ([]agentgo.AgentMessage, error) {
	created, err := a.runtimeRecorder.StartTurn(ctx, map[string]any{"turn": turn.TurnIndex})
	if err != nil {
		return nil, fmt.Errorf("record agentgo turn start: %w", err)
	}
	a.setTurn(created.ID)
	if a.beforeTurn != nil {
		return a.beforeTurn(ctx, turn)
	}
	return nil, nil
}

func (a *Adapter) handleAfterTurn(ctx context.Context, turn agentgo.AfterTurnContext) error {
	turnID, err := a.turnID(ctx)
	if err != nil {
		return err
	}
	if a.afterTurn != nil {
		if err := a.afterTurn(ctx, turn); err != nil {
			if _, recordErr := a.runtimeRecorder.FailTurn(ctx, turnID, err); recordErr != nil {
				return fmt.Errorf("agentgo after-turn failed: %v; record turn failure: %w", err, recordErr)
			}
			return err
		}
	}
	if _, err := a.runtimeRecorder.CompleteTurn(ctx, turnID, map[string]any{"turn": turn.TurnIndex}); err != nil {
		return fmt.Errorf("record agentgo turn completion: %w", err)
	}
	a.clearTurn()
	return nil
}

func (a *Adapter) loadMessages(ctx context.Context) ([]agentgo.AgentMessage, error) {
	var messages []agentgo.AgentMessage
	for event, err := range a.stateRecorder.Store().LoadLane(ctx, a.stateRecorder.Lane().ID, 0) {
		if err != nil {
			return nil, fmt.Errorf("load agentgo native state: %w", err)
		}
		if event.EventType != "lane.framework.agentgo.message.committed" {
			continue
		}
		kind, ok := event.Payload["message_type"].(string)
		if !ok {
			return nil, errors.New("agentgo state event is missing message_type")
		}
		encoded, ok := event.Payload["message_json"].(string)
		if !ok {
			return nil, errors.New("agentgo state event is missing message_json")
		}
		message, err := a.codec.Decode(kind, json.RawMessage(encoded))
		if err != nil {
			return nil, fmt.Errorf("decode agentgo message type %q: %w", kind, err)
		}
		messages = append(messages, message)
	}
	return messages, nil
}

func (a *Adapter) turnID(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.currentTurn != "" {
		return a.currentTurn, nil
	}
	turn, err := a.runtimeRecorder.StartTurn(ctx, map[string]any{"implicit": true})
	if err != nil {
		return "", fmt.Errorf("open implicit agentgo turn: %w", err)
	}
	a.currentTurn = turn.ID
	return turn.ID, nil
}

func (a *Adapter) setTurn(turnID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.currentTurn = turnID
}

func (a *Adapter) clearTurn() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.currentTurn = ""
}

type ConcreteMessageCodec struct{}

func (ConcreteMessageCodec) Encode(message agentgo.AgentMessage) (string, json.RawMessage, error) {
	switch value := message.(type) {
	case agentgo.Message:
		data, err := json.Marshal(value)
		return "message", data, err
	case *agentgo.Message:
		data, err := json.Marshal(value)
		return "message", data, err
	default:
		return "", nil, fmt.Errorf("unsupported custom AgentMessage %T; configure MessageCodec", message)
	}
}

func (ConcreteMessageCodec) Decode(kind string, data json.RawMessage) (agentgo.AgentMessage, error) {
	if kind != "message" {
		return nil, fmt.Errorf("unsupported message type %q", kind)
	}
	var message agentgo.Message
	if err := json.Unmarshal(data, &message); err != nil {
		return nil, err
	}
	return message, nil
}
