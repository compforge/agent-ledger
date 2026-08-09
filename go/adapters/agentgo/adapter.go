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
	SchemaVersion:    "1.0",
	AdapterID:        "agentgo",
	AdapterVersion:   "1",
	Framework:        "agentgo",
	FrameworkVersion: ">=0.0.1 <1",
	Capabilities: agentledger.AdapterCapabilities{
		ModelPrewrite:        "strict",
		ToolPrewrite:         "strict",
		OutcomeGate:          "strict",
		Recovery:             "native_store",
		PreservesNativeState: true,
	},
}

type MessageCodec interface {
	Encode(agentgo.AgentMessage) (kind string, data json.RawMessage, err error)
	Decode(kind string, data json.RawMessage) (agentgo.AgentMessage, error)
}

type Config struct {
	Store            agentledger.EventStore
	SessionID        string
	RunID            string
	Actor            agentledger.Actor
	NativeSessionID  string
	MessageCodec     MessageCodec
	BeforeTurn       agentgo.BeforeTurnHook
	AfterTurn        agentgo.AfterTurnHook
	OperationTimeout time.Duration
}

type Adapter struct {
	runtimeRecorder *agentledger.SessionRecorder
	stateRecorder   *agentledger.SessionRecorder
	codec           MessageCodec
	timeout         time.Duration
	beforeTurn      agentgo.BeforeTurnHook
	afterTurn       agentgo.AfterTurnHook

	mu          sync.Mutex
	currentStep string
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
	runtimeRecorder, err := agentledger.ResumeRecorder(ctx, agentledger.RecorderOptions{
		Store: config.Store, SessionID: config.SessionID, RunID: config.RunID, Actor: config.Actor,
	})
	if err != nil {
		return nil, fmt.Errorf("resume agentgo runtime recorder: %w", err)
	}
	stateRecorder, err := agentledger.ResumeRecorder(ctx, agentledger.RecorderOptions{
		Store: config.Store, SessionID: config.SessionID, RunID: config.RunID,
		StreamID: "framework/agentgo/" + nativeSessionID, Actor: config.Actor,
	})
	if err != nil {
		return nil, fmt.Errorf("resume agentgo state recorder: %w", err)
	}
	return &Adapter{
		runtimeRecorder: runtimeRecorder,
		stateRecorder:   stateRecorder,
		codec:           codec,
		timeout:         config.OperationTimeout,
		beforeTurn:      config.BeforeTurn,
		afterTurn:       config.AfterTurn,
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
		attempt, err := a.runtimeRecorder.BeforeToolCall(ctx, a.stepID(), map[string]any{
			"tool_call_id": call.ID,
			"tool_name":    call.Name,
			"arguments":    string(call.Args),
		})
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
		if err := a.runtimeRecorder.ToolCompleted(ctx, attempt, map[string]any{"result": string(result)}); err != nil {
			return nil, fmt.Errorf("record agentgo tool result after execution: %w", err)
		}
		return result, nil
	}
}

// Restore replaces an idle AgentGo transcript with its last durably committed prefix.
// Do not call it from an AgentGo event listener: HoldRuns waits for the active run to drain.
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
	_, err = a.stateRecorder.Record(ctx, "framework.agentgo.message.committed", map[string]any{
		"message_type": kind,
		"message_json": string(data),
	}, "", "")
	if err != nil {
		return fmt.Errorf("persist agentgo message: %w", err)
	}
	return nil
}

func (a *Adapter) handleBeforeTurn(ctx context.Context, turn agentgo.BeforeTurnContext) ([]agentgo.AgentMessage, error) {
	stepID := a.nextStep()
	if _, err := a.runtimeRecorder.Record(ctx, "step.started", map[string]any{"turn": turn.TurnIndex}, stepID, ""); err != nil {
		return nil, fmt.Errorf("record agentgo step start: %w", err)
	}
	if a.beforeTurn != nil {
		return a.beforeTurn(ctx, turn)
	}
	return nil, nil
}

func (a *Adapter) handleAfterTurn(ctx context.Context, turn agentgo.AfterTurnContext) error {
	stepID := a.stepID()
	if a.afterTurn != nil {
		if err := a.afterTurn(ctx, turn); err != nil {
			_, recordErr := a.runtimeRecorder.Record(ctx, "step.failed", map[string]any{"error": err.Error()}, stepID, "")
			if recordErr != nil {
				return fmt.Errorf("agentgo after-turn failed: %v; record step failure: %w", err, recordErr)
			}
			return err
		}
	}
	if _, err := a.runtimeRecorder.Record(ctx, "step.completed", map[string]any{"turn": turn.TurnIndex}, stepID, ""); err != nil {
		return fmt.Errorf("record agentgo step completion: %w", err)
	}
	a.clearStep()
	return nil
}

func (a *Adapter) loadMessages(ctx context.Context) ([]agentgo.AgentMessage, error) {
	var messages []agentgo.AgentMessage
	for event, err := range a.stateRecorder.Store().Load(ctx, a.stateRecorder.Stream(), -1) {
		if err != nil {
			return nil, fmt.Errorf("load agentgo native state: %w", err)
		}
		if event.EventType != "framework.agentgo.message.committed" {
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

func (a *Adapter) nextStep() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.currentStep = agentledger.NewID()
	return a.currentStep
}

func (a *Adapter) stepID() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.currentStep == "" {
		a.currentStep = agentledger.NewID()
	}
	return a.currentStep
}

func (a *Adapter) clearStep() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.currentStep = ""
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
