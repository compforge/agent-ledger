package agentgoadapter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentgo"
	agentcodec "github.com/compforge/agentgo/codec"
)

const CheckpointFormat = "application/vnd.compforge.agentgo.snapshot+json;version=1"

var Descriptor = agentledger.AdapterDescriptor{
	SchemaVersion: "1.0", AdapterID: "agentgo", AdapterVersion: "2",
	Framework: "agentgo", FrameworkVersion: ">=0.0.7 <1",
	Capabilities: agentledger.AdapterCapabilities{
		ModelPrewrite: "strict", ToolPrewrite: "strict", OutcomeGate: "best_effort",
		Recovery: "checkpoint", PreservesNativeState: true,
	},
}

type ToolSemantics struct {
	Effect         agentledger.Effect
	IdempotencyKey string
}

type ToolSemanticsResolver func(agentgo.ToolCall) ToolSemantics

type ModelIdentity struct {
	ID       string
	Provider string
}

type ModelIdentityResolver func(agentgo.ModelExecution) ModelIdentity

type ToolRetryDecision struct {
	Approved           bool
	RecoveryDecisionID string
}

// ToolRetryPolicy authorizes one physical retry of one unresolved tool
// Attempt. RecoveryDecisionID identifies the one-shot external decision that
// this retry consumes.
type ToolRetryPolicy func(agentledger.Action, agentledger.Attempt, agentgo.ToolCall) ToolRetryDecision

type Config struct {
	Store            agentledger.EventStore
	CheckpointStore  agentledger.CheckpointStore
	CheckpointKey    string
	SessionID        string
	RunID            string
	Actor            agentledger.Actor
	StateCodec       agentcodec.Codec
	OperationTimeout time.Duration

	BeforeRun  agentgo.BeforeRunHook
	AfterRun   agentgo.AfterRunHook
	BeforeTurn agentgo.BeforeTurnHook
	AfterTurn  agentgo.AfterTurnHook

	ModelMiddlewares []agentgo.ModelMiddleware
	ToolMiddlewares  []agentgo.ToolMiddleware
	// ModelIdentity supplies the caller-owned model resource selected for an
	// execution. AgentGo middleware does not expose the configured ChatModel.
	ModelIdentity ModelIdentityResolver

	// ToolSemantics fixes the logical Action's Effect and effective
	// idempotency key before execution.
	ToolSemantics ToolSemanticsResolver
	// CanRetryTool is fail-closed by default. The Adapter calls it before
	// AgentGo accepts a recovery run, so an unsafe unresolved tool never enters
	// the Loop.
	CanRetryTool ToolRetryPolicy
}

type Adapter struct {
	runtimeRecorder *agentledger.LaneRecorder
	checkpointStore agentledger.CheckpointStore
	checkpointKey   string
	stateCodec      agentcodec.Codec
	timeout         time.Duration

	beforeRun  agentgo.BeforeRunHook
	afterRun   agentgo.AfterRunHook
	beforeTurn agentgo.BeforeTurnHook
	afterTurn  agentgo.AfterTurnHook

	modelMiddlewares []agentgo.ModelMiddleware
	toolMiddlewares  []agentgo.ToolMiddleware
	modelIdentity    ModelIdentityResolver
	toolSemantics    ToolSemanticsResolver
	canRetryTool     ToolRetryPolicy

	mu                 sync.Mutex
	currentTurn        turnBinding
	executionScope     string
	checkpointRevision int64
	journal            *runJournal
	retryDecisions     map[string]ToolRetryDecision
}

type turnBinding struct {
	ID        string
	Index     int
	Completed bool
}

func New(ctx context.Context, config Config) (*Adapter, error) {
	if config.Store == nil {
		return nil, errors.New("agentgo adapter requires an event store")
	}
	if config.OperationTimeout <= 0 {
		return nil, errors.New("agentgo adapter operation timeout must be positive")
	}
	if config.CheckpointKey == "" {
		return nil, errors.New("agentgo adapter requires a caller-owned checkpoint key")
	}
	checkpointStore := config.CheckpointStore
	if checkpointStore == nil {
		checkpointStore, _ = config.Store.(agentledger.CheckpointStore)
	}
	if checkpointStore == nil {
		return nil, errors.New("agentgo adapter requires a checkpoint store")
	}
	stateCodec := config.StateCodec
	if stateCodec == nil {
		var err error
		stateCodec, err = agentgo.NewCodec()
		if err != nil {
			return nil, fmt.Errorf("create agentgo state codec: %w", err)
		}
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
	modelIdentity := config.ModelIdentity
	if modelIdentity == nil {
		modelIdentity = func(agentgo.ModelExecution) ModelIdentity { return ModelIdentity{} }
	}
	runtimeRecorder, err := agentledger.OpenRecorder(ctx, agentledger.RecorderOptions{
		Store: config.Store, SessionID: config.SessionID, RunID: config.RunID,
		LaneName: "main", Actor: config.Actor,
	})
	if err != nil {
		return nil, fmt.Errorf("open agentgo runtime recorder: %w", err)
	}
	return &Adapter{
		runtimeRecorder:  runtimeRecorder,
		checkpointStore:  checkpointStore,
		checkpointKey:    config.CheckpointKey,
		stateCodec:       stateCodec,
		timeout:          config.OperationTimeout,
		beforeRun:        config.BeforeRun,
		afterRun:         config.AfterRun,
		beforeTurn:       config.BeforeTurn,
		afterTurn:        config.AfterTurn,
		modelMiddlewares: append([]agentgo.ModelMiddleware(nil), config.ModelMiddlewares...),
		toolMiddlewares:  append([]agentgo.ToolMiddleware(nil), config.ToolMiddlewares...),
		modelIdentity:    modelIdentity,
		toolSemantics:    toolSemantics,
		canRetryTool:     canRetryTool,
		retryDecisions:   make(map[string]ToolRetryDecision),
	}, nil
}

// Options installs the complete strict Adapter profile. Application
// middlewares supplied in Config run inside the Ledger middleware, so an
// application short-circuit is recorded with the same Attempt lifecycle.
func (a *Adapter) Options() []agentgo.AgentOption {
	modelMiddlewares := append([]agentgo.ModelMiddleware{a.ModelMiddleware()}, a.modelMiddlewares...)
	toolMiddlewares := append([]agentgo.ToolMiddleware{a.ToolMiddleware()}, a.toolMiddlewares...)
	return []agentgo.AgentOption{
		agentgo.WithBeforeRun(a.handleBeforeRun),
		agentgo.WithAfterRun(a.handleAfterRun),
		agentgo.WithBeforeTurn(a.handleBeforeTurn),
		agentgo.WithAfterTurn(a.handleAfterTurn),
		agentgo.WithModelMiddlewares(modelMiddlewares...),
		agentgo.WithToolMiddlewares(toolMiddlewares...),
	}
}

func (a *Adapter) handleBeforeTurn(ctx context.Context, turn agentgo.BeforeTurnContext) ([]agentgo.AgentMessage, error) {
	opCtx, cancel := a.operationContext(ctx)
	defer cancel()
	binding, err := a.prepareTurn(opCtx, turn.TurnIndex)
	if err != nil {
		return nil, fmt.Errorf("record agentgo turn start: %w", err)
	}
	a.setTurn(binding)
	if a.beforeTurn == nil {
		return nil, nil
	}
	messages, hookErr := a.beforeTurn(ctx, turn)
	if hookErr == nil {
		return messages, nil
	}
	if !binding.Completed {
		if _, recordErr := a.runtimeRecorder.FailTurn(opCtx, binding.ID, hookErr); recordErr != nil {
			return nil, fmt.Errorf("agentgo before-turn failed: %v; record turn failure: %w", hookErr, recordErr)
		}
		a.markTurnFailed(binding.Index)
	}
	a.clearTurn()
	return nil, hookErr
}

func (a *Adapter) handleAfterTurn(ctx context.Context, turn agentgo.AfterTurnContext) error {
	binding, err := a.turn()
	if err != nil {
		return err
	}
	opCtx, cancel := a.operationContext(ctx)
	defer cancel()
	// AgentGo converts ToolMiddleware errors into ordinary tool-error results.
	// Catch a failed outcome write at the Turn boundary so no later model call
	// can consume that transient result. The admission checkpoint remains the
	// recovery baseline because AfterRun observes the resulting run error.
	if pending := a.pendingTools(); len(pending) > 0 {
		blocked := &RecoveryBlockedError{Tools: pending}
		// Leave the Turn open: recovery will re-enter this same logical Turn,
		// apply the known model outcome, and reconcile the unresolved tool.
		a.clearTurn()
		return blocked
	}
	if a.afterTurn != nil {
		if hookErr := a.afterTurn(ctx, turn); hookErr != nil {
			if !binding.Completed {
				if _, recordErr := a.runtimeRecorder.FailTurn(opCtx, binding.ID, hookErr); recordErr != nil {
					return fmt.Errorf("agentgo after-turn failed: %v; record turn failure: %w", hookErr, recordErr)
				}
				a.markTurnFailed(binding.Index)
			}
			a.clearTurn()
			return hookErr
		}
	}
	if !binding.Completed {
		if _, err := a.runtimeRecorder.CompleteTurn(opCtx, binding.ID, map[string]any{
			"turn_index": turn.TurnIndex, "execution_scope": a.scope(),
		}); err != nil {
			return fmt.Errorf("record agentgo turn completion: %w", err)
		}
		a.markTurnCompleted(binding.Index)
	}
	a.clearTurn()
	return nil
}

func (a *Adapter) turn() (turnBinding, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.currentTurn.ID == "" {
		return turnBinding{}, errors.New("agentgo execution has no active turn")
	}
	return a.currentTurn, nil
}

func (a *Adapter) setTurn(binding turnBinding) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.currentTurn = binding
}

func (a *Adapter) clearTurn() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.currentTurn = turnBinding{}
}

func (a *Adapter) scope() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.executionScope
}

func (a *Adapter) actionKey(execution agentgo.Execution) string {
	return a.scope() + "/" + execution.ID
}

func (a *Adapter) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, a.timeout)
}

func (a *Adapter) detachedOperationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), a.timeout)
}
