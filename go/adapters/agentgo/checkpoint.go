package agentgoadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentgo"
)

type nativeCheckpoint struct {
	Snapshot       agentgo.AgentSnapshot `codec:"snapshot"`
	ExecutionScope string                `codec:"execution_scope"`
	ScopeComplete  bool                  `codec:"scope_complete"`
}

type PendingToolRecovery struct {
	Action  agentledger.Action
	Attempt agentledger.Attempt
	Call    agentgo.ToolCall
}

// RecoveryBlockedError rejects a run before AgentGo enters its Loop because
// one or more prior tool Attempts need an external retry decision.
type RecoveryBlockedError struct {
	Tools []PendingToolRecovery
}

func (e *RecoveryBlockedError) Error() string {
	if len(e.Tools) == 0 {
		return "agentgo recovery is blocked"
	}
	tool := e.Tools[0]
	return fmt.Sprintf(
		"agentgo recovery is blocked by tool action %s attempt %s",
		tool.Action.ID,
		tool.Attempt.ID,
	)
}

func (a *Adapter) handleBeforeRun(ctx context.Context, run agentgo.BeforeRunContext) (agentgo.AgentSnapshot, error) {
	opCtx, cancel := a.operationContext(ctx)
	defer cancel()

	native, revision, found, err := a.loadCheckpoint(opCtx)
	if err != nil {
		return agentgo.AgentSnapshot{}, err
	}
	newScope := !found || native.ScopeComplete
	if !found {
		native.Snapshot = run.Snapshot
	}
	if newScope {
		native.ExecutionScope = agentledger.NewID()
		native.ScopeComplete = false
	}
	journal, err := loadRunJournal(opCtx, a.runtimeRecorder, native.ExecutionScope)
	if err != nil {
		return agentgo.AgentSnapshot{}, fmt.Errorf("load agentgo recovery journal: %w", err)
	}
	decisions, blocked := a.recoveryDecisions(journal)
	if len(blocked) > 0 {
		return agentgo.AgentSnapshot{}, &RecoveryBlockedError{Tools: blocked}
	}

	restored := native.Snapshot
	if a.beforeRun != nil {
		restored, err = a.beforeRun(ctx, agentgo.BeforeRunContext{
			Kind: run.Kind, Snapshot: restored, Input: run.Input,
		})
		if err != nil {
			return agentgo.AgentSnapshot{}, err
		}
	}
	native.Snapshot = restored
	a.setRecoveryState(native.ExecutionScope, revision, journal, decisions)
	if newScope {
		if err := a.persistCheckpoint(opCtx, native); err != nil {
			return agentgo.AgentSnapshot{}, fmt.Errorf("save agentgo run baseline: %w", err)
		}
	}
	return restored, nil
}

func (a *Adapter) handleAfterRun(ctx context.Context, run agentgo.AfterRunContext) error {
	var hookErr error
	if a.afterRun != nil {
		hookErr = a.afterRun(ctx, run)
	}
	// Keep the admission checkpoint on failed or interrupted runs. Replacing it
	// with the partially advanced in-memory state would make the durable Action
	// outcomes impossible to apply through AgentGo's native Loop on recovery.
	if run.Err != nil || hookErr != nil || run.Summary.EndReason != agentgo.EndReasonStop {
		return hookErr
	}
	native := nativeCheckpoint{
		Snapshot:       run.Snapshot,
		ExecutionScope: a.scope(),
		ScopeComplete:  true,
	}
	opCtx, cancel := a.detachedOperationContext(ctx)
	defer cancel()
	if err := a.persistCheckpoint(opCtx, native); err != nil {
		return fmt.Errorf("save agentgo final checkpoint: %w", err)
	}
	return nil
}

func (a *Adapter) persistCheckpoint(ctx context.Context, native nativeCheckpoint) error {
	checkpoint, err := a.saveCheckpoint(ctx, native)
	if err != nil {
		return err
	}
	_, err = a.runtimeRecorder.LinkCheckpoint(ctx, agentledger.CheckpointLink{
		CheckpointID:   checkpoint.ID,
		Profile:        "agentgo",
		ProfileVersion: "2",
		Metadata: map[string]any{
			"execution_scope": native.ExecutionScope,
			"scope_complete":  native.ScopeComplete,
		},
	})
	if err != nil {
		return fmt.Errorf("link agentgo checkpoint: %w", err)
	}
	return nil
}

func (a *Adapter) loadCheckpoint(ctx context.Context) (nativeCheckpoint, int64, bool, error) {
	checkpoint, found, err := a.checkpointStore.LoadLatestCheckpoint(ctx, a.checkpointKey)
	if err != nil {
		return nativeCheckpoint{}, 0, false, fmt.Errorf("load agentgo checkpoint: %w", err)
	}
	if !found {
		return nativeCheckpoint{}, 0, false, nil
	}
	if checkpoint.Format != CheckpointFormat {
		return nativeCheckpoint{}, 0, false, fmt.Errorf(
			"agentgo checkpoint format %q is not supported", checkpoint.Format,
		)
	}
	encoded, err := json.Marshal(checkpoint.State)
	if err != nil {
		return nativeCheckpoint{}, 0, false, fmt.Errorf("encode stored agentgo checkpoint: %w", err)
	}
	var native nativeCheckpoint
	if err := a.stateCodec.Unmarshal(encoded, &native); err != nil {
		return nativeCheckpoint{}, 0, false, fmt.Errorf("decode agentgo checkpoint: %w", err)
	}
	if native.ExecutionScope == "" {
		return nativeCheckpoint{}, 0, false, errors.New("agentgo checkpoint is missing execution_scope")
	}
	return native, checkpoint.Revision, true, nil
}

func (a *Adapter) saveCheckpoint(ctx context.Context, native nativeCheckpoint) (agentledger.Checkpoint, error) {
	encoded, err := a.stateCodec.Marshal(native)
	if err != nil {
		return agentledger.Checkpoint{}, fmt.Errorf("encode agentgo snapshot: %w", err)
	}
	var state map[string]any
	if err := json.Unmarshal(encoded, &state); err != nil {
		return agentledger.Checkpoint{}, fmt.Errorf("materialize agentgo checkpoint state: %w", err)
	}
	proposed := agentledger.NewCheckpoint(
		a.checkpointKey,
		a.runtimeRecorder.Actor().ID,
		CheckpointFormat,
		state,
	)
	anchor, err := a.latestAnchor(ctx)
	if err != nil {
		return agentledger.Checkpoint{}, err
	}
	proposed.Anchor = anchor

	a.mu.Lock()
	expectedRevision := a.checkpointRevision
	a.mu.Unlock()
	checkpoint, err := a.checkpointStore.SaveCheckpoint(ctx, expectedRevision, proposed)
	if err != nil {
		return agentledger.Checkpoint{}, err
	}
	a.mu.Lock()
	a.checkpointRevision = checkpoint.Revision
	a.mu.Unlock()
	return checkpoint, nil
}

func (a *Adapter) latestAnchor(ctx context.Context) (*agentledger.CheckpointAnchor, error) {
	lane := a.runtimeRecorder.Lane()
	if lane.LastSeq == 0 {
		return nil, nil
	}
	page, err := a.runtimeRecorder.Store().LoadLanePage(ctx, lane.ID, lane.LastSeq-1, 1)
	if err != nil {
		return nil, fmt.Errorf("load agentgo checkpoint anchor: %w", err)
	}
	if len(page.Events) != 1 || page.Events[0].Seq != lane.LastSeq {
		return nil, fmt.Errorf("agentgo runtime lane is missing event at seq %d", lane.LastSeq)
	}
	event := page.Events[0]
	return &agentledger.CheckpointAnchor{
		LaneID: lane.ID, LastAppliedSeq: event.Seq, LastAppliedEventID: event.ID,
	}, nil
}

func (a *Adapter) recoveryDecisions(journal *runJournal) (map[string]ToolRetryDecision, []PendingToolRecovery) {
	decisions := make(map[string]ToolRetryDecision)
	var blocked []PendingToolRecovery
	for _, pending := range journal.pendingTools() {
		decision := a.canRetryTool(pending.Action, pending.Attempt, pending.Call)
		if !decision.Approved {
			blocked = append(blocked, pending)
			continue
		}
		decisions[pending.Action.ID] = decision
	}
	if len(blocked) > 0 {
		return nil, blocked
	}
	return decisions, nil
}

func (a *Adapter) setRecoveryState(
	scope string,
	revision int64,
	journal *runJournal,
	decisions map[string]ToolRetryDecision,
) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.executionScope = scope
	a.checkpointRevision = revision
	a.journal = journal
	a.retryDecisions = decisions
	a.currentTurn = turnBinding{}
}
