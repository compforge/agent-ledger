package agentgoadapter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentgo"
)

const unresolvedAttemptReason = "adapter recovered an attempt without a durable outcome"

func (a *Adapter) beginModelAttempt(
	ctx context.Context,
	execution agentgo.Execution,
	payload map[string]any,
) (agentledger.AttemptHandle, error) {
	turn, err := a.turnForExecution(execution)
	if err != nil {
		return agentledger.AttemptHandle{}, err
	}
	key := a.actionKey(execution)
	record, exists := a.execution(agentledger.ActionTypeModelCall, key)
	if exists {
		if err := validateRequestFingerprint(record, payload); err != nil {
			return agentledger.AttemptHandle{}, err
		}
		if err := a.markUnresolvedAttemptsUnknown(ctx, record); err != nil {
			return agentledger.AttemptHandle{}, err
		}
		return a.retry(ctx, record, payload)
	}
	handle, err := a.runtimeRecorder.BeforeModelCallWithKey(ctx, turn.ID, key, payload)
	if err != nil {
		return agentledger.AttemptHandle{}, err
	}
	if err := a.rememberAttempt(ctx, handle, payload); err != nil {
		return agentledger.AttemptHandle{}, err
	}
	return handle, nil
}

func (a *Adapter) beginToolAttempt(
	ctx context.Context,
	execution agentgo.ToolExecution,
	semantics ToolSemantics,
	payload map[string]any,
) (agentledger.AttemptHandle, error) {
	turn, err := a.turnForExecution(execution.Execution)
	if err != nil {
		return agentledger.AttemptHandle{}, err
	}
	key := a.actionKey(execution.Execution)
	record, exists := a.execution(agentledger.ActionTypeToolCall, key)
	if !exists {
		handle, err := a.runtimeRecorder.BeforeToolCallWithEffect(
			ctx, turn.ID, key, payload, semantics.Effect,
		)
		if err != nil {
			return agentledger.AttemptHandle{}, err
		}
		if err := a.rememberAttempt(ctx, handle, payload); err != nil {
			return agentledger.AttemptHandle{}, err
		}
		return handle, nil
	}
	if err := validateRequestFingerprint(record, payload); err != nil {
		return agentledger.AttemptHandle{}, err
	}
	if agentledger.NormalizeEffect(record.Action.Effect) != semantics.Effect {
		return agentledger.AttemptHandle{}, fmt.Errorf(
			"tool action %s changed effect from %#v to %#v",
			record.Action.ID, record.Action.Effect, semantics.Effect,
		)
	}
	if err := validateRecoveredIdempotencyKey(record, semantics.IdempotencyKey); err != nil {
		return agentledger.AttemptHandle{}, err
	}
	if hasRequestedAttempt(record) {
		decision, err := a.consumeRetryDecision(record.Action.ID)
		if err != nil {
			return agentledger.AttemptHandle{}, err
		}
		payload["recovery_decision_id"] = decision.RecoveryDecisionID
	}
	if err := a.markUnresolvedAttemptsUnknown(ctx, record); err != nil {
		return agentledger.AttemptHandle{}, err
	}
	return a.retry(ctx, record, payload)
}

func (a *Adapter) retry(
	ctx context.Context,
	record executionRecord,
	payload map[string]any,
) (agentledger.AttemptHandle, error) {
	attemptNo := 1
	for _, attempt := range record.Attempts {
		if attempt.Attempt.AttemptNo >= attemptNo {
			attemptNo = attempt.Attempt.AttemptNo + 1
		}
	}
	handle, err := a.runtimeRecorder.Retry(ctx, record.Action.ID, attemptNo, payload)
	if err != nil {
		return agentledger.AttemptHandle{}, err
	}
	if err := a.rememberAttempt(ctx, handle, payload); err != nil {
		return agentledger.AttemptHandle{}, err
	}
	return handle, nil
}

func (a *Adapter) markUnresolvedAttemptsUnknown(ctx context.Context, record executionRecord) error {
	for _, attempt := range record.Attempts {
		if attempt.Requested == nil || attempt.Terminal != nil {
			continue
		}
		handle := handleFor(record.Action, attempt)
		terminal, err := a.runtimeRecorder.MarkAttemptOutcomeUnknown(
			ctx, handle, unresolvedAttemptReason, "",
		)
		if err != nil {
			return fmt.Errorf("mark attempt %s outcome unknown: %w", attempt.Attempt.ID, err)
		}
		a.journal.markAttemptTerminal(attempt.Attempt.ID, terminal)
	}
	return nil
}

func (a *Adapter) rememberAttempt(
	ctx context.Context,
	handle agentledger.AttemptHandle,
	payload map[string]any,
) error {
	action, found, err := a.runtimeRecorder.Store().GetAction(ctx, handle.ActionID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("recorded action %s was not found", handle.ActionID)
	}
	attempt, found, err := a.runtimeRecorder.Store().GetAttempt(ctx, handle.AttemptID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("recorded attempt %s was not found", handle.AttemptID)
	}
	a.mu.Lock()
	journal := a.journal
	a.mu.Unlock()
	if journal == nil {
		return errors.New("agentgo recovery journal is not loaded")
	}
	journal.addAttempt(action, attempt, handle.RequestedEventID, payload)
	return nil
}

func (a *Adapter) execution(actionType, actionKey string) (executionRecord, bool) {
	a.mu.Lock()
	journal := a.journal
	a.mu.Unlock()
	if journal == nil {
		return executionRecord{}, false
	}
	return journal.execution(actionType, actionKey)
}

func (a *Adapter) turnForExecution(execution agentgo.Execution) (turnBinding, error) {
	if execution.ID == "" {
		return turnBinding{}, errors.New("agentgo execution is missing an ID")
	}
	turn, err := a.turn()
	if err != nil {
		return turnBinding{}, err
	}
	if execution.TurnIndex > 0 && execution.TurnIndex != turn.Index {
		return turnBinding{}, fmt.Errorf(
			"agentgo execution turn %d does not match active turn %d",
			execution.TurnIndex, turn.Index,
		)
	}
	return turn, nil
}

func (a *Adapter) consumeRetryDecision(actionID string) (ToolRetryDecision, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	decision, found := a.retryDecisions[actionID]
	if !found || !decision.Approved {
		return ToolRetryDecision{}, fmt.Errorf("tool action %s has no approved recovery decision", actionID)
	}
	if decision.RecoveryDecisionID == "" {
		return ToolRetryDecision{}, fmt.Errorf("tool action %s recovery decision has no ID", actionID)
	}
	delete(a.retryDecisions, actionID)
	return decision, nil
}

func validateToolSemantics(semantics ToolSemantics) (ToolSemantics, error) {
	semantics.Effect = agentledger.NormalizeEffect(semantics.Effect)
	if semantics.Effect.Idempotency == agentledger.IdempotencyKeyed && semantics.IdempotencyKey == "" {
		return ToolSemantics{}, errors.New("keyed tool effect requires an effective idempotency key")
	}
	return semantics, nil
}

func validateRecoveredIdempotencyKey(record executionRecord, current string) error {
	for _, attempt := range record.Attempts {
		if attempt.Requested == nil {
			continue
		}
		original := stringValue(attempt.Requested.Payload["idempotency_key"])
		if original != current {
			return fmt.Errorf(
				"tool action %s changed idempotency key from %q to %q",
				record.Action.ID, original, current,
			)
		}
		return nil
	}
	return nil
}

func validateRequestFingerprint(record executionRecord, payload map[string]any) error {
	current := stringValue(payload["request_fingerprint"])
	for _, attempt := range record.Attempts {
		if attempt.Requested == nil {
			continue
		}
		original := stringValue(attempt.Requested.Payload["request_fingerprint"])
		if original == "" || current == "" {
			return fmt.Errorf("action %s has no replayable request fingerprint", record.Action.ID)
		}
		if original != current {
			return fmt.Errorf("action %s input diverged from its recorded execution", record.Action.ID)
		}
		return nil
	}
	// Structural rows may outlive a failed attempt.requested append. No
	// external execution crossed that strict prewrite boundary, so a new
	// Attempt may safely reuse the Action without a recovery decision.
	return nil
}

func hasRequestedAttempt(record executionRecord) bool {
	for _, attempt := range record.Attempts {
		if attempt.Requested != nil {
			return true
		}
	}
	return false
}

func requestFingerprint(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func handleFor(action agentledger.Action, attempt journalAttempt) agentledger.AttemptHandle {
	handle := agentledger.AttemptHandle{
		ActionType: action.Type,
		TurnID:     action.TurnID,
		ActionID:   action.ID,
		AttemptID:  attempt.Attempt.ID,
		AttemptNo:  attempt.Attempt.AttemptNo,
	}
	if attempt.Requested != nil {
		handle.RequestedEventID = attempt.Requested.ID
	}
	return handle
}

func executionPayload(execution agentgo.Execution) map[string]any {
	payload := map[string]any{
		"execution_id":      execution.ID,
		"execution_kind":    string(execution.Kind),
		"execution_attempt": execution.Attempt,
		"turn_index":        execution.TurnIndex,
	}
	if execution.ParentID != "" {
		payload["parent_execution_id"] = execution.ParentID
	}
	return payload
}
