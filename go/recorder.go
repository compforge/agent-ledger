package agentledger

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type RecorderOptions struct {
	Store          EventStore
	SessionID      string
	RunID          string
	LaneID         string
	LaneName       string
	Actor          Actor
	ParentLaneID   string
	ParentRunID    string
	RunCausationID string
}

type LaneRecorder struct {
	store           EventStore
	lane            Lane
	actor           Actor
	parentRunID     string
	runCausationID  string
	expectedLastSeq int64
	mu              sync.Mutex
}

type CheckpointLink struct {
	CheckpointID   string
	Profile        string
	ProfileVersion string
	Metadata       map[string]any
}

func OpenRecorder(ctx context.Context, options RecorderOptions) (*LaneRecorder, error) {
	if options.Store == nil {
		return nil, errors.New("recorder requires an event store")
	}
	if options.LaneName == "" {
		options.LaneName = "main"
	}
	storedActor, exists, err := options.Store.GetActor(ctx, options.Actor.ID)
	if err != nil {
		return nil, fmt.Errorf("get recorder actor: %w", err)
	}
	if !exists {
		if err := options.Store.CreateActor(ctx, options.Actor); err != nil {
			return nil, fmt.Errorf("create recorder actor: %w", err)
		}
	} else if storedActor.Type != options.Actor.Type || storedActor.Framework != options.Actor.Framework {
		return nil, fmt.Errorf("%w: actor %s", ErrEntityConflict, options.Actor.ID)
	}

	var lane Lane
	if options.LaneID != "" {
		lane, exists, err = options.Store.GetLane(ctx, options.LaneID)
		if err != nil {
			return nil, fmt.Errorf("get recorder lane: %w", err)
		}
		if !exists {
			lane = NewLane(options.SessionID, options.RunID, options.LaneName, options.ParentLaneID)
			lane.ID = options.LaneID
			if err := options.Store.CreateLane(ctx, lane); err != nil {
				return nil, fmt.Errorf("create recorder lane: %w", err)
			}
		}
	} else {
		lane, exists, err = options.Store.FindLane(ctx, options.SessionID, options.RunID, options.LaneName)
		if err != nil {
			return nil, fmt.Errorf("find recorder lane: %w", err)
		}
		if !exists {
			candidate := NewLane(options.SessionID, options.RunID, options.LaneName, options.ParentLaneID)
			if createErr := options.Store.CreateLane(ctx, candidate); createErr != nil {
				if !errors.Is(createErr, ErrEntityConflict) {
					return nil, fmt.Errorf("create recorder lane: %w", createErr)
				}
				lane, exists, err = options.Store.FindLane(ctx, options.SessionID, options.RunID, options.LaneName)
				if err != nil || !exists {
					return nil, fmt.Errorf("find concurrently created recorder lane: %w", err)
				}
			} else {
				lane = candidate
			}
		}
	}
	if lane.SessionID != options.SessionID || lane.RunID != options.RunID || lane.Name != options.LaneName {
		return nil, errors.New("lane identity does not match recorder options")
	}
	if options.ParentLaneID != "" && lane.ParentLaneID != options.ParentLaneID {
		return nil, errors.New("lane parent does not match recorder options")
	}
	return &LaneRecorder{
		store: options.Store, lane: lane, actor: options.Actor,
		parentRunID: options.ParentRunID, runCausationID: options.RunCausationID,
		expectedLastSeq: lane.LastSeq,
	}, nil
}

func (r *LaneRecorder) Store() EventStore { return r.store }
func (r *LaneRecorder) Lane() Lane        { return r.lane }
func (r *LaneRecorder) RunID() string     { return r.lane.RunID }
func (r *LaneRecorder) SessionID() string { return r.lane.SessionID }

// Append atomically records an ordered Event batch on the Recorder's Lane.
// The append ID is supplied by the caller so an uncertain write can be retried idempotently.
func (r *LaneRecorder) Append(ctx context.Context, appendID string, events ...ProposedEvent) (AppendReceipt, error) {
	_, receipt, err := r.appendEvents(ctx, appendID, events...)
	return receipt, err
}

func (r *LaneRecorder) Record(ctx context.Context, eventType, subjectID string, payload map[string]any, causationID string) (StoredEvent, error) {
	event := NewEvent(eventType, r.lane.ID, subjectID, r.actor)
	event.Payload = payloadOrEmpty(payload)
	event.CausationID = causationID
	return r.appendEvent(ctx, event)
}

func (r *LaneRecorder) StartRun(ctx context.Context, payload map[string]any) (StoredEvent, error) {
	payload = payloadOrEmpty(payload)
	if r.parentRunID != "" {
		payload["parent_run_id"] = r.parentRunID
	}
	return r.Record(ctx, EventTypeRunStarted, r.lane.RunID, payload, r.runCausationID)
}

func (r *LaneRecorder) CompleteRun(ctx context.Context, payload map[string]any) (StoredEvent, error) {
	return r.Record(ctx, EventTypeRunCompleted, r.lane.RunID, payload, "")
}

func (r *LaneRecorder) FailRun(ctx context.Context, failure error) (StoredEvent, error) {
	return r.Record(ctx, EventTypeRunFailed, r.lane.RunID, errorPayload(failure), "")
}

func (r *LaneRecorder) StartTurn(ctx context.Context, payload map[string]any) (Turn, error) {
	turn := NewTurn(r.lane.ID)
	if err := r.store.CreateTurn(ctx, turn); err != nil {
		return Turn{}, err
	}
	if _, err := r.Record(ctx, EventTypeTurnStarted, turn.ID, payload, ""); err != nil {
		return Turn{}, err
	}
	return turn, nil
}

func (r *LaneRecorder) CompleteTurn(ctx context.Context, turnID string, payload map[string]any) (StoredEvent, error) {
	return r.Record(ctx, EventTypeTurnCompleted, turnID, payload, "")
}

func (r *LaneRecorder) FailTurn(ctx context.Context, turnID string, failure error) (StoredEvent, error) {
	return r.Record(ctx, EventTypeTurnFailed, turnID, errorPayload(failure), "")
}

func (r *LaneRecorder) BeforeModelCall(ctx context.Context, turnID string, payload map[string]any) (AttemptHandle, error) {
	return r.beforeCall(ctx, ActionTypeModelCall, turnID, payload, Action{}, 1)
}

func (r *LaneRecorder) BeforeToolCall(ctx context.Context, turnID string, payload map[string]any) (AttemptHandle, error) {
	return r.beforeCall(ctx, ActionTypeToolCall, turnID, payload, Action{}, 1)
}

func (r *LaneRecorder) Retry(ctx context.Context, actionID string, attemptNo int, payload map[string]any) (AttemptHandle, error) {
	action, exists, err := r.store.GetAction(ctx, actionID)
	if err != nil {
		return AttemptHandle{}, err
	}
	if !exists {
		return AttemptHandle{}, fmt.Errorf("%w: action %s", ErrEntityNotFound, actionID)
	}
	if action.Type != ActionTypeModelCall && action.Type != ActionTypeToolCall {
		return AttemptHandle{}, fmt.Errorf("action %s is not retryable", actionID)
	}
	return r.beforeCall(ctx, action.Type, action.TurnID, payload, action, attemptNo)
}

func (r *LaneRecorder) ModelCompleted(ctx context.Context, attempt AttemptHandle, payload map[string]any) error {
	if attempt.ActionType != ActionTypeModelCall {
		return errors.New("attempt is not a model_call")
	}
	_, err := r.attemptCompleted(ctx, attempt, payload)
	return err
}

func (r *LaneRecorder) ModelFailed(ctx context.Context, attempt AttemptHandle, failure error) error {
	if attempt.ActionType != ActionTypeModelCall {
		return errors.New("attempt is not a model_call")
	}
	_, err := r.attemptFailed(ctx, attempt, failure)
	return err
}

func (r *LaneRecorder) ToolCompleted(ctx context.Context, attempt AttemptHandle, payload map[string]any) error {
	if attempt.ActionType != ActionTypeToolCall {
		return errors.New("attempt is not a tool_call")
	}
	_, err := r.attemptCompleted(ctx, attempt, payload)
	return err
}

func (r *LaneRecorder) ToolFailed(ctx context.Context, attempt AttemptHandle, failure error) error {
	if attempt.ActionType != ActionTypeToolCall {
		return errors.New("attempt is not a tool_call")
	}
	_, err := r.attemptFailed(ctx, attempt, failure)
	return err
}

func (r *LaneRecorder) SaveSnapshot(ctx context.Context, profile, profileVersion string, snapshot map[string]any) (StoredEvent, error) {
	return r.Record(ctx, EventTypeLaneFrameworkSnapshotSaved, r.lane.ID, map[string]any{
		"profile": profile, "profile_version": profileVersion, "snapshot": snapshot,
	}, "")
}

func (r *LaneRecorder) LinkCheckpoint(ctx context.Context, link CheckpointLink) (StoredEvent, error) {
	return r.appendEvent(ctx, r.checkpointLinkEvent(link))
}

func (r *LaneRecorder) checkpointLinkEvent(link CheckpointLink) ProposedEvent {
	if link.ProfileVersion == "" {
		link.ProfileVersion = "1"
	}
	event := NewEvent(EventTypeLaneFrameworkCheckpointLinked, r.lane.ID, r.lane.ID, r.actor)
	event.Payload = map[string]any{
		"profile": link.Profile, "profile_version": link.ProfileVersion,
		"checkpoint_id": link.CheckpointID, "metadata": payloadOrEmpty(link.Metadata),
	}
	return event
}

func (r *LaneRecorder) Child(ctx context.Context, runID string, actor Actor, causationID string) (*LaneRecorder, error) {
	return OpenRecorder(ctx, RecorderOptions{
		Store: r.store, SessionID: r.lane.SessionID, RunID: runID, Actor: actor,
		ParentLaneID: r.lane.ID, ParentRunID: r.lane.RunID, RunCausationID: causationID,
	})
}

func (r *LaneRecorder) beforeCall(ctx context.Context, actionType, turnID string, payload map[string]any, action Action, attemptNo int) (AttemptHandle, error) {
	if action.ID == "" {
		action = NewAction(turnID, actionType, "")
		if err := r.store.CreateAction(ctx, action); err != nil {
			return AttemptHandle{}, err
		}
	}
	attempt := NewAttempt(action.ID, attemptNo)
	if err := r.store.CreateAttempt(ctx, attempt); err != nil {
		return AttemptHandle{}, err
	}
	requested, err := r.Record(ctx, EventTypeAttemptRequested, attempt.ID, payload, "")
	if err != nil {
		return AttemptHandle{}, err
	}
	return AttemptHandle{
		ActionType: actionType, TurnID: turnID, ActionID: action.ID,
		AttemptID: attempt.ID, AttemptNo: attemptNo, RequestedEventID: requested.ID,
	}, nil
}

func (r *LaneRecorder) attemptCompleted(ctx context.Context, attempt AttemptHandle, payload map[string]any) (StoredEvent, error) {
	return r.Record(ctx, EventTypeAttemptCompleted, attempt.AttemptID, payload, attempt.RequestedEventID)
}

func (r *LaneRecorder) attemptFailed(ctx context.Context, attempt AttemptHandle, failure error) (StoredEvent, error) {
	return r.Record(ctx, EventTypeAttemptFailed, attempt.AttemptID, errorPayload(failure), attempt.RequestedEventID)
}

func (r *LaneRecorder) appendEvent(ctx context.Context, event ProposedEvent) (StoredEvent, error) {
	events, _, err := r.appendEvents(ctx, NewID(), event)
	if err != nil {
		return StoredEvent{}, err
	}
	return events[0], nil
}

func (r *LaneRecorder) appendEvents(ctx context.Context, appendID string, events ...ProposedEvent) ([]StoredEvent, AppendReceipt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	receipt, err := r.store.Append(ctx, r.lane.ID, r.expectedLastSeq, appendID, events...)
	if err != nil {
		return nil, AppendReceipt{}, err
	}
	// An idempotent replay returns its original receipt, which may predate later appends.
	if receipt.LastSeq > r.expectedLastSeq {
		r.expectedLastSeq = receipt.LastSeq
		r.lane.LastSeq = receipt.LastSeq
	}
	stored := make([]StoredEvent, 0, len(events))
	for index, event := range events {
		stored = append(stored, StoredEvent{
			ProposedEvent: event,
			Seq:           receipt.FirstSeq + int64(index),
			CommittedAt:   receipt.CommittedAt,
		})
	}
	return stored, receipt, nil
}

func errorPayload(err error) map[string]any {
	if err == nil {
		return map[string]any{"error": "unknown error"}
	}
	return map[string]any{"error": err.Error()}
}

func payloadOrEmpty(payload map[string]any) map[string]any {
	if payload == nil {
		return map[string]any{}
	}
	return payload
}
