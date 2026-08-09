package agentledger

import (
	"context"
	"fmt"
	"sync"
)

type RecorderOptions struct {
	Store           EventStore
	SessionID       string
	RunID           string
	StreamID        string
	Actor           Actor
	ExpectedVersion *int64
}

type SessionRecorder struct {
	store           EventStore
	stream          EventStream
	runID           string
	actor           Actor
	expectedVersion int64
	mu              sync.Mutex
}

func NewSessionRecorder(options RecorderOptions) *SessionRecorder {
	streamID := options.StreamID
	if streamID == "" {
		streamID = options.RunID
	}
	expectedVersion := int64(-1)
	if options.ExpectedVersion != nil {
		expectedVersion = *options.ExpectedVersion
	}
	return &SessionRecorder{
		store:           options.Store,
		stream:          EventStream{SessionID: options.SessionID, StreamID: streamID},
		runID:           options.RunID,
		actor:           options.Actor,
		expectedVersion: expectedVersion,
	}
}

func ResumeRecorder(ctx context.Context, options RecorderOptions) (*SessionRecorder, error) {
	recorder := NewSessionRecorder(options)
	expectedVersion := int64(-1)
	for event, err := range options.Store.Load(ctx, recorder.stream, -1) {
		if err != nil {
			return nil, fmt.Errorf("load recorder stream: %w", err)
		}
		expectedVersion = event.StreamVersion
	}
	recorder.expectedVersion = expectedVersion
	return recorder, nil
}

func (r *SessionRecorder) Stream() EventStream { return r.stream }
func (r *SessionRecorder) RunID() string       { return r.runID }
func (r *SessionRecorder) Store() EventStore   { return r.store }

func (r *SessionRecorder) Record(ctx context.Context, eventType string, payload map[string]any, stepID, attemptID string) (StoredEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	event := NewEvent(eventType, r.stream.SessionID, r.runID, r.actor)
	event.Payload = payload
	event.StepID = stepID
	event.AttemptID = attemptID
	receipt, err := r.store.Append(ctx, r.stream, r.expectedVersion, NewID(), event)
	if err != nil {
		return StoredEvent{}, err
	}
	r.expectedVersion = receipt.LastVersion
	return StoredEvent{
		ProposedEvent: event,
		StreamID:      r.stream.StreamID,
		StreamVersion: receipt.FirstVersion,
		CommitCursor:  receipt.FirstCursor,
		CommittedAt:   receipt.CommittedAt,
	}, nil
}

func (r *SessionRecorder) BeforeModelCall(ctx context.Context, stepID string, payload map[string]any) (AttemptHandle, error) {
	return r.beforeCall(ctx, "model", stepID, payload)
}

func (r *SessionRecorder) BeforeToolCall(ctx context.Context, stepID string, payload map[string]any) (AttemptHandle, error) {
	return r.beforeCall(ctx, "tool", stepID, payload)
}

func (r *SessionRecorder) ModelCompleted(ctx context.Context, attempt AttemptHandle, payload map[string]any) error {
	_, err := r.Record(ctx, "model.completed", payload, attempt.StepID, attempt.AttemptID)
	return err
}

func (r *SessionRecorder) ModelFailed(ctx context.Context, attempt AttemptHandle, failure error) error {
	_, err := r.Record(ctx, "model.failed", errorPayload(failure), attempt.StepID, attempt.AttemptID)
	return err
}

func (r *SessionRecorder) ToolCompleted(ctx context.Context, attempt AttemptHandle, payload map[string]any) error {
	_, err := r.Record(ctx, "tool.completed", payload, attempt.StepID, attempt.AttemptID)
	return err
}

func (r *SessionRecorder) ToolFailed(ctx context.Context, attempt AttemptHandle, failure error) error {
	_, err := r.Record(ctx, "tool.failed", errorPayload(failure), attempt.StepID, attempt.AttemptID)
	return err
}

func (r *SessionRecorder) beforeCall(ctx context.Context, kind, stepID string, payload map[string]any) (AttemptHandle, error) {
	attemptID := NewID()
	event, err := r.Record(ctx, kind+".requested", payload, stepID, attemptID)
	if err != nil {
		return AttemptHandle{}, err
	}
	return AttemptHandle{Kind: kind, StepID: stepID, AttemptID: attemptID, RequestedEventID: event.EventID}, nil
}

func errorPayload(err error) map[string]any {
	if err == nil {
		return map[string]any{"error": "unknown error"}
	}
	return map[string]any{"error": err.Error()}
}
