package agentledger

import "time"

type Actor struct {
	Type      string `json:"type"`
	ID        string `json:"id"`
	Framework string `json:"framework,omitempty"`
}

type EventStream struct {
	SessionID string `json:"session_id"`
	StreamID  string `json:"stream_id"`
}

type CausalParent struct {
	RunID           string
	CausedByEventID string
}

type ProposedEvent struct {
	SchemaVersion   string         `json:"schema_version"`
	EventID         string         `json:"event_id"`
	EventType       string         `json:"event_type"`
	SessionID       string         `json:"session_id"`
	RunID           string         `json:"run_id"`
	Actor           Actor          `json:"actor"`
	OccurredAt      string         `json:"occurred_at"`
	StepID          string         `json:"step_id,omitempty"`
	AttemptID       string         `json:"attempt_id,omitempty"`
	ParentRunID     string         `json:"parent_run_id,omitempty"`
	CausedByEventID string         `json:"caused_by_event_id,omitempty"`
	Payload         map[string]any `json:"payload"`
	Extensions      map[string]any `json:"extensions"`
}

type StoredEvent struct {
	ProposedEvent
	StreamID      string `json:"stream_id"`
	StreamVersion int64  `json:"stream_version"`
	CommitCursor  string `json:"commit_cursor"`
	CommittedAt   string `json:"committed_at"`
}

type CommitReceipt struct {
	Stream       EventStream `json:"stream"`
	AppendID     string      `json:"append_id"`
	Digest       string      `json:"digest"`
	FirstVersion int64       `json:"first_version"`
	LastVersion  int64       `json:"last_version"`
	FirstCursor  string      `json:"first_cursor"`
	LastCursor   string      `json:"last_cursor"`
	EventIDs     []string    `json:"event_ids"`
	CommittedAt  string      `json:"committed_at"`
}

type AdapterCapabilities struct {
	ModelPrewrite        string `json:"model_prewrite"`
	ToolPrewrite         string `json:"tool_prewrite"`
	OutcomeGate          string `json:"outcome_gate"`
	Recovery             string `json:"recovery"`
	PreservesNativeState bool   `json:"preserves_native_state"`
}

type AdapterDescriptor struct {
	SchemaVersion    string              `json:"schema_version"`
	AdapterID        string              `json:"adapter_id"`
	AdapterVersion   string              `json:"adapter_version"`
	Framework        string              `json:"framework"`
	FrameworkVersion string              `json:"framework_version"`
	Capabilities     AdapterCapabilities `json:"capabilities"`
}

type AttemptHandle struct {
	Kind             string
	StepID           string
	AttemptID        string
	RequestedEventID string
}

func NewEvent(eventType, sessionID, runID string, actor Actor) ProposedEvent {
	return ProposedEvent{
		SchemaVersion: "1.0",
		EventID:       NewID(),
		EventType:     eventType,
		SessionID:     sessionID,
		RunID:         runID,
		Actor:         actor,
		OccurredAt:    time.Now().UTC().Format(time.RFC3339Nano),
		Payload:       map[string]any{},
		Extensions:    map[string]any{},
	}
}
