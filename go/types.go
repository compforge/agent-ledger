package agentledger

import (
	"strings"
	"time"
)

type Actor struct {
	Type      string `json:"type"`
	ID        string `json:"id"`
	Key       string `json:"key,omitempty"`
	Framework string `json:"framework,omitempty"`
	CreatedAt string `json:"created_at"`
}

type Lane struct {
	ID           string `json:"id"`
	SessionID    string `json:"session_id"`
	RunID        string `json:"run_id"`
	Name         string `json:"name"`
	ParentLaneID string `json:"parent_lane_id,omitempty"`
	LastSeq      int64  `json:"last_seq"`
	CreatedAt    string `json:"created_at"`
}

type Turn struct {
	ID        string `json:"id"`
	LaneID    string `json:"lane_id"`
	CreatedAt string `json:"created_at"`
}

type Action struct {
	ID             string `json:"id"`
	TurnID         string `json:"turn_id"`
	Type           string `json:"type"`
	ParentActionID string `json:"parent_action_id,omitempty"`
	CreatedAt      string `json:"created_at"`
}

type Attempt struct {
	ID        string `json:"id"`
	ActionID  string `json:"action_id"`
	AttemptNo int    `json:"attempt_no"`
	CreatedAt string `json:"created_at"`
}

type ArtifactRef struct {
	URI         string `json:"uri"`
	SHA256      string `json:"sha256"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type"`
}

type CheckpointAnchor struct {
	LaneID             string `json:"lane_id"`
	LastAppliedSeq     int64  `json:"last_applied_seq"`
	LastAppliedEventID string `json:"last_applied_event_id"`
}

type ProposedCheckpoint struct {
	SchemaVersion string            `json:"schema_version"`
	ID            string            `json:"id"`
	Key           string            `json:"key"`
	ActorID       string            `json:"actor_id"`
	Format        string            `json:"format"`
	State         map[string]any    `json:"state,omitempty"`
	ArtifactRef   *ArtifactRef      `json:"artifact_ref,omitempty"`
	Anchor        *CheckpointAnchor `json:"anchor,omitempty"`
	Extensions    map[string]any    `json:"extensions"`
}

type Checkpoint struct {
	ProposedCheckpoint
	Revision  int64  `json:"revision"`
	CreatedAt string `json:"created_at"`
}

type ProposedEvent struct {
	SchemaVersion string         `json:"schema_version"`
	ID            string         `json:"id"`
	LaneID        string         `json:"lane_id"`
	SubjectID     string         `json:"subject_id"`
	EventType     string         `json:"event_type"`
	ActorID       string         `json:"actor_id"`
	CausationID   string         `json:"causation_id,omitempty"`
	OccurredAt    string         `json:"occurred_at"`
	Payload       map[string]any `json:"payload"`
	Extensions    map[string]any `json:"extensions"`
}

func (e ProposedEvent) SubjectKind() string {
	kind, _, _ := strings.Cut(e.EventType, ".")
	return kind
}

type StoredEvent struct {
	ProposedEvent
	Seq         int64  `json:"seq"`
	CommittedAt string `json:"committed_at"`
}

type AppendReceipt struct {
	ID          string   `json:"id"`
	LaneID      string   `json:"lane_id"`
	Digest      string   `json:"digest"`
	FirstSeq    int64    `json:"first_seq"`
	LastSeq     int64    `json:"last_seq"`
	EventIDs    []string `json:"event_ids"`
	CommittedAt string   `json:"committed_at"`
}

type SessionView struct {
	SessionID string        `json:"session_id"`
	Actors    []Actor       `json:"actors"`
	Lanes     []Lane        `json:"lanes"`
	Turns     []Turn        `json:"turns"`
	Actions   []Action      `json:"actions"`
	Attempts  []Attempt     `json:"attempts"`
	Events    []StoredEvent `json:"events"`
}

type RunView struct {
	SessionID string        `json:"session_id"`
	RunID     string        `json:"run_id"`
	Actors    []Actor       `json:"actors"`
	Lanes     []Lane        `json:"lanes"`
	Turns     []Turn        `json:"turns"`
	Actions   []Action      `json:"actions"`
	Attempts  []Attempt     `json:"attempts"`
	Events    []StoredEvent `json:"events"`
}

func SelectRun(view SessionView, runID string) RunView {
	result := RunView{SessionID: view.SessionID, RunID: runID}
	laneIDs := make(map[string]struct{})
	for _, lane := range view.Lanes {
		if lane.RunID == runID {
			result.Lanes = append(result.Lanes, lane)
			laneIDs[lane.ID] = struct{}{}
		}
	}
	turnIDs := make(map[string]struct{})
	for _, turn := range view.Turns {
		if _, ok := laneIDs[turn.LaneID]; ok {
			result.Turns = append(result.Turns, turn)
			turnIDs[turn.ID] = struct{}{}
		}
	}
	actionIDs := make(map[string]struct{})
	for _, action := range view.Actions {
		if _, ok := turnIDs[action.TurnID]; ok {
			result.Actions = append(result.Actions, action)
			actionIDs[action.ID] = struct{}{}
		}
	}
	for _, attempt := range view.Attempts {
		if _, ok := actionIDs[attempt.ActionID]; ok {
			result.Attempts = append(result.Attempts, attempt)
		}
	}
	actorIDs := make(map[string]struct{})
	for _, event := range view.Events {
		if _, ok := laneIDs[event.LaneID]; ok {
			result.Events = append(result.Events, event)
			actorIDs[event.ActorID] = struct{}{}
		}
	}
	for _, actor := range view.Actors {
		if _, ok := actorIDs[actor.ID]; ok {
			result.Actors = append(result.Actors, actor)
		}
	}
	return result
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
	ActionType       string
	TurnID           string
	ActionID         string
	AttemptID        string
	AttemptNo        int
	RequestedEventID string
}

func NewLane(sessionID, runID, name, parentLaneID string) Lane {
	if name == "" {
		name = "main"
	}
	return Lane{ID: NewID(), SessionID: sessionID, RunID: runID, Name: name, ParentLaneID: parentLaneID, CreatedAt: now()}
}

func NewActor(actorType, framework string) Actor {
	return Actor{ID: NewID(), Type: actorType, Framework: framework, CreatedAt: now()}
}

func NewActorWithKey(key, actorType, framework string) Actor {
	return Actor{ID: NewID(), Key: key, Type: actorType, Framework: framework, CreatedAt: now()}
}

func NewTurn(laneID string) Turn {
	return Turn{ID: NewID(), LaneID: laneID, CreatedAt: now()}
}

func NewAction(turnID, actionType, parentActionID string) Action {
	return Action{ID: NewID(), TurnID: turnID, Type: actionType, ParentActionID: parentActionID, CreatedAt: now()}
}

func NewAttempt(actionID string, attemptNo int) Attempt {
	return Attempt{ID: NewID(), ActionID: actionID, AttemptNo: attemptNo, CreatedAt: now()}
}

func NewCheckpoint(key, actorID, format string, state map[string]any) ProposedCheckpoint {
	return ProposedCheckpoint{
		SchemaVersion: "1.0", ID: NewID(), Key: key, ActorID: actorID,
		Format: format, State: state, Extensions: map[string]any{},
	}
}

func NewEvent(eventType, laneID, subjectID string, actor Actor) ProposedEvent {
	return ProposedEvent{
		SchemaVersion: "1.0",
		ID:            NewID(), LaneID: laneID, SubjectID: subjectID, EventType: eventType,
		ActorID:    actor.ID,
		OccurredAt: now(), Payload: map[string]any{}, Extensions: map[string]any{},
	}
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }
