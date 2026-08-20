package agentledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"sort"
	"sync"
	"time"
)

var (
	ErrLaneConflict                   = errors.New("lane sequence conflict")
	ErrCheckpointConflict             = errors.New("checkpoint revision conflict")
	ErrIdempotencyViolation           = errors.New("append id reused with different content")
	ErrCheckpointIdempotencyViolation = errors.New("checkpoint id reused with different content")
	ErrDuplicateEvent                 = errors.New("duplicate event id")
	ErrEntityConflict                 = errors.New("entity conflict")
	ErrEntityNotFound                 = errors.New("entity not found")
	ErrSubjectMismatch                = errors.New("event subject does not belong to lane")
)

type MemoryEventStore struct {
	mu                sync.Mutex
	actors            map[string]Actor
	lanes             map[string]Lane
	laneNames         map[string]string
	turns             map[string]Turn
	actions           map[string]Action
	attempts          map[string]Attempt
	attemptNumbers    map[string]struct{}
	events            map[string]StoredEvent
	laneEvents        map[string][]StoredEvent
	appends           map[string]AppendReceipt
	checkpoints       map[string]Checkpoint
	latestCheckpoints map[string]string
}

func NewMemoryEventStore() *MemoryEventStore {
	return &MemoryEventStore{
		actors: make(map[string]Actor), lanes: make(map[string]Lane), laneNames: make(map[string]string),
		turns: make(map[string]Turn), actions: make(map[string]Action),
		attempts: make(map[string]Attempt), attemptNumbers: make(map[string]struct{}),
		events: make(map[string]StoredEvent), laneEvents: make(map[string][]StoredEvent),
		appends:     make(map[string]AppendReceipt),
		checkpoints: make(map[string]Checkpoint), latestCheckpoints: make(map[string]string),
	}
}

func (s *MemoryEventStore) CreateActor(ctx context.Context, actor Actor) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.actors[actor.ID]; ok {
		return fmt.Errorf("%w: actor %s", ErrEntityConflict, actor.ID)
	}
	s.actors[actor.ID] = actor
	return nil
}

func (s *MemoryEventStore) GetActor(ctx context.Context, id string) (Actor, bool, error) {
	if err := ctx.Err(); err != nil {
		return Actor{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.actors[id]
	return value, ok, nil
}

func (s *MemoryEventStore) CreateLane(ctx context.Context, lane Lane) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if lane.LastSeq != 0 {
		return errors.New("new lane must have last_seq 0")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	nameKey := laneNameKey(lane.SessionID, lane.RunID, lane.Name)
	if _, ok := s.lanes[lane.ID]; ok {
		return fmt.Errorf("%w: lane %s", ErrEntityConflict, lane.ID)
	}
	if _, ok := s.laneNames[nameKey]; ok {
		return fmt.Errorf("%w: lane %s", ErrEntityConflict, lane.ID)
	}
	if lane.ParentLaneID != "" {
		parent, ok := s.lanes[lane.ParentLaneID]
		if !ok {
			return fmt.Errorf("%w: parent lane %s", ErrEntityNotFound, lane.ParentLaneID)
		}
		if parent.SessionID != lane.SessionID {
			return errors.New("parent lane must belong to same session")
		}
	}
	s.lanes[lane.ID] = lane
	s.laneNames[nameKey] = lane.ID
	return nil
}

func (s *MemoryEventStore) GetLane(ctx context.Context, id string) (Lane, bool, error) {
	if err := ctx.Err(); err != nil {
		return Lane{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.lanes[id]
	return value, ok, nil
}

func (s *MemoryEventStore) FindLane(ctx context.Context, sessionID, runID, name string) (Lane, bool, error) {
	if err := ctx.Err(); err != nil {
		return Lane{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.laneNames[laneNameKey(sessionID, runID, name)]
	if !ok {
		return Lane{}, false, nil
	}
	return s.lanes[id], true, nil
}

func (s *MemoryEventStore) CreateTurn(ctx context.Context, turn Turn) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.turns[turn.ID]; ok {
		return fmt.Errorf("%w: turn %s", ErrEntityConflict, turn.ID)
	}
	if _, ok := s.lanes[turn.LaneID]; !ok {
		return fmt.Errorf("%w: lane %s", ErrEntityNotFound, turn.LaneID)
	}
	s.turns[turn.ID] = turn
	return nil
}

func (s *MemoryEventStore) GetTurn(ctx context.Context, id string) (Turn, bool, error) {
	if err := ctx.Err(); err != nil {
		return Turn{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.turns[id]
	return value, ok, nil
}

func (s *MemoryEventStore) CreateAction(ctx context.Context, action Action) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.actions[action.ID]; ok {
		return fmt.Errorf("%w: action %s", ErrEntityConflict, action.ID)
	}
	if _, ok := s.turns[action.TurnID]; !ok {
		return fmt.Errorf("%w: turn %s", ErrEntityNotFound, action.TurnID)
	}
	if action.ParentActionID != "" {
		parent, ok := s.actions[action.ParentActionID]
		if !ok {
			return fmt.Errorf("%w: parent action %s", ErrEntityNotFound, action.ParentActionID)
		}
		if parent.TurnID != action.TurnID {
			return errors.New("parent action must belong to same turn")
		}
	}
	s.actions[action.ID] = action
	return nil
}

func (s *MemoryEventStore) GetAction(ctx context.Context, id string) (Action, bool, error) {
	if err := ctx.Err(); err != nil {
		return Action{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.actions[id]
	return value, ok, nil
}

func (s *MemoryEventStore) CreateAttempt(ctx context.Context, attempt Attempt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if attempt.AttemptNo < 1 {
		return errors.New("attempt_no must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	numberKey := attemptNumberKey(attempt.ActionID, attempt.AttemptNo)
	if _, ok := s.attempts[attempt.ID]; ok {
		return fmt.Errorf("%w: attempt %s", ErrEntityConflict, attempt.ID)
	}
	if _, ok := s.attemptNumbers[numberKey]; ok {
		return fmt.Errorf("%w: attempt %s", ErrEntityConflict, attempt.ID)
	}
	if _, ok := s.actions[attempt.ActionID]; !ok {
		return fmt.Errorf("%w: action %s", ErrEntityNotFound, attempt.ActionID)
	}
	s.attempts[attempt.ID] = attempt
	s.attemptNumbers[numberKey] = struct{}{}
	return nil
}

func (s *MemoryEventStore) GetAttempt(ctx context.Context, id string) (Attempt, bool, error) {
	if err := ctx.Err(); err != nil {
		return Attempt{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.attempts[id]
	return value, ok, nil
}

func (s *MemoryEventStore) SaveCheckpoint(ctx context.Context, expectedRevision int64, proposed ProposedCheckpoint) (Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, err
	}
	if expectedRevision < 0 {
		return Checkpoint{}, errors.New("expected_revision must be non-negative")
	}
	if err := validateCheckpoint(proposed); err != nil {
		return Checkpoint{}, err
	}
	if proposed.Extensions == nil {
		proposed.Extensions = map[string]any{}
	}
	snapshot, err := cloneOne(proposed)
	if err != nil {
		return Checkpoint{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if previous, ok := s.checkpoints[proposed.ID]; ok {
		encodedPrevious, _ := json.Marshal(previous.ProposedCheckpoint)
		encodedProposed, _ := json.Marshal(snapshot)
		if string(encodedPrevious) != string(encodedProposed) {
			return Checkpoint{}, ErrCheckpointIdempotencyViolation
		}
		return cloneOne(previous)
	}
	if _, ok := s.actors[proposed.ActorID]; !ok {
		return Checkpoint{}, fmt.Errorf("%w: actor %s", ErrEntityNotFound, proposed.ActorID)
	}
	actualRevision := int64(0)
	if id := s.latestCheckpoints[proposed.CheckpointKey]; id != "" {
		actualRevision = s.checkpoints[id].Revision
	}
	if actualRevision != expectedRevision {
		return Checkpoint{}, fmt.Errorf("%w: expected %d, actual %d", ErrCheckpointConflict, expectedRevision, actualRevision)
	}
	if proposed.Anchor != nil {
		event, ok := s.events[proposed.Anchor.LastAppliedEventID]
		if !ok || event.LaneID != proposed.Anchor.LaneID || event.Seq != proposed.Anchor.LastAppliedSeq {
			return Checkpoint{}, errors.New("checkpoint anchor must identify an existing lane event")
		}
	}
	stored := Checkpoint{ProposedCheckpoint: snapshot, Revision: actualRevision + 1, CreatedAt: now()}
	s.checkpoints[stored.ID] = stored
	s.latestCheckpoints[stored.CheckpointKey] = stored.ID
	return cloneOne(stored)
}

func (s *MemoryEventStore) GetCheckpoint(ctx context.Context, id string) (Checkpoint, bool, error) {
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.checkpoints[id]
	if !ok {
		return Checkpoint{}, false, nil
	}
	cloned, err := cloneOne(value)
	return cloned, true, err
}

func (s *MemoryEventStore) LoadLatestCheckpoint(ctx context.Context, checkpointKey string) (Checkpoint, bool, error) {
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.latestCheckpoints[checkpointKey]
	if id == "" {
		return Checkpoint{}, false, nil
	}
	cloned, err := cloneOne(s.checkpoints[id])
	return cloned, true, err
}

func (s *MemoryEventStore) Append(ctx context.Context, laneID string, expectedLastSeq int64, appendID string, events ...ProposedEvent) (AppendReceipt, error) {
	if err := ctx.Err(); err != nil {
		return AppendReceipt{}, err
	}
	if expectedLastSeq < 0 {
		return AppendReceipt{}, errors.New("expected_last_seq must be non-negative")
	}
	if appendID == "" || len(events) == 0 {
		return AppendReceipt{}, errors.New("append requires an id and at least one event")
	}
	batch, err := clone(events)
	if err != nil {
		return AppendReceipt{}, fmt.Errorf("snapshot append batch: %w", err)
	}
	seen := make(map[string]struct{}, len(batch))
	for _, event := range batch {
		if event.LaneID != laneID {
			return AppendReceipt{}, errors.New("all events must belong to target lane")
		}
		if _, ok := seen[event.ID]; ok {
			return AppendReceipt{}, fmt.Errorf("%w: %s", ErrDuplicateEvent, event.ID)
		}
		seen[event.ID] = struct{}{}
	}
	digest, err := CanonicalAppendDigest(batch)
	if err != nil {
		return AppendReceipt{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if previous, ok := s.appends[appendID]; ok {
		if previous.LaneID != laneID || previous.Digest != digest {
			return AppendReceipt{}, ErrIdempotencyViolation
		}
		return cloneOne(previous)
	}
	lane, ok := s.lanes[laneID]
	if !ok {
		return AppendReceipt{}, fmt.Errorf("%w: lane %s", ErrEntityNotFound, laneID)
	}
	if lane.LastSeq != expectedLastSeq {
		return AppendReceipt{}, fmt.Errorf("%w: expected %d, actual %d", ErrLaneConflict, expectedLastSeq, lane.LastSeq)
	}
	for _, event := range batch {
		if _, exists := s.events[event.ID]; exists {
			return AppendReceipt{}, fmt.Errorf("%w: %s", ErrDuplicateEvent, event.ID)
		}
		if _, exists := s.actors[event.ActorID]; !exists {
			return AppendReceipt{}, fmt.Errorf("%w: actor %s", ErrEntityNotFound, event.ActorID)
		}
		if !s.validSubject(lane, event) {
			return AppendReceipt{}, fmt.Errorf("%w: event %s", ErrSubjectMismatch, event.ID)
		}
	}
	prior := make(map[string]struct{}, len(s.events)+len(batch))
	for id := range s.events {
		prior[id] = struct{}{}
	}
	for _, event := range batch {
		if event.CausationID != "" {
			if _, exists := prior[event.CausationID]; !exists {
				return AppendReceipt{}, fmt.Errorf("%w: causation event %s", ErrEntityNotFound, event.CausationID)
			}
			if caused, exists := s.events[event.CausationID]; exists {
				causedLane := s.lanes[caused.LaneID]
				if causedLane.SessionID != lane.SessionID {
					return AppendReceipt{}, fmt.Errorf("%w: causation event %s", ErrSubjectMismatch, event.ID)
				}
			}
		}
		prior[event.ID] = struct{}{}
	}
	committedAt := now()
	stored := make([]StoredEvent, 0, len(batch))
	for index, event := range batch {
		stored = append(stored, StoredEvent{ProposedEvent: event, Seq: lane.LastSeq + int64(index) + 1, CommittedAt: committedAt})
	}
	receipt := AppendReceipt{ID: appendID, LaneID: laneID, Digest: digest, FirstSeq: stored[0].Seq, LastSeq: stored[len(stored)-1].Seq, CommittedAt: committedAt}
	for _, event := range stored {
		receipt.EventIDs = append(receipt.EventIDs, event.ID)
		s.events[event.ID] = event
	}
	lane.LastSeq = receipt.LastSeq
	s.lanes[laneID] = lane
	s.laneEvents[laneID] = append(s.laneEvents[laneID], stored...)
	s.appends[appendID] = receipt
	return cloneOne(receipt)
}

func (s *MemoryEventStore) LoadLane(ctx context.Context, laneID string, afterSeq int64) iter.Seq2[StoredEvent, error] {
	return func(yield func(StoredEvent, error) bool) {
		if afterSeq < 0 {
			yield(StoredEvent{}, errors.New("after_seq must be non-negative"))
			return
		}
		s.mu.Lock()
		_, exists := s.lanes[laneID]
		snapshot := append([]StoredEvent(nil), s.laneEvents[laneID]...)
		s.mu.Unlock()
		if !exists {
			yield(StoredEvent{}, fmt.Errorf("%w: lane %s", ErrEntityNotFound, laneID))
			return
		}
		events, err := clone(snapshot)
		if err != nil {
			yield(StoredEvent{}, err)
			return
		}
		for _, event := range events {
			if err := ctx.Err(); err != nil {
				yield(StoredEvent{}, err)
				return
			}
			if event.Seq > afterSeq && !yield(event, nil) {
				return
			}
		}
	}
}

func (s *MemoryEventStore) LoadSession(ctx context.Context, sessionID string) (SessionView, error) {
	if err := ctx.Err(); err != nil {
		return SessionView{}, err
	}
	s.mu.Lock()
	view := SessionView{SessionID: sessionID}
	laneIDs := map[string]struct{}{}
	for _, lane := range s.lanes {
		if lane.SessionID == sessionID {
			view.Lanes = append(view.Lanes, lane)
			laneIDs[lane.ID] = struct{}{}
		}
	}
	turnIDs := map[string]struct{}{}
	for _, turn := range s.turns {
		if _, ok := laneIDs[turn.LaneID]; ok {
			view.Turns = append(view.Turns, turn)
			turnIDs[turn.ID] = struct{}{}
		}
	}
	actionIDs := map[string]struct{}{}
	for _, action := range s.actions {
		if _, ok := turnIDs[action.TurnID]; ok {
			view.Actions = append(view.Actions, action)
			actionIDs[action.ID] = struct{}{}
		}
	}
	for _, attempt := range s.attempts {
		if _, ok := actionIDs[attempt.ActionID]; ok {
			view.Attempts = append(view.Attempts, attempt)
		}
	}
	for _, event := range s.events {
		if _, ok := laneIDs[event.LaneID]; ok {
			view.Events = append(view.Events, event)
		}
	}
	actorIDs := map[string]struct{}{}
	for _, event := range view.Events {
		actorIDs[event.ActorID] = struct{}{}
	}
	for _, actor := range s.actors {
		if _, ok := actorIDs[actor.ID]; ok {
			view.Actors = append(view.Actors, actor)
		}
	}
	s.mu.Unlock()
	sort.Slice(view.Events, func(i, j int) bool { return observedBefore(view.Events[i], view.Events[j]) })
	return cloneOne(view)
}

func observedBefore(left, right StoredEvent) bool {
	leftTime, leftErr := time.Parse(time.RFC3339Nano, left.CommittedAt)
	rightTime, rightErr := time.Parse(time.RFC3339Nano, right.CommittedAt)
	if leftErr == nil && rightErr == nil && !leftTime.Equal(rightTime) {
		return leftTime.Before(rightTime)
	}
	if left.CommittedAt != right.CommittedAt {
		return left.CommittedAt < right.CommittedAt
	}
	if left.LaneID != right.LaneID {
		return left.LaneID < right.LaneID
	}
	if left.Seq != right.Seq {
		return left.Seq < right.Seq
	}
	return left.ID < right.ID
}

func (s *MemoryEventStore) validSubject(lane Lane, event ProposedEvent) bool {
	switch event.SubjectKind() {
	case "session":
		return event.SubjectID == lane.SessionID
	case "run":
		return event.SubjectID == lane.RunID
	case "lane":
		return event.SubjectID == lane.ID
	case "turn":
		turn, ok := s.turns[event.SubjectID]
		return ok && turn.LaneID == lane.ID
	case "action":
		action, ok := s.actions[event.SubjectID]
		if !ok {
			return false
		}
		turn, ok := s.turns[action.TurnID]
		return ok && turn.LaneID == lane.ID
	case "attempt":
		attempt, ok := s.attempts[event.SubjectID]
		if !ok {
			return false
		}
		action, ok := s.actions[attempt.ActionID]
		if !ok {
			return false
		}
		turn, ok := s.turns[action.TurnID]
		return ok && turn.LaneID == lane.ID
	default:
		return false
	}
}

func laneNameKey(sessionID, runID, name string) string {
	return sessionID + "\x00" + runID + "\x00" + name
}
func attemptNumberKey(actionID string, attemptNo int) string {
	return fmt.Sprintf("%s\x00%d", actionID, attemptNo)
}

func clone[T any](value T) (T, error) {
	var result T
	encoded, err := json.Marshal(value)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(encoded, &result); err != nil {
		return result, err
	}
	return result, nil
}

func cloneOne[T any](value T) (T, error) { return clone(value) }

func validateCheckpoint(value ProposedCheckpoint) error {
	if value.SchemaVersion != "1.0" || value.ID == "" || value.CheckpointKey == "" || value.ActorID == "" || value.Format == "" {
		return errors.New("checkpoint requires schema version, id, key, actor, and format")
	}
	if (value.State == nil) == (value.ArtifactRef == nil) {
		return errors.New("exactly one of state and artifact_ref must be set")
	}
	if value.Anchor != nil && (value.Anchor.LaneID == "" || value.Anchor.LastAppliedSeq < 1 || value.Anchor.LastAppliedEventID == "") {
		return errors.New("checkpoint anchor requires lane, positive seq, and event")
	}
	return nil
}

var _ EventStore = (*MemoryEventStore)(nil)
var _ CheckpointStore = (*MemoryEventStore)(nil)
