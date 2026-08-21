package boltstore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"sort"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	bolt "go.etcd.io/bbolt"
)

var (
	actorsBucket          = []byte("actors")
	actorKeysBucket       = []byte("actor_keys")
	lanesBucket           = []byte("lanes")
	laneNamesBucket       = []byte("lane_names")
	turnsBucket           = []byte("turns")
	actionsBucket         = []byte("actions")
	attemptsBucket        = []byte("attempts")
	attemptNumbersBucket  = []byte("attempt_numbers")
	eventsBucket          = []byte("events")
	appendsBucket         = []byte("appends")
	laneEventsBucket      = []byte("lane_events")
	checkpointsBucket     = []byte("checkpoints")
	checkpointHeadsBucket = []byte("checkpoint_heads")
)

type Store struct{ db *bolt.DB }

func Open(path string, timeout time.Duration) (*Store, error) {
	if timeout <= 0 {
		return nil, errors.New("open bolt event store: timeout must be positive")
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: timeout})
	if err != nil {
		return nil, fmt.Errorf("open bolt event store %q: %w", path, err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("close bolt event store: %w", err)
	}
	return nil
}

func (s *Store) CreateActor(ctx context.Context, actor agentledger.Actor) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		actors, err := tx.CreateBucketIfNotExists(actorsBucket)
		if err != nil {
			return err
		}
		keys, err := tx.CreateBucketIfNotExists(actorKeysBucket)
		if err != nil {
			return err
		}
		if actors.Get([]byte(actor.ID)) != nil || actor.Key != "" && keys.Get([]byte(actor.Key)) != nil {
			return fmt.Errorf("%w: actor %s", agentledger.ErrEntityConflict, actor.ID)
		}
		if err := putJSON(actors, actor.ID, actor); err != nil {
			return err
		}
		if actor.Key != "" {
			return keys.Put([]byte(actor.Key), []byte(actor.ID))
		}
		return nil
	})
}

func (s *Store) GetActor(ctx context.Context, id string) (agentledger.Actor, bool, error) {
	return get[agentledger.Actor](ctx, s.db, actorsBucket, id)
}

func (s *Store) GetActorByKey(ctx context.Context, key string) (agentledger.Actor, bool, error) {
	if err := ctx.Err(); err != nil {
		return agentledger.Actor{}, false, err
	}
	var actor agentledger.Actor
	var found bool
	err := s.db.View(func(tx *bolt.Tx) error {
		keys := tx.Bucket(actorKeysBucket)
		if keys == nil {
			return nil
		}
		id := keys.Get([]byte(key))
		if id == nil {
			return nil
		}
		var err error
		actor, found, err = bucketGet[agentledger.Actor](tx.Bucket(actorsBucket), string(id))
		return err
	})
	return actor, found, err
}

func (s *Store) EnsureActor(ctx context.Context, actor agentledger.Actor) (agentledger.Actor, error) {
	var stored agentledger.Actor
	err := s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		actors, err := tx.CreateBucketIfNotExists(actorsBucket)
		if err != nil {
			return err
		}
		keys, err := tx.CreateBucketIfNotExists(actorKeysBucket)
		if err != nil {
			return err
		}
		lookupID := actor.ID
		if actor.Key != "" {
			if id := keys.Get([]byte(actor.Key)); id != nil {
				lookupID = string(id)
			}
		}
		if existing, ok, getErr := bucketGet[agentledger.Actor](actors, lookupID); getErr != nil {
			return getErr
		} else if ok {
			if existing.Key != actor.Key || existing.Type != actor.Type || existing.Framework != actor.Framework {
				return fmt.Errorf("%w: actor key %s", agentledger.ErrEntityConflict, actor.Key)
			}
			stored = existing
			return nil
		}
		if actors.Get([]byte(actor.ID)) != nil {
			return fmt.Errorf("%w: actor %s", agentledger.ErrEntityConflict, actor.ID)
		}
		if err := putJSON(actors, actor.ID, actor); err != nil {
			return err
		}
		if actor.Key != "" {
			if err := keys.Put([]byte(actor.Key), []byte(actor.ID)); err != nil {
				return err
			}
		}
		stored = actor
		return nil
	})
	return stored, err
}

func (s *Store) CreateLane(ctx context.Context, lane agentledger.Lane) error {
	if lane.LastSeq != 0 {
		return errors.New("new lane must have last_seq 0")
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		lanes, err := tx.CreateBucketIfNotExists(lanesBucket)
		if err != nil {
			return err
		}
		names, err := tx.CreateBucketIfNotExists(laneNamesBucket)
		if err != nil {
			return err
		}
		nameKey := composite(lane.SessionID, lane.RunID, lane.Name)
		if lanes.Get([]byte(lane.ID)) != nil || names.Get(nameKey) != nil {
			return fmt.Errorf("%w: lane %s", agentledger.ErrEntityConflict, lane.ID)
		}
		if lane.ParentLaneID != "" {
			parent, ok, err := bucketGet[agentledger.Lane](lanes, lane.ParentLaneID)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("%w: parent lane %s", agentledger.ErrEntityNotFound, lane.ParentLaneID)
			}
			if parent.SessionID != lane.SessionID {
				return errors.New("parent lane must belong to same session")
			}
		}
		if err := putJSON(lanes, lane.ID, lane); err != nil {
			return err
		}
		return names.Put(nameKey, []byte(lane.ID))
	})
}

func (s *Store) GetLane(ctx context.Context, id string) (agentledger.Lane, bool, error) {
	return get[agentledger.Lane](ctx, s.db, lanesBucket, id)
}

func (s *Store) FindLane(ctx context.Context, sessionID, runID, name string) (agentledger.Lane, bool, error) {
	if err := ctx.Err(); err != nil {
		return agentledger.Lane{}, false, err
	}
	var lane agentledger.Lane
	var found bool
	err := s.db.View(func(tx *bolt.Tx) error {
		names := tx.Bucket(laneNamesBucket)
		if names == nil {
			return nil
		}
		id := names.Get(composite(sessionID, runID, name))
		if id == nil {
			return nil
		}
		lanes := tx.Bucket(lanesBucket)
		if lanes == nil {
			return errors.New("lane name index points to missing bucket")
		}
		var err error
		lane, found, err = bucketGet[agentledger.Lane](lanes, string(id))
		return err
	})
	return lane, found, err
}

func (s *Store) CreateTurn(ctx context.Context, turn agentledger.Turn) error {
	return s.create(ctx, turnsBucket, turn.ID, turn, func(tx *bolt.Tx) error {
		if !bucketHas(tx.Bucket(lanesBucket), turn.LaneID) {
			return fmt.Errorf("%w: lane %s", agentledger.ErrEntityNotFound, turn.LaneID)
		}
		return nil
	})
}

func (s *Store) GetTurn(ctx context.Context, id string) (agentledger.Turn, bool, error) {
	return get[agentledger.Turn](ctx, s.db, turnsBucket, id)
}

func (s *Store) CreateAction(ctx context.Context, action agentledger.Action) error {
	return s.create(ctx, actionsBucket, action.ID, action, func(tx *bolt.Tx) error {
		if !bucketHas(tx.Bucket(turnsBucket), action.TurnID) {
			return fmt.Errorf("%w: turn %s", agentledger.ErrEntityNotFound, action.TurnID)
		}
		if action.ParentActionID != "" {
			parent, ok, err := bucketGet[agentledger.Action](tx.Bucket(actionsBucket), action.ParentActionID)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("%w: parent action %s", agentledger.ErrEntityNotFound, action.ParentActionID)
			}
			if parent.TurnID != action.TurnID {
				return errors.New("parent action must belong to same turn")
			}
		}
		return nil
	})
}

func (s *Store) GetAction(ctx context.Context, id string) (agentledger.Action, bool, error) {
	return get[agentledger.Action](ctx, s.db, actionsBucket, id)
}

func (s *Store) CreateAttempt(ctx context.Context, attempt agentledger.Attempt) error {
	if attempt.AttemptNo < 1 {
		return errors.New("attempt_no must be positive")
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		attempts, err := tx.CreateBucketIfNotExists(attemptsBucket)
		if err != nil {
			return err
		}
		numbers, err := tx.CreateBucketIfNotExists(attemptNumbersBucket)
		if err != nil {
			return err
		}
		if attempts.Get([]byte(attempt.ID)) != nil || numbers.Get(composite(attempt.ActionID, fmt.Sprint(attempt.AttemptNo))) != nil {
			return fmt.Errorf("%w: attempt %s", agentledger.ErrEntityConflict, attempt.ID)
		}
		if !bucketHas(tx.Bucket(actionsBucket), attempt.ActionID) {
			return fmt.Errorf("%w: action %s", agentledger.ErrEntityNotFound, attempt.ActionID)
		}
		if err := putJSON(attempts, attempt.ID, attempt); err != nil {
			return err
		}
		return numbers.Put(composite(attempt.ActionID, fmt.Sprint(attempt.AttemptNo)), []byte(attempt.ID))
	})
}

func (s *Store) GetAttempt(ctx context.Context, id string) (agentledger.Attempt, bool, error) {
	return get[agentledger.Attempt](ctx, s.db, attemptsBucket, id)
}

func (s *Store) SaveCheckpoint(ctx context.Context, expectedRevision int64, proposed agentledger.ProposedCheckpoint) (agentledger.Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return agentledger.Checkpoint{}, err
	}
	if expectedRevision < 0 {
		return agentledger.Checkpoint{}, errors.New("expected_revision must be non-negative")
	}
	if err := validateCheckpoint(proposed); err != nil {
		return agentledger.Checkpoint{}, err
	}
	if proposed.Extensions == nil {
		proposed.Extensions = map[string]any{}
	}
	snapshot, err := clone(proposed)
	if err != nil {
		return agentledger.Checkpoint{}, err
	}
	var stored agentledger.Checkpoint
	err = s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		checkpoints, err := tx.CreateBucketIfNotExists(checkpointsBucket)
		if err != nil {
			return err
		}
		heads, err := tx.CreateBucketIfNotExists(checkpointHeadsBucket)
		if err != nil {
			return err
		}
		if previous, ok, err := bucketGet[agentledger.Checkpoint](checkpoints, proposed.ID); err != nil {
			return err
		} else if ok {
			previousCheckpoint := previous.ProposedCheckpoint
			if previousCheckpoint.Extensions == nil {
				previousCheckpoint.Extensions = map[string]any{}
			}
			if !reflect.DeepEqual(previousCheckpoint, snapshot) {
				return agentledger.ErrCheckpointIdempotencyViolation
			}
			stored = previous
			return nil
		}
		if !bucketHas(tx.Bucket(actorsBucket), proposed.ActorID) {
			return fmt.Errorf("%w: actor %s", agentledger.ErrEntityNotFound, proposed.ActorID)
		}
		actualRevision := int64(0)
		if latestID := heads.Get([]byte(proposed.Key)); latestID != nil {
			latest, ok, err := bucketGet[agentledger.Checkpoint](checkpoints, string(latestID))
			if err != nil {
				return err
			}
			if !ok {
				return errors.New("checkpoint head points to missing checkpoint")
			}
			actualRevision = latest.Revision
		}
		if actualRevision != expectedRevision {
			return fmt.Errorf("%w: expected %d, actual %d", agentledger.ErrCheckpointConflict, expectedRevision, actualRevision)
		}
		if proposed.Anchor != nil {
			event, ok, err := bucketGet[agentledger.StoredEvent](tx.Bucket(eventsBucket), proposed.Anchor.LastAppliedEventID)
			if err != nil {
				return err
			}
			if !ok || event.LaneID != proposed.Anchor.LaneID || event.Seq != proposed.Anchor.LastAppliedSeq {
				return errors.New("checkpoint anchor must identify an existing lane event")
			}
		}
		stored = agentledger.Checkpoint{ProposedCheckpoint: snapshot, Revision: actualRevision + 1, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		if err := putJSON(checkpoints, stored.ID, stored); err != nil {
			return err
		}
		return heads.Put([]byte(stored.Key), []byte(stored.ID))
	})
	return stored, err
}

func (s *Store) GetCheckpoint(ctx context.Context, id string) (agentledger.Checkpoint, bool, error) {
	return get[agentledger.Checkpoint](ctx, s.db, checkpointsBucket, id)
}

func (s *Store) LoadLatestCheckpoint(ctx context.Context, key string) (agentledger.Checkpoint, bool, error) {
	if err := ctx.Err(); err != nil {
		return agentledger.Checkpoint{}, false, err
	}
	var checkpoint agentledger.Checkpoint
	var found bool
	err := s.db.View(func(tx *bolt.Tx) error {
		heads := tx.Bucket(checkpointHeadsBucket)
		if heads == nil {
			return nil
		}
		id := heads.Get([]byte(key))
		if id == nil {
			return nil
		}
		var err error
		checkpoint, found, err = bucketGet[agentledger.Checkpoint](tx.Bucket(checkpointsBucket), string(id))
		return err
	})
	return checkpoint, found, err
}

func (s *Store) Append(ctx context.Context, laneID string, expectedLastSeq int64, appendID string, events ...agentledger.ProposedEvent) (agentledger.AppendReceipt, error) {
	if err := ctx.Err(); err != nil {
		return agentledger.AppendReceipt{}, err
	}
	if expectedLastSeq < 0 || appendID == "" || len(events) == 0 {
		return agentledger.AppendReceipt{}, errors.New("append requires non-negative head, id, and events")
	}
	batch, err := clone(events)
	if err != nil {
		return agentledger.AppendReceipt{}, err
	}
	seen := map[string]struct{}{}
	for _, event := range batch {
		if event.LaneID != laneID {
			return agentledger.AppendReceipt{}, errors.New("all events must belong to target lane")
		}
		if _, ok := seen[event.ID]; ok {
			return agentledger.AppendReceipt{}, fmt.Errorf("%w: %s", agentledger.ErrDuplicateEvent, event.ID)
		}
		seen[event.ID] = struct{}{}
	}
	digest, err := agentledger.CanonicalAppendDigest(batch)
	if err != nil {
		return agentledger.AppendReceipt{}, err
	}
	var receipt agentledger.AppendReceipt
	err = s.db.Update(func(tx *bolt.Tx) error {
		lanes, err := tx.CreateBucketIfNotExists(lanesBucket)
		if err != nil {
			return err
		}
		actors, err := tx.CreateBucketIfNotExists(actorsBucket)
		if err != nil {
			return err
		}
		turns, err := tx.CreateBucketIfNotExists(turnsBucket)
		if err != nil {
			return err
		}
		actions, err := tx.CreateBucketIfNotExists(actionsBucket)
		if err != nil {
			return err
		}
		attempts, err := tx.CreateBucketIfNotExists(attemptsBucket)
		if err != nil {
			return err
		}
		eventsBucketRef, err := tx.CreateBucketIfNotExists(eventsBucket)
		if err != nil {
			return err
		}
		appends, err := tx.CreateBucketIfNotExists(appendsBucket)
		if err != nil {
			return err
		}
		laneEvents, err := tx.CreateBucketIfNotExists(laneEventsBucket)
		if err != nil {
			return err
		}
		if encoded := appends.Get([]byte(appendID)); encoded != nil {
			if err := json.Unmarshal(encoded, &receipt); err != nil {
				return err
			}
			if receipt.LaneID != laneID || receipt.Digest != digest {
				return agentledger.ErrIdempotencyViolation
			}
			return nil
		}
		lane, ok, err := bucketGet[agentledger.Lane](lanes, laneID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: lane %s", agentledger.ErrEntityNotFound, laneID)
		}
		if lane.LastSeq != expectedLastSeq {
			return fmt.Errorf("%w: expected %d, actual %d", agentledger.ErrLaneConflict, expectedLastSeq, lane.LastSeq)
		}
		for _, event := range batch {
			if eventsBucketRef.Get([]byte(event.ID)) != nil {
				return fmt.Errorf("%w: %s", agentledger.ErrDuplicateEvent, event.ID)
			}
			if !bucketHas(actors, event.ActorID) {
				return fmt.Errorf("%w: actor %s", agentledger.ErrEntityNotFound, event.ActorID)
			}
			if !validSubject(lane, event, turns, actions, attempts) {
				return fmt.Errorf("%w: event %s", agentledger.ErrSubjectMismatch, event.ID)
			}
		}
		prior := map[string]struct{}{}
		for _, event := range batch {
			if event.CausationID != "" {
				if _, ok := prior[event.CausationID]; !ok {
					caused, exists, err := bucketGet[agentledger.StoredEvent](eventsBucketRef, event.CausationID)
					if err != nil {
						return err
					}
					if !exists {
						return fmt.Errorf("%w: causation event %s", agentledger.ErrEntityNotFound, event.CausationID)
					}
					causedLane, _, err := bucketGet[agentledger.Lane](lanes, caused.LaneID)
					if err != nil {
						return err
					}
					if causedLane.SessionID != lane.SessionID {
						return fmt.Errorf("%w: causation event %s", agentledger.ErrSubjectMismatch, event.ID)
					}
				}
			}
			prior[event.ID] = struct{}{}
		}
		index, err := laneEvents.CreateBucketIfNotExists([]byte(laneID))
		if err != nil {
			return err
		}
		committedAt := time.Now().UTC().Format(time.RFC3339Nano)
		stored := make([]agentledger.StoredEvent, 0, len(batch))
		for offset, event := range batch {
			item := agentledger.StoredEvent{ProposedEvent: event, Seq: lane.LastSeq + int64(offset) + 1, CommittedAt: committedAt}
			if err := putJSON(eventsBucketRef, item.ID, item); err != nil {
				return err
			}
			if err := index.Put(sequenceKey(uint64(item.Seq)), []byte(item.ID)); err != nil {
				return err
			}
			stored = append(stored, item)
		}
		receipt = agentledger.AppendReceipt{ID: appendID, LaneID: laneID, Digest: digest, FirstSeq: stored[0].Seq, LastSeq: stored[len(stored)-1].Seq, CommittedAt: committedAt}
		for _, event := range stored {
			receipt.EventIDs = append(receipt.EventIDs, event.ID)
		}
		lane.LastSeq = receipt.LastSeq
		if err := putJSON(lanes, lane.ID, lane); err != nil {
			return err
		}
		return putJSON(appends, appendID, receipt)
	})
	if err != nil {
		return agentledger.AppendReceipt{}, fmt.Errorf("append bolt event batch: %w", err)
	}
	return clone(receipt)
}

func (s *Store) LoadLane(ctx context.Context, laneID string, afterSeq int64) iter.Seq2[agentledger.StoredEvent, error] {
	return func(yield func(agentledger.StoredEvent, error) bool) {
		if afterSeq < 0 {
			yield(agentledger.StoredEvent{}, errors.New("after_seq must be non-negative"))
			return
		}
		var result []agentledger.StoredEvent
		err := s.db.View(func(tx *bolt.Tx) error {
			if !bucketHas(tx.Bucket(lanesBucket), laneID) {
				return fmt.Errorf("%w: lane %s", agentledger.ErrEntityNotFound, laneID)
			}
			root := tx.Bucket(laneEventsBucket)
			if root == nil {
				return nil
			}
			index := root.Bucket([]byte(laneID))
			if index == nil {
				return nil
			}
			events := tx.Bucket(eventsBucket)
			cursor := index.Cursor()
			for key, id := cursor.Seek(sequenceKey(uint64(afterSeq + 1))); key != nil; key, id = cursor.Next() {
				item, ok, err := bucketGet[agentledger.StoredEvent](events, string(id))
				if err != nil {
					return err
				}
				if !ok {
					return errors.New("lane index points to missing event")
				}
				result = append(result, item)
			}
			return nil
		})
		if err != nil {
			yield(agentledger.StoredEvent{}, err)
			return
		}
		for _, event := range result {
			if err := ctx.Err(); err != nil {
				yield(agentledger.StoredEvent{}, err)
				return
			}
			if !yield(event, nil) {
				return
			}
		}
	}
}

func (s *Store) LoadSession(ctx context.Context, sessionID string) (agentledger.SessionView, error) {
	if err := ctx.Err(); err != nil {
		return agentledger.SessionView{}, err
	}
	view := agentledger.SessionView{SessionID: sessionID}
	err := s.db.View(func(tx *bolt.Tx) error {
		laneIDs := map[string]struct{}{}
		if err := scan[agentledger.Lane](tx.Bucket(lanesBucket), func(item agentledger.Lane) {
			if item.SessionID == sessionID {
				view.Lanes = append(view.Lanes, item)
				laneIDs[item.ID] = struct{}{}
			}
		}); err != nil {
			return err
		}
		turnIDs := map[string]struct{}{}
		if err := scan[agentledger.Turn](tx.Bucket(turnsBucket), func(item agentledger.Turn) {
			if _, ok := laneIDs[item.LaneID]; ok {
				view.Turns = append(view.Turns, item)
				turnIDs[item.ID] = struct{}{}
			}
		}); err != nil {
			return err
		}
		actionIDs := map[string]struct{}{}
		if err := scan[agentledger.Action](tx.Bucket(actionsBucket), func(item agentledger.Action) {
			if _, ok := turnIDs[item.TurnID]; ok {
				view.Actions = append(view.Actions, item)
				actionIDs[item.ID] = struct{}{}
			}
		}); err != nil {
			return err
		}
		if err := scan[agentledger.Attempt](tx.Bucket(attemptsBucket), func(item agentledger.Attempt) {
			if _, ok := actionIDs[item.ActionID]; ok {
				view.Attempts = append(view.Attempts, item)
			}
		}); err != nil {
			return err
		}
		actorIDs := map[string]struct{}{}
		if err := scan[agentledger.StoredEvent](tx.Bucket(eventsBucket), func(item agentledger.StoredEvent) {
			if _, ok := laneIDs[item.LaneID]; ok {
				view.Events = append(view.Events, item)
				actorIDs[item.ActorID] = struct{}{}
			}
		}); err != nil {
			return err
		}
		return scan[agentledger.Actor](tx.Bucket(actorsBucket), func(item agentledger.Actor) {
			if _, ok := actorIDs[item.ID]; ok {
				view.Actors = append(view.Actors, item)
			}
		})
	})
	if err != nil {
		return agentledger.SessionView{}, err
	}
	sort.Slice(view.Events, func(i, j int) bool { return observedBefore(view.Events[i], view.Events[j]) })
	return view, nil
}

func (s *Store) LoadRun(ctx context.Context, sessionID, runID string) (agentledger.RunView, error) {
	view, err := s.LoadSession(ctx, sessionID)
	if err != nil {
		return agentledger.RunView{}, err
	}
	return agentledger.SelectRun(view, runID), nil
}

func observedBefore(left, right agentledger.StoredEvent) bool {
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

func (s *Store) create(ctx context.Context, bucketName []byte, id string, value any, validate func(*bolt.Tx) error) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		bucket, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		if bucket.Get([]byte(id)) != nil {
			return fmt.Errorf("%w: %s", agentledger.ErrEntityConflict, id)
		}
		if err := validate(tx); err != nil {
			return err
		}
		return putJSON(bucket, id, value)
	})
}

func get[T any](ctx context.Context, db *bolt.DB, bucketName []byte, id string) (T, bool, error) {
	var value T
	if err := ctx.Err(); err != nil {
		return value, false, err
	}
	var found bool
	err := db.View(func(tx *bolt.Tx) error {
		var err error
		value, found, err = bucketGet[T](tx.Bucket(bucketName), id)
		return err
	})
	return value, found, err
}

func validSubject(lane agentledger.Lane, event agentledger.ProposedEvent, turns, actions, attempts *bolt.Bucket) bool {
	switch event.SubjectKind() {
	case "session":
		return event.SubjectID == lane.SessionID
	case "run":
		return event.SubjectID == lane.RunID
	case "lane":
		return event.SubjectID == lane.ID
	case "turn":
		turn, ok, _ := bucketGet[agentledger.Turn](turns, event.SubjectID)
		return ok && turn.LaneID == lane.ID
	case "action":
		action, ok, _ := bucketGet[agentledger.Action](actions, event.SubjectID)
		if !ok {
			return false
		}
		turn, ok, _ := bucketGet[agentledger.Turn](turns, action.TurnID)
		return ok && turn.LaneID == lane.ID
	case "attempt":
		attempt, ok, _ := bucketGet[agentledger.Attempt](attempts, event.SubjectID)
		if !ok {
			return false
		}
		action, ok, _ := bucketGet[agentledger.Action](actions, attempt.ActionID)
		if !ok {
			return false
		}
		turn, ok, _ := bucketGet[agentledger.Turn](turns, action.TurnID)
		return ok && turn.LaneID == lane.ID
	default:
		return false
	}
}

func bucketHas(bucket *bolt.Bucket, id string) bool {
	return bucket != nil && bucket.Get([]byte(id)) != nil
}

func bucketGet[T any](bucket *bolt.Bucket, id string) (T, bool, error) {
	var value T
	if bucket == nil {
		return value, false, nil
	}
	encoded := bucket.Get([]byte(id))
	if encoded == nil {
		return value, false, nil
	}
	if err := json.Unmarshal(encoded, &value); err != nil {
		return value, false, err
	}
	return value, true, nil
}

func putJSON(bucket *bolt.Bucket, id string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(id), encoded)
}

func scan[T any](bucket *bolt.Bucket, visit func(T)) error {
	if bucket == nil {
		return nil
	}
	return bucket.ForEach(func(_, encoded []byte) error {
		if encoded == nil {
			return nil
		}
		var item T
		if err := json.Unmarshal(encoded, &item); err != nil {
			return err
		}
		visit(item)
		return nil
	})
}

func composite(parts ...string) []byte {
	var result []byte
	for index, part := range parts {
		if index > 0 {
			result = append(result, 0)
		}
		result = append(result, part...)
	}
	return result
}
func sequenceKey(value uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, value)
	return key
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

func validateCheckpoint(value agentledger.ProposedCheckpoint) error {
	if value.SchemaVersion != "1.0" || value.ID == "" || value.Key == "" || value.ActorID == "" || value.Format == "" {
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

var _ agentledger.EventStore = (*Store)(nil)
var _ agentledger.CheckpointStore = (*Store)(nil)
