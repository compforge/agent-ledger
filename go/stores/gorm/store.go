package gormstore

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"time"

	"github.com/compforge/agent-ledger/go"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Store struct {
	db               *gorm.DB
	operationTimeout time.Duration
}

func New(db *gorm.DB, operationTimeout time.Duration) (*Store, error) {
	if db == nil {
		return nil, errors.New("gorm store requires a database")
	}
	if operationTimeout <= 0 {
		return nil, errors.New("gorm store operation timeout must be positive")
	}
	return &Store{db: db, operationTimeout: operationTimeout}, nil
}

func (s *Store) Initialize(ctx context.Context) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	return s.db.WithContext(ctx).AutoMigrate(
		&actorRow{}, &laneRow{}, &turnRow{}, &actionRow{}, &attemptRow{}, &eventRow{}, &appendRow{}, &checkpointRow{},
	)
}

func (s *Store) CreateActor(ctx context.Context, actor agentledger.Actor) error {
	return s.create(ctx, "actor", actor.ID, actorToRow(actor))
}

func (s *Store) GetActor(ctx context.Context, id string) (agentledger.Actor, bool, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var row actorRow
	result := s.db.WithContext(ctx).First(&row, "id = ?", id)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return agentledger.Actor{}, false, nil
	}
	if result.Error != nil {
		return agentledger.Actor{}, false, result.Error
	}
	return row.toModel(), true, nil
}

func (s *Store) CreateLane(ctx context.Context, lane agentledger.Lane) error {
	if lane.LastSeq != 0 {
		return errors.New("new lane must have last_seq 0")
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if lane.ParentLaneID != "" {
			var parent laneRow
			if err := tx.First(&parent, "id = ?", lane.ParentLaneID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return fmt.Errorf("%w: parent lane %s", agentledger.ErrEntityNotFound, lane.ParentLaneID)
				}
				return err
			}
			if parent.SessionID != lane.SessionID {
				return errors.New("parent lane must belong to same session")
			}
		}
		if err := tx.Create(laneToRow(lane)).Error; err != nil {
			return fmt.Errorf("%w: lane %s: %v", agentledger.ErrEntityConflict, lane.ID, err)
		}
		return nil
	})
}

func (s *Store) GetLane(ctx context.Context, id string) (agentledger.Lane, bool, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var row laneRow
	result := s.db.WithContext(ctx).First(&row, "id = ?", id)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return agentledger.Lane{}, false, nil
	}
	if result.Error != nil {
		return agentledger.Lane{}, false, result.Error
	}
	return row.toModel(), true, nil
}

func (s *Store) FindLane(ctx context.Context, sessionID, runID, name string) (agentledger.Lane, bool, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var row laneRow
	result := s.db.WithContext(ctx).Where("session_id = ? AND run_id = ? AND name = ?", sessionID, runID, name).First(&row)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return agentledger.Lane{}, false, nil
	}
	if result.Error != nil {
		return agentledger.Lane{}, false, result.Error
	}
	return row.toModel(), true, nil
}

func (s *Store) CreateTurn(ctx context.Context, turn agentledger.Turn) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if !rowExists[laneRow](tx, turn.LaneID) {
			return fmt.Errorf("%w: lane %s", agentledger.ErrEntityNotFound, turn.LaneID)
		}
		if err := tx.Create(turnToRow(turn)).Error; err != nil {
			return fmt.Errorf("%w: turn %s: %v", agentledger.ErrEntityConflict, turn.ID, err)
		}
		return nil
	})
}

func (s *Store) GetTurn(ctx context.Context, id string) (agentledger.Turn, bool, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var row turnRow
	result := s.db.WithContext(ctx).First(&row, "id = ?", id)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return agentledger.Turn{}, false, nil
	}
	if result.Error != nil {
		return agentledger.Turn{}, false, result.Error
	}
	return row.toModel(), true, nil
}

func (s *Store) CreateAction(ctx context.Context, action agentledger.Action) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var turn turnRow
		if err := tx.First(&turn, "id = ?", action.TurnID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("%w: turn %s", agentledger.ErrEntityNotFound, action.TurnID)
			}
			return err
		}
		if action.ParentActionID != "" {
			var parent actionRow
			if err := tx.First(&parent, "id = ?", action.ParentActionID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return fmt.Errorf("%w: parent action %s", agentledger.ErrEntityNotFound, action.ParentActionID)
				}
				return err
			}
			if parent.TurnID != action.TurnID {
				return errors.New("parent action must belong to same turn")
			}
		}
		if err := tx.Create(actionToRow(action)).Error; err != nil {
			return fmt.Errorf("%w: action %s: %v", agentledger.ErrEntityConflict, action.ID, err)
		}
		return nil
	})
}

func (s *Store) GetAction(ctx context.Context, id string) (agentledger.Action, bool, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var row actionRow
	result := s.db.WithContext(ctx).First(&row, "id = ?", id)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return agentledger.Action{}, false, nil
	}
	if result.Error != nil {
		return agentledger.Action{}, false, result.Error
	}
	return row.toModel(), true, nil
}

func (s *Store) CreateAttempt(ctx context.Context, attempt agentledger.Attempt) error {
	if attempt.AttemptNo < 1 {
		return errors.New("attempt_no must be positive")
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if !rowExists[actionRow](tx, attempt.ActionID) {
			return fmt.Errorf("%w: action %s", agentledger.ErrEntityNotFound, attempt.ActionID)
		}
		if err := tx.Create(attemptToRow(attempt)).Error; err != nil {
			return fmt.Errorf("%w: attempt %s: %v", agentledger.ErrEntityConflict, attempt.ID, err)
		}
		return nil
	})
}

func (s *Store) GetAttempt(ctx context.Context, id string) (agentledger.Attempt, bool, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var row attemptRow
	result := s.db.WithContext(ctx).First(&row, "id = ?", id)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return agentledger.Attempt{}, false, nil
	}
	if result.Error != nil {
		return agentledger.Attempt{}, false, result.Error
	}
	return row.toModel(), true, nil
}

func (s *Store) SaveCheckpoint(ctx context.Context, expectedRevision int64, proposed agentledger.ProposedCheckpoint) (agentledger.Checkpoint, error) {
	if expectedRevision < 0 {
		return agentledger.Checkpoint{}, errors.New("expected_revision must be non-negative")
	}
	if err := validateCheckpoint(proposed); err != nil {
		return agentledger.Checkpoint{}, err
	}
	if proposed.Extensions == nil {
		proposed.Extensions = map[string]any{}
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var stored agentledger.Checkpoint
	var insertErr error
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var previous checkpointRow
		result := tx.First(&previous, "id = ?", proposed.ID)
		if result.Error == nil {
			value, err := previous.toModel()
			if err != nil {
				return err
			}
			same, err := sameCheckpoint(value.ProposedCheckpoint, proposed)
			if err != nil {
				return err
			}
			if !same {
				return agentledger.ErrCheckpointIdempotencyViolation
			}
			stored = value
			return nil
		}
		if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return result.Error
		}
		if !rowExists[actorRow](tx, proposed.ActorID) {
			return fmt.Errorf("%w: actor %s", agentledger.ErrEntityNotFound, proposed.ActorID)
		}
		var latest checkpointRow
		result = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("checkpoint_key = ?", proposed.CheckpointKey).
			Order("revision DESC").First(&latest)
		actualRevision := int64(0)
		if result.Error == nil {
			actualRevision = latest.Revision
		} else if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return result.Error
		}
		if actualRevision != expectedRevision {
			return fmt.Errorf("%w: expected %d, actual %d", agentledger.ErrCheckpointConflict, expectedRevision, actualRevision)
		}
		if proposed.Anchor != nil {
			var event eventRow
			if err := tx.First(&event, "id = ?", proposed.Anchor.LastAppliedEventID).Error; err != nil {
				return errors.New("checkpoint anchor must identify an existing lane event")
			}
			if event.LaneID != proposed.Anchor.LaneID || event.Seq != proposed.Anchor.LastAppliedSeq {
				return errors.New("checkpoint anchor must identify an existing lane event")
			}
		}
		stored = agentledger.Checkpoint{ProposedCheckpoint: proposed, Revision: actualRevision + 1, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		row, err := checkpointToRow(stored)
		if err != nil {
			return err
		}
		if err := tx.Create(row).Error; err != nil {
			insertErr = err
			return err
		}
		return nil
	})
	if insertErr != nil {
		return s.reconcileCheckpointInsert(ctx, expectedRevision, proposed, insertErr)
	}
	return stored, err
}

func (s *Store) reconcileCheckpointInsert(ctx context.Context, expectedRevision int64, proposed agentledger.ProposedCheckpoint, insertErr error) (agentledger.Checkpoint, error) {
	// Re-read after the failed transaction: constraint names and error types vary by driver,
	// while the persisted rows unambiguously distinguish idempotency from revision conflicts.
	previous, ok, err := s.GetCheckpoint(ctx, proposed.ID)
	if err != nil {
		return agentledger.Checkpoint{}, fmt.Errorf("insert checkpoint: %w", insertErr)
	}
	if ok {
		same, err := sameCheckpoint(previous.ProposedCheckpoint, proposed)
		if err != nil {
			return agentledger.Checkpoint{}, err
		}
		if !same {
			return agentledger.Checkpoint{}, agentledger.ErrCheckpointIdempotencyViolation
		}
		return previous, nil
	}
	latest, ok, err := s.LoadLatestCheckpoint(ctx, proposed.CheckpointKey)
	if err != nil {
		return agentledger.Checkpoint{}, fmt.Errorf("insert checkpoint: %w", insertErr)
	}
	actualRevision := int64(0)
	if ok {
		actualRevision = latest.Revision
	}
	if actualRevision != expectedRevision {
		return agentledger.Checkpoint{}, fmt.Errorf("%w: expected %d, actual %d", agentledger.ErrCheckpointConflict, expectedRevision, actualRevision)
	}
	return agentledger.Checkpoint{}, fmt.Errorf("insert checkpoint: %w", insertErr)
}

func (s *Store) GetCheckpoint(ctx context.Context, id string) (agentledger.Checkpoint, bool, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var row checkpointRow
	result := s.db.WithContext(ctx).First(&row, "id = ?", id)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return agentledger.Checkpoint{}, false, nil
	}
	if result.Error != nil {
		return agentledger.Checkpoint{}, false, result.Error
	}
	value, err := row.toModel()
	return value, true, err
}

func (s *Store) LoadLatestCheckpoint(ctx context.Context, checkpointKey string) (agentledger.Checkpoint, bool, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var row checkpointRow
	result := s.db.WithContext(ctx).Where("checkpoint_key = ?", checkpointKey).Order("revision DESC").First(&row)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return agentledger.Checkpoint{}, false, nil
	}
	if result.Error != nil {
		return agentledger.Checkpoint{}, false, result.Error
	}
	value, err := row.toModel()
	return value, true, err
}

func (s *Store) Append(ctx context.Context, laneID string, expectedLastSeq int64, appendID string, events ...agentledger.ProposedEvent) (agentledger.AppendReceipt, error) {
	if expectedLastSeq < 0 {
		return agentledger.AppendReceipt{}, errors.New("expected_last_seq must be non-negative")
	}
	if appendID == "" || len(events) == 0 {
		return agentledger.AppendReceipt{}, errors.New("append requires an id and at least one event")
	}
	digest, err := agentledger.CanonicalAppendDigest(events)
	if err != nil {
		return agentledger.AppendReceipt{}, err
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var receipt agentledger.AppendReceipt
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var previous appendRow
		result := tx.First(&previous, "id = ?", appendID)
		if result.Error == nil {
			if previous.LaneID != laneID || previous.Digest != digest {
				return agentledger.ErrIdempotencyViolation
			}
			receipt = previous.toModel()
			var ids []string
			if err := tx.Model(&eventRow{}).Where("lane_id = ? AND seq BETWEEN ? AND ?", laneID, previous.FirstSeq, previous.LastSeq).Order("seq").Pluck("id", &ids).Error; err != nil {
				return err
			}
			receipt.EventIDs = ids
			return nil
		}
		if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return result.Error
		}

		var lane laneRow
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&lane, "id = ?", laneID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("%w: lane %s", agentledger.ErrEntityNotFound, laneID)
			}
			return err
		}
		if lane.LastSeq != expectedLastSeq {
			return fmt.Errorf("%w: expected %d, actual %d", agentledger.ErrLaneConflict, expectedLastSeq, lane.LastSeq)
		}
		if err := validateEvents(tx, lane, events); err != nil {
			return err
		}

		committedAt := time.Now().UTC()
		rows := make([]eventRow, 0, len(events))
		for index, event := range events {
			row, err := eventToRow(event, lane.LastSeq+int64(index)+1, committedAt)
			if err != nil {
				return err
			}
			rows = append(rows, row)
		}
		if err := tx.Create(&rows).Error; err != nil {
			return fmt.Errorf("%w: %v", agentledger.ErrDuplicateEvent, err)
		}
		lastSeq := rows[len(rows)-1].Seq
		update := tx.Model(&laneRow{}).Where("id = ? AND last_seq = ?", laneID, expectedLastSeq).Update("last_seq", lastSeq)
		if update.Error != nil {
			return update.Error
		}
		if update.RowsAffected != 1 {
			return agentledger.ErrLaneConflict
		}
		appendRecord := appendRow{
			ID: appendID, LaneID: laneID, Digest: digest, FirstSeq: rows[0].Seq,
			LastSeq: lastSeq, CommittedAt: committedAt,
		}
		if err := tx.Create(&appendRecord).Error; err != nil {
			return err
		}
		receipt = appendRecord.toModel()
		for _, row := range rows {
			receipt.EventIDs = append(receipt.EventIDs, row.ID)
		}
		return nil
	})
	return receipt, err
}

func (s *Store) LoadLane(ctx context.Context, laneID string, afterSeq int64) iter.Seq2[agentledger.StoredEvent, error] {
	return func(yield func(agentledger.StoredEvent, error) bool) {
		if afterSeq < 0 {
			yield(agentledger.StoredEvent{}, errors.New("after_seq must be non-negative"))
			return
		}
		ctx, cancel := s.withTimeout(ctx)
		defer cancel()
		var count int64
		if err := s.db.WithContext(ctx).Model(&laneRow{}).Where("id = ?", laneID).Count(&count).Error; err != nil {
			yield(agentledger.StoredEvent{}, err)
			return
		}
		if count == 0 {
			yield(agentledger.StoredEvent{}, fmt.Errorf("%w: lane %s", agentledger.ErrEntityNotFound, laneID))
			return
		}
		var rows []eventRow
		if err := s.db.WithContext(ctx).Where("lane_id = ? AND seq > ?", laneID, afterSeq).Order("seq").Find(&rows).Error; err != nil {
			yield(agentledger.StoredEvent{}, err)
			return
		}
		for _, row := range rows {
			event, err := row.toModel()
			if err != nil || !yield(event, err) {
				return
			}
		}
	}
}

func (s *Store) LoadSession(ctx context.Context, sessionID string) (agentledger.SessionView, error) {
	view, err := s.loadView(ctx, sessionID, nil)
	return agentledger.SessionView{
		SessionID: view.SessionID,
		Actors:    view.Actors,
		Lanes:     view.Lanes,
		Turns:     view.Turns,
		Actions:   view.Actions,
		Attempts:  view.Attempts,
		Events:    view.Events,
	}, err
}

func (s *Store) LoadRun(ctx context.Context, sessionID, runID string) (agentledger.RunView, error) {
	return s.loadView(ctx, sessionID, &runID)
}

func (s *Store) loadView(ctx context.Context, sessionID string, runID *string) (agentledger.RunView, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	view := agentledger.RunView{SessionID: sessionID}
	if runID != nil {
		view.RunID = *runID
	}
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var lanes []laneRow
		query := tx.Where("session_id = ?", sessionID)
		if runID != nil {
			query = query.Where("run_id = ?", *runID)
		}
		if err := query.Order("created_at, id").Find(&lanes).Error; err != nil {
			return err
		}
		if len(lanes) == 0 {
			return nil
		}
		laneIDs := rowIDs(lanes, func(row laneRow) string { return row.ID })
		for _, row := range lanes {
			view.Lanes = append(view.Lanes, row.toModel())
		}
		var turns []turnRow
		if err := tx.Where("lane_id IN ?", laneIDs).Order("created_at, id").Find(&turns).Error; err != nil {
			return err
		}
		turnIDs := rowIDs(turns, func(row turnRow) string { return row.ID })
		for _, row := range turns {
			view.Turns = append(view.Turns, row.toModel())
		}
		var actions []actionRow
		if len(turnIDs) > 0 {
			if err := tx.Where("turn_id IN ?", turnIDs).Order("created_at, id").Find(&actions).Error; err != nil {
				return err
			}
		}
		actionIDs := rowIDs(actions, func(row actionRow) string { return row.ID })
		for _, row := range actions {
			view.Actions = append(view.Actions, row.toModel())
		}
		var attempts []attemptRow
		if len(actionIDs) > 0 {
			if err := tx.Where("action_id IN ?", actionIDs).Order("created_at, id").Find(&attempts).Error; err != nil {
				return err
			}
		}
		for _, row := range attempts {
			view.Attempts = append(view.Attempts, row.toModel())
		}
		var eventRows []eventRow
		if err := tx.Where("lane_id IN ?", laneIDs).Order("committed_at, lane_id, seq, id").Find(&eventRows).Error; err != nil {
			return err
		}
		actorIDs := make([]string, 0)
		seenActors := make(map[string]struct{})
		for _, row := range eventRows {
			event, err := row.toModel()
			if err != nil {
				return err
			}
			view.Events = append(view.Events, event)
			if _, seen := seenActors[row.ActorID]; !seen {
				seenActors[row.ActorID] = struct{}{}
				actorIDs = append(actorIDs, row.ActorID)
			}
		}
		if len(actorIDs) > 0 {
			var actors []actorRow
			if err := tx.Where("id IN ?", actorIDs).Order("created_at, id").Find(&actors).Error; err != nil {
				return err
			}
			for _, row := range actors {
				view.Actors = append(view.Actors, row.toModel())
			}
		}
		return nil
	})
	return view, err
}

func (s *Store) create(ctx context.Context, kind, id string, value any) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	if err := s.db.WithContext(ctx).Create(value).Error; err != nil {
		return fmt.Errorf("%w: %s %s: %v", agentledger.ErrEntityConflict, kind, id, err)
	}
	return nil
}

func (s *Store) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.operationTimeout)
}

func validateEvents(tx *gorm.DB, lane laneRow, events []agentledger.ProposedEvent) error {
	seen := make(map[string]struct{}, len(events))
	for _, event := range events {
		if event.LaneID != lane.ID {
			return errors.New("all events must belong to target lane")
		}
		if _, exists := seen[event.ID]; exists {
			return fmt.Errorf("%w: %s", agentledger.ErrDuplicateEvent, event.ID)
		}
		seen[event.ID] = struct{}{}
		if !rowExists[actorRow](tx, event.ActorID) {
			return fmt.Errorf("%w: actor %s", agentledger.ErrEntityNotFound, event.ActorID)
		}
		if ok, err := validSubject(tx, lane, event); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("%w: event %s", agentledger.ErrSubjectMismatch, event.ID)
		}
		if event.CausationID != "" {
			if _, inBatch := seen[event.CausationID]; inBatch {
				continue
			}
			var caused eventRow
			if err := tx.First(&caused, "id = ?", event.CausationID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return fmt.Errorf("%w: causation event %s", agentledger.ErrEntityNotFound, event.CausationID)
				}
				return err
			}
			var causedLane laneRow
			if err := tx.First(&causedLane, "id = ?", caused.LaneID).Error; err != nil {
				return err
			}
			if causedLane.SessionID != lane.SessionID {
				return fmt.Errorf("%w: causation event %s", agentledger.ErrSubjectMismatch, event.ID)
			}
		}
	}
	return nil
}

func validSubject(tx *gorm.DB, lane laneRow, event agentledger.ProposedEvent) (bool, error) {
	switch event.SubjectKind() {
	case "session":
		return event.SubjectID == lane.SessionID, nil
	case "run":
		return event.SubjectID == lane.RunID, nil
	case "lane":
		return event.SubjectID == lane.ID, nil
	case "turn":
		var row turnRow
		result := tx.First(&row, "id = ?", event.SubjectID)
		return row.LaneID == lane.ID, ignoreNotFound(result.Error)
	case "action":
		var row actionRow
		if err := tx.First(&row, "id = ?", event.SubjectID).Error; err != nil {
			return false, ignoreNotFound(err)
		}
		var turn turnRow
		result := tx.First(&turn, "id = ?", row.TurnID)
		return turn.LaneID == lane.ID, ignoreNotFound(result.Error)
	case "attempt":
		var attempt attemptRow
		if err := tx.First(&attempt, "id = ?", event.SubjectID).Error; err != nil {
			return false, ignoreNotFound(err)
		}
		var action actionRow
		if err := tx.First(&action, "id = ?", attempt.ActionID).Error; err != nil {
			return false, ignoreNotFound(err)
		}
		var turn turnRow
		result := tx.First(&turn, "id = ?", action.TurnID)
		return turn.LaneID == lane.ID, ignoreNotFound(result.Error)
	default:
		return false, nil
	}
}

func ignoreNotFound(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	return err
}

func rowExists[T any](tx *gorm.DB, id string) bool {
	var count int64
	return tx.Model(new(T)).Where("id = ?", id).Count(&count).Error == nil && count == 1
}

func rowIDs[T any](rows []T, id func(T) string) []string {
	result := make([]string, 0, len(rows))
	for _, row := range rows {
		result = append(result, id(row))
	}
	return result
}

type jsonMap map[string]any

func (value jsonMap) Value() (driver.Value, error) {
	if value == nil {
		value = jsonMap{}
	}
	return json.Marshal(value)
}

func (value *jsonMap) Scan(source any) error {
	var data []byte
	switch typed := source.(type) {
	case []byte:
		data = typed
	case string:
		data = []byte(typed)
	case nil:
		*value = jsonMap{}
		return nil
	default:
		return fmt.Errorf("unsupported JSON source %T", source)
	}
	return json.Unmarshal(data, value)
}

type actorRow struct {
	ID        string    `gorm:"column:id;type:char(36);primaryKey"`
	Type      string    `gorm:"column:type;type:varchar(64);not null"`
	Framework *string   `gorm:"column:framework;type:varchar(191)"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
}

func (actorRow) TableName() string { return "ledger_actors" }

type laneRow struct {
	ID           string    `gorm:"column:id;type:char(36);primaryKey"`
	SessionID    string    `gorm:"column:session_id;type:varchar(191);not null;uniqueIndex:uq_ledger_lanes_owner_name;index:ix_ledger_lanes_owner"`
	RunID        string    `gorm:"column:run_id;type:varchar(191);not null;uniqueIndex:uq_ledger_lanes_owner_name;index:ix_ledger_lanes_owner"`
	Name         string    `gorm:"column:name;type:varchar(191);not null;uniqueIndex:uq_ledger_lanes_owner_name"`
	ParentLaneID *string   `gorm:"column:parent_lane_id;type:char(36);index:ix_ledger_lanes_parent"`
	LastSeq      int64     `gorm:"column:last_seq;not null;default:0"`
	CreatedAt    time.Time `gorm:"column:created_at;not null"`
}

func (laneRow) TableName() string { return "ledger_lanes" }

type turnRow struct {
	ID        string    `gorm:"column:id;type:char(36);primaryKey"`
	LaneID    string    `gorm:"column:lane_id;type:char(36);not null;index:ix_ledger_turns_lane"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
}

func (turnRow) TableName() string { return "ledger_turns" }

type actionRow struct {
	ID             string    `gorm:"column:id;type:char(36);primaryKey"`
	TurnID         string    `gorm:"column:turn_id;type:char(36);not null;index:ix_ledger_actions_turn"`
	Type           string    `gorm:"column:type;type:varchar(191);not null"`
	ParentActionID *string   `gorm:"column:parent_action_id;type:char(36);index:ix_ledger_actions_parent"`
	CreatedAt      time.Time `gorm:"column:created_at;not null"`
}

func (actionRow) TableName() string { return "ledger_actions" }

type attemptRow struct {
	ID        string    `gorm:"column:id;type:char(36);primaryKey"`
	ActionID  string    `gorm:"column:action_id;type:char(36);not null;uniqueIndex:uq_ledger_attempts_action_no;index:ix_ledger_attempts_action"`
	AttemptNo int       `gorm:"column:attempt_no;not null;uniqueIndex:uq_ledger_attempts_action_no"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
}

func (attemptRow) TableName() string { return "ledger_attempts" }

type eventRow struct {
	ID            string    `gorm:"column:id;type:char(36);primaryKey;index:ix_ledger_events_committed,priority:4"`
	SchemaVersion string    `gorm:"column:schema_version;type:varchar(16);not null"`
	LaneID        string    `gorm:"column:lane_id;type:char(36);not null;uniqueIndex:uq_ledger_events_lane_seq;index:ix_ledger_events_lane_type;index:ix_ledger_events_committed,priority:2"`
	Seq           int64     `gorm:"column:seq;not null;uniqueIndex:uq_ledger_events_lane_seq;index:ix_ledger_events_committed,priority:3"`
	SubjectID     string    `gorm:"column:subject_id;type:varchar(191);not null;index:ix_ledger_events_subject"`
	EventType     string    `gorm:"column:event_type;type:varchar(191);not null;index:ix_ledger_events_lane_type"`
	ActorID       string    `gorm:"column:actor_id;type:char(36);not null;index:ix_ledger_events_actor"`
	CausationID   *string   `gorm:"column:causation_id;type:char(36);index:ix_ledger_events_causation"`
	OccurredAt    time.Time `gorm:"column:occurred_at;not null"`
	CommittedAt   time.Time `gorm:"column:committed_at;not null;index:ix_ledger_events_committed,priority:1"`
	Payload       jsonMap   `gorm:"column:payload;type:json;not null"`
	Extensions    jsonMap   `gorm:"column:extensions;type:json;not null"`
}

func (eventRow) TableName() string { return "ledger_events" }

type appendRow struct {
	ID          string    `gorm:"column:id;type:char(36);primaryKey"`
	LaneID      string    `gorm:"column:lane_id;type:char(36);not null;index:ix_ledger_appends_lane_range"`
	Digest      string    `gorm:"column:digest;type:char(64);not null"`
	FirstSeq    int64     `gorm:"column:first_seq;not null;index:ix_ledger_appends_lane_range"`
	LastSeq     int64     `gorm:"column:last_seq;not null;index:ix_ledger_appends_lane_range"`
	CommittedAt time.Time `gorm:"column:committed_at;not null"`
}

func (appendRow) TableName() string { return "ledger_appends" }

type checkpointRow struct {
	ID                 string    `gorm:"column:id;type:char(36);primaryKey"`
	SchemaVersion      string    `gorm:"column:schema_version;type:varchar(16);not null"`
	CheckpointKey      string    `gorm:"column:checkpoint_key;type:varchar(191);not null;uniqueIndex:uq_ledger_checkpoints_key_revision;index:ix_ledger_checkpoints_latest"`
	Revision           int64     `gorm:"column:revision;not null;uniqueIndex:uq_ledger_checkpoints_key_revision;index:ix_ledger_checkpoints_latest"`
	ActorID            string    `gorm:"column:actor_id;type:char(36);not null;index:ix_ledger_checkpoints_actor"`
	Format             string    `gorm:"column:format;type:varchar(255);not null"`
	State              jsonMap   `gorm:"column:state;type:json"`
	ArtifactRef        jsonMap   `gorm:"column:artifact_ref;type:json"`
	LaneID             *string   `gorm:"column:lane_id;type:char(36);index:ix_ledger_checkpoints_lane_seq"`
	LastAppliedSeq     *int64    `gorm:"column:last_applied_seq;index:ix_ledger_checkpoints_lane_seq"`
	LastAppliedEventID *string   `gorm:"column:last_applied_event_id;type:char(36);index:ix_ledger_checkpoints_event"`
	Extensions         jsonMap   `gorm:"column:extensions;type:json;not null"`
	CreatedAt          time.Time `gorm:"column:created_at;not null"`
}

func (checkpointRow) TableName() string { return "ledger_checkpoints" }

func actorToRow(value agentledger.Actor) *actorRow {
	return &actorRow{ID: value.ID, Type: value.Type, Framework: nullable(value.Framework), CreatedAt: mustTime(value.CreatedAt)}
}
func (row actorRow) toModel() agentledger.Actor {
	return agentledger.Actor{ID: row.ID, Type: row.Type, Framework: stringValue(row.Framework), CreatedAt: formatTime(row.CreatedAt)}
}
func laneToRow(value agentledger.Lane) *laneRow {
	return &laneRow{ID: value.ID, SessionID: value.SessionID, RunID: value.RunID, Name: value.Name, ParentLaneID: nullable(value.ParentLaneID), LastSeq: value.LastSeq, CreatedAt: mustTime(value.CreatedAt)}
}
func (row laneRow) toModel() agentledger.Lane {
	return agentledger.Lane{ID: row.ID, SessionID: row.SessionID, RunID: row.RunID, Name: row.Name, ParentLaneID: stringValue(row.ParentLaneID), LastSeq: row.LastSeq, CreatedAt: formatTime(row.CreatedAt)}
}
func turnToRow(value agentledger.Turn) *turnRow {
	return &turnRow{ID: value.ID, LaneID: value.LaneID, CreatedAt: mustTime(value.CreatedAt)}
}
func (row turnRow) toModel() agentledger.Turn {
	return agentledger.Turn{ID: row.ID, LaneID: row.LaneID, CreatedAt: formatTime(row.CreatedAt)}
}
func actionToRow(value agentledger.Action) *actionRow {
	return &actionRow{ID: value.ID, TurnID: value.TurnID, Type: value.Type, ParentActionID: nullable(value.ParentActionID), CreatedAt: mustTime(value.CreatedAt)}
}
func (row actionRow) toModel() agentledger.Action {
	return agentledger.Action{ID: row.ID, TurnID: row.TurnID, Type: row.Type, ParentActionID: stringValue(row.ParentActionID), CreatedAt: formatTime(row.CreatedAt)}
}
func attemptToRow(value agentledger.Attempt) *attemptRow {
	return &attemptRow{ID: value.ID, ActionID: value.ActionID, AttemptNo: value.AttemptNo, CreatedAt: mustTime(value.CreatedAt)}
}
func (row attemptRow) toModel() agentledger.Attempt {
	return agentledger.Attempt{ID: row.ID, ActionID: row.ActionID, AttemptNo: row.AttemptNo, CreatedAt: formatTime(row.CreatedAt)}
}
func eventToRow(value agentledger.ProposedEvent, seq int64, committedAt time.Time) (eventRow, error) {
	occurredAt, err := time.Parse(time.RFC3339Nano, value.OccurredAt)
	if err != nil {
		return eventRow{}, fmt.Errorf("parse event occurred_at: %w", err)
	}
	return eventRow{
		ID: value.ID, SchemaVersion: value.SchemaVersion, LaneID: value.LaneID, Seq: seq,
		SubjectID: value.SubjectID, EventType: value.EventType, ActorID: value.ActorID,
		CausationID: nullable(value.CausationID), OccurredAt: occurredAt, CommittedAt: committedAt,
		Payload: jsonMap(value.Payload), Extensions: jsonMap(value.Extensions),
	}, nil
}
func (row eventRow) toModel() (agentledger.StoredEvent, error) {
	return agentledger.StoredEvent{ProposedEvent: agentledger.ProposedEvent{
		SchemaVersion: row.SchemaVersion, ID: row.ID, LaneID: row.LaneID, SubjectID: row.SubjectID,
		EventType: row.EventType, ActorID: row.ActorID, CausationID: stringValue(row.CausationID),
		OccurredAt: formatTime(row.OccurredAt), Payload: map[string]any(row.Payload), Extensions: map[string]any(row.Extensions),
	}, Seq: row.Seq, CommittedAt: formatTime(row.CommittedAt)}, nil
}
func (row appendRow) toModel() agentledger.AppendReceipt {
	return agentledger.AppendReceipt{ID: row.ID, LaneID: row.LaneID, Digest: row.Digest, FirstSeq: row.FirstSeq, LastSeq: row.LastSeq, CommittedAt: formatTime(row.CommittedAt)}
}

func checkpointToRow(value agentledger.Checkpoint) (*checkpointRow, error) {
	row := &checkpointRow{
		ID: value.ID, SchemaVersion: value.SchemaVersion, CheckpointKey: value.CheckpointKey,
		Revision: value.Revision, ActorID: value.ActorID, Format: value.Format,
		State: jsonMap(value.State), Extensions: jsonMap(value.Extensions), CreatedAt: mustTime(value.CreatedAt),
	}
	if value.ArtifactRef != nil {
		encoded, err := json.Marshal(value.ArtifactRef)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(encoded, &row.ArtifactRef); err != nil {
			return nil, err
		}
	}
	if value.Anchor != nil {
		row.LaneID = nullable(value.Anchor.LaneID)
		row.LastAppliedSeq = &value.Anchor.LastAppliedSeq
		row.LastAppliedEventID = nullable(value.Anchor.LastAppliedEventID)
	}
	return row, nil
}

func (row checkpointRow) toModel() (agentledger.Checkpoint, error) {
	proposed := agentledger.ProposedCheckpoint{
		SchemaVersion: row.SchemaVersion, ID: row.ID, CheckpointKey: row.CheckpointKey,
		ActorID: row.ActorID, Format: row.Format, State: map[string]any(row.State),
		Extensions: map[string]any(row.Extensions),
	}
	if len(row.ArtifactRef) > 0 {
		encoded, err := json.Marshal(row.ArtifactRef)
		if err != nil {
			return agentledger.Checkpoint{}, err
		}
		var ref agentledger.ArtifactRef
		if err := json.Unmarshal(encoded, &ref); err != nil {
			return agentledger.Checkpoint{}, err
		}
		proposed.ArtifactRef = &ref
	}
	if row.LaneID != nil && row.LastAppliedSeq != nil && row.LastAppliedEventID != nil {
		proposed.Anchor = &agentledger.CheckpointAnchor{
			LaneID: *row.LaneID, LastAppliedSeq: *row.LastAppliedSeq, LastAppliedEventID: *row.LastAppliedEventID,
		}
	}
	return agentledger.Checkpoint{ProposedCheckpoint: proposed, Revision: row.Revision, CreatedAt: formatTime(row.CreatedAt)}, nil
}

func validateCheckpoint(value agentledger.ProposedCheckpoint) error {
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

func sameCheckpoint(left, right agentledger.ProposedCheckpoint) (bool, error) {
	if left.Extensions == nil {
		left.Extensions = map[string]any{}
	}
	if right.Extensions == nil {
		right.Extensions = map[string]any{}
	}
	leftJSON, err := json.Marshal(left)
	if err != nil {
		return false, err
	}
	rightJSON, err := json.Marshal(right)
	if err != nil {
		return false, err
	}
	return string(leftJSON) == string(rightJSON), nil
}

func nullable(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
func mustTime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(fmt.Sprintf("invalid model timestamp %q: %v", value, err))
	}
	return parsed
}
func formatTime(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }

var _ agentledger.EventStore = (*Store)(nil)
var _ agentledger.CheckpointStore = (*Store)(nil)
