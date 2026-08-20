package gormstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/compforge/agent-ledger/go"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestSQLiteStorePersistsExecutionModel(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(memoryDSN()), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(db, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := store.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	actor := agentledger.NewActor("agent", "agentgo")
	lane := agentledger.NewLane("session", "run", "main", "")
	turn := agentledger.NewTurn(lane.ID)
	action := agentledger.NewAction(turn.ID, "model_call", "")
	attempt := agentledger.NewAttempt(action.ID, 1)
	creates := []struct {
		label  string
		create func() error
	}{
		{"actor", func() error { return store.CreateActor(ctx, actor) }},
		{"lane", func() error { return store.CreateLane(ctx, lane) }},
		{"turn", func() error { return store.CreateTurn(ctx, turn) }},
		{"action", func() error { return store.CreateAction(ctx, action) }},
		{"attempt", func() error { return store.CreateAttempt(ctx, attempt) }},
	}
	for _, item := range creates {
		if err := item.create(); err != nil {
			t.Fatalf("create %s: %v", item.label, err)
		}
	}
	event := agentledger.NewEvent("attempt.requested", lane.ID, attempt.ID, actor)
	event.Payload = map[string]any{"model": "test"}
	receipt, err := store.Append(ctx, lane.ID, 0, agentledger.NewID(), event)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.LastSeq != 1 {
		t.Fatalf("last seq = %d", receipt.LastSeq)
	}
	checkpoint := agentledger.NewCheckpoint(
		"native-session", actor.ID,
		"application/vnd.compforge.agentgo.messages+json;version=1",
		map[string]any{"messages": []any{"hello"}, "turn": 1},
	)
	checkpoint.Extensions = nil
	checkpoint.Anchor = &agentledger.CheckpointAnchor{LaneID: lane.ID, LastAppliedSeq: 1, LastAppliedEventID: event.ID}
	saved, err := store.SaveCheckpoint(ctx, 0, checkpoint)
	if err != nil || saved.Revision != 1 {
		t.Fatalf("checkpoint = %#v, %v", saved, err)
	}
	checkpoint.Extensions = map[string]any{}
	repeated, err := store.SaveCheckpoint(ctx, 0, checkpoint)
	if err != nil || repeated.ID != saved.ID {
		t.Fatalf("idempotent checkpoint = %#v, %v", repeated, err)
	}
	view, err := store.LoadSession(ctx, lane.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Actors) != 1 || len(view.Lanes) != 1 || len(view.Turns) != 1 || len(view.Actions) != 1 || len(view.Attempts) != 1 || len(view.Events) != 1 {
		t.Fatalf("incomplete session view: %#v", view)
	}
	runView, err := store.LoadRun(ctx, lane.SessionID, lane.RunID)
	if err != nil || len(runView.Lanes) != 1 || len(runView.Events) != 1 {
		t.Fatalf("incomplete run view: %#v, %v", runView, err)
	}
}

func TestCheckpointInsertFailureIsNotARevisionConflict(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(memoryDSN()), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(db, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := store.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	actor := agentledger.NewActor("agent", "agentgo")
	if err := store.CreateActor(ctx, actor); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE TRIGGER fail_checkpoint_insert BEFORE INSERT ON ledger_checkpoints BEGIN SELECT RAISE(ABORT, 'injected checkpoint insert failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	checkpoint := agentledger.NewCheckpoint("native-session", actor.ID, "application/json", map[string]any{"value": 1})
	_, err = store.SaveCheckpoint(ctx, 0, checkpoint)
	if err == nil || errors.Is(err, agentledger.ErrCheckpointConflict) || !strings.Contains(err.Error(), "injected checkpoint insert failure") {
		t.Fatalf("save error = %v, want original insert failure", err)
	}
}

func TestAutoMigrateCreatesNoForeignKeys(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(memoryDSN()), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	store, _ := New(db, time.Second)
	if err := store.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	tables := []string{"ledger_lanes", "ledger_turns", "ledger_actions", "ledger_attempts", "ledger_events", "ledger_appends", "ledger_checkpoints"}
	for _, table := range tables {
		var count int
		if err := db.Raw("SELECT COUNT(*) FROM pragma_foreign_key_list(?)", table).Scan(&count).Error; err != nil {
			t.Fatalf("inspect %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("table %s has %d foreign keys", table, count)
		}
	}
}

func memoryDSN() string {
	return fmt.Sprintf("file:%s?mode=memory&cache=shared", agentledger.NewID())
}
