package boltstore

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/compforge/agent-ledger/go"
	bolt "go.etcd.io/bbolt"
)

func TestStorePersistsLane(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	store, err := Open(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	actor := agentledger.NewActor("agent", "agentgo")
	lane := agentledger.NewLane("session", "run", "main", "")
	if err := store.CreateActor(ctx, actor); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateLane(ctx, lane); err != nil {
		t.Fatal(err)
	}
	event := agentledger.NewEvent("lane.started", lane.ID, lane.ID, actor)
	if _, err := store.Append(ctx, lane.ID, 0, agentledger.NewID(), event); err != nil {
		t.Fatal(err)
	}
	checkpoint := agentledger.NewCheckpoint(
		"native-session", actor.ID,
		"application/vnd.compforge.agentgo.messages+json;version=1",
		map[string]any{"messages": []any{"hello"}},
	)
	checkpoint.Extensions = nil
	checkpoint.Anchor = &agentledger.CheckpointAnchor{LaneID: lane.ID, LastAppliedSeq: 1, LastAppliedEventID: event.ID}
	saved, err := store.SaveCheckpoint(ctx, 0, checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint.Extensions = map[string]any{}
	repeated, err := store.SaveCheckpoint(ctx, 0, checkpoint)
	if err != nil || repeated.ID != saved.ID {
		t.Fatalf("idempotent checkpoint = %#v, %v", repeated, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	stored, ok, err := reopened.GetLane(ctx, lane.ID)
	if err != nil || !ok || stored.LastSeq != 1 {
		t.Fatalf("lane = %#v, %v", stored, err)
	}
	latest, ok, err := reopened.LoadLatestCheckpoint(ctx, checkpoint.Key)
	if err != nil || !ok || latest.ID != checkpoint.ID {
		t.Fatalf("checkpoint = %#v, %v", latest, err)
	}
	runView, err := reopened.LoadRun(ctx, lane.SessionID, lane.RunID)
	if err != nil || len(runView.Lanes) != 1 || len(runView.Events) != 1 {
		t.Fatalf("run view = %#v, %v", runView, err)
	}
}

func TestCheckpointRetryAcceptsLegacyNilExtensions(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "ledger.db"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	actor := agentledger.NewActor("agent", "agentgo")
	if err := store.CreateActor(ctx, actor); err != nil {
		t.Fatal(err)
	}
	checkpoint := agentledger.NewCheckpoint("native-session", actor.ID, "application/json", map[string]any{"value": 1})
	checkpoint.Extensions = nil
	legacy := agentledger.Checkpoint{ProposedCheckpoint: checkpoint, Revision: 1, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := store.db.Update(func(tx *bolt.Tx) error {
		checkpoints, err := tx.CreateBucketIfNotExists(checkpointsBucket)
		if err != nil {
			return err
		}
		if err := putJSON(checkpoints, legacy.ID, legacy); err != nil {
			return err
		}
		heads, err := tx.CreateBucketIfNotExists(checkpointHeadsBucket)
		if err != nil {
			return err
		}
		return heads.Put([]byte(legacy.Key), []byte(legacy.ID))
	}); err != nil {
		t.Fatal(err)
	}

	checkpoint.Extensions = map[string]any{}
	repeated, err := store.SaveCheckpoint(ctx, 0, checkpoint)
	if err != nil || repeated.ID != legacy.ID {
		t.Fatalf("idempotent checkpoint = %#v, %v", repeated, err)
	}
}
