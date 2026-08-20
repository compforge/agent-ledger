package boltstore

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/compforge/agent-ledger/go"
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
}
