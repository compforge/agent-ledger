package boltstore

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
)

func TestStorePersistsAndResumesAppendContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	store := openTestStore(t, path)
	stream := agentledger.EventStream{SessionID: "session-1", StreamID: "run-1"}
	event := agentledger.NewEvent("run.started", stream.SessionID, "run-1", agentledger.Actor{Type: "agent", ID: "test"})

	receipt, err := store.Append(context.Background(), stream, -1, "append-1", event)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	store = openTestStore(t, path)
	t.Cleanup(func() { _ = store.Close() })
	replayed, err := store.Append(context.Background(), stream, -1, "append-1", event)
	if err != nil {
		t.Fatalf("replay idempotent append: %v", err)
	}
	if replayed.Digest != receipt.Digest || replayed.LastVersion != receipt.LastVersion {
		t.Fatalf("replayed receipt = %#v, want %#v", replayed, receipt)
	}

	second := agentledger.NewEvent("run.completed", stream.SessionID, "run-1", agentledger.Actor{Type: "agent", ID: "test"})
	if _, err := store.Append(context.Background(), stream, -1, "append-2", second); !errors.Is(err, agentledger.ErrStreamConflict) {
		t.Fatalf("stale append error = %v, want ErrStreamConflict", err)
	}
	if _, err := store.Append(context.Background(), stream, 0, "append-2", second); err != nil {
		t.Fatalf("append after reopen: %v", err)
	}

	var eventTypes []string
	for stored, err := range store.ScanSession(context.Background(), stream.SessionID, "") {
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		eventTypes = append(eventTypes, stored.EventType)
	}
	if len(eventTypes) != 2 || eventTypes[0] != "run.started" || eventTypes[1] != "run.completed" {
		t.Fatalf("event types = %v", eventTypes)
	}
}

func TestStoreRejectsDuplicateEventAcrossStreamsInSession(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "ledger.db"))
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	event := agentledger.NewEvent("test.recorded", "session", "run-1", agentledger.Actor{Type: "agent", ID: "test"})
	if _, err := store.Append(ctx, agentledger.EventStream{SessionID: "session", StreamID: "run-1"}, -1, "one", event); err != nil {
		t.Fatalf("append first: %v", err)
	}
	event.RunID = "run-2"
	if _, err := store.Append(ctx, agentledger.EventStream{SessionID: "session", StreamID: "run-2"}, -1, "two", event); !errors.Is(err, agentledger.ErrDuplicateEvent) {
		t.Fatalf("duplicate error = %v, want ErrDuplicateEvent", err)
	}
}

func TestOpenRequiresExplicitTimeout(t *testing.T) {
	if _, err := Open(filepath.Join(t.TempDir(), "ledger.db"), 0); err == nil {
		t.Fatal("Open accepted zero timeout")
	}
}

func openTestStore(t *testing.T, path string) *Store {
	t.Helper()
	store, err := Open(path, time.Second)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store
}
