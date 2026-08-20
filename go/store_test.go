package agentledger

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryStoreExecutionHierarchyAndAppend(t *testing.T) {
	testStoreContract(t, NewMemoryEventStore())
}

func testStoreContract(t *testing.T, store EventStore) {
	t.Helper()
	ctx := context.Background()
	actor := NewActor("agent", "plain-loop")
	lane := NewLane("session-1", "run-1", "main", "")
	turn := NewTurn(lane.ID)
	action := NewAction(turn.ID, "model_call", "")
	attempt := NewAttempt(action.ID, 1)

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
	event := NewEvent("attempt.requested", lane.ID, attempt.ID, actor)
	event.Payload = map[string]any{"model": "test"}
	receipt, err := store.Append(ctx, lane.ID, 0, NewID(), event)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if receipt.FirstSeq != 1 || receipt.LastSeq != 1 {
		t.Fatalf("receipt sequence = %d..%d", receipt.FirstSeq, receipt.LastSeq)
	}

	var events []StoredEvent
	for stored, loadErr := range store.LoadLane(ctx, lane.ID, 0) {
		if loadErr != nil {
			t.Fatalf("load lane: %v", loadErr)
		}
		events = append(events, stored)
	}
	if len(events) != 1 || events[0].SubjectID != attempt.ID {
		t.Fatalf("events = %#v", events)
	}
	view, err := store.LoadSession(ctx, lane.SessionID)
	if err != nil {
		t.Fatalf("load session: %v", err)
	}
	if len(view.Actors) != 1 || len(view.Lanes) != 1 || len(view.Turns) != 1 || len(view.Actions) != 1 || len(view.Attempts) != 1 || len(view.Events) != 1 {
		t.Fatalf("incomplete session view: %#v", view)
	}
}

func TestMemoryStoreAppendIsIdempotentAndOptimistic(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEventStore()
	actor := NewActor("agent", "")
	lane := NewLane("session", "run", "main", "")
	if err := store.CreateActor(ctx, actor); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateLane(ctx, lane); err != nil {
		t.Fatal(err)
	}
	event := NewEvent("lane.started", lane.ID, lane.ID, actor)
	appendID := NewID()
	first, err := store.Append(ctx, lane.ID, 0, appendID, event)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Append(ctx, lane.ID, 0, appendID, event)
	if err != nil || second.LastSeq != first.LastSeq {
		t.Fatalf("idempotent append = %#v, %v", second, err)
	}
	other := NewEvent("lane.completed", lane.ID, lane.ID, actor)
	if _, err := store.Append(ctx, lane.ID, 0, NewID(), other); !errors.Is(err, ErrLaneConflict) {
		t.Fatalf("append error = %v, want lane conflict", err)
	}
}

func TestLaneRecorderCreatesAttemptsForRetries(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEventStore()
	recorder, err := OpenRecorder(ctx, RecorderOptions{
		Store: store, SessionID: "session", RunID: "run", Actor: NewActor("agent", "plain-loop"),
	})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := recorder.StartTurn(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := recorder.BeforeModelCall(ctx, turn.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.ModelFailed(ctx, first, errors.New("timeout")); err != nil {
		t.Fatal(err)
	}
	second, err := recorder.Retry(ctx, first.ActionID, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.ActionID != second.ActionID || first.AttemptID == second.AttemptID || second.AttemptNo != 2 {
		t.Fatalf("retry handles = %#v %#v", first, second)
	}
}

func TestCommittedEventContentIsImmutable(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEventStore()
	actor := NewActor("agent", "")
	lane := NewLane("session", "run", "main", "")
	_ = store.CreateActor(ctx, actor)
	_ = store.CreateLane(ctx, lane)
	event := NewEvent("lane.recorded", lane.ID, lane.ID, actor)
	event.Payload["nested"] = map[string]any{"value": float64(1)}
	if _, err := store.Append(ctx, lane.ID, 0, NewID(), event); err != nil {
		t.Fatal(err)
	}
	event.Payload["nested"].(map[string]any)["value"] = float64(2)
	var stored StoredEvent
	for item, err := range store.LoadLane(ctx, lane.ID, 0) {
		if err != nil {
			t.Fatal(err)
		}
		stored = item
	}
	if got := stored.Payload["nested"].(map[string]any)["value"]; got != float64(1) {
		t.Fatalf("stored value = %v", got)
	}
}
