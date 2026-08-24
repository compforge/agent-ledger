package agentledger

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestMemoryStoreExecutionHierarchyAndAppend(t *testing.T) {
	testStoreContract(t, NewMemoryEventStore())
}

func TestMemoryStoreLoadsBoundedLanePage(t *testing.T) {
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
	for index, eventType := range []string{"lane.started", "lane.recorded", "lane.completed"} {
		event := NewEvent(eventType, lane.ID, lane.ID, actor)
		if _, err := store.Append(ctx, lane.ID, int64(index), NewID(), event); err != nil {
			t.Fatal(err)
		}
	}

	first, err := store.LoadLanePage(ctx, lane.ID, 0, 2)
	if err != nil || len(first.Events) != 2 || !first.HasMore || first.Events[1].Seq != 2 {
		t.Fatalf("first page = %#v, %v", first, err)
	}
	second, err := store.LoadLanePage(ctx, lane.ID, first.Events[1].Seq, 2)
	if err != nil || len(second.Events) != 1 || second.HasMore || second.Events[0].Seq != 3 {
		t.Fatalf("second page = %#v, %v", second, err)
	}
}

func testStoreContract(t *testing.T, store EventStore) {
	t.Helper()
	ctx := context.Background()
	actor := NewActorWithKey("test/agent", "agent", "plain-loop")
	artifact := NewArtifact("model/request", "v1", "s3://artifacts/model-request-v1", strings.Repeat("0", 64), 42, "application/json")
	lane := NewLane("session-1", "run-1", "main", "")
	turn := NewTurn(lane.ID)
	action := NewAction(turn.ID, "model_call", "model-1", "")
	attempt := NewAttempt(action.ID, 1)

	creates := []struct {
		label  string
		create func() error
	}{
		{"actor", func() error { return store.CreateActor(ctx, actor) }},
		{"artifact", func() error { return store.CreateArtifact(ctx, artifact) }},
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
	byKey, ok, err := store.GetActorByKey(ctx, actor.Key)
	if err != nil || !ok || byKey.ID != actor.ID {
		t.Fatalf("actor by key = %#v, %v", byKey, err)
	}
	restarted := NewActorWithKey(actor.Key, actor.Type, actor.Framework)
	ensured, err := store.EnsureActor(ctx, restarted)
	if err != nil || ensured.ID != actor.ID {
		t.Fatalf("ensure actor = %#v, %v", ensured, err)
	}
	changed := NewActorWithKey(actor.Key, actor.Type, "other-framework")
	if _, err := store.EnsureActor(ctx, changed); !errors.Is(err, ErrEntityConflict) {
		t.Fatalf("ensure changed actor error = %v, want entity conflict", err)
	}
	byArtifactKey, ok, err := store.GetArtifactByKey(ctx, artifact.Key, artifact.Version)
	if err != nil || !ok || byArtifactKey.ID != artifact.ID {
		t.Fatalf("artifact by key/version = %#v, %v", byArtifactKey, err)
	}
	restartedArtifact := NewArtifact(artifact.Key, artifact.Version, artifact.URI, artifact.SHA256, artifact.Size, artifact.ContentType)
	ensuredArtifact, err := store.EnsureArtifact(ctx, restartedArtifact)
	if err != nil || ensuredArtifact.ID != artifact.ID {
		t.Fatalf("ensure artifact = %#v, %v", ensuredArtifact, err)
	}
	changedArtifact := NewArtifact(artifact.Key, artifact.Version, "s3://artifacts/different", artifact.SHA256, artifact.Size, artifact.ContentType)
	if _, err := store.EnsureArtifact(ctx, changedArtifact); !errors.Is(err, ErrEntityConflict) {
		t.Fatalf("ensure changed artifact error = %v, want entity conflict", err)
	}
	secondArtifact := NewArtifact(artifact.Key, "v2", artifact.URI+"-v2", artifact.SHA256, artifact.Size, artifact.ContentType)
	if _, err := store.EnsureArtifact(ctx, secondArtifact); err != nil {
		t.Fatalf("ensure second artifact version: %v", err)
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
	if view.Actions[0].Key != action.Key {
		t.Fatalf("action key = %q, want %q", view.Actions[0].Key, action.Key)
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

func TestMemoryStoreCheckpointVersionsAndAnchor(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEventStore()
	actor := NewActor("agent", "plain-loop")
	lane := NewLane("session", "run", "main", "")
	if err := store.CreateActor(ctx, actor); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateLane(ctx, lane); err != nil {
		t.Fatal(err)
	}
	event := NewEvent("lane.state.changed", lane.ID, lane.ID, actor)
	if _, err := store.Append(ctx, lane.ID, 0, NewID(), event); err != nil {
		t.Fatal(err)
	}
	proposed := NewCheckpoint(
		"native-session", actor.ID,
		"application/vnd.compforge.agentgo.messages+json;version=1",
		map[string]any{"messages": []any{"hello"}},
	)
	proposed.Extensions = nil
	proposed.Anchor = &CheckpointAnchor{LaneID: lane.ID, LastAppliedSeq: 1, LastAppliedEventID: event.ID}
	first, err := store.SaveCheckpoint(ctx, 0, proposed)
	if err != nil {
		t.Fatal(err)
	}
	if first.Revision != 1 {
		t.Fatalf("revision = %d", first.Revision)
	}
	proposed.Extensions = map[string]any{}
	repeated, err := store.SaveCheckpoint(ctx, 0, proposed)
	if err != nil || repeated.ID != first.ID {
		t.Fatalf("idempotent save = %#v, %v", repeated, err)
	}
	second := NewCheckpoint(proposed.Key, actor.ID, proposed.Format, map[string]any{"messages": []any{"hello", "world"}})
	latest, err := store.SaveCheckpoint(ctx, 1, second)
	if err != nil || latest.Revision != 2 {
		t.Fatalf("second save = %#v, %v", latest, err)
	}
	loaded, ok, err := store.LoadLatestCheckpoint(ctx, proposed.Key)
	if err != nil || !ok || loaded.ID != second.ID {
		t.Fatalf("latest checkpoint = %#v, %v", loaded, err)
	}
	if _, err := store.SaveCheckpoint(ctx, 0, NewCheckpoint(proposed.Key, actor.ID, proposed.Format, map[string]any{})); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("save error = %v, want checkpoint conflict", err)
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
	first, err := recorder.BeforeModelCall(ctx, turn.ID, map[string]any{"input": []any{}})
	if err != nil {
		t.Fatal(err)
	}
	failed, err := recorder.ModelFailed(ctx, first, errors.New("timeout"), map[string]any{
		"usage": map[string]any{"input_tokens": 8}, "provider_request_id": "request-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if failed.EventType != EventTypeAttemptFailed || failed.Payload["provider_request_id"] != "request-1" {
		t.Fatalf("failed Event = %#v", failed)
	}
	if usage, ok := failed.Payload["usage"].(map[string]any); !ok || usage["input_tokens"] != 8 {
		t.Fatalf("failed usage = %#v", failed.Payload["usage"])
	}
	if failure, ok := failed.Payload["error"].(map[string]any); !ok || failure["message"] != "timeout" {
		t.Fatalf("structured error = %#v", failed.Payload["error"])
	}
	second, err := recorder.Retry(ctx, first.ActionID, 2, map[string]any{"input": []any{}})
	if err != nil {
		t.Fatal(err)
	}
	if first.ActionID != second.ActionID || first.AttemptID == second.AttemptID || second.AttemptNo != 2 {
		t.Fatalf("retry handles = %#v %#v", first, second)
	}
	view, err := store.LoadRun(ctx, "session", "run")
	if err != nil {
		t.Fatal(err)
	}
	var starts, requests int
	for _, event := range view.Events {
		if event.EventType == EventTypeActionStarted {
			starts++
		}
		if event.EventType == EventTypeAttemptRequested {
			requests++
		}
	}
	if starts != 0 || requests != 2 {
		t.Fatalf("starts=%d requests=%d", starts, requests)
	}
}

func TestAttemptCancellationAndUnknownOutcomeAreTerminal(t *testing.T) {
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
	cancelled, err := recorder.BeforeModelCall(ctx, turn.ID, map[string]any{"input": []any{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.CancelAttempt(ctx, cancelled, "user_interrupt"); err != nil {
		t.Fatal(err)
	}
	unknown, err := recorder.BeforeToolCall(ctx, turn.ID, "tool-1", map[string]any{
		"tool_name": "write", "input": map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := recorder.Retry(ctx, unknown.ActionID, 2, map[string]any{
		"tool_name": "write", "input": map[string]any{}, "recovery_decision_id": "decision-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.MarkAttemptOutcomeUnknown(ctx, unknown, "worker_lost", replacement.AttemptID); err != nil {
		t.Fatal(err)
	}
	view, err := store.LoadRun(ctx, "session", "run")
	if err != nil {
		t.Fatal(err)
	}
	unresolved := InspectRun(view).UnresolvedAttempts
	if len(unresolved) != 1 || unresolved[0].AttemptID != replacement.AttemptID {
		t.Fatalf("unresolved attempts = %#v", unresolved)
	}
}

func TestRunCompletionLinksCheckpointAtomicallyAndRemainsInspectable(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEventStore()
	actor := NewActor("agent", "plain-loop")
	recorder, err := OpenRecorder(ctx, RecorderOptions{
		Store: store, SessionID: "session", RunID: "run", Actor: actor,
	})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := recorder.StartTurn(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	unresolved, err := recorder.BeforeToolCall(ctx, turn.ID, "charge-1", map[string]any{
		"tool_name": "charge", "input": map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpointLinked := NewEvent(
		EventTypeLaneFrameworkCheckpointLinked, recorder.Lane().ID, recorder.Lane().ID, actor,
	)
	checkpointLinked.Payload = map[string]any{
		"checkpoint_id": NewID(), "profile": "plain-loop", "profile_version": "1",
		"metadata": map[string]any{"reason": "idle"},
	}
	runCompleted := NewEvent(EventTypeRunCompleted, recorder.Lane().ID, recorder.RunID(), actor)
	runCompleted.Payload = map[string]any{"result": "done"}
	runCompleted.CausationID = checkpointLinked.ID
	appendID := NewID()
	receipt, err := recorder.Append(ctx, appendID, checkpointLinked, runCompleted)
	if err != nil {
		t.Fatal(err)
	}
	newer, err := recorder.Record(ctx, "lane.test.newer", recorder.Lane().ID, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := recorder.Append(ctx, appendID, checkpointLinked, runCompleted)
	if err != nil || repeated.ID != receipt.ID || repeated.LastSeq != receipt.LastSeq {
		t.Fatalf("idempotent recorder append = %#v, %v", repeated, err)
	}
	afterReplay, err := recorder.Record(ctx, "lane.test.after_replay", recorder.Lane().ID, nil, "")
	if err != nil {
		t.Fatalf("append after replay: %v", err)
	}
	if len(receipt.EventIDs) != 2 || receipt.EventIDs[0] != checkpointLinked.ID || receipt.EventIDs[1] != runCompleted.ID {
		t.Fatalf("completion receipt = %#v", receipt)
	}
	if receipt.FirstSeq+1 != receipt.LastSeq || newer.Seq != receipt.LastSeq+1 ||
		afterReplay.Seq != newer.Seq+1 || recorder.Lane().LastSeq != afterReplay.Seq {
		t.Fatalf("completion batch = %#v lane = %#v", receipt, recorder.Lane())
	}
	if runCompleted.CausationID != checkpointLinked.ID {
		t.Fatalf("run completion cause = %q", runCompleted.CausationID)
	}
	other, err := OpenRecorder(ctx, RecorderOptions{
		Store: store, SessionID: recorder.SessionID(), RunID: "other-run", Actor: recorder.actor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.StartRun(ctx, nil); err != nil {
		t.Fatal(err)
	}
	view, err := store.LoadRun(ctx, recorder.SessionID(), recorder.RunID())
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Lanes) != 1 || view.Lanes[0].RunID != recorder.RunID() {
		t.Fatalf("run view contains sibling Run: %#v", view.Lanes)
	}
	inspection := InspectRun(view)
	if len(inspection.TerminalEvents) != 1 || inspection.TerminalEvents[0].ID != runCompleted.ID {
		t.Fatalf("terminal events = %#v", inspection.TerminalEvents)
	}
	if len(inspection.LinkedCheckpoints) != 1 || inspection.LinkedCheckpoints[0].CheckpointID != checkpointLinked.Payload["checkpoint_id"] {
		t.Fatalf("checkpoint links = %#v", inspection.LinkedCheckpoints)
	}
	if inspection.LinkedCheckpoints[0].Event.Seq+1 != inspection.TerminalEvents[0].Seq ||
		inspection.LinkedCheckpoints[0].Event.CommittedAt != inspection.TerminalEvents[0].CommittedAt {
		t.Fatalf("completion events were not one append: %#v", inspection)
	}
	if len(inspection.UnresolvedAttempts) != 1 || inspection.UnresolvedAttempts[0].AttemptID != unresolved.AttemptID {
		t.Fatalf("unresolved attempts = %#v", inspection.UnresolvedAttempts)
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
