package agentledger

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
)

func TestEventStreamCanSpanSemanticRuns(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEventStore()
	stream := EventStream{SessionID: "session", StreamID: "framework/pi/native"}
	first := NewEvent("framework.state.recorded", "session", "run-1", Actor{Type: "agent", ID: "pi"})
	second := NewEvent("framework.state.recorded", "session", "run-2", Actor{Type: "agent", ID: "pi"})
	if _, err := store.Append(ctx, stream, -1, "append-1", first); err != nil {
		t.Fatalf("append first: %v", err)
	}
	if _, err := store.Append(ctx, stream, 0, "append-2", second); err != nil {
		t.Fatalf("append second: %v", err)
	}

	var runs []string
	for event, err := range store.Load(ctx, stream, -1) {
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		runs = append(runs, event.RunID)
	}
	if len(runs) != 2 || runs[0] != "run-1" || runs[1] != "run-2" {
		t.Fatalf("runs = %v", runs)
	}
}

func TestAppendDigestMatchesCrossLanguageVector(t *testing.T) {
	data, err := os.ReadFile("../conformance/vectors/append.json")
	if err != nil {
		t.Fatalf("read vector: %v", err)
	}
	var vector struct {
		Events []ProposedEvent `json:"events"`
		SHA256 string          `json:"sha256"`
	}
	if err := json.Unmarshal(data, &vector); err != nil {
		t.Fatalf("decode vector: %v", err)
	}
	digest, err := CanonicalAppendDigest(vector.Events)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if digest != vector.SHA256 {
		t.Fatalf("digest = %s, want %s", digest, vector.SHA256)
	}
}

func TestEventIDUniquenessIsScopedToSession(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEventStore()
	first := NewEvent("test.recorded", "session-1", "run-1", Actor{Type: "agent", ID: "test"})
	first.EventID = "shared-event-id"
	second := first
	second.SessionID = "session-2"
	second.RunID = "run-2"

	if _, err := store.Append(ctx, EventStream{SessionID: "session-1", StreamID: "run-1"}, -1, "append-1", first); err != nil {
		t.Fatalf("append first session: %v", err)
	}
	if _, err := store.Append(ctx, EventStream{SessionID: "session-2", StreamID: "run-2"}, -1, "append-2", second); err != nil {
		t.Fatalf("append second session: %v", err)
	}
}

func TestDuplicateEventIDRejectsWholeBatch(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEventStore()
	stream := EventStream{SessionID: "session", StreamID: "run"}
	first := NewEvent("test.recorded", "session", "run", Actor{Type: "agent", ID: "test"})
	second := first
	second.EventType = "test.second"

	if _, err := store.Append(ctx, stream, -1, "append", first, second); !errors.Is(err, ErrDuplicateEvent) {
		t.Fatalf("append error = %v, want ErrDuplicateEvent", err)
	}
	for event, err := range store.Load(ctx, stream, -1) {
		t.Fatalf("unexpected stored event %#v, error %v", event, err)
	}
}

func TestCommittedEventContentIsImmutable(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEventStore()
	stream := EventStream{SessionID: "session", StreamID: "run"}
	event := NewEvent("test.recorded", "session", "run", Actor{Type: "agent", ID: "test"})
	event.Payload["nested"] = map[string]any{"value": float64(1)}

	if _, err := store.Append(ctx, stream, -1, "append", event); err != nil {
		t.Fatalf("append: %v", err)
	}
	event.Payload["nested"].(map[string]any)["value"] = float64(2)

	stored := loadEvents(t, store, stream)
	stored[0].Payload["nested"].(map[string]any)["value"] = float64(3)
	reloaded := loadEvents(t, store, stream)
	if got := reloaded[0].Payload["nested"].(map[string]any)["value"]; got != float64(1) {
		t.Fatalf("committed payload value = %v, want 1", got)
	}
}

func loadEvents(t *testing.T, store EventStore, stream EventStream) []StoredEvent {
	t.Helper()
	var events []StoredEvent
	for event, err := range store.Load(context.Background(), stream, -1) {
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		events = append(events, event)
	}
	return events
}

func TestResumeRecorderRejectsExpectedVersion(t *testing.T) {
	expectedVersion := int64(0)
	_, err := ResumeRecorder(context.Background(), RecorderOptions{
		Store: NewMemoryEventStore(), ExpectedVersion: &expectedVersion,
	})
	if err == nil {
		t.Fatal("resume accepted an explicit expected version")
	}
}

func TestOrchestratorLinksMultipleAgentRuns(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEventStore()
	orchestrator := NewSessionRecorder(RecorderOptions{
		Store: store, SessionID: "session", RunID: "orchestrator-run",
		Actor: Actor{Type: "orchestrator", ID: "planner"},
	})
	if _, err := orchestrator.StartRun(ctx, nil); err != nil {
		t.Fatalf("start orchestrator: %v", err)
	}
	for _, role := range []string{"researcher", "reviewer"} {
		dispatch, err := orchestrator.Record(
			ctx, "orchestration.agent.dispatched", map[string]any{"role": role}, "", "",
		)
		if err != nil {
			t.Fatalf("record %s dispatch: %v", role, err)
		}
		child := orchestrator.Child(role+"-run", Actor{Type: "agent", ID: role}, dispatch.EventID)
		if _, err := child.StartRun(ctx, nil); err != nil {
			t.Fatalf("start %s: %v", role, err)
		}
	}

	var childRuns []string
	for event, err := range store.ScanSession(ctx, "session", "") {
		if err != nil {
			t.Fatalf("scan session: %v", err)
		}
		if event.ParentRunID == "" {
			continue
		}
		if event.ParentRunID != "orchestrator-run" || event.CausedByEventID == "" {
			t.Fatalf("invalid causal edge: %#v", event.ProposedEvent)
		}
		childRuns = append(childRuns, event.RunID)
	}
	if len(childRuns) != 2 || childRuns[0] != "researcher-run" || childRuns[1] != "reviewer-run" {
		t.Fatalf("child runs = %v", childRuns)
	}
}
