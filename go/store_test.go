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
