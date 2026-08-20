package agentledger

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestCoreVocabularyMatchesRegistry(t *testing.T) {
	encoded, err := os.ReadFile("../spec/vocabulary.json")
	if err != nil {
		t.Fatal(err)
	}
	var registry struct {
		ActionTypes []string `json:"action_types"`
		EventTypes  []string `json:"event_types"`
	}
	if err := json.Unmarshal(encoded, &registry); err != nil {
		t.Fatal(err)
	}
	actionTypes := []string{
		ActionTypeModelCall, ActionTypeToolCall, ActionTypeCompact, ActionTypeCheckpoint,
	}
	eventTypes := []string{
		EventTypeSessionStarted, EventTypeSessionCompleted,
		EventTypeRunStarted, EventTypeRunCompleted, EventTypeRunFailed, EventTypeRunCancelled,
		EventTypeLaneCreated,
		EventTypeTurnStarted, EventTypeTurnCompleted, EventTypeTurnFailed,
		EventTypeActionStarted, EventTypeActionCompleted, EventTypeActionFailed,
		EventTypeAttemptRequested, EventTypeAttemptCompleted, EventTypeAttemptFailed,
		EventTypeLaneFrameworkSnapshotSaved, EventTypeLaneFrameworkCheckpointLinked,
	}
	if !reflect.DeepEqual(actionTypes, registry.ActionTypes) {
		t.Fatalf("action types = %v, registry = %v", actionTypes, registry.ActionTypes)
	}
	if !reflect.DeepEqual(eventTypes, registry.EventTypes) {
		t.Fatalf("event types = %v, registry = %v", eventTypes, registry.EventTypes)
	}
}
