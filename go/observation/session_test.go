package observation

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestNativeViewRoundTrip(t *testing.T) {
	raw, err := os.ReadFile("../../conformance/vectors/session-view.json")
	if err != nil {
		t.Fatal(err)
	}
	var document SessionView
	if err = json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	if err = document.Validate(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	var before, after any
	if err = json.Unmarshal(raw, &before); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(encoded, &after); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("roundtrip changed source:\n%s", encoded)
	}
	turn := document.Nodes[1].(*Turn)
	tool := turn.Nodes[1].(*ToolCall)
	if string(tool.Request.Input) != "null" {
		t.Fatalf("null input lost: %s", tool.Request.Input)
	}
	tool.StartedAt = "2026-10-25T00:00:00Z"
	tool.FinishedAt = "2026-10-25T00:00:01Z"
	if len(tool.Sessions) != 1 {
		t.Fatal("session reference missing")
	}
	if err = document.Validate(); err != nil {
		t.Fatal(err)
	}
	tool.ModelCallID = "absent"
	if err = document.Validate(); err == nil {
		t.Fatal("dangling model call accepted")
	}
	tool.ModelCallID = "m1"
	tool.ID = turn.Nodes[0].Base().ID
	if err = document.Validate(); err == nil {
		t.Fatal("duplicate id accepted")
	}
}
