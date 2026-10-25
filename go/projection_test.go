package agentledger

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/compforge/agent-ledger/go/view"
)

func TestProjectViewKeepsRetriesAndParallelLanes(t *testing.T) {
	raw, err := os.ReadFile("../conformance/vectors/view-ledger.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Source   RunView  `json:"source"`
		CallIDs  []string `json:"call_ids"`
		Statuses []string `json:"statuses"`
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(fixture.Source)
	document, err := ProjectView(fixture.Source)
	if err != nil {
		t.Fatal(err)
	}
	ids, statuses := []string{}, []string{}
	var calls []view.Item
	lanes := map[string]bool{}
	for _, item := range document.Runs[0].Items {
		if turn, ok := item.(*view.Turn); ok {
			lanes[turn.LaneID] = true
			for _, call := range turn.Items {
				if call.Kind() == "model_call" || call.Kind() == "tool_call" {
					calls = append(calls, call)
					ids = append(ids, call.Base().ID)
					status := call.Base().Status
					if status == "" {
						status = "unknown"
					}
					statuses = append(statuses, status)
				}
			}
		}
	}
	if !reflect.DeepEqual(ids, fixture.CallIDs) || !reflect.DeepEqual(statuses, fixture.Statuses) {
		t.Fatalf("calls %v statuses %v", ids, statuses)
	}
	if len(lanes) != 2 {
		t.Fatal("parallel Lanes lost")
	}
	if len(document.Issues) != 1 || !strings.Contains(document.Issues[0], "result missing") {
		t.Fatalf("issues: %v", document.Issues)
	}
	first := calls[0].(*view.ModelCall)
	if first.Response.Usage == nil || *first.Response.Usage.InputTokens != 8 {
		t.Fatal("failed call usage lost")
	}
	first.Request.Input = json.RawMessage(`{"changed":true}`)
	after, _ := json.Marshal(fixture.Source)
	if string(before) != string(after) {
		t.Fatal("projection changed Ledger")
	}
}
