package ledgerview

import (
	"context"
	"encoding/json"
	"errors"

	"os"
	"reflect"
	"strings"
	"testing"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agent-ledger/go/common"
	"github.com/compforge/agent-ledger/go/view"
)

func TestProjectViewKeepsRetriesAndParallelLanes(t *testing.T) {
	raw, err := os.ReadFile("../../../conformance/vectors/view-ledger.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Source   agentledger.RunView `json:"source"`
		CallIDs  []string            `json:"call_ids"`
		Statuses []string            `json:"statuses"`
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(fixture.Source)
	document, err := Project(fixture.Source)
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

func TestTypedFailureRoundTrip(t *testing.T) {
	raw, err := os.ReadFile("../../../conformance/vectors/common-call.json")
	if err != nil {
		t.Fatal(err)
	}
	var vector map[string]map[string]any
	if err = json.Unmarshal(raw, &vector); err != nil {
		t.Fatal(err)
	}
	request, err := common.DecodeRequest(vector["request"])
	if err != nil {
		t.Fatal(err)
	}
	response, err := common.DecodeResponse(vector["response"])
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	recorder, err := agentledger.OpenRecorder(ctx, agentledger.RecorderOptions{Store: store, SessionID: "s", RunID: "r", Actor: agentledger.NewActor("agent", "test")})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := recorder.StartTurn(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := recorder.BeforeModelCall(ctx, turn.ID, request, vector["details"])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.ModelFailed(ctx, attempt, errors.New("fallback"), response, vector["details"]); err != nil {
		t.Fatal(err)
	}
	source, err := store.LoadRun(ctx, "s", "r")
	if err != nil {
		t.Fatal(err)
	}
	document, err := Project(source)
	if err != nil {
		t.Fatal(err)
	}
	var call *view.ModelCall
	for _, node := range document.Runs[0].Items {
		if turn, ok := node.(*view.Turn); ok {
			for _, item := range turn.Items {
				if model, ok := item.(*view.ModelCall); ok {
					call = model
				}
			}
		}
	}
	if call == nil || call.Status != "failed" {
		t.Fatal("failed call missing")
	}
	actual, _ := json.Marshal(call.Response)
	expected, _ := json.Marshal(vector["response"])
	var a, b any
	_ = json.Unmarshal(actual, &a)
	_ = json.Unmarshal(expected, &b)
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("response changed: %s", actual)
	}
	details := call.Attributes["event_details"].(map[string]any)
	if len(details) != 2 {
		t.Fatal("source details missing")
	}
	for _, value := range details {
		if !reflect.DeepEqual(value, vector["details"]) {
			t.Fatalf("details changed: %#v", value)
		}
	}
}
