package observation

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/compforge/agent-ledger/go/common"
)

func jobFixture(t *testing.T, name string) JobView {
	t.Helper()
	raw, err := os.ReadFile("../../conformance/vectors/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var view JobView
	if err = json.Unmarshal(raw, &view); err != nil {
		t.Fatal(err)
	}
	return view
}
func TestJobContract(t *testing.T) {
	for name, gaps := range map[string]int{"job-review.json": 0, "job-input.json": 1} {
		t.Run(name, func(t *testing.T) {
			view := jobFixture(t, name)
			if err := view.Validate(); err != nil {
				t.Fatal(err)
			}
			issues, err := view.Inspect()
			if err != nil || len(issues.Issues) != gaps {
				t.Fatalf("issues %v, %v", issues, err)
			}
			raw, _ := os.ReadFile("../../conformance/vectors/" + name)
			encoded, err := json.Marshal(view)
			if err != nil {
				t.Fatal(err)
			}
			var a, b any
			_ = json.Unmarshal(raw, &a)
			_ = json.Unmarshal(encoded, &b)
			if !reflect.DeepEqual(a, b) {
				t.Fatalf("roundtrip changed facts: %s", encoded)
			}
		})
	}
}
func TestJobReferenceValidation(t *testing.T) {
	view := jobFixture(t, "job-review.json")
	group := view.Nodes[1].(*Operation)
	group.Nodes = append(group.Nodes, group.Nodes[0])
	if view.Validate() == nil {
		t.Fatal("duplicate Session node accepted")
	}
	view = jobFixture(t, "job-review.json")
	group = view.Nodes[1].(*Operation)
	group.Nodes[1].(*ToolCall).ModelCallID = "t1"
	if view.Validate() == nil {
		t.Fatal("non-model target accepted")
	}
	view = jobFixture(t, "job-input.json")
	view.Relations = append(view.Relations, Relation{Kind: "depends_on", Source: NodeRef{ID: "assess"}, Target: NodeRef{ID: "missing", Session: &common.SessionRef{Source: "pi", ID: "other"}}})
	issues, err := view.Inspect()
	if err != nil || len(issues.Issues) != 2 {
		t.Fatalf("partial references %v %v", issues, err)
	}
	view.Relations[1].Target.Session = nil
	if view.Validate() == nil {
		t.Fatal("missing local node accepted")
	}
}
func TestSessionRejectsForeignItems(t *testing.T) {
	view := SessionView{SchemaVersion: SessionSchemaVersion, Session: common.SessionRef{Source: "pi", ID: "s"}, Nodes: Nodes{&ModelCall{Node: Node{ID: "m", Session: &common.SessionRef{Source: "pi", ID: "other"}}}}}
	if view.Validate() == nil {
		t.Fatal("foreign session accepted")
	}
}
func TestJobCompletionAndSharedCostAreSourceFacts(t *testing.T) {
	a, b := jobFixture(t, "job-input.json"), jobFixture(t, "job-input.json")
	b.Job.ID = "input-b"
	if !reflect.DeepEqual(a.Nodes, b.Nodes) {
		t.Fatal("shared calls changed")
	}
	a.Goal = json.RawMessage("null")
	a.Result = json.RawMessage("null")
	raw, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	var restored JobView
	if err = json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if string(restored.Result) != "null" {
		t.Fatal("explicit null lost")
	}
	a.StartedAt = "2026-10-26T01:00:00Z"
	a.FinishedAt = "2026-10-26T00:00:00Z"
	if a.Validate() == nil {
		t.Fatal("reverse job timing accepted")
	}
	a.SchemaVersion = "agent-ledger/job-view@1"
	if a.Validate() == nil {
		t.Fatal("old schema accepted")
	}
}
