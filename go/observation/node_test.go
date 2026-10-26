package observation

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/compforge/agent-ledger/go/common"
)

func selectionSession(t *testing.T) SessionView {
	t.Helper()
	raw, err := os.ReadFile("../../conformance/vectors/session-view.json")
	if err != nil {
		t.Fatal(err)
	}
	var session SessionView
	if err := json.Unmarshal(raw, &session); err != nil {
		t.Fatal(err)
	}
	return session
}

// +case:id=node_polymorphism,desc=`Concrete node fields survive encoding and selection`
func TestConcreteNodeRoundTrip(t *testing.T) {
	view := SessionView{
		SchemaVersion: SessionSchemaVersion,
		Session:       common.SessionRef{Source: "test", ID: "s"},
		Nodes: Nodes{&Operation{Node: Node{ID: "op"}, Operation: "check", Data: json.RawMessage("null"), Nodes: Nodes{
			&Turn{Node: Node{ID: "turn"}, Nodes: Nodes{
				&Input{Node: Node{ID: "i"}, Content: nil},
				&ModelCall{Node: Node{ID: "m"}, Request: &common.CallRequest{Input: json.RawMessage(`{"large":9007199254740993}`)}},
				&ToolCall{Node: Node{ID: "t"}, ModelCallID: "m", Request: &common.CallRequest{ToolName: "read", Input: json.RawMessage("null")}},
			}},
		}}},
	}
	raw, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	var restored SessionView
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if err := restored.Validate(); err != nil {
		t.Fatal(err)
	}
	selected, err := SelectNodes(restored, []string{"op"})
	if err != nil {
		t.Fatal(err)
	}
	operation := selected[0].(*Operation)
	turn := operation.Nodes[0].(*Turn)
	if _, ok := turn.Nodes[0].(*Input); !ok {
		t.Fatal("Input type lost")
	}
	if _, ok := turn.Nodes[1].(*ModelCall); !ok {
		t.Fatal("ModelCall type lost")
	}
	if _, ok := turn.Nodes[2].(*ToolCall); !ok {
		t.Fatal("ToolCall type lost")
	}
	encoded, err := json.Marshal(selected)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"large":9007199254740993`) || !strings.Contains(string(encoded), `"data":null`) {
		t.Fatalf("specialized JSON facts changed: %s", encoded)
	}
}

func TestSelectNodesOwnershipOrderAndIsolation(t *testing.T) {
	session := selectionSession(t)
	before, _ := json.Marshal(session)
	selected, err := SelectNodes(session, []string{"t1", "turn-1", "input-1", "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || selected[0].Base().ID != "input-1" || selected[1].Base().ID != "turn-1" {
		t.Fatal("source order or overlap changed")
	}
	var check func(Nodes)
	check = func(nodes Nodes) {
		for _, n := range nodes {
			if n.Base().Session == nil || *n.Base().Session != session.Session {
				t.Fatal("ownership missing")
			}
			check(children(n))
		}
	}
	check(selected)
	job := JobView{SchemaVersion: JobSchemaVersion, Job: JobRef{Source: "test", ID: "j"}, Nodes: selected}
	result, err := job.Inspect()
	if err != nil || len(result.Issues) != 0 {
		t.Fatalf("inspection: %v %v", result, err)
	}
	selected[1].(*Turn).Nodes[0].(*ModelCall).Request.Input = json.RawMessage(`"changed"`)
	selected[0].Base().Session.ID = "changed"
	after, _ := json.Marshal(session)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("selection mutated source")
	}
	if selected[1].Base().Session.ID != session.Session.ID {
		t.Fatal("selected nodes share mutable identity")
	}
	if _, err := SelectNodes(session, []string{"missing"}); err == nil {
		t.Fatal("missing ID accepted")
	}
	empty, err := SelectNodes(session, nil)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty selection: %v %v", empty, err)
	}
	fragment, err := SelectNodes(session, []string{"t1"})
	if err != nil {
		t.Fatal(err)
	}
	job.Nodes = fragment
	result, err = job.Inspect()
	if err != nil || len(fragment) != 1 || len(result.Issues) != 1 {
		t.Fatalf("unselected model silently included: %v %v", result, err)
	}
}
