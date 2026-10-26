package observation

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	view "github.com/compforge/agent-ledger/go/observation"
	"github.com/compforge/agentgo"
	"github.com/compforge/agentgo/session"
)

func recording(t *testing.T, records ...session.Record) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i, record := range records {
		record.Seq = uint64(i + 1)
		if record.SessionID == "" {
			record.SessionID = "session"
		}
		record.Timestamp = time.Date(2026, 1, 2, 3, 4, i, 0, time.UTC)
		if err := enc.Encode(record); err != nil {
			t.Fatal(err)
		}
	}
	return buf.Bytes()
}
func TestSessionAcrossRunsAndIncompleteEvidence(t *testing.T) {
	execution := &agentgo.Execution{ID: "call", Kind: agentgo.ExecutionKindModel, Attempt: 1}
	records := []session.Record{
		{Type: session.SessionStart, Data: json.RawMessage(`{"version":1}`)},
		{Type: string(agentgo.EventModelExecStart), RunID: "run-1", Execution: execution, Data: json.RawMessage(`{}`)},
	}
	prefix, err := ReadSessionView(bytes.NewReader(recording(t, records...)), nil)
	if err != nil {
		t.Fatal(err)
	}
	first := prefix.Nodes[1].(*view.ModelCall)
	if first.Response != nil || first.Status == "completed" {
		t.Fatal("EOF manufactured a completion")
	}
	records = append(records,
		session.Record{Type: string(agentgo.EventModelExecEnd), RunID: "run-1", Execution: execution, Data: json.RawMessage(`{"error":"provider failed","partial_message":{"text":"partial"}}`)},
		session.Record{Type: string(agentgo.EventModelExecStart), RunID: "run-2", Execution: execution, Data: json.RawMessage(`{}`)},
		session.Record{Type: string(agentgo.EventModelExecEnd), RunID: "run-2", Execution: execution, Data: json.RawMessage(`{"message":{"text":"done"}}`)},
	)
	raw := recording(t, records...)
	full, err := ReadSessionView(bytes.NewReader(append(raw, []byte(`{"unfinished":`)...)), nil)
	if err != nil {
		t.Fatal(err)
	}
	a, b := full.Nodes[1].(*view.ModelCall), full.Nodes[2].(*view.ModelCall)
	if a.ID != first.ID || a.ID == b.ID || a.Status != "failed" || b.Status != "completed" {
		t.Fatalf("bad call identities/outcomes: %#v %#v", a, b)
	}
	if string(a.Response.Output) != `{"text":"partial"}` || len(a.Evidence) != 2 {
		t.Fatal("partial response or evidence lost")
	}
	if full.Session.ID != "session" || !strings.Contains(strings.Join(full.Issues, " "), "unterminated") {
		t.Fatalf("bad session: %#v", full)
	}
}

func TestMixedSessionsRejected(t *testing.T) {
	raw := recording(t, session.Record{Type: session.SessionStart, Data: json.RawMessage(`{"version":1}`)}, session.Record{Type: session.RunStart, SessionID: "other", RunID: "run"})
	if _, err := ReadSessionView(bytes.NewReader(raw), nil); err == nil {
		t.Fatal("mixed sessions accepted")
	}
}

func TestNativeMessagesAndUsage(t *testing.T) {
	c, err := agentgo.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	user, err := c.Marshal(agentgo.UserMsg("followup"))
	if err != nil {
		t.Fatal(err)
	}
	assistant, err := c.Marshal(agentgo.Message{Role: agentgo.RoleAssistant, StopReason: agentgo.StopReasonStop, Usage: &agentgo.Usage{Input: 0, Output: 2, TotalTokens: 2}})
	if err != nil {
		t.Fatal(err)
	}
	userData, _ := json.Marshal(session.EventData{Message: user})
	modelData, _ := json.Marshal(session.EventData{Message: assistant})
	raw := recording(t, session.Record{Type: session.SessionStart, Data: json.RawMessage(`{"version":1}`)},
		session.Record{Type: string(agentgo.EventMessageEnd), RunID: "r", Data: userData},
		session.Record{Type: string(agentgo.EventModelExecEnd), RunID: "r", Execution: &agentgo.Execution{ID: "m", Kind: agentgo.ExecutionKindModel, Attempt: 1}, Data: modelData})
	observed, err := ReadSessionView(bytes.NewReader(raw), c)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Nodes[1].Kind() != "input" {
		t.Fatal("user input not represented")
	}
	model := observed.Nodes[2].(*view.ModelCall)
	if model.Response.Usage == nil || *model.Response.Usage.InputTokens != 0 {
		t.Fatal("zero usage not preserved")
	}
	if model.StartedAt != "" {
		t.Fatal("orphan completion synthesized a start")
	}
}
