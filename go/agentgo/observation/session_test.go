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

// +case=`Middleware can complete a tool without invoking its function; ToolCall follows the complete execution boundary and retains its disposition`
func TestToolExecutionIncludesShortCircuitAndInvocation(t *testing.T) {
	execution := &agentgo.Execution{ID: "tool", Kind: agentgo.ExecutionKindTool, Attempt: 1}
	event := func(kind agentgo.EventType, data string) session.Record {
		return session.Record{Type: string(kind), RunID: "run", Execution: execution, Data: json.RawMessage(data)}
	}
	start := session.Record{Type: session.SessionStart, Data: json.RawMessage(`{"version":1}`)}
	for _, tc := range []struct {
		name         string
		records      []session.Record
		args, result string
		evidence     int
	}{
		{"middleware", []session.Record{
			event(agentgo.EventToolExecStart, `{"tool":"submit","tool_id":"t","args":{"input":1}}`),
			event(agentgo.EventToolExecEnd, `{"tool":"submit","tool_id":"t","disposition":"short_circuited","tool_result":{"content":"accepted"}}`),
		}, `{"input":1}`, "accepted", 2},
		{"invocation", []session.Record{
			event(agentgo.EventToolExecStart, `{"tool":"submit","tool_id":"t","args":{"input":1}}`),
			event(agentgo.EventToolInvokeStart, `{"tool":"submit","tool_id":"t","args":{"input":2}}`),
			event(agentgo.EventToolInvokeEnd, `{"tool":"submit","tool_id":"t","tool_result":{"content":"inner"}}`),
			event(agentgo.EventToolExecEnd, `{"tool":"submit","tool_id":"t","disposition":"executed","tool_result":{"content":"outer"}}`),
		}, `{"input":2}`, "outer", 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observed, err := ReadSessionView(bytes.NewReader(recording(t, append([]session.Record{start}, tc.records...)...)), nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(observed.Nodes) != 2 {
				t.Fatalf("duplicated tool: %d", len(observed.Nodes))
			}
			call, ok := observed.Nodes[1].(*view.ToolCall)
			if !ok || call.Status != "completed" || string(call.Request.Input) != tc.args || !strings.Contains(string(call.Response.Output), tc.result) || len(call.Evidence) != tc.evidence {
				t.Fatalf("call=%+v", call)
			}
			if call.Attributes["disposition"] == nil || len(observed.Issues) > 0 {
				t.Fatalf("missing disposition or false gap: %+v", observed)
			}
		})
	}
	// Invocation success alone does not finalize the outer tool pipeline.
	raw := recording(t, start, event(agentgo.EventToolExecStart, `{"tool":"submit","args":{}}`), event(agentgo.EventToolInvokeEnd, `{"tool_result":{"content":"inner"}}`))
	partial, err := ReadSessionView(bytes.NewReader(raw), nil)
	if err != nil {
		t.Fatal(err)
	}
	tool := partial.Nodes[1].(*view.ToolCall)
	if tool.Response != nil || tool.FinishedAt != "" || len(partial.Issues) == 0 {
		t.Fatal("inner invocation finalized unfinished execution")
	}
}

// +case=`A malformed record after a valid Session prefix preserves native ownership and allows already observed calls to be selected into a Job`
func TestCorruptSuffixPreservesSelectablePrefix(t *testing.T) {
	execution := &agentgo.Execution{ID: "model", Kind: agentgo.ExecutionKindModel, Attempt: 1}
	raw := recording(t, session.Record{Type: session.SessionStart, Data: json.RawMessage(`{"version":1}`)},
		session.Record{Type: string(agentgo.EventModelExecStart), RunID: "r", Execution: execution}) // lifecycle Data may be absent
	raw = append(raw, []byte("{broken}\n")...)
	observed, err := ReadSessionView(bytes.NewReader(raw), nil)
	if err == nil || observed.Session.ID != "session" || observed.Session.Source != "agentgo" || len(observed.Issues) == 0 {
		t.Fatalf("prefix=%+v error=%v", observed, err)
	}
	call := observed.Nodes[1].(*view.ModelCall)
	selected, err := view.SelectNodes(observed, []string{call.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || selected[0].Base().Session.ID != "session" || call.Response != nil {
		t.Fatal("lost incomplete call or Session ownership")
	}
}
