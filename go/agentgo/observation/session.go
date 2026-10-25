// Package observation reads AgentGo native sessions without a Ledger Store.
package observation

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/compforge/agent-ledger/go/common"
	view "github.com/compforge/agent-ledger/go/observation"
	"github.com/compforge/agentgo"
	"github.com/compforge/agentgo/codec"
	"github.com/compforge/agentgo/session"
)

// ReadSessionView observes a native session prefix. Re-reading an extended prefix
// retains call IDs and fills outcomes; it never restores state or invokes a tool.
// +spec=`Call identity includes run, execution and attempt; EOF does not establish an outcome`
// +link=docs/observation.md
func ReadSessionView(reader io.Reader, messageCodec codec.Codec) (view.SessionView, error) {
	result := view.SessionView{SchemaVersion: view.SchemaVersion, Items: view.Items{}}
	if messageCodec == nil {
		var err error
		messageCodec, err = agentgo.NewCodec()
		if err != nil {
			return result, err
		}
	}
	decodeMessage := func(raw json.RawMessage, recordID string) agentgo.AgentMessage {
		if len(raw) == 0 {
			return nil
		}
		var message agentgo.AgentMessage
		if err := messageCodec.Unmarshal(raw, &message); err != nil {
			result.Issues = append(result.Issues, recordID+": message codec unavailable: "+err.Error())
			return nil
		}
		return message
	}
	calls := map[string]view.Item{}
	terminal := map[string]bool{}
	read, err := session.Read(reader, func(record session.Record) error {
		base := view.Node{ID: fmt.Sprintf("record:%d", record.Seq), RunID: record.RunID,
			Evidence:   []view.SourceRef{{Source: "agentgo", ID: fmt.Sprint(record.Seq)}},
			Attributes: map[string]any{"record_type": record.Type}}
		timestamp := record.Timestamp.Format(time.RFC3339Nano)
		// Preserve native records as evidence even where their semantics are not modeled.
		operation := func() {
			base.StartedAt = timestamp
			result.Items = append(result.Items, &view.Operation{Node: base, Operation: "agentgo." + record.Type, Data: record.Data})
		}
		kind := ""
		switch agentgo.EventType(record.Type) {
		case agentgo.EventMessageEnd:
			var data session.EventData
			if err := json.Unmarshal(record.Data, &data); err != nil {
				return fmt.Errorf("record %d: %w", record.Seq, err)
			}
			message := decodeMessage(data.Message, base.ID)
			if message != nil && message.GetRole() == agentgo.RoleUser {
				base.StartedAt = timestamp
				result.Items = append(result.Items, &view.Input{Node: base, Content: data.Message})
			} else {
				operation()
			}
			return nil
		case agentgo.EventModelExecStart, agentgo.EventModelRequest, agentgo.EventModelExecEnd:
			kind = "model_call"
		case agentgo.EventToolInvokeStart, agentgo.EventToolInvokeEnd:
			kind = "tool_call"
		default:
			operation()
			return nil
		}
		if record.Execution == nil || record.Execution.ID == "" || record.Execution.Attempt < 1 {
			operation()
			result.Issues = append(result.Issues, fmt.Sprintf("record %d: call identity not observed", record.Seq))
			return nil
		}
		var data session.EventData
		if err := json.Unmarshal(record.Data, &data); err != nil {
			return fmt.Errorf("record %d: %w", record.Seq, err)
		}
		keyJSON, _ := json.Marshal([]any{record.RunID, kind, record.Execution.ID, record.Execution.Attempt})
		key := string(keyJSON)
		call := calls[key]
		if call == nil {
			base.ID = "call:" + key
			base.Attributes["execution"] = *record.Execution
			base.Evidence = nil
			if kind == "model_call" {
				call = &view.ModelCall{Node: base}
			} else {
				call = &view.ToolCall{Node: base, ToolCallID: data.ToolID}
			}
			calls[key] = call
			result.Items = append(result.Items, call)
		}
		node := call.Base()
		node.Evidence = append(node.Evidence, view.SourceRef{Source: "agentgo", ID: fmt.Sprint(record.Seq)})
		node.Attributes[fmt.Sprintf("record:%d", record.Seq)] = record.Data
		var request **common.CallRequest
		var response **common.CallResponse
		switch c := call.(type) {
		case *view.ModelCall:
			request, response = &c.Request, &c.Response
		case *view.ToolCall:
			request, response = &c.Request, &c.Response
		}
		switch agentgo.EventType(record.Type) {
		case agentgo.EventModelExecStart:
			node.StartedAt = timestamp
		case agentgo.EventModelRequest:
			if data.ModelRequest != nil {
				input, err := json.Marshal(data.ModelRequest)
				if err != nil {
					return fmt.Errorf("record %d request: %w", record.Seq, err)
				}
				*request = &common.CallRequest{Input: input}
			}
		case agentgo.EventToolInvokeStart:
			node.StartedAt = timestamp
			*request = &common.CallRequest{ToolName: data.Tool, Input: data.Args}
		case agentgo.EventModelExecEnd, agentgo.EventToolInvokeEnd:
			node.FinishedAt = timestamp
			if terminal[key] {
				node.Status = "unknown"
				*response = nil
				result.Issues = append(result.Issues, "node "+node.ID+": multiple terminal observations")
				return nil
			}
			terminal[key] = true
			node.Status = "completed"
			outcome := &common.CallResponse{}
			if data.Error != "" || data.IsError {
				node.Status = "failed"
				outcome.Error = &common.CallError{Type: "agentgo", Message: data.Error}
			}
			if kind == "model_call" {
				outcome.Output = data.Message
				if len(data.PartialMessage) > 0 {
					outcome.Output = data.PartialMessage
				}
				if message, ok := decodeMessage(outcome.Output, node.ID).(agentgo.Message); ok {
					outcome.FinishReason = string(message.StopReason)
					if usage := message.Usage; usage != nil {
						input, output, read, write, total := int64(usage.Input), int64(usage.Output), int64(usage.CacheRead), int64(usage.CacheWrite), int64(usage.TotalTokens)
						outcome.Usage = &common.Usage{InputTokens: &input, OutputTokens: &output, CacheReadInputTokens: &read, CacheWriteInputTokens: &write, TotalTokens: &total}
						if usage.Model != "" {
							outcome.Model = &common.Model{ID: usage.Model, Provider: usage.Provider}
						}
					}
					if message.StopReason == agentgo.StopReasonAborted {
						node.Status = "cancelled"
					}
				}
			} else if data.ToolResult != nil {
				encoded, err := json.Marshal(data.ToolResult)
				if err != nil {
					return fmt.Errorf("record %d tool result: %w", record.Seq, err)
				}
				outcome.Output = encoded
			}
			*response = outcome
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	result.Session = common.SessionRef{Source: "agentgo", ID: read.SessionID}
	if read.TruncatedTail {
		result.Issues = append(result.Issues, "unterminated final record not observed")
	}
	// Traverse source order, not map iteration, so repeated snapshots are deterministic.
	for _, call := range result.Items {
		if call.Kind() != "model_call" && call.Kind() != "tool_call" {
			continue
		}
		if call.Base().StartedAt == "" {
			result.Issues = append(result.Issues, "node "+call.Base().ID+": start not observed")
		}
		if call.Base().FinishedAt == "" {
			result.Issues = append(result.Issues, "node "+call.Base().ID+": result not observed")
		}
	}
	return result, result.Validate()
}
