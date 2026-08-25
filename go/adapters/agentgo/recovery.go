package agentgoadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentgo"
)

// ReplayResult is an AgentGo-native transcript plus the exact Ledger prefix
// represented by it. The caller owns checkpoint persistence and continuation.
type ReplayResult struct {
	Messages         []agentgo.AgentMessage
	Anchor           *agentledger.CheckpointAnchor
	ReplayedMessages int
}

type durableToolOutcome struct {
	call   agentgo.ToolCall
	result agentgo.ToolResult
}

// Replay folds recorded model and tool outcomes after anchor into an AgentGo
// transcript without calling either the model or a tool again.
//
// Replay is intentionally narrower than general Ledger replay: it currently
// understands only terminal model/tool Attempt outcomes representable as
// AgentGo messages. It does not replay arbitrary events or operators such as
// compaction, and it never re-executes an Action or its external side effects.
//
// Replay stops before the first unresolved or unknowable Attempt. This keeps
// Anchor a contiguous prefix: agentd can persist the returned state, then make
// a separate retry or user-confirmation decision for the remaining Attempt.
func (a *Adapter) Replay(
	ctx context.Context,
	messages []agentgo.AgentMessage,
	anchor *agentledger.CheckpointAnchor,
) (ReplayResult, error) {
	result := ReplayResult{
		Messages: append([]agentgo.AgentMessage(nil), messages...),
		Anchor:   cloneAnchor(anchor),
	}
	lane := a.runtimeRecorder.Lane()
	if anchor != nil {
		if anchor.LaneID != lane.ID {
			return ReplayResult{}, fmt.Errorf("agentgo checkpoint anchor lane %s does not match runtime lane %s", anchor.LaneID, lane.ID)
		}
		if anchor.LastAppliedSeq < 0 {
			return ReplayResult{}, errors.New("agentgo checkpoint anchor sequence must be non-negative")
		}
	}

	view, err := a.runtimeRecorder.Store().LoadRun(ctx, lane.SessionID, lane.RunID)
	if err != nil {
		return ReplayResult{}, fmt.Errorf("load agentgo run for recovery: %w", err)
	}
	actions := make(map[string]agentledger.Action, len(view.Actions))
	for _, action := range view.Actions {
		actions[action.ID] = action
	}
	attempts := make(map[string]agentledger.Attempt, len(view.Attempts))
	for _, attempt := range view.Attempts {
		attempts[attempt.ID] = attempt
	}
	terminal := terminalEvents(view.Events, lane.ID)
	requested := requestedEvents(view.Events, lane.ID)

	afterSeq := int64(0)
	if anchor != nil {
		afterSeq = anchor.LastAppliedSeq
	}
	toolOutcomes := make(map[string]durableToolOutcome)
	for event, loadErr := range a.runtimeRecorder.Store().LoadLane(ctx, lane.ID, afterSeq) {
		if loadErr != nil {
			return ReplayResult{}, fmt.Errorf("load agentgo recovery tail: %w", loadErr)
		}
		if event.EventType == agentledger.EventTypeAttemptRequested {
			outcome, ok := terminal[event.SubjectID]
			if !ok {
				break
			}
			if outcome.EventType == agentledger.EventTypeAttemptCancelled ||
				outcome.EventType == agentledger.EventTypeAttemptOutcomeUnknown {
				return ReplayResult{}, fmt.Errorf(
					"attempt %s has non-replayable terminal outcome %s", event.SubjectID, outcome.EventType,
				)
			}
		}

		switch event.EventType {
		case agentledger.EventTypeAttemptCompleted:
			action, err := actionForAttempt(event.SubjectID, attempts, actions)
			if err != nil {
				return ReplayResult{}, err
			}
			switch action.Type {
			case agentledger.ActionTypeModelCall:
				message, err := decodeModelOutput(event.Payload)
				if err != nil {
					return ReplayResult{}, fmt.Errorf("replay model attempt %s: %w", event.SubjectID, err)
				}
				result.Messages = append(result.Messages, message)
				result.ReplayedMessages++
			case agentledger.ActionTypeToolCall:
				outcome, err := decodeToolOutcome(action, requested[event.SubjectID], event, false)
				if err != nil {
					return ReplayResult{}, err
				}
				toolOutcomes[action.Key] = outcome
				result.ReplayedMessages += a.flushToolOutcomes(&result.Messages, toolOutcomes)
			}
		case agentledger.EventTypeAttemptFailed:
			action, err := actionForAttempt(event.SubjectID, attempts, actions)
			if err != nil {
				return ReplayResult{}, err
			}
			if action.Type == agentledger.ActionTypeToolCall {
				outcome, err := decodeToolOutcome(action, requested[event.SubjectID], event, true)
				if err != nil {
					return ReplayResult{}, err
				}
				toolOutcomes[action.Key] = outcome
				result.ReplayedMessages += a.flushToolOutcomes(&result.Messages, toolOutcomes)
			}
		}
		result.Anchor = &agentledger.CheckpointAnchor{
			LaneID: lane.ID, LastAppliedSeq: event.Seq, LastAppliedEventID: event.ID,
		}
	}
	if len(toolOutcomes) > 0 {
		return ReplayResult{}, errors.New("durable tool outcome has no preceding unanswered AgentGo tool call")
	}
	return result, nil
}

func terminalEvents(events []agentledger.StoredEvent, laneID string) map[string]agentledger.StoredEvent {
	terminal := make(map[string]agentledger.StoredEvent)
	for _, event := range events {
		if event.LaneID != laneID {
			continue
		}
		switch event.EventType {
		case agentledger.EventTypeAttemptCompleted, agentledger.EventTypeAttemptFailed,
			agentledger.EventTypeAttemptCancelled, agentledger.EventTypeAttemptOutcomeUnknown:
			terminal[event.SubjectID] = event
		}
	}
	return terminal
}

func requestedEvents(events []agentledger.StoredEvent, laneID string) map[string]agentledger.StoredEvent {
	requested := make(map[string]agentledger.StoredEvent)
	for _, event := range events {
		if event.LaneID == laneID && event.EventType == agentledger.EventTypeAttemptRequested {
			requested[event.SubjectID] = event
		}
	}
	return requested
}

func actionForAttempt(
	attemptID string,
	attempts map[string]agentledger.Attempt,
	actions map[string]agentledger.Action,
) (agentledger.Action, error) {
	attempt, ok := attempts[attemptID]
	if !ok {
		return agentledger.Action{}, fmt.Errorf("agentgo recovery attempt %s is missing", attemptID)
	}
	action, ok := actions[attempt.ActionID]
	if !ok {
		return agentledger.Action{}, fmt.Errorf("agentgo recovery action %s is missing", attempt.ActionID)
	}
	return action, nil
}

func decodeModelOutput(payload map[string]any) (agentgo.Message, error) {
	output, ok := payload["output"]
	if !ok {
		if _, artifact := payload["output_artifact_id"]; artifact {
			return agentgo.Message{}, errors.New("artifact-backed model output requires caller materialization")
		}
		return agentgo.Message{}, errors.New("completed model attempt is missing output")
	}
	encoded, err := json.Marshal(output)
	if err != nil {
		return agentgo.Message{}, fmt.Errorf("encode output: %w", err)
	}
	var message agentgo.Message
	if err := json.Unmarshal(encoded, &message); err != nil {
		return agentgo.Message{}, fmt.Errorf("decode output: %w", err)
	}
	return message, nil
}

func decodeToolOutcome(
	action agentledger.Action,
	request agentledger.StoredEvent,
	outcome agentledger.StoredEvent,
	failed bool,
) (durableToolOutcome, error) {
	if action.Key == "" {
		return durableToolOutcome{}, fmt.Errorf("tool action %s is missing its tool call key", action.ID)
	}
	call := agentgo.ToolCall{ID: action.Key}
	if name, ok := request.Payload["tool_name"].(string); ok {
		call.Name = name
	}
	if input, ok := request.Payload["input"]; ok {
		encoded, err := json.Marshal(input)
		if err != nil {
			return durableToolOutcome{}, fmt.Errorf("encode tool action %s input: %w", action.ID, err)
		}
		call.Args = encoded
	}

	result := agentgo.ToolResult{ToolCallID: action.Key, ToolName: call.Name, IsError: failed}
	if failed {
		failure, ok := outcome.Payload["error"].(map[string]any)
		if !ok {
			return durableToolOutcome{}, fmt.Errorf("failed tool attempt %s is missing structured error", outcome.SubjectID)
		}
		message, _ := failure["message"].(string)
		if message == "" {
			message = "tool execution failed"
		}
		result.Content, _ = json.Marshal(message)
		result.Details = failure
	} else {
		output, ok := outcome.Payload["output"]
		if !ok {
			if _, artifact := outcome.Payload["output_artifact_id"]; artifact {
				return durableToolOutcome{}, fmt.Errorf("artifact-backed tool output for action %s requires caller materialization", action.ID)
			}
			return durableToolOutcome{}, fmt.Errorf("completed tool attempt %s is missing output", outcome.SubjectID)
		}
		encoded, err := json.Marshal(output)
		if err != nil {
			return durableToolOutcome{}, fmt.Errorf("encode tool action %s output: %w", action.ID, err)
		}
		result.Content = encoded
	}
	return durableToolOutcome{call: call, result: result}, nil
}

func (a *Adapter) flushToolOutcomes(
	messages *[]agentgo.AgentMessage,
	outcomes map[string]durableToolOutcome,
) int {
	replayed := 0
	for _, call := range pendingToolCalls(*messages) {
		outcome, ok := outcomes[call.ID]
		if !ok {
			break
		}
		message := a.toolResultMessage(outcome.call, outcome.result)
		*messages = append(*messages, message)
		delete(outcomes, call.ID)
		replayed++
	}
	return replayed
}

func (a *Adapter) toolResultMessage(call agentgo.ToolCall, result agentgo.ToolResult) agentgo.AgentMessage {
	if a.toolResultFactory != nil {
		if message := a.toolResultFactory(call, result); message != nil {
			return message
		}
	}
	message := agentgo.ToolResultMsg(result.ToolCallID, result.Content, result.IsError)
	if result.ToolName != "" {
		message.Metadata["tool_name"] = result.ToolName
	}
	return message
}

func pendingToolCalls(messages []agentgo.AgentMessage) []agentgo.ToolCall {
	answered := make(map[string]bool)
	for _, message := range messages {
		model, include := message.ToMessage()
		if !include || model.Role != agentgo.RoleTool {
			continue
		}
		if id, ok := model.Metadata["tool_call_id"].(string); ok {
			answered[id] = true
		}
	}
	var pending []agentgo.ToolCall
	for _, message := range messages {
		model, include := message.ToMessage()
		if !include || model.Role != agentgo.RoleAssistant {
			continue
		}
		for _, call := range model.ToolCalls() {
			if !answered[call.ID] {
				pending = append(pending, call)
			}
		}
	}
	return pending
}

func cloneAnchor(anchor *agentledger.CheckpointAnchor) *agentledger.CheckpointAnchor {
	if anchor == nil {
		return nil
	}
	copy := *anchor
	return &copy
}
