package agentgoadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentgo"
)

// ToolMiddleware records strict prewrite and terminal facts around the entire
// AgentGo tool pipeline. Known outcomes skip gates and external side effects
// when the native Loop is reconstructed after recovery.
func (a *Adapter) ToolMiddleware() agentgo.ToolMiddleware {
	return func(
		ctx context.Context,
		execution agentgo.ToolExecution,
		next agentgo.ToolExecuteFunc,
	) (agentgo.ToolResult, error) {
		semantics, err := validateToolSemantics(a.toolSemantics(execution.Call))
		if err != nil {
			return agentgo.ToolResult{}, err
		}
		payload, err := toolRequestPayload(execution, semantics)
		if err != nil {
			return agentgo.ToolResult{}, fmt.Errorf("fingerprint agentgo tool request: %w", err)
		}
		key := a.actionKey(execution.Execution)
		if record, found := a.execution(agentledger.ActionTypeToolCall, key); found {
			if err := validateRequestFingerprint(record, payload); err != nil {
				return agentgo.ToolResult{}, err
			}
			if result, replayErr, replay, decodeErr := replayToolResult(record); replay || decodeErr != nil {
				return result, errors.Join(replayErr, decodeErr)
			}
		}

		opCtx, cancel := a.operationContext(ctx)
		defer cancel()
		handle, err := a.beginToolAttempt(opCtx, execution, semantics, payload)
		if err != nil {
			return agentgo.ToolResult{}, fmt.Errorf("record agentgo tool request: %w", err)
		}
		result, callErr := next(ctx, execution)
		finishCtx, finishCancel := a.detachedOperationContext(ctx)
		defer finishCancel()
		outcomePayload := map[string]any{"output": persistedToolResultFrom(result)}
		var terminal agentledger.StoredEvent
		if callErr != nil || result.IsError {
			failure := callErr
			if failure == nil {
				failure = fmt.Errorf("tool %s returned an error result", execution.Call.Name)
			}
			terminal, err = a.runtimeRecorder.ToolFailed(finishCtx, handle, failure, outcomePayload)
		} else {
			terminal, err = a.runtimeRecorder.ToolCompleted(finishCtx, handle, outcomePayload)
		}
		if err != nil {
			return agentgo.ToolResult{}, fmt.Errorf("record agentgo tool outcome: %w", err)
		}
		a.journal.markAttemptTerminal(handle.AttemptID, terminal)
		return result, callErr
	}
}

func replayToolResult(record executionRecord) (agentgo.ToolResult, error, bool, error) {
	for index := len(record.Attempts) - 1; index >= 0; index-- {
		terminal := record.Attempts[index].Terminal
		if terminal == nil {
			continue
		}
		switch terminal.EventType {
		case agentledger.EventTypeAttemptCompleted, agentledger.EventTypeAttemptFailed:
			var persisted persistedToolResult
			if output, found := terminal.Payload["output"]; found {
				if err := decodePayload(output, &persisted); err != nil {
					return agentgo.ToolResult{}, nil, true, fmt.Errorf("decode replayed tool output: %w", err)
				}
			}
			result := persisted.agentGoResult()
			if terminal.EventType == agentledger.EventTypeAttemptFailed && result.ToolCallID == "" {
				return result, errors.New(failureMessage(*terminal)), true, nil
			}
			return result, nil, true, nil
		}
	}
	return agentgo.ToolResult{}, nil, false, nil
}

// ToolResult keeps some Loop-relevant fields out of its public JSON shape.
// Persist them explicitly so replay reconstructs rich content instead of
// silently degrading image/tool-reference results to plain text.
type persistedToolResult struct {
	ToolCallID    string                 `json:"tool_call_id"`
	ToolName      string                 `json:"tool_name,omitempty"`
	Content       json.RawMessage        `json:"content,omitempty"`
	ContentBlocks []agentgo.ContentBlock `json:"content_blocks,omitempty"`
	IsError       bool                   `json:"is_error,omitempty"`
	Details       any                    `json:"details,omitempty"`
}

func persistedToolResultFrom(result agentgo.ToolResult) persistedToolResult {
	return persistedToolResult{
		ToolCallID: result.ToolCallID, ToolName: result.ToolName,
		Content: result.Content, ContentBlocks: result.ContentBlocks,
		IsError: result.IsError, Details: result.Details,
	}
}

func (result persistedToolResult) agentGoResult() agentgo.ToolResult {
	return agentgo.ToolResult{
		ToolCallID: result.ToolCallID, ToolName: result.ToolName,
		Content: result.Content, ContentBlocks: result.ContentBlocks,
		IsError: result.IsError, Details: result.Details,
	}
}

func toolRequestPayload(execution agentgo.ToolExecution, semantics ToolSemantics) (map[string]any, error) {
	payload := executionPayload(execution.Execution)
	payload["tool_call_id"] = execution.Call.ID
	payload["tool_name"] = execution.Call.Name
	payload["input"] = execution.Call.Args
	if semantics.IdempotencyKey != "" {
		payload["idempotency_key"] = semantics.IdempotencyKey
	}
	fingerprintInput := map[string]any{
		"tool_call_id":      execution.Call.ID,
		"tool_name":         execution.Call.Name,
		"input":             normalizedToolInput(execution.Call),
		"idempotency_key":   semantics.IdempotencyKey,
		"args_invalid":      execution.Call.ArgsInvalid,
		"args_raw_text":     execution.Call.ArgsRawText,
		"args_parse_error":  execution.Call.ArgsParseError,
		"thought_signature": execution.Call.ThoughtSignature,
	}
	fingerprint, err := requestFingerprint(fingerprintInput)
	if err != nil {
		return nil, err
	}
	payload["request_fingerprint"] = fingerprint
	return payload, nil
}

func normalizedToolInput(call agentgo.ToolCall) any {
	var input any
	if err := json.Unmarshal(call.Args, &input); err == nil {
		return input
	}
	return string(call.Args)
}

func failureMessage(event agentledger.StoredEvent) string {
	errorValue, _ := event.Payload["error"].(map[string]any)
	if message := stringValue(errorValue["message"]); message != "" {
		return message
	}
	return "replayed tool attempt failed"
}
