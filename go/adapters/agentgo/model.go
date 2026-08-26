package agentgoadapter

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentgo"
)

// ModelMiddleware records one durable Attempt per physical model execution.
// A completed result is returned directly to AgentGo during recovery so the
// native Loop, rather than the Adapter, rebuilds transcript state.
func (a *Adapter) ModelMiddleware() agentgo.ModelMiddleware {
	return func(
		ctx context.Context,
		execution agentgo.ModelExecution,
		next agentgo.ModelExecuteFunc,
	) (agentgo.ModelResult, error) {
		opCtx, cancel := a.operationContext(ctx)
		defer cancel()

		payload, err := modelRequestPayload(execution, a.modelIdentity(execution))
		if err != nil {
			return agentgo.ModelResult{}, fmt.Errorf("fingerprint agentgo model request: %w", err)
		}
		key := a.actionKey(execution.Execution)
		if record, found := a.execution(agentledger.ActionTypeModelCall, key); found {
			if err := validateRequestFingerprint(record, payload); err != nil {
				return agentgo.ModelResult{}, err
			}
			if result, replay, err := replayModelResult(record); replay || err != nil {
				return result, err
			}
		}
		handle, err := a.beginModelAttempt(opCtx, execution.Execution, payload)
		if err != nil {
			return agentgo.ModelResult{}, fmt.Errorf("record agentgo model request: %w", err)
		}
		result, callErr := next(ctx, execution)
		finishCtx, finishCancel := a.detachedOperationContext(ctx)
		defer finishCancel()
		var terminal agentledger.StoredEvent
		if callErr != nil {
			terminal, err = a.runtimeRecorder.ModelFailed(
				finishCtx, handle, callErr, modelObservationPayload(result.Message),
			)
		} else {
			terminal, err = a.runtimeRecorder.ModelCompleted(
				finishCtx, handle, modelResultPayload(result),
			)
		}
		if err != nil {
			return agentgo.ModelResult{}, fmt.Errorf("record agentgo model outcome: %w", err)
		}
		a.journal.markAttemptTerminal(handle.AttemptID, terminal)
		return result, callErr
	}
}

func replayModelResult(record executionRecord) (agentgo.ModelResult, bool, error) {
	for index := len(record.Attempts) - 1; index >= 0; index-- {
		terminal := record.Attempts[index].Terminal
		if terminal == nil || terminal.EventType != agentledger.EventTypeAttemptCompleted {
			continue
		}
		var result agentgo.ModelResult
		if err := decodePayload(terminal.Payload["output"], &result.Message); err != nil {
			return agentgo.ModelResult{}, true, fmt.Errorf("decode replayed model output: %w", err)
		}
		result.HasCompletedToolCalls, _ = terminal.Payload["has_completed_tool_calls"].(bool)
		return result, true, nil
	}
	return agentgo.ModelResult{}, false, nil
}

func modelRequestPayload(execution agentgo.ModelExecution, identity ModelIdentity) (map[string]any, error) {
	payload := executionPayload(execution.Execution)
	input := map[string]any{
		"messages": modelMessageInputs(execution.Request.Messages),
		"tools":    execution.Request.Tools,
	}
	payload["input"] = input
	model := modelPayload(identity.Provider, identity.ID)
	if model != nil {
		payload["model"] = model
	}
	fingerprint, err := requestFingerprint(map[string]any{"input": input, "model": model})
	if err != nil {
		return nil, err
	}
	payload["request_fingerprint"] = fingerprint
	return payload, nil
}

func modelMessageInputs(messages []agentgo.Message) []map[string]any {
	inputs := make([]map[string]any, 0, len(messages))
	for _, message := range messages {
		input := map[string]any{
			"role":    message.Role,
			"content": message.Content,
		}
		if message.StopReason != "" {
			input["stop_reason"] = message.StopReason
		}
		if len(message.Metadata) > 0 {
			input["metadata"] = message.Metadata
		}
		inputs = append(inputs, input)
	}
	return inputs
}

func modelResultPayload(result agentgo.ModelResult) map[string]any {
	payload := modelObservationPayload(result.Message)
	payload["output"] = result.Message
	payload["has_completed_tool_calls"] = result.HasCompletedToolCalls
	if result.Message.StopReason != "" {
		payload["finish_reason"] = string(result.Message.StopReason)
	}
	return payload
}

func modelObservationPayload(message agentgo.Message) map[string]any {
	payload := make(map[string]any)
	if message.Usage == nil {
		return payload
	}
	payload["usage"] = map[string]any{
		"input_tokens":             message.Usage.Input,
		"output_tokens":            message.Usage.Output,
		"cache_read_input_tokens":  message.Usage.CacheRead,
		"cache_write_input_tokens": message.Usage.CacheWrite,
		"total_tokens":             message.Usage.TotalTokens,
	}
	if model := modelPayload(message.Usage.Provider, message.Usage.Model); model != nil {
		payload["model"] = model
	}
	return payload
}

func modelPayload(provider, modelName string) map[string]any {
	if modelName == "" {
		return nil
	}
	model := map[string]any{"id": modelName}
	if provider != "" {
		model["provider"] = provider
	}
	return model
}

func decodePayload(value any, target any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(encoded, target)
}
