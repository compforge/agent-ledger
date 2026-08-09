package agentgoadapter

import (
	"context"
	"errors"
	"fmt"

	"github.com/compforge/agentgo"
)

type recordingModel struct {
	inner   agentgo.ChatModel
	adapter *Adapter
}

func (m *recordingModel) Generate(ctx context.Context, messages []agentgo.Message, tools []agentgo.ToolSpec, options ...agentgo.CallOption) (*agentgo.LLMResponse, error) {
	attempt, err := m.adapter.runtimeRecorder.BeforeModelCall(ctx, m.adapter.stepID(), modelPayload(messages, tools))
	if err != nil {
		return nil, fmt.Errorf("record agentgo model request: %w", err)
	}
	response, callErr := m.inner.Generate(ctx, messages, tools, options...)
	if callErr != nil {
		if err := m.adapter.runtimeRecorder.ModelFailed(ctx, attempt, callErr); err != nil {
			return nil, fmt.Errorf("record agentgo model failure: %w", err)
		}
		return nil, callErr
	}
	if err := m.adapter.runtimeRecorder.ModelCompleted(ctx, attempt, map[string]any{"message": response.Message}); err != nil {
		return nil, fmt.Errorf("record agentgo model result: %w", err)
	}
	return response, nil
}

func (m *recordingModel) GenerateStream(ctx context.Context, messages []agentgo.Message, tools []agentgo.ToolSpec, options ...agentgo.CallOption) (<-chan agentgo.StreamEvent, error) {
	attempt, err := m.adapter.runtimeRecorder.BeforeModelCall(ctx, m.adapter.stepID(), modelPayload(messages, tools))
	if err != nil {
		return nil, fmt.Errorf("record agentgo model request: %w", err)
	}
	source, callErr := m.inner.GenerateStream(ctx, messages, tools, options...)
	if callErr != nil {
		if err := m.adapter.runtimeRecorder.ModelFailed(ctx, attempt, callErr); err != nil {
			return nil, fmt.Errorf("record agentgo model failure: %w", err)
		}
		return nil, callErr
	}
	output := make(chan agentgo.StreamEvent, 16)
	go func() {
		defer close(output)
		terminal := false
		for event := range source {
			if event.Type == agentgo.StreamEventDone {
				terminal = true
				if err := m.adapter.runtimeRecorder.ModelCompleted(ctx, attempt, map[string]any{"message": event.Message}); err != nil {
					event = agentgo.StreamEvent{Type: agentgo.StreamEventError, Err: fmt.Errorf("record agentgo model result: %w", err)}
				}
			} else if event.Type == agentgo.StreamEventError {
				terminal = true
				failure := event.Err
				if failure == nil {
					failure = errors.New("agentgo model stream failed")
				}
				if err := m.adapter.runtimeRecorder.ModelFailed(ctx, attempt, failure); err != nil {
					event.Err = fmt.Errorf("record agentgo model failure: %w", err)
				}
			}
			select {
			case output <- event:
			case <-ctx.Done():
				return
			}
		}
		if !terminal && ctx.Err() == nil {
			failure := errors.New("agentgo model stream closed without terminal event")
			if err := m.adapter.runtimeRecorder.ModelFailed(ctx, attempt, failure); err != nil {
				failure = fmt.Errorf("%v; record failure: %w", failure, err)
			}
			output <- agentgo.StreamEvent{Type: agentgo.StreamEventError, Err: failure}
		}
	}()
	return output, nil
}

func (m *recordingModel) SupportsTools() bool { return m.inner.SupportsTools() }

func (m *recordingModel) ProviderName() string {
	if named, ok := m.inner.(agentgo.ProviderNamer); ok {
		return named.ProviderName()
	}
	return ""
}

func (m *recordingModel) ModelName() string {
	if named, ok := m.inner.(agentgo.ModelNamer); ok {
		return named.ModelName()
	}
	return ""
}

func modelPayload(messages []agentgo.Message, tools []agentgo.ToolSpec) map[string]any {
	toolNames := make([]string, 0, len(tools))
	for _, tool := range tools {
		toolNames = append(toolNames, tool.Name)
	}
	return map[string]any{"messages": messages, "tools": toolNames}
}
