package agentgoadapter

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentgo"
)

func TestReplayDurableModelOutcomeWithoutCallingModelAgain(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	adapter := newTestAdapter(t, ctx, store)
	user := agentgo.UserMsg("hello")
	if _, err := adapter.WrapModel(fakeModel{}).Generate(ctx, []agentgo.Message{user}, nil); err != nil {
		t.Fatalf("record model outcome: %v", err)
	}

	replay, err := adapter.Replay(ctx, []agentgo.AgentMessage{user}, nil)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replay.Messages) != 2 || replay.Messages[1].TextContent() != "done" {
		t.Fatalf("messages = %#v", replay.Messages)
	}
	if replay.ReplayedMessages != 1 || replay.Anchor == nil {
		t.Fatalf("replay = %#v", replay)
	}
	if replay.Anchor.LastAppliedSeq != adapter.runtimeRecorder.Lane().LastSeq {
		t.Fatalf("anchor = %#v, lane = %#v", replay.Anchor, adapter.runtimeRecorder.Lane())
	}
}

func TestReplayDurableToolOutcomesInToolCallOrder(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	adapter := newTestAdapter(t, ctx, store)
	turnID, err := adapter.turnID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	anchor := firstRuntimeAnchor(t, ctx, adapter)
	call1 := agentgo.ToolCall{ID: "tool-1", Name: "read", Args: json.RawMessage(`{"path":"one"}`)}
	call2 := agentgo.ToolCall{ID: "tool-2", Name: "read", Args: json.RawMessage(`{"path":"two"}`)}
	attempt1 := requestTool(t, ctx, adapter, turnID, call1)
	attempt2 := requestTool(t, ctx, adapter, turnID, call2)
	if _, err := adapter.runtimeRecorder.ToolCompleted(ctx, attempt2, map[string]any{
		"output": json.RawMessage(`{"value":2}`),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.runtimeRecorder.ToolCompleted(ctx, attempt1, map[string]any{
		"output": json.RawMessage(`{"value":1}`),
	}); err != nil {
		t.Fatal(err)
	}
	assistant := agentgo.Message{
		Role: agentgo.RoleAssistant,
		Content: []agentgo.ContentBlock{
			agentgo.ToolCallBlock(call1), agentgo.ToolCallBlock(call2),
		},
	}

	replay, err := adapter.Replay(ctx, []agentgo.AgentMessage{assistant}, anchor)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replay.Messages) != 3 || replay.ReplayedMessages != 2 {
		t.Fatalf("replay = %#v", replay)
	}
	assertToolResult(t, replay.Messages[1], "tool-1", `{"value":1}`, false)
	assertToolResult(t, replay.Messages[2], "tool-2", `{"value":2}`, false)
}

func TestReplayDurableFailedToolOutcomeAsErrorResult(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	adapter := newTestAdapter(t, ctx, store)
	turnID, err := adapter.turnID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	anchor := firstRuntimeAnchor(t, ctx, adapter)
	call := agentgo.ToolCall{ID: "tool-1", Name: "write", Args: json.RawMessage(`{}`)}
	attempt := requestTool(t, ctx, adapter, turnID, call)
	if _, err := adapter.runtimeRecorder.ToolFailed(ctx, attempt, errors.New("boom"), nil); err != nil {
		t.Fatal(err)
	}
	assistant := agentgo.Message{
		Role: agentgo.RoleAssistant, Content: []agentgo.ContentBlock{agentgo.ToolCallBlock(call)},
	}

	replay, err := adapter.Replay(ctx, []agentgo.AgentMessage{assistant}, anchor)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	assertToolResult(t, replay.Messages[1], "tool-1", `"boom"`, true)
}

func TestReplayStopsBeforeUnresolvedAttempt(t *testing.T) {
	ctx := context.Background()
	store := agentledger.NewMemoryEventStore()
	adapter := newTestAdapter(t, ctx, store)
	turnID, err := adapter.turnID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	anchor := firstRuntimeAnchor(t, ctx, adapter)
	call := agentgo.ToolCall{ID: "tool-1", Name: "write", Args: json.RawMessage(`{}`)}
	requestTool(t, ctx, adapter, turnID, call)
	assistant := agentgo.Message{
		Role: agentgo.RoleAssistant, Content: []agentgo.ContentBlock{agentgo.ToolCallBlock(call)},
	}

	replay, err := adapter.Replay(ctx, []agentgo.AgentMessage{assistant}, anchor)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replay.Messages) != 1 || replay.ReplayedMessages != 0 || *replay.Anchor != *anchor {
		t.Fatalf("replay = %#v", replay)
	}
}

func firstRuntimeAnchor(t *testing.T, ctx context.Context, adapter *Adapter) *agentledger.CheckpointAnchor {
	t.Helper()
	for event, err := range adapter.runtimeRecorder.Store().LoadLane(ctx, adapter.runtimeRecorder.Lane().ID, 0) {
		if err != nil {
			t.Fatal(err)
		}
		return &agentledger.CheckpointAnchor{
			LaneID: event.LaneID, LastAppliedSeq: event.Seq, LastAppliedEventID: event.ID,
		}
	}
	t.Fatal("runtime lane has no event")
	return nil
}

func requestTool(
	t *testing.T,
	ctx context.Context,
	adapter *Adapter,
	turnID string,
	call agentgo.ToolCall,
) agentledger.AttemptHandle {
	t.Helper()
	attempt, err := adapter.beforeToolCall(ctx, turnID, call, agentledger.UnknownEffect(), map[string]any{
		"tool_name": call.Name, "input": call.Args,
	})
	if err != nil {
		t.Fatal(err)
	}
	return attempt
}

func assertToolResult(
	t *testing.T,
	message agentgo.AgentMessage,
	toolCallID string,
	content string,
	isError bool,
) {
	t.Helper()
	model, include := message.ToMessage()
	if !include || model.Role != agentgo.RoleTool || model.TextContent() != content ||
		model.Metadata["tool_call_id"] != toolCallID || model.Metadata["is_error"] != isError {
		t.Fatalf("tool result = %#v", model)
	}
}
