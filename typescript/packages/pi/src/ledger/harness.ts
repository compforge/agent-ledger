import type { CallResponse } from "@compforge/agent-common";
import { type AdapterDescriptor, type AttemptHandle, type JsonValue, LaneRecorder, type Turn } from "@compforge/agent-ledger";
import type { Agent, AgentMessage, StreamFn } from "@earendil-works/pi-agent-core";

export const PI_ADAPTER: AdapterDescriptor = {
  schema_version: "1.0", adapter_id: "pi-agent", adapter_version: "2",
  framework: "pi-agent-core", framework_version: ">=1.4 <2",
  capabilities: {
    model_prewrite: "strict", tool_prewrite: "strict", outcome_gate: "strict",
    recovery: "checkpoint", preserves_native_state: true,
  },
};

/**
 * @spec Bind before starting a run. Awaited Agent events gate tool execution and loop advancement.
 * @spec A recorder owns one Run; bind a new recorder for a subsequent prompt/run.
 * @spec Native Session persistence and checkpoint selection remain caller-owned.
 * @link spec/rfcs/0002-polyglot-adapters.md#pi-profile
 * @link typescript/packages/pi/test/pi.test.ts
 */
export function bindPiHarness(agent: Agent, recorder: LaneRecorder): () => void {
  if (agent.state.isStreaming) throw new Error("Cannot bind Pi Ledger during an active run");
  const original = agent.streamFunction;
  const pendingTools = new Map<string, AttemptHandle>();
  let pendingModel: AttemptHandle | undefined;
  let currentTurn: Turn | undefined;
  let turnCount = 0;
  let runFailure: string | undefined;
  let used = false;
  // A failed write poisons this binding: Pi may emit synthetic failure events, but those
  // must not turn an unrecorded outcome into a successful Run or trigger another effect.
  let writeFailure: unknown;
  const guarded = async <T>(action: () => Promise<T>): Promise<T> => {
    if (writeFailure) throw writeFailure;
    try { return await action(); } catch (error) { writeFailure = error; throw error; }
  };
  const stream: StreamFn = async (model, context, options) => {
    await guarded(async () => {
      if (!currentTurn) throw new Error("Pi model call occurred outside a turn");
      pendingModel = await recorder.beforeModelCall(currentTurn.id, {
        model: { id: model.id, provider: model.provider }, input: asJson(context.messages),
      });
    });
    return original(model, context, options);
  };
  agent.streamFunction = stream;
  const unsubscribe = agent.subscribe(event => guarded(async () => {
    switch (event.type) {
      case "agent_start":
        if (used) throw new Error("Pi Ledger binding already recorded a Run; bind a new recorder");
        used = true;
        await recorder.startRun({ adapter: { id: PI_ADAPTER.adapter_id, version: PI_ADAPTER.adapter_version },
          framework: { name: PI_ADAPTER.framework } });
        break;
      case "turn_start":
        currentTurn = await recorder.startTurn({ turn: ++turnCount });
        break;
      case "tool_execution_start":
        if (!currentTurn) throw new Error("Pi tool call occurred outside a turn");
        pendingTools.set(event.toolCallId, await recorder.beforeToolCall(currentTurn.id, event.toolCallId,
          { tool_name: event.toolName, input: asJson(event.args) }));
        break;
      case "tool_execution_end": {
        const attempt = pendingTools.get(event.toolCallId);
        if (!attempt) throw new Error(`Pi tool ${event.toolCallId} ended without a recorded request`);
        if (event.isError) await recorder.toolFailed(attempt, new Error(textContent(event.result.content)));
        else await recorder.toolCompleted(attempt, { output: asJson(event.result) });
        pendingTools.delete(event.toolCallId);
        break;
      }
      case "message_end": {
        if (event.message.role !== "assistant") break;
        const failure = event.message.stopReason === "error" || event.message.stopReason === "aborted"
          ? event.message.errorMessage ?? `Pi model stopped with ${event.message.stopReason}` : undefined;
        if (failure) runFailure = failure;
        const attempt = pendingModel;
        if (!attempt) break;
        if (failure) await recorder.modelFailed(attempt, new Error(failure));
        else await recorder.modelCompleted(attempt, modelResult(event.message));
        pendingModel = undefined;
        break;
      }
      case "turn_end":
        if (!currentTurn) throw new Error("Pi turn ended without a start");
        if (runFailure) await recorder.failTurn(currentTurn.id, new Error(runFailure));
        else await recorder.completeTurn(currentTurn.id, { turn: turnCount });
        currentTurn = undefined;
        break;
      case "agent_end":
        if (runFailure) await recorder.failRun(new Error(runFailure));
        else await recorder.completeRun({ turn_count: turnCount });
        break;
    }
  }));
  return () => {
    if (agent.state.isStreaming) throw new Error("Cannot unbind Pi Ledger during an active run");
    unsubscribe();
    if (agent.streamFunction === stream) agent.streamFunction = original;
  };
}

function asJson(value: unknown): JsonValue {
  if (value === undefined) return null;
  return JSON.parse(JSON.stringify(value)) as JsonValue;
}

function modelResult(message: Extract<AgentMessage, { role: "assistant" }>): CallResponse {
  return {
    output: asJson(message), finish_reason: message.stopReason,
    ...(message.responseModel ? { model: { id: message.responseModel, provider: message.provider } } : {}),
    ...(message.responseId ? { provider_request_id: message.responseId } : {}),
    usage: {
      input_tokens: message.usage.input, output_tokens: message.usage.output,
      cache_read_input_tokens: message.usage.cacheRead, cache_write_input_tokens: message.usage.cacheWrite,
      total_tokens: message.usage.totalTokens,
    },
  };
}

function textContent(content: unknown): string {
  if (!Array.isArray(content)) return "Pi tool failed";
  return content
    .filter((item): item is { text: string } => typeof item === "object" && item !== null && "text" in item)
    .map((item) => item.text)
    .join("\n") || "Pi tool failed";
}
