import {
  type AdapterDescriptor,
  type AttemptHandle,
  type JsonValue,
  LaneRecorder,
  type Turn,
} from "@agent-ledger/core";

export const PI_ADAPTER: AdapterDescriptor = {
  schema_version: "1.0",
  adapter_id: "pi-agent-harness",
  adapter_version: "1",
  framework: "pi-agent-core",
  framework_version: ">=0.80 <1",
  capabilities: {
    model_prewrite: "strict", tool_prewrite: "strict", outcome_gate: "strict",
    recovery: "native_store", preserves_native_state: true,
  },
};

export interface PiHarnessLike {
  on(type: any, handler: (event: any) => any): () => void;
  subscribe(listener: (event: any) => Promise<void> | void): () => void;
}

export function bindPiHarness(harness: PiHarnessLike, recorder: LaneRecorder): () => void {
  const disposers: Array<() => void> = [];
  const pendingTools = new Map<string, AttemptHandle>();
  const pendingModels: AttemptHandle[] = [];
  let turnCount = 0;
  let currentTurn: Turn | undefined;
  let context: JsonValue = [];
  let turnFailure: string | undefined;
  let runFailure: string | undefined;

  disposers.push(harness.on("context", (event) => {
    context = asJson(event.messages);
    return { messages: event.messages };
  }));
  disposers.push(harness.on("before_provider_request", async (event) => {
    if (currentTurn === undefined) throw new Error("Pi model call occurred outside a turn");
    const payload: { [key: string]: JsonValue } = { input: context };
    const model = modelIdentity(event.model);
    if (model !== undefined) payload.model = model;
    const attempt = await recorder.beforeModelCall(currentTurn.id, payload);
    pendingModels.push(attempt);
    return undefined;
  }));
  disposers.push(harness.on("tool_call", async (event) => {
    if (currentTurn === undefined) throw new Error("Pi tool call occurred outside a turn");
    const attempt = await recorder.beforeToolCall(
      currentTurn.id,
      event.toolCallId,
      { tool_name: event.toolName, input: asJson(event.input) },
    );
    pendingTools.set(event.toolCallId, attempt);
    return undefined;
  }));
  disposers.push(harness.on("tool_result", async (event) => {
    const attempt = pendingTools.get(event.toolCallId);
    if (!attempt) return undefined;
    if (event.isError) await recorder.toolFailed(attempt, new Error(textContent(event.content)));
    else await recorder.toolCompleted(attempt, {
      output: { content: asJson(event.content), details: asJson(event.details) },
    });
    pendingTools.delete(event.toolCallId);
    return undefined;
  }));
  disposers.push(harness.subscribe(async (event) => {
    switch (event.type) {
      case "agent_start":
        await recorder.startRun({
          adapter: { id: PI_ADAPTER.adapter_id, version: PI_ADAPTER.adapter_version },
          framework: { name: PI_ADAPTER.framework },
        });
        break;
      case "turn_start":
        turnCount += 1;
        turnFailure = undefined;
        currentTurn = await recorder.startTurn({ turn: turnCount });
        break;
      case "message_end": {
        if (event.message?.role !== "assistant") break;
        const stopReason = event.message.stopReason ?? event.message.stop_reason;
        if (stopReason === "error" || stopReason === "aborted") {
          turnFailure = event.message.errorMessage ?? `Pi model stopped with ${stopReason}`;
          runFailure = turnFailure;
        }
        const attempt = pendingModels.shift();
        if (!attempt) break;
        if (turnFailure) await recorder.modelFailed(attempt, new Error(turnFailure));
        else await recorder.modelCompleted(attempt, modelResult(event.message));
        break;
      }
      case "turn_end":
        if (currentTurn === undefined) throw new Error("Pi turn ended without a start");
        if (turnFailure) await recorder.failTurn(currentTurn.id, new Error(turnFailure));
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
    for (const dispose of disposers.reverse()) dispose();
  };
}

function asJson(value: unknown): JsonValue {
  if (value === undefined) return null;
  return JSON.parse(JSON.stringify(value)) as JsonValue;
}

function modelIdentity(model: unknown): { [key: string]: JsonValue } | undefined {
  if (typeof model !== "object" || model === null) return undefined;
  const candidate = model as Record<string, unknown>;
  if (typeof candidate.id !== "string" || candidate.id.length === 0) return undefined;
  return {
    id: candidate.id,
    ...(typeof candidate.provider === "string" && candidate.provider.length > 0
      ? { provider: candidate.provider }
      : {}),
  };
}

function modelResult(message: unknown): { [key: string]: JsonValue } {
  const payload: { [key: string]: JsonValue } = { output: asJson(message) };
  if (typeof message !== "object" || message === null) return payload;
  const candidate = message as Record<string, unknown>;
  const finishReason = candidate.stopReason ?? candidate.stop_reason;
  if (typeof finishReason === "string" && finishReason.length > 0) {
    payload.finish_reason = finishReason;
  }
  const model = modelIdentity({ id: candidate.model, provider: candidate.provider });
  if (model !== undefined) payload.model = model;
  const usage = normalizedUsage(candidate.usage);
  if (usage !== undefined) payload.usage = usage;
  return payload;
}

function normalizedUsage(value: unknown): { [key: string]: JsonValue } | undefined {
  if (typeof value !== "object" || value === null) return undefined;
  const source = value as Record<string, unknown>;
  const fields: Array<[string, string]> = [
    ["input_tokens", "input"], ["output_tokens", "output"],
    ["cache_read_input_tokens", "cacheRead"], ["cache_write_input_tokens", "cacheWrite"],
    ["total_tokens", "totalTokens"],
  ];
  const usage: { [key: string]: JsonValue } = {};
  for (const [target, sourceKey] of fields) {
    if (typeof source[sourceKey] === "number") usage[target] = source[sourceKey];
  }
  return Object.keys(usage).length === 0 ? undefined : usage;
}

function textContent(content: unknown): string {
  if (!Array.isArray(content)) return "Pi tool failed";
  return content
    .filter((item): item is { text: string } => typeof item === "object" && item !== null && "text" in item)
    .map((item) => item.text)
    .join("\n") || "Pi tool failed";
}
