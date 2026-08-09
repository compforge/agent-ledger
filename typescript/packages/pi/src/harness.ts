import {
  type AdapterDescriptor,
  type AttemptHandle,
  type JsonValue,
  SessionRecorder,
} from "@agent-ledger/core";

export const PI_ADAPTER: AdapterDescriptor = {
  schema_version: "1.0",
  adapter_id: "pi-agent-harness",
  adapter_version: "1",
  framework: "pi-agent-core",
  framework_version: ">=0.80 <1",
  capabilities: {
    model_prewrite: "strict",
    tool_prewrite: "strict",
    outcome_gate: "strict",
    recovery: "native_store",
    preserves_native_state: true,
  },
};

export interface PiHarnessLike {
  on(type: any, handler: (event: any) => any): () => void;
  subscribe(listener: (event: any) => Promise<void> | void): () => void;
}

export function bindPiHarness(harness: PiHarnessLike, recorder: SessionRecorder): () => void {
  const disposers: Array<() => void> = [];
  const pendingTools = new Map<string, AttemptHandle>();
  const pendingModels: AttemptHandle[] = [];
  let turn = 0;
  let stepId = crypto.randomUUID();
  let context: JsonValue = [];
  let turnFailure: string | undefined;
  let runFailure: string | undefined;

  disposers.push(harness.on("context", (event) => {
    context = asJson(event.messages);
    return { messages: event.messages };
  }));
  disposers.push(harness.on("before_provider_request", async (event) => {
    const attempt = await recorder.beforeModelCall(stepId, {
      model: asJson(modelIdentity(event.model)),
      context,
    });
    pendingModels.push(attempt);
    return undefined;
  }));
  disposers.push(harness.on("tool_call", async (event) => {
    const attempt = await recorder.beforeToolCall(stepId, {
      tool_call_id: event.toolCallId,
      tool_name: event.toolName,
      input: asJson(event.input),
    });
    pendingTools.set(event.toolCallId, attempt);
    return undefined;
  }));
  disposers.push(harness.on("tool_result", async (event) => {
    const attempt = pendingTools.get(event.toolCallId);
    if (!attempt) return undefined;
    if (event.isError) await recorder.toolFailed(attempt, new Error(textContent(event.content)));
    else await recorder.toolCompleted(attempt, { content: asJson(event.content), details: asJson(event.details) });
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
        turn += 1;
        stepId = crypto.randomUUID();
        turnFailure = undefined;
        await recorder.record("step.started", { stepId, payload: { turn } });
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
        else {
          await recorder.modelCompleted(attempt, { message: asJson(event.message) });
        }
        break;
      }
      case "turn_end":
        await recorder.record(turnFailure ? "step.failed" : "step.completed", {
          stepId,
          payload: turnFailure ? { turn, error: turnFailure } : { turn },
        });
        break;
      case "agent_end":
        await recorder.record(runFailure ? "run.failed" : "run.completed", {
          payload: runFailure ? { turn_count: turn, error: runFailure } : { turn_count: turn },
        });
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

function modelIdentity(model: unknown): Record<string, unknown> {
  if (typeof model !== "object" || model === null) return {};
  const candidate = model as Record<string, unknown>;
  return { id: candidate.id, provider: candidate.provider };
}

function textContent(content: unknown): string {
  if (!Array.isArray(content)) return "Pi tool failed";
  return content
    .filter((item): item is { text: string } => typeof item === "object" && item !== null && "text" in item)
    .map((item) => item.text)
    .join("\n") || "Pi tool failed";
}
