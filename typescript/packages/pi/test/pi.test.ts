import assert from "node:assert/strict";
import { test } from "node:test";
import { type AppendReceipt, LaneRecorder, MemoryEventStore, newActor, type ProposedEvent } from "@compforge/agent-ledger";
import { Agent, type StreamFn } from "@earendil-works/pi-agent-core";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { createAssistantMessageEventStream, Type, type AssistantMessage, type Model } from "@earendil-works/pi-ai";
import { bindPiHarness, dumpPiSession, restorePiSession } from "../src/ledger/index.js";

const model: Model<"openai-completions"> = { id: "test", name: "test", api: "openai-completions", provider: "test",
  baseUrl: "http://unused", reasoning: false, input: ["text"], contextWindow: 10000, maxTokens: 1000,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } };
const response = (tool = false): AssistantMessage => ({ role: "assistant", api: model.api, model: model.id,
  provider: model.provider, timestamp: 0, stopReason: tool ? "toolUse" : "stop",
  content: tool ? [{ type: "toolCall", id: "call", name: "lookup", arguments: {} }] : [{ type: "text", text: "done" }],
  usage: { input: 1, output: 2, cacheRead: 0, cacheWrite: 0, totalTokens: 3,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } });

function runtime() {
  let modelCalls = 0, toolCalls = 0;
  const stream: StreamFn = () => {
    const output = createAssistantMessageEventStream();
    const message = response(++modelCalls === 1);
    output.push({ type: "done", reason: message.stopReason as "stop" | "toolUse", message });
    return output;
  };
  const agent = new Agent({ streamFn: stream, initialState: { model, systemPrompt: "fixed", tools: [{
    name: "lookup", label: "lookup", description: "read", parameters: Type.Object({}),
    execute: async () => { toolCalls++; return { content: [{ type: "text", text: "found" }], details: {} }; },
  }] } });
  return { agent, counts: () => ({ modelCalls, toolCalls }), stream };
}

async function recorder(store: MemoryEventStore, runId = "run") {
  return LaneRecorder.open({ store, sessionId: "session", runId, actor: newActor("agent", "pi") });
}

test("real Pi Agent gates model/tool requests and outcomes; disposer restores stream", async () => {
  const store = new MemoryEventStore(), r = runtime();
  const dispose = bindPiHarness(r.agent, await recorder(store));
  await r.agent.prompt("hello");
  assert.deepEqual(r.counts(), { modelCalls: 2, toolCalls: 1 });
  const view = await store.loadSession("session");
  assert.deepEqual(view.actions.map(action => action.type), ["model_call", "tool_call", "model_call"]);
  assert.equal(view.events.filter(event => event.event_type === "attempt.requested").length, 3);
  assert.equal(view.events.filter(event => event.event_type === "attempt.completed").length, 3);
  assert.equal(view.events.at(-1)?.event_type, "run.completed");
  assert.equal(view.events.find(e => e.event_type === "attempt.completed")?.payload.model, undefined);
  const request = view.events.find(event => event.event_type === "attempt.requested")!;
  assert.ok(JSON.stringify(request.payload.input).includes('"role":"system"'));
  dispose(); assert.equal(r.agent.streamFunction, r.stream);
  const next = bindPiHarness(r.agent, await recorder(store, "next"));
  await r.agent.prompt("again"); next();
  assert.equal((await store.loadSession("session")).events.filter(e => e.event_type === "run.completed").length, 2);
});

class FailingStore extends MemoryEventStore {
  constructor(private readonly failAt: number) { super(); }
  calls = 0;
  override async append(laneId: string, seq: number, appendId: string, events: readonly ProposedEvent[]): Promise<AppendReceipt> {
    if (events.some(e => e.event_type === "attempt.requested" || e.event_type === "attempt.completed") && ++this.calls === this.failAt) {
      throw new Error("ledger write failed");
    }
    return super.append(laneId, seq, appendId, events);
  }
}

for (const [failAt, expected] of [[1, { modelCalls: 0, toolCalls: 0 }], [2, { modelCalls: 1, toolCalls: 0 }],
  [3, { modelCalls: 1, toolCalls: 0 }], [4, { modelCalls: 1, toolCalls: 1 }]] as const) {
  test(`Ledger write failure ${failAt} prevents the next effect in a real Pi Agent`, async () => {
    const store = new FailingStore(failAt), r = runtime();
    const dispose = bindPiHarness(r.agent, await recorder(store));
    await assert.rejects(r.agent.prompt("hello"), /ledger write failed/);
    assert.deepEqual(r.counts(), expected);
    assert.equal((await store.loadSession("session")).events.some(e => e.event_type === "run.completed"), false);
    dispose();
  });
}

test("Pi native checkpoint preserves identity, branches, compaction, context edits and active leaf", () => {
  const native = SessionManager.inMemory("/test");
  const user = native.appendMessage({ role: "user", content: "hello", timestamp: 0 });
  native.appendMessage(response());
  native.appendContextEdit(user, { content: "edited" });
  native.appendCompaction("summary", user, 100);
  const previous = native.getLeafId()!;
  native.branch(user);
  native.appendMessage({ role: "user", content: "branch", timestamp: 0 });
  native.branch(previous); // active leaf is intentionally not the final appended entry
  const snapshot = dumpPiSession(native);
  const restored = restorePiSession(JSON.parse(JSON.stringify(snapshot)));
  assert.equal(restored.getSessionId(), native.getSessionId());
  assert.equal(restored.getLeafId(), previous);
  assert.deepEqual(restored.getEntries(), JSON.parse(JSON.stringify(native.getEntries())));
  assert.deepEqual(restored.buildSessionContext(), native.buildSessionContext());
  snapshot.entries.length = 0;
  assert.ok(restored.getEntries().length > 0);
  native.resetLeaf();
  assert.equal(restorePiSession(dumpPiSession(native)).getLeafId(), null);
  assert.throws(() => restorePiSession({ ...dumpPiSession(native), leafId: "missing" }), /leaf missing not found/);
});
