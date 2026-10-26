import { execFileSync } from "node:child_process";
import assert from "node:assert/strict";
import { appendFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { InMemorySessionStorage, Session, runAgentLoop, type StreamFn, type AgentContext, type SessionTreeEntry } from "@earendil-works/pi-agent-core";
import { createAssistantMessageEventStream, type AssistantMessage, type Model } from "@earendil-works/pi-ai/compat";
import type { ModelCall, ToolCall } from "@compforge/agent-observation";
import { PiObservation, observationWriter, readObservation, readSessionView, sessionView } from "../src/observation/index.js";

const model: Model<"openai-completions"> = { id: "test", name: "test", api: "openai-completions", provider: "test",
  baseUrl: "http://unused", reasoning: false, input: ["text"], contextWindow: 10000, maxTokens: 1000,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } };
const metadata = { id: "session", createdAt: "2026-01-01T00:00:00Z" };
function message(tool = false): AssistantMessage {
  return { role: "assistant", api: model.api, model: model.id, provider: model.provider, timestamp: Date.now(),
    content: tool ? [{ type: "toolCall", id: "lookup", name: "lookup", arguments: { key: "x" } }] : [{ type: "text", text: "done" }],
    stopReason: tool ? "toolUse" : "stop",
    usage: { input: 0, output: 2, cacheRead: 0, cacheWrite: 0, totalTokens: 2, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
}
function provider(tool = false): StreamFn {
  return async (selected, context, options) => {
    await options?.onPayload?.({ messages: context.messages, tools: context.tools, system: context.systemPrompt }, selected);
    const stream = createAssistantMessageEventStream(), response = message(tool);
    stream.push({ type: "done", reason: response.stopReason as "stop" | "toolUse", message: response });
    return stream;
  };
}
function fixture(t: { after: (fn: () => void) => void }) {
  const dir = mkdtempSync(join(tmpdir(), "pi-observation-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const path = join(dir, "observation.jsonl"), errors: Error[] = [];
  const create = () => new PiObservation({ sessionId: metadata.id, write: observationWriter(path), onError: error => errors.push(error) });
  return { path, errors, create };
}

test("real Pi loop links native entries to stable calls; restart, branches and context transforms stay observable", async t => {
  const f = fixture(t);
  let capture = f.create();
  let native = new InMemorySessionStorage({ metadata });
  let session = new Session(capture.storage(native));
  let calls = 0;
  const transport: StreamFn = (...args) => provider(++calls === 1)(...args);
  const tool = { name: "lookup", label: "lookup", description: "read", parameters: { type: "object", properties: {} },
    execute: async () => ({ content: [{ type: "text", text: "found" }], details: {} }) };
  const context: AgentContext = { systemPrompt: "fixed", messages: [], tools: [tool as NonNullable<AgentContext["tools"]>[number]] };
  const run = async (text: string) => runAgentLoop([{ role: "user", content: text, timestamp: Date.now() }], context,
    { model, convertToLlm: messages => messages as Parameters<StreamFn>[1]["messages"],
      transformContext: async messages => messages.filter(m => m.role !== "user" || m.content !== "hidden"),
      onPayload: payload => ({ ...(payload as object), transformed: true }) },
    async event => {
      capture.event(event);
      if (event.type === "message_end") await session.appendMessage(event.message);
    }, undefined, capture.stream(transport));
  context.messages.push({ role: "user", content: "hidden", timestamp: 0 });
  await run("first");
  let view = await readSessionView(native, readObservation(f.path, metadata.id));
  const models = view.nodes.filter(n => n.kind === "model_call") as ModelCall[];
  const tools = view.nodes.filter(n => n.kind === "tool_call") as ToolCall[];
  assert.equal(models.length, 2);
  assert.equal(tools.length, 1);
  assert.equal(tools[0]!.model_call_id, models[0]!.id);
  assert.equal(models[0]!.response!.usage!.input_tokens, 0);
  assert.equal((models[0]!.request!.input as Record<string, unknown>).transformed, true);
  assert.ok(!JSON.stringify(models[0]!.request!.input).includes("hidden"));
  assert.equal(view.nodes.filter(n => n.kind === "input").length, 1);
  assert.ok(!view.issues!.some(x => x.includes("exact model request not recorded")));
  const firstIds = view.nodes.map(n => n.id);
  const user = (await native.getEntries()).find(e => e.type === "message" && e.message.role === "user")!;
  capture.input(user, "initial");

  // Fresh observer and native storage, as after a process restart. The observer does not replay state.
  native = new InMemorySessionStorage({ metadata, entries: JSON.parse(JSON.stringify(await native.getEntries())) });
  capture = f.create(); session = new Session(capture.storage(native));
  context.messages = (await session.buildContext()).messages;
  await run("followup");
  view = await readSessionView(native, readObservation(f.path, metadata.id));
  assert.deepEqual(view.nodes.slice(0, firstIds.length).map(n => n.id), firstIds);
  const followup = (await native.getEntries()).find(e => e.type === "message" && e.message.role === "user" && e.message.content === "followup")!;
  capture.input(followup, "followup");
  assert.equal((await readSessionView(native, readObservation(f.path, metadata.id))).nodes.find(n => n.id === followup.id)?.kind, "input");

  await native.setLeafId(user.id);
  await session.appendCompaction("short", user.id, 500);
  context.messages = (await session.buildContext()).messages;
  calls = 0; // same provider tool ID on a sibling branch
  await run("branch");
  view = await readSessionView(native, readObservation(f.path, metadata.id));
  const branchTools = view.nodes.filter(n => n.kind === "tool_call") as ToolCall[];
  assert.equal(branchTools.length, 2);
  assert.notEqual(branchTools[0]!.model_call_id, branchTools[1]!.model_call_id);
  assert.ok(view.nodes.some(n => n.kind === "operation"));
  assert.deepEqual(f.errors, []);
});

test("pending call survives an interrupted reader; resume does not reuse its identity", async t => {
  const f = fixture(t), capture = f.create();
  const never = createAssistantMessageEventStream();
  await capture.stream(async (m, _c, options) => {
    await options?.onPayload?.({ messages: [] }, m);
    return never;
  })(model, { messages: [] });
  const pending = readObservation(f.path, metadata.id);
  assert.equal(pending.nodes[0]!.status, "unknown");
  assert.ok(pending.issues.some(x => x.includes("completion not observed")));
  const next = await f.create().stream(provider())(model, { messages: [] });
  await next.result();
  const moduleUrl = new URL("../src/observation/index.js", import.meta.url).href;
  const restored = JSON.parse(execFileSync(process.execPath, ["--input-type=module", "-e",
    `import { readObservation } from ${JSON.stringify(moduleUrl)}; process.stdout.write(JSON.stringify(readObservation(process.argv[1], 'session')));`, f.path], { encoding: "utf8" }));
  assert.equal(restored.nodes.length, 2);
  assert.notEqual(restored.nodes[0]!.id, restored.nodes[1]!.id);
  assert.equal(restored.nodes[1]!.status, "completed");
  appendFileSync(f.path, '{"version":');
  const prefix = readObservation(f.path, metadata.id);
  assert.equal(prefix.nodes.length, 2);
  assert.ok(prefix.issues.some(x => x.includes("incomplete trailing")));
});

test("capture preserves transformed null, all payload callbacks and no payload remains unknown", async t => {
  const f = fixture(t), capture = f.create();
  const stream = await capture.stream(async (m, _c, options) => {
    assert.equal(await options?.onPayload?.({ original: 1 }, m), null);
    assert.equal(await options?.onPayload?.({ original: 2 }, m), null);
    const output = createAssistantMessageEventStream();
    output.push({ type: "done", reason: "stop", message: message() }); return output;
  })(model, { messages: [] }, { onPayload: () => null });
  for await (const _event of stream) { /* iterator-only consumer */ }
  await stream.result();
  const view = sessionView(metadata, [], readObservation(f.path, metadata.id));
  assert.equal((view.nodes[0] as ModelCall).request!.input, null);
  assert.equal(view.nodes.filter(n => n.kind === "model_call").length, 1);
  assert.equal(view.nodes.filter(n => n.kind === "operation").length, 1);
  const noPayload = await capture.stream(async () => {
    const output = createAssistantMessageEventStream(); output.push({ type: "done", reason: "stop", message: message() }); return output;
  })(model, { messages: [] });
  await noPayload.result();
  assert.ok(sessionView(metadata, [], readObservation(f.path, metadata.id)).issues!.some(x => x.includes("provider payload not observed")));
});

test("stream errors, cancellation and capture failures preserve the agent's behavior", async t => {
  const f = fixture(t), capture = f.create();
  await assert.rejects(async () => capture.stream(async () => { throw new Error("transport down"); })(model, { messages: [] }), /transport down/);
  const cancelled = { ...message(), stopReason: "aborted" as const, errorMessage: "cancelled" };
  const stream = await capture.stream(async () => {
    const output = createAssistantMessageEventStream(); output.push({ type: "error", reason: "aborted", error: cancelled }); return output;
  })(model, { messages: [] });
  assert.equal(await stream.result(), cancelled);
  const nodes = readObservation(f.path, metadata.id).nodes;
  assert.equal(nodes[0]!.status, "failed"); assert.equal(nodes[1]!.status, "cancelled");
  const errors: Error[] = [];
  const broken = new PiObservation({ sessionId: "session", write: () => { throw new Error("disk full"); }, onError: e => errors.push(e) });
  const healthy = await broken.stream(provider())(model, { messages: [] });
  assert.equal((await healthy.result()).stopReason, "stop");
  assert.ok(errors.length >= 2);
});

test("identity mismatch, corrupted records and explicit links cannot silently mix Sessions", async t => {
  const f = fixture(t), capture = f.create();
  await (await capture.stream(provider())(model, { messages: [] })).result();
  const snapshot = readObservation(f.path, metadata.id);
  assert.throws(() => sessionView({ ...metadata, id: "other" }, [], snapshot), /Session mismatch/);
  const wrong = readObservation(f.path, "other");
  assert.equal(wrong.nodes.length, 0);
  assert.ok(wrong.issues.some(x => x.includes("identity mismatch")));
  appendFileSync(f.path, 'null\n');
  assert.ok(readObservation(f.path, metadata.id).issues.some(x => x.includes("suffix not read")));
  const node = snapshot.nodes[0]!;
  const entry: SessionTreeEntry = { type: "message", id: "native", parentId: null, timestamp: metadata.createdAt, message: message() };
  const linked = { ...snapshot, links: [{ nodeId: node.id, entryId: "native" }] };
  assert.equal(sessionView(metadata, [entry], linked).nodes.length, 1);
  assert.throws(() => sessionView(metadata, [entry], { ...snapshot,
    nodes: [...snapshot.nodes, { id: "other", kind: "model_call" }],
    links: [...linked.links, { nodeId: "other", entryId: "native" }] }), /conflicting observation link/);
});

test("unfinished tools and lost terminal records keep native outcomes without fabricated failures", async t => {
  const f = fixture(t), capture = f.create();
  capture.event({ type: "tool_execution_start", toolCallId: "pending", toolName: "read", args: { path: "file" } });
  const partial = sessionView(metadata, [], readObservation(f.path, metadata.id));
  assert.equal(partial.nodes[0]!.status, "unknown");
  const response = message();
  const native: SessionTreeEntry = { type: "message", id: "result", parentId: null, timestamp: metadata.createdAt, message: response };
  const restored = sessionView(metadata, [native], { sessionId: metadata.id, issues: ["node lost: completion not observed"],
    nodes: [{ id: "lost", kind: "model_call", status: "unknown", started_at: metadata.createdAt }],
    links: [{ nodeId: "lost", entryId: "result" }] });
  assert.equal(restored.nodes[0]!.status, "completed");
  assert.ok((restored.nodes[0] as ModelCall).response);
  assert.ok(!restored.issues!.includes("node lost: completion not observed"));
});

test("partial tail is retained and new segments can be read together", async t => {
  const f = fixture(t);
  await (await f.create().stream(provider())(model, { messages: [] })).result();
  appendFileSync(f.path, '{"version":');
  const second = f.create();
  await (await second.stream(provider())(model, { messages: [] })).result();
  assert.ok(f.errors.some(e => e.message.includes("incomplete tail")));
  const nextPath = `${f.path}.next`;
  const resumed = new PiObservation({ sessionId: metadata.id, write: observationWriter(nextPath), onError: e => { throw e; } });
  await (await resumed.stream(provider())(model, { messages: [] })).result();
  const recovered = readObservation([f.path, nextPath], metadata.id);
  assert.equal(recovered.nodes.filter(n => n.kind === "model_call").length, 2);
  assert.ok(recovered.issues.some(x => x.includes("incomplete trailing")));
});
