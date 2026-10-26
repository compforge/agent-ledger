import assert from "node:assert/strict";
import test from "node:test";
import type { SessionEntry } from "@earendil-works/pi-coding-agent";
import { sessionView } from "../src/observation/index.js";
import type { ModelCall, ToolCall } from "@compforge/agent-observation";

const metadata = {type: "session" as const, version: 3, cwd: "/test", id: "native-session", timestamp: "2026-01-02T03:04:05.000Z"};
const timestamp = metadata.timestamp;
function assistant(id: string, parentId: string | null, argument: string): SessionEntry {
  return {type: "message", id, parentId, timestamp, message: {
    role: "assistant", api: "openai-completions", provider: "openai", model: "requested",
    usage: {input: 0, output: 2, cacheRead: 0, cacheWrite: 0, totalTokens: 2, cost: {input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0}},
    content: [{type: "toolCall", id: "same-tool-id", name: "lookup", arguments: {value: argument}}],
    stopReason: "toolUse", timestamp: 0,
  }};
}
function tool(id: string, parentId: string): SessionEntry {
  return {type: "message", id, parentId, timestamp, message: {role: "toolResult", toolCallId: "same-tool-id", toolName: "lookup", content: [{type: "text", text: "answer"}], isError: false, timestamp: 0}};
}

test("Pi observation preserves append order, branch ownership and stable IDs across followup", () => {
  const entries: SessionEntry[] = [
    {type: "message", id: "input", parentId: null, timestamp, message: {role: "user", content: "hello", timestamp: 0}},
    assistant("first", "input", "A"), assistant("branch", "input", "B"), tool("result", "first"),
  ];
  const before = structuredClone(entries);
  const observed = sessionView(metadata, entries);
  assert.deepEqual(observed.nodes.map(x => x.id), ["input", "first", "branch", "result"]);
  const result = observed.nodes[3] as ToolCall;
  assert.equal(result.model_call_id, "first");
  assert.deepEqual(result.request!.input, {value: "A"});
  assert.equal((observed.nodes[1] as ModelCall).request, undefined);
  assert.equal((observed.nodes[1] as ModelCall).response!.model, undefined);
  assert.equal((observed.nodes[1] as ModelCall).response!.usage!.input_tokens, 0);
  entries.push({type: "message", id: "followup", parentId: "result", timestamp, message: {role: "user", content: "next", timestamp: 0}});
  const extended = sessionView(metadata, entries);
  assert.deepEqual(extended.nodes.slice(0, 4), observed.nodes);
  result.request!.input = "changed";
  assert.deepEqual(entries.slice(0, 4), before);
});

test("Pi keeps orphan results and proposals without manufacturing invocations", () => {
  const observed = sessionView(metadata, [assistant("proposal", null, "A"), tool("orphan", "not-observed")]);
  assert.equal(observed.nodes.filter(x => x.kind === "tool_call").length, 1);
  const result = observed.nodes[1] as ToolCall;
  assert.equal(result.request, undefined);
  assert.equal(result.model_call_id, undefined);
  assert.ok(result.response);
  assert.ok(observed.issues!.some(x => x.includes("tool request not observed")));
  assert.throws(() => sessionView(metadata, [assistant("same", null, "A"), assistant("same", null, "B")]), /duplicate entry/);
});

