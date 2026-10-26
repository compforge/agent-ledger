import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { renderHTML, validate, type SessionView, type Turn, type ModelCall, type ToolCall } from "../src/index.js";
const fixture = (): SessionView => JSON.parse(readFileSync(new URL("../../../../../conformance/vectors/session-view.json", import.meta.url), "utf8")) as SessionView;

test("native IDs, exact message order, null input and related-session references survive", () => {
  const document = fixture();validate(document);
  assert.deepEqual(JSON.parse(JSON.stringify(document)), fixture());
  const turn = document.nodes[1] as Turn;
  const model = turn.nodes[0] as ModelCall;
  assert.deepEqual((model.request!.input as {messages: {role:string}[]}).messages.map(x => x.role), ["system", "developer", "user", "user"]);
  const tool = turn.nodes[1] as ToolCall;
  assert.equal(tool.request!.input, null);
  assert.equal((turn.nodes[2] as ToolCall).status, undefined);
  tool.started_at = "2026-10-25T00:00:00Z"; tool.finished_at = "2026-10-25T00:00:01Z";
  assert.ok(tool.sessions![0]!.id);
  validate(document);
});

test("invalid identities, dangling model linkage and negative usage fail", () => {
  const document = fixture();
  document.nodes[1]!.id = document.nodes[0]!.id;
  assert.throws(() => validate(document), /duplicate/);
  const linked = fixture(); ((linked.nodes[1] as Turn).nodes[1] as ToolCall).model_call_id = "absent";
  assert.throws(() => validate(linked), /missing model call/);
  const negative = fixture(); ((negative.nodes[1] as Turn).nodes[0] as ModelCall).response!.usage!.input_tokens = -1;
  assert.throws(() => validate(negative), /usage/);
});

test("HTML renders untrusted bodies as text and preserves source order", () => {
  const html = renderHTML(fixture(), '<img src=x onerror="alert(1)">');
  assert.ok(!html.includes("<script>"));assert.ok(!html.includes("<img"));
  assert.ok(html.includes("&lt;script&gt;alert(1)&lt;/script&gt;"));
  assert.ok(html.indexOf("input-1") < html.indexOf("turn-1"));
  assert.ok(html.includes("outcome_unknown"));assert.ok(html.includes("No response body recorded"));
});
