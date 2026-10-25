import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { renderHTML, validate, type Document, type Turn, type ModelCall, type ToolCall } from "../src/index.js";
const fixture = (): Document => JSON.parse(readFileSync(new URL("../../../../../conformance/vectors/view.json", import.meta.url), "utf8")) as Document;

test("native IDs, exact message order, null input and asynchronous nested execution survive", () => {
  const document = fixture();validate(document);
  assert.deepEqual(JSON.parse(JSON.stringify(document)), fixture());
  const turn = document.runs[0]!.items[1] as Turn;
  const model = turn.items[0] as ModelCall;
  assert.deepEqual((model.request!.input as {messages: {role:string}[]}).messages.map(x => x.role), ["system", "developer", "user", "user"]);
  const tool = turn.items[1] as ToolCall;
  assert.equal(tool.request!.input, null);
  assert.equal((turn.items[2] as ToolCall).status, undefined);
  tool.started_at = "2026-10-25T00:00:00Z"; tool.finished_at = "2026-10-25T00:00:01Z";
  tool.runs![0]!.started_at = "2026-10-25T00:00:02Z";
  validate(document);
});

test("invalid identities, dangling model linkage and negative usage fail", () => {
  const document = fixture();
  document.runs[0]!.items[1]!.id = document.runs[0]!.items[0]!.id;
  assert.throws(() => validate(document), /duplicate/);
  const linked = fixture(); ((linked.runs[0]!.items[1] as Turn).items[1] as ToolCall).model_call_id = "absent";
  assert.throws(() => validate(linked), /missing model call/);
  const negative = fixture(); ((negative.runs[0]!.items[1] as Turn).items[0] as ModelCall).response!.usage!.input_tokens = -1;
  assert.throws(() => validate(negative), /usage/);
});

test("HTML renders untrusted bodies as text and preserves source order", () => {
  const html = renderHTML(fixture(), '<img src=x onerror="alert(1)">');
  assert.ok(!html.includes("<script>"));assert.ok(!html.includes("<img"));
  assert.ok(html.includes("&lt;script&gt;alert(1)&lt;/script&gt;"));
  assert.ok(html.indexOf("input-1") < html.indexOf("turn-1"));
  assert.ok(html.includes("outcome_unknown"));assert.ok(html.includes("No response body recorded"));
});
