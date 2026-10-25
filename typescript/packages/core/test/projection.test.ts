import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { projectView } from "../src/projection.js";
import type { RunView } from "../src/types.js";
import type { ModelCall, ToolCall, Turn } from "@agent-ledger/view";
const fixture = () => JSON.parse(readFileSync(new URL("../../../../../conformance/vectors/view-ledger.json", import.meta.url), "utf8")) as {source: RunView; call_ids: string[]; statuses: string[]};

test("projection preserves retry attempts, parallel Lanes, failures and unresolved calls", () => {
  const data=fixture();const before=structuredClone(data.source);const document=projectView(data.source);
  const turns=document.runs[0]!.items.filter((item): item is Turn => item.kind === "turn");
  const calls=turns.flatMap(turn => turn.items.filter((item): item is ModelCall|ToolCall => item.kind === "model_call" || item.kind === "tool_call"));
  assert.deepEqual(calls.map(c => c.id),data.call_ids);
  assert.deepEqual(calls.map(c => c.status ?? "unknown"),data.statuses);
  assert.equal(document.issues!.filter(x=>x.includes("result missing")).length,1);
  assert.equal(calls[0]!.response!.usage!.input_tokens,8);
  assert.notEqual(turns[0]!.lane_id,turns[1]!.lane_id);
  calls[0]!.request!.input = "changed";
  assert.deepEqual(data.source,before);
  assert.ok(document.runs[0]!.items.some(item=>item.kind === "operation" && item.operation === "lane.vendor.notice"));
});
