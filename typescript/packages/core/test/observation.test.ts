import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { sessionView } from "../src/observation.js";
import type { SessionView as LedgerSessionView } from "@compforge/agent-ledger";
import type { ModelCall, ToolCall, Turn, Operation } from "@compforge/agent-observation";
const fixture = () => JSON.parse(readFileSync(new URL("../../../../../conformance/vectors/view-ledger.json", import.meta.url), "utf8")) as {source: LedgerSessionView; call_ids: string[]; statuses: string[]};

test("projection preserves retry attempts, parallel Lanes, failures and unresolved calls", () => {
  const data=fixture();const before=structuredClone(data.source);const document=sessionView(data.source);
  const turns=document.nodes.filter((item): item is Turn => item.kind === "turn");
  const calls=turns.flatMap(turn => turn.nodes.filter((item): item is ModelCall|ToolCall => item.kind === "model_call" || item.kind === "tool_call"));
  assert.deepEqual(calls.map(c => c.id),data.call_ids);
  assert.deepEqual(calls.map(c => c.status ?? "unknown"),data.statuses);
  assert.equal(document.issues!.filter(x=>x.includes("result missing")).length,1);
  assert.equal(calls[0]!.response!.usage!.input_tokens,8);
  assert.notEqual(turns[0]!.lane_id,turns[1]!.lane_id);
  calls[0]!.request!.input = "changed";
  assert.deepEqual(data.source,before);
  assert.ok(document.nodes.some(item=>item.kind === "operation" && (item as Operation).operation === "lane.vendor.notice"));
});


test("typed failed calls retain structured evidence through Recorder and View", async () => {
 const {LaneRecorder, MemoryEventStore, newActor} = await import("@compforge/agent-ledger");
 const {decodeRequest, decodeResponse} = await import("@compforge/agent-common");
 const vector = JSON.parse(readFileSync(new URL("../../../../../conformance/vectors/common-call.json", import.meta.url), "utf8"));
 const store = new MemoryEventStore();
 const recorder = await LaneRecorder.open({store, sessionId: "s", runId: "r", actor: newActor("agent", "test")});
 const turn = await recorder.startTurn();
 const attempt = await recorder.beforeModelCall(turn.id, decodeRequest(vector.request), vector.details);
 await recorder.modelFailed(attempt, new Error("fallback"), decodeResponse(vector.response), vector.details);
 const document = sessionView(await store.loadSession("s"));
 const call = (document.nodes.find(n => n.kind === "turn") as Turn).nodes.find(n => n.kind === "model_call") as ModelCall;
 assert.equal(call.status, "failed");
 assert.deepEqual(call.request, vector.request);
 assert.deepEqual(call.response, vector.response);
 assert.deepEqual(Object.values(call.attributes!.event_details as object), [vector.details, vector.details]);
});


test("one session includes successive runs and excludes another session", async () => {
 const {LaneRecorder, MemoryEventStore, newActor} = await import("@compforge/agent-ledger");
 const store=new MemoryEventStore();
 for (const [sessionId,runId] of [["s","one"],["s","two"],["other","one"]]) {
  const recorder=await LaneRecorder.open({store,sessionId:sessionId!,runId:runId!,actor:newActor("agent")});
  const turn=await recorder.startTurn();
  const call=await recorder.beforeModelCall(turn.id,{input:runId!});
  await recorder.modelCompleted(call,{output:"done"});
  await recorder.completeRun();
 }
 const result=sessionView(await store.loadSession("s"));
 assert.deepEqual(result.session,{source:"ledger",id:"s"});
 assert.equal(result.nodes.filter(x=>x.kind==="turn").length,2);
 assert.deepEqual(result.nodes.filter(x=>x.kind==="turn").map(x=>x.run_id),["one","two"]);
 assert.equal("status" in result,false);
 const mixed=await store.loadSession("s");mixed.lanes[0]!.session_id="other";
 assert.throws(()=>sessionView(mixed),/different session/);
});
