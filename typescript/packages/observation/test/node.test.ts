import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import {
  inspect, selectNodes, renderHTML,
  type Node, type Input, type ModelCall, type ToolCall, type Operation, type Turn,
  type SessionView, type JobView,
} from "../src/index.js";

function fixture(name: string): unknown {
  return JSON.parse(readFileSync(new URL(`../../../../../conformance/vectors/${name}`, import.meta.url), "utf8"));
}
function flatten(nodes: Node[]): Node[] {
  return nodes.flatMap(node => [node, ...(
    node.kind === "operation" || node.kind === "turn" ? flatten((node as Operation | Turn).nodes ?? []) : []
  )]);
}

test("Node collections preserve all concrete fields through JSON", () => {
  const input: Input = {kind: "input", id: "i", content: null};
  const model: ModelCall = {kind: "model_call", id: "m", request: {input: {messages: ["original"]}}};
  const tool: ToolCall = {kind: "tool_call", id: "t", model_call_id: "m", request: {tool_name: "read", input: null}};
  const turn: Turn = {kind: "turn", id: "turn", nodes: [input, model, tool]};
  const operation: Operation = {kind: "operation", id: "op", operation: "check", data: null, nodes: [turn]};
  const view: SessionView = {schema_version: "agent-ledger/session@1", session: {source: "test", id: "s"}, nodes: [operation]};
  const decoded: SessionView = JSON.parse(JSON.stringify(view));
  inspect(decoded);
  assert.deepEqual(decoded, view);
  assert.deepEqual(flatten(decoded.nodes).map(node => node.kind), ["operation", "turn", "input", "model_call", "tool_call"]);
});

test("selection preserves source order, removes overlapping roots and isolates mutable facts", () => {
  const session = fixture("session-view.json") as SessionView;
  const before = structuredClone(session);
  const nodes = selectNodes(session, ["t1", "turn-1", "input-1", "turn-1"]);
  assert.deepEqual(nodes.map(node => node.id), ["input-1", "turn-1"]);
  assert.ok(flatten(nodes).every(node => JSON.stringify(node.session) === JSON.stringify(session.session)));
  const job: JobView = {schema_version: "agent-ledger/job@1", job: {source: "test", id: "j"}, nodes};
  assert.deepEqual(inspect(job).issues, []);
  ((nodes[1] as Turn).nodes[0] as ModelCall).request!.input = "changed";
  nodes[0]!.session!.id = "changed";
  assert.deepEqual(session, before);
  assert.equal(nodes[1]!.session!.id, session.session.id);
});

test("selection rejects unknown IDs and does not silently include related models", () => {
  const session = fixture("session-view.json") as SessionView;
  assert.throws(() => selectNodes(session, ["missing"]), /missing nodes/);
  assert.deepEqual(selectNodes(session, []), []);
  const nodes = selectNodes(session, ["t1"]);
  assert.deepEqual(nodes.map(node => node.id), ["t1"]);
  const job: JobView = {schema_version: "agent-ledger/job@1", job: {source: "test", id: "j"}, nodes};
  const before = structuredClone(job);
  assert.equal(inspect(job).issues.length, 1);
  assert.deepEqual(job, before);
});

test("Job links resolve scoped IDs while unrecorded fragments have no dead links", () => {
  const job = fixture("job-review.json") as JobView;
  const html = renderHTML(job);
  const ids = [...html.matchAll(/<article id="([^"]+)"/g)].map(match => match[1]);
  const links = [...html.matchAll(/href="#([^"]+)"/g)].map(match => match[1]);
  assert.equal(new Set(ids).size, ids.length);
  assert.ok(links.length >= 4);
  assert.ok(links.every(id => ids.includes(id)));
  assert.match(html, /hands off to/);
  assert.match(html, /depends on/);
  assert.match(html, /Execution Session: agentgo \/ r1/);
  assert.match(html, /Execution Session: agentgo \/ r2/);
  const partial = renderHTML(fixture("job-input.json") as JobView);
  assert.match(partial, /fragment not included/);
  assert.doesNotMatch(partial, /href="#[^"]*earlier-model/);
});
