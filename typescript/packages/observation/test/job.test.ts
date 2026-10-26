import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import {
  validate, referenceIssues, renderHTML,
  type JobView, type SessionView, type Operation, type ToolCall, type Item,
} from "../src/index.js";

function fixture(name: string): unknown {
  return JSON.parse(readFileSync(new URL(`../../../../../conformance/vectors/${name}`, import.meta.url), "utf8"));
}

function jobFixture(name: string): JobView {
  return fixture(name) as JobView;
}

test("Job fixtures preserve native IDs, cross-session calls, nesting and partial evidence", () => {
  for (const [name, gaps] of [["job-review.json", 0], ["job-input.json", 1]] as const) {
    const job = jobFixture(name);
    const before = JSON.stringify(job);
    validate(job);
    assert.equal(referenceIssues(job).length, gaps);
    assert.equal(JSON.stringify(job), before);
    assert.equal(job.status, "completed");
    const html = renderHTML(job);
    assert.match(html, /Execution Session/);
    assert.match(html, /not an exclusive job cost/);
    assert.match(html, /Relations/);
  }
});

test("task-oriented Job uses the same observations as its Session", () => {
  const session = fixture("session-view.json") as SessionView;
  const original = JSON.stringify(session);
  const items = structuredClone(session.items);
  function attach(items: Item[]): void {
    for (const item of items) {
      item.session = session.session;
      if (item.kind === "turn" || item.kind === "operation") attach(item.items ?? []);
    }
  }
  attach(items);
  const job: JobView = {
    schema_version: "agent-ledger/job@1",
    job: {source: "test", id: "task"},
    items,
  };
  validate(job);
  assert.deepEqual(referenceIssues(job), []);
  assert.equal(JSON.stringify(session), original);
});

test("graph validation is scoped by Session, not by bare IDs or parent grouping", () => {
  const job = jobFixture("job-review.json");
  const discover = job.items[1] as Operation;
  discover.items!.push(structuredClone(discover.items![0]!));
  assert.throws(() => validate(job), /duplicate/);

  const wrong = jobFixture("job-review.json");
  ((wrong.items[1] as Operation).items![1] as ToolCall).model_call_id = "t1";
  assert.throws(() => validate(wrong), /not a model call/);

  const session = fixture("session-view.json") as SessionView;
  session.items[0]!.session = {source: "elsewhere", id: "foreign"};
  assert.throws(() => validate(session), /foreign Session/);
});

test("missing external fragments stay visible while local references are strict", () => {
  const job = jobFixture("job-input.json");
  job.relations!.push({
    kind: "depends_on",
    source: {id: "assess"},
    target: {session: {source: "pi", id: "other"}, id: "missing"},
  });
  assert.equal(referenceIssues(job).length, 2);
  assert.match(renderHTML(job), /outside Job snapshot/);
  job.relations![1]!.target = {id: "missing"};
  assert.throws(() => validate(job), /missing job item/);
});

test("shared call keeps its identity and full recorded usage across job views", () => {
  const a = jobFixture("job-input.json");
  const b = structuredClone(a);
  b.job.id = "input-b";
  assert.deepEqual(a.items, b.items);
  assert.equal("usage" in a, false);
  a.result = null;
  a.goal = null;
  validate(a);
  assert.equal(JSON.parse(JSON.stringify(a)).result, null);
});

test("Job rendering escapes source-provided content", () => {
  const job = jobFixture("job-input.json");
  job.name = '<img onerror="alert(1)">';
  job.result = '<script>bad()</script>';
  const html = renderHTML(job);
  assert.ok(!html.includes("<img"));
  assert.ok(!html.includes("<script>"));
});

test("root validation rejects unknown status, reverse time and retired schema", () => {
  const job = jobFixture("job-review.json");
  job.started_at = "2026-10-26T01:00:00Z";
  job.finished_at = "2026-10-26T00:00:00Z";
  assert.throws(() => validate(job), /finish precedes start/);

  const invalid = {...jobFixture("job-input.json"), status: "good"};
  assert.throws(() => validate(invalid as JobView), /invalid status/);
  const old = {...jobFixture("job-input.json"), schema_version: "agent-ledger/job-view@1"};
  assert.throws(() => validate(old as JobView), /unsupported View schema/);
});
