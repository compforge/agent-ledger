import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { test } from "node:test";

import {
  canonicalAppendDigest,
  LaneConflict,
  LaneRecorder,
  MemoryEventStore,
  newAction,
  newActor,
  newAttempt,
  newId,
  newLane,
  newTurn,
  proposedEvent,
} from "../src/index.js";

test("execution hierarchy projects a session", async () => {
  const store = new MemoryEventStore();
  const actor = newActor("agent", "plain-loop");
  const lane = newLane("session", "run", "main");
  const turn = newTurn(lane.id);
  const action = newAction(turn.id, "model_call");
  const attempt = newAttempt(action.id, 1);
  await store.createActor(actor);
  await store.createLane(lane);
  await store.createTurn(turn);
  await store.createAction(action);
  await store.createAttempt(attempt);
  const event = proposedEvent({
    lane_id: lane.id,
    subject_id: attempt.id,
    event_type: "attempt.requested",
    actor_id: actor.id,
    payload: { model: "test" },
  });
  const receipt = await store.append(lane.id, 0, newId(), [event]);
  assert.equal(receipt.last_seq, 1);
  const view = await store.loadSession("session");
  assert.deepEqual(
    [view.actors.length, view.lanes.length, view.turns.length, view.actions.length, view.attempts.length, view.events.length],
    [1, 1, 1, 1, 1, 1],
  );
});

test("append digest matches the cross-language vector", async () => {
  const vector = JSON.parse(
    await readFile(resolve(process.cwd(), "../conformance/vectors/append.json"), "utf8"),
  ) as { events: Parameters<typeof canonicalAppendDigest>[0]; sha256: string };
  assert.equal(canonicalAppendDigest(vector.events), vector.sha256);
});

test("append is idempotent and rejects a stale lane sequence", async () => {
  const store = new MemoryEventStore();
  const actor = newActor("agent");
  const lane = newLane("session", "run");
  await store.createActor(actor);
  await store.createLane(lane);
  const event = proposedEvent({
    lane_id: lane.id, subject_id: lane.id, event_type: "lane.started", actor_id: actor.id,
  });
  const appendId = newId();
  const first = await store.append(lane.id, 0, appendId, [event]);
  const second = await store.append(lane.id, 0, appendId, [event]);
  assert.deepEqual(second, first);
  await assert.rejects(
    store.append(lane.id, 0, newId(), [proposedEvent({
      lane_id: lane.id, subject_id: lane.id, event_type: "lane.completed", actor_id: actor.id,
    })]),
    LaneConflict,
  );
});

test("recorder gives retries a new attempt under the same action", async () => {
  const store = new MemoryEventStore();
  const recorder = await LaneRecorder.open({
    store, sessionId: "session", runId: "run", actor: newActor("agent", "plain-loop"),
  });
  const turn = await recorder.startTurn();
  const first = await recorder.beforeModelCall(turn.id, { model: "test" });
  await recorder.modelFailed(first, new Error("timeout"));
  const second = await recorder.retry(first.action_id, 2, { model: "test" });
  assert.equal(second.action_id, first.action_id);
  assert.notEqual(second.attempt_id, first.attempt_id);
  assert.equal(second.attempt_no, 2);
});

test("committed event content is immutable", async () => {
  const store = new MemoryEventStore();
  const actor = newActor("agent");
  const lane = newLane("session", "run");
  await store.createActor(actor);
  await store.createLane(lane);
  const event = proposedEvent({
    lane_id: lane.id, subject_id: lane.id, event_type: "lane.recorded", actor_id: actor.id,
    payload: { nested: { value: 1 } },
  });
  await store.append(lane.id, 0, newId(), [event]);
  (event.payload.nested as { value: number }).value = 2;
  const stored = [];
  for await (const item of store.loadLane(lane.id)) stored.push(item);
  (stored[0]!.payload.nested as { value: number }).value = 3;
  const reloaded = [];
  for await (const item of store.loadLane(lane.id)) reloaded.push(item);
  assert.deepEqual(reloaded[0]!.payload.nested, { value: 1 });
});
