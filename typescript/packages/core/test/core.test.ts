import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { test } from "node:test";

import {
  ActionType,
  canonicalAppendDigest,
  CheckpointConflict,
  EventType,
  inspectRun,
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
  proposedCheckpoint,
} from "../src/index.js";

test("core vocabulary matches the cross-language registry", async () => {
  const vocabulary = JSON.parse(
    await readFile(resolve(process.cwd(), "../spec/vocabulary.json"), "utf8"),
  ) as { action_types: string[]; event_types: string[] };

  assert.deepEqual(Object.values(ActionType), vocabulary.action_types);
  assert.deepEqual(Object.values(EventType), vocabulary.event_types);
});

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

test("checkpoint save is versioned, idempotent, and optionally anchored", async () => {
  const store = new MemoryEventStore();
  const actor = newActor("agent", "pi");
  const lane = newLane("session", "run");
  await store.createActor(actor);
  await store.createLane(lane);
  const event = proposedEvent({
    lane_id: lane.id, subject_id: lane.id, event_type: "lane.state.changed", actor_id: actor.id,
  });
  await store.append(lane.id, 0, newId(), [event]);
  const proposed = proposedCheckpoint({
    checkpoint_key: "native-session",
    actor_id: actor.id,
    format: "application/vnd.compforge.pi.session+json;version=1",
    state: { messages: ["hello"] },
    anchor: { lane_id: lane.id, last_applied_seq: 1, last_applied_event_id: event.id },
  });

  const first = await store.saveCheckpoint(0, proposed);

  assert.equal(first.revision, 1);
  assert.deepEqual(await store.saveCheckpoint(0, proposed), first);
  const second = proposedCheckpoint({
    checkpoint_key: proposed.checkpoint_key,
    actor_id: actor.id,
    format: proposed.format,
    state: { messages: ["hello", "world"] },
  });
  assert.equal((await store.saveCheckpoint(1, second)).revision, 2);
  assert.equal((await store.loadLatestCheckpoint(proposed.checkpoint_key))?.id, second.id);
  await assert.rejects(
    store.saveCheckpoint(0, proposedCheckpoint({
      checkpoint_key: proposed.checkpoint_key,
      actor_id: actor.id,
      format: proposed.format,
      state: {},
    })),
    CheckpointConflict,
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

test("run completion links a checkpoint atomically and remains inspectable", async () => {
  const store = new MemoryEventStore();
  const recorder = await LaneRecorder.open({
    store, sessionId: "session", runId: "run", actor: newActor("agent", "plain-loop"),
  });
  const turn = await recorder.startTurn();
  const unresolved = await recorder.beforeToolCall(turn.id, { tool: "charge" });

  const completion = await recorder.completeRunWithCheckpoint("plain-loop", newId(), {
    metadata: { reason: "idle" }, payload: { result: "done" },
  });
  const inspection = inspectRun(await store.loadRun("session", "run"));

  assert.deepEqual(
    completion.receipt.event_ids,
    [completion.checkpointLinked.id, completion.runCompleted.id],
  );
  assert.equal(completion.checkpointLinked.seq + 1, completion.runCompleted.seq);
  assert.equal(completion.checkpointLinked.committed_at, completion.runCompleted.committed_at);
  assert.equal(completion.runCompleted.causation_id, completion.checkpointLinked.id);
  assert.deepEqual(inspection.terminal_events.map((event) => event.id), [completion.runCompleted.id]);
  assert.equal(
    inspection.linked_checkpoints[0]?.checkpoint_id,
    completion.checkpointLinked.payload.checkpoint_id,
  );
  assert.deepEqual(
    inspection.unresolved_attempts.map((attempt) => attempt.attempt_id),
    [unresolved.attempt_id],
  );
});

test("loadRun selects only the requested Run", async () => {
  const store = new MemoryEventStore();
  const actor = newActor("agent");
  await store.createActor(actor);
  const target = newLane("session", "target");
  const other = newLane("session", "other");
  await store.createLane(target);
  await store.createLane(other);
  const targetEvent = proposedEvent({
    lane_id: target.id, subject_id: target.id, event_type: "lane.started", actor_id: actor.id,
  });
  const otherEvent = proposedEvent({
    lane_id: other.id, subject_id: other.id, event_type: "lane.started", actor_id: actor.id,
  });
  await store.append(target.id, 0, newId(), [targetEvent]);
  await store.append(other.id, 0, newId(), [otherEvent]);

  const view = await store.loadRun("session", "target");

  assert.deepEqual(view.lanes.map((lane) => lane.id), [target.id]);
  assert.deepEqual(view.events.map((event) => event.id), [targetEvent.id]);
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
