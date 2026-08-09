import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { test } from "node:test";

import { canonicalAppendDigest, DuplicateEvent, MemoryEventStore, proposedEvent } from "../src/index.js";

test("event streams order framework state independently from semantic runs", async () => {
  const store = new MemoryEventStore();
  const stream = { session_id: "session", stream_id: "framework/pi/native" };
  const first = proposedEvent({
    event_id: "event-1",
    occurred_at: "2026-01-02T03:04:05.000Z",
    event_type: "framework.state.recorded",
    session_id: "session",
    run_id: "run-1",
    actor: { type: "agent", id: "pi" },
  });
  const second = { ...first, event_id: "event-2", run_id: "run-2" };

  await store.append(stream, -1, "append-1", [first]);
  await store.append(stream, 0, "append-2", [second]);

  const events = [];
  for await (const event of store.readStream(stream)) events.push(event);
  assert.deepEqual(events.map((event) => event.run_id), ["run-1", "run-2"]);
  assert.equal(canonicalAppendDigest([first]), canonicalAppendDigest([structuredClone(first)]));
});

test("append digest matches the cross-language vector", async () => {
  const vector = JSON.parse(
    await readFile(resolve(process.cwd(), "../conformance/vectors/append.json"), "utf8"),
  ) as { events: Parameters<typeof canonicalAppendDigest>[0]; sha256: string };
  assert.equal(canonicalAppendDigest(vector.events), vector.sha256);
});

test("event id uniqueness is scoped to a session", async () => {
  const store = new MemoryEventStore();
  const original = proposedEvent({
    event_id: "shared-event-id",
    occurred_at: "2026-01-02T03:04:05.000Z",
    event_type: "test.recorded",
    session_id: "session-1",
    run_id: "run-1",
    actor: { type: "agent", id: "test" },
  });
  const reused = { ...original, session_id: "session-2", run_id: "run-2" };

  await store.append({ session_id: "session-1", stream_id: "run-1" }, -1, "append-1", [original]);
  await store.append({ session_id: "session-2", stream_id: "run-2" }, -1, "append-2", [reused]);
});

test("duplicate event ids reject the whole append batch", async () => {
  const store = new MemoryEventStore();
  const stream = { session_id: "session", stream_id: "run" };
  const first = proposedEvent({
    event_id: "duplicate-event",
    occurred_at: "2026-01-02T03:04:05.000Z",
    event_type: "test.first",
    session_id: stream.session_id,
    run_id: stream.stream_id,
    actor: { type: "agent", id: "test" },
  });

  await assert.rejects(
    store.append(stream, -1, "append", [first, { ...first, event_type: "test.second" }]),
    DuplicateEvent,
  );
  const stored = [];
  for await (const event of store.readStream(stream)) stored.push(event);
  assert.deepEqual(stored, []);
});
