import assert from "node:assert/strict";
import { test } from "node:test";

import {
  type CommitReceipt,
  type EventStream,
  MemoryEventStore,
  type ProposedEvent,
  SessionRecorder,
} from "@agent-ledger/core";

import { bindPiHarness, LedgerPiSessionStorage } from "../src/index.js";

test("Pi native entries round-trip through ledger storage", async () => {
  const store = new MemoryEventStore();
  const actor = { type: "agent" as const, id: "pi", framework: "pi" };
  const storage = await LedgerPiSessionStorage.create({
    store,
    sessionId: "ledger-session",
    runId: "runtime-1",
    actor,
    metadata: { id: "pi-session", createdAt: "2026-01-02T03:04:05.000Z" },
  });
  await storage.appendEntry({
    type: "message",
    id: "entry-1",
    parentId: null,
    timestamp: "2026-01-02T03:04:06.000Z",
    message: { role: "user", content: "hello", timestamp: Date.now() },
  });

  const restored = await LedgerPiSessionStorage.open({
    store,
    sessionId: "ledger-session",
    nativeSessionId: "pi-session",
    runId: "runtime-2",
    actor,
  });

  assert.equal(await restored.getLeafId(), "entry-1");
  assert.deepEqual(await restored.getPathToRoot("entry-1"), await storage.getEntries());
});

test("Pi direct harness hooks record model boundaries", async () => {
  const store = new MemoryEventStore();
  const recorder = new SessionRecorder({
    store,
    sessionId: "session",
    runId: "run",
    actor: { type: "agent", id: "pi" },
  });
  const harness = new FakeHarness();
  bindPiHarness(harness, recorder);

  await harness.emit("turn_start", {});
  await harness.hook("context", { messages: [{ role: "user", content: "hello" }] });
  await harness.hook("before_provider_request", { model: { id: "model", provider: "test" } });
  await harness.emit("message_end", { message: { role: "assistant", content: "done", stopReason: "stop" } });
  await harness.emit("turn_end", {});

  const events = [];
  for await (const event of store.readStream({ session_id: "session", stream_id: "run" })) events.push(event);
  assert.deepEqual(events.map((event) => event.event_type), [
    "step.started",
    "model.requested",
    "model.completed",
    "step.completed",
  ]);
});

test("Pi direct harness hook fails closed before model execution", async () => {
  const store = new ModelPrewriteFailingStore();
  const recorder = new SessionRecorder({
    store,
    sessionId: "session",
    runId: "run",
    actor: { type: "agent", id: "pi" },
  });
  const harness = new FakeHarness();
  bindPiHarness(harness, recorder);
  await harness.emit("turn_start", {});
  let modelStarted = false;

  await assert.rejects(async () => {
    await harness.hook("before_provider_request", { model: { id: "model", provider: "test" } });
    modelStarted = true;
  }, /prewrite failed/);
  assert.equal(modelStarted, false);
});

test("Pi model errors fail the enclosing step and run", async () => {
  const store = new MemoryEventStore();
  const recorder = new SessionRecorder({
    store,
    sessionId: "session",
    runId: "run",
    actor: { type: "agent", id: "pi" },
  });
  const harness = new FakeHarness();
  bindPiHarness(harness, recorder);

  await harness.emit("agent_start", {});
  await harness.emit("turn_start", {});
  await harness.hook("before_provider_request", { model: { id: "model", provider: "test" } });
  await harness.emit("message_end", {
    message: { role: "assistant", stopReason: "error", errorMessage: "rate limited" },
  });
  await harness.emit("turn_end", {});
  await harness.emit("agent_end", {});

  const events = [];
  for await (const event of store.readStream({ session_id: "session", stream_id: "run" })) events.push(event);
  assert.deepEqual(events.map((event) => event.event_type), [
    "run.started",
    "step.started",
    "model.requested",
    "model.failed",
    "step.failed",
    "run.failed",
  ]);
});

class FakeHarness {
  readonly #hooks = new Map<string, Array<(event: any) => any>>();
  readonly #listeners: Array<(event: any) => Promise<void> | void> = [];

  on(type: any, handler: (event: any) => any): () => void {
    const handlers = this.#hooks.get(String(type)) ?? [];
    handlers.push(handler);
    this.#hooks.set(String(type), handlers);
    return () => { handlers.splice(handlers.indexOf(handler), 1); };
  }

  subscribe(listener: (event: any) => Promise<void> | void): () => void {
    this.#listeners.push(listener);
    return () => { this.#listeners.splice(this.#listeners.indexOf(listener), 1); };
  }

  async hook(type: string, event: Record<string, unknown>): Promise<void> {
    for (const handler of this.#hooks.get(type) ?? []) await handler({ type, ...event });
  }

  async emit(type: string, event: Record<string, unknown>): Promise<void> {
    for (const listener of this.#listeners) await listener({ type, ...event });
  }
}

class ModelPrewriteFailingStore extends MemoryEventStore {
  override async append(
    stream: EventStream,
    expectedVersion: number,
    appendId: string,
    events: readonly ProposedEvent[],
  ): Promise<CommitReceipt> {
    if (events.some((event) => event.event_type === "model.requested")) throw new Error("prewrite failed");
    return super.append(stream, expectedVersion, appendId, events);
  }
}
