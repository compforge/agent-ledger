import { canonicalAppendDigest } from "./canonical.js";
import type { EventStore } from "./store.js";
import type { CommitReceipt, EventStream, ProposedEvent, StoredEvent } from "./types.js";

export class StreamConflict extends Error {}
export class IdempotencyViolation extends Error {}
export class DuplicateEvent extends Error {}

export class MemoryEventStore implements EventStore {
  readonly #streams = new Map<string, StoredEvent[]>();
  readonly #sessions = new Map<string, StoredEvent[]>();
  readonly #receipts = new Map<string, CommitReceipt>();
  readonly #eventIds = new Set<string>();

  async append(
    stream: EventStream,
    expectedVersion: number,
    appendId: string,
    events: readonly ProposedEvent[],
  ): Promise<CommitReceipt> {
    if (events.length === 0) throw new TypeError("append requires at least one event");
    if (events.some((event) => event.session_id !== stream.session_id)) {
      throw new TypeError("all events must belong to the target stream's session");
    }
    const batch = structuredClone(events) as ProposedEvent[];
    const digest = canonicalAppendDigest(batch);
    const receiptKey = `${stream.session_id}\u0000${stream.stream_id}\u0000${appendId}`;
    const previous = this.#receipts.get(receiptKey);
    if (previous) {
      if (previous.digest !== digest) throw new IdempotencyViolation(appendId);
      return structuredClone(previous);
    }
    const streamKey = `${stream.session_id}\u0000${stream.stream_id}`;
    const current = this.#streams.get(streamKey) ?? [];
    if (current.length - 1 !== expectedVersion) throw new StreamConflict(stream.stream_id);
    const batchEventIds = new Set<string>();
    for (const event of batch) {
      if (batchEventIds.has(event.event_id)) throw new DuplicateEvent(event.event_id);
      batchEventIds.add(event.event_id);
      if (this.#eventIds.has(eventKey(stream.session_id, event.event_id))) {
        throw new DuplicateEvent(event.event_id);
      }
    }

    const session = this.#sessions.get(stream.session_id) ?? [];
    const committedAt = new Date().toISOString();
    const stored = batch.map((event, offset): StoredEvent => ({
      ...event,
      stream_id: stream.stream_id,
      stream_version: expectedVersion + offset + 1,
      commit_cursor: String(session.length + offset),
      committed_at: committedAt,
    }));
    const receipt: CommitReceipt = {
      stream: structuredClone(stream),
      append_id: appendId,
      digest,
      first_version: stored[0]!.stream_version,
      last_version: stored.at(-1)!.stream_version,
      first_cursor: stored[0]!.commit_cursor,
      last_cursor: stored.at(-1)!.commit_cursor,
      event_ids: stored.map((event) => event.event_id),
      committed_at: committedAt,
    };
    this.#streams.set(streamKey, [...current, ...stored]);
    this.#sessions.set(stream.session_id, [...session, ...stored]);
    for (const event of stored) this.#eventIds.add(eventKey(stream.session_id, event.event_id));
    this.#receipts.set(receiptKey, receipt);
    return structuredClone(receipt);
  }

  async *readStream(stream: EventStream, afterVersion = -1): AsyncIterable<StoredEvent> {
    const key = `${stream.session_id}\u0000${stream.stream_id}`;
    for (const event of this.#streams.get(key) ?? []) {
      if (event.stream_version > afterVersion) yield structuredClone(event);
    }
  }

  async *scanSession(sessionId: string, afterCursor = "-1"): AsyncIterable<StoredEvent> {
    const cursor = Number.parseInt(afterCursor, 10);
    if (!Number.isInteger(cursor) || cursor < -1) throw new TypeError(`invalid cursor ${afterCursor}`);
    for (const event of this.#sessions.get(sessionId) ?? []) {
      if (Number.parseInt(event.commit_cursor, 10) > cursor) yield structuredClone(event);
    }
  }
}

function eventKey(sessionId: string, eventId: string): string {
  return `${sessionId}\u0000${eventId}`;
}
