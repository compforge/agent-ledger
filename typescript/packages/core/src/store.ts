import type { CommitReceipt, EventStream, ProposedEvent, StoredEvent } from "./types.js";

export interface EventStore {
  append(
    stream: EventStream,
    expectedVersion: number,
    appendId: string,
    events: readonly ProposedEvent[],
  ): Promise<CommitReceipt>;
  readStream(stream: EventStream, afterVersion?: number): AsyncIterable<StoredEvent>;
  scanSession(sessionId: string, afterCursor?: string): AsyncIterable<StoredEvent>;
}
