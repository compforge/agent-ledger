import type { EventStore } from "./store.js";
import { proposedEvent, type Actor, type AttemptHandle, type EventStream, type JsonValue, type StoredEvent } from "./types.js";

export interface CausalParent {
  runId: string;
  causedByEventId: string;
}

export interface RecorderOptions {
  store: EventStore;
  sessionId: string;
  runId: string;
  actor: Actor;
  streamId?: string;
  parent?: CausalParent;
  expectedVersion?: number;
}

export class SessionRecorder {
  readonly store: EventStore;
  readonly stream: EventStream;
  readonly runId: string;
  readonly actor: Actor;
  readonly parent: CausalParent | undefined;
  #expectedVersion: number;
  #tail = Promise.resolve();

  constructor(options: RecorderOptions) {
    this.store = options.store;
    this.runId = options.runId;
    this.actor = options.actor;
    this.parent = options.parent === undefined ? undefined : { ...options.parent };
    this.stream = { session_id: options.sessionId, stream_id: options.streamId ?? options.runId };
    this.#expectedVersion = options.expectedVersion ?? -1;
  }

  static async resume(options: Omit<RecorderOptions, "expectedVersion">): Promise<SessionRecorder> {
    let version = -1;
    const stream = { session_id: options.sessionId, stream_id: options.streamId ?? options.runId };
    for await (const event of options.store.readStream(stream)) version = event.stream_version;
    return new SessionRecorder({ ...options, expectedVersion: version });
  }

  async record(
    eventType: string,
    options: {
      payload?: { [key: string]: JsonValue };
      stepId?: string;
      attemptId?: string;
      parentRunId?: string;
      causedByEventId?: string;
      appendId?: string;
    } = {},
  ): Promise<StoredEvent> {
    if ((options.parentRunId === undefined) !== (options.causedByEventId === undefined)) {
      throw new Error("parentRunId and causedByEventId must be set together");
    }
    return this.#serialize(async () => {
      const event = proposedEvent({
        event_type: eventType,
        session_id: this.stream.session_id,
        run_id: this.runId,
        actor: this.actor,
        ...(options.stepId === undefined ? {} : { step_id: options.stepId }),
        ...(options.attemptId === undefined ? {} : { attempt_id: options.attemptId }),
        ...(options.parentRunId === undefined ? {} : { parent_run_id: options.parentRunId }),
        ...(options.causedByEventId === undefined ? {} : { caused_by_event_id: options.causedByEventId }),
        payload: options.payload ?? {},
      });
      const receipt = await this.store.append(
        this.stream,
        this.#expectedVersion,
        options.appendId ?? crypto.randomUUID(),
        [event],
      );
      this.#expectedVersion = receipt.last_version;
      return {
        ...event,
        stream_id: this.stream.stream_id,
        stream_version: receipt.first_version,
        commit_cursor: receipt.first_cursor,
        committed_at: receipt.committed_at,
      };
    });
  }

  startRun(payload: { [key: string]: JsonValue } = {}): Promise<StoredEvent> {
    return this.record("run.started", {
      payload,
      ...(this.parent === undefined
        ? {}
        : { parentRunId: this.parent.runId, causedByEventId: this.parent.causedByEventId }),
    });
  }

  child(options: {
    runId: string;
    actor: Actor;
    causedByEventId: string;
    streamId?: string;
  }): SessionRecorder {
    return new SessionRecorder({
      store: this.store,
      sessionId: this.stream.session_id,
      runId: options.runId,
      actor: options.actor,
      ...(options.streamId === undefined ? {} : { streamId: options.streamId }),
      parent: { runId: this.runId, causedByEventId: options.causedByEventId },
    });
  }

  async beforeModelCall(stepId: string, payload: { [key: string]: JsonValue }): Promise<AttemptHandle> {
    return this.#beforeCall("model", stepId, payload);
  }

  async beforeToolCall(stepId: string, payload: { [key: string]: JsonValue }): Promise<AttemptHandle> {
    return this.#beforeCall("tool", stepId, payload);
  }

  modelCompleted(attempt: AttemptHandle, payload: { [key: string]: JsonValue }): Promise<StoredEvent> {
    return this.#finishCall(attempt, "model.completed", payload);
  }

  modelFailed(attempt: AttemptHandle, error: unknown): Promise<StoredEvent> {
    return this.#finishCall(attempt, "model.failed", errorPayload(error));
  }

  toolCompleted(attempt: AttemptHandle, payload: { [key: string]: JsonValue }): Promise<StoredEvent> {
    return this.#finishCall(attempt, "tool.completed", payload);
  }

  toolFailed(attempt: AttemptHandle, error: unknown): Promise<StoredEvent> {
    return this.#finishCall(attempt, "tool.failed", errorPayload(error));
  }

  async #beforeCall(
    kind: "model" | "tool",
    stepId: string,
    payload: { [key: string]: JsonValue },
  ): Promise<AttemptHandle> {
    const attemptId = crypto.randomUUID();
    const event = await this.record(`${kind}.requested`, { stepId, attemptId, payload });
    return { kind, step_id: stepId, attempt_id: attemptId, requested_event_id: event.event_id };
  }

  #finishCall(
    attempt: AttemptHandle,
    eventType: string,
    payload: { [key: string]: JsonValue },
  ): Promise<StoredEvent> {
    return this.record(eventType, { stepId: attempt.step_id, attemptId: attempt.attempt_id, payload });
  }

  async #serialize<T>(operation: () => Promise<T>): Promise<T> {
    const ready = this.#tail;
    let release!: () => void;
    this.#tail = new Promise<void>((resolve) => { release = resolve; });
    await ready;
    try {
      return await operation();
    } finally {
      release();
    }
  }
}

function errorPayload(error: unknown): { [key: string]: JsonValue } {
  return { error: error instanceof Error ? error.message : String(error) };
}
