import { EntityConflict, EntityNotFound } from "./memory-store.js";
import type { EventStore } from "./store.js";
import {
  newAction,
  newAttempt,
  newId,
  newLane,
  newTurn,
  proposedEvent,
  type Action,
  type Actor,
  type AppendReceipt,
  type AttemptHandle,
  type JsonValue,
  type Lane,
  type ProposedEvent,
  type StoredEvent,
  type Turn,
} from "./types.js";
import { ActionType, EventType } from "./vocabulary.js";

export interface CausalParent {
  runId: string;
  causationId: string;
}

export interface RecorderOptions {
  store: EventStore;
  sessionId: string;
  runId: string;
  actor: Actor;
  laneId?: string;
  laneName?: string;
  parentLaneId?: string;
  parent?: CausalParent;
}

export class LaneRecorder {
  readonly store: EventStore;
  readonly lane: Lane;
  readonly actor: Actor;
  readonly parent: CausalParent | undefined;
  #expectedLastSeq: number;
  #tail = Promise.resolve();

  private constructor(options: RecorderOptions, lane: Lane) {
    this.store = options.store;
    this.lane = lane;
    this.actor = structuredClone(options.actor);
    this.parent = options.parent === undefined ? undefined : { ...options.parent };
    this.#expectedLastSeq = lane.last_seq;
  }

  static async open(options: RecorderOptions): Promise<LaneRecorder> {
    const storedActor = await options.store.getActor(options.actor.id);
    if (storedActor === undefined) await options.store.createActor(options.actor);
    else if (storedActor.type !== options.actor.type || storedActor.framework !== options.actor.framework) {
      throw new EntityConflict(`actor ${options.actor.id}`);
    }

    const laneName = options.laneName ?? "main";
    let lane: Lane | undefined;
    if (options.laneId !== undefined) {
      lane = await options.store.getLane(options.laneId);
      if (lane === undefined) {
        lane = newLane(options.sessionId, options.runId, laneName, options.parentLaneId);
        lane.id = options.laneId;
        await options.store.createLane(lane);
      }
    } else {
      lane = await options.store.findLane(options.sessionId, options.runId, laneName);
      if (lane === undefined) {
        const candidate = newLane(options.sessionId, options.runId, laneName, options.parentLaneId);
        try {
          await options.store.createLane(candidate);
          lane = candidate;
        } catch (error) {
          if (!(error instanceof EntityConflict)) throw error;
          lane = await options.store.findLane(options.sessionId, options.runId, laneName);
          if (lane === undefined) throw error;
        }
      }
    }
    if (lane.session_id !== options.sessionId || lane.run_id !== options.runId || lane.name !== laneName) {
      throw new Error("lane identity does not match recorder options");
    }
    if (options.parentLaneId !== undefined && lane.parent_lane_id !== options.parentLaneId) {
      throw new Error("lane parent does not match recorder options");
    }
    return new LaneRecorder(options, lane);
  }

  record(
    eventType: string,
    subjectId: string,
    options: {
      payload?: { [key: string]: JsonValue };
      causationId?: string;
      appendId?: string;
    } = {},
  ): Promise<StoredEvent> {
    const event = proposedEvent({
      lane_id: this.lane.id,
      subject_id: subjectId,
      event_type: eventType,
      actor_id: this.actor.id,
      ...(options.causationId === undefined ? {} : { causation_id: options.causationId }),
      payload: options.payload ?? {},
    });
    return this.#appendEvents([event], options.appendId).then(({ events }) => events[0]!);
  }

  /** Atomically appends an ordered Event batch and advances the Lane cursor. */
  append(
    events: readonly ProposedEvent[],
    options: { appendId?: string } = {},
  ): Promise<AppendReceipt> {
    return this.#appendEvents(events, options.appendId).then(({ receipt }) => receipt);
  }

  startRun(payload: { [key: string]: JsonValue } = {}): Promise<StoredEvent> {
    return this.record(EventType.RUN_STARTED, this.lane.run_id, {
      payload: this.parent === undefined ? payload : { ...payload, parent_run_id: this.parent.runId },
      ...(this.parent === undefined ? {} : { causationId: this.parent.causationId }),
    });
  }

  completeRun(payload: { [key: string]: JsonValue } = {}): Promise<StoredEvent> {
    return this.record(EventType.RUN_COMPLETED, this.lane.run_id, { payload });
  }

  failRun(error: unknown): Promise<StoredEvent> {
    return this.record(EventType.RUN_FAILED, this.lane.run_id, { payload: errorPayload(error) });
  }

  async startTurn(payload: { [key: string]: JsonValue } = {}): Promise<Turn> {
    const turn = newTurn(this.lane.id);
    await this.store.createTurn(turn);
    await this.record(EventType.TURN_STARTED, turn.id, { payload });
    return turn;
  }

  completeTurn(turnId: string, payload: { [key: string]: JsonValue } = {}): Promise<StoredEvent> {
    return this.record(EventType.TURN_COMPLETED, turnId, { payload });
  }

  failTurn(turnId: string, error: unknown): Promise<StoredEvent> {
    return this.record(EventType.TURN_FAILED, turnId, { payload: errorPayload(error) });
  }

  beforeModelCall(turnId: string, payload: { [key: string]: JsonValue }): Promise<AttemptHandle> {
    return this.#beforeCall(ActionType.MODEL_CALL, turnId, payload, undefined, 1);
  }

  beforeToolCall(turnId: string, payload: { [key: string]: JsonValue }): Promise<AttemptHandle> {
    return this.#beforeCall(ActionType.TOOL_CALL, turnId, payload, undefined, 1);
  }

  async retry(actionId: string, attemptNo: number, payload: { [key: string]: JsonValue }): Promise<AttemptHandle> {
    const action = await this.store.getAction(actionId);
    if (action === undefined) throw new EntityNotFound(`action ${actionId}`);
    if (action.type !== ActionType.MODEL_CALL && action.type !== ActionType.TOOL_CALL) throw new Error(`action ${actionId} is not retryable`);
    return this.#beforeCall(action.type, action.turn_id, payload, action, attemptNo);
  }

  modelCompleted(attempt: AttemptHandle, payload: { [key: string]: JsonValue }): Promise<StoredEvent> {
    this.#assertActionType(attempt, ActionType.MODEL_CALL);
    return this.#completeAttempt(attempt, payload);
  }

  modelFailed(attempt: AttemptHandle, error: unknown): Promise<StoredEvent> {
    this.#assertActionType(attempt, ActionType.MODEL_CALL);
    return this.#failAttempt(attempt, error);
  }

  toolCompleted(attempt: AttemptHandle, payload: { [key: string]: JsonValue }): Promise<StoredEvent> {
    this.#assertActionType(attempt, ActionType.TOOL_CALL);
    return this.#completeAttempt(attempt, payload);
  }

  toolFailed(attempt: AttemptHandle, error: unknown): Promise<StoredEvent> {
    this.#assertActionType(attempt, ActionType.TOOL_CALL);
    return this.#failAttempt(attempt, error);
  }

  saveSnapshot(profile: string, profileVersion: string, snapshot: JsonValue): Promise<StoredEvent> {
    return this.record(EventType.LANE_FRAMEWORK_SNAPSHOT_SAVED, this.lane.id, {
      payload: { profile, profile_version: profileVersion, snapshot },
    });
  }

  linkCheckpoint(
    profile: string,
    checkpointId: string,
    profileVersion = "1",
    metadata: { [key: string]: JsonValue } = {},
  ): Promise<StoredEvent> {
    return this.#appendEvents([
      this.#checkpointLinkEvent(profile, checkpointId, profileVersion, metadata),
    ]).then(({ events }) => events[0]!);
  }

  async child(options: { runId: string; actor: Actor; causationId: string }): Promise<LaneRecorder> {
    return LaneRecorder.open({
      store: this.store, sessionId: this.lane.session_id, runId: options.runId, actor: options.actor,
      parentLaneId: this.lane.id, parent: { runId: this.lane.run_id, causationId: options.causationId },
    });
  }

  async #beforeCall(
    actionType: string,
    turnId: string,
    payload: { [key: string]: JsonValue },
    action: Action | undefined,
    attemptNo: number,
  ): Promise<AttemptHandle> {
    const targetAction = action ?? newAction(turnId, actionType);
    if (action === undefined) await this.store.createAction(targetAction);
    const attempt = newAttempt(targetAction.id, attemptNo);
    await this.store.createAttempt(attempt);
    const requested = await this.record(EventType.ATTEMPT_REQUESTED, attempt.id, { payload });
    return {
      action_type: actionType, turn_id: turnId, action_id: targetAction.id,
      attempt_id: attempt.id, attempt_no: attemptNo, requested_event_id: requested.id,
    };
  }

  #completeAttempt(attempt: AttemptHandle, payload: { [key: string]: JsonValue }): Promise<StoredEvent> {
    return this.record(EventType.ATTEMPT_COMPLETED, attempt.attempt_id, {
      payload, causationId: attempt.requested_event_id,
    });
  }

  #failAttempt(attempt: AttemptHandle, error: unknown): Promise<StoredEvent> {
    return this.record(EventType.ATTEMPT_FAILED, attempt.attempt_id, {
      payload: errorPayload(error), causationId: attempt.requested_event_id,
    });
  }

  #assertActionType(attempt: AttemptHandle, expected: string): void {
    if (attempt.action_type !== expected) throw new Error(`attempt is not a ${expected}`);
  }

  #checkpointLinkEvent(
    profile: string,
    checkpointId: string,
    profileVersion: string,
    metadata: { [key: string]: JsonValue },
  ): ProposedEvent {
    return proposedEvent({
      lane_id: this.lane.id,
      subject_id: this.lane.id,
      event_type: EventType.LANE_FRAMEWORK_CHECKPOINT_LINKED,
      actor_id: this.actor.id,
      payload: { profile, profile_version: profileVersion, checkpoint_id: checkpointId, metadata },
    });
  }

  #appendEvents(
    events: readonly ProposedEvent[],
    appendId?: string,
  ): Promise<{ events: StoredEvent[]; receipt: AppendReceipt }> {
    return this.#serialize(async () => {
      const receipt = await this.store.append(
        this.lane.id,
        this.#expectedLastSeq,
        appendId ?? newId(),
        events,
      );
      this.#expectedLastSeq = receipt.last_seq;
      this.lane.last_seq = receipt.last_seq;
      const stored = events.map((event, index) => ({
        ...event,
        seq: receipt.first_seq + index,
        committed_at: receipt.committed_at,
      }));
      return { events: stored, receipt };
    });
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
