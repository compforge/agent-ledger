import { canonicalAppendDigest } from "./canonical.js";
import type { EventStore } from "./store.js";
import type {
  Action, Actor, AppendReceipt, Attempt, Lane, ProposedEvent, SessionView, StoredEvent, Turn,
} from "./types.js";

export class LaneConflict extends Error {}
export class IdempotencyViolation extends Error {}
export class DuplicateEvent extends Error {}
export class EntityConflict extends Error {}
export class EntityNotFound extends Error {}
export class SubjectMismatch extends Error {}

export class MemoryEventStore implements EventStore {
  readonly #actors = new Map<string, Actor>();
  readonly #lanes = new Map<string, Lane>();
  readonly #laneNames = new Map<string, string>();
  readonly #turns = new Map<string, Turn>();
  readonly #actions = new Map<string, Action>();
  readonly #attempts = new Map<string, Attempt>();
  readonly #attemptNumbers = new Set<string>();
  readonly #events = new Map<string, StoredEvent>();
  readonly #laneEvents = new Map<string, StoredEvent[]>();
  readonly #appends = new Map<string, AppendReceipt>();

  async createActor(actor: Actor): Promise<void> {
    this.#create(this.#actors, "actor", actor.id, actor);
  }

  async getActor(id: string): Promise<Actor | undefined> {
    return cloneOptional(this.#actors.get(id));
  }

  async createLane(lane: Lane): Promise<void> {
    if (lane.last_seq !== 0) throw new TypeError("new lane must have last_seq 0");
    if (this.#lanes.has(lane.id) || this.#laneNames.has(laneNameKey(lane.session_id, lane.run_id, lane.name))) {
      throw new EntityConflict(`lane ${lane.id}`);
    }
    if (lane.parent_lane_id !== undefined) {
      const parent = this.#lanes.get(lane.parent_lane_id);
      if (!parent) throw new EntityNotFound(`parent lane ${lane.parent_lane_id}`);
      if (parent.session_id !== lane.session_id) throw new TypeError("parent lane must belong to same session");
    }
    this.#lanes.set(lane.id, structuredClone(lane));
    this.#laneNames.set(laneNameKey(lane.session_id, lane.run_id, lane.name), lane.id);
  }

  async getLane(id: string): Promise<Lane | undefined> {
    return cloneOptional(this.#lanes.get(id));
  }

  async findLane(sessionId: string, runId: string, name: string): Promise<Lane | undefined> {
    const id = this.#laneNames.get(laneNameKey(sessionId, runId, name));
    return id === undefined ? undefined : structuredClone(this.#lanes.get(id)!);
  }

  async createTurn(turn: Turn): Promise<void> {
    if (!this.#lanes.has(turn.lane_id)) throw new EntityNotFound(`lane ${turn.lane_id}`);
    this.#create(this.#turns, "turn", turn.id, turn);
  }

  async getTurn(id: string): Promise<Turn | undefined> {
    return cloneOptional(this.#turns.get(id));
  }

  async createAction(action: Action): Promise<void> {
    if (!this.#turns.has(action.turn_id)) throw new EntityNotFound(`turn ${action.turn_id}`);
    if (action.parent_action_id !== undefined) {
      const parent = this.#actions.get(action.parent_action_id);
      if (!parent) throw new EntityNotFound(`parent action ${action.parent_action_id}`);
      if (parent.turn_id !== action.turn_id) throw new TypeError("parent action must belong to same turn");
    }
    this.#create(this.#actions, "action", action.id, action);
  }

  async getAction(id: string): Promise<Action | undefined> {
    return cloneOptional(this.#actions.get(id));
  }

  async createAttempt(attempt: Attempt): Promise<void> {
    if (attempt.attempt_no < 1) throw new TypeError("attempt_no must be positive");
    if (!this.#actions.has(attempt.action_id)) throw new EntityNotFound(`action ${attempt.action_id}`);
    const numberKey = `${attempt.action_id}\u0000${attempt.attempt_no}`;
    if (this.#attemptNumbers.has(numberKey)) throw new EntityConflict(`attempt ${attempt.id}`);
    this.#create(this.#attempts, "attempt", attempt.id, attempt);
    this.#attemptNumbers.add(numberKey);
  }

  async getAttempt(id: string): Promise<Attempt | undefined> {
    return cloneOptional(this.#attempts.get(id));
  }

  async append(
    laneId: string,
    expectedLastSeq: number,
    appendId: string,
    events: readonly ProposedEvent[],
  ): Promise<AppendReceipt> {
    if (expectedLastSeq < 0) throw new TypeError("expectedLastSeq must be non-negative");
    if (appendId === "" || events.length === 0) throw new TypeError("append requires an id and at least one event");
    const batch = structuredClone(events) as ProposedEvent[];
    const digest = canonicalAppendDigest(batch);
    const previous = this.#appends.get(appendId);
    if (previous) {
      if (previous.lane_id !== laneId || previous.digest !== digest) throw new IdempotencyViolation(appendId);
      return structuredClone(previous);
    }
    const lane = this.#lanes.get(laneId);
    if (!lane) throw new EntityNotFound(`lane ${laneId}`);
    if (lane.last_seq !== expectedLastSeq) throw new LaneConflict(laneId);
    const prior = new Set(this.#events.keys());
    const batchIds = new Set<string>();
    for (const event of batch) {
      if (event.lane_id !== laneId) throw new TypeError("all events must belong to target lane");
      if (batchIds.has(event.id) || this.#events.has(event.id)) throw new DuplicateEvent(event.id);
      batchIds.add(event.id);
      if (!this.#actors.has(event.actor_id)) throw new EntityNotFound(`actor ${event.actor_id}`);
      if (!this.#validSubject(lane, event)) throw new SubjectMismatch(event.id);
      if (event.causation_id !== undefined) {
        if (!prior.has(event.causation_id)) throw new EntityNotFound(`causation event ${event.causation_id}`);
        const caused = this.#events.get(event.causation_id);
        if (caused && this.#lanes.get(caused.lane_id)?.session_id !== lane.session_id) {
          throw new SubjectMismatch(event.id);
        }
      }
      prior.add(event.id);
    }

    const committedAt = new Date().toISOString();
    const stored = batch.map((event, index): StoredEvent => ({
      ...event, seq: lane.last_seq + index + 1, committed_at: committedAt,
    }));
    const receipt: AppendReceipt = {
      id: appendId, lane_id: laneId, digest, first_seq: stored[0]!.seq,
      last_seq: stored.at(-1)!.seq, event_ids: stored.map((event) => event.id), committed_at: committedAt,
    };
    for (const event of stored) this.#events.set(event.id, event);
    lane.last_seq = receipt.last_seq;
    this.#laneEvents.set(laneId, [...(this.#laneEvents.get(laneId) ?? []), ...stored]);
    this.#appends.set(appendId, receipt);
    return structuredClone(receipt);
  }

  async *loadLane(laneId: string, afterSeq = 0): AsyncIterable<StoredEvent> {
    if (afterSeq < 0) throw new TypeError("afterSeq must be non-negative");
    if (!this.#lanes.has(laneId)) throw new EntityNotFound(`lane ${laneId}`);
    for (const event of this.#laneEvents.get(laneId) ?? []) {
      if (event.seq > afterSeq) yield structuredClone(event);
    }
  }

  async loadSession(sessionId: string): Promise<SessionView> {
    const lanes = [...this.#lanes.values()].filter((lane) => lane.session_id === sessionId);
    const laneIds = new Set(lanes.map((lane) => lane.id));
    const turns = [...this.#turns.values()].filter((turn) => laneIds.has(turn.lane_id));
    const turnIds = new Set(turns.map((turn) => turn.id));
    const actions = [...this.#actions.values()].filter((action) => turnIds.has(action.turn_id));
    const actionIds = new Set(actions.map((action) => action.id));
    const attempts = [...this.#attempts.values()].filter((attempt) => actionIds.has(attempt.action_id));
    const events = [...this.#events.values()]
      .filter((event) => laneIds.has(event.lane_id))
      .sort((left, right) => left.committed_at.localeCompare(right.committed_at)
        || left.lane_id.localeCompare(right.lane_id)
        || left.seq - right.seq
        || left.id.localeCompare(right.id));
    const actorIds = new Set(events.map((event) => event.actor_id));
    const actors = [...this.#actors.values()].filter((actor) => actorIds.has(actor.id));
    return structuredClone({ session_id: sessionId, actors, lanes, turns, actions, attempts, events });
  }

  #create<T>(target: Map<string, T>, kind: string, id: string, value: T): void {
    if (target.has(id)) throw new EntityConflict(`${kind} ${id}`);
    target.set(id, structuredClone(value));
  }

  #validSubject(lane: Lane, event: ProposedEvent): boolean {
    const kind = event.event_type.split(".", 1)[0];
    if (kind === "session") return event.subject_id === lane.session_id;
    if (kind === "run") return event.subject_id === lane.run_id;
    if (kind === "lane") return event.subject_id === lane.id;
    if (kind === "turn") return this.#turns.get(event.subject_id)?.lane_id === lane.id;
    if (kind === "action") {
      const action = this.#actions.get(event.subject_id);
      return action !== undefined && this.#turns.get(action.turn_id)?.lane_id === lane.id;
    }
    if (kind === "attempt") {
      const attempt = this.#attempts.get(event.subject_id);
      const action = attempt === undefined ? undefined : this.#actions.get(attempt.action_id);
      return action !== undefined && this.#turns.get(action.turn_id)?.lane_id === lane.id;
    }
    return false;
  }
}

function laneNameKey(sessionId: string, runId: string, name: string): string {
  return `${sessionId}\u0000${runId}\u0000${name}`;
}

function cloneOptional<T>(value: T | undefined): T | undefined {
  return value === undefined ? undefined : structuredClone(value);
}
