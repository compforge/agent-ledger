import { canonicalAppendDigest, canonicalize } from "./canonical.js";
import type { CheckpointStore, EventStore } from "./store.js";
import type {
  Action, Actor, AppendReceipt, Attempt, Checkpoint, Lane, ProposedCheckpoint, ProposedEvent,
  RunView, SessionView, StoredEvent, Turn,
} from "./types.js";
import { selectRun } from "./types.js";

export class LaneConflict extends Error {}
export class CheckpointConflict extends Error {}
export class IdempotencyViolation extends Error {}
export class CheckpointIdempotencyViolation extends Error {}
export class DuplicateEvent extends Error {}
export class EntityConflict extends Error {}
export class EntityNotFound extends Error {}
export class SubjectMismatch extends Error {}

export class MemoryEventStore implements EventStore, CheckpointStore {
  readonly #actors = new Map<string, Actor>();
  readonly #actorKeys = new Map<string, string>();
  readonly #lanes = new Map<string, Lane>();
  readonly #laneNames = new Map<string, string>();
  readonly #turns = new Map<string, Turn>();
  readonly #actions = new Map<string, Action>();
  readonly #attempts = new Map<string, Attempt>();
  readonly #attemptNumbers = new Set<string>();
  readonly #events = new Map<string, StoredEvent>();
  readonly #laneEvents = new Map<string, StoredEvent[]>();
  readonly #appends = new Map<string, AppendReceipt>();
  readonly #checkpoints = new Map<string, Checkpoint>();
  readonly #latestCheckpoints = new Map<string, string>();

  async createActor(actor: Actor): Promise<void> {
    if (actor.key !== undefined && this.#actorKeys.has(actor.key)) throw new EntityConflict(`actor key ${actor.key}`);
    this.#create(this.#actors, "actor", actor.id, actor);
    if (actor.key !== undefined) this.#actorKeys.set(actor.key, actor.id);
  }

  async getActor(id: string): Promise<Actor | undefined> {
    return cloneOptional(this.#actors.get(id));
  }

  async getActorByKey(key: string): Promise<Actor | undefined> {
    const id = this.#actorKeys.get(key);
    return id === undefined ? undefined : cloneOptional(this.#actors.get(id));
  }

  async ensureActor(actor: Actor): Promise<Actor> {
    const stored = actor.key === undefined
      ? this.#actors.get(actor.id)
      : this.#actors.get(this.#actorKeys.get(actor.key) ?? "");
    if (stored !== undefined) {
      requireSameActor(stored, actor);
      return structuredClone(stored);
    }
    await this.createActor(actor);
    return structuredClone(actor);
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

  async saveCheckpoint(expectedRevision: number, proposed: ProposedCheckpoint): Promise<Checkpoint> {
    if (!Number.isSafeInteger(expectedRevision) || expectedRevision < 0) {
      throw new TypeError("expectedRevision must be a non-negative safe integer");
    }
    validateCheckpoint(proposed);
    const checkpoint = structuredClone(proposed);
    const previous = this.#checkpoints.get(checkpoint.id);
    if (previous) {
      if (!sameCheckpoint(previous, checkpoint)) throw new CheckpointIdempotencyViolation(checkpoint.id);
      return structuredClone(previous);
    }
    if (!this.#actors.has(checkpoint.actor_id)) throw new EntityNotFound(`actor ${checkpoint.actor_id}`);
    const latestId = this.#latestCheckpoints.get(checkpoint.key);
    const actualRevision = latestId === undefined ? 0 : this.#checkpoints.get(latestId)!.revision;
    if (actualRevision !== expectedRevision) {
      throw new CheckpointConflict(`expected ${expectedRevision}, actual ${actualRevision}`);
    }
    if (checkpoint.anchor !== undefined) {
      const event = this.#events.get(checkpoint.anchor.last_applied_event_id);
      if (event === undefined
        || event.lane_id !== checkpoint.anchor.lane_id
        || event.seq !== checkpoint.anchor.last_applied_seq) {
        throw new TypeError("checkpoint anchor must identify an existing lane event");
      }
    }
    const stored: Checkpoint = {
      ...checkpoint,
      revision: actualRevision + 1,
      created_at: new Date().toISOString(),
    };
    this.#checkpoints.set(stored.id, stored);
    this.#latestCheckpoints.set(stored.key, stored.id);
    return structuredClone(stored);
  }

  async getCheckpoint(id: string): Promise<Checkpoint | undefined> {
    return cloneOptional(this.#checkpoints.get(id));
  }

  async loadLatestCheckpoint(key: string): Promise<Checkpoint | undefined> {
    const id = this.#latestCheckpoints.get(key);
    return id === undefined ? undefined : structuredClone(this.#checkpoints.get(id)!);
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

  async loadRun(sessionId: string, runId: string): Promise<RunView> {
    return selectRun(await this.loadSession(sessionId), runId);
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

function requireSameActor(stored: Actor, proposed: Actor): void {
  if (stored.key !== proposed.key || stored.type !== proposed.type || stored.framework !== proposed.framework) {
    throw new EntityConflict(`actor key ${proposed.key ?? proposed.id}`);
  }
}

function validateCheckpoint(value: ProposedCheckpoint): void {
  if (value.schema_version !== "1.0" || value.id === "" || value.key === ""
    || value.actor_id === "" || value.format === "") {
    throw new TypeError("checkpoint requires schema version, id, key, actor, and format");
  }
  if (("state" in value) === ("artifact_ref" in value)) {
    throw new TypeError("exactly one of state and artifact_ref must be set");
  }
  if (value.anchor !== undefined && (!Number.isSafeInteger(value.anchor.last_applied_seq)
    || value.anchor.last_applied_seq < 1 || value.anchor.lane_id === ""
    || value.anchor.last_applied_event_id === "")) {
    throw new TypeError("checkpoint anchor requires lane, positive seq, and event");
  }
}

function sameCheckpoint(stored: Checkpoint, proposed: ProposedCheckpoint): boolean {
  const { revision: _revision, created_at: _createdAt, ...existing } = stored;
  return canonicalize(existing) === canonicalize(proposed);
}
