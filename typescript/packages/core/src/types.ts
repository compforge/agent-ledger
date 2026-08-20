export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export interface Actor {
  id: string;
  type: string;
  framework?: string;
  created_at: string;
}

export interface Lane {
  id: string;
  session_id: string;
  run_id: string;
  name: string;
  parent_lane_id?: string;
  last_seq: number;
  created_at: string;
}

export interface Turn {
  id: string;
  lane_id: string;
  created_at: string;
}

export interface Action {
  id: string;
  turn_id: string;
  type: string;
  parent_action_id?: string;
  created_at: string;
}

export interface Attempt {
  id: string;
  action_id: string;
  attempt_no: number;
  created_at: string;
}

export interface ArtifactRef {
  uri: string;
  sha256: string;
  size: number;
  content_type: string;
}

export interface CheckpointAnchor {
  lane_id: string;
  last_applied_seq: number;
  last_applied_event_id: string;
}

interface ProposedCheckpointBase {
  schema_version: "1.0";
  id: string;
  checkpoint_key: string;
  actor_id: string;
  format: string;
  anchor?: CheckpointAnchor;
  extensions: { [key: string]: JsonValue };
}

export type ProposedCheckpoint = ProposedCheckpointBase & (
  | { state: { [key: string]: JsonValue }; artifact_ref?: never }
  | { artifact_ref: ArtifactRef; state?: never }
);

export type Checkpoint = ProposedCheckpoint & {
  revision: number;
  created_at: string;
};

export interface ProposedEvent {
  schema_version: "1.0";
  id: string;
  lane_id: string;
  subject_id: string;
  event_type: string;
  actor_id: string;
  causation_id?: string;
  occurred_at: string;
  payload: { [key: string]: JsonValue };
  extensions: { [key: string]: JsonValue };
}

export interface StoredEvent extends ProposedEvent {
  seq: number;
  committed_at: string;
}

export interface AppendReceipt {
  id: string;
  lane_id: string;
  digest: string;
  first_seq: number;
  last_seq: number;
  event_ids: string[];
  committed_at: string;
}

export interface SessionView {
  session_id: string;
  actors: Actor[];
  lanes: Lane[];
  turns: Turn[];
  actions: Action[];
  attempts: Attempt[];
  events: StoredEvent[];
}

export interface RunView {
  session_id: string;
  run_id: string;
  actors: Actor[];
  lanes: Lane[];
  turns: Turn[];
  actions: Action[];
  attempts: Attempt[];
  events: StoredEvent[];
}

export function selectRun(view: SessionView, runId: string): RunView {
  const lanes = view.lanes.filter((lane) => lane.run_id === runId);
  const laneIds = new Set(lanes.map((lane) => lane.id));
  const turns = view.turns.filter((turn) => laneIds.has(turn.lane_id));
  const turnIds = new Set(turns.map((turn) => turn.id));
  const actions = view.actions.filter((action) => turnIds.has(action.turn_id));
  const actionIds = new Set(actions.map((action) => action.id));
  const attempts = view.attempts.filter((attempt) => actionIds.has(attempt.action_id));
  const events = view.events.filter((event) => laneIds.has(event.lane_id));
  const actorIds = new Set(events.map((event) => event.actor_id));
  const actors = view.actors.filter((actor) => actorIds.has(actor.id));
  return structuredClone({
    session_id: view.session_id, run_id: runId, actors, lanes, turns, actions, attempts, events,
  });
}

export type RecordingGuarantee = "strict" | "best_effort" | "unsupported";
export type RecoveryMode = "native_store" | "snapshot" | "checkpoint" | "unsupported";

export interface AdapterDescriptor {
  schema_version: "1.0";
  adapter_id: string;
  adapter_version: string;
  framework: string;
  framework_version: string;
  capabilities: {
    model_prewrite: RecordingGuarantee;
    tool_prewrite: RecordingGuarantee;
    outcome_gate: RecordingGuarantee;
    recovery: RecoveryMode;
    preserves_native_state: boolean;
  };
}

export interface AttemptHandle {
  action_type: string;
  turn_id: string;
  action_id: string;
  attempt_id: string;
  attempt_no: number;
  requested_event_id: string;
}

export function newId(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  let timestamp = BigInt(Date.now());
  for (let index = 5; index >= 0; index -= 1) {
    bytes[index] = Number(timestamp & 0xffn);
    timestamp >>= 8n;
  }
  bytes[6] = (bytes[6]! & 0x0f) | 0x70;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = Array.from(bytes, (value) => value.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export function newActor(type: string, framework?: string): Actor {
  return { id: newId(), type, ...(framework === undefined ? {} : { framework }), created_at: now() };
}

export function newLane(sessionId: string, runId: string, name = "main", parentLaneId?: string): Lane {
  return {
    id: newId(), session_id: sessionId, run_id: runId, name,
    ...(parentLaneId === undefined ? {} : { parent_lane_id: parentLaneId }),
    last_seq: 0, created_at: now(),
  };
}

export function newTurn(laneId: string): Turn {
  return { id: newId(), lane_id: laneId, created_at: now() };
}

export function newAction(turnId: string, type: string, parentActionId?: string): Action {
  return {
    id: newId(), turn_id: turnId, type,
    ...(parentActionId === undefined ? {} : { parent_action_id: parentActionId }),
    created_at: now(),
  };
}

export function newAttempt(actionId: string, attemptNo: number): Attempt {
  return { id: newId(), action_id: actionId, attempt_no: attemptNo, created_at: now() };
}

export function proposedCheckpoint(
  checkpoint: Omit<ProposedCheckpoint, "schema_version" | "id" | "extensions"> &
    Partial<Pick<ProposedCheckpointBase, "id" | "extensions">>,
): ProposedCheckpoint {
  return {
    schema_version: "1.0",
    id: checkpoint.id ?? newId(),
    extensions: checkpoint.extensions ?? {},
    ...checkpoint,
  } as ProposedCheckpoint;
}

export function proposedEvent(
  event: Omit<ProposedEvent, "schema_version" | "id" | "occurred_at" | "payload" | "extensions"> &
    Partial<Pick<ProposedEvent, "id" | "occurred_at" | "payload" | "extensions">>,
): ProposedEvent {
  return {
    schema_version: "1.0",
    id: event.id ?? newId(),
    occurred_at: event.occurred_at ?? now(),
    payload: event.payload ?? {},
    extensions: event.extensions ?? {},
    ...event,
  };
}

function now(): string {
  return new Date().toISOString();
}
