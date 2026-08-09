export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export type ActorType = "agent" | "orchestrator" | "tool" | "human" | "system";

export interface Actor {
  type: ActorType;
  id: string;
  framework?: string;
}

export interface EventStream {
  session_id: string;
  stream_id: string;
}

export interface ProposedEvent {
  schema_version: "1.0";
  event_id: string;
  event_type: string;
  session_id: string;
  run_id: string;
  actor: Actor;
  occurred_at: string;
  step_id?: string;
  attempt_id?: string;
  parent_run_id?: string;
  caused_by_event_id?: string;
  payload: { [key: string]: JsonValue };
  extensions: { [key: string]: JsonValue };
}

export interface StoredEvent extends ProposedEvent {
  stream_id: string;
  stream_version: number;
  commit_cursor: string;
  committed_at: string;
}

export interface CommitReceipt {
  stream: EventStream;
  append_id: string;
  digest: string;
  first_version: number;
  last_version: number;
  first_cursor: string;
  last_cursor: string;
  event_ids: string[];
  committed_at: string;
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
  kind: "model" | "tool";
  step_id: string;
  attempt_id: string;
  requested_event_id: string;
}

export function proposedEvent(
  event: Omit<ProposedEvent, "schema_version" | "event_id" | "occurred_at" | "payload" | "extensions"> &
    Partial<Pick<ProposedEvent, "event_id" | "occurred_at" | "payload" | "extensions">>,
): ProposedEvent {
  return {
    schema_version: "1.0",
    event_id: event.event_id ?? crypto.randomUUID(),
    occurred_at: event.occurred_at ?? new Date().toISOString(),
    payload: event.payload ?? {},
    extensions: event.extensions ?? {},
    ...event,
  };
}
