import type {
  Action, Attempt, JsonValue, Lane, RunView, StoredEvent, Turn,
} from "./types.js";
import { EventType } from "./vocabulary.js";

export interface UnresolvedAttempt {
  run_id: string;
  lane_id: string;
  turn_id: string;
  action_id: string;
  action_type: string;
  attempt_id: string;
  attempt_no: number;
  requested_event_id: string;
}

export interface LinkedCheckpoint {
  event: StoredEvent;
  checkpoint_id?: string;
  profile?: string;
  profile_version?: string;
  metadata: { [key: string]: JsonValue };
}

export interface RunInspection {
  run_id: string;
  terminal_events: StoredEvent[];
  linked_checkpoints: LinkedCheckpoint[];
  unresolved_attempts: UnresolvedAttempt[];
}

export function inspectRun(view: RunView): RunInspection {
  const terminalTypes = new Set<string>([
    EventType.RUN_COMPLETED,
    EventType.RUN_FAILED,
    EventType.RUN_CANCELLED,
  ]);
  const terminalEvents = view.events.filter(
    (event) => terminalTypes.has(event.event_type) && event.subject_id === view.run_id,
  );
  const linkedCheckpoints = view.events
    .filter((event) => event.event_type === EventType.LANE_FRAMEWORK_CHECKPOINT_LINKED)
    .map((event): LinkedCheckpoint => {
      const checkpointId = event.payload.checkpoint_id;
      const profile = event.payload.profile;
      const profileVersion = event.payload.profile_version;
      const metadata = event.payload.metadata;
      return {
        event,
        ...(typeof checkpointId === "string" ? { checkpoint_id: checkpointId } : {}),
        ...(typeof profile === "string" ? { profile } : {}),
        ...(typeof profileVersion === "string" ? { profile_version: profileVersion } : {}),
        metadata: isJsonObject(metadata) ? metadata : {},
      };
    });
  return structuredClone({
    run_id: view.run_id,
    terminal_events: terminalEvents,
    linked_checkpoints: linkedCheckpoints,
    unresolved_attempts: unresolvedAttempts(view),
  });
}

function unresolvedAttempts(view: RunView): UnresolvedAttempt[] {
  const lanes = new Map<string, Lane>(view.lanes.map((lane) => [lane.id, lane]));
  const turns = new Map<string, Turn>(view.turns.map((turn) => [turn.id, turn]));
  const actions = new Map<string, Action>(view.actions.map((action) => [action.id, action]));
  const attempts = new Map<string, Attempt>(
    view.attempts.map((attempt) => [attempt.id, attempt]),
  );
  const open = new Map<string, UnresolvedAttempt>();
  for (const event of view.events) {
    if (!event.event_type.startsWith("attempt.")) continue;
    const attempt = attempts.get(event.subject_id);
    const action = attempt === undefined ? undefined : actions.get(attempt.action_id);
    const turn = action === undefined ? undefined : turns.get(action.turn_id);
    const lane = turn === undefined ? undefined : lanes.get(turn.lane_id);
    if (attempt === undefined || action === undefined || turn === undefined || lane === undefined) {
      continue;
    }
    if (event.event_type === EventType.ATTEMPT_REQUESTED) {
      open.set(attempt.id, {
        run_id: lane.run_id,
        lane_id: lane.id,
        turn_id: turn.id,
        action_id: action.id,
        action_type: action.type,
        attempt_id: attempt.id,
        attempt_no: attempt.attempt_no,
        requested_event_id: event.id,
      });
    } else if (
      event.event_type === EventType.ATTEMPT_COMPLETED
      || event.event_type === EventType.ATTEMPT_FAILED
    ) {
      open.delete(attempt.id);
    }
  }
  return [...open.values()];
}

function isJsonObject(value: JsonValue | undefined): value is { [key: string]: JsonValue } {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
