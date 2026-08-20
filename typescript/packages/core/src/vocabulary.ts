/** Core values are portable across adapters; persisted type fields remain open to extensions. */
export const ActionType = {
  MODEL_CALL: "model_call",
  TOOL_CALL: "tool_call",
  COMPACT: "compact",
  CHECKPOINT: "checkpoint",
} as const;

export type CoreActionType = (typeof ActionType)[keyof typeof ActionType];

/** Core values are portable lifecycle and framework-state facts. */
export const EventType = {
  SESSION_STARTED: "session.started",
  SESSION_COMPLETED: "session.completed",
  RUN_STARTED: "run.started",
  RUN_COMPLETED: "run.completed",
  RUN_FAILED: "run.failed",
  RUN_CANCELLED: "run.cancelled",
  LANE_CREATED: "lane.created",
  TURN_STARTED: "turn.started",
  TURN_COMPLETED: "turn.completed",
  TURN_FAILED: "turn.failed",
  ACTION_STARTED: "action.started",
  ACTION_COMPLETED: "action.completed",
  ACTION_FAILED: "action.failed",
  ATTEMPT_REQUESTED: "attempt.requested",
  ATTEMPT_COMPLETED: "attempt.completed",
  ATTEMPT_FAILED: "attempt.failed",
  LANE_FRAMEWORK_SNAPSHOT_SAVED: "lane.framework.snapshot.saved",
  LANE_FRAMEWORK_CHECKPOINT_LINKED: "lane.framework.checkpoint.linked",
} as const;

export type CoreEventType = (typeof EventType)[keyof typeof EventType];
