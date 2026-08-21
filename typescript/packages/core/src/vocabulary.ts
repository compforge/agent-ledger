/** Core values are portable across adapters; persisted type fields remain open to extensions. */
export const ActionType = {
  MODEL_CALL: "model_call",
  TOOL_CALL: "tool_call",
  COMPACT: "compact",
  CHECKPOINT: "checkpoint",
} as const;

export type CoreActionType = (typeof ActionType)[keyof typeof ActionType];

export const EffectKind = {
  NONE: "none",
  READ: "read",
  WRITE: "write",
  UNKNOWN: "unknown",
} as const;

export type CoreEffectKind = (typeof EffectKind)[keyof typeof EffectKind];

export const Idempotency = {
  NOT_APPLICABLE: "not_applicable",
  INHERENT: "inherent",
  KEYED: "keyed",
  NONE: "none",
  UNKNOWN: "unknown",
} as const;

export type CoreIdempotency = (typeof Idempotency)[keyof typeof Idempotency];

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
