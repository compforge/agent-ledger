package agentledger

// Core Action types are portable across Harness adapters. Action.Type remains open to extensions.
const (
	ActionTypeModelCall  = "model_call"
	ActionTypeToolCall   = "tool_call"
	ActionTypeCompact    = "compact"
	ActionTypeCheckpoint = "checkpoint"
)

// Core Event types are portable lifecycle and framework-state facts. EventType remains open to extensions.
const (
	EventTypeSessionStarted                = "session.started"
	EventTypeSessionCompleted              = "session.completed"
	EventTypeRunStarted                    = "run.started"
	EventTypeRunCompleted                  = "run.completed"
	EventTypeRunFailed                     = "run.failed"
	EventTypeRunCancelled                  = "run.cancelled"
	EventTypeLaneCreated                   = "lane.created"
	EventTypeTurnStarted                   = "turn.started"
	EventTypeTurnCompleted                 = "turn.completed"
	EventTypeTurnFailed                    = "turn.failed"
	EventTypeActionStarted                 = "action.started"
	EventTypeActionCompleted               = "action.completed"
	EventTypeActionFailed                  = "action.failed"
	EventTypeAttemptRequested              = "attempt.requested"
	EventTypeAttemptCompleted              = "attempt.completed"
	EventTypeAttemptFailed                 = "attempt.failed"
	EventTypeLaneFrameworkSnapshotSaved    = "lane.framework.snapshot.saved"
	EventTypeLaneFrameworkCheckpointLinked = "lane.framework.checkpoint.linked"
)
