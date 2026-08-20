package agentledger

type UnresolvedAttempt struct {
	RunID            string `json:"run_id"`
	LaneID           string `json:"lane_id"`
	TurnID           string `json:"turn_id"`
	ActionID         string `json:"action_id"`
	ActionType       string `json:"action_type"`
	AttemptID        string `json:"attempt_id"`
	AttemptNo        int    `json:"attempt_no"`
	RequestedEventID string `json:"requested_event_id"`
}

type LinkedCheckpoint struct {
	Event          StoredEvent    `json:"event"`
	CheckpointID   string         `json:"checkpoint_id,omitempty"`
	Profile        string         `json:"profile,omitempty"`
	ProfileVersion string         `json:"profile_version,omitempty"`
	Metadata       map[string]any `json:"metadata"`
}

type RunInspection struct {
	RunID              string              `json:"run_id"`
	TerminalEvents     []StoredEvent       `json:"terminal_events"`
	LinkedCheckpoints  []LinkedCheckpoint  `json:"linked_checkpoints"`
	UnresolvedAttempts []UnresolvedAttempt `json:"unresolved_attempts"`
}

func InspectRun(view RunView) RunInspection {
	result := RunInspection{RunID: view.RunID}
	for _, event := range view.Events {
		switch event.EventType {
		case EventTypeRunCompleted, EventTypeRunFailed, EventTypeRunCancelled:
			if event.SubjectID == view.RunID {
				result.TerminalEvents = append(result.TerminalEvents, event)
			}
		case EventTypeLaneFrameworkCheckpointLinked:
			metadata, _ := event.Payload["metadata"].(map[string]any)
			if metadata == nil {
				metadata = map[string]any{}
			}
			checkpointID, _ := event.Payload["checkpoint_id"].(string)
			profile, _ := event.Payload["profile"].(string)
			profileVersion, _ := event.Payload["profile_version"].(string)
			result.LinkedCheckpoints = append(result.LinkedCheckpoints, LinkedCheckpoint{
				Event: event, CheckpointID: checkpointID, Profile: profile,
				ProfileVersion: profileVersion, Metadata: metadata,
			})
		}
	}
	result.UnresolvedAttempts = unresolvedAttempts(view)
	return result
}

func unresolvedAttempts(view RunView) []UnresolvedAttempt {
	lanes := make(map[string]Lane, len(view.Lanes))
	for _, lane := range view.Lanes {
		lanes[lane.ID] = lane
	}
	turns := make(map[string]Turn, len(view.Turns))
	for _, turn := range view.Turns {
		turns[turn.ID] = turn
	}
	actions := make(map[string]Action, len(view.Actions))
	for _, action := range view.Actions {
		actions[action.ID] = action
	}
	attempts := make(map[string]Attempt, len(view.Attempts))
	for _, attempt := range view.Attempts {
		attempts[attempt.ID] = attempt
	}
	open := make(map[string]UnresolvedAttempt)
	order := make([]string, 0)
	ordered := make(map[string]struct{})
	for _, event := range view.Events {
		if event.SubjectKind() != "attempt" {
			continue
		}
		attempt, ok := attempts[event.SubjectID]
		if !ok {
			continue
		}
		action, ok := actions[attempt.ActionID]
		if !ok {
			continue
		}
		turn, ok := turns[action.TurnID]
		if !ok {
			continue
		}
		lane, ok := lanes[turn.LaneID]
		if !ok {
			continue
		}
		switch event.EventType {
		case EventTypeAttemptRequested:
			if _, exists := ordered[attempt.ID]; !exists {
				order = append(order, attempt.ID)
				ordered[attempt.ID] = struct{}{}
			}
			open[attempt.ID] = UnresolvedAttempt{
				RunID: lane.RunID, LaneID: lane.ID, TurnID: turn.ID,
				ActionID: action.ID, ActionType: action.Type,
				AttemptID: attempt.ID, AttemptNo: attempt.AttemptNo,
				RequestedEventID: event.ID,
			}
		case EventTypeAttemptCompleted, EventTypeAttemptFailed:
			delete(open, attempt.ID)
		}
	}
	result := make([]UnresolvedAttempt, 0, len(open))
	for _, attemptID := range order {
		if attempt, ok := open[attemptID]; ok {
			result = append(result, attempt)
		}
	}
	return result
}
