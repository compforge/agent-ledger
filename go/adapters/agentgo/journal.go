package agentgoadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/compforge/agent-ledger/go"
	"github.com/compforge/agentgo"
)

type runJournal struct {
	mu      sync.Mutex
	scope   string
	turns   map[int][]*journalTurn
	actions map[string]*journalAction
}

type journalTurn struct {
	Turn      agentledger.Turn
	Completed bool
	Failed    bool
}

type journalAction struct {
	Action   agentledger.Action
	Attempts []journalAttempt
}

type journalAttempt struct {
	Attempt   agentledger.Attempt
	Requested *agentledger.StoredEvent
	Terminal  *agentledger.StoredEvent
}

type executionRecord struct {
	Action   agentledger.Action
	Attempts []journalAttempt
}

func loadRunJournal(
	ctx context.Context,
	recorder *agentledger.LaneRecorder,
	scope string,
) (*runJournal, error) {
	view, err := recorder.Store().LoadRun(ctx, recorder.SessionID(), recorder.RunID())
	if err != nil {
		return nil, err
	}
	journal := &runJournal{
		scope: scope, turns: make(map[int][]*journalTurn), actions: make(map[string]*journalAction),
	}
	laneID := recorder.Lane().ID
	turnsByID := make(map[string]agentledger.Turn)
	for _, turn := range view.Turns {
		if turn.LaneID == laneID {
			turnsByID[turn.ID] = turn
		}
	}
	turnRecords := make(map[string]*journalTurn)
	for _, event := range view.Events {
		if event.LaneID != laneID || event.EventType != agentledger.EventTypeTurnStarted {
			continue
		}
		turn, ok := turnsByID[event.SubjectID]
		if !ok || stringValue(event.Payload["execution_scope"]) != scope {
			continue
		}
		turnIndex, ok := intValue(event.Payload["turn_index"])
		if !ok || turnIndex <= 0 {
			continue
		}
		record := &journalTurn{Turn: turn}
		journal.turns[turnIndex] = append(journal.turns[turnIndex], record)
		turnRecords[turn.ID] = record
	}
	for _, event := range view.Events {
		record := turnRecords[event.SubjectID]
		if record == nil {
			continue
		}
		switch event.EventType {
		case agentledger.EventTypeTurnCompleted:
			record.Completed = true
		case agentledger.EventTypeTurnFailed:
			record.Failed = true
		}
	}
	prefix := scope + "/"
	for _, action := range view.Actions {
		if turnRecords[action.TurnID] == nil || !strings.HasPrefix(action.Key, prefix) {
			continue
		}
		key := executionMapKey(action.Type, action.Key)
		copy := action
		journal.actions[key] = &journalAction{Action: copy}
	}
	attemptsByID := make(map[string]*journalAttempt)
	for _, attempt := range view.Attempts {
		action := journal.actionsByID(attempt.ActionID)
		if action == nil {
			continue
		}
		action.Attempts = append(action.Attempts, journalAttempt{Attempt: attempt})
		attemptsByID[attempt.ID] = &action.Attempts[len(action.Attempts)-1]
	}
	for _, action := range journal.actions {
		sort.Slice(action.Attempts, func(i, k int) bool {
			return action.Attempts[i].Attempt.AttemptNo < action.Attempts[k].Attempt.AttemptNo
		})
		for index := range action.Attempts {
			attemptsByID[action.Attempts[index].Attempt.ID] = &action.Attempts[index]
		}
	}
	for _, event := range view.Events {
		if event.LaneID != laneID {
			continue
		}
		attempt := attemptsByID[event.SubjectID]
		if attempt == nil {
			continue
		}
		copy := event
		switch event.EventType {
		case agentledger.EventTypeAttemptRequested:
			attempt.Requested = &copy
		case agentledger.EventTypeAttemptCompleted, agentledger.EventTypeAttemptFailed,
			agentledger.EventTypeAttemptCancelled, agentledger.EventTypeAttemptOutcomeUnknown:
			attempt.Terminal = &copy
		}
	}
	return journal, nil
}

func (j *runJournal) actionsByID(actionID string) *journalAction {
	for _, action := range j.actions {
		if action.Action.ID == actionID {
			return action
		}
	}
	return nil
}

func (a *Adapter) prepareTurn(ctx context.Context, turnIndex int) (turnBinding, error) {
	a.mu.Lock()
	journal := a.journal
	a.mu.Unlock()
	if journal == nil {
		return turnBinding{}, fmt.Errorf("agentgo recovery journal is not loaded")
	}
	if binding, ok := journal.reusableTurn(turnIndex); ok {
		return binding, nil
	}
	turn, err := a.runtimeRecorder.StartTurn(ctx, map[string]any{
		"turn_index": turnIndex, "execution_scope": journal.scope,
	})
	if err != nil {
		return turnBinding{}, err
	}
	binding := turnBinding{ID: turn.ID, Index: turnIndex}
	journal.addTurn(turnIndex, turn)
	return binding, nil
}

func (j *runJournal) reusableTurn(turnIndex int) (turnBinding, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	records := j.turns[turnIndex]
	for index := len(records) - 1; index >= 0; index-- {
		record := records[index]
		if record.Completed {
			return turnBinding{ID: record.Turn.ID, Index: turnIndex, Completed: true}, true
		}
		if !record.Failed {
			return turnBinding{ID: record.Turn.ID, Index: turnIndex}, true
		}
	}
	return turnBinding{}, false
}

func (j *runJournal) addTurn(turnIndex int, turn agentledger.Turn) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.turns[turnIndex] = append(j.turns[turnIndex], &journalTurn{Turn: turn})
}

func (a *Adapter) markTurnCompleted(turnIndex int) {
	a.mu.Lock()
	journal := a.journal
	a.mu.Unlock()
	if journal != nil {
		journal.markTurn(turnIndex, true)
	}
}

func (a *Adapter) markTurnFailed(turnIndex int) {
	a.mu.Lock()
	journal := a.journal
	a.mu.Unlock()
	if journal != nil {
		journal.markTurn(turnIndex, false)
	}
}

func (a *Adapter) pendingTools() []PendingToolRecovery {
	a.mu.Lock()
	journal := a.journal
	a.mu.Unlock()
	if journal == nil {
		return nil
	}
	return journal.pendingTools()
}

func (j *runJournal) markTurn(turnIndex int, completed bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	records := j.turns[turnIndex]
	if len(records) == 0 {
		return
	}
	records[len(records)-1].Completed = completed
	records[len(records)-1].Failed = !completed
}

func (j *runJournal) execution(actionType, actionKey string) (executionRecord, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	action := j.actions[executionMapKey(actionType, actionKey)]
	if action == nil {
		return executionRecord{}, false
	}
	return executionRecord{Action: action.Action, Attempts: cloneAttempts(action.Attempts)}, true
}

func (j *runJournal) addAttempt(
	action agentledger.Action,
	attempt agentledger.Attempt,
	requestedID string,
	payload map[string]any,
) {
	j.mu.Lock()
	defer j.mu.Unlock()
	key := executionMapKey(action.Type, action.Key)
	record := j.actions[key]
	if record == nil {
		record = &journalAction{Action: action}
		j.actions[key] = record
	}
	request := agentledger.StoredEvent{ProposedEvent: agentledger.ProposedEvent{
		ID: requestedID, EventType: agentledger.EventTypeAttemptRequested,
		SubjectID: attempt.ID, Payload: clonePayload(payload),
	}}
	record.Attempts = append(record.Attempts, journalAttempt{Attempt: attempt, Requested: &request})
}

func (j *runJournal) markAttemptTerminal(attemptID string, terminal agentledger.StoredEvent) {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, action := range j.actions {
		for index := range action.Attempts {
			if action.Attempts[index].Attempt.ID == attemptID {
				copy := terminal
				action.Attempts[index].Terminal = &copy
				return
			}
		}
	}
}

func (j *runJournal) pendingTools() []PendingToolRecovery {
	j.mu.Lock()
	defer j.mu.Unlock()
	var pending []PendingToolRecovery
	for _, record := range j.actions {
		if record.Action.Type != agentledger.ActionTypeToolCall || hasReplayableToolOutcome(record.Attempts) {
			continue
		}
		latest, ok := latestAttempt(record.Attempts)
		if !ok || latest.Requested == nil {
			continue
		}
		call, err := toolCallFromRequest(record.Action, *latest.Requested)
		if err != nil {
			continue
		}
		pending = append(pending, PendingToolRecovery{
			Action: record.Action, Attempt: latest.Attempt, Call: call,
		})
	}
	sort.Slice(pending, func(i, k int) bool {
		return pending[i].Attempt.CreatedAt < pending[k].Attempt.CreatedAt
	})
	return pending
}

func latestAttempt(attempts []journalAttempt) (journalAttempt, bool) {
	if len(attempts) == 0 {
		return journalAttempt{}, false
	}
	return attempts[len(attempts)-1], true
}

func hasReplayableToolOutcome(attempts []journalAttempt) bool {
	for index := len(attempts) - 1; index >= 0; index-- {
		terminal := attempts[index].Terminal
		if terminal == nil {
			continue
		}
		if terminal.EventType == agentledger.EventTypeAttemptCompleted || terminal.EventType == agentledger.EventTypeAttemptFailed {
			return true
		}
	}
	return false
}

func toolCallFromRequest(action agentledger.Action, request agentledger.StoredEvent) (agentgo.ToolCall, error) {
	call := agentgo.ToolCall{
		ID:   stringValue(request.Payload["tool_call_id"]),
		Name: stringValue(request.Payload["tool_name"]),
	}
	if call.ID == "" {
		_, call.ID, _ = strings.Cut(action.Key, "/")
	}
	if input, ok := request.Payload["input"]; ok {
		encoded, err := json.Marshal(input)
		if err != nil {
			return agentgo.ToolCall{}, fmt.Errorf("encode tool input: %w", err)
		}
		call.Args = encoded
	}
	return call, nil
}

func executionMapKey(actionType, actionKey string) string {
	return actionType + "\x00" + actionKey
}

func cloneAttempts(source []journalAttempt) []journalAttempt {
	result := make([]journalAttempt, len(source))
	copy(result, source)
	return result
}

func clonePayload(source map[string]any) map[string]any {
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func stringValue(value any) string {
	result, _ := value.(string)
	return result
}

func intValue(value any) (int, bool) {
	switch number := value.(type) {
	case int:
		return number, true
	case int64:
		return int(number), true
	case float64:
		return int(number), number == float64(int(number))
	case json.Number:
		parsed, err := number.Int64()
		return int(parsed), err == nil
	default:
		return 0, false
	}
}
