package observation

import (
	"encoding/json"

	"fmt"
	"sort"
	"strings"

	agentledger "github.com/compforge/agent-ledger/go"
	"github.com/compforge/agent-ledger/go/common"
	view "github.com/compforge/agent-ledger/go/observation"
)

var viewTerminals = map[string]string{"completed": "completed", "failed": "failed", "cancelled": "cancelled", "outcome_unknown": "outcome_unknown"}

// SessionView observes one Session query without selecting retry, cancellation or recovery policy.
// +spec=`Each physical call survives projection; missing results and recorded unknown outcomes differ`
// +link=docs/observation.md
func SessionView(source agentledger.SessionView) (view.SessionView, error) {
	document := view.SessionView{SchemaVersion: view.SessionSchemaVersion, Session: common.SessionRef{Source: "ledger", ID: source.SessionID}, Nodes: view.Nodes{}}
	actions := map[string]agentledger.Action{}
	attempts := map[string]agentledger.Attempt{}
	rows := map[string]agentledger.Turn{}
	turns := map[string]*view.Turn{}
	calls := map[string]interface {
		Kind() string
		Base() *view.Node
	}{}
	terminals := map[string][]agentledger.StoredEvent{}
	callOrder := []string{}
	for _, x := range source.Actions {
		actions[x.ID] = x
	}
	for _, x := range source.Attempts {
		attempts[x.ID] = x
	}
	for _, x := range source.Turns {
		rows[x.ID] = x
	}
	observe := func(node *view.Node, event agentledger.StoredEvent) {
		node.Evidence = append(node.Evidence, view.SourceRef{Source: "ledger", ID: event.ID})
		if node.Attributes == nil {
			node.Attributes = map[string]any{}
		}
		suffix := event.EventType[strings.LastIndex(event.EventType, ".")+1:]
		if (suffix == "started" || suffix == "requested") && node.StartedAt == "" {
			node.StartedAt = event.OccurredAt
		}
		if status, ok := viewTerminals[suffix]; ok {
			terminals[node.ID] = append(terminals[node.ID], event)
			node.FinishedAt = event.OccurredAt
			node.Status = status
			for _, previous := range terminals[node.ID] {
				if previous.EventType[strings.LastIndex(previous.EventType, ".")+1:] != suffix {
					node.Status = "unknown"
				}
			}
			if len(terminals[node.ID]) > 1 {
				node.Attributes["terminal_observations"] = terminals[node.ID]
				document.Issues = append(document.Issues, "node "+node.ID+": multiple terminal observations")
			}
		}
		if len(event.Extensions) > 0 {
			extensions, _ := node.Attributes["event_extensions"].(map[string]any)
			if extensions == nil {
				extensions = map[string]any{}
				node.Attributes["event_extensions"] = extensions
			}
			extensions[event.ID] = event.Extensions
		}
	}
	turnFor := func(id string) *view.Turn {
		if turn, ok := turns[id]; ok {
			return turn
		}
		row := rows[id]
		turn := &view.Turn{Node: view.Node{ID: "turn:" + id, LaneID: row.LaneID}, Nodes: view.Nodes{}}
		for _, lane := range source.Lanes {
			if lane.ID == row.LaneID {
				turn.RunID = lane.RunID
				break
			}
		}
		turns[id] = turn
		document.Nodes = append(document.Nodes, turn)
		return turn
	}
	laneIDs := map[string]bool{}
	for _, lane := range source.Lanes {
		if lane.SessionID != source.SessionID {
			return document, fmt.Errorf("lane %s: different session", lane.ID)
		}
		laneIDs[lane.ID] = true
	}
	for _, event := range source.Events {
		if !laneIDs[event.LaneID] {
			return document, fmt.Errorf("event %s: missing Lane %s", event.ID, event.LaneID)
		}
	}
	for _, lane := range source.Lanes {
		document.Lanes = append(document.Lanes, view.Lane{ID: lane.ID, Name: lane.Name, ParentID: lane.ParentLaneID})
		events := []agentledger.StoredEvent{}
		for _, event := range source.Events {
			if event.LaneID == lane.ID {
				events = append(events, event)
			}
		}
		sort.SliceStable(events, func(i, j int) bool { return events[i].Seq < events[j].Seq })
		for _, event := range events {
			_, hasTurn := rows[event.SubjectID]
			if strings.HasPrefix(event.EventType, "turn.") && hasTurn {
				observe(&turnFor(event.SubjectID).Node, event)
			} else if strings.HasPrefix(event.EventType, "attempt.") {
				attempt, ok := attempts[event.SubjectID]
				action, hasAction := actions[attempt.ActionID]
				row, hasRow := rows[action.TurnID]
				if !ok || !hasAction || !hasRow {
					return document, fmt.Errorf("event %s: missing Attempt/Action/Turn ownership", event.ID)
				}
				if row.LaneID != event.LaneID {
					return document, fmt.Errorf("event %s: Attempt belongs to another Lane", event.ID)
				}
				call := calls[attempt.ID]
				if call == nil {
					base := view.Node{ID: "call:" + attempt.ID, LaneID: lane.ID, RunID: lane.RunID, Attributes: map[string]any{"action_id": action.ID, "attempt_id": attempt.ID, "attempt_no": attempt.AttemptNo}}
					switch action.Type {
					case "model_call":
						call = &view.ModelCall{Node: base}
					case "tool_call":
						call = &view.ToolCall{Node: base, ToolCallID: action.Key}
					default:
						call = &view.Operation{Node: base, Operation: action.Type, Nodes: view.Nodes{}}
					}
					calls[attempt.ID] = call
					callOrder = append(callOrder, attempt.ID)
					turn := turnFor(action.TurnID)
					turn.Nodes = append(turn.Nodes, call)
				}
				observe(call.Base(), event)
				var request **common.CallRequest
				var response **common.CallResponse
				switch value := call.(type) {
				case *view.ModelCall:
					request = &value.Request
					response = &value.Response
				case *view.ToolCall:
					request = &value.Request
					response = &value.Response
				case *view.Operation:
					value.Nodes = append(value.Nodes, &view.Operation{Node: view.Node{ID: "event:" + event.ID, Evidence: []view.SourceRef{{Source: "ledger", ID: event.ID}}}, Operation: event.EventType, Data: event.Payload})
				}
				if request != nil {
					suffix := event.EventType[strings.LastIndex(event.EventType, ".")+1:]
					if event.EventType == "attempt.requested" {
						value, err := common.DecodeRequest(event.Payload)
						if err != nil {
							return document, fmt.Errorf("event %s request: %w", event.ID, err)
						}
						*request = &value
					} else if _, ok := viewTerminals[suffix]; ok {
						if len(terminals[call.Base().ID]) == 1 {
							value, err := common.DecodeResponse(event.Payload)
							if err != nil {
								return document, fmt.Errorf("event %s response: %w", event.ID, err)
							}
							*response = &value
						} else {
							*response = nil
						}
					} else {
						records, _ := call.Base().Attributes["events"].([]agentledger.StoredEvent)
						call.Base().Attributes["events"] = append(records, event)
					}
					extra := common.CallDetails(event.Payload)
					if len(extra) > 0 {
						details, _ := call.Base().Attributes["event_details"].(map[string]any)
						if details == nil {
							details = map[string]any{}
							call.Base().Attributes["event_details"] = details
						}
						details[event.ID] = extra
					}
				}
				continue
			}
			op := &view.Operation{Node: view.Node{ID: "event:" + event.ID, LaneID: lane.ID, RunID: lane.RunID, StartedAt: event.OccurredAt, Evidence: []view.SourceRef{{Source: "ledger", ID: event.ID}}, Attributes: map[string]any{"extensions": event.Extensions}}, Operation: event.EventType, Data: event.Payload}
			if strings.HasPrefix(event.EventType, "turn.") && hasTurn {
				turn := turnFor(event.SubjectID)
				turn.Nodes = append(turn.Nodes, op)
			} else {
				document.Nodes = append(document.Nodes, op)
			}
		}
	}
	for _, row := range source.Turns {
		turnFor(row.ID)
	}
	for _, attempt := range source.Attempts {
		if calls[attempt.ID] == nil {
			document.Issues = append(document.Issues, "attempt "+attempt.ID+": no recorded call events")
		}
	}
	for _, id := range callOrder {
		node := calls[id].Base()
		if node.StartedAt == "" {
			document.Issues = append(document.Issues, "node "+node.ID+": request missing")
		}
		if len(terminals[node.ID]) == 0 {
			document.Issues = append(document.Issues, "node "+node.ID+": result missing")
		}
	}
	if err := document.Validate(); err != nil {
		return document, err
	}
	// Detach mutable source payloads; projecting must never let a reader rewrite Ledger evidence.
	encoded, err := json.Marshal(document)
	if err != nil {
		return document, err
	}
	var detached view.SessionView
	err = json.Unmarshal(encoded, &detached)
	return detached, err
}
