// Package observation describes Session and Job execution without Ledger storage dependencies.
package observation

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/compforge/agent-ledger/go/common"
)

const SessionSchemaVersion = "agent-ledger/session@1"
const JobSchemaVersion = "agent-ledger/job@1"

type SourceRef struct {
	Source string `json:"source"`
	ID     string `json:"id"`
	URI    string `json:"uri,omitempty"`
}

type Node struct {
	Session    *common.SessionRef `json:"session,omitempty"`
	ID         string             `json:"id"`
	Name       string             `json:"name,omitempty"`
	RunID      string             `json:"run_id,omitempty"`
	LaneID     string             `json:"lane_id,omitempty"`
	StartedAt  string             `json:"started_at,omitempty"`
	FinishedAt string             `json:"finished_at,omitempty"`
	Status     string             `json:"status,omitempty"`
	Evidence   []SourceRef        `json:"evidence,omitempty"`
	Attributes map[string]any     `json:"attributes,omitempty"`
}

type Input struct {
	Node
	Mode    string `json:"mode,omitempty"`
	Content any    `json:"content"`
}
type ModelCall struct {
	Node
	Request  *common.CallRequest  `json:"request,omitempty"`
	Response *common.CallResponse `json:"response,omitempty"`
}
type ToolCall struct {
	Node
	ToolCallID  string               `json:"tool_call_id,omitempty"`
	ModelCallID string               `json:"model_call_id,omitempty"`
	Request     *common.CallRequest  `json:"request,omitempty"`
	Response    *common.CallResponse `json:"response,omitempty"`
	Sessions    []common.SessionRef  `json:"sessions,omitempty"`
}
type Operation struct {
	Node
	Operation string `json:"operation"`
	Data      any    `json:"data,omitempty"`
	Items     Items  `json:"items,omitempty"`
}
type Turn struct {
	Node
	Items Items `json:"items"`
}
type Lane struct {
	ID       string `json:"id"`
	Name     string `json:"name,omitempty"`
	ParentID string `json:"parent_id,omitempty"`
}

type Item interface {
	Kind() string
	Base() *Node
}

func (n *Input) Kind() string     { return "input" }
func (n *ModelCall) Kind() string { return "model_call" }
func (n *ToolCall) Kind() string  { return "tool_call" }
func (n *Operation) Kind() string { return "operation" }
func (n *Turn) Kind() string      { return "turn" }
func (n *Input) Base() *Node      { return &n.Node }
func (n *ModelCall) Base() *Node  { return &n.Node }
func (n *ToolCall) Base() *Node   { return &n.Node }
func (n *Operation) Base() *Node  { return &n.Node }
func (n *Turn) Base() *Node       { return &n.Node }

func (n *Input) MarshalJSON() ([]byte, error) {
	type plain Input
	return json.Marshal(struct {
		Kind string `json:"kind"`
		*plain
	}{n.Kind(), (*plain)(n)})
}
func (n *ModelCall) MarshalJSON() ([]byte, error) {
	type plain ModelCall
	return json.Marshal(struct {
		Kind string `json:"kind"`
		*plain
	}{n.Kind(), (*plain)(n)})
}
func (n *ToolCall) MarshalJSON() ([]byte, error) {
	type plain ToolCall
	return json.Marshal(struct {
		Kind string `json:"kind"`
		*plain
	}{n.Kind(), (*plain)(n)})
}
func (n *Operation) MarshalJSON() ([]byte, error) {
	type plain Operation
	return json.Marshal(struct {
		Kind string `json:"kind"`
		*plain
	}{n.Kind(), (*plain)(n)})
}
func (n *Turn) MarshalJSON() ([]byte, error) {
	type plain Turn
	return json.Marshal(struct {
		Kind string `json:"kind"`
		*plain
	}{n.Kind(), (*plain)(n)})
}

type Items []Item

func (items *Items) UnmarshalJSON(data []byte) error {
	var records []json.RawMessage
	if err := json.Unmarshal(data, &records); err != nil {
		return err
	}
	result := make(Items, 0, len(records))
	for _, raw := range records {
		var tag struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(raw, &tag); err != nil {
			return err
		}
		var item Item
		switch tag.Kind {
		case "input":
			item = &Input{}
		case "model_call":
			item = &ModelCall{}
		case "tool_call":
			item = &ToolCall{}
		case "operation":
			item = &Operation{}
		case "turn":
			item = &Turn{}
		default:
			return fmt.Errorf("unknown view item kind %q", tag.Kind)
		}
		if err := json.Unmarshal(raw, item); err != nil {
			return err
		}
		result = append(result, item)
	}
	*items = result
	return nil
}

type SessionView struct {
	Attributes    map[string]any    `json:"attributes,omitempty"`
	SchemaVersion string            `json:"schema_version"`
	Session       common.SessionRef `json:"session"`
	Items         Items             `json:"items"`
	Lanes         []Lane            `json:"lanes,omitempty"`
	Issues        []string          `json:"issues,omitempty"`
}

// Validate checks observed facts, not recoverability or business success.
func (d SessionView) Validate() error {
	if d.SchemaVersion != SessionSchemaVersion {
		return fmt.Errorf("unsupported View schema %q", d.SchemaVersion)
	}
	if d.Session.Source == "" || d.Session.ID == "" {
		return fmt.Errorf("missing session identity")
	}
	_, err := checkGraph(d.Items, &d.Session, nil)
	return err
}

// A JobRef is a business-owned task identity, not a durable Ledger entity.
type JobRef struct {
	Source string `json:"source"`
	ID     string `json:"id"`
}
type ItemRef struct {
	ID      string             `json:"id"`
	Session *common.SessionRef `json:"session,omitempty"`
}
type Relation struct {
	Kind     string      `json:"kind"`
	Source   ItemRef     `json:"source"`
	Target   ItemRef     `json:"target"`
	Evidence []SourceRef `json:"evidence,omitempty"`
}

// JobView selects work toward one goal across continuing Sessions.
// +spec=`Job completion is source-owned; shared calls do not imply exclusive cost ownership`
// +link=docs/observation.md
type JobView struct {
	SchemaVersion string          `json:"schema_version"`
	Job           JobRef          `json:"job"`
	Name          string          `json:"name,omitempty"`
	Goal          json.RawMessage `json:"goal,omitempty"`
	Result        json.RawMessage `json:"result,omitempty"`
	Status        string          `json:"status,omitempty"`
	StartedAt     string          `json:"started_at,omitempty"`
	FinishedAt    string          `json:"finished_at,omitempty"`
	Items         Items           `json:"items"`
	Relations     []Relation      `json:"relations,omitempty"`
	Attributes    map[string]any  `json:"attributes,omitempty"`
	Issues        []string        `json:"issues,omitempty"`
}

func (d JobView) Validate() error {
	_, err := d.ReferenceIssues()
	return err
}

// ReferenceIssues returns missing external fragments without mutating source evidence.
func (d JobView) ReferenceIssues() ([]string, error) {
	if d.SchemaVersion != JobSchemaVersion {
		return nil, fmt.Errorf("unsupported View schema %q", d.SchemaVersion)
	}
	if d.Job.Source == "" || d.Job.ID == "" {
		return nil, fmt.Errorf("missing job identity")
	}
	if err := checkNode(&Node{ID: d.Job.ID, Status: d.Status, StartedAt: d.StartedAt, FinishedAt: d.FinishedAt}); err != nil {
		return nil, err
	}
	return checkGraph(d.Items, nil, d.Relations)
}

type itemKey struct{ Source, Session, ID string }

func key(id string, session *common.SessionRef) itemKey {
	if session == nil {
		return itemKey{ID: id}
	}
	return itemKey{Source: session.Source, Session: session.ID, ID: id}
}
func owner(node *Node, session *common.SessionRef) *common.SessionRef {
	if node.Session != nil {
		return node.Session
	}
	return session
}
func checkEvidence(refs []SourceRef) error {
	for _, ref := range refs {
		if ref.Source == "" || ref.ID == "" {
			return fmt.Errorf("invalid evidence reference")
		}
	}
	return nil
}
func checkNode(n *Node) error {
	if n.ID == "" {
		return fmt.Errorf("missing view node id")
	}
	if n.Session != nil && (n.Session.Source == "" || n.Session.ID == "") {
		return fmt.Errorf("node %s: invalid Session identity", n.ID)
	}
	switch n.Status {
	case "", "unknown", "running", "completed", "failed", "cancelled", "outcome_unknown":
	default:
		return fmt.Errorf("node %s: invalid status %s", n.ID, n.Status)
	}
	var start, end time.Time
	for i, value := range []string{n.StartedAt, n.FinishedAt} {
		if value == "" {
			continue
		}
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return fmt.Errorf("node %s: invalid timestamp: %w", n.ID, err)
		}
		if i == 0 {
			start = parsed
		} else {
			end = parsed
		}
	}
	if !start.IsZero() && !end.IsZero() && end.Before(start) {
		return fmt.Errorf("node %s: finish precedes start", n.ID)
	}
	if err := checkEvidence(n.Evidence); err != nil {
		return fmt.Errorf("node %s: %w", n.ID, err)
	}
	return nil
}

// +spec=`Identity is Session-scoped; partial Job snapshots retain unresolved Session references`
func checkGraph(items Items, session *common.SessionRef, relations []Relation) ([]string, error) {
	nodes := map[itemKey]Item{}
	var tools []*ToolCall
	var issues []string
	var visit func(Item, string) error
	visit = func(item Item, parent string) error {
		if item == nil {
			return fmt.Errorf("nil view item")
		}
		n := item.Base()
		if err := checkNode(n); err != nil {
			return err
		}
		if session != nil && n.Session != nil && *session != *n.Session {
			return fmt.Errorf("node %s: foreign Session inside SessionView", n.ID)
		}
		identity := key(n.ID, owner(n, session))
		if _, exists := nodes[identity]; exists {
			return fmt.Errorf("duplicate view node %v", identity)
		}
		nodes[identity] = item
		if item.Kind() == "turn" && parent != "session" && parent != "job" && parent != "operation" {
			return fmt.Errorf("turn %s: invalid parent %s", n.ID, parent)
		}
		var children Items
		var request *common.CallRequest
		var response *common.CallResponse
		switch v := item.(type) {
		case *Turn:
			children = v.Items
		case *Operation:
			if v.Operation == "" {
				return fmt.Errorf("operation %s: empty operation type", n.ID)
			}
			children = v.Items
		case *ModelCall:
			request, response = v.Request, v.Response
		case *ToolCall:
			tools = append(tools, v)
			request, response = v.Request, v.Response
			for _, ref := range v.Sessions {
				if ref.Source == "" || ref.ID == "" {
					return fmt.Errorf("tool %s: invalid session reference", n.ID)
				}
			}
		}
		if request != nil {
			if err := request.Validate(); err != nil {
				return fmt.Errorf("node %s request: %w", n.ID, err)
			}
		}
		if response != nil {
			if err := response.Validate(); err != nil {
				return fmt.Errorf("node %s response: %w", n.ID, err)
			}
		}
		for _, child := range children {
			if err := visit(child, item.Kind()); err != nil {
				return err
			}
		}
		return nil
	}
	root := "job"
	if session != nil {
		root = "session"
	}
	for _, item := range items {
		if err := visit(item, root); err != nil {
			return nil, err
		}
	}
	for _, tool := range tools {
		if tool.ModelCallID == "" {
			continue
		}
		scope := owner(&tool.Node, session)
		target, exists := nodes[key(tool.ModelCallID, scope)]
		if exists && target.Kind() != "model_call" {
			return nil, fmt.Errorf("tool %s: referenced item is not a model call", tool.ID)
		}
		if !exists {
			if session != nil || scope == nil {
				return nil, fmt.Errorf("tool %s: missing model call %s", tool.ID, tool.ModelCallID)
			}
			issues = append(issues, fmt.Sprintf("tool %v: model call %s outside Job snapshot", key(tool.ID, scope), tool.ModelCallID))
		}
	}
	for _, relation := range relations {
		if relation.Kind != "depends_on" && relation.Kind != "handoff" {
			return nil, fmt.Errorf("invalid relation %s", relation.Kind)
		}
		if err := checkEvidence(relation.Evidence); err != nil {
			return nil, err
		}
		for _, ref := range []ItemRef{relation.Source, relation.Target} {
			if ref.ID == "" {
				return nil, fmt.Errorf("missing relation item id")
			}
			if ref.Session != nil && (ref.Session.Source == "" || ref.Session.ID == "") {
				return nil, fmt.Errorf("invalid relation Session identity")
			}
			if _, exists := nodes[key(ref.ID, ref.Session)]; !exists {
				if ref.Session == nil {
					return nil, fmt.Errorf("%s: missing job item %s", relation.Kind, ref.ID)
				}
				issues = append(issues, fmt.Sprintf("%s: Session item %v outside Job snapshot", relation.Kind, key(ref.ID, ref.Session)))
			}
		}
	}
	unique := make([]string, 0, len(issues))
	seen := map[string]bool{}
	for _, issue := range issues {
		if !seen[issue] {
			unique = append(unique, issue)
			seen[issue] = true
		}
	}
	return unique, nil
}
