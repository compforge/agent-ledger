// Package view describes observed agent execution without Ledger storage dependencies.
package view

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/compforge/agent-ledger/go/common"
)

const SchemaVersion = "agent-ledger/view@1"

type SourceRef struct {
	Source string `json:"source"`
	ID     string `json:"id"`
	URI    string `json:"uri,omitempty"`
}

type Node struct {
	ID         string         `json:"id"`
	Name       string         `json:"name,omitempty"`
	LaneID     string         `json:"lane_id,omitempty"`
	StartedAt  string         `json:"started_at,omitempty"`
	FinishedAt string         `json:"finished_at,omitempty"`
	Status     string         `json:"status,omitempty"`
	Evidence   []SourceRef    `json:"evidence,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
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
	Runs        []*Run               `json:"runs,omitempty"`
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
type Run struct {
	Node
	SessionID string `json:"session_id,omitempty"`
	Lanes     []Lane `json:"lanes,omitempty"`
	Items     Items  `json:"items"`
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
func (n *Run) Kind() string       { return "run" }
func (n *Input) Base() *Node      { return &n.Node }
func (n *ModelCall) Base() *Node  { return &n.Node }
func (n *ToolCall) Base() *Node   { return &n.Node }
func (n *Operation) Base() *Node  { return &n.Node }
func (n *Turn) Base() *Node       { return &n.Node }
func (n *Run) Base() *Node        { return &n.Node }

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
func (n *Run) MarshalJSON() ([]byte, error) {
	type plain Run
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
		case "run":
			item = &Run{}
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

type Document struct {
	SchemaVersion string   `json:"schema_version"`
	Runs          []*Run   `json:"runs"`
	Issues        []string `json:"issues,omitempty"`
}

// Validate checks read-model relationships, not recoverability or business success.
// +spec=`Partial timing and asynchronous children are permitted; no parent timing window is inferred`
// +link=docs/execution-view.md
func (d Document) Validate() error {
	if d.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported View schema %q", d.SchemaVersion)
	}
	ids := map[string]bool{}
	modelIDs := map[string]bool{}
	var tools []*ToolCall
	var visit func(Item, string) error
	visit = func(item Item, parent string) error {
		if item == nil {
			return fmt.Errorf("nil view item")
		}
		n := item.Base()
		if n.ID == "" || ids[n.ID] {
			return fmt.Errorf("invalid or duplicate view node %q", n.ID)
		}
		ids[n.ID] = true
		if item.Kind() == "turn" && parent != "run" && parent != "operation" {
			return fmt.Errorf("turn %s: invalid parent %s", n.ID, parent)
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
		for _, ref := range n.Evidence {
			if ref.Source == "" || ref.ID == "" {
				return fmt.Errorf("node %s: invalid evidence reference", n.ID)
			}
		}
		var children Items
		var request *common.CallRequest
		var response *common.CallResponse
		switch v := item.(type) {
		case *Run:
			children = v.Items
		case *Turn:
			children = v.Items
		case *Operation:
			if v.Operation == "" {
				return fmt.Errorf("operation %s: empty operation type", n.ID)
			}
			children = v.Items
		case *ModelCall:
			modelIDs[n.ID] = true
			request = v.Request
			response = v.Response
		case *ToolCall:
			tools = append(tools, v)
			request = v.Request
			response = v.Response
			for _, run := range v.Runs {
				children = append(children, run)
			}
		}
		if request != nil && len(request.Input) > 0 && request.InputArtifactID != "" {
			return fmt.Errorf("node %s: ambiguous request content", n.ID)
		}
		if response != nil && len(response.Output) > 0 && response.OutputArtifactID != "" {
			return fmt.Errorf("node %s: ambiguous response content", n.ID)
		}
		if request != nil && request.Model != nil && request.Model.ID == "" {
			return fmt.Errorf("node %s: empty model id", n.ID)
		}
		if response != nil {
			if response.Model != nil && response.Model.ID == "" {
				return fmt.Errorf("node %s: empty model id", n.ID)
			}
			if response.Usage != nil {
				u := response.Usage
				for _, v := range []*int64{u.InputTokens, u.OutputTokens, u.CacheReadInputTokens, u.CacheWriteInputTokens, u.TotalTokens} {
					if v != nil && *v < 0 {
						return fmt.Errorf("node %s: negative usage", n.ID)
					}
				}
			}
		}
		for _, child := range children {
			if err := visit(child, item.Kind()); err != nil {
				return err
			}
		}
		return nil
	}
	for _, run := range d.Runs {
		if run == nil {
			return fmt.Errorf("nil run")
		}
		if err := visit(run, "document"); err != nil {
			return err
		}
	}
	for _, tool := range tools {
		if tool.ModelCallID != "" && !modelIDs[tool.ModelCallID] {
			return fmt.Errorf("tool %s: missing model call %s", tool.ID, tool.ModelCallID)
		}
	}
	return nil
}
