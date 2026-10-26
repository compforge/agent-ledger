// Package observation describes Session and Job execution without Ledger storage dependencies.
package observation

import "github.com/compforge/agent-ledger/go/common"

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
	Nodes     Nodes  `json:"nodes,omitempty"`
}
type Turn struct {
	Node
	Nodes Nodes `json:"nodes"`
}
type node interface {
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

type NodeRef struct {
	ID      string             `json:"id"`
	Session *common.SessionRef `json:"session,omitempty"`
}

func children(n node) Nodes {
	switch v := n.(type) {
	case *Operation:
		return v.Nodes
	case *Turn:
		return v.Nodes
	default:
		return nil
	}
}
