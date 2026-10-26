package observation

import (
	"fmt"
	"reflect"
	"time"

	"github.com/compforge/agent-ledger/go/common"
)

// Inspection reports observation gaps without changing execution outcomes.
type Inspection struct {
	Issues []string `json:"issues"`
}

type nodeKey struct{ Source, Session, ID string }

func key(id string, session *common.SessionRef) nodeKey {
	if session == nil {
		return nodeKey{ID: id}
	}
	return nodeKey{Source: session.Source, Session: session.ID, ID: id}
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
func inspectGraph(nodes Nodes, session *common.SessionRef, relations []Relation) (Inspection, error) {
	index := map[nodeKey]node{}
	var tools []*ToolCall
	var issues []string
	var visit func(node, string) error
	visit = func(value node, parent string) error {
		if value == nil || (reflect.ValueOf(value).Kind() == reflect.Pointer && reflect.ValueOf(value).IsNil()) {
			return fmt.Errorf("nil view node")
		}
		n := value.Base()
		if err := checkNode(n); err != nil {
			return err
		}
		if session != nil && n.Session != nil && *session != *n.Session {
			return fmt.Errorf("node %s: foreign Session inside SessionView", n.ID)
		}
		identity := key(n.ID, owner(n, session))
		if _, exists := index[identity]; exists {
			return fmt.Errorf("duplicate view node %v", identity)
		}
		index[identity] = value
		if value.Kind() == "turn" && parent != "session" && parent != "job" && parent != "operation" {
			return fmt.Errorf("turn %s: invalid parent %s", n.ID, parent)
		}
		var children Nodes
		var request *common.CallRequest
		var response *common.CallResponse
		switch v := value.(type) {
		case *Input:
		case *Turn:
			children = v.Nodes
		case *Operation:
			if v.Operation == "" {
				return fmt.Errorf("operation %s: empty operation type", n.ID)
			}
			children = v.Nodes
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
		default:
			return fmt.Errorf("node %s: unsupported concrete node type", n.ID)
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
			if err := visit(child, value.Kind()); err != nil {
				return err
			}
		}
		return nil
	}
	root := "job"
	if session != nil {
		root = "session"
	}
	for _, value := range nodes {
		if err := visit(value, root); err != nil {
			return Inspection{}, err
		}
	}
	for _, tool := range tools {
		if tool.ModelCallID == "" {
			continue
		}
		scope := owner(&tool.Node, session)
		target, exists := index[key(tool.ModelCallID, scope)]
		if exists && target.Kind() != "model_call" {
			return Inspection{}, fmt.Errorf("tool %s: referenced node is not a model call", tool.ID)
		}
		if !exists {
			if session != nil || scope == nil {
				return Inspection{}, fmt.Errorf("tool %s: missing model call %s", tool.ID, tool.ModelCallID)
			}
			issues = append(issues, fmt.Sprintf("tool %v: model call %s outside Job snapshot", key(tool.ID, scope), tool.ModelCallID))
		}
	}
	for _, relation := range relations {
		if relation.Kind != "depends_on" && relation.Kind != "handoff" {
			return Inspection{}, fmt.Errorf("invalid relation %s", relation.Kind)
		}
		if err := checkEvidence(relation.Evidence); err != nil {
			return Inspection{}, err
		}
		for _, ref := range []NodeRef{relation.Source, relation.Target} {
			if ref.ID == "" {
				return Inspection{}, fmt.Errorf("missing relation node id")
			}
			if ref.Session != nil && (ref.Session.Source == "" || ref.Session.ID == "") {
				return Inspection{}, fmt.Errorf("invalid relation Session identity")
			}
			if _, exists := index[key(ref.ID, ref.Session)]; !exists {
				if ref.Session == nil {
					return Inspection{}, fmt.Errorf("%s: missing job node %s", relation.Kind, ref.ID)
				}
				issues = append(issues, fmt.Sprintf("%s: Session node %v outside Job snapshot", relation.Kind, key(ref.ID, ref.Session)))
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
	return Inspection{Issues: unique}, nil
}
