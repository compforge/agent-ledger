package observation

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/compforge/agent-ledger/go/common"
)

const SessionSchemaVersion = "agent-ledger/session@1"

type Lane struct {
	ID       string `json:"id"`
	Name     string `json:"name,omitempty"`
	ParentID string `json:"parent_id,omitempty"`
}

type SessionView struct {
	Attributes    map[string]any    `json:"attributes,omitempty"`
	SchemaVersion string            `json:"schema_version"`
	Session       common.SessionRef `json:"session"`
	Nodes         Nodes             `json:"nodes"`
	Lanes         []Lane            `json:"lanes,omitempty"`
	Issues        []string          `json:"issues,omitempty"`
}

func (d SessionView) Validate() error {
	_, err := d.Inspect()
	return err
}

func (d SessionView) Inspect() (Inspection, error) {
	if d.SchemaVersion != SessionSchemaVersion {
		return Inspection{}, fmt.Errorf("unsupported View schema %q", d.SchemaVersion)
	}
	if d.Session.Source == "" || d.Session.ID == "" {
		return Inspection{}, fmt.Errorf("missing session identity")
	}
	return inspectGraph(d.Nodes, &d.Session, nil)
}

// SelectNodes copies whole subtrees in source order, with explicit Session ownership.
// +spec=`Selection never mutates source facts or expands to unselected linked calls`
// +link=docs/observation.md
func SelectNodes(session SessionView, ids []string) (Nodes, error) {
	if err := session.Validate(); err != nil {
		return nil, err
	}
	wanted := map[string]bool{}
	for _, id := range ids {
		wanted[id] = false
	}
	selected := Nodes{}
	var collect func(node, bool)
	collect = func(n node, covered bool) {
		_, chosen := wanted[n.Base().ID]
		if chosen {
			wanted[n.Base().ID] = true
			if !covered {
				selected = append(selected, n)
			}
		}
		for _, child := range children(n) {
			collect(child, covered || chosen)
		}
	}
	for _, n := range session.Nodes {
		collect(n, false)
	}
	missing := []string{}
	for id, found := range wanted {
		if !found {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, fmt.Errorf("Session %s: missing nodes %v", session.Session.ID, missing)
	}
	// Use the wire contract to deep-copy native JSON values and concrete variants together.
	raw, err := json.Marshal(selected)
	if err != nil {
		return nil, fmt.Errorf("copy Session nodes: %w", err)
	}
	var result Nodes
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("copy Session nodes: %w", err)
	}
	var attach func(node)
	attach = func(n node) {
		owner := session.Session
		n.Base().Session = &owner
		for _, child := range children(n) {
			attach(child)
		}
	}
	for _, n := range result {
		attach(n)
	}
	return result, nil
}
