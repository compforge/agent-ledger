package observation

import (
	"encoding/json"
	"fmt"
)

const JobSchemaVersion = "agent-ledger/job@1"

// A JobRef is a business-owned task identity, not a durable Ledger entity.
type JobRef struct {
	Source string `json:"source"`
	ID     string `json:"id"`
}
type Relation struct {
	Kind     string      `json:"kind"`
	Source   NodeRef     `json:"source"`
	Target   NodeRef     `json:"target"`
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
	Nodes         Nodes           `json:"nodes"`
	Relations     []Relation      `json:"relations,omitempty"`
	Attributes    map[string]any  `json:"attributes,omitempty"`
	Issues        []string        `json:"issues,omitempty"`
}

func (d JobView) Validate() error {
	_, err := d.Inspect()
	return err
}

// Inspect checks one Job snapshot without mutating source evidence.
func (d JobView) Inspect() (Inspection, error) {
	if d.SchemaVersion != JobSchemaVersion {
		return Inspection{}, fmt.Errorf("unsupported View schema %q", d.SchemaVersion)
	}
	if d.Job.Source == "" || d.Job.ID == "" {
		return Inspection{}, fmt.Errorf("missing job identity")
	}
	if err := checkNode(&Node{ID: d.Job.ID, Status: d.Status, StartedAt: d.StartedAt, FinishedAt: d.FinishedAt}); err != nil {
		return Inspection{}, err
	}
	return inspectGraph(d.Nodes, nil, d.Relations)
}
