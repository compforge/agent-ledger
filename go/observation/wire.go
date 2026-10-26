package observation

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Nodes preserves concrete node types at the JSON boundary.
// +spec=`Node collections round-trip every concrete type and its specialized fields`
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

type Nodes []node

func (nodes *Nodes) UnmarshalJSON(data []byte) error {
	var records []json.RawMessage
	if err := json.Unmarshal(data, &records); err != nil {
		return err
	}
	result := make(Nodes, 0, len(records))
	for _, raw := range records {
		var tag struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(raw, &tag); err != nil {
			return err
		}
		var value node
		switch tag.Kind {
		case "input":
			value = &Input{}
		case "model_call":
			value = &ModelCall{}
		case "tool_call":
			value = &ToolCall{}
		case "operation":
			value = &Operation{}
		case "turn":
			value = &Turn{}
		default:
			return fmt.Errorf("unknown view node kind %q", tag.Kind)
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(value); err != nil {
			return err
		}
		result = append(result, value)
	}
	*nodes = result
	return nil
}

func (n *Operation) UnmarshalJSON(raw []byte) error {
	type plain Operation
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode((*plain)(n)); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if data, ok := fields["data"]; ok && bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		n.Data = json.RawMessage("null")
	}
	return nil
}

func (nodes Nodes) MarshalJSON() ([]byte, error) {
	if nodes == nil {
		return []byte("[]"), nil
	}
	return json.Marshal([]node(nodes))
}
