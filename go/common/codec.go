package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// DecodeRequest reads shared facts from an event or native source envelope.
func DecodeRequest(payload map[string]any) (CallRequest, error) {
	var facts CallRequest
	err := decodeFacts(payload, &facts)
	if err == nil {
		err = facts.Validate()
	}
	return facts, err
}
func DecodeResponse(payload map[string]any) (CallResponse, error) {
	var facts CallResponse
	err := decodeFacts(payload, &facts)
	if err == nil {
		err = facts.Validate()
	}
	return facts, err
}
func decodeFacts(payload map[string]any, target any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

var callFields = func() map[string]bool {
	fields := map[string]bool{}
	for _, typ := range []reflect.Type{reflect.TypeFor[CallRequest](), reflect.TypeFor[CallResponse]()} {
		for i := 0; i < typ.NumField(); i++ {
			fields[strings.Split(typ.Field(i).Tag.Get("json"), ",")[0]] = true
		}
	}
	return fields
}()

// CallDetails retains source-specific evidence outside the shared contract.
func CallDetails(payload map[string]any) map[string]any {
	details := map[string]any{}
	for key, value := range payload {
		if !callFields[key] {
			details[key] = value
		}
	}
	return details
}

func (facts CallRequest) Payload() (map[string]any, error) {
	if err := facts.Validate(); err != nil {
		return nil, err
	}
	return encodeFacts(facts)
}
func (facts CallResponse) Payload() (map[string]any, error) {
	if err := facts.Validate(); err != nil {
		return nil, err
	}
	return encodeFacts(facts)
}
func encodeFacts(facts any) (map[string]any, error) {
	data, err := json.Marshal(facts)
	if err != nil {
		return nil, err
	}
	var payload map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	err = decoder.Decode(&payload)
	return payload, err
}

func (facts CallRequest) Validate() error {
	if facts.Input != nil && facts.InputArtifactID != "" {
		return fmt.Errorf("ambiguous request content")
	}
	if facts.Input != nil && !json.Valid(facts.Input) {
		return fmt.Errorf("invalid request JSON")
	}
	return validateModel(facts.Model)
}
func (facts CallResponse) Validate() error {
	if facts.Output != nil && facts.OutputArtifactID != "" {
		return fmt.Errorf("ambiguous response content")
	}
	if facts.Output != nil && !json.Valid(facts.Output) {
		return fmt.Errorf("invalid response JSON")
	}
	if err := validateModel(facts.Model); err != nil {
		return err
	}
	if facts.Usage != nil {
		u := facts.Usage
		for _, value := range []*int64{u.InputTokens, u.OutputTokens, u.CacheReadInputTokens, u.CacheWriteInputTokens, u.TotalTokens} {
			if value != nil && *value < 0 {
				return fmt.Errorf("negative usage")
			}
		}
	}
	return nil
}
func validateModel(model *Model) error {
	if model != nil && model.ID == "" {
		return fmt.Errorf("empty model id")
	}
	return nil
}
