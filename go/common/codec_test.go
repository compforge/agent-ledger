package common

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestSharedCallRoundTrip(t *testing.T) {
	raw, err := os.ReadFile("../../conformance/vectors/common-call.json")
	if err != nil {
		t.Fatal(err)
	}
	var vector map[string]map[string]any
	if err := json.Unmarshal(raw, &vector); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"request", "response"} {
		payload := map[string]any{}
		for k, v := range vector[name] {
			payload[k] = v
		}
		for k, v := range vector["details"] {
			payload[k] = v
		}
		var result map[string]any
		if name == "request" {
			facts, err := DecodeRequest(payload)
			if err != nil {
				t.Fatal(err)
			}
			result, err = facts.Payload()
			if err != nil {
				t.Fatal(err)
			}
		} else {
			facts, err := DecodeResponse(payload)
			if err != nil {
				t.Fatal(err)
			}
			result, err = facts.Payload()
			if err != nil {
				t.Fatal(err)
			}
		}
		encoded, _ := json.Marshal(result)
		expected, _ := json.Marshal(vector[name])
		if string(encoded) != string(expected) {
			t.Fatalf("%s: %s != %s", name, encoded, expected)
		}
		if !reflect.DeepEqual(CallDetails(payload), vector["details"]) {
			t.Fatal("source details lost")
		}
	}
}

func TestCallContentAndUsageValidation(t *testing.T) {
	if _, err := DecodeRequest(map[string]any{"input": nil, "input_artifact_id": "artifact"}); err == nil {
		t.Fatal("ambiguous input accepted")
	}
	if _, err := DecodeResponse(map[string]any{"usage": map[string]any{"input_tokens": -1}}); err == nil {
		t.Fatal("negative usage accepted")
	}
	facts := CallRequest{Input: json.RawMessage(`{"id":9007199254740993}`)}
	payload, err := facts.Payload()
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(payload["input"])
	if string(raw) != string(facts.Input) {
		t.Fatal("native integer precision lost")
	}
}
