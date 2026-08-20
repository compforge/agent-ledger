package agentledger

import (
	"encoding/json"
	"os"
	"testing"
)

func TestAppendDigestMatchesCrossLanguageVector(t *testing.T) {
	data, err := os.ReadFile("../conformance/vectors/append.json")
	if err != nil {
		t.Fatal(err)
	}
	var vector struct {
		Events []ProposedEvent `json:"events"`
		SHA256 string          `json:"sha256"`
	}
	if err := json.Unmarshal(data, &vector); err != nil {
		t.Fatal(err)
	}
	digest, err := CanonicalAppendDigest(vector.Events)
	if err != nil {
		t.Fatal(err)
	}
	if digest != vector.SHA256 {
		t.Fatalf("digest = %s, want %s", digest, vector.SHA256)
	}
}
