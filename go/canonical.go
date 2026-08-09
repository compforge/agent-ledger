package agentledger

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
)

func CanonicalAppendDigest(events []ProposedEvent) (string, error) {
	encoded, err := json.Marshal(events)
	if err != nil {
		return "", fmt.Errorf("marshal append batch: %w", err)
	}
	canonical, err := jsoncanonicalizer.Transform(encoded)
	if err != nil {
		return "", fmt.Errorf("canonicalize append batch: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}
