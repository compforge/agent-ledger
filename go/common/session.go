package common

// SessionRef identifies one continuing agent session in a caller-defined source namespace.
type SessionRef struct {
	Source string `json:"source"`
	ID     string `json:"id"`
}
