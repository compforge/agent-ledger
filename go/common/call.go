// Package common defines call facts shared by Ledger and View.
package common

import "encoding/json"

// Model identifies the requested or observed model, independently of storage identity.
type Model struct {
	ID       string `json:"id"`
	Provider string `json:"provider,omitempty"`
}

type Usage struct {
	InputTokens           *int64 `json:"input_tokens,omitempty"`
	OutputTokens          *int64 `json:"output_tokens,omitempty"`
	CacheReadInputTokens  *int64 `json:"cache_read_input_tokens,omitempty"`
	CacheWriteInputTokens *int64 `json:"cache_write_input_tokens,omitempty"`
	TotalTokens           *int64 `json:"total_tokens,omitempty"`
}

type CallError struct {
	Type      string `json:"type"`
	Message   string `json:"message"`
	Code      string `json:"code,omitempty"`
	Retryable *bool  `json:"retryable,omitempty"`
}

// CallRequest preserves native messages and tools without flattening their content.
type CallRequest struct {
	Input           json.RawMessage `json:"input,omitempty"`
	InputArtifactID string          `json:"input_artifact_id,omitempty"`
	Model           *Model          `json:"model,omitempty"`
	ToolName        string          `json:"tool_name,omitempty"`
	ClientRequestID string          `json:"client_request_id,omitempty"`
}

type CallResponse struct {
	Output              json.RawMessage `json:"output,omitempty"`
	OutputArtifactID    string          `json:"output_artifact_id,omitempty"`
	Model               *Model          `json:"model,omitempty"`
	Usage               *Usage          `json:"usage,omitempty"`
	Error               *CallError      `json:"error,omitempty"`
	FinishReason        string          `json:"finish_reason,omitempty"`
	ProviderRequestID   string          `json:"provider_request_id,omitempty"`
	ExternalOperationID string          `json:"external_operation_id,omitempty"`
}
