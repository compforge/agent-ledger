/** Shared execution facts. No Ledger store or View dependency. */
export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export interface Model { id: string; provider?: string }
export interface Usage {
  input_tokens?: number;
  output_tokens?: number;
  cache_read_input_tokens?: number;
  cache_write_input_tokens?: number;
  total_tokens?: number;
}
export interface CallError { type: string; message: string; code?: string; retryable?: boolean }
export interface CallRequest {
  input?: JsonValue;
  input_artifact_id?: string;
  model?: Model;
  tool_name?: string;
  client_request_id?: string;
}
export interface CallResponse {
  output?: JsonValue;
  output_artifact_id?: string;
  model?: Model;
  usage?: Usage;
  error?: CallError;
  finish_reason?: string;
  provider_request_id?: string;
  external_operation_id?: string;
}
