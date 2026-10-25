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


type JsonObject = { [key: string]: JsonValue };
type Validator = (value: JsonValue) => void;
const native: Validator = () => {};
const string: Validator = value => { if (typeof value !== "string" || !value.length) throw new Error("expected nonempty string"); };
const object = (value: JsonValue): JsonObject => {
 if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error("expected object");
 return value;
};
const model: Validator = value => {
 const fields = object(value);
 string(fields.id!);
 if (fields.provider !== undefined) string(fields.provider);
};
const usage: Validator = value => {
 for (const count of Object.values(object(value))) if (typeof count !== "number" || !Number.isSafeInteger(count) || count < 0) throw new Error("invalid usage");
};
const error: Validator = value => {
 const fields = object(value);
 string(fields.type!);
 if (typeof fields.message !== "string") throw new Error("expected error message");
 if (fields.code !== undefined) string(fields.code);
 if (fields.retryable !== undefined && typeof fields.retryable !== "boolean") throw new Error("expected retryable boolean");
};
// +spec=Every shared field has one codec used by both Recorder and View
// +link=docs/observation.md
const requestFields: Record<keyof CallRequest, Validator> = {
 input: native, input_artifact_id: string, model, tool_name: string, client_request_id: string,
};
const responseFields: Record<keyof CallResponse, Validator> = {
 output: native, output_artifact_id: string, model, usage, error,
 finish_reason: string, provider_request_id: string, external_operation_id: string,
};
function select(payload: JsonObject, fields: object): JsonObject {
 return Object.fromEntries(Object.entries(payload).filter(([key]) => Object.hasOwn(fields, key)));
}
function validateFields(facts: object, fields: Record<string, Validator>): void {
 for (const [key, value] of Object.entries(facts)) {
  const validate = Object.hasOwn(fields, key) ? fields[key] : undefined;
  if (!validate) throw new Error(`unknown call field: ${key}`);
  if (value !== undefined) {
   try { validate(value as JsonValue); }
   catch (error) { throw new Error(`call field ${key}: ${String(error)}`, {cause: error}); }
  }
 }
}
export function callDetails(payload: JsonObject): JsonObject {
 return Object.fromEntries(Object.entries(payload).filter(([key]) => !Object.hasOwn(requestFields, key) && !Object.hasOwn(responseFields, key)));
}
export function decodeRequest(payload: JsonObject): CallRequest {
 const facts = select(payload, requestFields) as CallRequest;
 validateRequest(facts);
 return structuredClone(facts);
}
export function decodeResponse(payload: JsonObject): CallResponse {
 const facts = select(payload, responseFields) as CallResponse;
 validateResponse(facts);
 return structuredClone(facts);
}
export function requestPayload(facts: CallRequest): JsonObject {
 validateRequest(facts);
 return JSON.parse(JSON.stringify(facts)) as JsonObject;
}
export function responsePayload(facts: CallResponse): JsonObject {
 validateResponse(facts);
 return JSON.parse(JSON.stringify(facts)) as JsonObject;
}
export function validateRequest(facts: CallRequest): void {
 validateFields(facts, requestFields);
 if (facts.input !== undefined && facts.input_artifact_id !== undefined) throw new Error("ambiguous request content");
}
export function validateResponse(facts: CallResponse): void {
 validateFields(facts, responseFields);
 if (facts.output !== undefined && facts.output_artifact_id !== undefined) throw new Error("ambiguous response content");
}

/** Source-scoped identity of one continuing agent session, independent of business task IDs. */
export interface SessionRef { source: string; id: string }
