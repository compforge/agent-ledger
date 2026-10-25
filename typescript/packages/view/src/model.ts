import type { CallRequest, CallResponse, JsonValue } from "@agent-ledger/common";
export type Status = "unknown" | "running" | "completed" | "failed" | "cancelled" | "outcome_unknown";
export interface SourceRef { source: string; id: string; uri?: string }
export interface Node {
  id: string;
  name?: string;
  lane_id?: string;
  started_at?: string;
  finished_at?: string;
  status?: Status;
  evidence?: SourceRef[];
  attributes?: { [key: string]: JsonValue };
}
export interface Input extends Node { kind: "input"; mode?: string; content: JsonValue }
export interface ModelCall extends Node { kind: "model_call"; request?: CallRequest; response?: CallResponse }
export interface ToolCall extends Node {
  kind: "tool_call";
  tool_call_id?: string;
  model_call_id?: string;
  request?: CallRequest;
  response?: CallResponse;
  runs?: Run[];
}
export interface Operation extends Node { kind: "operation"; operation: string; data?: JsonValue; items?: Item[] }
export interface Turn extends Node { kind: "turn"; items: Item[] }
export interface Lane { id: string; name?: string; parent_id?: string }
export interface Run extends Node { kind: "run"; session_id?: string; lanes?: Lane[]; items: Item[] }
export type Item = Input | ModelCall | ToolCall | Operation | Turn | Run;
export interface Document { schema_version: "agent-ledger/view@1"; runs: Run[]; issues?: string[] }

/** Validate relationships without manufacturing timing or recovery state.
 * @spec Missing timing is permitted; cross-lane order and async children imply no containment window.
 * @see docs/execution-view.md
 */
export function validate(document: Document): void {
  if (document.schema_version !== "agent-ledger/view@1") throw new Error("unsupported View schema");
  const ids = new Set<string>();
  const modelIds = new Set<string>();
  const tools: ToolCall[] = [];
  function visit(node: Item, parent: string): void {
    if (!node.id || ids.has(node.id)) throw new Error(`invalid or duplicate view node: ${node.id}`);
    ids.add(node.id);
    if (node.status !== undefined && !["unknown", "running", "completed", "failed", "cancelled", "outcome_unknown"].includes(node.status)) throw new Error(`node ${node.id}: invalid status`);
    if (node.kind === "turn" && parent !== "run" && parent !== "operation") throw new Error(`turn ${node.id}: invalid parent ${parent}`);
    for (const value of [node.started_at, node.finished_at]) {
      if (value !== undefined && (!/(Z|[+-]\d{2}:\d{2})$/.test(value) || !Number.isFinite(Date.parse(value)))) throw new Error(`node ${node.id}: invalid timestamp`);
    }
    if (node.started_at !== undefined && node.finished_at !== undefined && Date.parse(node.finished_at) < Date.parse(node.started_at)) throw new Error(`node ${node.id}: finish precedes start`);
    for (const ref of node.evidence ?? []) if (!ref.source || !ref.id) throw new Error(`node ${node.id}: invalid evidence reference`);
    if (node.kind === "model_call") modelIds.add(node.id);
    if (node.kind === "model_call" || node.kind === "tool_call") {
      if (node.request && "input" in node.request && node.request.input_artifact_id !== undefined) throw new Error(`node ${node.id}: ambiguous request content`);
      if (node.response && "output" in node.response && node.response.output_artifact_id !== undefined) throw new Error(`node ${node.id}: ambiguous response content`);
      for (const value of Object.values(node.response?.usage ?? {})) if (!Number.isSafeInteger(value) || value < 0) throw new Error(`node ${node.id}: invalid usage`);
      if (node.request?.model && !node.request.model.id || node.response?.model && !node.response.model.id) throw new Error(`node ${node.id}: empty model id`);
    }
    if (node.kind === "tool_call") {
      tools.push(node);
      for (const run of node.runs ?? []) visit(run, node.kind);
    }
    if (node.kind === "run" || node.kind === "turn" || node.kind === "operation") for (const child of node.items ?? []) visit(child, node.kind);
  }
  for (const run of document.runs) visit(run, "document");
  for (const tool of tools) if (tool.model_call_id !== undefined && !modelIds.has(tool.model_call_id)) throw new Error(`tool ${tool.id}: missing model call ${tool.model_call_id}`);
}
