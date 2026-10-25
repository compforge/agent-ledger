import { validateRequest, validateResponse, type CallRequest, type CallResponse, type JsonValue, type SessionRef } from "@compforge/agent-common";
export type Status = "unknown" | "running" | "completed" | "failed" | "cancelled" | "outcome_unknown";
export interface SourceRef { source: string; id: string; uri?: string }
export interface Node {
  id: string;
  name?: string;
  lane_id?: string;
  run_id?: string;
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
  sessions?: SessionRef[];
}
export interface Operation extends Node { kind: "operation"; operation: string; data?: JsonValue; items?: Item[] }
export interface Turn extends Node { kind: "turn"; items: Item[] }
export interface Lane { id: string; name?: string; parent_id?: string }
export type Item = Input | ModelCall | ToolCall | Operation | Turn;
/**
 * @spec One source-scoped Session per view; stable item IDs survive followup and resume.
 * @see docs/observation.md
 */
export interface SessionView {
 schema_version: "agent-ledger/session-view@1";
 session: SessionRef;
 items: Item[];
 lanes?: Lane[];
 attributes?: { [key: string]: JsonValue };
 issues?: string[];
}

/** Validate relationships without manufacturing timing or recovery state.
 * @spec Missing timing is permitted; cross-lane order and async children imply no containment window.
 * @see docs/observation.md
 */
export function validate(document: SessionView): void {
  if (document.schema_version !== "agent-ledger/session-view@1") throw new Error("unsupported View schema");
  if (!document.session?.source || !document.session.id) throw new Error("missing session identity");
  const ids = new Set<string>();
  const modelIds = new Set<string>();
  const tools: ToolCall[] = [];
  function visit(node: Item, parent: string): void {
    if (!node.id || ids.has(node.id)) throw new Error(`invalid or duplicate view node: ${node.id}`);
    ids.add(node.id);
    if (node.status !== undefined && !["unknown", "running", "completed", "failed", "cancelled", "outcome_unknown"].includes(node.status)) throw new Error(`node ${node.id}: invalid status`);
    if (node.kind === "turn" && parent !== "session" && parent !== "operation") throw new Error(`turn ${node.id}: invalid parent ${parent}`);
    for (const value of [node.started_at, node.finished_at]) {
      if (value !== undefined && (!/(Z|[+-]\d{2}:\d{2})$/.test(value) || !Number.isFinite(Date.parse(value)))) throw new Error(`node ${node.id}: invalid timestamp`);
    }
    if (node.started_at !== undefined && node.finished_at !== undefined && Date.parse(node.finished_at) < Date.parse(node.started_at)) throw new Error(`node ${node.id}: finish precedes start`);
    for (const ref of node.evidence ?? []) if (!ref.source || !ref.id) throw new Error(`node ${node.id}: invalid evidence reference`);
    if (node.kind === "model_call") modelIds.add(node.id);
    if (node.kind === "model_call" || node.kind === "tool_call") {
      if (node.request) validateRequest(node.request);
      if (node.response) validateResponse(node.response);
    }
    if (node.kind === "tool_call") {
      tools.push(node);
      for (const session of node.sessions ?? []) if (!session.source || !session.id) throw new Error(`tool ${node.id}: invalid session reference`);
    }
    if (node.kind === "turn" || node.kind === "operation") for (const child of node.items ?? []) visit(child, node.kind);
  }
  for (const item of document.items) visit(item, "session");
  for (const tool of tools) if (tool.model_call_id !== undefined && !modelIds.has(tool.model_call_id)) throw new Error(`tool ${tool.id}: missing model call ${tool.model_call_id}`);
}
