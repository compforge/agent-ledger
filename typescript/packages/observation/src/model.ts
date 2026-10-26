import { validateRequest, validateResponse, type CallRequest, type CallResponse, type JsonValue, type SessionRef } from "@compforge/agent-common";
export type Status = "unknown" | "running" | "completed" | "failed" | "cancelled" | "outcome_unknown";
export interface SourceRef { source: string; id: string; uri?: string }
export interface Node {
  id: string;
  name?: string;
  session?: SessionRef;
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
 schema_version: "agent-ledger/session@1";
 session: SessionRef;
 items: Item[];
 lanes?: Lane[];
 attributes?: { [key: string]: JsonValue };
 issues?: string[];
}

export interface JobRef { source: string; id: string }
export interface ItemRef { id: string; session?: SessionRef }
export interface Relation {
  kind: "depends_on" | "handoff";
  source: ItemRef;
  target: ItemRef;
  evidence?: SourceRef[];
}

/**
 * @spec A Job groups observed work across Sessions, without claiming exclusive ownership of calls.
 * @see docs/observation.md
 */
export interface JobView {
  schema_version: "agent-ledger/job@1";
  job: JobRef;
  name?: string;
  goal?: JsonValue;
  result?: JsonValue;
  status?: Status;
  started_at?: string;
  finished_at?: string;
  items: Item[];
  relations?: Relation[];
  attributes?: { [key: string]: JsonValue };
  issues?: string[];
}

export type View = SessionView | JobView;

function identity(ref: SessionRef | JobRef): void {
  if (!ref || typeof ref.source !== "string" || !ref.source || typeof ref.id !== "string" || !ref.id) throw new Error("missing source-scoped identity");
}
function timing(owner: string, start?: string, end?: string): void {
  for (const value of [start, end]) {
    if (value !== undefined && (!/(Z|[+-]\d{2}:\d{2})$/.test(value) || !Number.isFinite(Date.parse(value)))) throw new Error(`${owner}: invalid timestamp`);
  }
  if (start !== undefined && end !== undefined && Date.parse(end) < Date.parse(start)) throw new Error(`${owner}: finish precedes start`);
}
function status(value: string | undefined): void {
  if (value !== undefined && !["unknown", "running", "completed", "failed", "cancelled", "outcome_unknown"].includes(value)) throw new Error(`invalid status: ${value}`);
}
function evidence(refs: readonly SourceRef[] = []): void {
  for (const ref of refs) identity(ref);
}
function key(id: string, session?: SessionRef): string {
  return JSON.stringify([session?.source ?? "", session?.id ?? "", id]);
}

/** @spec Shared graph validation preserves Session-scoped identities and permits partial Job evidence. */
function check(document: View): string[] {
  if (!document || !["agent-ledger/session@1", "agent-ledger/job@1"].includes(document.schema_version)) throw new Error("unsupported View schema");
  const session = document.schema_version === "agent-ledger/session@1" ? document.session : undefined;
  if (document.schema_version === "agent-ledger/session@1") identity(document.session);
  else {
    identity(document.job);
    status(document.status);
    timing(`job ${document.job.id}`, document.started_at, document.finished_at);
  }
  const nodes = new Map<string, Item>();
  const tools: ToolCall[] = [];
  const issues: string[] = [];
  function visit(node: Item, parent: string): void {
    if (!node || typeof node.id !== "string" || !node.id) throw new Error("missing view node id");
    if (node.session !== undefined) {
      identity(node.session);
      if (session && key("", session) !== key("", node.session)) throw new Error(`node ${node.id}: foreign Session inside SessionView`);
    }
    const id = key(node.id, node.session ?? session);
    if (nodes.has(id)) throw new Error(`duplicate view node: ${id}`);
    nodes.set(id, node);
    status(node.status);
    timing(`node ${node.id}`, node.started_at, node.finished_at);
    evidence(node.evidence);
    if (node.kind === "turn" && !["session", "job", "operation"].includes(parent)) throw new Error(`turn ${node.id}: invalid parent ${parent}`);
    if (node.kind === "model_call" || node.kind === "tool_call") {
      if (node.request) validateRequest(node.request);
      if (node.response) validateResponse(node.response);
    }
    if (node.kind === "tool_call") {
      tools.push(node);
      for (const ref of node.sessions ?? []) identity(ref);
    }
    if (node.kind === "operation" && !node.operation) throw new Error(`operation ${node.id}: missing operation`);
    if (node.kind === "input" && !Object.hasOwn(node, "content")) throw new Error(`input ${node.id}: missing content`);
    if (!["input", "model_call", "tool_call", "turn", "operation"].includes(node.kind)) throw new Error("unknown view item kind");
    if (node.kind === "turn" || node.kind === "operation") for (const child of node.items ?? []) visit(child, node.kind);
  }
  for (const item of document.items) visit(item, session ? "session" : "job");
  for (const tool of tools) {
    if (tool.model_call_id === undefined) continue;
    const owner = tool.session ?? session;
    const model = nodes.get(key(tool.model_call_id, owner));
    if (model && model.kind !== "model_call") throw new Error(`tool ${tool.id}: referenced item is not a model call`);
    if (!model) {
      if (session || !owner) throw new Error(`tool ${tool.id}: missing model call ${tool.model_call_id}`);
      issues.push(`tool ${key(tool.id, owner)}: model call ${tool.model_call_id} outside Job snapshot`);
    }
  }
  if (document.schema_version === "agent-ledger/job@1") for (const relation of document.relations ?? []) {
    if (!["depends_on", "handoff"].includes(relation.kind)) throw new Error(`invalid relation: ${relation.kind}`);
    evidence(relation.evidence);
    for (const ref of [relation.source, relation.target]) {
      if (!ref?.id) throw new Error("missing relation item id");
      if (ref.session) identity(ref.session);
      if (!nodes.has(key(ref.id, ref.session))) {
        if (!ref.session) throw new Error(`${relation.kind}: missing job item ${ref.id}`);
        issues.push(`${relation.kind}: Session item ${key(ref.id, ref.session)} outside Job snapshot`);
      }
    }
  }
  return [...new Set(issues)];
}

export function validate(document: View): void { check(document); }
/** Missing external fragments are observation gaps, not fabricated failed executions. */
export function referenceIssues(document: View): string[] { return check(document); }
