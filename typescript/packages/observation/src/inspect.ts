import { validateRequest, validateResponse, type SessionRef } from "@compforge/agent-common";
import { concrete, type Node, type ToolCall, type SourceRef } from "./node.js";
import type { SessionView } from "./session.js";
import type { JobView, JobRef } from "./job.js";

export type View = SessionView | JobView;
export interface Inspection { issues: string[] }

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
export function nodeKey(id: string, session?: SessionRef): string {
  return JSON.stringify([session?.source ?? "", session?.id ?? "", id]);
}

/** @spec Shared graph validation preserves Session-scoped identities and permits partial Job evidence. */
export function inspect(document: View): Inspection {
  if (!document || !["agent-ledger/session@1", "agent-ledger/job@1"].includes(document.schema_version)) throw new Error("unsupported View schema");
  const session = document.schema_version === "agent-ledger/session@1" ? document.session : undefined;
  if (document.schema_version === "agent-ledger/session@1") identity(document.session);
  else {
    identity(document.job);
    status(document.status);
    timing(`job ${document.job.id}`, document.started_at, document.finished_at);
  }
  const nodes = new Map<string, Node>();
  const tools: ToolCall[] = [];
  const issues: string[] = [];
  function visit(value: Node, parent: string): void {
    const node = concrete(value);
    if (!node || typeof node.id !== "string" || !node.id) throw new Error("missing view node id");
    if (node.session !== undefined) {
      identity(node.session);
      if (session && nodeKey("", session) !== nodeKey("", node.session)) throw new Error(`node ${node.id}: foreign Session inside SessionView`);
    }
    const id = nodeKey(node.id, node.session ?? session);
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
    if (node.kind === "operation" && (typeof node.operation !== "string" || !node.operation)) throw new Error(`operation ${node.id}: missing operation`);
    if (node.kind === "input" && !Object.hasOwn(node, "content")) throw new Error(`input ${node.id}: missing content`);
    if ((node.kind === "turn" || node.kind === "operation") && node.nodes !== undefined && !Array.isArray(node.nodes)) throw new Error(`node ${node.id}: nodes must be an array`);
    if (node.kind === "turn" && !Array.isArray(node.nodes)) throw new Error(`turn ${node.id}: missing nodes`);
    if (node.kind === "turn" || node.kind === "operation") for (const child of node.nodes ?? []) visit(child, node.kind);
  }
  if (!Array.isArray(document.nodes)) throw new Error("view nodes must be an array");
  for (const value of document.nodes) visit(value, session ? "session" : "job");
  for (const tool of tools) {
    if (tool.model_call_id === undefined) continue;
    const owner = tool.session ?? session;
    const model = nodes.get(nodeKey(tool.model_call_id, owner));
    if (model && model.kind !== "model_call") throw new Error(`tool ${tool.id}: referenced node is not a model call`);
    if (!model) {
      if (session || !owner) throw new Error(`tool ${tool.id}: missing model call ${tool.model_call_id}`);
      issues.push(`tool ${nodeKey(tool.id, owner)}: model call ${tool.model_call_id} outside Job snapshot`);
    }
  }
  if (document.schema_version === "agent-ledger/job@1") for (const relation of document.relations ?? []) {
    if (!["depends_on", "handoff"].includes(relation.kind)) throw new Error(`invalid relation: ${relation.kind}`);
    evidence(relation.evidence);
    for (const ref of [relation.source, relation.target]) {
      if (!ref?.id) throw new Error("missing relation node id");
      if (ref.session) identity(ref.session);
      if (!nodes.has(nodeKey(ref.id, ref.session))) {
        if (!ref.session) throw new Error(`${relation.kind}: missing job node ${ref.id}`);
        issues.push(`${relation.kind}: Session node ${nodeKey(ref.id, ref.session)} outside Job snapshot`);
      }
    }
  }
  return {issues: [...new Set(issues)]};
}

export function validate(document: View): void { inspect(document); }
