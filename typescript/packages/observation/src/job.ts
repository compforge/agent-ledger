import type { JsonValue } from "@compforge/agent-common";
import type { Node, NodeRef, SourceRef, Status } from "./node.js";

export interface JobRef { source: string; id: string }
export interface Relation {
  kind: "depends_on" | "handoff";
  source: NodeRef;
  target: NodeRef;
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
  nodes: Node[];
  relations?: Relation[];
  attributes?: { [key: string]: JsonValue };
  issues?: string[];
}

