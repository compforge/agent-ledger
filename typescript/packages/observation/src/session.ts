import type { JsonValue, SessionRef } from "@compforge/agent-common";
import { children, type Node } from "./node.js";
import { inspect } from "./inspect.js";

export interface Lane { id: string; name?: string; parent_id?: string }
/**
 * @spec One source-scoped Session per view; stable node IDs survive followup and resume.
 * @see docs/observation.md
 */
export interface SessionView {
 schema_version: "agent-ledger/session@1";
 session: SessionRef;
 nodes: Node[];
 lanes?: Lane[];
 attributes?: { [key: string]: JsonValue };
 issues?: string[];
}

/**
 * @spec Copy selected subtrees in source order and make every node's Session ownership explicit.
 * @see docs/observation.md
 */
export function selectNodes(session: SessionView, nodeIds: readonly string[]): Node[] {
  inspect(session);
  const wanted = new Set(nodeIds);
  const found = new Set<string>();
  const selected: Node[] = [];
  function collect(node: Node, covered: boolean): void {
    const chosen = wanted.has(node.id);
    if (chosen) {
      found.add(node.id);
      if (!covered) selected.push(structuredClone(node));
    }
    for (const child of children(node)) collect(child, covered || chosen);
  }
  for (const node of session.nodes) collect(node, false);
  const missing = [...wanted].filter(id => !found.has(id));
  if (missing.length) throw new Error(`Session ${session.session.id}: missing nodes ${missing.join(", ")}`);
  function attach(node: Node): void {
    node.session = {...session.session};
    for (const child of children(node)) attach(child);
  }
  selected.forEach(attach);
  return selected;
}
