import { inspect, type Input, type ModelCall, type Node, type SessionView, type ToolCall } from "@compforge/agent-observation";
import type { PiObservationSnapshot } from "./journal.js";

/** Enrich native observations only through explicit entry links. Never join by timestamps or tool names. */
export function mergeObservation(native: SessionView, snapshot: PiObservationSnapshot): SessionView {
  if (native.session.source !== "pi" || native.session.id !== snapshot.sessionId) throw new Error("Pi observation Session mismatch");
  const result = structuredClone(native);
  result.issues = [...(result.issues ?? []), ...snapshot.issues];
  const captured = new Map(snapshot.nodes.map(node => [node.id, structuredClone(node)]));
  const nativeById = new Map(result.nodes.map(node => [node.id, node]));
  const byEntry = new Map<string, string>();
  const byNode = new Map<string, string>();
  for (const { entryId, mode } of snapshot.inputs) {
    const input = nativeById.get(entryId);
    if (input?.kind !== "input") { result.issues.push(`input ${entryId}: native entry not observed`); continue; }
    (input as Input).mode = mode;
  }
  for (const { nodeId, entryId } of snapshot.links) {
    const node = captured.get(nodeId), entry = nativeById.get(entryId);
    if (!node || !entry) { result.issues.push(`entry link ${entryId} -> ${nodeId}: evidence not observed`); continue; }
    if (node.kind !== entry.kind) throw new Error(`entry ${entryId}: observation kind mismatch`);
    if ((byEntry.has(entryId) && byEntry.get(entryId) !== nodeId) || (byNode.has(nodeId) && byNode.get(nodeId) !== entryId)) {
      throw new Error(`entry ${entryId}: conflicting observation link`);
    }
    byEntry.set(entryId, nodeId); byNode.set(nodeId, entryId);
  }
  result.nodes = result.nodes.map(entry => {
    const id = byEntry.get(entry.id);
    if (!id) return entry;
    const node = captured.get(id)!;
    captured.delete(id);
    // Native persistence can establish an outcome even when the observation terminal write was lost.
    const merged: Node = { ...entry, ...node, evidence: [...(entry.evidence ?? []), { source: "pi.observation", id }],
      attributes: { ...entry.attributes, ...node.attributes, native_entry_id: entry.id } };
    // Native bodies and final outcomes are authoritative. Supplementary errors only survive without them.
    if (entry.kind === "model_call" || entry.kind === "tool_call") {
      const original = entry as ModelCall | ToolCall;
      if (original.response) (merged as ModelCall | ToolCall).response = original.response;
      if (entry.status) merged.status = entry.status;
    }
    if (!node.finished_at && entry.finished_at) {
      merged.status = entry.status!;
      result.issues = result.issues!.filter(issue => issue !== `node ${id}: completion not observed`);
    }
    if (node.kind === "model_call" && (node as ModelCall).request) {
      result.issues = result.issues!.filter(issue => issue !== `node ${entry.id}: exact model request not recorded in Pi session`);
    }
    return merged;
  });
  if (captured.size) {
    result.issues.push("Unlinked observation nodes follow native entries in capture order; their array position does not establish cross-source chronology");
    result.nodes.push(...captured.values());
    for (const node of captured.values()) {
      if (node.kind === "model_call" || node.kind === "tool_call") result.issues.push(`node ${node.id}: native result not linked or unavailable; body not copied into observation`);
    }
  }
  for (const node of result.nodes) {
    if (node.kind === "tool_call") {
      const tool = node as ToolCall;
      if (tool.model_call_id && byEntry.has(tool.model_call_id)) tool.model_call_id = byEntry.get(tool.model_call_id)!;
    }
    if (node.kind === "model_call" && !(node as ModelCall).request && !nativeById.has(node.id)) {
      result.issues.push(`node ${node.id}: provider payload not observed`);
    }
  }
  // Supplemental fragments can legitimately lack a model start. Preserve the gap explicitly;
  // SessionView still requires any populated model_call_id to resolve locally.
  const ids = new Set(result.nodes.map(node => node.id));
  for (const node of result.nodes) {
    if (node.kind !== "tool_call") continue;
    const tool = node as ToolCall;
    if (tool.model_call_id && !ids.has(tool.model_call_id)) {
      result.issues.push(`node ${node.id}: model ${tool.model_call_id} not observed`);
      tool.attributes = { ...tool.attributes, unobserved_model_call_id: tool.model_call_id };
      delete tool.model_call_id;
    }
  }
  inspect(result);
  return result;
}
