import { type CallRequest, type CallResponse, type JsonValue, type SessionRef } from "@compforge/agent-common";
export type Status = "unknown" | "running" | "completed" | "failed" | "cancelled" | "outcome_unknown";
export interface SourceRef { source: string; id: string; uri?: string }
export interface Node {
  kind: "input" | "model_call" | "tool_call" | "operation" | "turn";
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
export interface Operation extends Node { kind: "operation"; operation: string; data?: JsonValue; nodes?: Node[] }
export interface Turn extends Node { kind: "turn"; nodes: Node[] }
export interface NodeRef { id: string; session?: SessionRef }

// Internal narrowing for the wire discriminator; public collections expose Node.
type ConcreteNode = Input | ModelCall | ToolCall | Operation | Turn;
export function concrete(node: Node): ConcreteNode {
  if (!node || !["input", "model_call", "tool_call", "operation", "turn"].includes(node.kind)) {
    throw new Error("unknown observation node kind");
  }
  return node as ConcreteNode;
}

export function children(node: Node): Node[] {
  const value = concrete(node);
  return value.kind === "operation" || value.kind === "turn" ? value.nodes ?? [] : [];
}
