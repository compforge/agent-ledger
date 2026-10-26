import type { SessionMetadata, SessionTreeEntry, SessionStorage } from "@earendil-works/pi-agent-core";
import type { JsonValue } from "@compforge/agent-common";
import { validate, type SessionView, type Node, type ModelCall, type ToolCall } from "@compforge/agent-observation";

/**
 * @spec Read Pi's append-ordered session evidence without constructing model requests or executing tools.
 * @spec Tool proposals become observed calls only when an actual result exists; matching follows native ancestry.
 * @see docs/observation.md
 */
export function sessionView(metadata: SessionMetadata, entries: readonly SessionTreeEntry[]): SessionView {
  const result: SessionView = {schema_version: "agent-ledger/session@1", session: {source: "pi", id: metadata.id}, items: [], attributes: {metadata: json(metadata)}, issues: []};
  const byId = new Map<string, SessionTreeEntry>();
  const models = new Map<string, ModelCall>();
  for (const entry of entries) {
    if (byId.has(entry.id)) throw new Error(`Pi session ${metadata.id}: duplicate entry ${entry.id}`);
    if (entry.parentId !== null && !byId.has(entry.parentId)) result.issues!.push(`entry ${entry.id}: parent ${entry.parentId} not observed`);
    const base: Node = {id: entry.id, evidence: [{source: "pi", id: entry.id}], attributes: {parent_id: entry.parentId, entry_type: entry.type}};
    if (entry.type !== "message") {
      result.items.push({...base, kind: "operation", operation: `pi.${entry.type}`, data: json(entry), started_at: entry.timestamp});
    } else {
      const message = entry.message;
      if (message.role === "user") {
        result.items.push({...base, kind: "input", content: json(message), started_at: entry.timestamp});
      } else if (message.role === "assistant") {
        const model: ModelCall = {...base, kind: "model_call", finished_at: entry.timestamp,
          status: message.stopReason === "error" ? "failed" : message.stopReason === "aborted" ? "cancelled" : "completed",
          response: {output: json(message), finish_reason: message.stopReason,
            usage: {input_tokens: message.usage.input, output_tokens: message.usage.output,
              cache_read_input_tokens: message.usage.cacheRead, cache_write_input_tokens: message.usage.cacheWrite,
              total_tokens: message.usage.totalTokens}}};
        // Pi's `model` records the selected model; only responseModel establishes the returned identity.
        model.attributes!.requested_model = {id: message.model, provider: message.provider};
        if (message.responseModel) model.response!.model = {id: message.responseModel, provider: message.provider};
        if (message.responseId) model.response!.provider_request_id = message.responseId;
        if (message.errorMessage) model.response!.error = {type: message.stopReason, message: message.errorMessage};
        result.items.push(model); models.set(entry.id, model);
        result.issues!.push(`node ${entry.id}: exact model request not recorded in Pi session`);
      } else if (message.role === "toolResult") {
        const tool: ToolCall = {...base, kind: "tool_call", tool_call_id: message.toolCallId,
          finished_at: entry.timestamp, status: message.isError ? "failed" : "completed",
          response: {output: json(message)}};
        // A tool ID can occur again after a branch. Search only this result's ancestry.
        let parent = entry.parentId === null ? undefined : byId.get(entry.parentId);
        while (parent) {
          if (parent.type === "message" && parent.message.role === "assistant") {
            const proposal = parent.message.content.find(part => part.type === "toolCall" && part.id === message.toolCallId);
            if (proposal?.type === "toolCall") {
              tool.request = {tool_name: proposal.name, input: json(proposal.arguments)};
              tool.model_call_id = models.get(parent.id)!.id;
              break;
            }
          }
          parent = parent.parentId === null ? undefined : byId.get(parent.parentId);
        }
        if (!tool.request) result.issues!.push(`node ${entry.id}: tool request not observed`);
        tool.name = message.toolName;
        result.items.push(tool);
      } else {
        result.items.push({...base, kind: "operation", operation: `pi.message.${message.role}`, data: json(message), started_at: entry.timestamp});
      }
    }
    byId.set(entry.id, entry);
  }
  validate(result);
  return structuredClone(result);
}

/** Read-only convenience for native or Ledger-backed Pi SessionStorage. */
export async function readSessionView(storage: SessionStorage): Promise<SessionView> {
  return sessionView(await storage.getMetadata(), await storage.getEntries());
}

function json(value: unknown): JsonValue {
  return JSON.parse(JSON.stringify(value)) as JsonValue;
}
