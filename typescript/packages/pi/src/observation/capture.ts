import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import type { AgentEvent, SessionStorage, SessionTreeEntry, StreamFn } from "@earendil-works/pi-agent-core";
import type { JsonValue } from "@compforge/agent-common";
import type { ModelCall, Node, Operation, ToolCall } from "@compforge/agent-observation";

type ObservationEvent = { type: "node"; node: Node } | { type: "link"; nodeId: string; entryId: string } |
  { type: "input"; entryId: string; mode: string } | { type: "gap"; reason: string };
export type PiObservationRecord = { version: 1; sessionId: string } & ObservationEvent;

export interface PiObservationOptions {
  sessionId: string;
  /** Synchronous append keeps records ordered before the observed event is forwarded. */
  write: (record: PiObservationRecord) => void;
  /** Observation failures are reported here, never used to stop or retry the agent. */
  onError: (error: Error) => void;
}

type Response = Awaited<ReturnType<Awaited<ReturnType<StreamFn>>["result"]>>;
const json = (value: unknown): JsonValue => JSON.parse(JSON.stringify(value)) as JsonValue;
const now = () => new Date().toISOString();

/**
 * @spec Supplement native Session records; no separate copies of stream context, input bodies, response bodies or tool results.
 * @spec Per-native-Session observation only. No Ledger, recovery state, credentials or historical replay.
 * @spec Stream invocation IDs exist before a response; native entry links never rename those IDs.
 * @see docs/observation.md
 */
export class PiObservation {
  private readonly responses = new WeakMap<object, string>();
  private readonly proposals = new Map<string, { modelId: string; name: string; input: unknown }>();
  private readonly tools = new Map<string, { id: string; ended: boolean }>();

  constructor(private readonly options: PiObservationOptions) {
    if (!options.sessionId) throw new Error("Pi observation requires a native Session ID");
  }

  private safely(action: () => void) {
    try { action(); }
    catch (error) {
      // A diagnostic callback must not accidentally become a model/tool failure.
      try { this.options.onError(error instanceof Error ? error : new Error(String(error))); } catch { /* caller owns diagnostics */ }
    }
  }
  private write(record: ObservationEvent) {
    try { this.options.write({ version: 1, sessionId: this.options.sessionId, ...record }); }
    catch (error) {
      const id = record.type === "node" ? record.node.id : record.type === "link" ? record.nodeId : record.type === "input" ? record.entryId : "gap";
      throw new Error(`Pi observation ${this.options.sessionId} ${record.type} ${id}: ${error instanceof Error ? error.message : String(error)}`, { cause: error });
    }
  }
  private node(node: Node) { this.write({ type: "node", node: { ...node, evidence: [{ source: "pi.observation", id: node.id }] } }); }

  /**
   * Wrap the actual provider stream, inside any wrapper that replaces onPayload.
   * The callback observes payload AFTER the supplied onPayload transformation. Providers must
   * honor that return value; this is the final hook payload, not a claim about HTTP wire bytes.
   */
  stream(stream: StreamFn): StreamFn {
    return async (model, context, options) => {
      const id = `pi-observation:${randomUUID()}`;
      this.safely(() => this.node({ id, kind: "model_call", started_at: now(),
        attributes: { request_boundary: "provider.onPayload.return" } }));
      let payloadCount = 0;
      let settled = false;
      const complete = (message: Response) => {
        if (settled) return;
        settled = true;
        this.responses.set(message, id);
        this.proposals.clear();
        this.safely(() => {
          for (const part of message.content) if (part.type === "toolCall") {
            this.proposals.set(part.id, { modelId: id, name: part.name, input: structuredClone(part.arguments) });
          }
        });
        this.safely(() => this.node({ id, kind: "model_call", finished_at: now(),
          status: message.stopReason === "error" ? "failed" : message.stopReason === "aborted" ? "cancelled" : "completed",
          ...(message.errorMessage ? { response: { error: { type: message.stopReason, message: message.errorMessage } } } : {}) } as ModelCall));
      };
      const failed = (error: unknown) => {
        if (settled) return;
        settled = true;
        this.safely(() => this.node({ id, kind: "model_call", finished_at: now(), status: "failed",
          response: { error: { type: "stream_error", message: error instanceof Error ? error.message : String(error) } } } as ModelCall));
      };
      try {
        const source = await stream(model, context, { ...options, onPayload: async (payload, selectedModel) => {
          const replacement = await options?.onPayload?.(payload, selectedModel);
          const effective = replacement === undefined ? payload : replacement;
          payloadCount++;
          this.safely(() => {
            const request = { input: json(effective), model: { id: selectedModel.id, provider: selectedModel.provider } };
            if (payloadCount === 1) this.node({ id, kind: "model_call", request } as ModelCall);
            else this.node({ id: `${id}:payload:${payloadCount}`, kind: "operation", operation: "pi.provider_payload",
              started_at: now(), data: { model_call_id: id, ordinal: payloadCount, request } } as Operation);
          });
          return replacement;
        } });
        // Preserve Pi's stream object and event protocol. Both result-only and iterator consumers
        // record completion once, before the final message can be appended to native storage.
        return new Proxy(source, { get(target, key) {
          if (key === "result") return async () => {
            try { const result = await target.result(); complete(result); return result; }
            catch (error) { failed(error); throw error; }
          };
          if (key === Symbol.asyncIterator) return async function* () {
            try {
              for await (const event of target) {
                if (event.type === "done" || event.type === "error") complete(await target.result());
                yield event;
              }
            } catch (error) { failed(error); throw error; }
          };
          const value = Reflect.get(target, key, target);
          return typeof value === "function" ? value.bind(target) : value;
        } });
      } catch (error) { failed(error); throw error; }
    };
  }

  /** Subscribe to native Agent events. No queue inspection or guessed input delivery modes. */
  event(event: AgentEvent): void {
    if (event.type === "turn_start" || event.type === "agent_end") {
      this.proposals.clear(); this.tools.clear();
    } else if (event.type === "tool_execution_start") {
      const id = `pi-observation:${randomUUID()}`;
      this.tools.set(event.toolCallId, { id, ended: false });
      const proposal = this.proposals.get(event.toolCallId);
      this.safely(() => this.node({ id, kind: "tool_call", tool_call_id: event.toolCallId,
        ...(proposal ? { model_call_id: proposal.modelId } : {}),
        // Native proposals suffice unless validation/hooks changed the actual execution arguments.
        ...(!proposal || proposal.name !== event.toolName || !isDeepStrictEqual(proposal.input, event.args)
          ? { request: { tool_name: event.toolName, input: json(event.args) } } : {}),
        started_at: now(), name: event.toolName } as ToolCall));
    } else if (event.type === "tool_execution_end") {
      const tool = this.tools.get(event.toolCallId);
      if (!tool) return; // An unobserved start stays a native-only result; do not invent one.
      tool.ended = true;
      this.safely(() => this.node({ id: tool.id, kind: "tool_call", finished_at: now(),
        status: event.isError ? "failed" : "completed" } as ToolCall));
    }
  }

  /** Caller supplies the delivery mode only AFTER Pi has applied and persisted this user entry. */
  input(entry: SessionTreeEntry, mode: string): void {
    if (entry.type !== "message" || entry.message.role !== "user") throw new Error("Expected an applied native user entry");
    this.safely(() => this.write({ type: "input", entryId: entry.id, mode }));
  }

  /** Explicit association for integrations that copy messages before native persistence. */
  link(nodeId: string, entryId: string): void {
    this.safely(() => this.write({ type: "link", nodeId, entryId }));
  }
  modelCallId(message: object): string | undefined { return this.responses.get(message); }

  /** Called AFTER native append. Object identity establishes model linkage, never text/timing similarity. */
  entry(entry: SessionTreeEntry): void {
    if (entry.type !== "message") return;
    if (entry.message.role === "assistant") {
      const id = this.responses.get(entry.message);
      if (id) this.link(id, entry.id);
    } else if (entry.message.role === "toolResult") {
      const tool = this.tools.get(entry.message.toolCallId);
      if (tool?.ended) { this.link(tool.id, entry.id); this.tools.delete(entry.message.toolCallId); }
    }
  }

  /** Opt-in native storage decoration; all persistence and recovery still belong to Pi. */
  storage<T extends SessionStorage>(storage: T): T {
    const observation = this;
    return new Proxy(storage, { get(target, key) {
      if (key === "appendEntry") return async (entry: SessionTreeEntry) => {
        await target.appendEntry(entry);
        observation.entry(entry);
      };
      const value = Reflect.get(target, key, target);
      return typeof value === "function" ? value.bind(target) : value;
    } });
  }
}
