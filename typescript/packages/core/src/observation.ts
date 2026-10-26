import { decodeRequest, decodeResponse, callDetails, type JsonValue } from "@compforge/agent-common";
import { validate, type SessionView, type Node, type Turn, type ModelCall, type ToolCall, type Operation, type Status } from "@compforge/agent-observation";
import type { SessionView as LedgerSessionView, StoredEvent } from "./types.js";

const terminal: Record<string, Status> = { completed: "completed", failed: "failed", cancelled: "cancelled", outcome_unknown: "outcome_unknown" };
/** Read-only projection. Every physical attempt stays visible; no recovery decisions are made. */
export function sessionView(source: LedgerSessionView): SessionView {
  const document: SessionView = {schema_version: "agent-ledger/session@1", session: {source: "ledger", id: source.session_id}, items: [], lanes: [], issues: []};
  const issue = (message: string) => document.issues!.push(message);
  const actions = new Map(source.actions.map(x => [x.id, x]));
  const attempts = new Map(source.attempts.map(x => [x.id, x]));
  const rows = new Map(source.turns.map(x => [x.id, x]));
  const turns = new Map<string, Turn>();
  const calls = new Map<string, ModelCall | ToolCall | Operation>();
  const terminals = new Map<string, StoredEvent[]>();
  function observe(node: Node, event: StoredEvent): void {
    (node.evidence ??= []).push({source: "ledger", id: event.id});
    const suffix = event.event_type.split(".").at(-1)!;
    if ((suffix === "started" || suffix === "requested") && node.started_at === undefined) node.started_at = event.occurred_at;
    if (terminal[suffix] !== undefined) {
      const observations = terminals.get(node.id) ?? [];
      observations.push(event); terminals.set(node.id, observations);
      node.finished_at = event.occurred_at;
      node.status = new Set(observations.map(e => terminal[e.event_type.split(".").at(-1)!])).size === 1 ? terminal[suffix] : "unknown";
      if (observations.length > 1) {
        (node.attributes ??= {}).terminal_observations = JSON.parse(JSON.stringify(observations)) as JsonValue;
        issue(`node ${node.id}: multiple terminal observations`);
      }
    }
    if (Object.keys(event.extensions).length) {
      const extensions = ((node.attributes ??= {}).event_extensions ??= {}) as Record<string, JsonValue>;
      extensions[event.id] = event.extensions;
    }
  }
  function turnFor(id: string): Turn {
    let turn = turns.get(id);
    if (turn === undefined) {
      const row = rows.get(id);
      if (row === undefined) throw new Error(`missing Turn ${id}`);
      turn = {kind: "turn", id: `turn:${id}`, lane_id: row.lane_id, run_id: source.lanes.find(l => l.id === row.lane_id)!.run_id, items: []};
      turns.set(id, turn);document.items.push(turn);
    }
    return turn;
  }
  const laneIds = new Set(source.lanes.map(x => x.id));
  for (const event of source.events) if (!laneIds.has(event.lane_id)) throw new Error(`event ${event.id}: missing Lane ${event.lane_id}`);
  for (const lane of source.lanes) {
    if (lane.session_id !== source.session_id) throw new Error(`Lane ${lane.id}: different session`);
    document.lanes!.push({id: lane.id, name: lane.name, ...(lane.parent_lane_id === undefined ? {} : {parent_id: lane.parent_lane_id})});
    for (const event of source.events.filter(e => e.lane_id === lane.id).sort((a,b) => a.seq-b.seq)) {
      if (event.event_type.startsWith("turn.") && rows.has(event.subject_id)) observe(turnFor(event.subject_id), event);
      else if (event.event_type.startsWith("attempt.")) {
        const attempt = attempts.get(event.subject_id);
        const action = attempt && actions.get(attempt.action_id);
        if (!attempt || !action || !rows.has(action.turn_id)) throw new Error(`event ${event.id}: missing Attempt/Action/Turn ownership`);
        if (rows.get(action.turn_id)!.lane_id !== event.lane_id) throw new Error(`event ${event.id}: Attempt belongs to another Lane`);
        let call = calls.get(attempt.id);
        if (!call) {
          const base = {id: `call:${attempt.id}`, lane_id: lane.id, run_id: lane.run_id, attributes: {action_id: action.id, attempt_id: attempt.id, attempt_no: attempt.attempt_no}};
          if (action.type === "model_call") call = {...base, kind: "model_call"};
          else if (action.type === "tool_call") call = {...base, kind: "tool_call", ...(action.key === undefined ? {} : {tool_call_id: action.key})};
          else call = {...base, kind: "operation", operation: action.type, items: []};
          calls.set(attempt.id, call);turnFor(action.turn_id).items.push(call);
        }
        observe(call,event);
        if (call.kind !== "operation") {
          if (event.event_type === "attempt.requested") call.request = decodeRequest(event.payload);
          else if (terminal[event.event_type.split(".").at(-1)!] !== undefined) {
            if (terminals.get(call.id)!.length === 1) call.response = decodeResponse(event.payload);
            else delete call.response;
          } else {
            const records = ((call.attributes ??= {}).events ??= []) as JsonValue[];
            records.push(JSON.parse(JSON.stringify(event)) as JsonValue);
          }
          const extra = callDetails(event.payload);
          if (Object.keys(extra).length) {
            const details = ((call.attributes ??= {}).event_details ??= {}) as Record<string, JsonValue>;
            details[event.id] = extra;
          }
        } else call.items!.push({kind:"operation",id:`event:${event.id}`,operation:event.event_type,data:event.payload,evidence:[{source:"ledger",id:event.id}]});
        continue;
      }
      const operation: Operation = {kind:"operation",id:`event:${event.id}`,lane_id:lane.id,operation:event.event_type,started_at:event.occurred_at,data:event.payload,evidence:[{source:"ledger",id:event.id}],attributes:{extensions:event.extensions}};
      if (event.event_type.startsWith("turn.") && rows.has(event.subject_id)) turnFor(event.subject_id).items.push(operation);
      else document.items.push(operation);
    }
  }
  for (const row of source.turns) turnFor(row.id);
  for (const attempt of source.attempts) if (!calls.has(attempt.id)) issue(`attempt ${attempt.id}: no recorded call events`);
  for (const call of calls.values()) {
    if (call.started_at === undefined) issue(`node ${call.id}: request missing`);
    if (!terminals.has(call.id)) issue(`node ${call.id}: result missing`);
  }
  validate(document);
  return structuredClone(document);
}
