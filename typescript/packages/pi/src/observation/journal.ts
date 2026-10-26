import { appendFileSync, existsSync, readFileSync } from "node:fs";
import { gzipSync, gunzipSync } from "node:zlib";
import type { Node } from "@compforge/agent-observation";
import type { PiObservationRecord } from "./capture.js";

export interface PiObservationSnapshot {
  sessionId: string;
  nodes: Node[];
  links: Array<{ nodeId: string; entryId: string }>;
  inputs: Array<{ entryId: string; mode: string }>;
  issues: string[];
}

/** Disk budget per supplemental file; retention across files belongs to the caller. */
export const DEFAULT_OBSERVATION_MAX_BYTES = 16 * 1024 * 1024;
const encode = (record: PiObservationRecord) => `${JSON.stringify(gzipSync(JSON.stringify(record)).toString("base64"))}\n`;
const decode = (line: string): PiObservationRecord => JSON.parse(gunzipSync(Buffer.from(JSON.parse(line), "base64")).toString("utf8"));

/**
 * @spec JSONL frames contain independent gzip records, preserving complete prefixes after interruption.
 * @spec Cap actual disk bytes. Reserve a gap marker; stop rather than evict evidence or grow without bound.
 * @see docs/observation.md
 */
export function observationWriter(path: string, options: { maxBytes?: number } = {}): (record: PiObservationRecord) => void {
  const maxBytes = options.maxBytes ?? DEFAULT_OBSERVATION_MAX_BYTES;
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1024) throw new Error("Pi observation maxBytes must be an integer >= 1024");
  let size: number | undefined;
  let stopped = false;
  return record => {
    if (stopped) return;
    if (size === undefined) {
      const text = existsSync(path) ? readFileSync(path, "utf8") : "";
      if (text && !text.endsWith("\n")) throw new Error("Pi observation has an incomplete tail; retain this file and start a new segment");
      if (text) {
        const last = decode(text.slice(text.lastIndexOf("\n", text.length - 2) + 1, -1));
        if (last.sessionId !== record.sessionId) throw new Error("Pi observation writer Session identity mismatch");
        if (last.type === "gap") { stopped = true; throw new Error(last.reason); }
      }
      size = Buffer.byteLength(text);
    }
    const line = encode(record);
    const reason = `Observation capacity reached (${maxBytes} bytes); subsequent supplemental evidence was not recorded`;
    const gap = encode({ version: 1, sessionId: record.sessionId, type: "gap", reason });
    if (size + Buffer.byteLength(line) + Buffer.byteLength(gap) > maxBytes) {
      stopped = true;
      if (size + Buffer.byteLength(gap) <= maxBytes) appendFileSync(path, gap, { encoding: "utf8", mode: 0o600 });
      throw new Error(reason);
    }
    appendFileSync(path, line, { encoding: "utf8", mode: 0o600 });
    size += Buffer.byteLength(line);
  };
}

/** Read complete prefixes of caller-selected segments; missing native bodies are resolved only when projecting. */
export function readObservation(path: string | readonly string[], sessionId: string): PiObservationSnapshot {
  const result: PiObservationSnapshot = { sessionId, nodes: [], links: [], inputs: [], issues: [] };
  const nodes = new Map<string, Node>();
  const paths = typeof path === "string" ? [path] : path;
  for (const [segment, file] of paths.entries()) {
    const lines = readFileSync(file, "utf8").split("\n");
    for (let index = 0; index < lines.length - 1; index++) {
      let record: PiObservationRecord;
      try {
        record = decode(lines[index]!);
        if (record.version !== 1 || record.sessionId !== sessionId) throw new Error("version or Session identity mismatch");
        if (record.type === "node") {
          if (!record.node || typeof record.node.id !== "string" || !record.node.id ||
            !["model_call", "tool_call", "operation"].includes(record.node.kind)) throw new Error("invalid supplemental node");
          const previous = nodes.get(record.node.id);
          if (previous && previous.kind !== record.node.kind) throw new Error("node kind changed");
          nodes.set(record.node.id, { ...previous, ...record.node });
        } else if (record.type === "link") {
          if (typeof record.nodeId !== "string" || typeof record.entryId !== "string") throw new Error("invalid entry link");
          result.links.push({ nodeId: record.nodeId, entryId: record.entryId });
        } else if (record.type === "input") {
          if (typeof record.entryId !== "string" || typeof record.mode !== "string") throw new Error("invalid input reference");
          result.inputs.push({ entryId: record.entryId, mode: record.mode });
        } else if (record.type === "gap") result.issues.push(record.reason);
        else throw new Error("unknown record type");
      } catch (error) {
        result.issues.push(`observation segment ${segment + 1} line ${index + 1}: ${error instanceof Error ? error.message : String(error)}; suffix not read`);
        break;
      }
    }
    if (lines.at(-1)) result.issues.push(`observation segment ${segment + 1} has an incomplete trailing record`);
  }
  result.nodes = [...nodes.values()];
  for (const node of result.nodes) {
    if ((node.kind === "model_call" || node.kind === "tool_call") && !node.finished_at) {
      node.status = "unknown";
      result.issues.push(`node ${node.id}: completion not observed`);
    }
  }
  return result;
}
