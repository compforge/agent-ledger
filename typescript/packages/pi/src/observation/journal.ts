import { appendFileSync, closeSync, fstatSync, openSync, readFileSync, readSync } from "node:fs";
import type { Node } from "@compforge/agent-observation";
import type { PiObservationRecord } from "./capture.js";

export interface PiObservationSnapshot {
  sessionId: string;
  nodes: Node[];
  links: Array<{ nodeId: string; entryId: string }>;
  issues: string[];
}

/** One append-only file per native Session; caller owns path, directory, retention and single-writer lease. */
export function observationWriter(path: string): (record: PiObservationRecord) => void {
  let checked = false;
  return record => {
    if (!checked) {
      const fd = openSync(path, "a+", 0o600);
      try {
        const size = fstatSync(fd).size;
        if (size) {
          const tail = Buffer.alloc(1);
          readSync(fd, tail, 0, 1, size - 1);
          if (tail[0] !== 10) throw new Error("Pi observation has an incomplete tail; retain this file and start a new segment");
        }
        checked = true;
      } finally { closeSync(fd); }
    }
    appendFileSync(path, `${JSON.stringify(record)}\n`, { encoding: "utf8", mode: 0o600 });
  };
}

/** Read a complete prefix; a truncated/corrupt suffix is evidence loss, not a failed agent call. */
export function readObservation(path: string | readonly string[], sessionId: string): PiObservationSnapshot {
  const result: PiObservationSnapshot = { sessionId, nodes: [], links: [], issues: [] };
  const nodes = new Map<string, Node>();
  const paths = typeof path === "string" ? [path] : path;
  for (const [segment, file] of paths.entries()) {
    const lines = readFileSync(file, "utf8").split("\n");
    for (let index = 0; index < lines.length - 1; index++) {
      let record: PiObservationRecord;
      try {
        record = JSON.parse(lines[index]!) as PiObservationRecord;
        if (record.version !== 1 || record.sessionId !== sessionId) throw new Error("version or Session identity mismatch");
        if (record.type === "node") {
          if (!record.node || typeof record.node.id !== "string" || !record.node.id ||
            !["input", "model_call", "tool_call", "operation", "turn"].includes(record.node.kind)) throw new Error("invalid node");
          const previous = nodes.get(record.node.id);
          if (previous && previous.kind !== record.node.kind) throw new Error("node kind changed");
          nodes.set(record.node.id, { ...previous, ...record.node });
        } else if (record.type === "link") {
          if (typeof record.nodeId !== "string" || typeof record.entryId !== "string") throw new Error("invalid entry link");
          result.links.push({ nodeId: record.nodeId, entryId: record.entryId });
        } else throw new Error("unknown record type");
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
