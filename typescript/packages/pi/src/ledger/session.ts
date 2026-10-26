import { SessionManager, type SessionHeader, type SessionEntry } from "@earendil-works/pi-coding-agent";

export const PI_SESSION_FORMAT = "application/vnd.compforge.pi.session+json;version=1";

export interface PiSessionSnapshot {
  header: SessionHeader;
  entries: SessionEntry[];
  leafId: string | null;
}

/** @spec Capture only at a caller-controlled quiescent boundary; no live Agent or queues are saved. */
export function dumpPiSession(manager: SessionManager): PiSessionSnapshot {
  const header = manager.getHeader();
  if (!header) throw new Error("Cannot checkpoint Pi Session without a header");
  return structuredClone({ header, entries: manager.getEntries(), leafId: manager.getLeafId() });
}

/**
 * @spec Restore through Pi's public API. Pi owns branch, compaction and context-edit interpretation.
 * @spec The restored manager is in-memory; caller chooses Checkpoint retention or native file ownership.
 * @link docs/checkpoint.md
 * @link typescript/packages/pi/test/pi.test.ts
 */
export function restorePiSession(snapshot: PiSessionSnapshot): SessionManager {
  const { header, entries, leafId } = structuredClone(snapshot);
  if (header.type !== "session" || !header.id) throw new Error("Invalid Pi Session checkpoint header");
  const ids = new Set<string>();
  for (const entry of entries) {
    if (ids.has(entry.id) || (entry.parentId !== null && !ids.has(entry.parentId))) {
      throw new Error(`Invalid Pi Session ${header.id} checkpoint entry ${entry.id}`);
    }
    ids.add(entry.id);
  }
  if (leafId !== null && !ids.has(leafId)) throw new Error(`Pi checkpoint leaf ${leafId} not found`);
  const manager = SessionManager.inMemory(header.cwd, undefined, [header, ...entries]);
  if (leafId === null) manager.resetLeaf();
  else manager.branch(leafId);
  return manager;
}
