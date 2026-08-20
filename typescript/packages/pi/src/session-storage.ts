import {
  type Actor,
  type EventStore,
  type JsonValue,
  LaneRecorder,
  type StoredEvent,
} from "@agent-ledger/core";
import type { LeafEntry, SessionMetadata, SessionStorage, SessionTreeEntry } from "@earendil-works/pi-agent-core";

export interface LedgerPiSessionStorageOptions<TMetadata extends SessionMetadata> {
  store: EventStore;
  sessionId: string;
  runId: string;
  actor: Actor;
  metadata: TMetadata;
}

const CREATED = "lane.framework.pi.session.created";
const ENTRY_APPENDED = "lane.framework.pi.entry.appended";

/** Pi-compatible native session storage; Pi remains the owner of context reconstruction. */
export class LedgerPiSessionStorage<TMetadata extends SessionMetadata = SessionMetadata>
  implements SessionStorage<TMetadata> {
  readonly #metadata: TMetadata;
  readonly #recorder: LaneRecorder;
  readonly #entries: SessionTreeEntry[] = [];
  readonly #byId = new Map<string, SessionTreeEntry>();
  readonly #labels = new Map<string, string>();
  #leafId: string | null = null;

  private constructor(metadata: TMetadata, recorder: LaneRecorder) {
    this.#metadata = structuredClone(metadata);
    this.#recorder = recorder;
  }

  static async create<TMetadata extends SessionMetadata>(
    options: LedgerPiSessionStorageOptions<TMetadata>,
  ): Promise<LedgerPiSessionStorage<TMetadata>> {
    const recorder = await LaneRecorder.open({
      store: options.store, sessionId: options.sessionId, runId: options.runId,
      actor: options.actor, laneName: laneName(options.metadata.id),
    });
    const storage = new LedgerPiSessionStorage(options.metadata, recorder);
    await recorder.record(CREATED, recorder.lane.id, { payload: { metadata: asJson(options.metadata) } });
    return storage;
  }

  static async open<TMetadata extends SessionMetadata>(options: {
    store: EventStore;
    sessionId: string;
    nativeSessionId: string;
    runId: string;
    actor: Actor;
  }): Promise<LedgerPiSessionStorage<TMetadata>> {
    const lane = await options.store.findLane(options.sessionId, options.runId, laneName(options.nativeSessionId));
    if (lane === undefined) throw new Error(`Pi session ${options.nativeSessionId} does not exist`);
    const events: StoredEvent[] = [];
    for await (const event of options.store.loadLane(lane.id)) events.push(event);
    const created = events.find((event) => event.event_type === CREATED);
    if (!created) throw new Error(`Pi session ${options.nativeSessionId} does not exist`);
    const metadata = created.payload.metadata as unknown as TMetadata;
    const recorder = await LaneRecorder.open({
      store: options.store, sessionId: options.sessionId, runId: options.runId,
      actor: options.actor, laneId: lane.id, laneName: lane.name,
    });
    const storage = new LedgerPiSessionStorage(metadata, recorder);
    for (const event of events) {
      if (event.event_type === ENTRY_APPENDED) storage.#applyEntry(event.payload.entry as unknown as SessionTreeEntry);
    }
    return storage;
  }

  async getMetadata(): Promise<TMetadata> { return structuredClone(this.#metadata); }
  async getLeafId(): Promise<string | null> { return this.#leafId; }

  async setLeafId(leafId: string | null): Promise<void> {
    if (leafId !== null && !this.#byId.has(leafId)) throw new Error(`Pi entry ${leafId} not found`);
    const entry: LeafEntry = {
      type: "leaf", id: await this.createEntryId(), parentId: this.#leafId,
      timestamp: new Date().toISOString(), targetId: leafId,
    };
    await this.appendEntry(entry);
  }

  async createEntryId(): Promise<string> {
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const id = crypto.randomUUID().slice(-8);
      if (!this.#byId.has(id)) return id;
    }
    return crypto.randomUUID();
  }

  async appendEntry(entry: SessionTreeEntry): Promise<void> {
    if (this.#byId.has(entry.id)) throw new Error(`Pi entry ${entry.id} already exists`);
    await this.#recorder.record(ENTRY_APPENDED, this.#recorder.lane.id, { payload: { entry: asJson(entry) } });
    this.#applyEntry(entry);
  }

  async getEntry(id: string): Promise<SessionTreeEntry | undefined> {
    const entry = this.#byId.get(id);
    return entry ? structuredClone(entry) : undefined;
  }

  async findEntries<TType extends SessionTreeEntry["type"]>(
    type: TType,
  ): Promise<Array<Extract<SessionTreeEntry, { type: TType }>>> {
    return this.#entries
      .filter((entry) => entry.type === type)
      .map((entry) => structuredClone(entry) as Extract<SessionTreeEntry, { type: TType }>);
  }

  async getLabel(id: string): Promise<string | undefined> { return this.#labels.get(id); }

  async getPathToRoot(leafId: string | null): Promise<SessionTreeEntry[]> {
    if (leafId === null) return [];
    const path: SessionTreeEntry[] = [];
    let current = this.#byId.get(leafId);
    if (!current) throw new Error(`Pi entry ${leafId} not found`);
    while (current) {
      path.unshift(structuredClone(current));
      if (current.parentId === null) break;
      current = this.#byId.get(current.parentId);
      if (!current) throw new Error("Pi session contains a dangling parent reference");
    }
    return path;
  }

  async getEntries(): Promise<SessionTreeEntry[]> { return structuredClone(this.#entries); }

  #applyEntry(entry: SessionTreeEntry): void {
    const snapshot = structuredClone(entry);
    this.#entries.push(snapshot);
    this.#byId.set(snapshot.id, snapshot);
    this.#leafId = snapshot.type === "leaf" ? (snapshot.targetId as string | null) : snapshot.id;
    if (snapshot.type === "label") {
      const targetId = snapshot.targetId as string;
      const label = typeof snapshot.label === "string" ? snapshot.label.trim() : "";
      if (label) this.#labels.set(targetId, label);
      else this.#labels.delete(targetId);
    }
  }
}

function laneName(nativeSessionId: string): string { return `framework/pi/${nativeSessionId}`; }
function asJson(value: unknown): JsonValue { return JSON.parse(JSON.stringify(value)) as JsonValue; }
