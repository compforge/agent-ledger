import type {
  Action,
  Actor,
  AppendReceipt,
  Attempt,
  Lane,
  ProposedEvent,
  SessionView,
  StoredEvent,
  Turn,
} from "./types.js";

export interface EventStore {
  createActor(actor: Actor): Promise<void>;
  getActor(id: string): Promise<Actor | undefined>;
  createLane(lane: Lane): Promise<void>;
  getLane(id: string): Promise<Lane | undefined>;
  findLane(sessionId: string, runId: string, name: string): Promise<Lane | undefined>;
  createTurn(turn: Turn): Promise<void>;
  getTurn(id: string): Promise<Turn | undefined>;
  createAction(action: Action): Promise<void>;
  getAction(id: string): Promise<Action | undefined>;
  createAttempt(attempt: Attempt): Promise<void>;
  getAttempt(id: string): Promise<Attempt | undefined>;
  append(laneId: string, expectedLastSeq: number, appendId: string, events: readonly ProposedEvent[]): Promise<AppendReceipt>;
  loadLane(laneId: string, afterSeq?: number): AsyncIterable<StoredEvent>;
  loadSession(sessionId: string): Promise<SessionView>;
}
