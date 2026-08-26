// Package agentgoadapter binds AgentGo's native Run, Turn, model, and tool
// boundaries to Agent Ledger.
//
// Install Adapter.Options after the Agent's base options. BeforeRun restores
// an AgentSnapshot checkpoint, while model and tool middleware return known
// durable outcomes through AgentGo's own Loop. The Adapter never reconstructs
// conversation messages directly. Unresolved tools fail closed unless the
// caller authorizes one retry with a durable recovery decision ID.
// AgentGo currently normalizes ToolMiddleware errors into tool-error results;
// the Adapter stops unresolved outcomes at AfterTurn, so its declared outcome
// gate remains best-effort rather than strict.
//
// The host owns the durable inbox. After process loss it must re-deliver input
// that had not yet entered the saved AgentSnapshot.
package agentgoadapter
