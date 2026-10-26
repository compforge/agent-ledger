# @compforge/agent-pi

Pi integration for Agent Ledger's two capabilities:

- `@compforge/agent-pi/ledger`: `bindPiHarness`, `dumpPiSession`, `restorePiSession`; durable execution and native Session checkpoints.
- `@compforge/agent-pi/observation`: `sessionView`, `readSessionView`; native Session observations without a Ledger Store.

```ts
import { readSessionView } from "@compforge/agent-pi/observation";
const view = await readSessionView(sessionManager);
```

The observation entrypoint does not import Ledger Core. Native Session entries remain authoritative;
missing model requests stay absent and tool results match only proposals on their ancestor path.

## Pi runtime and recovery

The adapter targets Pi **1.4.x** and is tested against `pi-agent-core`, `pi-ai` and
`pi-coding-agent` **1.4.0**. `Agent` owns the loop; coding-agent's `SessionManager` owns native
entries, context reconstruction, branches, compaction and context edits.

```ts
import { bindPiHarness, dumpPiSession, restorePiSession, PI_SESSION_FORMAT } from "@compforge/agent-pi/ledger";

const dispose = bindPiHarness(agent, recorder); // one LaneRecorder Run per binding
try {
  await agent.prompt("inspect the change");
} finally {
  dispose();
}

// At a quiescent boundary, the caller may save this native snapshot in a CheckpointStore.
const snapshot = dumpPiSession(sessionManager);
const format = PI_SESSION_FORMAT;
const restoredManager = restorePiSession(snapshot);
```

Ledger binds directly to `Agent.streamFunction` and awaited `Agent.subscribe` events. It records
model requests before invoking the stream and tool requests before validation/hooks/execution.
The model request is the normalized transcript (including system messages and tool declarations);
the tool request is the proposal at `tool_execution_start`. Provider-internal retries and modified
provider payloads are not independent Ledger Attempts. Outcome writes must finish before the loop
advances. A failed write poisons the binding so synthetic failure events cannot mask an unrecorded
outcome. Bind while idle, before execution, and dispose while idle; a subsequent prompt requires a
new Run/recorder. This binding covers the Agent loop, not coding-agent background compaction calls.

Native files remain the normal recovery source. Use `SessionManager.open(file)` to reopen them.
`dumpPiSession` is opt-in for applications that choose Checkpoint storage, typically with an
in-memory manager. It captures native entries and the active leaf, but not a running Agent,
queued steer/followup messages or external tool effects. `restorePiSession` returns an in-memory
manager through Pi's public API. The caller owns Checkpoint format checks, revision/retention,
Ledger anchors and reconciliation of unfinished effects; restoration never replays tools.
Observation capture does not create these snapshots or a duplicate native transcript.

## Supplement native Session evidence

Pi owns its Session record. Observation reads native inputs, assistant responses, tool proposals
and results, branches and compaction entries. It stores only facts missing from that record:
provider payloads, runtime start/end metadata, small error diagnostics, explicit entry links and
caller-confirmed input delivery modes. It does not persist stream context, response bodies, usage,
unchanged tool arguments, tool result bodies, input bodies, SessionView or JobView snapshots.
If tool validation or hooks changed execution arguments from the native proposal, only that missing
argument evidence is supplemented.

```ts
import { createAgentSession } from "@earendil-works/pi-coding-agent";
import {
  PiObservation, observationWriter, readObservation, readSessionView,
} from "@compforge/agent-pi/observation";

const observation = new PiObservation({
  sessionId: sessionManager.getSessionId(),
  write: observationWriter(observationPath, { maxBytes: 16 * 1024 * 1024 }),
  onError: error => logger.error({ error }, "Pi supplemental evidence capture failed"),
});
const { session } = await createAgentSession({
  ...options, sessionManager: observation.storage(sessionManager),
});
// Install before prompting; the native AgentSession still owns all message appends.
session.agent.streamFunction = observation.stream(session.agent.streamFunction);
session.agent.beforeToolCall = observation.beforeToolCall(session.agent.beforeToolCall);
const unsubscribe = session.agent.subscribe(event => observation.event(event));
try {
  await session.prompt("inspect the change");
} finally {
  unsubscribe();
  session.dispose();
}

const captured = readObservation(observationPath, sessionManager.getSessionId());
const view = await readSessionView(sessionManager, captured);
```

Use one observer for one serialized native Session, retaining its native ID after restart and using
a new ID for a fork. The observer holds active tool associations and weak message references; it
does not retain or replay a Session's historical bodies.

### Request boundary

`stream` wraps the provider stream **inside** any integration that replaces `onPayload`. A Harness
that constructs its own callback needs the wrapper at its model/provider stream boundary, not
around its outer prompt operation. Existing callbacks run first; the observer records their return
value, including explicit `null`, or the mutated original when they return `undefined`.

The recorded boundary is `provider.onPayload.return`: the payload handed back to the provider,
not network bytes or proof of HTTP delivery. Providers must honor that callback. Providers that
omit it leave requests absent with an issue. No request is reconstructed from Session history.
Multiple payload callbacks in one stream remain `pi.provider_payload` Operations linked through
`data.model_call_id`; they do not manufacture independent calls or per-request outcomes. Requests
can contain repeated historical messages, so they still need compression and a storage budget.

### Native identities and bodies

Calls receive stable observation IDs before execution. The storage decorator links model responses
by object identity after native append succeeds and links tool results to observed execution events.
It preserves synchronous `SessionManager.appendMessage`, native methods and entry IDs.
Use the decorated manager when creating the AgentSession; public AgentSession message events occur
before native append and are not a persistence receipt. Keep one observer bound to one native
Session ID; create a new observer/manager binding when switching Sessions. Integrations that copy messages can explicitly
call `modelCallId(originalMessage)` and `link(nodeId, nativeEntryId)`. No timestamp, tool-name or
text similarity matching is performed. Native IDs remain in evidence and `attributes.native_entry_id`.

Response bodies, usage and tool arguments/results are loaded from those native entries at read time.
Pi emits tool start before validation. `beforeToolCall` wraps the existing hook and captures
validated/hook-edited arguments immediately before execution; those override the native proposal.
Install the observer after the caller has configured its tool hook.
Small runtime errors can remain in supplemental metadata when no native result was persisted. If a
native entry is absent, the view retains observed timing/status and exposes the missing body; it does
not replace the native Session with a second complete execution log. Native outcomes take precedence
when present. A completed runtime call does not imply that its result was successfully persisted.

To annotate delivery mode, call `observation.input(persistedUserEntry, "followup")` (or `initial` /
`steer`) after application and persistence. Only the entry ID and mode are saved. Queue admission
is not delivery, and missing native input bodies remain explicit gaps.

### Compressed files and capacity

`observationWriter` writes JSONL frames containing base64-encoded, independently gzip-compressed
JSON records. Record-level compression allows a complete prefix to be read even if the final frame
was interrupted. Encoding is private to the Pi adapter; callers use `readObservation`, not a plain
JSONL-to-SessionView conversion.

The default limit is **16 MiB per file**, configurable through `maxBytes` (minimum 1024). The budget
counts actual encoded disk bytes, including a reserved capacity marker. When full, the writer saves
the gap marker, reports the limit through `onError`, and stops appending. Reopening that capped file
does not resume capture. It neither silently drops old evidence nor creates unbounded new files.

The caller owns directory creation, single-writer exclusivity, file lifecycle and total retention
across Sessions/segments. New files use mode `0600`; existing file permissions remain caller-owned.
Writes are synchronous but not fsync checkpoints. Observation write failures never trigger agent
failure or replay. Capture can be disabled entirely when native evidence suffices.

### Reading and sharing

```ts
const captured = readObservation(orderedSegmentPaths, nativeSessionId);
const view = await readSessionView(sessionManager, captured);
```

The reader retains complete prefixes of truncated/corrupt segments and reports gaps. A writer refuses
to append to an incomplete tail: retain it and let the caller choose whether its retention budget
permits another segment. Missing files are explicit read errors. Unfinished calls remain unknown;
reading after restart does not infer failure, cancellation or success.

Linked nodes follow native entry order. Unlinked supplemental nodes follow in capture order, with
an explicit chronology issue; array position does not establish cross-source order. SessionView is
an on-demand read projection, and a self-contained snapshot is produced only when a consumer exports
it for sharing. Preserve the referenced native records for as long as their bodies are needed.
