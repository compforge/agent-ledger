# @compforge/agent-pi

Pi integration for Agent Ledger's two capabilities:

- `@compforge/agent-pi/ledger`: `bindPiHarness`, `LedgerPiSessionStorage`; durable execution and native recovery storage.
- `@compforge/agent-pi/observation`: `sessionView`, `readSessionView`; native Session observations without a Ledger Store.

```ts
import { readSessionView } from "@compforge/agent-pi/observation";
const view = await readSessionView(storage);
```

The observation entrypoint does not import Ledger Core. Native Session entries remain authoritative;
missing model requests stay absent and tool results match only proposals on their ancestor path.

## Capture model and tool execution

`PiObservation` records supplementary execution evidence without a Ledger Store. Native Session
storage still owns history and recovery. Use one observer for one serialized native Session; reuse
its native ID across process restarts and create another observer for a fork.

```ts
import { Session, runAgentLoop } from "@earendil-works/pi-agent-core";
import {
  PiObservation, observationWriter, readObservation, readSessionView,
} from "@compforge/agent-pi/observation";

const observation = new PiObservation({
  sessionId: (await nativeStorage.getMetadata()).id,
  write: observationWriter(observationPath),
  onError: error => logger.error({ error }, "Pi observation capture failed"),
});
const storage = observation.storage(nativeStorage);
const session = new Session(storage);
const stream = observation.stream(providerStream);

// A native AgentHarness owns this append already: give it the decorated storage instead
// and subscribe with observation.event(event). Do not append a message twice.
await runAgentLoop(prompts, context, config, async event => {
  observation.event(event);
  if (event.type === "message_end") await session.appendMessage(event.message);
}, signal, stream);

const captured = readObservation(observationPath, (await storage.getMetadata()).id);
const view = await readSessionView(storage, captured);
```

`stream` must wrap the provider stream **inside** any integration that replaces `onPayload`.
For example, a Harness that constructs its own callback needs the wrapper at its model/provider
stream boundary, not merely around the Harness's outer prompt operation. Existing callbacks run
first; the observer records their returned payload, including an explicit `null`, or the mutated
original when they return `undefined`. The callback's result and native stream events are unchanged.

The recorded request boundary is `provider.onPayload.return`. This is the payload handed back to
the provider implementation, not a network packet or proof of HTTP delivery. A provider must honor
that callback. The separately recorded `attributes.context` is the input to the wrapped Pi stream;
it is not a substitute for a missing provider payload. Stream options, auth headers and API keys
are not recorded. Providers that do not call `onPayload` leave requests absent with an issue.
Multiple payload callbacks in one stream remain `pi.provider_payload` Operations linked by
`data.model_call_id`; they do not fabricate independent calls, attempts or per-request outcomes.

A call gets a stable observation node ID before execution. `storage` associates the final model
message by object identity after a successful native append and links tool results to observed
execution events. It preserves all storage methods and native IDs. An integration that copies
messages can use `modelCallId(originalMessage)` and `link(nodeId, nativeEntryId)` explicitly.
No timestamp, tool-name or text matching is performed. A copied message without an explicit link
remains separate evidence, and the view reports the gap. Linked calls retain their observation ID
and expose the original entry ID through evidence and `attributes.native_entry_id`.

Native user entries, compaction and branch operations come from the Session. To record a known
input delivery mode, call `observation.input(persistedUserEntry, "followup")` (or `initial` / `steer`)
only after that entry has actually been applied and persisted. Admission to a queue is not delivery;
the adapter does not infer modes from message contents or queue timing.

## Files, interruptions and evidence gaps

`observationWriter` appends synchronous JSONL records with mode `0600` for a new file. The application
owns the directory, permissions on existing files, exclusive writer, retention and size limits.
The observer retains only active tool associations and weak message references, not Session history.
It does not replay the observation file to resume an Agent. Writes are not fsync checkpoints and
power-loss durability is not guaranteed. Capture failures are delivered to the required `onError`
callback and do not fail or replay model/tool operations.

The reader folds patches for stable call IDs and accepts an ordered list of segment paths:

```ts
const captured = readObservation(segmentPaths, nativeSessionId);
const view = await readSessionView(nativeStorage, captured);
```

A partial/corrupt suffix contributes an issue while its complete prefix remains available. A writer
refuses to append after an incomplete final line; retain that segment and start a new one. Missing
segments/files are explicit read errors. Unfinished calls have unknown outcomes; reading after a
process restart never invents failure, cancellation or success. A retained native result can supply
a missing observation outcome through an explicit entry link.

Merged views follow native entry order for linked nodes. Unlinked capture nodes follow in capture
order with an explicit chronology issue; array position does not assert ordering between those two
sources. Native-only messages and captured-only calls are not silently deduplicated. All projections
are read-only, and missing links, payloads and outcomes remain visible for downstream analysis.
