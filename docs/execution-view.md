# Shared execution facts (common) and View

Ledger and View describe the same agent execution from different perspectives. The `common`
module owns call facts: model identity, native request/response content, usage and errors. Ledger
persists these facts in Events, with Action/Attempt recovery semantics. View organizes observed
facts into Runs, Turns and calls for inspection and rendering.

View has no Store dependency and accepts native Session or Trace evidence directly. Source IDs
need not be UUIDs. Each ModelCall or ToolCall represents one physical call. Action and Attempt
references may be retained as evidence but are not mandatory View hierarchy levels.

Run identity is supplied by the source; it may contain several Lanes. Lane-local order and
membership survive projection. Cross-Lane display order does not imply causality. Missing timing
is absent, missing outcomes remain unknown, and recorded `outcome_unknown` stays distinct from
missing evidence. A projection never chooses a retry or recovery action.

## Packages and ownership

`common` is the common call vocabulary. `Model`, `Usage`, `CallError`, `CallRequest` and
`CallResponse` contain observed values, not Ledger IDs, OCC cursors or recovery policy. The existing
Ledger call-payload schema references these shared definitions. AgentGo/Pi adapters and Recorder
use them as well as View.

Ledger's `RunView` remains an Events-and-entities query result. View's `Run` is an execution tree.
There is no new authoritative Session or Run row. The Ledger-to-View projection lives on the
Ledger side and imports View; View imports only shared execution facts. Native evidence can enter
View without a dependency on Ledger Core.

```text
Shared execution facts (common)
       ↑          ↑
 Ledger Core     View model → View rendering
       └───────────↑
            projection
```

## View contract

`Document` contains a schema version, Runs and evidence issues. Every node has an ID, optional
name/Lane/timestamps/status, source references and structured attributes. IDs must be unique within
a Document. Larger combined views must namespace native IDs when necessary and retain the original
identity in evidence references.

- Run: source-defined execution scope, optional Session identity, Lane descriptions and ordered items.
- Turn: a recorded interaction boundary containing ordered items. Sources without Turn evidence may
  attach calls directly to Run. Do not infer turns from elapsed time.
- Input: native input content and its delivery mode, including initial input, follow-up and steer.
- ModelCall / ToolCall: one physical invocation, optional shared request/response facts. ToolCall may
  reference its requesting ModelCall and contain observed child Runs. A proposed tool in model
  output is not a ToolCall until actual execution is observed.
- Operation: other observed work, including preparation, compact, recovery and source extensions.
  It can contain ordered items. Unknown source events remain visible operations.

Items preserve source ordering. A list does not assert serial execution. Different Lanes may
interleave in wall time; async child Runs may outlive their parent call. No parent containment
window or missing duration is fabricated. A missing result has status `unknown`; a durable
`outcome_unknown` fact retains that status. Conflicting terminal observations remain inspectable,
produce an issue, and do not select a response body. Completion is a recorded execution fact,
not a quality verdict.

The request's native `input` preserves actual messages, roles, tools and multimodal structure.
It is not reconstructed from a conversation transcript. Requested and actual model identity remain
on request and response respectively. Usage fields are optional: missing is not zero. Content may
instead reference an Artifact ID. Projection does not fetch content or execute source URLs.

JSON Schema validates the wire structure; SDK validation checks IDs, timing and model/tool links.
Schema resources resolve locally in conformance tests. Ledger projects one loaded Run at a time;
selection, paging and storage retention remain with the caller.

## Usage

Python, reading an already loaded Ledger Run:

```python
from agent_ledger import project_view

recording = await store.load_run(session_id, run_id)
document = project_view(recording)
```

TypeScript, reading and rendering a Ledger Run:

```ts
import { projectView } from "@agent-ledger/core";
import { renderHTML } from "@agent-ledger/view";

const document = projectView(await store.loadRun(sessionId, runId));
const html = renderHTML(document, "Agent execution");
```

Go:

```go
document, err := agentledger.ProjectView(recording)
if err != nil { return err }
return json.NewEncoder(writer).Encode(document)
```

Rendering is implemented once in TypeScript. Python/Go producers may serialize the View document
for this renderer; they do not shell out to it or fetch external assets. HTML is script-free,
escapes recorded content, and uses native details controls. Evidence paths appear as text.

## Verification

`conformance/vectors/view.json` covers native IDs, ordered message roles, zero usage, explicit null,
missing results and nested execution. `view-ledger.json` covers two overlapping Lanes, retries,
failed-call usage, unknown outcomes and open event vocabulary. All language SDKs read these same
fixtures. Additional tests check conflicting terminal observations and renderer escaping.

The existing Action/Attempt, Event, Checkpoint and Store contracts are unchanged. View is a derived
read model and is never a recovery checkpoint. CCR/Pi/Trace ingestion adapters and richer rendering
can consume this public contract without changing Ledger write semantics.
