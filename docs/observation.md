# Agent observation

Agent Ledger has three capability boundaries:

- `common` describes shared facts: Session references, call requests/responses, model identity,
  usage and errors. It owns neither storage nor presentation.
- `ledger` supports recoverability through immutable execution facts, Action/Attempt identity,
  idempotent append and opaque Harness-native Checkpoints. The Harness and host decide how to resume.
- `observation` supports observability. `SessionView` follows one Agent Session;
  `JobView` follows one task across Sessions. Both use the same observed node types. Renderers and downstream analysis consume
  this model; observation does not own evaluation policy or Agent execution.

Names are scoped by package. Ledger's `SessionView` is a query result containing durable entities
and Events; observation's `SessionView` organizes input, calls and other observed work. Consumers
importing both use aliases. A shared name is not grounds for inventing another abstraction; only
facts with the same meaning and contract belong in common.

## Agent and Job relationship

An Agent works through a loop. Task-oriented and continuous describe how that Agent serves Jobs;
they do not define two kinds of loop or impose a Session lifecycle.

- **Task-oriented:** the Agent serves a particular Job, and its execution typically ends when that
  work is complete.
- **Continuous:** the Agent keeps accepting work for multiple Jobs. Each Job occupies part of its
  activity; completing one Job does not end the Agent's service.

Continuous service can wait for input, pause and resume. It does not require uninterrupted model
calls or a process that never exits. The application owns this service relationship; Observation
needs no task-oriented/continuous mode field.

SessionView follows what happened within one Agent Session: inputs, model calls, tool calls and
other operations. JobView follows progress toward a business goal across the relevant execution
facts. One Session can contribute to multiple Jobs, and one Job can involve multiple Sessions.
A task-oriented execution may produce nearly identical Session and Job observations, while their
identities and completion boundaries remain distinct. Ending a loop invocation, closing a Session
and completing a Job must not be inferred from one another.

## Session boundary

One SessionView identifies exactly one Session with `session: {source, id}`. `source` is the
source namespace; IDs need not be UUIDs. A Session is a continuing agent interaction, not a process
or business task. Followup, steer and resume can continue that Session. An independent or forked
Session has another identity. Cross-Session task flow is organized by JobView.

The Ledger owns no authoritative Session row. Adapters must supply a Session identity for the agent
interaction they record; a business task ID alone does not establish that multiple loops share one
Session. Pi retains its native Session ID even when Ledger-backed storage uses a different namespace.
Those two IDs must not be silently substituted in observation.

## SessionView contract

The wire document has `schema_version: "agent-ledger/session@1"`, a Session reference, ordered
`nodes`, optional Lane descriptions, attributes and evidence issues. There is no multi-Run document
wrapper or nested Session body. Related Sessions can be referenced from a tool observation.

`Node` owns common identity, Session ownership, timing, status and evidence. Concrete node types
extend it with their own fields. Views and grouping nodes use `nodes` collections. JSON uses `kind` to recover concrete types and retain all
specialized fields. Nodes are:

- `Input`: observed native input, optionally carrying its recorded delivery mode (initial, followup,
  steer, or a source-specific value). Absence of a mode is unknown, not proof of an initial input.
- `ModelCall` / `ToolCall`: one observed invocation at the source's execution boundary, with optional
  shared request/response facts. Provider-internal attempts remain source evidence when distinct
  provider-call boundaries cannot be reconstructed. A proposed tool alone is not an executed tool.
- `Turn`: a source-recorded interaction boundary. It is optional; producers need not manufacture it
  to represent a call. A source may preserve native turn events as Operations instead of grouping.
- `Operation`: other work, such as compaction, initialization, recovery or a source-specific event.
  It can group ordered nodes. Unknown source facts remain visible here.

Node IDs are stable and unique within a SessionView. Native Run/Lane IDs can annotate nodes without
becoming mandatory observation hierarchy levels. Source order does not imply serial execution or
causality. Parent-child grouping does not impose an invented timing window. Foreign Session bodies
are never silently merged into this Session's nodes.

A model request preserves the exact recorded request, including message order, tools and native
content. It is not rebuilt from earlier transcript messages. Requested and observed model identities
remain distinct. Missing timing, content or usage stays absent; explicit null and zero stay explicit.
Source references and issues preserve missing or conflicting evidence. Orphan results remain visible;
an unanswered request does not become a failed or successful call merely because input ended.

The root has no inferred completion status: one run ending or one answer being delivered does not
close a continuing Session. Call completion is an execution fact, not a quality verdict.

## JobView contract

`agent-ledger/job@1` identifies one business-owned Job with `job: {source, id}`. It carries optional
`name`, `goal`, `result`, execution `status`, `started_at`, `finished_at`, `attributes` and `issues`;
`nodes` is required. A Job is an observation boundary, not a scheduler entity or a durable Ledger
record. The application decides whether it denotes an overall request or a smaller unit of work.
Job completion is explicitly recorded by the source; a continuing Agent Session may remain active.
`completed` describes execution, not a positive quality verdict. A result may be deliberate silence,
recorded understanding or delegation rather than a user-facing output. Goal and result preserve
explicit JSON null separately from absence.

Both views use Input, ModelCall, ToolCall, Operation and optional native Turn. A Job has no separate
Step class. Job-local Operations can organize discovery, verification, waiting, routing, delivery or
other deterministic work. Native Operations retain their actual source identity. Nesting expresses
work grouping; recorded time intervals can overlap. Neither nesting nor array order proves causality,
and no parent time-containment rule is imposed.

### Identity and partial Session evidence

Every node has optional `session: SessionRef`, meaning the Session that owns this execution fact.
This is distinct from ToolCall's `sessions`, which references other Sessions associated with the call.
Operation gains no callee Session-reference field.

- Inside SessionView, the root Session is implicit. An explicit node Session must match that root.
- Inside JobView, an node with `session` is identified by `(source, session id, node id)`.
- A Job node without `session` is identified in the Job's own namespace.
- Session ownership is explicit per node in a Job; it is never inherited from an enclosing Operation.
- Native IDs remain unchanged. Two Sessions may use the same local node ID. Duplicate facts in one
  namespace are rejected; additional work associations should reference the fact rather than copy it.
- `run_id`, `lane_id` and `model_call_id` are interpreted within that node's Session (or the current
  Job for a Job-local node). A same-ID model in another Session cannot satisfy a tool's association.

Applications select recorded Session nodes using explicit business correlations, add their owning
Session references, and preserve original call bodies and IDs. They do not mutate the source Session.
A Job may contain only a fragment. Missing Session-scoped model-call targets and relation endpoints
are reported by Python `inspect(view)`, Go `view.Inspect()` and TypeScript
`inspect(view)` in their `issues` / `Issues` result. Those gaps do not fabricate failed executions or
trigger fetching. Missing Job-local relation targets and model-call links are invalid; an included non-model target is invalid.
Source `issues` remain unchanged. SessionView continues to require its local model-call targets.

### Selecting Session nodes

Business adapters choose which recorded executions contribute to a Job. Observation supplies
`selectNodes(session, nodeIds)` in TypeScript, `select_nodes` in Python and `SelectNodes` in Go to
copy those facts safely. Selection includes each chosen node's entire subtree, preserves source
order and removes overlapping roots. Every copied node receives its own explicit Session reference.
IDs, bodies and evidence remain unchanged; editing the result does not change the source Session.
Missing requested IDs are errors, and an empty selection produces an empty collection.

Selection does not pull in linked calls automatically. Selecting a ToolCall alone can leave its
ModelCall outside the Job snapshot; inspection reports that gap. The application supplies Job-local
Operations and relations. The library neither infers business membership nor fetches other Sessions.

### Inspecting a snapshot

Inspection walks nodes, checks scoped identities and resolves references once. Invalid structure
raises an error; missing external fragments are returned as observation issues without modifying
source facts. Python and TypeScript expose `inspect(view)`; Go exposes `view.Inspect()`. The result
contains `issues` / `Issues`. Validation is a convenience that discards that result. Python also
checks snapshots during model construction. Shared graph rules and each view's constraints stay
separate from rendering and business outcomes.

### Dependencies and handoffs

Optional `relations` explicitly connect observed nodes:

```json
{
  "kind": "handoff",
  "source": {"id": "discover"},
  "target": {"id": "m1", "session": {"source": "agentgo", "id": "reviewer-2"}},
  "evidence": [{"source": "business-event", "id": "handoff-1"}]
}
```

`depends_on` means source depends on target; `handoff` means source hands work to target. NodeRef
uses `id` and optional `session`, following the same identity rules. Explicit evidence supports the
relation; timestamps alone are insufficient. Conflicting business relationships remain source facts
for evaluation rather than being silently rewritten into a supposedly correct workflow.

### Effect and cost evaluation

SessionView supports inspecting continuity, context use and calls. JobView supports evaluating task
completion, division of work, handoffs and redundant effort. Criteria and scores remain with analysis
consumers; Observation provides facts, not an evaluator.

A single model invocation may handle several inputs or Jobs. Several JobViews may reference that
same call without renaming it, splitting its prompt, or allocating its usage. Full recorded usage is
not an exclusive Job cost. Consumers need an explicit attribution policy before summing across Jobs;
within one Job they can deduplicate by source-scoped execution identity.

### Application integrations

CCR's current business Timeline is a migration source, not a dependency or superclass. A CCR adapter
can construct JobView from Unit/Hypothesis correlations and original Session calls, expressing both
Agent work and deterministic stages as shared nodes. Replacing Timeline requires migrating recording,
persistence, queries and UI in that application; this library does not claim that migration is done.

Aide can use one Input processing task as a Job: Input → routing Operation → Topic assessments →
Task/Routine work → optional Output delivery. Parallel topics and shared model calls retain their
identities. An Output can answer several inputs, and handling may finish without an Output. The
application records scope and completion explicitly; a persistent Routine need not keep every input
Job open forever. Business adapters, collection and persistence remain application-owned.

## Following an active Session

The initial API produces snapshots. Reading a longer source prefix retains existing node IDs,
adds newly observed nodes, and fills results on the same calls. Consumers can replace a snapshot
without duplicating calls. No separate streaming server, subscription protocol or durable observation
store is required. Reading a source does not rewrite it, restore a checkpoint, or execute any action.
Native snapshot consistency and retention remain with the source owner.

## Adaptation belongs to each library

```text
common
  ↑             ↑
ledger       observation
  ↑             ↑
  └── pi / agentgo ──┘
```

Each library package owns both integrations, exposed through independent entrypoints:

| Library | Recoverability | Observability |
| --- | --- | --- |
| Pi (TypeScript) | `@compforge/agent-pi/ledger` | `@compforge/agent-pi/observation` |
| AgentGo (Go) | `go/agentgo/ledger` | `go/agentgo/observation` |

Pi observation consumes native `SessionMetadata` and `SessionTreeEntry` values, or a read-only
`SessionStorage`. Tool-result association follows exact tool IDs on the entry's ancestor path,
so sibling branches cannot accidentally supply arguments. Session messages do not contain exact
model requests or reliable start times: those fields remain absent and issues explain the gap.
A result without its request remains an observed ToolCall. Pi's selected `model` stays source
metadata; only an explicit `responseModel` establishes response model identity.

AgentGo observation uses its native Session reader. Call identity includes Run, Execution and
Attempt; repeated execution IDs in later runs remain distinct. The source reader rejects mixed
Sessions and corrupt complete records, and reports an unfinished final record. The supplied codec
can decode custom messages; absent codecs leave raw evidence and an issue. Records are never
interpreted as permission to replay tools. The Go SDK uses Go 1.26 and the AgentGo native Session API.

Generic Ledger-to-observation conversion is an optional read-side capability inside Ledger, not a
separate projection package. Core imports do not load observation/rendering. It consumes a loaded
Ledger Session query, preserves each Attempt and Lane-local order, and makes no recovery decisions.
A full Session query is the input; caller-created partial queries must not be represented as complete
coverage without reporting their scope externally.

## Usage

Pi, without a Ledger Store:

```ts
import { readSessionView } from "@compforge/agent-pi/observation";
import { renderHTML } from "@compforge/agent-observation";

const view = await readSessionView(storage);
const html = renderHTML(view, "Session progress");
```

Ledger query to observation (TypeScript):

```ts
import { sessionView } from "@compforge/agent-ledger/observation";
const view = sessionView(await store.loadSession(sessionId));
```

Python:

```python
from agent_ledger.ledger.observation import session_view
from agent_ledger.models import SessionView as LedgerSessionView
from agent_ledger.observation import SessionView

recording: LedgerSessionView = await store.load_session(session_id)
view: SessionView = session_view(recording)
```

Go:

```go
import (
    agentgoobservation "github.com/compforge/agent-ledger/go/agentgo/observation"
    ledgerobservation "github.com/compforge/agent-ledger/go/ledger/observation"
)

// nil selects AgentGo's built-in message codec.
view, err := agentgoobservation.ReadSessionView(reader, nil)
// Alternatively, use a Session query already loaded from Ledger.
view, err = ledgerobservation.SessionView(recording)
```

The TypeScript renderer is shared by JSON producers in all languages:

```sh
npx --package @compforge/agent-observation agent-observation session.json -o session.html
npx --package @compforge/agent-observation agent-observation job.json -o job.html
```

Session pages emphasize inputs and execution continuity. Job pages emphasize goals, outcomes,
work groups and visible Session ownership. Both share concrete node details. Model-call associations,
dependencies and handoffs link to nodes using scoped identities; absent fragments are labeled instead
of becoming dead links. Each render uses one inspection result. Grouping and source order do not
imply serial execution or causality.

Rendering is script-free, escapes native content, and never fetches evidence URLs or Artifact bodies.
Analysis consumers can use the same model without rendering or storing it in Ledger.

## Contract verification

`spec/schemas/session.schema.json` and `job.schema.json` reference the same
`observation.schema.json` node contract; all three SDKs agree on the wire format. The shared
`conformance/vectors/session-view.json` covers ordered content, null/zero, missing outcomes and
references to other Sessions. Ledger fixtures cover retries, parallel Lanes and conflicting outcomes.
Library adapter tests cover Pi ancestry, stable followup IDs, AgentGo run-scoped calls, incomplete
records and native user messages. Recovery adapter tests continue to enforce write-before-execute
and unresolved-side-effect behavior independently of observation.

The shared `job-review.json` vector covers nested Agent work, overlapping intervals, Session-local
ID collisions, deterministic work and handoffs. `job-input.json` covers shared calls, partial evidence
and intentional silence. Rendering accepts either root, displays Session provenance and reference
gaps, and never resumes an Agent or automatically fetches external evidence. Node roundtrip and selection
cases cover every concrete type, explicit null, ownership, overlapping roots, independent copies and
links between equal local IDs in different Sessions.
