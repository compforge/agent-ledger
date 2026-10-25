# Session observation

Agent Ledger has three capability boundaries:

- `common` describes shared facts: Session references, call requests/responses, model identity,
  usage and errors. It owns neither storage nor presentation.
- `ledger` supports recoverability through immutable execution facts, Action/Attempt identity,
  idempotent append and opaque Harness-native Checkpoints. The Harness and host decide how to resume.
- `observation` supports observability. Its current main capability is `SessionView`, a structured
  read model for following and analyzing one Agent Session. Renderers and downstream analysis consume
  this model; observation does not own evaluation policy or Agent execution.

Names are scoped by package. Ledger's `SessionView` is a query result containing durable entities
and Events; observation's `SessionView` organizes input, calls and other observed work. Consumers
importing both use aliases. A shared name is not grounds for inventing another abstraction; only
facts with the same meaning and contract belong in common.

## Session boundary

One SessionView identifies exactly one Session with `session: {source, id}`. `source` is the
source namespace; IDs need not be UUIDs. A Session is a continuing agent interaction, not a process
or business task. Followup, steer and resume can continue that Session. An independent or forked
Session has another identity. Cross-Session business flow, such as a task passing between agents,
belongs to a future Timeline and is outside this model.

The Ledger owns no authoritative Session row. Adapters must supply a Session identity for the agent
interaction they record; a business task ID alone does not establish that multiple loops share one
Session. Pi retains its native Session ID even when Ledger-backed storage uses a different namespace.
Those two IDs must not be silently substituted in observation.

## SessionView contract

The wire document has `schema_version: "agent-ledger/session-view@1"`, a Session reference, ordered
`items`, optional Lane descriptions, attributes and evidence issues. There is no multi-Run document
wrapper or nested Session body. Related Sessions can be referenced from a tool observation.

Items are:

- `Input`: observed native input, optionally carrying its recorded delivery mode (initial, followup,
  steer, or a source-specific value). Absence of a mode is unknown, not proof of an initial input.
- `ModelCall` / `ToolCall`: one observed invocation at the source's execution boundary, with optional
  shared request/response facts. Provider-internal attempts remain source evidence when distinct
  provider-call boundaries cannot be reconstructed. A proposed tool alone is not an executed tool.
- `Turn`: a source-recorded interaction boundary. It is optional; producers need not manufacture it
  to represent a call. A source may preserve native turn events as Operations instead of grouping.
- `Operation`: other work, such as compaction, initialization, recovery or a source-specific event.
  It can group ordered items. Unknown source facts remain visible here.

Node IDs are stable and unique within a SessionView. Native Run/Lane IDs can annotate nodes without
becoming mandatory observation hierarchy levels. Source order does not imply serial execution or
causality. Parent-child grouping does not impose an invented timing window. Foreign Session bodies
are never silently merged into this Session's items.

A model request preserves the exact recorded request, including message order, tools and native
content. It is not rebuilt from earlier transcript messages. Requested and observed model identities
remain distinct. Missing timing, content or usage stays absent; explicit null and zero stay explicit.
Source references and issues preserve missing or conflicting evidence. Orphan results remain visible;
an unanswered request does not become a failed or successful call merely because input ended.

The root has no inferred completion status: one run ending or one answer being delivered does not
close a continuing Session. Call completion is an execution fact, not a quality verdict.

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
npx --package @compforge/agent-observation session-view session-view.json -o session-view.html
```

Rendering is script-free, escapes native content, and never fetches evidence URLs or Artifact bodies.
Analysis consumers can use the same model without rendering or storing it in Ledger.

## Contract verification

`spec/schemas/session-view.schema.json` and the three SDKs agree on the wire format. The shared
`conformance/vectors/session-view.json` covers ordered content, null/zero, missing outcomes and
references to other Sessions. Ledger fixtures cover retries, parallel Lanes and conflicting outcomes.
Library adapter tests cover Pi ancestry, stable followup IDs, AgentGo run-scoped calls, incomplete
records and native user messages. Recovery adapter tests continue to enforce write-before-execute
and unresolved-side-effect behavior independently of observation.
