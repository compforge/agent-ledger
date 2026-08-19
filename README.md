# Agent Ledger

Agent Ledger is a framework-neutral specification and a set of polyglot adapters for durable agent
execution records. Agent loops and orchestrators append to the same session history, producing a
causal account of model calls, tool calls, delegation, framework-native state, and outcomes.

The specification is the stable product. Language SDKs are deliberately small; most project code
lives in adapters that understand a framework's hooks, messages, checkpoints, and resume APIs.

## Architecture position

Agent Ledger is not another agent loop or workflow engine. It is a shared evidence layer across
both:

```text
Orchestrator ── decisions, delegation, approvals ──┐
                                                   ├── Agent Ledger session
Agent loops ── steps, attempts, native state ──────┘          │
                                                              ├── recovery
                                                              ├── global timeline
                                                              └── analysis / evaluation
```

An orchestrator owns desired state, scheduling, and run ownership. Each agent framework owns its
native context and resume API. Agent Ledger owns the immutable facts that let those systems explain
and reconstruct what happened.

## Model

- `Session` groups one end-to-end task across processes, languages, agents, and orchestration runs.
- `Run` identifies one semantic execution by an agent or orchestrator and participates in the
  causal DAG.
- `EventStream` is an optimistic-concurrency partition. It may contain one run's execution events or
  framework-native state that survives several runtime runs.
- `Step` is logical work that survives retries; `Attempt` is one physical model or tool invocation.
- Normalized events are the source for timelines and trajectories. Framework-native records are the
  lossless input to framework-owned resume.

Requested events are committed before an external call. A requested event without a terminal event
is unresolved after a crash. It is input to the adapter's reconciliation policy; an adapter must
not silently replay a side-effecting tool.

## Repository

| Area | Responsibility |
| --- | --- |
| `spec/` | Event, append, adapter capability, and recovery contracts |
| `conformance/` | Cross-language golden vectors and adapter contract tests |
| `python/` | Python core SDK plus memory, Redis, and SQLAlchemy stores |
| `typescript/` | TypeScript core SDK and Pi adapter |
| `go/` | Go core SDK, memory/Bolt stores, and AgentGo adapter |

Current framework profiles are integration examples, not definitions of the core session model:

| Adapter | Recording | Recovery |
| --- | --- | --- |
| Pi AgentHarness | Awaited model/tool hooks | Ledger-backed Pi `SessionStorage` |
| AgentGo | `ChatModel` wrapper, turn hooks, message committer, tool middleware | Native message codec with `HoldRuns` + `SetMessages` + `Continue` |
| Plain Python loop | Explicit recorder calls | Snapshot plus tail replay |

Every adapter publishes machine-readable capabilities such as `strict`, `best_effort`, and
`unsupported`; installing a telemetry-only hook never silently claims durable recovery.

Pi's append-only session tree is preserved in a dedicated framework stream because Pi needs it for
lossless reconstruction. Its entry types, active leaf, and branching rules remain Pi-owned rather
than becoming requirements for other agents or orchestrators.

## Store contract

Applications inject an `EventStore`. V1 has no mandatory collector or `/agent-session` service:
framework processes write directly to an in-memory, Redis, or database implementation selected by
the host.

```text
append(stream, expected_version, append_id, events)
read_stream(stream, after_version)
scan_session(session_id, after_cursor)
```

Appends are atomic, idempotent by RFC 8785 canonical event content, and protected by optimistic
concurrency. `commit_cursor` gives one session a display/pagination order; causal links, not cursor
or timestamps, define execution relationships.

Committed event history is append-only. `EventStore` does not update or delete accepted events;
corrections and redactions are represented by later events. Internal version and cursor bookkeeping
may change as new events arrive, while physical retention remains an explicit deployment policy
outside the logical event contract.

## Development

```bash
make fix
make lint
make test
make build
```

See [RFC 0001](spec/rfcs/0001-agent-ledger.md) for the ledger contract and
[RFC 0002](spec/rfcs/0002-polyglot-adapters.md) for framework recording and recovery boundaries.
The [orchestrated agents example](python/examples/orchestrated_agents.py) shows an orchestrator and
multiple agent loops contributing to one causal session.
