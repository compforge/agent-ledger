# Agent Ledger

Agent Ledger is a framework-neutral specification and a set of polyglot adapters for durable agent
sessions. It records model and tool attempts before execution, preserves causal timelines across
distributed agents, and lets each framework rebuild its own native session after interruption.

The specification is the stable product. Language SDKs are deliberately small; most project code
lives in adapters that understand a framework's hooks, messages, checkpoints, and resume APIs.

## Model

- `Session` groups one end-to-end task across processes, languages, and agents.
- `Run` identifies one semantic agent execution and participates in the causal DAG.
- `EventStream` is an optimistic-concurrency partition. It may contain one run's execution events or
  framework-native state that survives several runtime runs.
- `Step` is logical work that survives retries; `Attempt` is one physical model or tool invocation.
- Normalized events are the source for timelines and trajectories. Framework-native records are the
  source for resume.

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
| `go/` | Go core SDK and AgentGo adapter |

Current framework profiles:

| Adapter | Recording | Recovery |
| --- | --- | --- |
| Pi AgentHarness | Awaited model/tool hooks | Ledger-backed Pi `SessionStorage` |
| AgentGo | `ChatModel` wrapper, turn hooks, message committer, tool middleware | Native message codec with `HoldRuns` + `SetMessages` + `Continue` |
| Plain Python loop | Explicit recorder calls | Snapshot plus tail replay |

Every adapter publishes machine-readable capabilities such as `strict`, `best_effort`, and
`unsupported`; installing a telemetry-only hook never silently claims durable recovery.

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

## Development

```bash
make fix
make lint
make test
make build
```

See [RFC 0001](spec/rfcs/0001-agent-ledger.md) for the ledger contract and
[RFC 0002](spec/rfcs/0002-polyglot-adapters.md) for framework recording and recovery boundaries.
