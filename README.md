# Agent Ledger

Agent Ledger is a framework-neutral execution ledger and checkpoint store for agent harnesses and
orchestrators. It records immutable execution facts and versioned harness-native recovery baselines
so a host can audit execution, derive trajectories, and recover safely.

The specification is the stable product. Language SDKs implement the same object and append
contracts; framework adapters bind those contracts to concrete harness hooks and recovery APIs.

## Architecture position

Agent Ledger is not an agent loop, workflow engine, scheduler, or universal checkpoint format.

```text
Orchestrator ── Session / Run identity, delegation ──┐
                                                    ├── Agent Ledger
Harness ── Lane / Turn / Action / Attempt facts ────┘       │
                                                            ├── audit
Harness-native state ── Checkpoint Store ───────────────────┼── recovery
                                                            └── trajectory / eval
```

The orchestrator owns control state. Each harness owns the meaning of its native state. Agent
Ledger stores that state opaquely as Checkpoints and owns the append-only execution facts that
connect it to later work.

## Model

```text
Session → Run → Lane → Turn → Action → Attempt
                         ↘ immutable Events

Actor ────────────────────────────────↗

CheckpointKey → Checkpoint revision* ── optional Lane/Event anchor
```

- `Session` is one upstream task; `Run` is one upstream harness execution.
- `Lane` is one serial line inside a Run and the optimistic-concurrency boundary. A Run normally
  has a `main` Lane and may have branch or framework-native-state Lanes.
- `Turn` is a stable interaction boundary.
- `Action` is logical work such as `model_call`, `tool_call`, or `compact`.
- `Attempt` is one physical try of an Action; retrying creates a new `attempt_no`.
- `Event` is an immutable lifecycle, input, output, or audit fact about any hierarchy subject.
- `Actor` stores stable producer identity once; high-volume Events only retain `actor_id`.

Session and Run IDs are supplied by the host. Ledger-owned IDs use UUIDv7. Requested Events are
committed before external calls. A requested Attempt without a terminal Event is unresolved after
a crash and must be reconciled; a side-effecting tool is never silently retried.

## Repository

| Area | Responsibility |
| --- | --- |
| `spec/` | Object model, append semantics, adapter capabilities, recovery boundaries, reference SQL |
| `conformance/` | Cross-language canonical encoding and digest vectors |
| `python/` | Python SDK with Memory, Redis, and SQLAlchemy Stores |
| `typescript/` | TypeScript SDK and Pi adapter |
| `go/` | Go SDK with Memory, Bolt, and GORM Stores plus AgentGo adapter |

GORM and SQLAlchemy Stores accept application-owned database handles. SQLite is used in contract
tests; production applications may inject MySQL or another supported relational driver. The SQL
schema deliberately has no foreign-key constraints: Store implementations validate immutable
ownership relationships in application code.

## Store contract

Applications inject an `EventStore`:

```text
create_actor / create_lane / create_turn / create_action / create_attempt
append(lane_id, expected_last_seq, append_id, events)
load_lane(lane_id, after_seq)
load_session(session_id)
```

An empty Lane has `last_seq = 0`; its first Event has `seq = 1`. Appends are atomic, idempotent by
RFC 8785 canonical Event content, and protected by Lane-local optimistic concurrency. Event and
append IDs are globally unique. `seq` orders one Lane only; cross-Lane display order never implies
causality.

Committed Events are append-only. Corrections and redactions are later Events; physical retention
is an explicit deployment policy outside the logical Store contract.

Applications that need recovery inject a `CheckpointStore`:

```text
save_checkpoint(expected_revision, proposed_checkpoint)
get_checkpoint(checkpoint_id)
load_latest_checkpoint(checkpoint_key)
```

Checkpoint formats are opaque to Ledger, for example
`application/vnd.compforge.agentgo.message+json;version=1`. A Checkpoint may stand alone or anchor
the last applied Event in a Lane; recovery then reads Events after that seq. See
[Checkpoint](docs/checkpoint.md) for the full boundary and save contract.

Go applications inject an already configured GORM handle:

```go
db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
store, err := gormstore.New(db, 5*time.Second)
err = store.Initialize(ctx)
```

The application remains responsible for driver choice, credentials, pool sizing, and connection
lifecycle. `Initialize` creates the Ledger tables without foreign keys.

## Framework adapters

| Adapter | Recording | Recovery |
| --- | --- | --- |
| Pi AgentHarness | Awaited Turn, model, and tool hooks | Ledger-backed Pi `SessionStorage` Lane |
| AgentGo | Model wrapper, Turn hooks, message committer, tool middleware | Native messages with `HoldRuns`, `SetMessages`, and `Continue` |
| Plain Python loop | Explicit `LaneRecorder` calls | Snapshot plus completed-outcome replay |

Every adapter publishes actual guarantees such as `strict`, `best_effort`, or `unsupported`.
Normalized Events support inspection and trajectories; only a harness-native state binding may
claim lossless recovery.

## Development

```bash
make fix
make lint
make test
make build
```

See [RFC 0001](spec/rfcs/0001-agent-ledger.md) for the core contract and
[RFC 0002](spec/rfcs/0002-polyglot-adapters.md) for adapter boundaries.
