# Agent Ledger

Agent Ledger is a framework-neutral event ledger for durable agent sessions. It records agent
steps before model and tool execution, keeps causal links across distributed agent runs, and gives
framework adapters enough facts to rebuild their own run context after a restart.

The ledger is the source of truth for **what happened**. Recovery remains owned by the framework
integration that understands its checkpoint and `RunContext` types.

## Why another event log?

Traditional logs and traces explain service execution. Agent Ledger adds agent-native invariants:

- `Session` groups one end-to-end task across processes and agents.
- `Run` is the optimistic-concurrency stream written by one agent loop.
- `Step` is a logical unit; `Attempt` is one physical model or tool invocation.
- requested events are committed before external calls, so interrupted calls remain visible.
- `parent_run_id` and `caused_by_event_id` form a causal DAG without relying on timestamps.
- trajectories such as ATIF are projections, not the durable source of truth.

## Quick start

```python
from agent_ledger import Actor, SessionRecorder
from agent_ledger.stores.memory import MemoryEventStore

store = MemoryEventStore()
recorder = SessionRecorder(
    store=store,
    session_id="session-1",
    run_id="run-1",
    actor=Actor(type="agent", id="researcher"),
)

await recorder.start_run(payload={"task": "summarize"})
attempt = await recorder.before_model_call(
    step_id="step-1",
    payload={"model": "example-model", "messages": [{"role": "user", "content": "Hi"}]},
)

# The real model call starts only after model.requested is durably appended.
response = await model.generate()
await recorder.model_completed(attempt, payload={"message": response})
```

If the process stops after `before_model_call`, inspection reports an unresolved attempt. An adapter
can then ask the provider for a result, require human confirmation, or retry with a new
`attempt_id`; the generic library never silently repeats an external side effect.

## Stores

`EventStore` has three implementations:

- `MemoryEventStore`: process-local reference implementation and test double.
- `RedisEventStore`: atomic append through Lua, with per-session cluster key co-location.
- `SqlEventStore`: one SQLAlchemy 2.x implementation for SQLite, MySQL, and PostgreSQL.

Redis and SQL clients are supplied by the application so pool size, connection timeout, and
deployment-specific durability are explicit. Install optional dependencies with
`agent-ledger[redis]`, `agent-ledger[sql]`, `agent-ledger[mysql]`, or
`agent-ledger[postgres]`.

## Design boundaries

- No collector or mandatory network service in v1.
- No generic cross-framework `RunContext` serializer.
- No automatic replay of an unresolved tool side effect.
- No exactly-once claim. Appends are atomic and idempotent; external calls are not transactional
  with the ledger.
- No global ordering claim. `commit_cursor` orders one session's stored events for display and
  pagination; causal links define execution relationships.

See [RFC 0001](spec/rfcs/0001-agent-ledger.md) for the contract and
[the plain-loop example](examples/plain_loop.py) for adapter-owned recovery.

## Development

```bash
uv sync --all-extras
make fix
make lint
make test
```

