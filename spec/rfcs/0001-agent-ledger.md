# RFC 0001: Agent Ledger v1

## Status

Draft specification for the `0.x` library line.

## Problem

Agent loops can be interrupted by process replacement, autoscaling, model errors, and rate limits.
Framework checkpoints solve recovery inside one runtime, but they do not necessarily provide a
framework-neutral timeline across distributed agents. Agent Ledger defines the durable facts that
framework integrations can share while leaving framework state restoration to those integrations.

## Concepts

| Concept | Meaning |
| --- | --- |
| Session | End-to-end task and the boundary of the global timeline. |
| Run Stream | One agent loop's ordered append stream inside a session. |
| Step | Logical work that survives retries. |
| Attempt | One physical model or tool call within a step. |
| Event | Immutable fact proposed by a producer and enriched by a store. |
| Framework Profile | Mapping between one framework's hooks/checkpoints and ledger events. |
| Trajectory | Read-side projection for evaluation or analysis. |

A child agent starts another run in the same session. Its `run.started` event carries
`parent_run_id` and `caused_by_event_id`. The causal DAG, rather than wall-clock order, is the
authoritative relationship between runs.

## Event envelope

`spec/schemas/event.schema.json` is normative. Producers set the identity and causal fields. A
store adds `stream_version`, `commit_cursor`, and `committed_at` when the event is accepted.

`occurred_at` is the producer timestamp. `committed_at` is the store timestamp. Neither field
establishes causality.

Large inputs and outputs should use an `ArtifactRef`. The event keeps the content digest, media
type, byte size, and URI while an application-selected `ArtifactStore` owns the bytes.

## Append contract

The empty stream version is `-1`; the first stored event has version `0`.

```python
await store.append(stream, expected_version, append_id, events)
```

The operation is an atomic batch with these outcomes:

1. If `append_id` was committed with identical canonical event content, return its original
   receipt.
2. If the same `append_id` names different content, fail with `IdempotencyViolation`.
3. If `expected_version` differs from the run stream's current version, fail with
   `StreamConflict`.
4. Otherwise append all events contiguously or append none.

`expected_version` is scoped to `(session_id, run_id)`. A store must reject an event identifier
already present in the same session; producers must generate globally unique event identifiers.

`commit_cursor` is an opaque, session-scoped pagination token. Consumers must not compare cursors
from different sessions or infer causality from cursor order.

## Write-before-execute

Framework hooks must await `model.requested` or `tool.requested` before invoking the external
operation. If that append fails, the call must not start. The completed or failed outcome must be
appended before the loop advances.

After a crash, a requested event without a terminal event represents an unresolved attempt. The
framework profile decides whether to query an external provider, ask for confirmation, mark the
attempt failed, or retry with a new `attempt_id`. Repeating a tool automatically is unsafe because
the first call may have produced a side effect.

Retries keep the same `step_id` and use a new `attempt_id`.

## Framework recovery

The core library can find stream gaps, unresolved attempts, and run-parent edges. It does not
construct a generic run context. A Framework Profile defines:

- hook-to-event mappings;
- snapshot/checkpoint encoding;
- reconstruction of the framework's native context;
- policy for unresolved attempts.

The bundled plain-loop profile demonstrates this boundary. Frameworks with native checkpointing
can store a checkpoint link or snapshot event and restore with their own APIs.

## Store durability

An append receipt means the selected store accepted the transaction. End-to-end durability still
depends on deployment configuration: Redis persistence/replication/failover or database commit and
backup guarantees. Client pools and operation timeouts are application configuration, not hidden
library defaults.

Run ownership is external. Optimistic concurrency prevents two writers from both advancing a
stream, but leases, fencing, and scheduling belong to the orchestrator.

## Compatibility

Readers must reject unsupported major schema versions and should preserve unknown event types,
payload fields, and `extensions`. Additive fields are permitted within a major version.

