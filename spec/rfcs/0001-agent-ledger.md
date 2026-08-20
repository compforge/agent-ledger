# RFC 0001: Agent Ledger v1

## Status

Draft specification for the `0.x` library line.

## Problem

Agent harnesses can be interrupted by process replacement, model failures, rate limits, and
side-effecting tools. Framework-native checkpoints preserve harness context, but do not provide a
framework-neutral, append-only account of what happened. Agent Ledger defines that execution
account and an opaque Checkpoint Store while leaving control state and the meaning of harness state
to their respective owners.

## Execution model

```text
Session → Run → Lane → Turn → Action → Attempt
                         ↘ immutable Events

Actor ────────────────────────────────↗
```

| Concept | Meaning |
| --- | --- |
| Session | One end-to-end task owned and identified by an upstream orchestrator. |
| Run | One logical harness execution inside a Session, also identified upstream. Process recovery continues the same Run. |
| Lane | One serial execution line inside a Run and the boundary for ordering and optimistic concurrency. |
| Turn | One stable interaction or checkpoint boundary inside a Lane. |
| Action | One logical harness action inside a Turn. Its concrete `type` is extensible, for example `model_call`, `tool_call`, or `compact`. |
| Attempt | One physical try of an Action. Retries keep the Action and increment `attempt_no`. |
| Actor | One stable producer or initiator identity referenced by Events. |
| Event | An immutable fact about a Session, Run, Lane, Turn, Action, or Attempt. |
| Checkpoint | One immutable revision of opaque Harness-native state, optionally anchored to a Lane Event. |

A Run has one main Lane and may have additional Lanes for branches or framework-native records.
Turns are serial within a Lane; different Lanes may progress independently. `main` is a Lane name,
not a reserved identifier.

Session and Run identities come from the upstream system. Agent Ledger does not create authoritative
Session or Run rows. Ledger-owned Actor, Lane, Turn, Action, Attempt, Event, and append receipt
identifiers are UUIDv7 values.

## Layer boundary

- an agent harness records turns, actions, attempts, outcomes, and links to framework-native state;
- an orchestrator supplies Session and Run identities while retaining desired state, scheduling,
  leases, and reconciliation;
- stores persist immutable execution identities, Events, and Checkpoints, enforce OCC, and validate
  ownership links;
- framework adapters dump and restore native harness state with the framework's own APIs;
- readers derive timelines, recovery input, trajectories, alerts, and evaluation data.

The Ledger does not own an agent loop, construct a universal harness context, decide whether a
result is correct, or activate learned prompts, tools, or skills.

## Entity contract

Lane ownership is immutable: one Lane belongs to exactly one `(session_id, run_id)`. A Turn belongs
to one Lane, an Action belongs to one Turn, and an Attempt belongs to one Action. `parent_lane_id`
and `parent_action_id` express optional structural relationships without changing ownership.

Entity tables describe identity and containment, not lifecycle status. Actor rows hold stable
`type` and optional `framework` attributes so the high-volume Event table only repeats `actor_id`.
Actor attributes are immutable; a semantic change creates a new Actor. Started, completed, failed,
cancelled, checkpointed, and reconciled are immutable Events. `Lane.last_seq` protects Event append
ordering. Checkpoint revisions are immutable; a backend may maintain a mutable latest pointer as an
index over them.

SQL schemas intentionally omit foreign-key constraints. Stores MUST validate logical ownership on
writes. This keeps migration, archival, partitioning, and cross-database operation independent from
database-specific foreign-key behavior.

## Event envelope

`spec/schemas/event.schema.json` is normative. Producers set the Event identity, Lane, subject,
Actor reference, timestamp, payload, extensions, and optional causation link. A Store assigns `seq` and
`committed_at` when accepting the Event.

`subject_id` identifies the Session, Run, Lane, Turn, Action, or Attempt described by the Event.
The first segment of `event_type` identifies that subject kind. Core and extension Event types
therefore follow `<subject-kind>.<name>`, for example `attempt.requested`, `turn.completed`, or
`lane.framework.pi.entry.appended`. A Store derives the subject kind from this prefix and verifies
that `subject_id` resolves to the target Lane.

Because Session and Run identifiers are upstream strings, `subject_id` is represented as a string
for every subject kind. Valid ownership means:

- a Session subject equals the Lane's `session_id`;
- a Run subject equals the Lane's `run_id`;
- a Lane subject equals the target `lane_id`;
- a Turn, Action, or Attempt resolves through its immutable parents to the target Lane.

`actor_id` references the Actor that performed or initiated the recorded action. It does not
identify the adapter that wrote the Event. The referenced Actor carries an extensible `type` and
optional `framework`; recommended types include `user`, `agent`, `orchestrator`, `harness`, `model`,
`tool`, and `system`. Adapter-specific recording details belong in `extensions`.

`causation_id` optionally references the Event that caused the current fact. It is a logical Event
reference, not a database foreign key. Timestamps and cross-Lane observation order never establish
causality.

`payload` contains fact-specific data. `extensions` contains namespaced framework, vendor, or
application additions outside the core contract. Readers MUST preserve unknown Event types,
payload fields, and extensions.

Large inputs and outputs SHOULD use an `ArtifactRef`. An application-selected Artifact Store owns
the bytes; the Event keeps their digest, media type, size, and URI.

## Append contract

An empty Lane has `last_seq = 0`; its first accepted Event has `seq = 1`.

```python
await store.append(lane_id, expected_last_seq, append_id, events)
```

The operation is one atomic batch:

1. If `append_id` was committed on the Lane with identical canonical Event content, return the
   original receipt.
2. If the same `append_id` names different content, fail with `IdempotencyViolation`.
3. If `expected_last_seq` differs from the Lane's current `last_seq`, fail with `LaneConflict`.
4. Otherwise assign contiguous `seq` values, append all Events, update `last_seq`, and persist the
   receipt in one transaction, or change nothing.

The append digest is SHA-256 over RFC 8785 canonical JSON for the ordered proposed-Event array.
Optional fields that are absent are omitted; explicit `null` remains part of the digest.

Event IDs and append IDs are globally unique. `(lane_id, seq)` and `(action_id, attempt_no)` are
unique. A Store snapshots caller-owned input and MUST NOT expose
mutable references that can rewrite accepted history.

`seq` is authoritative only inside its Lane. UUIDv7 creation time, wall-clock timestamps, and a
read-side merge of multiple Lanes are useful for observation but do not create a global causal
order.

## Write-before-execute

For a model or tool Action, an adapter creates an Attempt and durably appends
`attempt.requested` before invoking the external operation. It appends `attempt.completed` or
`attempt.failed` before the harness advances.

A requested Attempt without a terminal Event is unresolved after a crash. Recovery may query the
provider, apply a known completed result, abandon the old Attempt and create the next `attempt_no`,
or ask for human resolution. A side-effecting tool MUST NOT be silently retried.

## Framework recovery

The Checkpoint Store and Event Ledger are complementary:

```text
restore native checkpoint
+ replay recorded completed outcomes after that checkpoint
+ reconcile unresolved Attempts
+ continue the unfinished Turn
```

The framework adapter owns checkpoint encoding, `format` compatibility, restoration, replay into
native context, and unresolved-Attempt policy. The Store treats state as opaque JSON or an
`ArtifactRef`. Normalized Events alone are not claimed to rebuild contexts containing branches,
compaction state, queues, custom messages, or opaque checkpoints. RFC 0002 defines this adapter
boundary; `docs/checkpoint.md` defines the save and anchor contract.

## Read models

The append log is the source of execution facts. Timelines, unresolved-Attempt inspection,
trajectories, and recovery plans are projections over Events plus immutable containment rows. They
may be rebuilt without mutating Ledger history.

A cross-Lane Session timeline is an observation projection. Consumers use `causation_id` and
containment relationships for explanation; they do not infer causality from display order. A
projection MUST preserve `seq` order within each Lane even when several appends share the same
wall-clock timestamp.

## Store durability

An append receipt means the selected Store accepted the transaction. End-to-end durability still
depends on database, Redis, or embedded-store configuration and backup policy. Client pools and
operation timeouts are explicit application configuration.

Run ownership is external. Lane OCC prevents two writers from both advancing one Lane, but leases,
fencing, scheduling, and multi-agent orchestration remain outside Agent Ledger.

## Compatibility

Readers reject unsupported major schema versions and preserve unknown extensible values. Additive
fields are permitted within a major version. The SQL layout is a reference persistence shape; the
Event JSON Schema and behavioral append contract define cross-language compatibility.
