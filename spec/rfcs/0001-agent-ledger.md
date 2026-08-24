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
| Run | An upstream-defined grouping of one or more Lanes inside a Session. Ledger does not define its domain meaning or lifecycle. |
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

Session and Run identities come from the upstream system. Agent Ledger treats `run_id` as an opaque,
stable containment key and does not create authoritative Session or Run rows. Ledger-owned Actor,
Lane, Turn, Action, Attempt, Event, and append receipt identifiers are UUIDv7 values.

### Fact ledger and structure ledger

The model deliberately keeps two complementary accounts:

- Events are the append-only fact ledger: the authoritative account of what happened and in what
  Lane order. This is the core and highest-volume dataset.
- Session/Run identity and the Lane → Turn → Action → Attempt hierarchy form the structure ledger.
  Lane, Turn, Action, and Attempt rows materialize that containment so readers do not have to infer
  the execution tree by replaying and joining only Events.

The same system could theoretically be represented with Events alone, but ownership validation,
recovery queries, trajectory projection, and navigation would all become contextual replay
problems. The structure ledger is therefore a compact index over durable execution identity, not a
second source of lifecycle truth: mutable status still comes from Events.

Because Events dominate storage volume, immutable identity, containment, and recovery semantics
live once on their structural subject where appropriate. An Event references the relevant
granularity through `subject_id` and records the fact specific to that occurrence instead of
copying the subject's Run, Lane, Turn, Action, or Effect fields into every row. This normalization
does not move concrete execution data out of an Attempt merely to save bytes: each physical
request and outcome remains self-contained in its Attempt Events, while large bodies use Artifact
references.

## Layer boundary

- an agent harness records turns, actions, attempts, outcomes, and links to framework-native state;
- an upstream host supplies Session and Run identities while retaining their business meaning and
  lifecycle;
- stores persist immutable execution identities, Events, and Checkpoints, enforce OCC, and validate
  ownership links;
- framework adapters dump and restore native harness state with the framework's own APIs;
- readers derive timelines, recovery input, trajectories, alerts, and evaluation data.

The Ledger does not own an agent loop, construct a universal harness context, decide whether a
result is correct, or activate learned prompts, tools, or skills.

A managed-agent API may project model spans from model Attempts, tool-use and message output from
their canonical payloads, and recovery diagnostics from unresolved Attempts. Queued user input,
session status such as idle or rescheduled, permission state such as requires-action, and ephemeral
stream deltas remain host or Harness state. A tool proposed by a model but not yet approved is part
of the completed model output and native Harness state; `attempt.requested` begins only when the
physical tool execution is ready to cross its write-before-execute boundary.

## Entity contract

Lane ownership is immutable: one Lane belongs to exactly one `(session_id, run_id)`. A Turn belongs
to one Lane, an Action belongs to one Turn, and an Attempt belongs to one Action. `parent_lane_id`
and `parent_action_id` express optional structural relationships without changing ownership.
`Action.key` is an optional caller-owned identity for logical work; for a Core `tool_call`, adapters
store the Harness-visible `tool_call_id` there rather than repeating it on every Attempt Event.

The Action/Attempt boundary follows two questions:

- Action answers “which logical work is this, and what recovery semantics remain fixed?” It owns
  identity and containment (`type`, `key`, parent), plus the immutable `Effect` used by recovery
  policy.
- Attempt answers “what exact physical execution was requested, and what outcome was observed?” Its
  Events carry the complete model/tool request, provider or external-operation identifiers, result,
  usage, error, cancellation, and unknown-outcome facts.

A field does not move to Action merely because retries normally repeat it. `tool_name`, model/tool
input, requested model, `client_request_id`, and `idempotency_key` describe a concrete outbound
request and therefore belong to `attempt.requested`; each Attempt remains independently auditable.
Cross-Attempt rules are invariants over those requests—for example keyed idempotency requires every
retry to repeat the first Attempt's effective key. In contrast, `tool_call_id` names the logical
tool use itself and therefore belongs in `Action.key`.

Entity tables describe identity and containment, not lifecycle status. Actor rows hold stable
`type`, optional `framework`, and an optional upstream `key` so the high-volume Event table only
repeats `actor_id`. A non-empty Actor key is unique within one Store and lets a producer recover the
same Ledger-owned Actor ID after process replacement. Actor attributes are immutable; reusing a key
with different attributes is a conflict, and a semantic change creates a new Actor. Started, completed, failed,
cancelled, checkpointed, and reconciled are immutable Events. `Lane.last_seq` protects Event append
ordering. Checkpoint revisions are immutable; a backend may maintain a mutable latest pointer as an
index over them.

Public `key` fields are caller-owned lookup identities. `Actor.key`, `Artifact.key`, `Action.key`,
and `Checkpoint.key` are opaque
strings to Ledger: Ledger stores and compares the complete value but does not derive, parse,
normalize, or assign business meaning to it. A caller may use a namespaced composite value such as
`system:tenant:agent:version` to preserve every dimension it needs for later lookup; the delimiter
and individual segments remain entirely caller-defined. `Actor.key` resolves one stable Actor;
`Artifact.key` names one logical artifact while its caller-owned opaque `version` selects an exact,
immutable row; `Action.key` identifies caller-defined logical work; and `Checkpoint.key` groups
revisions of one caller-defined recoverable instance. Ledger compares Artifact versions but does
not parse them, choose a latest version, or model a version graph.

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
optional `framework`; its optional `key` is the upstream producer's stable, namespaced identity,
not a display name. Recommended types include `user`, `agent`, `orchestrator`, `harness`, `model`,
`tool`, and `system`. Adapter-specific recording details belong in `extensions`.

`causation_id` optionally references the Event that caused the current fact. It is a logical Event
reference, not a database foreign key. Timestamps and cross-Lane observation order never establish
causality.

`payload` contains fact-specific data. `extensions` contains namespaced framework, vendor, or
application additions outside the core contract. Readers MUST preserve unknown Event types,
payload fields, and extensions. Core payload profiles standardize portable fields without turning
the Store into a contextual validator: validating a Core call payload requires resolving the
subject Action type, and remains a producer and conformance responsibility.

Large inputs and outputs SHOULD first be registered as an `Artifact`. Each row is one immutable
version identified by a Ledger-owned `id` and a caller-owned `(key, version)` pair. Ledger stores
the content URI, digest, media type, and size; an application-selected Content Store owns the
bytes. Events and Checkpoints retain only `artifact_id`.

## Core vocabulary

`spec/vocabulary.json` is the machine-readable registry of Core Action and Event type values. SDKs
export constants for this vocabulary, but `Action.type` and `event_type` remain strings: Stores MUST
preserve unknown values and MUST NOT reject an otherwise valid record merely because its type is
not in the Core registry.

Core Action types are:

| Type | Meaning |
| --- | --- |
| `model_call` | One logical model invocation; physical retries are Attempts. |
| `tool_call` | One logical tool invocation; physical retries are Attempts. |
| `compact` | A Harness context compaction. |
| `checkpoint` | A Harness checkpoint operation; the durable result is a separate Checkpoint object. |

Every Action has an immutable `Effect` fixed before execution:

- `kind` is `none`, `read`, `write`, or `unknown`;
- `idempotency` is `not_applicable`, `inherent`, `keyed`, `none`, or `unknown`.

`unknown` preserves uncertainty rather than guessing. Ledger stores these facts but does not decide
whether a caller retries an Action. Extension Action types use the same Core Effect vocabulary.

Core lifecycle Event types are `session.started/completed`, `run.started/completed/failed/cancelled`,
`lane.created`, `turn.started/completed/failed`, `action.started/completed/failed`, and
`attempt.requested/completed/failed/cancelled/outcome_unknown`. The standard framework-state Events are
`lane.framework.snapshot.saved` and `lane.framework.checkpoint.linked`.
`action.started` is reserved for extension Actions whose lifecycle begins independently of an
Attempt; Core model/tool calls use their first `attempt.requested` as the start fact.

## Core call payload profiles

`spec/schemas/call-payload.schema.json` is the normative field-level contract for Core
`model_call` and `tool_call` Attempt Events. Payloads use one canonical shape across Harnesses;
provider, framework, and application fields belong in Event `extensions` rather than alternate
payload aliases.

Large request and result bodies use the same inline-or-reference rule in both profiles: exactly one
of `input` / `input_artifact_id` or `output` / `output_artifact_id` is present. The inline value may
be any JSON value, including `null`. The referenced form identifies an existing immutable Artifact
version.

### Model calls

| Event | Standard payload |
| --- | --- |
| `attempt.requested` | `input` or `input_artifact_id`; optional requested `model {id, provider}` and caller-generated `client_request_id` |
| `attempt.completed` | `output` or `output_artifact_id`; optional actual `model`, open `finish_reason`, normalized `usage`, and `provider_request_id` |
| `attempt.failed` | structured `error`; optional actual `model`, partial `usage`, and `provider_request_id` |

The requested and actual model may differ when a gateway routes aliases or performs fallback.
`client_request_id` is fixed and persisted before one physical request; when the provider accepts
that caller identifier, it can also be used to reconcile an unresolved Attempt. A retry is another
Attempt and normally receives another client request identifier.
`usage` uses provider-neutral token fields: `input_tokens`, `output_tokens`,
`cache_read_input_tokens`, `cache_write_input_tokens`, and `total_tokens`. Missing fields mean the
producer did not observe them; zero means an observed zero.

### Tool calls

| Event | Standard payload |
| --- | --- |
| `attempt.requested` | `tool_name` and `input` or `input_artifact_id`; optional `idempotency_key` and `recovery_decision_id`; `Action.key` stores `tool_call_id` |
| `attempt.completed` | `output` or `output_artifact_id`; optional `external_operation_id` for later reconciliation |
| `attempt.failed` | structured `error`; optional `external_operation_id` |

`Action.key` identifies the Harness-visible logical tool request and remains the same across
Attempts. When an Action declares `Effect.idempotency = keyed`, its first
`attempt.requested` MUST contain the effective `idempotency_key`, and retries MUST reuse it.
`recovery_decision_id` identifies the one caller decision that authorized a retry after an unknown
outcome; it does not grant permission to later retries.

All Core failure payloads use `error {type, message, code?, retryable?}`. `type` is a stable
machine-oriented classification; `message` is the human-readable diagnostic. `retryable` reports
an observed provider or tool property and is not a recovery decision.
Recorder failure operations accept the observed terminal payload separately from the error and
merge both into one `attempt.failed` Event. This preserves partial usage and provider/external
operation identifiers even when the call itself returns an error.

### Attempt terminal outcomes

`attempt.completed` records a known successful outcome and `attempt.failed` a known failed outcome.
`attempt.cancelled` records a confirmed cancellation and carries `reason`. Cancellation and
unknown-outcome payloads may retain an observed `provider_request_id` or `external_operation_id` for
later audit and reconciliation. A caller MUST use
`attempt.outcome_unknown`, not `attempt.cancelled`, when it cannot prove whether an external
operation took effect; its payload carries `reason` and may identify a
`superseded_by_attempt_id`.

All four Events are terminal for that physical Attempt. A requested Attempt without one of them is
unresolved. `attempt.outcome_unknown` closes the old bookkeeping lifecycle after an explicit
recovery decision without pretending that the external outcome is known.

The Core payload of `lane.framework.checkpoint.linked` identifies the exact persisted Checkpoint
with `checkpoint_id`, the recovery binding with `profile` and `profile_version`, and optional
profile-owned `metadata`. A malformed or unknown payload remains an immutable Event; readers do
not rewrite or silently promote it into a selected recovery point.

Two-segment Event names are reserved for the Core vocabulary. Extension Events use
`<subject-kind>.<namespace>.<name...>`, for example `lane.framework.pi.entry.appended`; custom Action
types similarly use a namespace such as `framework.pi.branch_switch`. This naming rule prevents
independent adapters from claiming short names while keeping the protocol open to new Harnesses.

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

Core `LaneRecorder` APIs expose this ordered batch operation without requiring callers to manage
`expected_last_seq`; a Recorder serializes its appends and advances its cached Lane head from the
accepted receipt. This is the general composition boundary for causally related Events, not a
transaction over arbitrary Ledger entities or external systems.

The append digest is SHA-256 over RFC 8785 canonical JSON for the ordered proposed-Event array.
Optional fields that are absent are omitted; explicit `null` remains part of the digest.

Event IDs and append IDs are globally unique. `(lane_id, seq)` and `(action_id, attempt_no)` are
unique. A Store snapshots caller-owned input and MUST NOT expose
mutable references that can rewrite accepted history.

`seq` is authoritative only inside its Lane. UUIDv7 creation time, wall-clock timestamps, and a
read-side merge of multiple Lanes are useful for observation but do not create a global causal
order.

## Write-before-execute

For a model or tool call, an adapter first creates the logical Action with its key and fixed Effect,
then creates an Attempt and durably appends `attempt.requested` before the external operation. The
first requested Attempt is also the observable start of that Action; emitting a separate
`action.started` would duplicate the same fact. Retries append another Attempt and its complete
request. The adapter appends a terminal Attempt outcome before the harness advances.

A requested Attempt without a terminal Event is unresolved after a crash. Recovery may query the
provider, apply a known outcome, or ask for human resolution. After an explicit decision to retry,
the producer records the old Attempt as `attempt.outcome_unknown` and creates the next
`attempt_no`. An unresolved `write` without known idempotency, or any `unknown` Effect, MUST NOT be
silently retried.

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
`artifact_id` pointing to one immutable Artifact version. Normalized Events alone are not claimed to rebuild contexts containing branches,
compaction state, queues, custom messages, or opaque checkpoints. RFC 0002 defines this adapter
boundary; `docs/checkpoint.md` defines the save and anchor contract.

If a Checkpoint is the safe terminal boundary of a Run, the adapter first saves that Checkpoint and
then SHOULD append `lane.framework.checkpoint.linked` followed by `run.completed` in one atomic
Lane batch, with the completion Event caused by the link Event. This prevents readers from
observing that terminal execution fact without its recovery material reference. It does not make
Checkpoint storage and Event append one transaction: a crash between them leaves an unlinked
Checkpoint that may be reconciled or collected.

## Read models

The append log is the source of execution facts. `load_run(session_id, run_id)` returns a `RunView`
containing only Lanes owned by that upstream pair and their Turns, Actions, Attempts, Events, and
referenced Actors. It is a bounded read model, not an authoritative Run row. Timelines,
unresolved-Attempt inspection, trajectories, and recovery plans are projections over Events plus
immutable containment rows. They may be rebuilt without mutating Ledger history.

Run inspection exposes all terminal Events, linked Checkpoints, and unresolved Attempts. It does
not collapse conflicting terminal facts, select a Checkpoint, mark upstream input as processed, or
decide retry, termination, scheduling, and acceptance. In particular, `run.completed` means the
producer declared the upstream-defined Run scope complete; the upstream host remains responsible
for interpreting and accepting that result into its own fenced control state.

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
