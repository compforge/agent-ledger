# Conformance

Language SDKs consume the same golden vectors to verify proposed-Event encoding and append digest
identity, and compare their exported Core type constants with `spec/vocabulary.json`. Adapter
contract suites live beside their implementations.

Core conformance fixtures also validate `model_call` and `tool_call` Attempt payloads against
`spec/schemas/call-payload.schema.json`; aliases outside those profiles are not conformant.

Store suites verify:

- immutable Actor, Artifact, Lane, Turn, Action, and Attempt ownership;
- unique Artifact `(key, version)` identity and conflict detection when its content locator changes;
- unique `(action_id, attempt_no)` and globally unique Event/append IDs;
- Lane-local `last_seq`, atomic append, optimistic concurrency, and idempotency;
- `event_type` subject-prefix validation and same-Session causation;
- append-only snapshots: mutating caller proposals or returned projections cannot rewrite history;
- Lane `seq` preservation in multi-Lane Session projections.
- `(session_id, run_id)` reads that exclude sibling Runs while preserving their complete hierarchy.

Core recorder suites verify that the public batch append shares the Store's atomicity and advances
the Recorder's cached Lane head. Terminal Checkpoint linking and `run.completed` exercise this
general composition boundary. Run inspection must surface every terminal fact, linked Checkpoint,
and unresolved Attempt without deriving orchestrator control state.

Strict Adapter suites inject failures before model calls, before tools, after external outcomes, and
during native-state restoration. Passing these vectors and behaviors is required before an SDK can
claim Agent Ledger schema `1.x` compatibility.

## Session observation

`session-view.json` is a single-Session round-trip fixture; `view-ledger.json` projects the same retries,
parallel Lanes and incomplete outcomes through every SDK. Shared call definitions live in
`common.schema.json`; Ledger payload profiles and `session.schema.json` reference them.
Validation retains absent values and explicit zero/null without inventing timing or recovery state.

- `vectors/common-call.json`: common request/response codecs preserve null, zero usage and separate source-specific details across all three SDKs.

`job-review.json` and `job-input.json` exercise JobView across all SDKs: Session-scoped IDs,
partial evidence, deterministic operations, handoffs and shared calls without cost allocation.
