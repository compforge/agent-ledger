# Conformance

Language SDKs consume the same golden vectors to verify proposed-Event encoding and append digest
identity, and compare their exported Core type constants with `spec/vocabulary.json`. Adapter
contract suites live beside their implementations.

Store suites verify:

- immutable Actor, Lane, Turn, Action, and Attempt ownership;
- unique `(action_id, attempt_no)` and globally unique Event/append IDs;
- Lane-local `last_seq`, atomic append, optimistic concurrency, and idempotency;
- `event_type` subject-prefix validation and same-Session causation;
- append-only snapshots: mutating caller proposals or returned projections cannot rewrite history;
- Lane `seq` preservation in multi-Lane Session projections.
- `(session_id, run_id)` reads that exclude sibling Runs while preserving their complete hierarchy.

Core recorder suites also verify that terminal Checkpoint linking and `run.completed` share one
atomic Lane append. Run inspection must surface every terminal fact, linked Checkpoint, and
unresolved Attempt without deriving orchestrator control state.

Strict Adapter suites inject failures before model calls, before tools, after external outcomes, and
during native-state restoration. Passing these vectors and behaviors is required before an SDK can
claim Agent Ledger schema `1.x` compatibility.
