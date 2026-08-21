# RFC 0002: Polyglot Harness Adapters

## Status

Draft specification for adapter packages in the `0.x` line.

## Problem

Agent Ledger is useful only when it enters a Harness before external work starts, after outcomes
arrive, and when native recovery state changes. Those boundaries are Harness-owned and
language-specific. A universal hook API would either expose only the lowest common denominator or
claim guarantees that a Harness cannot provide.

The stable product is therefore the Ledger contract. Language SDKs are thin implementations;
Harness adapters own integration code and evolve with their target Harness.

## Adapter responsibilities

An Adapter has two independent responsibilities:

1. **Recording binding** maps Harness boundaries to Lane, Turn, Action, Attempt, and Event writes.
2. **Recovery binding** dumps lossless Harness-native state into a `CheckpointStore`, restores it,
   and invokes the Harness continuation API.

Normalized Events serve audit, timelines, trajectories, inspection, and alerting. Native state
serves lossless recovery. An Adapter must not claim that normalized Events rebuild a context when
the Harness also depends on branches, compaction state, queues, custom messages, or opaque
checkpoints.

## Capability declaration

Every Adapter publishes an `AdapterDescriptor` conforming to `schemas/adapter.schema.json`.
Recording boundaries use three guarantee levels:

- `strict`: the Harness awaits the write and does not begin or advance past the external action if
  persistence fails;
- `best_effort`: the callback is ordered, but an error may be swallowed or cannot stop execution;
- `unsupported`: the Harness exposes no trustworthy boundary.

Recovery uses `native_store`, `snapshot`, `checkpoint`, or `unsupported`. The descriptor describes
the fully installed Adapter. For example, AgentGo reaches strict physical model Attempts only when
its model wrapper is installed together with its hooks.

## Lanes and native state

A Lane is one serial execution line and the OCC partition identified by `lane_id`. Each Lane belongs
immutably to one upstream `(session_id, run_id)` and starts at `last_seq = 0`.

Recommended Lane names are:

```text
main                                      normalized Harness execution
branch/<branch_id>                        parallel or speculative Harness branch
framework/pi/<native_session_id>          Pi session tree for this Run
framework/agentgo/<native_session_id>     AgentGo native messages for this Run
```

An Adapter reopens existing Lanes when the upstream host supplies the same `run_id`; it does not
infer the Run boundary from process or recovery lifecycle. Native state is not silently shared
across different Run IDs: the Adapter must explicitly import a checkpoint or snapshot into a new
Lane and record the link.

`seq` orders one Lane. `load_session` may merge Lanes for display, but timestamp or merge position
does not create causality; consumers use containment and `causation_id`.

## Turn, Action, and Attempt mapping

One Harness Turn may contain several model calls and tool calls:

```text
Turn
  ├── Action(model_call) → Attempt 1
  ├── Action(tool_call)  → Attempt 1
  ├── Action(compact)
  └── Action(model_call) → Attempt 1
```

Actions are logical and retain the Effect fixed before their first Attempt. A provider or tool retry creates another Attempt under the same Action. A
strict Adapter creates the Attempt and commits `attempt.requested` before physical execution, then
commits `attempt.completed` or `attempt.failed` before the Harness consumes the outcome.

Harness work outside model and tool calls uses a concrete Action `type`, such as `compact` or
`checkpoint`; `operation` is only the conceptual category, not a stored Action type.

## Pi profile

The strict Pi integration implements its native `SessionStorage` in a dedicated framework Lane.
Pi remains responsible for rebuilding model context from its append-only entry tree. Direct
AgentHarness hooks add normalized Turn, model Action, tool Action, Attempt, and Run Events to the
main Lane.

Pi's entry types, active leaf, and branching rules are private recovery semantics. Other Harnesses
and orchestrators are not required to expose them.

A coding-agent extension that swallows model-hook errors must declare model prewrite as
`best_effort`. It may emit telemetry, but it is not the strict profile.

## AgentGo profile

The AgentGo integration combines:

- `WithMessageCommitter` for native transcript durability before messages enter context or trigger
  tools;
- `WithBeforeTurn` and `WithAfterTurn` for Turn boundaries;
- a `ChatModel` wrapper for every physical provider Attempt, including internal retries;
- `ToolMiddleware` for write-before-tool and outcome gating;
- `HoldRuns`, `SetMessages`, and `Continue` for restoration.

Custom `AgentMessage` implementations require an application codec. The default codec rejects
unknown message types instead of silently lowering them. Queues that AgentGo cannot export remain a
declared recovery limitation until a host wrapper captures them.

## Recovery sequence

Adapters follow the same semantic sequence even though their APIs differ:

1. freeze or create an idle Harness runtime;
2. load the latest Checkpoint and reject an unsupported state `format` before restoration;
3. replay already completed outcomes after that checkpoint into native state without re-executing
   their external actions;
4. inspect unresolved Attempts and reconcile them with provider/tool state;
5. never retry an unresolved side-effecting tool unless idempotency or explicit human resolution
   makes it safe;
6. invoke the Harness native resume or continue API.

An explicit resolution authorizes one physical retry of one unresolved Attempt, not the Action or
Session indefinitely. The adapter records the caller's decision identifier in the new
`attempt.requested` payload and terminalizes superseded Attempts as `outcome_unknown` before
crossing the external execution boundary. If the new Attempt is also unresolved, recovery requires
a new decision. These payload fields are caller-owned evidence; Core does not interpret them.

Recovery may return a blocked or decision-required result. Starting execution is not proof that
recovery was safe.

## Conformance

Core SDKs must pass the shared RFC 8785 append vectors and Store behavior tests. Strict Adapters
add failure injection at model prewrite, tool prewrite, outcome write, native-state write, and
restore boundaries. Tests prove that the external operation did not start or the Harness did not
advance after a required write failed.
