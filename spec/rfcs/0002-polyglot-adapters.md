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
its model and tool middleware are installed together with its Run and Turn hooks.

## Lanes and native state

A Lane is one serial execution line and the OCC partition identified by `lane_id`. Each Lane belongs
immutably to one upstream `(session_id, run_id)` and starts at `last_seq = 0`.

Recommended Lane names are:

```text
main                                      normalized Harness execution
branch/<branch_id>                        parallel or speculative Harness branch
framework/pi/<native_session_id>          Pi session tree for this Run
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

Actions are logical and retain their caller-owned key and Effect across retries. An Adapter may use
a Harness execution ID directly or scope it with a caller-owned Run identity; Ledger does not parse
the key. A provider or tool retry creates another Attempt under the same Action. Before every
physical execution, a strict Adapter creates the Attempt and commits its complete
`attempt.requested`; the first requested Attempt also marks the logical Action as started. The
Adapter commits a Core terminal Attempt outcome before the Harness consumes the outcome.

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

- `WithBeforeRun` and `WithAfterRun` for native `AgentSnapshot` Checkpoints at Run admission and a
  clean terminal boundary;
- `WithBeforeTurn` and `WithAfterTurn` for Turn boundaries;
- `ModelMiddleware` for every physical provider Attempt, including AgentGo context-summary calls;
- `ToolMiddleware` around the complete validation, authorization, and execution pipeline;
- stable AgentGo `Execution.ID` values, scoped by the Adapter's Checkpoint execution scope, as
  logical Action keys.

The Checkpoint uses AgentGo's codec-aware `AgentSnapshot`, including Loop progress and accepted
steering/follow-up queues. Custom `AgentMessage` implementations register with the application
codec supplied to the Adapter. The default AgentGo codec supports built-in state types and rejects
unknown values instead of silently lowering them.

Recovery does not splice normalized Events into a transcript. `BeforeRun` restores the admission
Snapshot, then model and tool middleware return completed outcomes as their normal result while the
AgentGo Loop runs again. This preserves AgentGo's message commit, tool-result construction, progress,
and context-management rules. Every request carries a semantic fingerprint; a stable Execution ID
with different input fails closed instead of consuming an unrelated old outcome.

The Adapter owns only input already accepted into `AgentSnapshot`. Input passed to the current
`Prompt`, `Continue`, or `Inject` remains an upstream durable-inbox responsibility until AgentGo
accepts it. After process loss the host must re-deliver that input; Ledger records that AgentGo knew
an execution, not that the upstream request source acknowledged consumption.

AgentGo currently converts a `ToolMiddleware` error into a normal tool-error result. The Adapter
therefore checks for an unresolved tool again in `AfterTurn` and stops the Run before another model
or tool execution, while retaining the admission Checkpoint for recovery. This prevents further
external progress but cannot prevent AgentGo from briefly projecting the synthetic tool-error
result in process memory, so the profile declares `outcome_gate = best_effort`. A future fatal tool
middleware error contract can raise that capability to `strict`.

## Recovery sequence

Adapters follow the same semantic sequence even though their APIs differ:

1. freeze or create an idle Harness runtime;
2. load the latest Checkpoint and reject an unsupported state `format` before restoration;
3. return already completed outcomes through the Harness's native execution boundary, without
   re-executing their external actions or mutating native state out of band;
4. inspect unresolved Attempts and reconcile them with provider/tool state;
5. never retry an unresolved side-effecting tool unless idempotency or explicit human resolution
   makes it safe;
6. invoke the Harness native resume or continue API.

An explicit resolution authorizes one physical retry of one unresolved Attempt, not the Action or
Session indefinitely. The adapter records `recovery_decision_id` in the new `attempt.requested`
payload and terminalizes superseded Attempts with `attempt.outcome_unknown` before crossing the
external execution boundary. If the new Attempt is also unresolved, recovery requires a new
decision. These identifiers are caller-owned evidence; Core does not interpret their values.

Recovery may return a blocked or decision-required result. Starting execution is not proof that
recovery was safe.

## Conformance

Core SDKs must pass the shared RFC 8785 append vectors and Store behavior tests. Strict Adapters
add failure injection at model prewrite, tool prewrite, outcome write, native-state write, and
restore boundaries. Tests prove that the external operation did not start or the Harness did not
advance after a required write failed.
