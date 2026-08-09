# RFC 0002: Polyglot Framework Adapters

## Status

Draft specification for adapter packages in the `0.x` line.

## Problem

Agent Ledger is useful only when it can enter a framework at the points where external work starts
and where native recovery state changes. Those points are framework-owned and language-specific.
A universal hook API would either expose the lowest common denominator or claim guarantees that a
framework cannot provide.

The stable product is therefore the ledger specification. Language SDKs are thin implementations,
while framework adapters own most integration code and evolve with their target frameworks.

## Adapter responsibilities

An adapter has two independent responsibilities:

1. **Recording binding** maps framework lifecycle boundaries to normalized Ledger events.
2. **Recovery binding** persists lossless framework-native state and invokes the framework's own
   continuation API.

Normalized events serve global timelines, trajectories, inspection, and alerting. Native state
serves recovery. An adapter must not claim that normalized events alone can rebuild a context when
the framework also depends on branches, compaction state, queues, custom messages, or opaque
checkpoints.

## Capability declaration

Every adapter publishes an `AdapterDescriptor` conforming to `schemas/adapter.schema.json`.
Recording boundaries use three guarantee levels:

- `strict`: the framework awaits the write and does not begin or advance past the external action
  when persistence fails;
- `best_effort`: the callback is ordered but an error may be swallowed or cannot stop execution;
- `unsupported`: the framework exposes no trustworthy boundary.

Recovery uses `native_store`, `snapshot`, `checkpoint`, or `unsupported`. The descriptor describes
the fully documented installation. For example, AgentGo reaches strict physical model attempts only
when its model wrapper is installed together with its hooks.

## Event streams and semantic runs

An `EventStream` is the optimistic-concurrency partition identified by `(session_id, stream_id)`.
`run_id` remains a semantic field on every event. Separating the two allows a framework-native state
stream to survive process replacement and contain mutations caused by several runtime runs.

Recommended stream identities are:

```text
run/<run_id>                         normalized execution owned by one loop
framework/pi/<native_session_id>    Pi session tree
framework/agentgo/<session_id>      AgentGo native messages
```

Language SDKs may use `stream_id = run_id` for normalized runs. A session scan merges all streams by
the store-assigned `commit_cursor`; causality remains defined by run and event links, not cursor or
wall-clock order.

## Pi profile

The strict Pi integration implements its native `SessionStorage`. Entries and active-leaf changes
are stored losslessly, then Pi's `Session` rebuilds model context. Direct AgentHarness hooks add
normalized model, tool, turn, and run events.

A coding-agent extension that swallows model-hook errors must declare model prewrite as
`best_effort`. It can still emit telemetry, but it is not the strict profile.

## AgentGo profile

The AgentGo integration combines:

- `WithMessageCommitter` for native transcript durability before messages enter context or trigger
  tools;
- `WithBeforeTurn` and `WithAfterTurn` for logical step boundaries;
- a `ChatModel` wrapper for every physical provider attempt, including internal retries;
- `ToolMiddleware` for write-before-tool and outcome gating;
- `HoldRuns`, `SetMessages`, and `Continue` for restoration.

Custom `AgentMessage` implementations require an application codec. The default codec must reject
unknown message types instead of silently lowering them to model messages. Queues that a framework
cannot export are a declared recovery limitation and must be captured by a host wrapper before they
can be replayed.

## Recovery sequence

Adapters follow the same semantic sequence even though their APIs differ:

1. freeze or create an idle framework runtime;
2. load the latest compatible native state and tail mutations;
3. restore the framework-owned session or context;
4. inspect unresolved Ledger attempts and reconcile them with native state;
5. never replay an unresolved side-effecting tool unless explicitly safe;
6. invoke the framework's native resume or continue API.

Recovery may return a blocked or decision-required result. Starting execution is not proof that
recovery was safe.

## Conformance

Core SDKs must pass the shared RFC 8785 append vectors. Strict adapters additionally inject failures
at model prewrite, tool prewrite, outcome write, native-state write, and restore boundaries. A test
must prove that the relevant external operation did not start or the loop did not advance.
