# Conformance

Language SDKs consume the same golden vectors to verify event encoding and append identity. Adapter
contract suites live beside their implementations. The conformance target includes failure
injection before model calls, before tools, after external outcomes, and during native-state
restoration.

Store contract suites must also verify append-only history: appends snapshot proposed content, and
mutating caller-owned proposals or returned read models cannot rewrite committed events. An
implementation that exposes event update or deletion as `EventStore` behavior is not conformant.

Passing these vectors is required before an SDK can claim Agent Ledger schema `1.x` compatibility.
