"""Session observation models, independent of Ledger storage and recovery policy."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import Field, model_validator

from agent_ledger.common import CallRequest, CallResponse, Fact, SessionRef

Status = Literal["unknown", "running", "completed", "failed", "cancelled", "outcome_unknown"]


class SourceRef(Fact):
    source: str = Field(min_length=1)
    id: str = Field(min_length=1)
    uri: str | None = None


class Node(Fact):
    id: str = Field(min_length=1)
    name: str | None = None
    lane_id: str | None = None
    run_id: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    status: Status = "unknown"
    evidence: list[SourceRef] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def check_time(self) -> Node:
        times = []
        for value in (self.started_at, self.finished_at):
            if value is not None:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    raise ValueError(f"node {self.id}: timestamps require a timezone")
                times.append(parsed)
        if len(times) == 2 and times[1] < times[0]:
            raise ValueError(f"node {self.id}: finish precedes start")
        return self


class Input(Node):
    kind: Literal["input"] = "input"
    mode: str | None = None
    content: Any = Field(json_schema_extra={"preserve_null": True})


class ModelCall(Node):
    kind: Literal["model_call"] = "model_call"
    request: CallRequest | None = None
    response: CallResponse | None = None


class ToolCall(Node):
    kind: Literal["tool_call"] = "tool_call"
    tool_call_id: str | None = None
    model_call_id: str | None = None
    request: CallRequest | None = None
    response: CallResponse | None = None
    sessions: list[SessionRef] = Field(default_factory=list)


class Operation(Node):
    kind: Literal["operation"] = "operation"
    operation: str = Field(min_length=1)
    data: Any = Field(default=None, json_schema_extra={"preserve_null": True})
    items: list[Item] = Field(default_factory=list)


class Turn(Node):
    kind: Literal["turn"] = "turn"
    items: list[Item] = Field(default_factory=list)


class Lane(Fact):
    id: str = Field(min_length=1)
    name: str | None = None
    parent_id: str | None = None


Item = Annotated[Input | ModelCall | ToolCall | Operation | Turn, Field(discriminator="kind")]
for _model in (ToolCall, Operation, Turn):
    _model.model_rebuild()


class SessionView(Fact):
    schema_version: Literal["agent-ledger/session-view@1"] = "agent-ledger/session-view@1"
    session: SessionRef
    attributes: dict[str, Any] = Field(default_factory=dict)
    items: list[Item]
    lanes: list[Lane] = Field(default_factory=list)
    issues: list[str] = Field(default_factory=list)

    # +spec=`View preserves native IDs and partial evidence without requiring a Ledger hierarchy`
    # +link=docs/observation.md
    @model_validator(mode="after")
    def check_graph(self) -> SessionView:
        ids: set[str] = set()
        calls: dict[str, ModelCall] = {}
        links: list[ToolCall] = []

        def visit(node: Item, parent_kind: str = "session") -> None:
            if node.id in ids:
                raise ValueError(f"duplicate view node: {node.id}")
            ids.add(node.id)
            if isinstance(node, Turn) and parent_kind not in {"session", "operation"}:
                raise ValueError(f"turn {node.id} has invalid parent {parent_kind}")
            if isinstance(node, ModelCall):
                calls[node.id] = node
            if isinstance(node, ToolCall):
                links.append(node)
            if isinstance(node, (Turn, Operation)):
                for child in node.items:
                    visit(child, node.kind)

        for item in self.items:
            visit(item)
        for tool in links:
            if tool.model_call_id is not None and tool.model_call_id not in calls:
                raise ValueError(f"tool {tool.id}: missing model call {tool.model_call_id}")
        return self
