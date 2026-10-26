"""Observed nodes and their concrete execution facts."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BeforeValidator, Field, SerializeAsAny, TypeAdapter, model_validator

from agent_ledger.common import CallRequest, CallResponse, Fact, SessionRef

Status = Literal["unknown", "running", "completed", "failed", "cancelled", "outcome_unknown"]


class SourceRef(Fact):
    source: str = Field(min_length=1)
    id: str = Field(min_length=1)
    uri: str | None = None


class Node(Fact):
    kind: str
    id: str = Field(min_length=1)
    name: str | None = None
    session: SessionRef | None = None
    lane_id: str | None = None
    run_id: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    status: Status = "unknown"
    evidence: list[SourceRef] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def check_time(self) -> Node:
        _check_time("node " + self.id, self.started_at, self.finished_at)
        return self


def _decode_node(value: Any) -> Node:
    return _node_adapter.validate_python(value)


# +spec=`Node collections preserve concrete fields during decoding and serialization`
_NodeValue = Annotated[Node, BeforeValidator(_decode_node), SerializeAsAny()]


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
    nodes: list[_NodeValue] = Field(default_factory=list)


class Turn(Node):
    kind: Literal["turn"] = "turn"
    nodes: list[_NodeValue] = Field(default_factory=list)


class NodeRef(Fact):
    id: str = Field(min_length=1)
    session: SessionRef | None = None


_WireNode = Annotated[Input | ModelCall | ToolCall | Operation | Turn, Field(discriminator="kind")]
_node_adapter: TypeAdapter[_WireNode] = TypeAdapter(_WireNode)


def _check_time(owner: str, start: str | None, end: str | None) -> None:
    times = []
    for value in (start, end):
        if value is not None:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError(f"{owner}: timestamps require a timezone")
            times.append(parsed)
    if len(times) == 2 and times[1] < times[0]:
        raise ValueError(f"{owner}: finish precedes start")
