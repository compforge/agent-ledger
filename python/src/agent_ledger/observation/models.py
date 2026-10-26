"""Session and Job observations; shared facts without storage or execution policy."""

from __future__ import annotations

from collections.abc import Sequence
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
    schema_version: Literal["agent-ledger/session@1"] = "agent-ledger/session@1"
    session: SessionRef
    attributes: dict[str, Any] = Field(default_factory=dict)
    items: list[Item]
    lanes: list[Lane] = Field(default_factory=list)
    issues: list[str] = Field(default_factory=list)

    # +spec=`One Session owns all contained facts; a Job selects facts across Sessions`
    # +link=docs/observation.md
    @model_validator(mode="after")
    def check_graph(self) -> SessionView:
        _check_graph(self.items, self.session)
        return self


class JobRef(Fact):
    source: str = Field(min_length=1)
    id: str = Field(min_length=1)


class ItemRef(Fact):
    id: str = Field(min_length=1)
    session: SessionRef | None = None


class Relation(Fact):
    kind: Literal["depends_on", "handoff"]
    source: ItemRef
    target: ItemRef
    evidence: list[SourceRef] = Field(default_factory=list)


class JobView(Fact):
    schema_version: Literal["agent-ledger/job@1"] = "agent-ledger/job@1"
    job: JobRef
    name: str | None = None
    goal: Any = Field(default=None, json_schema_extra={"preserve_null": True})
    result: Any = Field(default=None, json_schema_extra={"preserve_null": True})
    status: Status = "unknown"
    started_at: str | None = None
    finished_at: str | None = None
    items: list[Item]
    relations: list[Relation] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)
    issues: list[str] = Field(default_factory=list)

    # +spec=`Job completion is source-owned; Session fragments never imply exclusive cost ownership`
    # +link=docs/observation.md
    @model_validator(mode="after")
    def check_graph(self) -> JobView:
        _check_time("job " + self.job.id, self.started_at, self.finished_at)
        _check_graph(self.items, None, self.relations)
        return self

    def reference_issues(self) -> list[str]:
        """Describe missing Session fragments without changing evidence or inventing calls."""
        return _check_graph(self.items, None, self.relations)


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


def _key(id: str, session: SessionRef | None) -> tuple[str, str, str]:
    return (session.source, session.id, id) if session else ("", "", id)


def _check_graph(
    items: list[Item],
    session: SessionRef | None,
    relations: Sequence[Relation] = (),
) -> list[str]:
    nodes: dict[tuple[str, str, str], Item] = {}
    tools: list[ToolCall] = []
    issues: list[str] = []

    def visit(node: Item, parent: str) -> None:
        if session is not None and node.session is not None and node.session != session:
            raise ValueError(f"node {node.id}: foreign Session inside SessionView")
        identity = _key(node.id, node.session or session)
        if identity in nodes:
            raise ValueError(f"duplicate view node: {identity}")
        nodes[identity] = node
        if isinstance(node, Turn) and parent not in {"session", "job", "operation"}:
            raise ValueError(f"turn {node.id}: invalid parent {parent}")
        if isinstance(node, ToolCall):
            tools.append(node)
        if isinstance(node, (Turn, Operation)):
            for child in node.items:
                visit(child, node.kind)

    for item in items:
        visit(item, "session" if session else "job")
    for tool in tools:
        if tool.model_call_id is None:
            continue
        owner = tool.session or session
        target = nodes.get(_key(tool.model_call_id, owner))
        if target is not None and not isinstance(target, ModelCall):
            raise ValueError(f"tool {tool.id}: referenced item is not a model call")
        if target is None:
            if session is not None or owner is None:
                raise ValueError(f"tool {tool.id}: missing model call {tool.model_call_id}")
            issues.append(
                f"tool {_key(tool.id, owner)}: model call {tool.model_call_id} outside Job snapshot"
            )
    for relation in relations:
        for ref in (relation.source, relation.target):
            if _key(ref.id, ref.session) not in nodes:
                if ref.session is None:
                    raise ValueError(f"{relation.kind}: missing job item {ref.id}")
                issues.append(
                    f"{relation.kind}: Session item {_key(ref.id, ref.session)} "
                    "outside Job snapshot"
                )
    return list(dict.fromkeys(issues))
