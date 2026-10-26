"""Business-owned work across Agent Sessions."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import Field, model_validator

from agent_ledger.common import Fact

from .node import NodeRef, SourceRef, Status, _NodeValue


class JobRef(Fact):
    source: str = Field(min_length=1)
    id: str = Field(min_length=1)


class Relation(Fact):
    kind: Literal["depends_on", "handoff"]
    source: NodeRef
    target: NodeRef
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
    nodes: list[_NodeValue]
    relations: list[Relation] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)
    issues: list[str] = Field(default_factory=list)

    # +spec=`Job completion is source-owned; Session fragments never imply exclusive cost ownership`
    # +link=docs/observation.md
    @model_validator(mode="after")
    def check_graph(self) -> JobView:
        from .inspect import inspect

        inspect(self)
        return self
