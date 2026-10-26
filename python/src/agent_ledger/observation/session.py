"""One Agent Session and source-preserving node selection."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal

from pydantic import Field, model_validator

from agent_ledger.common import Fact, SessionRef

from .node import Node, Operation, Turn, _NodeValue


class Lane(Fact):
    id: str = Field(min_length=1)
    name: str | None = None
    parent_id: str | None = None


class SessionView(Fact):
    schema_version: Literal["agent-ledger/session@1"] = "agent-ledger/session@1"
    session: SessionRef
    attributes: dict[str, Any] = Field(default_factory=dict)
    nodes: list[_NodeValue]
    lanes: list[Lane] = Field(default_factory=list)
    issues: list[str] = Field(default_factory=list)

    # +spec=`One Session owns all contained facts; a Job selects facts across Sessions`
    # +link=docs/observation.md
    @model_validator(mode="after")
    def check_graph(self) -> SessionView:
        from .inspect import inspect

        inspect(self)
        return self


# +spec=`Selection copies whole subtrees, preserves source order and resolves Session ownership`
# +link=docs/observation.md
def select_nodes(session: SessionView, node_ids: Sequence[str]) -> list[Node]:
    from .inspect import inspect

    inspect(session)
    wanted = set(node_ids)
    found: set[str] = set()
    selected: list[Node] = []

    def collect(node: Node, covered: bool) -> None:
        chosen = node.id in wanted
        if chosen:
            found.add(node.id)
            if not covered:
                selected.append(node.model_copy(deep=True))
        if isinstance(node, (Operation, Turn)):
            for child in node.nodes:
                collect(child, covered or chosen)

    def attach(node: Node) -> None:
        node.session = session.session.model_copy(deep=True)
        if isinstance(node, (Operation, Turn)):
            for child in node.nodes:
                attach(child)

    for node in session.nodes:
        collect(node, False)
    missing = wanted - found
    if missing:
        raise ValueError(f"Session {session.session.id}: missing nodes {sorted(missing)}")
    for node in selected:
        attach(node)
    return selected
