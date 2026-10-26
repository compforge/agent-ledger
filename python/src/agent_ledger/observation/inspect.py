"""Inspect a snapshot once; observation gaps do not change recorded outcomes."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from agent_ledger.common import SessionRef

from .job import JobView, Relation
from .node import Input, ModelCall, Node, Operation, ToolCall, Turn, _check_time
from .session import SessionView


@dataclass
class Inspection:
    issues: list[str]


# +spec=`Invalid structure raises; external gaps are returned without source mutation`
# +link=docs/observation.md
def inspect(view: SessionView | JobView) -> Inspection:
    if isinstance(view, SessionView):
        issues = _check_graph(view.nodes, view.session)
    else:
        _check_time("job " + view.job.id, view.started_at, view.finished_at)
        issues = _check_graph(view.nodes, None, view.relations)
    return Inspection(issues=issues)


def validate(view: SessionView | JobView) -> None:
    inspect(view)


def _key(id: str, session: SessionRef | None) -> tuple[str, str, str]:
    return (session.source, session.id, id) if session else ("", "", id)


def _check_graph(
    nodes: Sequence[Node],
    session: SessionRef | None,
    relations: Sequence[Relation] = (),
) -> list[str]:
    index: dict[tuple[str, str, str], Node] = {}
    tools: list[ToolCall] = []
    issues: list[str] = []

    def visit(node: Node, parent: str) -> None:
        if not isinstance(node, (Input, ModelCall, ToolCall, Operation, Turn)):
            raise ValueError(f"node {node.id}: unsupported concrete node type")
        if session is not None and node.session is not None and node.session != session:
            raise ValueError(f"node {node.id}: foreign Session inside SessionView")
        identity = _key(node.id, node.session or session)
        if identity in index:
            raise ValueError(f"duplicate view node: {identity}")
        index[identity] = node
        if isinstance(node, Turn) and parent not in {"session", "job", "operation"}:
            raise ValueError(f"turn {node.id}: invalid parent {parent}")
        if isinstance(node, ToolCall):
            tools.append(node)
        if isinstance(node, (Turn, Operation)):
            for child in node.nodes:
                visit(child, node.kind)

    for value in nodes:
        visit(value, "session" if session else "job")
    for tool in tools:
        if tool.model_call_id is None:
            continue
        owner = tool.session or session
        target = index.get(_key(tool.model_call_id, owner))
        if target is not None and not isinstance(target, ModelCall):
            raise ValueError(f"tool {tool.id}: referenced node is not a model call")
        if target is None:
            if session is not None or owner is None:
                raise ValueError(f"tool {tool.id}: missing model call {tool.model_call_id}")
            issues.append(
                f"tool {_key(tool.id, owner)}: model call {tool.model_call_id} outside Job snapshot"
            )
    for relation in relations:
        for ref in (relation.source, relation.target):
            if _key(ref.id, ref.session) not in index:
                if ref.session is None:
                    raise ValueError(f"{relation.kind}: missing job node {ref.id}")
                issues.append(
                    f"{relation.kind}: Session node {_key(ref.id, ref.session)} "
                    "outside Job snapshot"
                )
    return list(dict.fromkeys(issues))
