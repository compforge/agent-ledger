import json
from pathlib import Path

import pytest

from agent_ledger.observation import (
    Input,
    JobView,
    ModelCall,
    Node,
    Operation,
    SessionView,
    ToolCall,
    Turn,
    inspect,
    select_nodes,
)


def session_fixture():
    path = Path(__file__).parents[2] / "conformance/vectors/session-view.json"
    return SessionView.model_validate_json(path.read_text())


def flatten(nodes):
    for node in nodes:
        yield node
        if isinstance(node, (Operation, Turn)):
            yield from flatten(node.nodes)


# +case:id=node_polymorphism,desc=`Concrete fields survive a Node collection roundtrip`
def test_node_collection_preserves_concrete_types_and_fields():
    nodes: list[Node] = [
        Input(id="input", content=None),
        ModelCall(id="model", request={"input": {"value": 9007199254740993}}),
        ToolCall(id="tool", model_call_id="model", request={"tool_name": "read", "input": None}),
        Operation(
            id="operation",
            operation="check",
            data=None,
            nodes=[
                Turn(id="turn", nodes=[Input(id="nested", content="next")]),
            ],
        ),
    ]
    view = SessionView(session={"source": "test", "id": "session"}, nodes=nodes)
    encoded = view.model_dump_json()
    restored = SessionView.model_validate_json(encoded)
    assert [type(n) for n in flatten(restored.nodes)] == [type(n) for n in flatten(nodes)]
    assert json.loads(restored.model_dump_json()) == json.loads(encoded)
    assert restored.nodes[1].request.input["value"] == 9007199254740993
    assert json.loads(encoded)["nodes"][3]["data"] is None
    assert "NodeBase" not in encoded


# +case:id=select_subtrees,desc=`Selection preserves ownership, order and independent source facts`
def test_selection_deduplicates_overlaps_and_does_not_mutate_source():
    session = session_fixture()
    before = session.model_dump_json()
    selected = select_nodes(session, ["t1", "turn-1", "input-1", "turn-1"])
    assert [n.id for n in selected] == ["input-1", "turn-1"]
    assert all(n.session == session.session for n in flatten(selected))
    job = JobView(job={"source": "test", "id": "job"}, nodes=selected)
    assert not inspect(job).issues
    selected[1].nodes[0].request.input["messages"][0]["content"] = "changed"
    selected[0].session.id = "changed"
    assert session.model_dump_json() == before
    assert selected[1].session.id == "native-session"


def test_selection_reports_missing_ids_and_keeps_unselected_models_as_gaps():
    session = session_fixture()
    with pytest.raises(ValueError, match="missing nodes"):
        select_nodes(session, ["absent"])
    assert select_nodes(session, []) == []
    selected = select_nodes(session, ["t1"])
    assert [n.id for n in selected] == ["t1"]
    job = JobView(job={"source": "test", "id": "job"}, nodes=selected)
    before = job.model_dump_json()
    assert len(inspect(job).issues) == 1
    assert job.model_dump_json() == before
