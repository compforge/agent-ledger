import copy
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError
from referencing import Registry, Resource

from agent_ledger.observation import JobView, Operation, SessionView, inspect, select_nodes

ROOT = Path(__file__).parents[2]


def fixture(name):
    return json.loads((ROOT / "conformance/vectors" / name).read_text())


def schema():
    schemas = [
        json.loads((ROOT / "spec/schemas" / name).read_text())
        for name in ("common.schema.json", "observation.schema.json", "job.schema.json")
    ]
    registry = Registry().with_resources((s["$id"], Resource.from_contents(s)) for s in schemas)
    return Draft202012Validator(schemas[-1], registry=registry, format_checker=FormatChecker())


@pytest.mark.parametrize("name,gaps", [("job-review.json", 0), ("job-input.json", 1)])
def test_job_contract_roundtrip(name, gaps):
    raw = fixture(name)
    before = copy.deepcopy(raw)
    schema().validate(raw)
    job = JobView.model_validate(raw)
    assert job.model_dump(mode="json", exclude_unset=True) == raw
    assert len(inspect(job).issues) == gaps
    assert raw == before
    assert job.status == "completed"


def test_task_oriented_session_and_job_share_exact_items():
    raw = fixture("session-view.json")
    session = SessionView.model_validate(raw)
    nodes = select_nodes(session, [node.id for node in session.nodes])
    job = JobView(job={"source": "test", "id": "task"}, nodes=nodes)
    assert not inspect(job).issues

    def strip(nodes):
        for item in nodes:
            item.pop("session", None)
            strip(item.get("nodes", []))

    observed = job.model_dump(mode="json")["nodes"]
    strip(observed)
    assert observed == session.model_dump(mode="json")["nodes"]


@pytest.mark.parametrize(
    "change",
    [
        "duplicate",
        "foreign_session",
        "wrong_kind",
        "missing_local",
        "bad_time",
        "bad_status",
        "old_schema",
    ],
)
def test_invalid_job_graph(change):
    raw = fixture("job-review.json")
    if change == "duplicate":
        raw["nodes"][1]["nodes"].append(copy.deepcopy(raw["nodes"][1]["nodes"][0]))
    elif change == "foreign_session":
        native = fixture("session-view.json")
        native["nodes"][0]["session"] = {"source": "elsewhere", "id": "foreign"}
        with pytest.raises(ValidationError, match="foreign Session"):
            SessionView.model_validate(native)
        return
    elif change == "wrong_kind":
        raw["nodes"][1]["nodes"][1]["model_call_id"] = "t1"
    elif change == "missing_local":
        raw["relations"][0]["target"] = {"id": "absent"}
    elif change == "bad_time":
        raw.update(started_at="2026-10-26T01:00:00Z", finished_at="2026-10-26T00:00:00Z")
    elif change == "bad_status":
        raw["status"] = "good"
    else:
        raw["schema_version"] = "agent-ledger/job-view@1"
    with pytest.raises(ValidationError):
        JobView.model_validate(raw)


def test_partial_external_links_stay_explicit_and_job_local_links_are_strict():
    raw = fixture("job-input.json")
    raw["relations"].append(
        {
            "kind": "depends_on",
            "source": {"id": "assess"},
            "target": {"session": {"source": "pi", "id": "other"}, "id": "missing"},
        }
    )
    job = JobView.model_validate(raw)
    assert len(inspect(job).issues) == 2
    raw["nodes"][2]["nodes"][1].pop("session")
    with pytest.raises(ValidationError, match="missing model call"):
        JobView.model_validate(raw)


def test_shared_call_is_not_reidentified_or_cost_allocated_per_job():
    first = JobView.model_validate(fixture("job-input.json"))
    second = first.model_copy(deep=True)
    second.job.id = "input-b"
    a, b = first.nodes[2], second.nodes[2]
    assert isinstance(a, Operation) and isinstance(b, Operation)
    assert a.nodes == b.nodes
    assert a.nodes[0].id == "m1"
    assert a.nodes[0].session == b.nodes[0].session
    assert "usage" not in first.model_dump()


def test_explicit_null_goal_and_result_are_distinct_from_absence():
    raw = {"job": {"source": "test", "id": "task"}, "nodes": []}
    assert "result" not in JobView.model_validate(raw).model_dump()
    assert (
        JobView.model_validate({**raw, "goal": None, "result": None}).model_dump()["result"] is None
    )
