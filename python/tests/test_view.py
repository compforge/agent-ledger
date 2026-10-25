import copy
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError
from referencing import Registry, Resource

from agent_ledger.common import CallRequest, CallResponse, Model, Usage
from agent_ledger.models import RunView
from agent_ledger.projection import project_view
from agent_ledger.view import Document, ModelCall, ToolCall, Turn

ROOT = Path(__file__).parents[2]


def fixture(name: str) -> dict:
    return json.loads((ROOT / "conformance/vectors" / name).read_text())


def schema_validator() -> Draft202012Validator:
    schemas = [
        json.loads((ROOT / "spec/schemas" / name).read_text())
        for name in ("common.schema.json", "view.schema.json")
    ]
    registry = Registry().with_resources((s["$id"], Resource.from_contents(s)) for s in schemas)
    return Draft202012Validator(schemas[1], registry=registry, format_checker=FormatChecker())


def test_native_view_round_trip_and_schema() -> None:
    raw = fixture("view.json")
    schema_validator().validate(raw)
    document = Document.model_validate(raw)
    assert document.model_dump(mode="json", exclude_unset=True) == raw
    turn = document.runs[0].items[1]
    assert isinstance(turn, Turn)
    model, tool = turn.items[:2]
    assert isinstance(model, ModelCall) and isinstance(tool, ToolCall)
    assert model.request is not None and tool.request is not None
    assert [m["role"] for m in model.request.input["messages"]] == [
        "system",
        "developer",
        "user",
        "user",
    ]
    assert tool.request.input is None


@pytest.mark.parametrize("change", ["duplicate", "negative_usage", "missing_model", "reverse_time"])
def test_invalid_view(change: str) -> None:
    raw = fixture("view.json")
    items = raw["runs"][0]["items"][1]["items"]
    if change == "duplicate":
        items[1]["id"] = items[0]["id"]
    elif change == "negative_usage":
        items[0]["response"]["usage"]["input_tokens"] = -1
    elif change == "missing_model":
        items[1]["model_call_id"] = "absent"
    else:
        items[1].update(started_at="2026-10-25T01:00:00Z", finished_at="2026-10-25T00:00:00Z")
    with pytest.raises(ValidationError):
        Document.model_validate(raw)


def test_ledger_projection_preserves_calls_parallelism_and_gaps() -> None:
    data = fixture("view-ledger.json")
    source = RunView.model_validate(data["source"])
    before = source.model_copy(deep=True)
    document = project_view(source)
    turns = [item for item in document.runs[0].items if isinstance(item, Turn)]
    calls = [
        item for turn in turns for item in turn.items if isinstance(item, (ModelCall, ToolCall))
    ]
    assert [call.id for call in calls] == data["call_ids"]
    assert [call.status for call in calls] == data["statuses"]
    assert turns[0].lane_id != turns[1].lane_id
    assert sum("result missing" in issue for issue in document.issues) == 1
    assert calls[0].response is not None and calls[0].response.usage is not None
    assert calls[0].response.usage.input_tokens == 8
    assert calls[0].request is not None
    calls[0].request.input["messages"][0]["content"] = "changed"
    assert source == before
    schema_validator().validate(document.model_dump(mode="json"))


def test_conflicting_terminals_remain_visible() -> None:
    data = fixture("view-ledger.json")
    event = copy.deepcopy(
        next(e for e in data["source"]["events"] if e["event_type"] == "attempt.completed")
    )
    event.update(
        id="018f0f43-7b9a-7cc1-8000-000000000099",
        seq=99,
        event_type="attempt.failed",
        payload={"error": {"type": "late", "message": "conflict"}},
    )
    data["source"]["events"].append(event)
    document = project_view(RunView.model_validate(data["source"]))
    turn = next(item for item in document.runs[0].items if isinstance(item, Turn))
    call = next(item for item in turn.items if item.id == data["call_ids"][1])
    assert isinstance(call, ModelCall)
    assert call.status == "unknown" and call.response is None
    assert len(call.attributes["terminal_observations"]) == 2


@pytest.mark.asyncio
async def test_recorder_accepts_shared_call_types() -> None:
    from agent_ledger import Actor, LaneRecorder
    from agent_ledger.stores.memory import MemoryEventStore

    store = MemoryEventStore()
    recorder = await LaneRecorder.open(
        store=store, session_id="shared", run_id="run", actor=Actor(type="test")
    )
    turn = await recorder.start_turn()
    handle = await recorder.before_model_call(
        turn,
        payload=CallRequest(
            input={"messages": [{"role": "user", "content": "hi"}]}, model=Model(id="m")
        ),
    )
    await recorder.model_completed(
        handle, payload=CallResponse(output=None, usage=Usage(input_tokens=0))
    )
    result = project_view(await store.load_run("shared", "run"))
    call = next(
        item
        for t in result.runs[0].items
        if isinstance(t, Turn)
        for item in t.items
        if isinstance(item, ModelCall)
    )
    assert call.response is not None
    assert "output" in call.response.model_fields_set
    assert call.response.usage is not None and call.response.usage.input_tokens == 0


def test_null_and_absent_content_are_distinct_and_artifacts_are_exclusive() -> None:
    assert CallResponse(output=None).model_dump(mode="json") == {"output": None}
    assert CallResponse().model_dump(mode="json") == {}
    with pytest.raises(ValidationError, match="both input"):
        CallRequest(input=None, input_artifact_id="body")
