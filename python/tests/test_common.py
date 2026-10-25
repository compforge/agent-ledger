import json
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import Field

from agent_ledger.common import CallRequest, CallResponse, Fact, call_details
from agent_ledger.view.models import Input, Operation


def test_common_roundtrip_keeps_null_zero_and_source_details():
    vector = json.loads(
        (Path(__file__).parents[2] / "conformance/vectors/common-call.json").read_text()
    )
    for name, model in [("request", CallRequest), ("response", CallResponse)]:
        payload = vector[name] | vector["details"]
        facts = model.from_payload(payload)
        assert facts.model_dump(mode="json", exclude_unset=True) == vector[name]
        assert call_details(payload) == vector["details"]


def test_null_is_declared_by_field_not_name():
    class Sample(Fact):
        content: str | None = None
        observation: object = Field(default=None, json_schema_extra={"preserve_null": True})

    assert Sample(content=None, observation=None).model_dump() == {"observation": None}
    assert Sample().model_dump() == {}
    assert Input(id="input", content=None).model_dump()["content"] is None
    assert Operation(id="op", operation="vendor", data=None).model_dump()["data"] is None
    assert "data" not in Operation(id="op", operation="vendor").model_dump()


def test_ledger_does_not_import_view_or_projection():
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, agent_ledger; "
            "assert 'agent_ledger.view' not in sys.modules; "
            "assert 'agent_ledger.projections.view' not in sys.modules",
        ],
        check=True,
    )


@pytest.mark.parametrize(
    "model,payload",
    [
        (CallRequest, {"input": None, "input_artifact_id": "artifact"}),
        (CallResponse, {"output": None, "output_artifact_id": "artifact"}),
        (CallResponse, {"usage": {"input_tokens": -1}}),
    ],
)
def test_invalid_facts_fail_at_common_boundary(model, payload):
    with pytest.raises(ValueError):
        model.from_payload(payload)
