import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from agent_ledger.models import Checkpoint, ProposedEvent, canonical_append_digest


def test_event_schema_and_digest_match_cross_language_vector() -> None:
    root = Path(__file__).parents[2]
    vector: dict[str, Any] = json.loads(
        (root / "conformance/vectors/append.json").read_text(encoding="utf-8")
    )
    schema = json.loads((root / "spec/schemas/event.schema.json").read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    for raw_event in vector["events"]:
        validator.validate(raw_event)
    events = tuple(ProposedEvent.model_validate(event) for event in vector["events"])
    assert canonical_append_digest(events) == vector["sha256"]


def test_checkpoint_schema_accepts_inline_state() -> None:
    root = Path(__file__).parents[2]
    schema = json.loads((root / "spec/schemas/checkpoint.schema.json").read_text())
    checkpoint = {
        "schema_version": "1.0",
        "id": "018f0f43-7b9a-7cc1-8000-000000000001",
        "checkpoint_key": "native-session",
        "revision": 1,
        "actor_id": "018f0f43-7b9a-7cc1-8000-000000000002",
        "format": "application/vnd.compforge.test.state+json;version=1",
        "state": {"messages": []},
        "extensions": {},
        "created_at": "2026-08-20T00:00:00Z",
    }
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(checkpoint)
    Checkpoint.model_validate(checkpoint)
