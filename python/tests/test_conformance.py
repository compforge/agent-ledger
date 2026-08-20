import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from agent_ledger.models import ProposedEvent, canonical_append_digest


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
