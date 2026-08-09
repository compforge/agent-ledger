from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from agent_ledger import Actor, MemoryArtifactStore, ProposedEvent, StoredEvent
from agent_ledger.models import utc_now


def test_event_matches_normative_json_schema() -> None:
    event = ProposedEvent(
        event_type="test.recorded",
        session_id="session",
        run_id="run",
        actor=Actor(type="agent", id="test"),
    )
    schema_path = Path(__file__).parents[1] / "spec" / "schemas" / "event.schema.json"
    schema = json.loads(schema_path.read_text())
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    validator.validate(event.model_dump(mode="json", exclude_none=True))
    stored = StoredEvent.from_proposed(
        event,
        stream_version=0,
        commit_cursor="0",
        committed_at=utc_now(),
    )
    validator.validate(stored.model_dump(mode="json", exclude_none=True))


async def test_memory_artifact_round_trip() -> None:
    store = MemoryArtifactStore()
    ref = await store.put("session", b"large model output", "text/plain")

    assert ref.size == 18
    assert await store.get(ref) == b"large model output"
