from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from agent_ledger import Actor, MemoryArtifactStore, ProposedEvent, StoredEvent
from agent_ledger.frameworks.plain_loop import PlainLoopProfile
from agent_ledger.models import canonical_append_digest, utc_now


def test_event_matches_normative_json_schema() -> None:
    event = ProposedEvent(
        event_type="test.recorded",
        session_id="session",
        run_id="run",
        actor=Actor(type="agent", id="test"),
    )
    schema_path = Path(__file__).parents[2] / "spec" / "schemas" / "event.schema.json"
    schema = json.loads(schema_path.read_text())
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    validator.validate(event.model_dump(mode="json", exclude_none=True))
    stored = StoredEvent.from_proposed(
        event,
        stream_id="run",
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


def test_append_digest_matches_cross_language_vector() -> None:
    vector_path = Path(__file__).parents[2] / "conformance" / "vectors" / "append.json"
    vector = json.loads(vector_path.read_text())
    events = [ProposedEvent.model_validate(event) for event in vector["events"]]

    assert canonical_append_digest(events) == vector["sha256"]


def test_adapter_descriptor_matches_normative_schema() -> None:
    schema_path = Path(__file__).parents[2] / "spec" / "schemas" / "adapter.schema.json"
    schema = json.loads(schema_path.read_text())
    Draft202012Validator(schema).validate(PlainLoopProfile.descriptor.model_dump(mode="json"))
