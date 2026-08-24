import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError as JSONSchemaValidationError

from agent_ledger.models import (
    ActionType,
    Artifact,
    Checkpoint,
    EffectKind,
    EventType,
    Idempotency,
    ProposedEvent,
    canonical_append_digest,
)


def test_artifact_schema_requires_an_explicit_version() -> None:
    root = Path(__file__).parents[2]
    schema = json.loads((root / "spec/schemas/artifact.schema.json").read_text())
    artifact = Artifact(
        key="model/request",
        version="v1",
        uri="s3://artifacts/model-request-v1",
        sha256="0" * 64,
        size=42,
        content_type="application/json",
    )
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(
        artifact.model_dump(mode="json")
    )


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
        "key": "native-session",
        "revision": 1,
        "actor_id": "018f0f43-7b9a-7cc1-8000-000000000002",
        "format": "application/vnd.compforge.test.state+json;version=1",
        "state": {"messages": []},
        "extensions": {},
        "created_at": "2026-08-20T00:00:00Z",
    }
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(checkpoint)
    Checkpoint.model_validate(checkpoint)


def test_core_attempt_payload_profiles() -> None:
    root = Path(__file__).parents[2]
    schema = json.loads(
        (root / "spec/schemas/call-payload.schema.json").read_text(encoding="utf-8")
    )

    examples = {
        "model_call_attempt_requested": {
            "model": {"id": "claude-sonnet", "provider": "anthropic"},
            "input": [{"role": "user", "content": "hello"}],
            "client_request_id": "request-1",
        },
        "model_call_completed": {
            "output": {"role": "assistant", "content": "hello"},
            "finish_reason": "end_turn",
            "usage": {"input_tokens": 10, "output_tokens": 4},
            "provider_request_id": "provider-request-1",
        },
        "model_call_failed": {
            "error": {"type": "rate_limit", "message": "limited", "retryable": True}
        },
        "tool_call_attempt_requested": {
            "tool_name": "write_file",
            "input": {"path": "README.md"},
            "idempotency_key": "write-file-1",
        },
        "tool_call_completed": {
            "output": {"written": True},
            "external_operation_id": "operation-1",
        },
        "tool_call_failed": {"error": {"type": "permission_denied", "message": "denied"}},
        "attempt_cancelled": {"reason": "user_interrupt"},
        "attempt_outcome_unknown": {
            "reason": "worker_lost",
            "superseded_by_attempt_id": "018f0f43-7b9a-7cc1-8000-000000000001",
        },
    }
    for profile, payload in examples.items():
        profile_schema = {
            "$schema": schema["$schema"],
            "$defs": schema["$defs"],
            "$ref": f"#/$defs/{profile}",
        }
        Draft202012Validator(profile_schema).validate(payload)

    model_completed = {
        "$schema": schema["$schema"],
        "$defs": schema["$defs"],
        "$ref": "#/$defs/model_call_completed",
    }
    with pytest.raises(JSONSchemaValidationError):
        Draft202012Validator(model_completed).validate({"message": "legacy alias"})

    tool_requested = {
        "$schema": schema["$schema"],
        "$defs": schema["$defs"],
        "$ref": "#/$defs/tool_call_attempt_requested",
    }
    with pytest.raises(JSONSchemaValidationError):
        Draft202012Validator(tool_requested).validate(
            {
                "tool_call_id": "tool-1",
                "tool_name": "write_file",
                "input": {"path": "README.md"},
            }
        )


def test_core_vocabulary_matches_cross_language_registry() -> None:
    root = Path(__file__).parents[2]
    vocabulary = json.loads((root / "spec/vocabulary.json").read_text(encoding="utf-8"))

    assert [item.value for item in ActionType] == vocabulary["action_types"]
    assert [item.value for item in EffectKind] == vocabulary["effect_kinds"]
    assert [item.value for item in Idempotency] == vocabulary["idempotency"]
    assert [item.value for item in EventType] == vocabulary["event_types"]
