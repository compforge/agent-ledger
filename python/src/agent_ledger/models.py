from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal, Self
from uuid import uuid4

import rfc8785
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SCHEMA_VERSION: Literal["1.0"] = "1.0"


def utc_now() -> datetime:
    return datetime.now(UTC)


class EventType(StrEnum):
    SESSION_STARTED = "session.started"
    SESSION_COMPLETED = "session.completed"
    RUN_STARTED = "run.started"
    RUN_COMPLETED = "run.completed"
    RUN_FAILED = "run.failed"
    RUN_CANCELLED = "run.cancelled"
    STEP_STARTED = "step.started"
    STEP_COMPLETED = "step.completed"
    STEP_FAILED = "step.failed"
    MODEL_REQUESTED = "model.requested"
    MODEL_COMPLETED = "model.completed"
    MODEL_FAILED = "model.failed"
    TOOL_REQUESTED = "tool.requested"
    TOOL_COMPLETED = "tool.completed"
    TOOL_FAILED = "tool.failed"
    FRAMEWORK_SNAPSHOT_SAVED = "framework.snapshot.saved"
    FRAMEWORK_CHECKPOINT_LINKED = "framework.checkpoint.linked"


class Actor(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    type: Literal["agent", "orchestrator", "tool", "human", "system"]
    id: str = Field(min_length=1)
    framework: str | None = Field(default=None, min_length=1)


class ArtifactRef(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    uri: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size: int = Field(ge=0)
    content_type: str = Field(min_length=1)


class EventStream(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    session_id: str = Field(min_length=1)
    stream_id: str = Field(min_length=1)


class ProposedEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal["1.0"] = SCHEMA_VERSION
    event_id: str = Field(default_factory=lambda: str(uuid4()), min_length=1)
    event_type: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    actor: Actor
    occurred_at: datetime = Field(default_factory=utc_now)
    step_id: str | None = Field(default=None, min_length=1)
    attempt_id: str | None = Field(default=None, min_length=1)
    parent_run_id: str | None = Field(default=None, min_length=1)
    caused_by_event_id: str | None = Field(default=None, min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    extensions: dict[str, Any] = Field(default_factory=dict)

    @field_validator("occurred_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone")
        return value

    @model_validator(mode="after")
    def validate_causal_parent(self) -> Self:
        if (self.parent_run_id is None) != (self.caused_by_event_id is None):
            raise ValueError("parent_run_id and caused_by_event_id must be set together")
        return self


class StoredEvent(ProposedEvent):
    stream_id: str = Field(min_length=1)
    stream_version: int = Field(ge=0)
    commit_cursor: str = Field(min_length=1)
    committed_at: datetime

    @field_validator("committed_at")
    @classmethod
    def require_commit_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("committed_at must include a timezone")
        return value

    @classmethod
    def from_proposed(
        cls,
        event: ProposedEvent,
        *,
        stream_id: str,
        stream_version: int,
        commit_cursor: str,
        committed_at: datetime,
    ) -> StoredEvent:
        return cls.model_validate(
            {
                **event.model_dump(),
                "stream_id": stream_id,
                "stream_version": stream_version,
                "commit_cursor": commit_cursor,
                "committed_at": committed_at,
            }
        )


class CommitReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    stream: EventStream
    append_id: str = Field(min_length=1)
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    first_version: int = Field(ge=0)
    last_version: int = Field(ge=0)
    first_cursor: str = Field(min_length=1)
    last_cursor: str = Field(min_length=1)
    event_ids: tuple[str, ...] = Field(min_length=1)
    committed_at: datetime

    @field_validator("committed_at")
    @classmethod
    def require_commit_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("committed_at must include a timezone")
        return value


def canonical_append_digest(events: list[ProposedEvent] | tuple[ProposedEvent, ...]) -> str:
    body = [event.model_dump(mode="json", exclude_none=True) for event in events]
    canonical = rfc8785.dumps(body)
    return hashlib.sha256(canonical).hexdigest()
