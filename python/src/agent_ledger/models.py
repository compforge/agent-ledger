from __future__ import annotations

import hashlib
import secrets
import time
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID

import rfc8785
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SCHEMA_VERSION: Literal["1.0"] = "1.0"
UUID7_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"


def utc_now() -> datetime:
    return datetime.now(UTC)


def new_id() -> str:
    """Return an RFC 9562 UUIDv7 without requiring a runtime-specific UUID package."""
    timestamp_ms = int(time.time() * 1000) & ((1 << 48) - 1)
    value = timestamp_ms << 80
    value |= 0x7 << 76
    value |= secrets.randbits(12) << 64
    value |= 0b10 << 62
    value |= secrets.randbits(62)
    return str(UUID(int=value))


class ActionType(StrEnum):
    MODEL_CALL = "model_call"
    TOOL_CALL = "tool_call"
    COMPACT = "compact"
    CHECKPOINT = "checkpoint"


class EventType(StrEnum):
    SESSION_STARTED = "session.started"
    SESSION_COMPLETED = "session.completed"
    RUN_STARTED = "run.started"
    RUN_COMPLETED = "run.completed"
    RUN_FAILED = "run.failed"
    RUN_CANCELLED = "run.cancelled"
    LANE_CREATED = "lane.created"
    TURN_STARTED = "turn.started"
    TURN_COMPLETED = "turn.completed"
    TURN_FAILED = "turn.failed"
    ACTION_STARTED = "action.started"
    ACTION_COMPLETED = "action.completed"
    ACTION_FAILED = "action.failed"
    ATTEMPT_REQUESTED = "attempt.requested"
    ATTEMPT_COMPLETED = "attempt.completed"
    ATTEMPT_FAILED = "attempt.failed"
    LANE_FRAMEWORK_SNAPSHOT_SAVED = "lane.framework.snapshot.saved"
    LANE_FRAMEWORK_CHECKPOINT_LINKED = "lane.framework.checkpoint.linked"


class Actor(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str = Field(default_factory=new_id, pattern=UUID7_PATTERN)
    type: str = Field(min_length=1)
    framework: str | None = Field(default=None, min_length=1)
    created_at: datetime = Field(default_factory=utc_now)

    @field_validator("created_at")
    @classmethod
    def require_created_timezone(cls, value: datetime) -> datetime:
        return _require_timezone("created_at", value)


class ArtifactRef(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    uri: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size: int = Field(ge=0)
    content_type: str = Field(min_length=1)


class CheckpointAnchor(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    lane_id: str = Field(pattern=UUID7_PATTERN)
    last_applied_seq: int = Field(ge=1)
    last_applied_event_id: str = Field(pattern=UUID7_PATTERN)


class ProposedCheckpoint(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal["1.0"] = SCHEMA_VERSION
    id: str = Field(default_factory=new_id, pattern=UUID7_PATTERN)
    checkpoint_key: str = Field(min_length=1)
    actor_id: str = Field(pattern=UUID7_PATTERN)
    format: str = Field(min_length=1)
    state: dict[str, Any] | None = None
    artifact_ref: ArtifactRef | None = None
    anchor: CheckpointAnchor | None = None
    extensions: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def require_one_state_source(self) -> ProposedCheckpoint:
        if (self.state is None) == (self.artifact_ref is None):
            raise ValueError("exactly one of state and artifact_ref must be set")
        return self


class Checkpoint(ProposedCheckpoint):
    revision: int = Field(ge=1)
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def require_created_timezone(cls, value: datetime) -> datetime:
        return _require_timezone("created_at", value)

    @classmethod
    def from_proposed(
        cls,
        value: ProposedCheckpoint,
        *,
        revision: int,
        created_at: datetime,
    ) -> Checkpoint:
        return cls.model_validate(
            {**value.model_dump(), "revision": revision, "created_at": created_at}
        )


class Lane(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str = Field(default_factory=new_id, pattern=UUID7_PATTERN)
    session_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    name: str = Field(default="main", min_length=1)
    parent_lane_id: str | None = Field(default=None, pattern=UUID7_PATTERN)
    last_seq: int = Field(default=0, ge=0)
    created_at: datetime = Field(default_factory=utc_now)

    @field_validator("created_at")
    @classmethod
    def require_created_timezone(cls, value: datetime) -> datetime:
        return _require_timezone("created_at", value)


class Turn(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str = Field(default_factory=new_id, pattern=UUID7_PATTERN)
    lane_id: str = Field(pattern=UUID7_PATTERN)
    created_at: datetime = Field(default_factory=utc_now)

    @field_validator("created_at")
    @classmethod
    def require_created_timezone(cls, value: datetime) -> datetime:
        return _require_timezone("created_at", value)


class Action(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str = Field(default_factory=new_id, pattern=UUID7_PATTERN)
    turn_id: str = Field(pattern=UUID7_PATTERN)
    type: str = Field(min_length=1)
    parent_action_id: str | None = Field(default=None, pattern=UUID7_PATTERN)
    created_at: datetime = Field(default_factory=utc_now)

    @field_validator("created_at")
    @classmethod
    def require_created_timezone(cls, value: datetime) -> datetime:
        return _require_timezone("created_at", value)


class Attempt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str = Field(default_factory=new_id, pattern=UUID7_PATTERN)
    action_id: str = Field(pattern=UUID7_PATTERN)
    attempt_no: int = Field(ge=1)
    created_at: datetime = Field(default_factory=utc_now)

    @field_validator("created_at")
    @classmethod
    def require_created_timezone(cls, value: datetime) -> datetime:
        return _require_timezone("created_at", value)


class ProposedEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal["1.0"] = SCHEMA_VERSION
    id: str = Field(default_factory=new_id, pattern=UUID7_PATTERN)
    lane_id: str = Field(pattern=UUID7_PATTERN)
    subject_id: str = Field(min_length=1)
    event_type: str = Field(pattern=r"^(session|run|lane|turn|action|attempt)\..+$")
    actor_id: str = Field(pattern=UUID7_PATTERN)
    causation_id: str | None = Field(default=None, pattern=UUID7_PATTERN)
    occurred_at: datetime = Field(default_factory=utc_now)
    payload: dict[str, Any] = Field(default_factory=dict)
    extensions: dict[str, Any] = Field(default_factory=dict)

    @field_validator("occurred_at")
    @classmethod
    def require_occurred_timezone(cls, value: datetime) -> datetime:
        return _require_timezone("occurred_at", value)

    @property
    def subject_kind(self) -> str:
        return self.event_type.partition(".")[0]


class StoredEvent(ProposedEvent):
    seq: int = Field(ge=1)
    committed_at: datetime

    @field_validator("committed_at")
    @classmethod
    def require_commit_timezone(cls, value: datetime) -> datetime:
        return _require_timezone("committed_at", value)

    @classmethod
    def from_proposed(
        cls,
        event: ProposedEvent,
        *,
        seq: int,
        committed_at: datetime,
    ) -> StoredEvent:
        return cls.model_validate({**event.model_dump(), "seq": seq, "committed_at": committed_at})


class AppendReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str = Field(pattern=UUID7_PATTERN)
    lane_id: str = Field(pattern=UUID7_PATTERN)
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    first_seq: int = Field(ge=1)
    last_seq: int = Field(ge=1)
    event_ids: tuple[str, ...] = Field(min_length=1)
    committed_at: datetime

    @field_validator("committed_at")
    @classmethod
    def require_commit_timezone(cls, value: datetime) -> datetime:
        return _require_timezone("committed_at", value)


class SessionView(BaseModel):
    """Read-side snapshot; Session identity remains owned by the upstream orchestrator."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    session_id: str = Field(min_length=1)
    actors: tuple[Actor, ...] = ()
    lanes: tuple[Lane, ...] = ()
    turns: tuple[Turn, ...] = ()
    actions: tuple[Action, ...] = ()
    attempts: tuple[Attempt, ...] = ()
    events: tuple[StoredEvent, ...] = ()


def canonical_append_digest(events: list[ProposedEvent] | tuple[ProposedEvent, ...]) -> str:
    body = [event.model_dump(mode="json", exclude_none=True) for event in events]
    return hashlib.sha256(rfc8785.dumps(body)).hexdigest()


def canonical_checkpoint_digest(checkpoint: ProposedCheckpoint) -> str:
    body = checkpoint.model_dump(mode="json", exclude_none=True)
    return hashlib.sha256(rfc8785.dumps(body)).hexdigest()


def _require_timezone(field: str, value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return value
