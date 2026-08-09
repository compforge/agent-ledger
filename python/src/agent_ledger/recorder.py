from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any, Literal, Self
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from agent_ledger.models import (
    Actor,
    CommitReceipt,
    EventStream,
    EventType,
    ProposedEvent,
    StoredEvent,
)
from agent_ledger.store import EventStore


class AttemptHandle(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["model", "tool"]
    step_id: str = Field(min_length=1)
    attempt_id: str = Field(min_length=1)
    requested_event_id: str = Field(min_length=1)


class SessionRecorder:
    """Framework-facing recorder that serializes writes to one execution stream."""

    def __init__(
        self,
        *,
        store: EventStore,
        session_id: str,
        run_id: str,
        actor: Actor,
        expected_version: int = -1,
        parent_run_id: str | None = None,
        caused_by_event_id: str | None = None,
    ) -> None:
        if (parent_run_id is None) != (caused_by_event_id is None):
            raise ValueError("parent_run_id and caused_by_event_id must be set together")
        self.store = store
        self.run_id = run_id
        self.stream = EventStream(session_id=session_id, stream_id=run_id)
        self.actor = actor
        self.expected_version = expected_version
        self.parent_run_id = parent_run_id
        self.caused_by_event_id = caused_by_event_id
        self._append_lock = asyncio.Lock()

    @classmethod
    async def resume(
        cls,
        *,
        store: EventStore,
        session_id: str,
        run_id: str,
        actor: Actor,
    ) -> Self:
        stream = EventStream(session_id=session_id, stream_id=run_id)
        expected_version = -1
        parent_run_id: str | None = None
        caused_by_event_id: str | None = None
        async for event in store.read_stream(stream):
            expected_version = event.stream_version
            if event.parent_run_id is not None:
                parent_run_id = event.parent_run_id
                caused_by_event_id = event.caused_by_event_id
        return cls(
            store=store,
            session_id=session_id,
            run_id=run_id,
            actor=actor,
            expected_version=expected_version,
            parent_run_id=parent_run_id,
            caused_by_event_id=caused_by_event_id,
        )

    async def append(
        self,
        events: Sequence[ProposedEvent],
        *,
        append_id: str | None = None,
    ) -> CommitReceipt:
        async with self._append_lock:
            receipt = await self.store.append(
                self.stream,
                self.expected_version,
                append_id or str(uuid4()),
                events,
            )
            self.expected_version = receipt.last_version
            return receipt

    async def record(
        self,
        event_type: str,
        *,
        payload: dict[str, Any] | None = None,
        step_id: str | None = None,
        attempt_id: str | None = None,
        extensions: dict[str, Any] | None = None,
        append_id: str | None = None,
    ) -> StoredEvent:
        event = ProposedEvent(
            event_type=event_type,
            session_id=self.stream.session_id,
            run_id=self.run_id,
            actor=self.actor,
            step_id=step_id,
            attempt_id=attempt_id,
            payload=payload or {},
            extensions=extensions or {},
        )
        receipt = await self.append([event], append_id=append_id)
        return StoredEvent.from_proposed(
            event,
            stream_id=self.stream.stream_id,
            stream_version=receipt.first_version,
            commit_cursor=receipt.first_cursor,
            committed_at=receipt.committed_at,
        )

    async def start_run(self, *, payload: dict[str, Any] | None = None) -> StoredEvent:
        event = ProposedEvent(
            event_type=EventType.RUN_STARTED,
            session_id=self.stream.session_id,
            run_id=self.run_id,
            actor=self.actor,
            parent_run_id=self.parent_run_id,
            caused_by_event_id=self.caused_by_event_id,
            payload=payload or {},
        )
        receipt = await self.append([event])
        return StoredEvent.from_proposed(
            event,
            stream_id=self.stream.stream_id,
            stream_version=receipt.first_version,
            commit_cursor=receipt.first_cursor,
            committed_at=receipt.committed_at,
        )

    async def complete_run(self, *, payload: dict[str, Any] | None = None) -> StoredEvent:
        return await self.record(EventType.RUN_COMPLETED, payload=payload)

    async def fail_run(
        self,
        error: BaseException,
        *,
        payload: dict[str, Any] | None = None,
    ) -> StoredEvent:
        return await self.record(EventType.RUN_FAILED, payload=_error_payload(error, payload))

    async def start_step(
        self,
        step_id: str,
        *,
        payload: dict[str, Any] | None = None,
    ) -> StoredEvent:
        return await self.record(EventType.STEP_STARTED, step_id=step_id, payload=payload)

    async def complete_step(
        self,
        step_id: str,
        *,
        payload: dict[str, Any] | None = None,
    ) -> StoredEvent:
        return await self.record(EventType.STEP_COMPLETED, step_id=step_id, payload=payload)

    async def fail_step(
        self,
        step_id: str,
        error: BaseException,
        *,
        payload: dict[str, Any] | None = None,
    ) -> StoredEvent:
        return await self.record(
            EventType.STEP_FAILED,
            step_id=step_id,
            payload=_error_payload(error, payload),
        )

    async def before_model_call(
        self,
        step_id: str,
        *,
        payload: dict[str, Any],
        attempt_id: str | None = None,
    ) -> AttemptHandle:
        attempt_id = attempt_id or str(uuid4())
        requested = await self.record(
            EventType.MODEL_REQUESTED,
            step_id=step_id,
            attempt_id=attempt_id,
            payload=payload,
        )
        return AttemptHandle(
            kind="model",
            step_id=step_id,
            attempt_id=attempt_id,
            requested_event_id=requested.event_id,
        )

    async def model_completed(
        self,
        attempt: AttemptHandle,
        *,
        payload: dict[str, Any],
    ) -> StoredEvent:
        _require_kind(attempt, "model")
        return await self.record(
            EventType.MODEL_COMPLETED,
            step_id=attempt.step_id,
            attempt_id=attempt.attempt_id,
            payload=payload,
        )

    async def model_failed(
        self,
        attempt: AttemptHandle,
        error: BaseException,
        *,
        payload: dict[str, Any] | None = None,
    ) -> StoredEvent:
        _require_kind(attempt, "model")
        return await self.record(
            EventType.MODEL_FAILED,
            step_id=attempt.step_id,
            attempt_id=attempt.attempt_id,
            payload=_error_payload(error, payload),
        )

    async def before_tool_call(
        self,
        step_id: str,
        *,
        payload: dict[str, Any],
        attempt_id: str | None = None,
    ) -> AttemptHandle:
        attempt_id = attempt_id or str(uuid4())
        requested = await self.record(
            EventType.TOOL_REQUESTED,
            step_id=step_id,
            attempt_id=attempt_id,
            payload=payload,
        )
        return AttemptHandle(
            kind="tool",
            step_id=step_id,
            attempt_id=attempt_id,
            requested_event_id=requested.event_id,
        )

    async def tool_completed(
        self,
        attempt: AttemptHandle,
        *,
        payload: dict[str, Any],
    ) -> StoredEvent:
        _require_kind(attempt, "tool")
        return await self.record(
            EventType.TOOL_COMPLETED,
            step_id=attempt.step_id,
            attempt_id=attempt.attempt_id,
            payload=payload,
        )

    async def tool_failed(
        self,
        attempt: AttemptHandle,
        error: BaseException,
        *,
        payload: dict[str, Any] | None = None,
    ) -> StoredEvent:
        _require_kind(attempt, "tool")
        return await self.record(
            EventType.TOOL_FAILED,
            step_id=attempt.step_id,
            attempt_id=attempt.attempt_id,
            payload=_error_payload(error, payload),
        )

    async def save_snapshot(
        self,
        profile: str,
        snapshot: dict[str, Any],
        *,
        profile_version: str = "1",
    ) -> StoredEvent:
        return await self.record(
            EventType.FRAMEWORK_SNAPSHOT_SAVED,
            payload={
                "profile": profile,
                "profile_version": profile_version,
                "snapshot": snapshot,
            },
        )

    async def link_checkpoint(
        self,
        profile: str,
        checkpoint_id: str,
        *,
        profile_version: str = "1",
        metadata: dict[str, Any] | None = None,
    ) -> StoredEvent:
        return await self.record(
            EventType.FRAMEWORK_CHECKPOINT_LINKED,
            payload={
                "profile": profile,
                "profile_version": profile_version,
                "checkpoint_id": checkpoint_id,
                "metadata": metadata or {},
            },
        )

    def child(
        self,
        *,
        run_id: str,
        actor: Actor,
        caused_by_event_id: str,
    ) -> SessionRecorder:
        return SessionRecorder(
            store=self.store,
            session_id=self.stream.session_id,
            run_id=run_id,
            actor=actor,
            parent_run_id=self.run_id,
            caused_by_event_id=caused_by_event_id,
        )


def _require_kind(attempt: AttemptHandle, expected: Literal["model", "tool"]) -> None:
    if attempt.kind != expected:
        raise ValueError(f"expected a {expected} attempt, got {attempt.kind}")


def _error_payload(
    error: BaseException,
    payload: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        **(payload or {}),
        "error_type": type(error).__name__,
        "error": str(error),
    }
