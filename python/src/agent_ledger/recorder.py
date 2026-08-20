from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any, Literal, Self, cast

from pydantic import BaseModel, ConfigDict, Field

from agent_ledger.errors import EntityConflict, EntityNotFound
from agent_ledger.models import (
    Action,
    Actor,
    AppendReceipt,
    Attempt,
    EventType,
    Lane,
    ProposedEvent,
    StoredEvent,
    Turn,
    new_id,
)
from agent_ledger.store import EventStore


class AttemptHandle(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    action_type: Literal["model_call", "tool_call"]
    turn_id: str = Field(min_length=1)
    action_id: str = Field(min_length=1)
    attempt_id: str = Field(min_length=1)
    attempt_no: int = Field(ge=1)
    requested_event_id: str = Field(min_length=1)


class LaneRecorder:
    """Framework-facing recorder that serializes writes to one Lane."""

    def __init__(
        self,
        *,
        store: EventStore,
        lane: Lane,
        actor: Actor,
        parent_run_id: str | None = None,
        run_causation_id: str | None = None,
    ) -> None:
        self.store = store
        self.lane = lane
        self.actor = actor
        self.parent_run_id = parent_run_id
        self.run_causation_id = run_causation_id
        self.expected_last_seq = lane.last_seq
        self._append_lock = asyncio.Lock()

    @classmethod
    async def open(
        cls,
        *,
        store: EventStore,
        session_id: str,
        run_id: str,
        actor: Actor,
        lane_id: str | None = None,
        lane_name: str = "main",
        parent_lane_id: str | None = None,
        parent_run_id: str | None = None,
        run_causation_id: str | None = None,
    ) -> Self:
        lane: Lane | None
        if lane_id is not None:
            lane = await store.get_lane(lane_id)
            if lane is None:
                lane = Lane(
                    id=lane_id,
                    session_id=session_id,
                    run_id=run_id,
                    name=lane_name,
                    parent_lane_id=parent_lane_id,
                )
                await store.create_lane(lane)
        else:
            lane = await store.find_lane(session_id, run_id, lane_name)
            if lane is None:
                candidate = Lane(
                    session_id=session_id,
                    run_id=run_id,
                    name=lane_name,
                    parent_lane_id=parent_lane_id,
                )
                try:
                    await store.create_lane(candidate)
                    lane = candidate
                except EntityConflict:
                    lane = await store.find_lane(session_id, run_id, lane_name)
                    if lane is None:
                        raise
        if lane.session_id != session_id or lane.run_id != run_id or lane.name != lane_name:
            raise ValueError("lane identity does not match recorder options")
        if parent_lane_id is not None and lane.parent_lane_id != parent_lane_id:
            raise ValueError("lane parent does not match recorder options")
        stored_actor = await store.get_actor(actor.id)
        if stored_actor is None:
            await store.create_actor(actor)
        elif stored_actor.type != actor.type or stored_actor.framework != actor.framework:
            raise EntityConflict("actor", actor.id)
        return cls(
            store=store,
            lane=lane,
            actor=actor,
            parent_run_id=parent_run_id,
            run_causation_id=run_causation_id,
        )

    @property
    def session_id(self) -> str:
        return self.lane.session_id

    @property
    def run_id(self) -> str:
        return self.lane.run_id

    async def append(
        self,
        events: Sequence[ProposedEvent],
        *,
        append_id: str | None = None,
    ) -> AppendReceipt:
        async with self._append_lock:
            receipt = await self.store.append(
                self.lane.id,
                self.expected_last_seq,
                append_id or new_id(),
                events,
            )
            self.expected_last_seq = receipt.last_seq
            self.lane = self.lane.model_copy(update={"last_seq": receipt.last_seq})
            return receipt

    async def record(
        self,
        event_type: str,
        subject_id: str,
        *,
        payload: dict[str, Any] | None = None,
        extensions: dict[str, Any] | None = None,
        causation_id: str | None = None,
        append_id: str | None = None,
    ) -> StoredEvent:
        event = ProposedEvent(
            lane_id=self.lane.id,
            subject_id=subject_id,
            event_type=event_type,
            actor_id=self.actor.id,
            causation_id=causation_id,
            payload=payload or {},
            extensions=extensions or {},
        )
        receipt = await self.append([event], append_id=append_id)
        return StoredEvent.from_proposed(
            event,
            seq=receipt.first_seq,
            committed_at=receipt.committed_at,
        )

    async def start_run(self, *, payload: dict[str, Any] | None = None) -> StoredEvent:
        run_payload = dict(payload or {})
        if self.parent_run_id is not None:
            run_payload["parent_run_id"] = self.parent_run_id
        return await self.record(
            EventType.RUN_STARTED,
            self.run_id,
            payload=run_payload,
            causation_id=self.run_causation_id,
        )

    async def complete_run(self, *, payload: dict[str, Any] | None = None) -> StoredEvent:
        return await self.record(EventType.RUN_COMPLETED, self.run_id, payload=payload)

    async def fail_run(
        self,
        error: BaseException,
        *,
        payload: dict[str, Any] | None = None,
    ) -> StoredEvent:
        return await self.record(
            EventType.RUN_FAILED,
            self.run_id,
            payload=_error_payload(error, payload),
        )

    async def start_turn(self, *, payload: dict[str, Any] | None = None) -> Turn:
        turn = Turn(lane_id=self.lane.id)
        await self.store.create_turn(turn)
        await self.record(EventType.TURN_STARTED, turn.id, payload=payload)
        return turn

    async def complete_turn(
        self,
        turn: Turn | str,
        *,
        payload: dict[str, Any] | None = None,
    ) -> StoredEvent:
        return await self.record(EventType.TURN_COMPLETED, _entity_id(turn), payload=payload)

    async def fail_turn(
        self,
        turn: Turn | str,
        error: BaseException,
        *,
        payload: dict[str, Any] | None = None,
    ) -> StoredEvent:
        return await self.record(
            EventType.TURN_FAILED,
            _entity_id(turn),
            payload=_error_payload(error, payload),
        )

    async def before_model_call(
        self,
        turn: Turn | str,
        *,
        payload: dict[str, Any],
    ) -> AttemptHandle:
        return await self._before_call("model_call", _entity_id(turn), payload)

    async def before_tool_call(
        self,
        turn: Turn | str,
        *,
        payload: dict[str, Any],
    ) -> AttemptHandle:
        return await self._before_call("tool_call", _entity_id(turn), payload)

    async def retry(
        self,
        action_id: str,
        attempt_no: int,
        *,
        payload: dict[str, Any],
    ) -> AttemptHandle:
        action = await self.store.get_action(action_id)
        if action is None:
            raise EntityNotFound("action", action_id)
        if action.type not in {"model_call", "tool_call"}:
            raise ValueError(f"action {action_id!r} is not retryable")
        action_type = cast(Literal["model_call", "tool_call"], action.type)
        return await self._before_call(
            action_type,
            action.turn_id,
            payload,
            action=action,
            attempt_no=attempt_no,
        )

    async def attempt_completed(
        self,
        attempt: AttemptHandle,
        *,
        payload: dict[str, Any],
    ) -> StoredEvent:
        return await self.record(
            EventType.ATTEMPT_COMPLETED,
            attempt.attempt_id,
            payload=payload,
            causation_id=attempt.requested_event_id,
        )

    async def attempt_failed(
        self,
        attempt: AttemptHandle,
        error: BaseException,
        *,
        payload: dict[str, Any] | None = None,
    ) -> StoredEvent:
        return await self.record(
            EventType.ATTEMPT_FAILED,
            attempt.attempt_id,
            payload=_error_payload(error, payload),
            causation_id=attempt.requested_event_id,
        )

    async def model_completed(
        self,
        attempt: AttemptHandle,
        *,
        payload: dict[str, Any],
    ) -> StoredEvent:
        _require_action_type(attempt, "model_call")
        return await self.attempt_completed(attempt, payload=payload)

    async def model_failed(
        self,
        attempt: AttemptHandle,
        error: BaseException,
        *,
        payload: dict[str, Any] | None = None,
    ) -> StoredEvent:
        _require_action_type(attempt, "model_call")
        return await self.attempt_failed(attempt, error, payload=payload)

    async def tool_completed(
        self,
        attempt: AttemptHandle,
        *,
        payload: dict[str, Any],
    ) -> StoredEvent:
        _require_action_type(attempt, "tool_call")
        return await self.attempt_completed(attempt, payload=payload)

    async def tool_failed(
        self,
        attempt: AttemptHandle,
        error: BaseException,
        *,
        payload: dict[str, Any] | None = None,
    ) -> StoredEvent:
        _require_action_type(attempt, "tool_call")
        return await self.attempt_failed(attempt, error, payload=payload)

    async def record_action(
        self,
        turn: Turn | str,
        action_type: str,
        *,
        event_type: str = EventType.ACTION_COMPLETED,
        payload: dict[str, Any] | None = None,
        parent_action_id: str | None = None,
    ) -> tuple[Action, StoredEvent]:
        action = Action(
            turn_id=_entity_id(turn),
            type=action_type,
            parent_action_id=parent_action_id,
        )
        await self.store.create_action(action)
        event = await self.record(event_type, action.id, payload=payload)
        return action, event

    async def save_snapshot(
        self,
        profile: str,
        snapshot: dict[str, Any],
        *,
        profile_version: str = "1",
    ) -> StoredEvent:
        return await self.record(
            EventType.LANE_FRAMEWORK_SNAPSHOT_SAVED,
            self.lane.id,
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
            EventType.LANE_FRAMEWORK_CHECKPOINT_LINKED,
            self.lane.id,
            payload={
                "profile": profile,
                "profile_version": profile_version,
                "checkpoint_id": checkpoint_id,
                "metadata": metadata or {},
            },
        )

    async def child(
        self,
        *,
        run_id: str,
        actor: Actor,
        causation_id: str,
    ) -> LaneRecorder:
        return await LaneRecorder.open(
            store=self.store,
            session_id=self.session_id,
            run_id=run_id,
            actor=actor,
            parent_lane_id=self.lane.id,
            parent_run_id=self.run_id,
            run_causation_id=causation_id,
        )

    async def _before_call(
        self,
        action_type: Literal["model_call", "tool_call"],
        turn_id: str,
        payload: dict[str, Any],
        *,
        action: Action | None = None,
        attempt_no: int = 1,
    ) -> AttemptHandle:
        if action is None:
            action = Action(turn_id=turn_id, type=action_type)
            await self.store.create_action(action)
        attempt = Attempt(action_id=action.id, attempt_no=attempt_no)
        await self.store.create_attempt(attempt)
        requested = await self.record(
            EventType.ATTEMPT_REQUESTED,
            attempt.id,
            payload=payload,
        )
        return AttemptHandle(
            action_type=action_type,
            turn_id=turn_id,
            action_id=action.id,
            attempt_id=attempt.id,
            attempt_no=attempt_no,
            requested_event_id=requested.id,
        )


def _entity_id(entity: Turn | str) -> str:
    return entity.id if isinstance(entity, Turn) else entity


def _require_action_type(
    attempt: AttemptHandle,
    expected: Literal["model_call", "tool_call"],
) -> None:
    if attempt.action_type != expected:
        raise ValueError(f"expected a {expected} attempt, got {attempt.action_type}")


def _error_payload(
    error: BaseException,
    payload: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        **(payload or {}),
        "error_type": type(error).__name__,
        "error": str(error),
    }
