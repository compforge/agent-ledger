from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence

from agent_ledger.errors import (
    DuplicateEvent,
    EntityConflict,
    EntityNotFound,
    IdempotencyViolation,
    LaneConflict,
    SubjectMismatch,
)
from agent_ledger.models import (
    Action,
    Actor,
    AppendReceipt,
    Attempt,
    Lane,
    ProposedEvent,
    SessionView,
    StoredEvent,
    Turn,
    utc_now,
)
from agent_ledger.stores._validation import validate_append


class MemoryEventStore:
    """Process-local reference implementation of the EventStore contract."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._actors: dict[str, Actor] = {}
        self._lanes: dict[str, Lane] = {}
        self._lane_names: dict[tuple[str, str, str], str] = {}
        self._turns: dict[str, Turn] = {}
        self._actions: dict[str, Action] = {}
        self._attempts: dict[str, Attempt] = {}
        self._attempt_numbers: set[tuple[str, int]] = set()
        self._events: dict[str, StoredEvent] = {}
        self._lane_events: dict[str, list[StoredEvent]] = {}
        self._appends: dict[str, AppendReceipt] = {}

    async def create_actor(self, actor: Actor) -> None:
        async with self._lock:
            if actor.id in self._actors:
                raise EntityConflict("actor", actor.id)
            self._actors[actor.id] = actor.model_copy(deep=True)

    async def get_actor(self, actor_id: str) -> Actor | None:
        async with self._lock:
            actor = self._actors.get(actor_id)
            return actor.model_copy(deep=True) if actor is not None else None

    async def create_lane(self, lane: Lane) -> None:
        async with self._lock:
            if (
                lane.id in self._lanes
                or (lane.session_id, lane.run_id, lane.name) in self._lane_names
            ):
                raise EntityConflict("lane", lane.id)
            if lane.last_seq != 0:
                raise ValueError("a new lane must have last_seq 0")
            if lane.parent_lane_id is not None:
                parent = self._lanes.get(lane.parent_lane_id)
                if parent is None:
                    raise EntityNotFound("parent lane", lane.parent_lane_id)
                if parent.session_id != lane.session_id:
                    raise ValueError("parent lane must belong to the same session")
            snapshot = lane.model_copy(deep=True)
            self._lanes[lane.id] = snapshot
            self._lane_names[(lane.session_id, lane.run_id, lane.name)] = lane.id

    async def get_lane(self, lane_id: str) -> Lane | None:
        async with self._lock:
            lane = self._lanes.get(lane_id)
            return lane.model_copy(deep=True) if lane is not None else None

    async def find_lane(self, session_id: str, run_id: str, name: str) -> Lane | None:
        async with self._lock:
            lane_id = self._lane_names.get((session_id, run_id, name))
            lane = self._lanes.get(lane_id) if lane_id is not None else None
            return lane.model_copy(deep=True) if lane is not None else None

    async def create_turn(self, turn: Turn) -> None:
        async with self._lock:
            if turn.id in self._turns:
                raise EntityConflict("turn", turn.id)
            if turn.lane_id not in self._lanes:
                raise EntityNotFound("lane", turn.lane_id)
            self._turns[turn.id] = turn.model_copy(deep=True)

    async def get_turn(self, turn_id: str) -> Turn | None:
        async with self._lock:
            turn = self._turns.get(turn_id)
            return turn.model_copy(deep=True) if turn is not None else None

    async def create_action(self, action: Action) -> None:
        async with self._lock:
            if action.id in self._actions:
                raise EntityConflict("action", action.id)
            turn = self._turns.get(action.turn_id)
            if turn is None:
                raise EntityNotFound("turn", action.turn_id)
            if action.parent_action_id is not None:
                parent = self._actions.get(action.parent_action_id)
                if parent is None:
                    raise EntityNotFound("parent action", action.parent_action_id)
                if parent.turn_id != action.turn_id:
                    raise ValueError("parent action must belong to the same turn")
            self._actions[action.id] = action.model_copy(deep=True)

    async def get_action(self, action_id: str) -> Action | None:
        async with self._lock:
            action = self._actions.get(action_id)
            return action.model_copy(deep=True) if action is not None else None

    async def create_attempt(self, attempt: Attempt) -> None:
        async with self._lock:
            number_key = (attempt.action_id, attempt.attempt_no)
            if attempt.id in self._attempts or number_key in self._attempt_numbers:
                raise EntityConflict("attempt", attempt.id)
            if attempt.action_id not in self._actions:
                raise EntityNotFound("action", attempt.action_id)
            self._attempts[attempt.id] = attempt.model_copy(deep=True)
            self._attempt_numbers.add(number_key)

    async def get_attempt(self, attempt_id: str) -> Attempt | None:
        async with self._lock:
            attempt = self._attempts.get(attempt_id)
            return attempt.model_copy(deep=True) if attempt is not None else None

    async def append(
        self,
        lane_id: str,
        expected_last_seq: int,
        append_id: str,
        events: Sequence[ProposedEvent],
    ) -> AppendReceipt:
        batch, digest = validate_append(lane_id, expected_last_seq, append_id, events)
        async with self._lock:
            previous = self._appends.get(append_id)
            if previous is not None:
                if previous.lane_id != lane_id or previous.digest != digest:
                    raise IdempotencyViolation(append_id)
                return previous.model_copy(deep=True)

            lane = self._lanes.get(lane_id)
            if lane is None:
                raise EntityNotFound("lane", lane_id)
            if lane.last_seq != expected_last_seq:
                raise LaneConflict(expected_last_seq, lane.last_seq)

            for event in batch:
                if event.id in self._events:
                    raise DuplicateEvent(event.id)
                if event.actor_id not in self._actors:
                    raise EntityNotFound("actor", event.actor_id)
                self._validate_subject(lane, event)

            prior_ids = set(self._events)
            for event in batch:
                if event.causation_id is not None and event.causation_id not in prior_ids:
                    raise EntityNotFound("causation event", event.causation_id)
                prior_ids.add(event.id)

            committed_at = utc_now()
            first_seq = lane.last_seq + 1
            stored = [
                StoredEvent.from_proposed(event, seq=first_seq + offset, committed_at=committed_at)
                for offset, event in enumerate(batch)
            ]
            receipt = AppendReceipt(
                id=append_id,
                lane_id=lane_id,
                digest=digest,
                first_seq=stored[0].seq,
                last_seq=stored[-1].seq,
                event_ids=tuple(event.id for event in stored),
                committed_at=committed_at,
            )
            self._lanes[lane_id] = lane.model_copy(update={"last_seq": receipt.last_seq})
            self._lane_events.setdefault(lane_id, []).extend(stored)
            for event in stored:
                self._events[event.id] = event
            self._appends[append_id] = receipt
            return receipt.model_copy(deep=True)

    async def read_lane(
        self,
        lane_id: str,
        after_seq: int = 0,
    ) -> AsyncIterator[StoredEvent]:
        if after_seq < 0:
            raise ValueError("after_seq must be non-negative")
        async with self._lock:
            if lane_id not in self._lanes:
                raise EntityNotFound("lane", lane_id)
            events = [
                event.model_copy(deep=True)
                for event in self._lane_events.get(lane_id, [])
                if event.seq > after_seq
            ]
        for event in events:
            yield event

    async def load_session(self, session_id: str) -> SessionView:
        async with self._lock:
            lanes = [lane for lane in self._lanes.values() if lane.session_id == session_id]
            lane_ids = {lane.id for lane in lanes}
            turns = [turn for turn in self._turns.values() if turn.lane_id in lane_ids]
            turn_ids = {turn.id for turn in turns}
            actions = [action for action in self._actions.values() if action.turn_id in turn_ids]
            action_ids = {action.id for action in actions}
            attempts = [
                attempt for attempt in self._attempts.values() if attempt.action_id in action_ids
            ]
            events = [event for event in self._events.values() if event.lane_id in lane_ids]
            actor_ids = {event.actor_id for event in events}
            actors = [actor for actor in self._actors.values() if actor.id in actor_ids]
            events.sort(key=lambda event: (event.committed_at, event.lane_id, event.seq, event.id))
            return SessionView(
                session_id=session_id,
                actors=tuple(actor.model_copy(deep=True) for actor in actors),
                lanes=tuple(lane.model_copy(deep=True) for lane in lanes),
                turns=tuple(turn.model_copy(deep=True) for turn in turns),
                actions=tuple(action.model_copy(deep=True) for action in actions),
                attempts=tuple(attempt.model_copy(deep=True) for attempt in attempts),
                events=tuple(event.model_copy(deep=True) for event in events),
            )

    def _validate_subject(self, lane: Lane, event: ProposedEvent) -> None:
        kind = event.subject_kind
        valid = False
        if kind == "session":
            valid = event.subject_id == lane.session_id
        elif kind == "run":
            valid = event.subject_id == lane.run_id
        elif kind == "lane":
            valid = event.subject_id == lane.id
        elif kind == "turn":
            turn = self._turns.get(event.subject_id)
            valid = turn is not None and turn.lane_id == lane.id
        elif kind == "action":
            action = self._actions.get(event.subject_id)
            turn = self._turns.get(action.turn_id) if action is not None else None
            valid = turn is not None and turn.lane_id == lane.id
        elif kind == "attempt":
            attempt = self._attempts.get(event.subject_id)
            action = self._actions.get(attempt.action_id) if attempt is not None else None
            turn = self._turns.get(action.turn_id) if action is not None else None
            valid = turn is not None and turn.lane_id == lane.id
        if not valid:
            raise SubjectMismatch(event.id, lane.id)
