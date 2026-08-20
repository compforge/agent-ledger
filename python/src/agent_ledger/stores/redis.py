from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator, Awaitable, Sequence
from typing import Any, cast

from redis.asyncio import Redis
from redis.exceptions import RedisError

from agent_ledger.errors import (
    DuplicateEvent,
    EntityConflict,
    EntityNotFound,
    IdempotencyViolation,
    LaneConflict,
    StoreError,
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

_CREATE_LANE_LUA = r"""
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 or redis.call('HEXISTS', KEYS[2], ARGV[2]) == 1 then
    return 'conflict'
end
if ARGV[3] ~= '' then
    local parent = redis.call('HGET', KEYS[1], ARGV[3])
    if not parent then return 'missing-parent' end
    if cjson.decode(parent)['session_id'] ~= ARGV[4] then return 'wrong-parent' end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[5])
redis.call('HSET', KEYS[2], ARGV[2], ARGV[1])
return 'ok'
"""

_CREATE_CHILD_LUA = r"""
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then return 'conflict' end
if redis.call('HEXISTS', KEYS[2], ARGV[2]) == 0 then return 'missing-parent' end
if ARGV[3] ~= '' then
    local parent = redis.call('HGET', KEYS[1], ARGV[3])
    if not parent then return 'missing-extra-parent' end
    if cjson.decode(parent)[ARGV[4]] ~= ARGV[2] then return 'wrong-extra-parent' end
end
if ARGV[5] ~= '' and redis.call('HEXISTS', KEYS[3], ARGV[5]) == 1 then return 'number-conflict' end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[6])
if ARGV[5] ~= '' then redis.call('HSET', KEYS[3], ARGV[5], ARGV[1]) end
return 'ok'
"""

_APPEND_LUA = r"""
local previous = redis.call('HGET', KEYS[3], ARGV[3])
if previous then
    local receipt = cjson.decode(previous)
    if receipt['lane_id'] == ARGV[1] and receipt['digest'] == ARGV[4] then
        return {'existing', previous}
    end
    return {'idempotency', ''}
end

local lane_raw = redis.call('HGET', KEYS[1], ARGV[1])
if not lane_raw then return {'missing-lane', ''} end
local lane = cjson.decode(lane_raw)
if tonumber(lane['last_seq']) ~= tonumber(ARGV[2]) then
    return {'conflict', tostring(lane['last_seq'])}
end

local count = tonumber(ARGV[6])
for index = 0, count - 1 do
    local event_id = ARGV[7 + index * 2]
    if redis.call('HEXISTS', KEYS[2], event_id) == 1 then
        return {'duplicate', event_id}
    end
end

local first_seq = tonumber(lane['last_seq']) + 1
local event_ids = {}
for index = 0, count - 1 do
    local event_id = ARGV[7 + index * 2]
    local event = cjson.decode(ARGV[8 + index * 2])
    local seq = first_seq + index
    event['seq'] = seq
    event['committed_at'] = ARGV[5]
    redis.call('HSET', KEYS[2], event_id, cjson.encode(event))
    redis.call('ZADD', KEYS[4], seq, event_id)
    event_ids[index + 1] = event_id
end

local last_seq = first_seq + count - 1
lane['last_seq'] = last_seq
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(lane))
local receipt = {
    id = ARGV[3], lane_id = ARGV[1], digest = ARGV[4],
    first_seq = first_seq, last_seq = last_seq,
    event_ids = event_ids, committed_at = ARGV[5]
}
local encoded = cjson.encode(receipt)
redis.call('HSET', KEYS[3], ARGV[3], encoded)
return {'ok', encoded}
"""


class RedisEventStore:
    """Redis Store with atomic Lane OCC and append idempotency."""

    def __init__(
        self,
        client: Redis,
        *,
        prefix: str = "agent-ledger",
        operation_timeout: float = 5.0,
    ) -> None:
        if not prefix or "{" in prefix or "}" in prefix:
            raise ValueError("prefix must be non-empty and must not contain braces")
        if operation_timeout <= 0:
            raise ValueError("operation_timeout must be positive")
        self._client = client
        self._root = f"{prefix}:{{ledger}}"
        self._operation_timeout = operation_timeout
        self._create_lane_script = client.register_script(_CREATE_LANE_LUA)
        self._create_child_script = client.register_script(_CREATE_CHILD_LUA)
        self._append_script = client.register_script(_APPEND_LUA)

    async def create_actor(self, actor: Actor) -> None:
        try:
            created = await asyncio.wait_for(
                cast(
                    Awaitable[Any],
                    self._client.hsetnx(self._key("actors"), actor.id, actor.model_dump_json()),
                ),
                timeout=self._operation_timeout,
            )
        except TimeoutError as error:
            raise StoreError("Redis actor creation timed out") from error
        except RedisError as error:
            raise StoreError("Redis actor creation failed") from error
        if not created:
            raise EntityConflict("actor", actor.id)

    async def get_actor(self, actor_id: str) -> Actor | None:
        return cast(Actor | None, await self._get_model("actors", actor_id, Actor))

    async def create_lane(self, lane: Lane) -> None:
        if lane.last_seq != 0:
            raise ValueError("a new lane must have last_seq 0")
        result = await self._run_script(
            self._create_lane_script,
            [self._key("lanes"), self._key("lane-names")],
            [
                lane.id,
                _lane_name_key(lane.session_id, lane.run_id, lane.name),
                lane.parent_lane_id or "",
                lane.session_id,
                lane.model_dump_json(),
            ],
            "Redis lane creation",
        )
        status = _decode(result)
        if status == "conflict":
            raise EntityConflict("lane", lane.id)
        if status == "missing-parent":
            raise EntityNotFound("parent lane", lane.parent_lane_id or "")
        if status == "wrong-parent":
            raise ValueError("parent lane must belong to the same session")
        if status != "ok":
            raise StoreError(f"Redis lane creation returned {status!r}")

    async def get_lane(self, lane_id: str) -> Lane | None:
        return cast(Lane | None, await self._get_model("lanes", lane_id, Lane))

    async def find_lane(self, session_id: str, run_id: str, name: str) -> Lane | None:
        try:
            lane_id = await asyncio.wait_for(
                cast(
                    Awaitable[Any],
                    self._client.hget(
                        self._key("lane-names"), _lane_name_key(session_id, run_id, name)
                    ),
                ),
                timeout=self._operation_timeout,
            )
        except TimeoutError as error:
            raise StoreError("Redis lane lookup timed out") from error
        except RedisError as error:
            raise StoreError("Redis lane lookup failed") from error
        return await self.get_lane(_decode(lane_id)) if lane_id is not None else None

    async def create_turn(self, turn: Turn) -> None:
        await self._create_child("turn", turn.id, "turns", turn.lane_id, "lanes", turn)

    async def get_turn(self, turn_id: str) -> Turn | None:
        return cast(Turn | None, await self._get_model("turns", turn_id, Turn))

    async def create_action(self, action: Action) -> None:
        await self._create_child(
            "action",
            action.id,
            "actions",
            action.turn_id,
            "turns",
            action,
            extra_parent_id=action.parent_action_id,
            extra_parent_field="turn_id",
        )

    async def get_action(self, action_id: str) -> Action | None:
        return cast(Action | None, await self._get_model("actions", action_id, Action))

    async def create_attempt(self, attempt: Attempt) -> None:
        await self._create_child(
            "attempt",
            attempt.id,
            "attempts",
            attempt.action_id,
            "actions",
            attempt,
            unique_key=f"{attempt.action_id}\x00{attempt.attempt_no}",
        )

    async def get_attempt(self, attempt_id: str) -> Attempt | None:
        return cast(Attempt | None, await self._get_model("attempts", attempt_id, Attempt))

    async def append(
        self,
        lane_id: str,
        expected_last_seq: int,
        append_id: str,
        events: Sequence[ProposedEvent],
    ) -> AppendReceipt:
        batch, digest = validate_append(lane_id, expected_last_seq, append_id, events)
        lane = await self.get_lane(lane_id)
        if lane is None:
            raise EntityNotFound("lane", lane_id)
        actor_ids = {event.actor_id for event in batch}
        for actor_id in actor_ids:
            if await self.get_actor(actor_id) is None:
                raise EntityNotFound("actor", actor_id)
        prior_ids: set[str] = set()
        for event in batch:
            await self._validate_subject(lane, event)
            if event.causation_id is not None and event.causation_id not in prior_ids:
                caused = await self._get_model("events", event.causation_id, StoredEvent)
                if caused is None:
                    raise EntityNotFound("causation event", event.causation_id)
                caused_lane = await self.get_lane(caused.lane_id)
                if caused_lane is None or caused_lane.session_id != lane.session_id:
                    raise SubjectMismatch(event.id, lane.id)
            prior_ids.add(event.id)

        committed_at = utc_now()
        args: list[str | int] = [
            lane_id,
            expected_last_seq,
            append_id,
            digest,
            committed_at.isoformat(),
            len(batch),
        ]
        for event in batch:
            args.extend([event.id, event.model_dump_json()])
        result = await self._run_script(
            self._append_script,
            [
                self._key("lanes"),
                self._key("events"),
                self._key("appends"),
                self._key(f"lane-events:{_hash(lane_id)}"),
            ],
            args,
            "Redis append",
        )
        if not isinstance(result, list) or len(result) != 2:
            raise StoreError("Redis append returned an invalid response")
        status, payload = _decode(result[0]), _decode(result[1])
        if status in {"ok", "existing"}:
            return AppendReceipt.model_validate_json(payload)
        if status == "conflict":
            raise LaneConflict(expected_last_seq, int(payload))
        if status == "idempotency":
            raise IdempotencyViolation(append_id)
        if status == "duplicate":
            raise DuplicateEvent(payload)
        if status == "missing-lane":
            raise EntityNotFound("lane", lane_id)
        raise StoreError(f"Redis append returned unknown status {status!r}")

    async def read_lane(
        self,
        lane_id: str,
        after_seq: int = 0,
    ) -> AsyncIterator[StoredEvent]:
        if after_seq < 0:
            raise ValueError("after_seq must be non-negative")
        if await self.get_lane(lane_id) is None:
            raise EntityNotFound("lane", lane_id)
        try:
            ids = await asyncio.wait_for(
                self._client.zrangebyscore(
                    self._key(f"lane-events:{_hash(lane_id)}"), f"({after_seq}", "+inf"
                ),
                timeout=self._operation_timeout,
            )
            if not ids:
                return
            raw = await asyncio.wait_for(
                cast(Awaitable[list[Any]], self._client.hmget(self._key("events"), ids)),
                timeout=self._operation_timeout,
            )
        except TimeoutError as error:
            raise StoreError("Redis lane read timed out") from error
        except RedisError as error:
            raise StoreError("Redis lane read failed") from error
        for value in raw:
            if value is None:
                raise StoreError("Redis Lane index refers to a missing Event")
            yield StoredEvent.model_validate_json(value)

    async def load_session(self, session_id: str) -> SessionView:
        lanes = [
            lane for lane in await self._all_models("lanes", Lane) if lane.session_id == session_id
        ]
        lane_ids = {lane.id for lane in lanes}
        turns = [turn for turn in await self._all_models("turns", Turn) if turn.lane_id in lane_ids]
        turn_ids = {turn.id for turn in turns}
        actions = [
            action
            for action in await self._all_models("actions", Action)
            if action.turn_id in turn_ids
        ]
        action_ids = {action.id for action in actions}
        attempts = [
            attempt
            for attempt in await self._all_models("attempts", Attempt)
            if attempt.action_id in action_ids
        ]
        events = [
            event
            for event in await self._all_models("events", StoredEvent)
            if event.lane_id in lane_ids
        ]
        events.sort(key=lambda event: (event.committed_at, event.lane_id, event.seq, event.id))
        actor_ids = {event.actor_id for event in events}
        actors = [
            actor for actor in await self._all_models("actors", Actor) if actor.id in actor_ids
        ]
        return SessionView(
            session_id=session_id,
            actors=tuple(actors),
            lanes=tuple(lanes),
            turns=tuple(turns),
            actions=tuple(actions),
            attempts=tuple(attempts),
            events=tuple(events),
        )

    async def _create_child(
        self,
        entity: str,
        entity_id: str,
        bucket: str,
        parent_id: str,
        parent_bucket: str,
        value: Any,
        *,
        extra_parent_id: str | None = None,
        extra_parent_field: str = "",
        unique_key: str = "",
    ) -> None:
        result = await self._run_script(
            self._create_child_script,
            [self._key(bucket), self._key(parent_bucket), self._key("attempt-numbers")],
            [
                entity_id,
                parent_id,
                extra_parent_id or "",
                extra_parent_field,
                unique_key,
                value.model_dump_json(),
            ],
            f"Redis {entity} creation",
        )
        status = _decode(result)
        if status in {"conflict", "number-conflict"}:
            raise EntityConflict(entity, entity_id)
        if status == "missing-parent":
            raise EntityNotFound(parent_bucket.removesuffix("s"), parent_id)
        if status == "missing-extra-parent":
            raise EntityNotFound(f"parent {entity}", extra_parent_id or "")
        if status == "wrong-extra-parent":
            raise ValueError(f"parent {entity} belongs to a different owner")
        if status != "ok":
            raise StoreError(f"Redis {entity} creation returned {status!r}")

    async def _validate_subject(self, lane: Lane, event: ProposedEvent) -> None:
        kind = event.subject_kind
        valid = False
        if kind == "session":
            valid = event.subject_id == lane.session_id
        elif kind == "run":
            valid = event.subject_id == lane.run_id
        elif kind == "lane":
            valid = event.subject_id == lane.id
        elif kind == "turn":
            turn = await self.get_turn(event.subject_id)
            valid = turn is not None and turn.lane_id == lane.id
        elif kind == "action":
            action = await self.get_action(event.subject_id)
            turn = await self.get_turn(action.turn_id) if action is not None else None
            valid = turn is not None and turn.lane_id == lane.id
        elif kind == "attempt":
            attempt = await self.get_attempt(event.subject_id)
            action = await self.get_action(attempt.action_id) if attempt is not None else None
            turn = await self.get_turn(action.turn_id) if action is not None else None
            valid = turn is not None and turn.lane_id == lane.id
        if not valid:
            raise SubjectMismatch(event.id, lane.id)

    async def _get_model(self, bucket: str, entity_id: str, model: Any) -> Any:
        try:
            raw = await asyncio.wait_for(
                cast(Awaitable[Any], self._client.hget(self._key(bucket), entity_id)),
                timeout=self._operation_timeout,
            )
        except TimeoutError as error:
            raise StoreError("Redis entity lookup timed out") from error
        except RedisError as error:
            raise StoreError("Redis entity lookup failed") from error
        return model.model_validate_json(raw) if raw is not None else None

    async def _all_models(self, bucket: str, model: Any) -> list[Any]:
        try:
            values = await asyncio.wait_for(
                cast(Awaitable[list[Any]], self._client.hvals(self._key(bucket))),
                timeout=self._operation_timeout,
            )
        except TimeoutError as error:
            raise StoreError("Redis session read timed out") from error
        except RedisError as error:
            raise StoreError("Redis session read failed") from error
        return [model.model_validate_json(value) for value in values]

    async def _run_script(
        self,
        script: Any,
        keys: list[str],
        args: list[str | int],
        operation: str,
    ) -> Any:
        try:
            return await asyncio.wait_for(
                script(keys=keys, args=args),
                timeout=self._operation_timeout,
            )
        except TimeoutError as error:
            raise StoreError(f"{operation} timed out") from error
        except RedisError as error:
            raise StoreError(f"{operation} failed") from error

    def _key(self, suffix: str) -> str:
        return f"{self._root}:{suffix}"


def _lane_name_key(session_id: str, run_id: str, name: str) -> str:
    return _hash(f"{session_id}\x00{run_id}\x00{name}")


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    if isinstance(value, str):
        return value
    raise StoreError("Redis returned a non-text value")
