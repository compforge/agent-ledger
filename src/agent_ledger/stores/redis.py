from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator, Awaitable, Sequence
from typing import Any, cast

from redis.asyncio import Redis
from redis.exceptions import RedisError

from agent_ledger.errors import (
    DuplicateEvent,
    IdempotencyViolation,
    InvalidCursor,
    StoreError,
    StreamConflict,
)
from agent_ledger.models import CommitReceipt, ProposedEvent, RunStream, StoredEvent, utc_now
from agent_ledger.stores._validation import validate_append

_APPEND_LUA = r"""
local existing = redis.call('HGET', KEYS[5], ARGV[3])
if existing then
    local receipt = cjson.decode(existing)
    if receipt['digest'] == ARGV[4] then
        return {'existing', existing}
    end
    return {'idempotency', ''}
end

local current = redis.call('HGET', KEYS[1], ARGV[2])
if not current then current = '-1' end
if tonumber(current) ~= tonumber(ARGV[1]) then
    return {'conflict', current}
end

local count = tonumber(ARGV[8])
for index = 0, count - 1 do
    local event_id = ARGV[9 + index * 2]
    if redis.call('HEXISTS', KEYS[2], event_id) == 1 then
        return {'duplicate', event_id}
    end
end

local cursor = redis.call('HGET', KEYS[1], 'cursor')
if not cursor then cursor = '-1' end
local first_version = tonumber(current) + 1
local first_cursor = tonumber(cursor) + 1
local event_ids = {}

for index = 0, count - 1 do
    local event_id = ARGV[9 + index * 2]
    local event = cjson.decode(ARGV[10 + index * 2])
    local version = first_version + index
    local commit_cursor = first_cursor + index
    event['stream_version'] = version
    event['commit_cursor'] = tostring(commit_cursor)
    event['committed_at'] = ARGV[5]
    redis.call('HSET', KEYS[2], event_id, cjson.encode(event))
    redis.call('ZADD', KEYS[3], commit_cursor, event_id)
    redis.call('ZADD', KEYS[4], version, event_id)
    event_ids[index + 1] = event_id
end

local last_version = first_version + count - 1
local last_cursor = first_cursor + count - 1
redis.call('HSET', KEYS[1], ARGV[2], tostring(last_version))
redis.call('HSET', KEYS[1], 'cursor', tostring(last_cursor))

local receipt = {
    stream = {session_id = ARGV[6], run_id = ARGV[7]},
    append_id = ARGV[3],
    digest = ARGV[4],
    first_version = first_version,
    last_version = last_version,
    first_cursor = tostring(first_cursor),
    last_cursor = tostring(last_cursor),
    event_ids = event_ids,
    committed_at = ARGV[5]
}
local encoded = cjson.encode(receipt)
redis.call('HSET', KEYS[5], ARGV[3], encoded)
return {'ok', encoded}
"""


class RedisEventStore:
    """Redis store with atomic OCC and idempotency implemented by Lua."""

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
        self._prefix = prefix
        self._operation_timeout = operation_timeout
        self._append_script = client.register_script(_APPEND_LUA)

    async def append(
        self,
        stream: RunStream,
        expected_version: int,
        append_id: str,
        events: Sequence[ProposedEvent],
    ) -> CommitReceipt:
        batch, digest = validate_append(stream, expected_version, append_id, events)
        keys = self._keys(stream)
        committed_at = utc_now()
        args: list[str | int] = [
            expected_version,
            f"version:{_hash(stream.run_id)}",
            append_id,
            digest,
            committed_at.isoformat(),
            stream.session_id,
            stream.run_id,
            len(batch),
        ]
        for event in batch:
            args.extend(
                [
                    event.event_id,
                    json.dumps(
                        event.model_dump(mode="json"),
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ),
                ]
            )

        try:
            result: Any = await asyncio.wait_for(
                self._append_script(keys=keys, args=args),
                timeout=self._operation_timeout,
            )
        except TimeoutError as error:
            raise StoreError("Redis append timed out") from error
        except RedisError as error:
            raise StoreError("Redis append failed") from error

        if not isinstance(result, list) or len(result) != 2:
            raise StoreError("Redis append returned an invalid response")
        status = _decode(result[0])
        payload = _decode(result[1])
        if status in {"ok", "existing"}:
            return CommitReceipt.model_validate_json(payload)
        if status == "conflict":
            raise StreamConflict(expected_version, int(payload))
        if status == "idempotency":
            raise IdempotencyViolation(append_id)
        if status == "duplicate":
            raise DuplicateEvent(payload)
        raise StoreError(f"Redis append returned unknown status {status!r}")

    async def read_stream(
        self,
        stream: RunStream,
        after_version: int = -1,
    ) -> AsyncIterator[StoredEvent]:
        keys = self._keys(stream)
        try:
            event_ids = await asyncio.wait_for(
                self._client.zrangebyscore(keys[3], f"({after_version}", "+inf"),
                timeout=self._operation_timeout,
            )
            if not event_ids:
                return
            raw_events = await asyncio.wait_for(
                cast(Awaitable[list[Any]], self._client.hmget(keys[1], event_ids)),
                timeout=self._operation_timeout,
            )
        except TimeoutError as error:
            raise StoreError("Redis stream read timed out") from error
        except RedisError as error:
            raise StoreError("Redis stream read failed") from error
        for raw in raw_events:
            if raw is None:
                raise StoreError("Redis stream index refers to a missing event")
            yield StoredEvent.model_validate_json(raw)

    async def scan_session(
        self,
        session_id: str,
        after_cursor: str | None = None,
    ) -> AsyncIterator[StoredEvent]:
        cursor = _parse_cursor(after_cursor)
        stream = RunStream(session_id=session_id, run_id="_")
        keys = self._keys(stream)
        try:
            event_ids = await asyncio.wait_for(
                self._client.zrangebyscore(keys[2], f"({cursor}", "+inf"),
                timeout=self._operation_timeout,
            )
            if not event_ids:
                return
            raw_events = await asyncio.wait_for(
                cast(Awaitable[list[Any]], self._client.hmget(keys[1], event_ids)),
                timeout=self._operation_timeout,
            )
        except TimeoutError as error:
            raise StoreError("Redis session scan timed out") from error
        except RedisError as error:
            raise StoreError("Redis session scan failed") from error
        for raw in raw_events:
            if raw is None:
                raise StoreError("Redis session index refers to a missing event")
            yield StoredEvent.model_validate_json(raw)

    def _keys(self, stream: RunStream) -> list[str]:
        tag = _hash(stream.session_id)
        root = f"{self._prefix}:{{{tag}}}"
        run = _hash(stream.run_id)
        return [
            f"{root}:meta",
            f"{root}:events",
            f"{root}:session-index",
            f"{root}:run-index:{run}",
            f"{root}:commits:{run}",
        ]


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    if isinstance(value, str):
        return value
    raise StoreError("Redis returned a non-text value")


def _parse_cursor(cursor: str | None) -> int:
    if cursor is None:
        return -1
    try:
        value = int(cursor)
    except ValueError as error:
        raise InvalidCursor(cursor) from error
    if value < -1:
        raise InvalidCursor(cursor)
    return value
