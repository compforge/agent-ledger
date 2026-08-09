from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence

from agent_ledger.errors import DuplicateEvent, IdempotencyViolation, InvalidCursor, StreamConflict
from agent_ledger.models import (
    CommitReceipt,
    ProposedEvent,
    RunStream,
    StoredEvent,
    utc_now,
)
from agent_ledger.stores._validation import validate_append


class MemoryEventStore:
    """Process-local reference implementation of the EventStore contract."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._stream_events: dict[tuple[str, str], list[StoredEvent]] = {}
        self._session_events: dict[str, list[StoredEvent]] = {}
        self._event_ids: set[str] = set()
        self._receipts: dict[tuple[str, str, str], CommitReceipt] = {}
        self._session_cursors: dict[str, int] = {}

    async def append(
        self,
        stream: RunStream,
        expected_version: int,
        append_id: str,
        events: Sequence[ProposedEvent],
    ) -> CommitReceipt:
        batch, digest = validate_append(stream, expected_version, append_id, events)
        receipt_key = (stream.session_id, stream.run_id, append_id)
        stream_key = (stream.session_id, stream.run_id)

        async with self._lock:
            existing = self._receipts.get(receipt_key)
            if existing is not None:
                if existing.digest != digest:
                    raise IdempotencyViolation(append_id)
                return existing.model_copy(deep=True)

            stream_events = self._stream_events.get(stream_key, [])
            actual_version = len(stream_events) - 1
            if actual_version != expected_version:
                raise StreamConflict(expected_version, actual_version)

            for event in batch:
                if event.event_id in self._event_ids:
                    raise DuplicateEvent(event.event_id)

            committed_at = utc_now()
            first_version = expected_version + 1
            next_cursor = self._session_cursors.get(stream.session_id, -1) + 1
            stored: list[StoredEvent] = []
            for offset, event in enumerate(batch):
                stored.append(
                    StoredEvent.from_proposed(
                        event,
                        stream_version=first_version + offset,
                        commit_cursor=str(next_cursor + offset),
                        committed_at=committed_at,
                    )
                )

            receipt = CommitReceipt(
                stream=stream,
                append_id=append_id,
                digest=digest,
                first_version=stored[0].stream_version,
                last_version=stored[-1].stream_version,
                first_cursor=stored[0].commit_cursor,
                last_cursor=stored[-1].commit_cursor,
                event_ids=tuple(event.event_id for event in stored),
                committed_at=committed_at,
            )

            self._stream_events.setdefault(stream_key, []).extend(stored)
            self._session_events.setdefault(stream.session_id, []).extend(stored)
            self._event_ids.update(event.event_id for event in stored)
            self._session_cursors[stream.session_id] = next_cursor + len(stored) - 1
            self._receipts[receipt_key] = receipt
            return receipt.model_copy(deep=True)

    async def read_stream(
        self,
        stream: RunStream,
        after_version: int = -1,
    ) -> AsyncIterator[StoredEvent]:
        async with self._lock:
            events = [
                event.model_copy(deep=True)
                for event in self._stream_events.get((stream.session_id, stream.run_id), [])
                if event.stream_version > after_version
            ]
        for event in events:
            yield event

    async def scan_session(
        self,
        session_id: str,
        after_cursor: str | None = None,
    ) -> AsyncIterator[StoredEvent]:
        cursor = _parse_cursor(after_cursor)
        async with self._lock:
            events = [
                event.model_copy(deep=True)
                for event in self._session_events.get(session_id, [])
                if int(event.commit_cursor) > cursor
            ]
        for event in events:
            yield event


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
