from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest

from agent_ledger import (
    Actor,
    DuplicateEvent,
    EventStream,
    IdempotencyViolation,
    ProposedEvent,
    StoredEvent,
    StreamConflict,
)
from agent_ledger.store import EventStore


def event(session_id: str, run_id: str, *, event_id: str | None = None) -> ProposedEvent:
    return ProposedEvent(
        event_id=event_id or str(uuid4()),
        event_type="test.recorded",
        session_id=session_id,
        run_id=run_id,
        actor=Actor(type="agent", id="test"),
        payload={"value": str(uuid4())},
    )


async def collect(iterator: AsyncIterator[StoredEvent]) -> list[StoredEvent]:
    return [item async for item in iterator]


async def test_atomic_batch_versions_and_reads(event_store: EventStore) -> None:
    session_id = str(uuid4())
    run_id = str(uuid4())
    stream = EventStream(session_id=session_id, stream_id=run_id)
    first = event(session_id, run_id)
    second = event(session_id, run_id)

    receipt = await event_store.append(stream, -1, str(uuid4()), [first, second])

    assert (receipt.first_version, receipt.last_version) == (0, 1)
    stored = await collect(event_store.read_stream(stream))
    assert [item.event_id for item in stored] == [first.event_id, second.event_id]
    assert [item.stream_version for item in stored] == [0, 1]
    assert all(item.committed_at == receipt.committed_at for item in stored)


async def test_append_is_idempotent_by_content(event_store: EventStore) -> None:
    session_id = str(uuid4())
    run_id = str(uuid4())
    stream = EventStream(session_id=session_id, stream_id=run_id)
    item = event(session_id, run_id)
    append_id = str(uuid4())

    original = await event_store.append(stream, -1, append_id, [item])
    repeated = await event_store.append(stream, -1, append_id, [item])

    assert repeated == original
    assert len(await collect(event_store.read_stream(stream))) == 1

    with pytest.raises(IdempotencyViolation):
        await event_store.append(stream, -1, append_id, [event(session_id, run_id)])


async def test_optimistic_concurrency(event_store: EventStore) -> None:
    session_id = str(uuid4())
    run_id = str(uuid4())
    stream = EventStream(session_id=session_id, stream_id=run_id)
    await event_store.append(stream, -1, str(uuid4()), [event(session_id, run_id)])

    with pytest.raises(StreamConflict) as caught:
        await event_store.append(stream, -1, str(uuid4()), [event(session_id, run_id)])

    assert caught.value.actual_version == 0
    assert len(await collect(event_store.read_stream(stream))) == 1


async def test_duplicate_event_rolls_back_whole_batch(event_store: EventStore) -> None:
    session_id = str(uuid4())
    first_run_id = str(uuid4())
    first_stream = EventStream(session_id=session_id, stream_id=first_run_id)
    duplicate_id = str(uuid4())
    await event_store.append(
        first_stream,
        -1,
        str(uuid4()),
        [event(session_id, first_run_id, event_id=duplicate_id)],
    )

    second_run_id = str(uuid4())
    second_stream = EventStream(session_id=session_id, stream_id=second_run_id)
    with pytest.raises(DuplicateEvent):
        await event_store.append(
            second_stream,
            -1,
            str(uuid4()),
            [
                event(session_id, second_run_id),
                event(session_id, second_run_id, event_id=duplicate_id),
            ],
        )

    assert await collect(event_store.read_stream(second_stream)) == []


async def test_event_id_uniqueness_is_scoped_to_session(event_store: EventStore) -> None:
    event_id = str(uuid4())
    first_session = str(uuid4())
    second_session = str(uuid4())
    first_run = str(uuid4())
    second_run = str(uuid4())

    await event_store.append(
        EventStream(session_id=first_session, stream_id=first_run),
        -1,
        str(uuid4()),
        [event(first_session, first_run, event_id=event_id)],
    )
    await event_store.append(
        EventStream(session_id=second_session, stream_id=second_run),
        -1,
        str(uuid4()),
        [event(second_session, second_run, event_id=event_id)],
    )


async def test_session_scan_merges_runs_and_pages(event_store: EventStore) -> None:
    session_id = str(uuid4())
    first_run_id = str(uuid4())
    second_run_id = str(uuid4())
    first_stream = EventStream(session_id=session_id, stream_id=first_run_id)
    second_stream = EventStream(session_id=session_id, stream_id=second_run_id)
    first = event(session_id, first_run_id)
    second = event(session_id, second_run_id)
    third = event(session_id, first_run_id)

    await event_store.append(first_stream, -1, str(uuid4()), [first])
    await event_store.append(second_stream, -1, str(uuid4()), [second])
    await event_store.append(first_stream, 0, str(uuid4()), [third])

    timeline = await collect(event_store.scan_session(session_id))
    assert [item.event_id for item in timeline] == [first.event_id, second.event_id, third.event_id]
    page = await collect(event_store.scan_session(session_id, timeline[0].commit_cursor))
    assert [item.event_id for item in page] == [second.event_id, third.event_id]


async def test_framework_state_stream_can_span_runtime_runs(event_store: EventStore) -> None:
    session_id = str(uuid4())
    stream = EventStream(session_id=session_id, stream_id="framework/pi/native-session")
    first = event(session_id, "runtime-1")
    second = event(session_id, "runtime-2")

    await event_store.append(stream, -1, str(uuid4()), [first])
    await event_store.append(stream, 0, str(uuid4()), [second])

    stored = await collect(event_store.read_stream(stream))
    assert [item.run_id for item in stored] == ["runtime-1", "runtime-2"]


async def test_append_snapshots_nested_payload(event_store: EventStore) -> None:
    session_id = str(uuid4())
    run_id = str(uuid4())
    stream = EventStream(session_id=session_id, stream_id=run_id)
    item = event(session_id, run_id)
    item.payload["nested"] = {"value": 1}

    await event_store.append(stream, -1, str(uuid4()), [item])
    item.payload["nested"]["value"] = 2

    stored = await collect(event_store.read_stream(stream))
    assert stored[0].payload["nested"] == {"value": 1}
