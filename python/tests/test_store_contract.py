from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest

from agent_ledger import (
    Action,
    Actor,
    Attempt,
    DuplicateEvent,
    EntityConflict,
    IdempotencyViolation,
    Lane,
    LaneConflict,
    ProposedEvent,
    StoredEvent,
    SubjectMismatch,
    Turn,
    new_id,
)
from agent_ledger.store import EventStore


@dataclass(frozen=True)
class Target:
    lane: Lane
    actor: Actor

    @property
    def id(self) -> str:
        return self.lane.id

    @property
    def session_id(self) -> str:
        return self.lane.session_id


async def lane(
    store: EventStore,
    session_id: str = "session",
    run_id: str = "run",
    name: str = "main",
) -> Target:
    value = Lane(session_id=session_id, run_id=run_id, name=name)
    actor = Actor(type="harness", framework="test")
    await store.create_actor(actor)
    await store.create_lane(value)
    return Target(value, actor)


def event(target: Target, *, event_id: str | None = None) -> ProposedEvent:
    return ProposedEvent(
        id=event_id or new_id(),
        lane_id=target.id,
        subject_id=target.id,
        event_type="lane.test.recorded",
        actor_id=target.actor.id,
        payload={"value": new_id()},
    )


async def collect(iterator: AsyncIterator[StoredEvent]) -> list[StoredEvent]:
    return [item async for item in iterator]


async def test_atomic_batch_sequences_and_reads(event_store: EventStore) -> None:
    target = await lane(event_store)
    first, second = event(target), event(target)

    receipt = await event_store.append(target.id, 0, new_id(), [first, second])

    assert (receipt.first_seq, receipt.last_seq) == (1, 2)
    stored = await collect(event_store.read_lane(target.id))
    assert [item.id for item in stored] == [first.id, second.id]
    assert [item.seq for item in stored] == [1, 2]
    assert all(item.committed_at == receipt.committed_at for item in stored)
    assert (await event_store.get_lane(target.id)).last_seq == 2  # type: ignore[union-attr]


async def test_append_is_idempotent_by_content(event_store: EventStore) -> None:
    target = await lane(event_store)
    item = event(target)
    append_id = new_id()

    original = await event_store.append(target.id, 0, append_id, [item])
    repeated = await event_store.append(target.id, 0, append_id, [item])

    assert repeated == original
    assert len(await collect(event_store.read_lane(target.id))) == 1
    with pytest.raises(IdempotencyViolation):
        await event_store.append(target.id, 0, append_id, [event(target)])


async def test_lane_optimistic_concurrency(event_store: EventStore) -> None:
    target = await lane(event_store)
    await event_store.append(target.id, 0, new_id(), [event(target)])

    with pytest.raises(LaneConflict) as caught:
        await event_store.append(target.id, 0, new_id(), [event(target)])

    assert caught.value.actual_last_seq == 1
    assert len(await collect(event_store.read_lane(target.id))) == 1


async def test_duplicate_event_id_is_global(event_store: EventStore) -> None:
    duplicate_id = new_id()
    first = await lane(event_store, session_id="session-1", run_id="run-1")
    second = await lane(event_store, session_id="session-2", run_id="run-2")
    await event_store.append(first.id, 0, new_id(), [event(first, event_id=duplicate_id)])

    with pytest.raises(DuplicateEvent):
        await event_store.append(second.id, 0, new_id(), [event(second, event_id=duplicate_id)])


async def test_subject_must_resolve_to_target_lane(event_store: EventStore) -> None:
    first = await lane(event_store, name="main")
    second = await lane(event_store, name="branch")
    turn = Turn(lane_id=first.id)
    await event_store.create_turn(turn)
    wrong = ProposedEvent(
        lane_id=second.id,
        subject_id=turn.id,
        event_type="turn.started",
        actor_id=second.actor.id,
    )

    with pytest.raises(SubjectMismatch):
        await event_store.append(second.id, 0, new_id(), [wrong])


async def test_attempt_number_is_unique_within_action(event_store: EventStore) -> None:
    target = await lane(event_store)
    turn = Turn(lane_id=target.id)
    action = Action(turn_id=turn.id, type="model_call")
    await event_store.create_turn(turn)
    await event_store.create_action(action)
    await event_store.create_attempt(Attempt(action_id=action.id, attempt_no=1))

    with pytest.raises(EntityConflict):
        await event_store.create_attempt(Attempt(action_id=action.id, attempt_no=1))


async def test_load_session_contains_structure_and_events(event_store: EventStore) -> None:
    target = await lane(event_store)
    turn = Turn(lane_id=target.id)
    action = Action(turn_id=turn.id, type="compact")
    attempt = Attempt(action_id=action.id, attempt_no=1)
    await event_store.create_turn(turn)
    await event_store.create_action(action)
    await event_store.create_attempt(attempt)
    requested = ProposedEvent(
        lane_id=target.id,
        subject_id=attempt.id,
        event_type="attempt.requested",
        actor_id=target.actor.id,
    )
    await event_store.append(target.id, 0, new_id(), [requested])

    view = await event_store.load_session(target.session_id)

    assert [item.id for item in view.actors] == [target.actor.id]
    assert [item.id for item in view.lanes] == [target.id]
    assert [item.id for item in view.turns] == [turn.id]
    assert [item.id for item in view.actions] == [action.id]
    assert [item.id for item in view.attempts] == [attempt.id]
    assert [item.id for item in view.events] == [requested.id]


async def test_committed_event_content_is_immutable(event_store: EventStore) -> None:
    target = await lane(event_store)
    item = event(target)
    item.payload["nested"] = {"value": 1}
    await event_store.append(target.id, 0, new_id(), [item])
    item.payload["nested"]["value"] = 2

    stored = await collect(event_store.read_lane(target.id))
    assert stored[0].payload["nested"] == {"value": 1}
    stored[0].payload["nested"]["value"] = 3
    reloaded = await collect(event_store.read_lane(target.id))
    assert reloaded[0].payload["nested"] == {"value": 1}
