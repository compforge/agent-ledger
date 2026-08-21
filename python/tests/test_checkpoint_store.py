from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError

from agent_ledger import (
    Actor,
    ArtifactRef,
    CheckpointAnchor,
    CheckpointConflict,
    CheckpointIdempotencyViolation,
    Lane,
    ProposedCheckpoint,
    ProposedEvent,
    new_id,
)
from agent_ledger.store import CheckpointStore, EventStore
from agent_ledger.stores.sql import SqlEventStore


async def test_checkpoint_versions_and_idempotency(event_store: EventStore) -> None:
    store: CheckpointStore = event_store  # type: ignore[assignment]
    actor = Actor(type="harness", framework="test")
    await store.create_actor(actor)
    first = ProposedCheckpoint(
        key="native-session",
        actor_id=actor.id,
        format="application/vnd.compforge.test.state+json;version=1",
        state={"messages": ["hello"]},
    )

    saved = await store.save_checkpoint(0, first)

    assert saved.revision == 1
    assert await store.save_checkpoint(0, first) == saved
    assert await store.get_checkpoint(first.id) == saved
    assert await store.load_latest_checkpoint(first.key) == saved

    second = ProposedCheckpoint(
        key=first.key,
        actor_id=actor.id,
        format=first.format,
        state={"messages": ["hello", "world"]},
    )
    latest = await store.save_checkpoint(1, second)
    assert latest.revision == 2
    assert await store.load_latest_checkpoint(first.key) == latest

    with pytest.raises(CheckpointConflict):
        await store.save_checkpoint(
            0,
            ProposedCheckpoint(
                key=first.key,
                actor_id=actor.id,
                format=first.format,
                state={"messages": []},
            ),
        )
    with pytest.raises(CheckpointIdempotencyViolation):
        await store.save_checkpoint(
            2,
            first.model_copy(update={"state": {"messages": ["different"]}}),
        )


async def test_checkpoint_can_anchor_a_ledger_event(event_store: EventStore) -> None:
    store: CheckpointStore = event_store  # type: ignore[assignment]
    actor = Actor(type="harness", framework="test")
    lane = Lane(session_id="session", run_id="run")
    await event_store.create_actor(actor)
    await event_store.create_lane(lane)
    event = ProposedEvent(
        lane_id=lane.id,
        subject_id=lane.id,
        event_type="lane.state.changed",
        actor_id=actor.id,
    )
    await event_store.append(lane.id, 0, new_id(), [event])
    checkpoint = ProposedCheckpoint(
        key="native-session",
        actor_id=actor.id,
        format="application/vnd.compforge.test.state+json;version=1",
        state={"cursor": 1},
        anchor=CheckpointAnchor(
            lane_id=lane.id,
            last_applied_seq=1,
            last_applied_event_id=event.id,
        ),
    )

    saved = await store.save_checkpoint(0, checkpoint)

    assert saved.anchor == checkpoint.anchor


async def test_checkpoint_can_reference_an_artifact(event_store: EventStore) -> None:
    store: CheckpointStore = event_store  # type: ignore[assignment]
    actor = Actor(type="harness", framework="test")
    await store.create_actor(actor)
    checkpoint = ProposedCheckpoint(
        key="large-native-session",
        actor_id=actor.id,
        format="application/vnd.compforge.test.state+json;version=1",
        artifact_ref=ArtifactRef(
            uri="memory://checkpoint",
            sha256="0" * 64,
            size=1024,
            content_type="application/json",
        ),
    )

    saved = await store.save_checkpoint(0, checkpoint)

    assert saved.artifact_ref == checkpoint.artifact_ref
    assert saved.state is None


async def test_sql_checkpoint_insert_reconciliation(event_store: EventStore) -> None:
    if not isinstance(event_store, SqlEventStore):
        pytest.skip("SQL store only")
    actor = Actor(type="harness", framework="test")
    await event_store.create_actor(actor)
    checkpoint = ProposedCheckpoint(
        key="native-session",
        actor_id=actor.id,
        format="application/json",
        state={"messages": ["hello"]},
    )
    saved = await event_store.save_checkpoint(0, checkpoint)
    insert_error = IntegrityError("INSERT", {}, Exception("constraint"))

    assert (
        await event_store._reconcile_checkpoint_integrity_error(checkpoint, 0, insert_error)
        == saved
    )
    with pytest.raises(CheckpointIdempotencyViolation):
        await event_store._reconcile_checkpoint_integrity_error(
            checkpoint.model_copy(update={"state": {"messages": ["different"]}}),
            0,
            insert_error,
        )
    with pytest.raises(CheckpointConflict):
        await event_store._reconcile_checkpoint_integrity_error(
            ProposedCheckpoint(
                key=checkpoint.key,
                actor_id=actor.id,
                format=checkpoint.format,
                state={"messages": ["other"]},
            ),
            0,
            insert_error,
        )
