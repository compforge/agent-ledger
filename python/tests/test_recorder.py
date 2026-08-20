from __future__ import annotations

import pytest

from agent_ledger import (
    Actor,
    EventType,
    LaneRecorder,
    ProposedEvent,
    StoreError,
    Turn,
    inspect_run,
    inspect_session,
    new_id,
)
from agent_ledger.frameworks.plain_loop import PlainLoopContext, PlainLoopProfile
from agent_ledger.stores.memory import MemoryEventStore


async def test_model_hook_is_durable_before_call() -> None:
    store = MemoryEventStore()
    recorder = await _recorder(store)
    turn = await recorder.start_turn()

    attempt = await recorder.before_model_call(turn, payload={"model": "test"})
    events_before_call = [event async for event in store.read_lane(recorder.lane.id)]
    await recorder.model_completed(attempt, payload={"message": "done"})

    assert [event.event_type for event in events_before_call] == [
        EventType.TURN_STARTED,
        EventType.ATTEMPT_REQUESTED,
    ]


async def test_failed_prewrite_prevents_external_call() -> None:
    class FailingStore(MemoryEventStore):
        async def append(self, *args: object, **kwargs: object):  # type: ignore[no-untyped-def]
            raise StoreError("unavailable")

    recorder = await _recorder(FailingStore())
    created = Turn(lane_id=recorder.lane.id)
    await recorder.store.create_turn(created)
    called = False
    with pytest.raises(StoreError):
        await recorder.before_tool_call(created, payload={"name": "charge"})
        called = True
    assert not called


async def test_retry_keeps_action_and_increments_attempt_number() -> None:
    store = MemoryEventStore()
    recorder = await _recorder(store)
    turn = await recorder.start_turn()
    first = await recorder.before_model_call(turn, payload={"model": "test"})
    await recorder.model_failed(first, RuntimeError("limited"))
    second = await recorder.retry(first.action_id, 2, payload={"model": "test"})

    inspection = inspect_session(await store.load_session(recorder.session_id))

    assert first.action_id == second.action_id
    assert second.attempt_no == 2
    assert len(inspection.unresolved_attempts) == 1
    assert inspection.unresolved_attempts[0].attempt_id == second.attempt_id


async def test_orchestrator_links_multiple_agent_runs() -> None:
    store = MemoryEventStore()
    parent = await LaneRecorder.open(
        store=store,
        session_id="session",
        run_id="orchestrator-run",
        actor=Actor(type="orchestrator"),
    )
    await parent.start_run()
    children: list[LaneRecorder] = []
    for role in ("researcher", "reviewer"):
        trigger = await parent.record(
            "run.agent.dispatched",
            parent.run_id,
            payload={"role": role},
        )
        child = await parent.child(
            run_id=f"{role}-run",
            actor=Actor(type="agent"),
            causation_id=trigger.id,
        )
        await child.start_run()
        children.append(child)

    inspection = inspect_session(await store.load_session(parent.session_id))

    assert len(inspection.run_edges) == 2
    assert {edge.parent_run_id for edge in inspection.run_edges} == {parent.run_id}
    assert {edge.child_run_id for edge in inspection.run_edges} == {
        child.run_id for child in children
    }


async def test_plain_loop_profile_restores_snapshot_and_tail() -> None:
    store = MemoryEventStore()
    recorder = await _recorder(store)
    profile = PlainLoopProfile()
    await profile.save(
        recorder,
        PlainLoopContext(messages=[{"role": "user", "content": "hello"}]),
    )
    turn = await recorder.start_turn()
    attempt = await recorder.before_model_call(turn, payload={"model": "test"})
    await recorder.model_completed(
        attempt,
        payload={"message": {"role": "assistant", "content": "hi"}},
    )
    await recorder.complete_turn(turn)

    recovered = profile.recover(await store.load_session(recorder.session_id), recorder.lane.id)

    assert recovered.context.messages[-1]["content"] == "hi"
    assert recovered.context.completed_turns == [turn.id]
    assert recovered.unresolved_attempts == ()
    assert recovered.restored_through_seq == 5


async def test_recorder_open_resumes_lane_head() -> None:
    store = MemoryEventStore()
    original = await _recorder(store)
    await original.start_run()
    resumed = await LaneRecorder.open(
        store=store,
        session_id=original.session_id,
        run_id=original.run_id,
        actor=original.actor,
    )
    await resumed.complete_run()

    events = [event async for event in store.read_lane(original.lane.id)]
    assert [event.seq for event in events] == [1, 2]


async def test_checkpoint_link_and_run_completion_are_one_inspectable_append() -> None:
    store = MemoryEventStore()
    recorder = await _recorder(store)
    turn = await recorder.start_turn()
    unresolved = await recorder.before_tool_call(turn, payload={"tool": "charge"})

    checkpoint_linked = ProposedEvent(
        lane_id=recorder.lane.id,
        subject_id=recorder.lane.id,
        event_type=EventType.LANE_FRAMEWORK_CHECKPOINT_LINKED,
        actor_id=recorder.actor.id,
        payload={
            "checkpoint_id": new_id(),
            "profile": "plain-loop",
            "profile_version": "1",
            "metadata": {"reason": "idle"},
        },
    )
    run_completed = ProposedEvent(
        lane_id=recorder.lane.id,
        subject_id=recorder.run_id,
        event_type=EventType.RUN_COMPLETED,
        actor_id=recorder.actor.id,
        causation_id=checkpoint_linked.id,
        payload={"result": "done"},
    )
    append_id = new_id()
    receipt = await recorder.append([checkpoint_linked, run_completed], append_id=append_id)
    repeated = await recorder.append([checkpoint_linked, run_completed], append_id=append_id)
    inspection = inspect_run(await store.load_run(recorder.session_id, recorder.run_id))
    stored_link = inspection.linked_checkpoints[0].event
    stored_completion = inspection.terminal_events[0]

    assert receipt.event_ids == (checkpoint_linked.id, run_completed.id)
    assert repeated == receipt
    assert recorder.lane.last_seq == receipt.last_seq
    assert stored_link.seq + 1 == stored_completion.seq
    assert stored_link.committed_at == stored_completion.committed_at
    assert stored_completion.causation_id == stored_link.id
    assert [event.id for event in inspection.terminal_events] == [run_completed.id]
    assert (
        inspection.linked_checkpoints[0].checkpoint_id == checkpoint_linked.payload["checkpoint_id"]
    )
    assert [attempt.attempt_id for attempt in inspection.unresolved_attempts] == [
        unresolved.attempt_id
    ]


async def _recorder(store: MemoryEventStore) -> LaneRecorder:
    return await LaneRecorder.open(
        store=store,
        session_id="session",
        run_id="run",
        actor=Actor(type="agent", framework="plain-loop"),
    )
