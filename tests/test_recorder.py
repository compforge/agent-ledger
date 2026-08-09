from __future__ import annotations

from uuid import uuid4

import pytest

from agent_ledger import Actor, EventType, SessionRecorder, StoreError, inspect_session
from agent_ledger.frameworks.plain_loop import PlainLoopContext, PlainLoopProfile
from agent_ledger.stores.memory import MemoryEventStore


async def test_model_hook_is_durable_before_call() -> None:
    store = MemoryEventStore()
    recorder = _recorder(store)
    called = False

    attempt = await recorder.before_model_call("step-1", payload={"model": "test"})
    events_before_call = [event async for event in store.read_stream(recorder.stream)]
    called = True
    await recorder.model_completed(attempt, payload={"message": "done"})

    assert called
    assert [event.event_type for event in events_before_call] == [EventType.MODEL_REQUESTED]


async def test_failed_prewrite_prevents_external_call() -> None:
    class FailingStore(MemoryEventStore):
        async def append(self, *args: object, **kwargs: object):  # type: ignore[no-untyped-def]
            raise StoreError("unavailable")

    recorder = _recorder(FailingStore())
    called = False

    with pytest.raises(StoreError):
        await recorder.before_tool_call("step-1", payload={"name": "charge"})
        called = True

    assert not called


async def test_retry_keeps_step_and_gets_new_attempt() -> None:
    store = MemoryEventStore()
    recorder = _recorder(store)
    first = await recorder.before_model_call("step-1", payload={"model": "test"})
    await recorder.model_failed(first, RuntimeError("limited"))
    second = await recorder.before_model_call("step-1", payload={"model": "test"})

    events = [event async for event in store.scan_session(recorder.stream.session_id)]
    inspection = inspect_session(events)

    assert first.attempt_id != second.attempt_id
    assert len(inspection.unresolved_attempts) == 1
    assert inspection.unresolved_attempts[0].attempt_id == second.attempt_id
    assert inspection.unresolved_attempts[0].step_id == "step-1"


async def test_child_run_forms_causal_edge() -> None:
    store = MemoryEventStore()
    parent = _recorder(store)
    trigger = await parent.start_step("delegate")
    child = parent.child(
        run_id=str(uuid4()),
        actor=Actor(type="agent", id="child"),
        caused_by_event_id=trigger.event_id,
    )
    await child.start_run()

    events = [event async for event in store.scan_session(parent.stream.session_id)]
    inspection = inspect_session(events)

    assert len(inspection.run_edges) == 1
    assert inspection.run_edges[0].parent_run_id == parent.stream.run_id
    assert inspection.run_edges[0].child_run_id == child.stream.run_id
    assert inspection.run_edges[0].caused_by_event_id == trigger.event_id


async def test_plain_loop_profile_restores_snapshot_and_tail() -> None:
    store = MemoryEventStore()
    recorder = _recorder(store)
    profile = PlainLoopProfile()
    await profile.save(
        recorder,
        PlainLoopContext(messages=[{"role": "user", "content": "hello"}]),
    )
    attempt = await recorder.before_model_call("step-1", payload={"model": "test"})
    await recorder.model_completed(
        attempt,
        payload={"message": {"role": "assistant", "content": "hi"}},
    )
    await recorder.complete_step("step-1")

    events = [event async for event in store.read_stream(recorder.stream)]
    recovered = profile.recover(events)

    assert recovered.context.messages[-1]["content"] == "hi"
    assert recovered.context.completed_steps == ["step-1"]
    assert recovered.unresolved_attempts == ()
    assert recovered.restored_through_version == 3


async def test_recorder_resume_continues_at_last_version() -> None:
    store = MemoryEventStore()
    original = _recorder(store)
    await original.start_run()
    resumed = await SessionRecorder.resume(
        store=store,
        session_id=original.stream.session_id,
        run_id=original.stream.run_id,
        actor=original.actor,
    )
    await resumed.complete_run()

    events = [event async for event in store.read_stream(original.stream)]
    assert [event.stream_version for event in events] == [0, 1]


def _recorder(store: MemoryEventStore) -> SessionRecorder:
    return SessionRecorder(
        store=store,
        session_id=str(uuid4()),
        run_id=str(uuid4()),
        actor=Actor(type="agent", id="test", framework="plain-loop"),
    )
