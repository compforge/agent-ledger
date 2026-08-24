from agent_ledger import Actor, LaneRecorder, project_atif
from agent_ledger.stores.memory import MemoryEventStore


async def test_atif_projection_uses_action_type_and_attempt_events() -> None:
    store = MemoryEventStore()
    recorder = await LaneRecorder.open(
        store=store,
        session_id="session",
        run_id="run",
        actor=Actor(type="agent"),
    )
    await recorder.start_run(payload={"messages": [{"role": "user", "content": "hello"}]})
    turn = await recorder.start_turn()
    model = await recorder.before_model_call(
        turn,
        payload={"model": {"id": "test-model"}, "input": [{"role": "user"}]},
    )
    await recorder.model_completed(
        model,
        payload={"output": {"role": "assistant", "content": "checking"}},
    )
    tool = await recorder.before_tool_call(
        turn,
        action_key="call-1",
        payload={"tool_name": "lookup", "input": {"q": "x"}},
    )
    await recorder.tool_completed(tool, payload={"output": "done"})

    trajectories = project_atif(await store.load_session("session"))

    assert len(trajectories) == 1
    assert trajectories[0]["agent"]["model_name"] == "test-model"
    assert trajectories[0]["steps"][-1]["tool_calls"][0]["function_name"] == "lookup"
    assert trajectories[0]["steps"][-1]["observation"]["results"][0]["content"] == "done"
