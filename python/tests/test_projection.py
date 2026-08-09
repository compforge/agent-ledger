from __future__ import annotations

from uuid import uuid4

from agent_ledger import Actor, SessionRecorder, project_atif
from agent_ledger.stores.memory import MemoryEventStore


async def test_atif_projection_includes_tools_and_subagents() -> None:
    store = MemoryEventStore()
    parent = SessionRecorder(
        store=store,
        session_id=str(uuid4()),
        run_id="parent",
        actor=Actor(type="agent", id="parent"),
    )
    await parent.start_run(payload={"messages": [{"role": "user", "content": "find it"}]})
    model = await parent.before_model_call("step-1", payload={"model": "test-model"})
    await parent.model_completed(model, payload={"message": "searching"})
    tool = await parent.before_tool_call(
        "step-1",
        payload={"name": "search", "arguments": {"query": "it"}},
    )
    await parent.tool_completed(tool, payload={"result": {"hits": 1}})

    child = parent.child(
        run_id="child",
        actor=Actor(type="agent", id="child"),
        caused_by_event_id=tool.requested_event_id,
    )
    await child.start_run()

    events = [event async for event in store.scan_session(parent.stream.session_id)]
    trajectories = project_atif(events, agent_name="example", agent_version="1")

    assert len(trajectories) == 1
    trajectory = trajectories[0]
    assert trajectory["schema_version"] == "ATIF-v1.7"
    assert trajectory["agent"]["model_name"] == "test-model"
    agent_step = next(step for step in trajectory["steps"] if step["source"] == "agent")
    assert agent_step["tool_calls"][0]["function_name"] == "search"
    assert agent_step["observation"]["results"][0]["content"] == '{"hits": 1}'
    assert trajectory["subagent_trajectories"][0]["trajectory_id"] == "child"
