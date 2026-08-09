import asyncio
from uuid import uuid4

from agent_ledger import Actor, SessionRecorder, inspect_session
from agent_ledger.stores.memory import MemoryEventStore


async def main() -> None:
    store = MemoryEventStore()
    session_id = str(uuid4())
    orchestrator = SessionRecorder(
        store=store,
        session_id=session_id,
        run_id="orchestrator-1",
        actor=Actor(type="orchestrator", id="planner"),
    )
    await orchestrator.start_run(payload={"goal": "research and review a proposal"})

    for role in ("researcher", "reviewer"):
        dispatch = await orchestrator.record(
            "orchestration.agent.dispatched",
            payload={"role": role},
        )
        agent = orchestrator.child(
            run_id=f"{role}-1",
            actor=Actor(type="agent", id=role, framework="plain-loop"),
            caused_by_event_id=dispatch.event_id,
        )
        await agent.start_run(
            payload={
                "agent": {"id": role, "version": "1"},
                "framework": {"name": "plain-loop"},
                "code": {"revision": "example"},
            }
        )
        step_id = str(uuid4())
        await agent.start_step(step_id, payload={"role": role})
        await agent.complete_step(step_id)
        await agent.complete_run()

    events = [event async for event in store.scan_session(session_id)]
    inspection = inspect_session(events)
    print(f"events={len(inspection.timeline)} run_edges={len(inspection.run_edges)}")


if __name__ == "__main__":
    asyncio.run(main())
