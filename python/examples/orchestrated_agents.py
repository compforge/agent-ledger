import asyncio
from uuid import uuid4

from agent_ledger import Actor, LaneRecorder, inspect_session
from agent_ledger.stores.memory import MemoryEventStore


async def main() -> None:
    store = MemoryEventStore()
    session_id = str(uuid4())
    orchestrator = await LaneRecorder.open(
        store=store,
        session_id=session_id,
        run_id="orchestrator-1",
        actor=Actor(type="orchestrator"),
    )
    await orchestrator.start_run(payload={"goal": "research and review a proposal"})

    for role in ("researcher", "reviewer"):
        dispatch = await orchestrator.record(
            "run.agent.dispatched",
            orchestrator.run_id,
            payload={"role": role},
        )
        agent = await orchestrator.child(
            run_id=f"{role}-1",
            actor=Actor(type="agent", framework="plain-loop"),
            causation_id=dispatch.id,
        )
        await agent.start_run(
            payload={
                "agent": {"role": role, "version": "1"},
                "framework": {"name": "plain-loop"},
                "code": {"revision": "example"},
            }
        )
        turn = await agent.start_turn(payload={"role": role})
        await agent.complete_turn(turn)
        await agent.complete_run()

    view = await store.load_session(session_id)
    inspection = inspect_session(view)
    print(f"events={len(inspection.timeline)} run_edges={len(inspection.run_edges)}")


if __name__ == "__main__":
    asyncio.run(main())
