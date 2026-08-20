import asyncio

from agent_ledger import Actor, LaneRecorder
from agent_ledger.frameworks.plain_loop import PlainLoopContext, PlainLoopProfile
from agent_ledger.stores.memory import MemoryEventStore


async def main() -> None:
    store = MemoryEventStore()
    recorder = await LaneRecorder.open(
        store=store,
        session_id="session-1",
        run_id="run-1",
        actor=Actor(type="agent", framework="plain-loop"),
    )
    profile = PlainLoopProfile()

    await recorder.start_run(payload={"messages": [{"role": "user", "content": "hello"}]})
    turn = await recorder.start_turn()
    attempt = await recorder.before_model_call(turn, payload={"model": "example-model"})
    await recorder.model_completed(
        attempt,
        payload={"message": {"role": "assistant", "content": "Hello!"}},
    )
    await recorder.complete_turn(turn)
    await profile.save(
        recorder,
        PlainLoopContext(
            messages=[
                {"role": "user", "content": "hello"},
                {"role": "assistant", "content": "Hello!"},
            ],
            completed_turns=[turn.id],
        ),
    )

    view = await store.load_session(recorder.session_id)
    recovered = profile.recover(view, recorder.lane.id)
    print(recovered.context.model_dump())


if __name__ == "__main__":
    asyncio.run(main())
