import asyncio

from agent_ledger import Actor, SessionRecorder
from agent_ledger.frameworks.plain_loop import PlainLoopContext, PlainLoopProfile
from agent_ledger.stores.memory import MemoryEventStore


async def main() -> None:
    store = MemoryEventStore()
    actor = Actor(type="agent", id="example", framework="plain-loop")
    recorder = SessionRecorder(
        store=store,
        session_id="session-1",
        run_id="run-1",
        actor=actor,
    )
    profile = PlainLoopProfile()

    await recorder.start_run(payload={"messages": [{"role": "user", "content": "hello"}]})
    await recorder.start_step("step-1")
    attempt = await recorder.before_model_call(
        "step-1",
        payload={"model": "example-model"},
    )
    await recorder.model_completed(
        attempt,
        payload={"message": {"role": "assistant", "content": "Hello!"}},
    )
    await recorder.complete_step("step-1")
    await profile.save(
        recorder,
        PlainLoopContext(
            messages=[
                {"role": "user", "content": "hello"},
                {"role": "assistant", "content": "Hello!"},
            ],
            completed_steps=["step-1"],
        ),
    )

    events = [event async for event in store.read_stream(recorder.stream)]
    recovered = profile.recover(events)
    print(recovered.context.model_dump())


if __name__ == "__main__":
    asyncio.run(main())
