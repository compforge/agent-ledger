from collections.abc import AsyncIterator, Sequence
from typing import Protocol

from agent_ledger.models import (
    Action,
    Actor,
    AppendReceipt,
    Attempt,
    Lane,
    ProposedEvent,
    SessionView,
    StoredEvent,
    Turn,
)


class EventStore(Protocol):
    async def create_actor(self, actor: Actor) -> None: ...

    async def get_actor(self, actor_id: str) -> Actor | None: ...

    async def create_lane(self, lane: Lane) -> None: ...

    async def get_lane(self, lane_id: str) -> Lane | None: ...

    async def find_lane(self, session_id: str, run_id: str, name: str) -> Lane | None: ...

    async def create_turn(self, turn: Turn) -> None: ...

    async def get_turn(self, turn_id: str) -> Turn | None: ...

    async def create_action(self, action: Action) -> None: ...

    async def get_action(self, action_id: str) -> Action | None: ...

    async def create_attempt(self, attempt: Attempt) -> None: ...

    async def get_attempt(self, attempt_id: str) -> Attempt | None: ...

    async def append(
        self,
        lane_id: str,
        expected_last_seq: int,
        append_id: str,
        events: Sequence[ProposedEvent],
    ) -> AppendReceipt: ...

    def read_lane(
        self,
        lane_id: str,
        after_seq: int = 0,
    ) -> AsyncIterator[StoredEvent]: ...

    async def load_session(self, session_id: str) -> SessionView: ...
