from collections.abc import AsyncIterator, Sequence
from typing import Protocol

from agent_ledger.models import (
    Action,
    Actor,
    AppendReceipt,
    Attempt,
    Checkpoint,
    Lane,
    ProposedCheckpoint,
    ProposedEvent,
    RunView,
    SessionView,
    StoredEvent,
    Turn,
)


class ActorStore(Protocol):
    async def create_actor(self, actor: Actor) -> None: ...

    async def get_actor(self, actor_id: str) -> Actor | None: ...


class EventStore(ActorStore, Protocol):
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

    async def load_run(self, session_id: str, run_id: str) -> RunView: ...


class CheckpointStore(ActorStore, Protocol):
    async def save_checkpoint(
        self,
        expected_revision: int,
        checkpoint: ProposedCheckpoint,
    ) -> Checkpoint: ...

    async def get_checkpoint(self, checkpoint_id: str) -> Checkpoint | None: ...

    async def load_latest_checkpoint(self, checkpoint_key: str) -> Checkpoint | None: ...
