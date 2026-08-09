from collections.abc import Sequence
from typing import Generic, Protocol, TypeVar

from agent_ledger.models import StoredEvent
from agent_ledger.recorder import SessionRecorder

ContextT = TypeVar("ContextT")


class FrameworkProfile(Protocol, Generic[ContextT]):
    profile_id: str
    profile_version: str

    async def save(self, recorder: SessionRecorder, context: ContextT) -> StoredEvent: ...

    def restore(self, events: Sequence[StoredEvent]) -> ContextT: ...
