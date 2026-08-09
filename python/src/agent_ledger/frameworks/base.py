from collections.abc import Sequence
from typing import Generic, Protocol, TypeVar

from agent_ledger.adapters import AdapterDescriptor
from agent_ledger.models import StoredEvent
from agent_ledger.recorder import SessionRecorder

ContextT = TypeVar("ContextT")


class StateAdapter(Protocol, Generic[ContextT]):
    """Persists and rebuilds framework-native state without defining hook APIs."""

    descriptor: AdapterDescriptor

    async def save(self, recorder: SessionRecorder, context: ContextT) -> StoredEvent: ...

    def restore(self, events: Sequence[StoredEvent]) -> ContextT: ...
