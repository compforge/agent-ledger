from typing import Generic, Protocol, TypeVar

from agent_ledger.adapters import AdapterDescriptor
from agent_ledger.models import SessionView, StoredEvent
from agent_ledger.recorder import LaneRecorder

ContextT = TypeVar("ContextT")


class StateAdapter(Protocol, Generic[ContextT]):
    """Persists and rebuilds framework-native state without defining hook APIs."""

    descriptor: AdapterDescriptor

    async def save(self, recorder: LaneRecorder, context: ContextT) -> StoredEvent: ...

    def restore(self, view: SessionView, lane_id: str) -> ContextT: ...
