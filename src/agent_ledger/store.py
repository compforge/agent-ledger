from collections.abc import AsyncIterator, Sequence
from typing import Protocol

from agent_ledger.models import CommitReceipt, ProposedEvent, RunStream, StoredEvent


class EventStore(Protocol):
    async def append(
        self,
        stream: RunStream,
        expected_version: int,
        append_id: str,
        events: Sequence[ProposedEvent],
    ) -> CommitReceipt: ...

    def read_stream(
        self,
        stream: RunStream,
        after_version: int = -1,
    ) -> AsyncIterator[StoredEvent]: ...

    def scan_session(
        self,
        session_id: str,
        after_cursor: str | None = None,
    ) -> AsyncIterator[StoredEvent]: ...
