from collections.abc import Sequence

from agent_ledger.errors import StoreError
from agent_ledger.models import ProposedEvent, RunStream, canonical_append_digest


def validate_append(
    stream: RunStream,
    expected_version: int,
    append_id: str,
    events: Sequence[ProposedEvent],
) -> tuple[tuple[ProposedEvent, ...], str]:
    if expected_version < -1:
        raise StoreError("expected_version must be at least -1")
    if not append_id:
        raise StoreError("append_id must not be empty")

    # Pydantic's frozen model prevents field reassignment but does not freeze nested JSON.
    # Snapshot the proposal before the first await so caller mutation cannot change a commit.
    batch = tuple(event.model_copy(deep=True) for event in events)
    if not batch:
        raise StoreError("an append batch must contain at least one event")

    event_ids: set[str] = set()
    for event in batch:
        if event.stream != stream:
            raise StoreError("all events must belong to the target run stream")
        if event.event_id in event_ids:
            raise StoreError(f"event_id {event.event_id!r} appears more than once in the batch")
        event_ids.add(event.event_id)

    return batch, canonical_append_digest(batch)
