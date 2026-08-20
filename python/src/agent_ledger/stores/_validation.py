from collections.abc import Sequence

from agent_ledger.models import ProposedEvent, canonical_append_digest


def validate_append(
    lane_id: str,
    expected_last_seq: int,
    append_id: str,
    events: Sequence[ProposedEvent],
) -> tuple[tuple[ProposedEvent, ...], str]:
    if expected_last_seq < 0:
        raise ValueError("expected_last_seq must be non-negative")
    if not append_id:
        raise ValueError("append_id must not be empty")
    if not events:
        raise ValueError("append requires at least one event")
    batch = tuple(event.model_copy(deep=True) for event in events)
    seen: set[str] = set()
    for event in batch:
        if event.lane_id != lane_id:
            raise ValueError("all events must belong to the target lane")
        if event.id in seen:
            raise ValueError(f"duplicate event id {event.id!r} in append batch")
        seen.add(event.id)
    return batch, canonical_append_digest(batch)
