class AgentLedgerError(Exception):
    """Base class for ledger errors."""


class StoreError(AgentLedgerError):
    """The selected store could not complete an operation."""


class LaneConflict(StoreError):
    def __init__(self, expected_last_seq: int, actual_last_seq: int) -> None:
        self.expected_last_seq = expected_last_seq
        self.actual_last_seq = actual_last_seq
        super().__init__(
            f"lane sequence conflict: expected {expected_last_seq}, actual {actual_last_seq}"
        )


class CheckpointConflict(StoreError):
    def __init__(self, expected_revision: int, actual_revision: int) -> None:
        self.expected_revision = expected_revision
        self.actual_revision = actual_revision
        super().__init__(
            f"checkpoint revision conflict: expected {expected_revision}, actual {actual_revision}"
        )


class IdempotencyViolation(StoreError):
    def __init__(self, append_id: str) -> None:
        self.append_id = append_id
        super().__init__(f"append id {append_id!r} was already used for different events")


class CheckpointIdempotencyViolation(StoreError):
    def __init__(self, checkpoint_id: str) -> None:
        self.checkpoint_id = checkpoint_id
        super().__init__(f"checkpoint id {checkpoint_id!r} was already used for different content")


class DuplicateEvent(StoreError):
    def __init__(self, event_id: str) -> None:
        self.event_id = event_id
        super().__init__(f"event id {event_id!r} already exists")


class EntityConflict(StoreError):
    def __init__(self, entity: str, entity_id: str) -> None:
        self.entity = entity
        self.entity_id = entity_id
        super().__init__(f"{entity} {entity_id!r} already exists or violates a uniqueness rule")


class EntityNotFound(StoreError):
    def __init__(self, entity: str, entity_id: str) -> None:
        self.entity = entity
        self.entity_id = entity_id
        super().__init__(f"{entity} {entity_id!r} does not exist")


class SubjectMismatch(StoreError):
    def __init__(self, event_id: str, lane_id: str) -> None:
        self.event_id = event_id
        self.lane_id = lane_id
        super().__init__(f"event {event_id!r} subject does not belong to lane {lane_id!r}")
