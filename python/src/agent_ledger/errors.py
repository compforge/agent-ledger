class AgentLedgerError(Exception):
    """Base class for ledger errors."""


class StoreError(AgentLedgerError):
    """The selected store could not complete an operation."""


class StreamConflict(StoreError):
    def __init__(self, expected_version: int, actual_version: int) -> None:
        self.expected_version = expected_version
        self.actual_version = actual_version
        super().__init__(
            f"stream version conflict: expected {expected_version}, actual {actual_version}"
        )


class IdempotencyViolation(StoreError):
    def __init__(self, append_id: str) -> None:
        self.append_id = append_id
        super().__init__(f"append_id {append_id!r} was already used for different events")


class DuplicateEvent(StoreError):
    def __init__(self, event_id: str) -> None:
        self.event_id = event_id
        super().__init__(f"event_id {event_id!r} already exists")


class InvalidCursor(StoreError):
    def __init__(self, cursor: str) -> None:
        self.cursor = cursor
        super().__init__(f"invalid commit cursor: {cursor!r}")
