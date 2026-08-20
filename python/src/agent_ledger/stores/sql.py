from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    Index,
    Integer,
    String,
    UniqueConstraint,
    select,
)
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from agent_ledger.errors import (
    AgentLedgerError,
    CheckpointConflict,
    CheckpointIdempotencyViolation,
    DuplicateEvent,
    EntityConflict,
    EntityNotFound,
    IdempotencyViolation,
    LaneConflict,
    StoreError,
    SubjectMismatch,
)
from agent_ledger.models import (
    Action,
    Actor,
    AppendReceipt,
    Attempt,
    Checkpoint,
    CheckpointAnchor,
    Lane,
    ProposedCheckpoint,
    ProposedEvent,
    SessionView,
    StoredEvent,
    Turn,
    utc_now,
)
from agent_ledger.stores._validation import validate_append

_ID_LENGTH = 191


class _Base(DeclarativeBase):
    pass


class _Lane(_Base):
    __tablename__ = "ledger_lanes"
    __table_args__ = (
        UniqueConstraint("session_id", "run_id", "name", name="uq_ledger_lanes_owner_name"),
        Index("ix_ledger_lanes_owner", "session_id", "run_id"),
        Index("ix_ledger_lanes_parent", "parent_lane_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(_ID_LENGTH), nullable=False)
    run_id: Mapped[str] = mapped_column(String(_ID_LENGTH), nullable=False)
    name: Mapped[str] = mapped_column(String(_ID_LENGTH), nullable=False)
    parent_lane_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    last_seq: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _Actor(_Base):
    __tablename__ = "ledger_actors"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    type: Mapped[str] = mapped_column(String(64), nullable=False)
    framework: Mapped[str | None] = mapped_column(String(_ID_LENGTH), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _Turn(_Base):
    __tablename__ = "ledger_turns"
    __table_args__ = (Index("ix_ledger_turns_lane", "lane_id"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    lane_id: Mapped[str] = mapped_column(String(36), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _Action(_Base):
    __tablename__ = "ledger_actions"
    __table_args__ = (
        Index("ix_ledger_actions_turn", "turn_id"),
        Index("ix_ledger_actions_parent", "parent_action_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    turn_id: Mapped[str] = mapped_column(String(36), nullable=False)
    type: Mapped[str] = mapped_column(String(_ID_LENGTH), nullable=False)
    parent_action_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _Attempt(_Base):
    __tablename__ = "ledger_attempts"
    __table_args__ = (
        UniqueConstraint("action_id", "attempt_no", name="uq_ledger_attempts_action_no"),
        Index("ix_ledger_attempts_action", "action_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    action_id: Mapped[str] = mapped_column(String(36), nullable=False)
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _Event(_Base):
    __tablename__ = "ledger_events"
    __table_args__ = (
        UniqueConstraint("lane_id", "seq", name="uq_ledger_events_lane_seq"),
        Index("ix_ledger_events_subject", "subject_id"),
        Index("ix_ledger_events_actor", "actor_id"),
        Index("ix_ledger_events_lane_type", "lane_id", "event_type"),
        Index("ix_ledger_events_committed", "committed_at", "lane_id", "seq", "id"),
        Index("ix_ledger_events_causation", "causation_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    lane_id: Mapped[str] = mapped_column(String(36), nullable=False)
    seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    subject_id: Mapped[str] = mapped_column(String(_ID_LENGTH), nullable=False)
    event_type: Mapped[str] = mapped_column(String(_ID_LENGTH), nullable=False)
    actor_id: Mapped[str] = mapped_column(String(36), nullable=False)
    causation_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    committed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    extensions: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


class _Append(_Base):
    __tablename__ = "ledger_appends"
    __table_args__ = (Index("ix_ledger_appends_lane_range", "lane_id", "first_seq", "last_seq"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    lane_id: Mapped[str] = mapped_column(String(36), nullable=False)
    digest: Mapped[str] = mapped_column(String(64), nullable=False)
    first_seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    last_seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    committed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class _Checkpoint(_Base):
    __tablename__ = "ledger_checkpoints"
    __table_args__ = (
        UniqueConstraint("checkpoint_key", "revision", name="uq_ledger_checkpoints_key_revision"),
        Index("ix_ledger_checkpoints_actor", "actor_id"),
        Index("ix_ledger_checkpoints_latest", "checkpoint_key", "revision"),
        Index("ix_ledger_checkpoints_lane_seq", "lane_id", "last_applied_seq"),
        Index("ix_ledger_checkpoints_event", "last_applied_event_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    checkpoint_key: Mapped[str] = mapped_column(String(_ID_LENGTH), nullable=False)
    revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    actor_id: Mapped[str] = mapped_column(String(36), nullable=False)
    format: Mapped[str] = mapped_column(String(255), nullable=False)
    state: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    artifact_ref: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    lane_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    last_applied_seq: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    last_applied_event_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    extensions: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class SqlEventStore:
    """SQLAlchemy Store shared by SQLite, MySQL, and PostgreSQL."""

    def __init__(self, engine: AsyncEngine, *, operation_timeout: float = 10.0) -> None:
        if operation_timeout <= 0:
            raise ValueError("operation_timeout must be positive")
        self._engine = engine
        self._operation_timeout = operation_timeout

    async def initialize(self) -> None:
        try:
            async with asyncio.timeout(self._operation_timeout):
                async with self._engine.begin() as connection:
                    await connection.run_sync(_Base.metadata.create_all)
        except TimeoutError as error:
            raise StoreError("SQL schema initialization timed out") from error
        except SQLAlchemyError as error:
            raise StoreError("SQL schema initialization failed") from error

    async def create_actor(self, actor: Actor) -> None:
        async def operation(session: AsyncSession) -> None:
            session.add(_actor_row(actor))

        await self._create("actor", actor.id, operation)

    async def get_actor(self, actor_id: str) -> Actor | None:
        row = await self._get(_Actor, actor_id)
        return _actor_model(row) if row is not None else None

    async def create_lane(self, lane: Lane) -> None:
        if lane.last_seq != 0:
            raise ValueError("a new lane must have last_seq 0")

        async def operation(session: AsyncSession) -> None:
            if lane.parent_lane_id is not None:
                parent = await session.get(_Lane, lane.parent_lane_id)
                if parent is None:
                    raise EntityNotFound("parent lane", lane.parent_lane_id)
                if parent.session_id != lane.session_id:
                    raise ValueError("parent lane must belong to the same session")
            session.add(_lane_row(lane))

        await self._create("lane", lane.id, operation)

    async def get_lane(self, lane_id: str) -> Lane | None:
        row = await self._get(_Lane, lane_id)
        return _lane_model(row) if row is not None else None

    async def find_lane(self, session_id: str, run_id: str, name: str) -> Lane | None:
        try:
            async with asyncio.timeout(self._operation_timeout):
                async with AsyncSession(self._engine) as session:
                    row = await session.scalar(
                        select(_Lane).where(
                            _Lane.session_id == session_id,
                            _Lane.run_id == run_id,
                            _Lane.name == name,
                        )
                    )
                    return _lane_model(row) if row is not None else None
        except TimeoutError as error:
            raise StoreError("SQL lane lookup timed out") from error
        except SQLAlchemyError as error:
            raise StoreError("SQL lane lookup failed") from error

    async def create_turn(self, turn: Turn) -> None:
        async def operation(session: AsyncSession) -> None:
            if await session.get(_Lane, turn.lane_id) is None:
                raise EntityNotFound("lane", turn.lane_id)
            session.add(_turn_row(turn))

        await self._create("turn", turn.id, operation)

    async def get_turn(self, turn_id: str) -> Turn | None:
        row = await self._get(_Turn, turn_id)
        return _turn_model(row) if row is not None else None

    async def create_action(self, action: Action) -> None:
        async def operation(session: AsyncSession) -> None:
            if await session.get(_Turn, action.turn_id) is None:
                raise EntityNotFound("turn", action.turn_id)
            if action.parent_action_id is not None:
                parent = await session.get(_Action, action.parent_action_id)
                if parent is None:
                    raise EntityNotFound("parent action", action.parent_action_id)
                if parent.turn_id != action.turn_id:
                    raise ValueError("parent action must belong to the same turn")
            session.add(_action_row(action))

        await self._create("action", action.id, operation)

    async def get_action(self, action_id: str) -> Action | None:
        row = await self._get(_Action, action_id)
        return _action_model(row) if row is not None else None

    async def create_attempt(self, attempt: Attempt) -> None:
        async def operation(session: AsyncSession) -> None:
            if await session.get(_Action, attempt.action_id) is None:
                raise EntityNotFound("action", attempt.action_id)
            session.add(_attempt_row(attempt))

        await self._create("attempt", attempt.id, operation)

    async def get_attempt(self, attempt_id: str) -> Attempt | None:
        row = await self._get(_Attempt, attempt_id)
        return _attempt_model(row) if row is not None else None

    async def save_checkpoint(
        self,
        expected_revision: int,
        checkpoint: ProposedCheckpoint,
    ) -> Checkpoint:
        if expected_revision < 0:
            raise ValueError("expected_revision must be non-negative")
        try:
            async with asyncio.timeout(self._operation_timeout):
                async with AsyncSession(self._engine) as session:
                    async with session.begin():
                        previous = await session.get(_Checkpoint, checkpoint.id)
                        if previous is not None:
                            stored = _checkpoint_model(previous)
                            proposed = ProposedCheckpoint.model_validate(
                                stored.model_dump(exclude={"revision", "created_at"})
                            )
                            if proposed != checkpoint:
                                raise CheckpointIdempotencyViolation(checkpoint.id)
                            return stored
                        if await session.get(_Actor, checkpoint.actor_id) is None:
                            raise EntityNotFound("actor", checkpoint.actor_id)
                        latest = await session.scalar(
                            select(_Checkpoint)
                            .where(_Checkpoint.checkpoint_key == checkpoint.checkpoint_key)
                            .order_by(_Checkpoint.revision.desc())
                            .limit(1)
                            .with_for_update()
                        )
                        actual_revision = latest.revision if latest is not None else 0
                        if actual_revision != expected_revision:
                            raise CheckpointConflict(expected_revision, actual_revision)
                        if checkpoint.anchor is not None:
                            event = await session.get(
                                _Event, checkpoint.anchor.last_applied_event_id
                            )
                            if (
                                event is None
                                or event.lane_id != checkpoint.anchor.lane_id
                                or event.seq != checkpoint.anchor.last_applied_seq
                            ):
                                raise ValueError(
                                    "checkpoint anchor must identify an existing lane event"
                                )
                        stored = Checkpoint.from_proposed(
                            checkpoint,
                            revision=actual_revision + 1,
                            created_at=utc_now(),
                        )
                        session.add(_checkpoint_row(stored))
                    return stored
        except TimeoutError as error:
            raise StoreError("SQL checkpoint save timed out") from error
        except IntegrityError as error:
            return await self._reconcile_checkpoint_integrity_error(
                checkpoint, expected_revision, error
            )
        except AgentLedgerError:
            raise
        except SQLAlchemyError as error:
            raise StoreError("SQL checkpoint save failed") from error

    async def _reconcile_checkpoint_integrity_error(
        self,
        checkpoint: ProposedCheckpoint,
        expected_revision: int,
        insert_error: IntegrityError,
    ) -> Checkpoint:
        # Re-read outside the rolled-back transaction. This avoids driver-specific
        # constraint parsing and distinguishes checkpoint ID reuse from revision races.
        try:
            previous = await self.get_checkpoint(checkpoint.id)
        except StoreError:
            raise StoreError("SQL checkpoint save failed") from insert_error
        if previous is not None:
            proposed = ProposedCheckpoint.model_validate(
                previous.model_dump(exclude={"revision", "created_at"})
            )
            if proposed != checkpoint:
                raise CheckpointIdempotencyViolation(checkpoint.id)
            return previous
        try:
            latest = await self.load_latest_checkpoint(checkpoint.checkpoint_key)
        except StoreError:
            raise StoreError("SQL checkpoint save failed") from insert_error
        actual_revision = latest.revision if latest is not None else 0
        if actual_revision != expected_revision:
            raise CheckpointConflict(expected_revision, actual_revision) from insert_error
        raise StoreError("SQL checkpoint save failed") from insert_error

    async def get_checkpoint(self, checkpoint_id: str) -> Checkpoint | None:
        row = await self._get(_Checkpoint, checkpoint_id)
        return _checkpoint_model(row) if row is not None else None

    async def load_latest_checkpoint(self, checkpoint_key: str) -> Checkpoint | None:
        try:
            async with asyncio.timeout(self._operation_timeout):
                async with AsyncSession(self._engine) as session:
                    row = await session.scalar(
                        select(_Checkpoint)
                        .where(_Checkpoint.checkpoint_key == checkpoint_key)
                        .order_by(_Checkpoint.revision.desc())
                        .limit(1)
                    )
                    return _checkpoint_model(row) if row is not None else None
        except TimeoutError as error:
            raise StoreError("SQL checkpoint lookup timed out") from error
        except SQLAlchemyError as error:
            raise StoreError("SQL checkpoint lookup failed") from error

    async def append(
        self,
        lane_id: str,
        expected_last_seq: int,
        append_id: str,
        events: Sequence[ProposedEvent],
    ) -> AppendReceipt:
        batch, digest = validate_append(lane_id, expected_last_seq, append_id, events)
        for attempt in range(3):
            try:
                async with asyncio.timeout(self._operation_timeout):
                    return await self._append_once(
                        lane_id,
                        expected_last_seq,
                        append_id,
                        batch,
                        digest,
                    )
            except IntegrityError as error:
                if attempt == 2:
                    raise StoreError("SQL append failed after concurrent writes") from error
            except TimeoutError as error:
                raise StoreError("SQL append timed out") from error
            except AgentLedgerError:
                raise
            except SQLAlchemyError as error:
                raise StoreError("SQL append failed") from error
        raise AssertionError("unreachable")

    async def _append_once(
        self,
        lane_id: str,
        expected_last_seq: int,
        append_id: str,
        batch: tuple[ProposedEvent, ...],
        digest: str,
    ) -> AppendReceipt:
        async with AsyncSession(self._engine, expire_on_commit=False) as session:
            async with session.begin():
                previous = await session.scalar(
                    select(_Append).where(_Append.id == append_id).with_for_update()
                )
                if previous is not None:
                    if previous.lane_id != lane_id or previous.digest != digest:
                        raise IdempotencyViolation(append_id)
                    event_ids = tuple(
                        await session.scalars(
                            select(_Event.id)
                            .where(
                                _Event.lane_id == lane_id,
                                _Event.seq >= previous.first_seq,
                                _Event.seq <= previous.last_seq,
                            )
                            .order_by(_Event.seq)
                        )
                    )
                    return AppendReceipt(
                        id=previous.id,
                        lane_id=previous.lane_id,
                        digest=previous.digest,
                        first_seq=previous.first_seq,
                        last_seq=previous.last_seq,
                        event_ids=event_ids,
                        committed_at=_aware(previous.committed_at),
                    )

                lane = await session.scalar(
                    select(_Lane).where(_Lane.id == lane_id).with_for_update()
                )
                if lane is None:
                    raise EntityNotFound("lane", lane_id)
                if lane.last_seq != expected_last_seq:
                    raise LaneConflict(expected_last_seq, lane.last_seq)

                duplicate = await session.scalar(
                    select(_Event.id).where(_Event.id.in_(event.id for event in batch))
                )
                if duplicate is not None:
                    raise DuplicateEvent(duplicate)
                if await session.get(_Actor, batch[0].actor_id) is None:
                    raise EntityNotFound("actor", batch[0].actor_id)
                for event in batch[1:]:
                    if (
                        event.actor_id != batch[0].actor_id
                        and await session.get(_Actor, event.actor_id) is None
                    ):
                        raise EntityNotFound("actor", event.actor_id)

                batch_ids: set[str] = set()
                for event in batch:
                    await self._validate_subject(session, lane, event)
                    if event.causation_id is not None and event.causation_id not in batch_ids:
                        caused = await session.get(_Event, event.causation_id)
                        if caused is None:
                            raise EntityNotFound("causation event", event.causation_id)
                        caused_lane = await session.get(_Lane, caused.lane_id)
                        if caused_lane is None or caused_lane.session_id != lane.session_id:
                            raise SubjectMismatch(event.id, lane.id)
                    batch_ids.add(event.id)

                committed_at = utc_now()
                first_seq = lane.last_seq + 1
                rows: list[_Event] = []
                for offset, event in enumerate(batch):
                    row = _event_row(event, first_seq + offset, committed_at)
                    rows.append(row)
                    session.add(row)
                last_seq = first_seq + len(rows) - 1
                lane.last_seq = last_seq
                session.add(
                    _Append(
                        id=append_id,
                        lane_id=lane_id,
                        digest=digest,
                        first_seq=first_seq,
                        last_seq=last_seq,
                        committed_at=committed_at,
                    )
                )
            return AppendReceipt(
                id=append_id,
                lane_id=lane_id,
                digest=digest,
                first_seq=first_seq,
                last_seq=last_seq,
                event_ids=tuple(row.id for row in rows),
                committed_at=committed_at,
            )

    async def read_lane(
        self,
        lane_id: str,
        after_seq: int = 0,
    ) -> AsyncIterator[StoredEvent]:
        if after_seq < 0:
            raise ValueError("after_seq must be non-negative")
        try:
            async with asyncio.timeout(self._operation_timeout):
                async with AsyncSession(self._engine) as session:
                    if await session.get(_Lane, lane_id) is None:
                        raise EntityNotFound("lane", lane_id)
                    rows = list(
                        await session.scalars(
                            select(_Event)
                            .where(_Event.lane_id == lane_id, _Event.seq > after_seq)
                            .order_by(_Event.seq)
                        )
                    )
        except TimeoutError as error:
            raise StoreError("SQL lane read timed out") from error
        except AgentLedgerError:
            raise
        except SQLAlchemyError as error:
            raise StoreError("SQL lane read failed") from error
        for row in rows:
            yield _event_model(row)

    async def load_session(self, session_id: str) -> SessionView:
        try:
            async with asyncio.timeout(self._operation_timeout):
                async with AsyncSession(self._engine) as session:
                    lane_rows = list(
                        await session.scalars(
                            select(_Lane)
                            .where(_Lane.session_id == session_id)
                            .order_by(_Lane.created_at)
                        )
                    )
                    lane_ids = [row.id for row in lane_rows]
                    if not lane_ids:
                        return SessionView(session_id=session_id)
                    turn_rows = list(
                        await session.scalars(
                            select(_Turn)
                            .where(_Turn.lane_id.in_(lane_ids))
                            .order_by(_Turn.created_at)
                        )
                    )
                    turn_ids = [row.id for row in turn_rows]
                    action_rows = (
                        list(
                            await session.scalars(
                                select(_Action)
                                .where(_Action.turn_id.in_(turn_ids))
                                .order_by(_Action.created_at)
                            )
                        )
                        if turn_ids
                        else []
                    )
                    action_ids = [row.id for row in action_rows]
                    attempt_rows = (
                        list(
                            await session.scalars(
                                select(_Attempt)
                                .where(_Attempt.action_id.in_(action_ids))
                                .order_by(_Attempt.created_at)
                            )
                        )
                        if action_ids
                        else []
                    )
                    event_rows = list(
                        await session.scalars(
                            select(_Event)
                            .where(_Event.lane_id.in_(lane_ids))
                            .order_by(_Event.committed_at, _Event.lane_id, _Event.seq, _Event.id)
                        )
                    )
                    actor_ids = {row.actor_id for row in event_rows}
                    actor_rows = (
                        list(await session.scalars(select(_Actor).where(_Actor.id.in_(actor_ids))))
                        if actor_ids
                        else []
                    )
        except TimeoutError as error:
            raise StoreError("SQL session read timed out") from error
        except SQLAlchemyError as error:
            raise StoreError("SQL session read failed") from error
        return SessionView(
            session_id=session_id,
            actors=tuple(_actor_model(row) for row in actor_rows),
            lanes=tuple(_lane_model(row) for row in lane_rows),
            turns=tuple(_turn_model(row) for row in turn_rows),
            actions=tuple(_action_model(row) for row in action_rows),
            attempts=tuple(_attempt_model(row) for row in attempt_rows),
            events=tuple(_event_model(row) for row in event_rows),
        )

    async def _validate_subject(
        self,
        session: AsyncSession,
        lane: _Lane,
        event: ProposedEvent,
    ) -> None:
        kind = event.subject_kind
        valid = False
        if kind == "session":
            valid = event.subject_id == lane.session_id
        elif kind == "run":
            valid = event.subject_id == lane.run_id
        elif kind == "lane":
            valid = event.subject_id == lane.id
        elif kind == "turn":
            turn = await session.get(_Turn, event.subject_id)
            valid = turn is not None and turn.lane_id == lane.id
        elif kind == "action":
            action = await session.get(_Action, event.subject_id)
            turn = await session.get(_Turn, action.turn_id) if action is not None else None
            valid = turn is not None and turn.lane_id == lane.id
        elif kind == "attempt":
            attempt = await session.get(_Attempt, event.subject_id)
            action = await session.get(_Action, attempt.action_id) if attempt is not None else None
            turn = await session.get(_Turn, action.turn_id) if action is not None else None
            valid = turn is not None and turn.lane_id == lane.id
        if not valid:
            raise SubjectMismatch(event.id, lane.id)

    async def _create(self, entity: str, entity_id: str, operation: Any) -> None:
        try:
            async with asyncio.timeout(self._operation_timeout):
                async with AsyncSession(self._engine) as session:
                    async with session.begin():
                        await operation(session)
        except TimeoutError as error:
            raise StoreError(f"SQL {entity} creation timed out") from error
        except IntegrityError as error:
            raise EntityConflict(entity, entity_id) from error
        except AgentLedgerError:
            raise
        except SQLAlchemyError as error:
            raise StoreError(f"SQL {entity} creation failed") from error

    async def _get(self, model: Any, entity_id: str) -> Any:
        try:
            async with asyncio.timeout(self._operation_timeout):
                async with AsyncSession(self._engine) as session:
                    return await session.get(model, entity_id)
        except TimeoutError as error:
            raise StoreError("SQL entity lookup timed out") from error
        except SQLAlchemyError as error:
            raise StoreError("SQL entity lookup failed") from error


def _lane_row(value: Lane) -> _Lane:
    return _Lane(**value.model_dump())


def _actor_row(value: Actor) -> _Actor:
    return _Actor(**value.model_dump())


def _turn_row(value: Turn) -> _Turn:
    return _Turn(**value.model_dump())


def _action_row(value: Action) -> _Action:
    return _Action(**value.model_dump())


def _attempt_row(value: Attempt) -> _Attempt:
    return _Attempt(**value.model_dump())


def _event_row(value: ProposedEvent, seq: int, committed_at: datetime) -> _Event:
    return _Event(**value.model_dump(), seq=seq, committed_at=committed_at)


def _checkpoint_row(value: Checkpoint) -> _Checkpoint:
    anchor = value.anchor
    return _Checkpoint(
        id=value.id,
        schema_version=value.schema_version,
        checkpoint_key=value.checkpoint_key,
        revision=value.revision,
        actor_id=value.actor_id,
        format=value.format,
        state=value.state,
        artifact_ref=value.artifact_ref.model_dump(mode="json")
        if value.artifact_ref is not None
        else None,
        lane_id=anchor.lane_id if anchor is not None else None,
        last_applied_seq=anchor.last_applied_seq if anchor is not None else None,
        last_applied_event_id=anchor.last_applied_event_id if anchor is not None else None,
        extensions=value.extensions,
        created_at=value.created_at,
    )


def _lane_model(row: _Lane) -> Lane:
    return Lane(
        id=row.id,
        session_id=row.session_id,
        run_id=row.run_id,
        name=row.name,
        parent_lane_id=row.parent_lane_id,
        last_seq=row.last_seq,
        created_at=_aware(row.created_at),
    )


def _actor_model(row: _Actor) -> Actor:
    return Actor(
        id=row.id,
        type=row.type,
        framework=row.framework,
        created_at=_aware(row.created_at),
    )


def _turn_model(row: _Turn) -> Turn:
    return Turn(id=row.id, lane_id=row.lane_id, created_at=_aware(row.created_at))


def _action_model(row: _Action) -> Action:
    return Action(
        id=row.id,
        turn_id=row.turn_id,
        type=row.type,
        parent_action_id=row.parent_action_id,
        created_at=_aware(row.created_at),
    )


def _attempt_model(row: _Attempt) -> Attempt:
    return Attempt(
        id=row.id,
        action_id=row.action_id,
        attempt_no=row.attempt_no,
        created_at=_aware(row.created_at),
    )


def _event_model(row: _Event) -> StoredEvent:
    return StoredEvent.model_validate(
        {
            "schema_version": row.schema_version,
            "id": row.id,
            "lane_id": row.lane_id,
            "seq": row.seq,
            "subject_id": row.subject_id,
            "event_type": row.event_type,
            "actor_id": row.actor_id,
            "causation_id": row.causation_id,
            "occurred_at": _aware(row.occurred_at),
            "committed_at": _aware(row.committed_at),
            "payload": row.payload,
            "extensions": row.extensions,
        }
    )


def _checkpoint_model(row: _Checkpoint) -> Checkpoint:
    anchor = (
        CheckpointAnchor(
            lane_id=row.lane_id,
            last_applied_seq=row.last_applied_seq,
            last_applied_event_id=row.last_applied_event_id,
        )
        if row.lane_id is not None
        and row.last_applied_seq is not None
        and row.last_applied_event_id is not None
        else None
    )
    return Checkpoint.model_validate(
        {
            "schema_version": row.schema_version,
            "id": row.id,
            "checkpoint_key": row.checkpoint_key,
            "revision": row.revision,
            "actor_id": row.actor_id,
            "format": row.format,
            "state": row.state,
            "artifact_ref": row.artifact_ref,
            "anchor": anchor,
            "extensions": row.extensions,
            "created_at": _aware(row.created_at),
        }
    )


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value
