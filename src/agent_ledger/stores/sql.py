from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    Index,
    Integer,
    String,
    UniqueConstraint,
    insert,
    select,
)
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from agent_ledger.errors import (
    AgentLedgerError,
    DuplicateEvent,
    IdempotencyViolation,
    InvalidCursor,
    StoreError,
    StreamConflict,
)
from agent_ledger.models import (
    CommitReceipt,
    ProposedEvent,
    RunStream,
    StoredEvent,
    utc_now,
)
from agent_ledger.stores._validation import validate_append

_ID_LENGTH = 191
_CURSOR_TYPE = BigInteger().with_variant(Integer, "sqlite")


class _Base(DeclarativeBase):
    pass


class _SchemaVersion(_Base):
    __tablename__ = "agent_ledger_schema_version"

    component: Mapped[str] = mapped_column(String(64), primary_key=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False)


class _RunStream(_Base):
    __tablename__ = "agent_ledger_run_streams"

    session_id: Mapped[str] = mapped_column(String(_ID_LENGTH), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(_ID_LENGTH), primary_key=True)
    current_version: Mapped[int] = mapped_column(BigInteger, nullable=False)


class _AppendCommit(_Base):
    __tablename__ = "agent_ledger_append_commits"

    session_id: Mapped[str] = mapped_column(String(_ID_LENGTH), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(_ID_LENGTH), primary_key=True)
    append_id: Mapped[str] = mapped_column(String(_ID_LENGTH), primary_key=True)
    digest: Mapped[str] = mapped_column(String(64), nullable=False)
    receipt: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


class _Event(_Base):
    __tablename__ = "agent_ledger_events"
    __table_args__ = (
        UniqueConstraint(
            "session_id",
            "run_id",
            "stream_version",
            name="uq_agent_ledger_stream_version",
        ),
        Index("ix_agent_ledger_session_cursor", "session_id", "commit_position"),
    )

    commit_position: Mapped[int] = mapped_column(
        _CURSOR_TYPE,
        primary_key=True,
        autoincrement=True,
    )
    event_id: Mapped[str] = mapped_column(String(_ID_LENGTH), unique=True, nullable=False)
    session_id: Mapped[str] = mapped_column(String(_ID_LENGTH), nullable=False)
    run_id: Mapped[str] = mapped_column(String(_ID_LENGTH), nullable=False)
    stream_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    committed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    event: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


class SqlEventStore:
    """SQLAlchemy event store shared by SQLite, MySQL, and PostgreSQL."""

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
                    result = await connection.execute(
                        select(_SchemaVersion.version).where(_SchemaVersion.component == "core")
                    )
                    version = result.scalar_one_or_none()
                    if version is None:
                        await connection.execute(
                            insert(_SchemaVersion).values(component="core", version=1)
                        )
                    elif version != 1:
                        raise StoreError(f"unsupported SQL schema version: {version}")
        except TimeoutError as error:
            raise StoreError("SQL schema initialization timed out") from error
        except SQLAlchemyError as error:
            raise StoreError("SQL schema initialization failed") from error

    async def append(
        self,
        stream: RunStream,
        expected_version: int,
        append_id: str,
        events: Sequence[ProposedEvent],
    ) -> CommitReceipt:
        batch, digest = validate_append(stream, expected_version, append_id, events)

        for attempt in range(3):
            try:
                async with asyncio.timeout(self._operation_timeout):
                    return await self._append_once(
                        stream,
                        expected_version,
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
        stream: RunStream,
        expected_version: int,
        append_id: str,
        batch: tuple[ProposedEvent, ...],
        digest: str,
    ) -> CommitReceipt:
        async with AsyncSession(self._engine, expire_on_commit=False) as session:
            async with session.begin():
                existing = await session.scalar(
                    select(_AppendCommit)
                    .where(
                        _AppendCommit.session_id == stream.session_id,
                        _AppendCommit.run_id == stream.run_id,
                        _AppendCommit.append_id == append_id,
                    )
                    .with_for_update()
                )
                if existing is not None:
                    if existing.digest != digest:
                        raise IdempotencyViolation(append_id)
                    return CommitReceipt.model_validate(existing.receipt)

                stream_row = await session.scalar(
                    select(_RunStream)
                    .where(
                        _RunStream.session_id == stream.session_id,
                        _RunStream.run_id == stream.run_id,
                    )
                    .with_for_update()
                )
                if stream_row is None:
                    stream_row = _RunStream(
                        session_id=stream.session_id,
                        run_id=stream.run_id,
                        current_version=-1,
                    )
                    session.add(stream_row)
                    await session.flush()

                if stream_row.current_version != expected_version:
                    raise StreamConflict(expected_version, stream_row.current_version)

                duplicate = await session.scalar(
                    select(_Event.event_id).where(
                        _Event.event_id.in_(event.event_id for event in batch)
                    )
                )
                if duplicate is not None:
                    raise DuplicateEvent(duplicate)

                committed_at = utc_now()
                first_version = expected_version + 1
                rows: list[_Event] = []
                for offset, event in enumerate(batch):
                    row = _Event(
                        event_id=event.event_id,
                        session_id=stream.session_id,
                        run_id=stream.run_id,
                        stream_version=first_version + offset,
                        committed_at=committed_at,
                        event=event.model_dump(mode="json"),
                    )
                    rows.append(row)
                    session.add(row)
                await session.flush()

                stored = [
                    StoredEvent.from_proposed(
                        event,
                        stream_version=row.stream_version,
                        commit_cursor=str(row.commit_position),
                        committed_at=committed_at,
                    )
                    for event, row in zip(batch, rows, strict=True)
                ]
                for row, event in zip(rows, stored, strict=True):
                    row.event = event.model_dump(mode="json")

                receipt = CommitReceipt(
                    stream=stream,
                    append_id=append_id,
                    digest=digest,
                    first_version=stored[0].stream_version,
                    last_version=stored[-1].stream_version,
                    first_cursor=stored[0].commit_cursor,
                    last_cursor=stored[-1].commit_cursor,
                    event_ids=tuple(event.event_id for event in stored),
                    committed_at=committed_at,
                )
                stream_row.current_version = receipt.last_version
                session.add(
                    _AppendCommit(
                        session_id=stream.session_id,
                        run_id=stream.run_id,
                        append_id=append_id,
                        digest=digest,
                        receipt=receipt.model_dump(mode="json"),
                    )
                )
            return receipt

    async def read_stream(
        self,
        stream: RunStream,
        after_version: int = -1,
    ) -> AsyncIterator[StoredEvent]:
        try:
            async with asyncio.timeout(self._operation_timeout):
                async with AsyncSession(self._engine) as session:
                    result = await session.scalars(
                        select(_Event)
                        .where(
                            _Event.session_id == stream.session_id,
                            _Event.run_id == stream.run_id,
                            _Event.stream_version > after_version,
                        )
                        .order_by(_Event.stream_version)
                    )
                    events = [StoredEvent.model_validate(row.event) for row in result]
        except TimeoutError as error:
            raise StoreError("SQL stream read timed out") from error
        except SQLAlchemyError as error:
            raise StoreError("SQL stream read failed") from error
        for event in events:
            yield event

    async def scan_session(
        self,
        session_id: str,
        after_cursor: str | None = None,
    ) -> AsyncIterator[StoredEvent]:
        cursor = _parse_cursor(after_cursor)
        try:
            async with asyncio.timeout(self._operation_timeout):
                async with AsyncSession(self._engine) as session:
                    result = await session.scalars(
                        select(_Event)
                        .where(
                            _Event.session_id == session_id,
                            _Event.commit_position > cursor,
                        )
                        .order_by(_Event.commit_position)
                    )
                    events = [StoredEvent.model_validate(row.event) for row in result]
        except TimeoutError as error:
            raise StoreError("SQL session scan timed out") from error
        except SQLAlchemyError as error:
            raise StoreError("SQL session scan failed") from error
        for event in events:
            yield event


def _parse_cursor(cursor: str | None) -> int:
    if cursor is None:
        return -1
    try:
        value = int(cursor)
    except ValueError as error:
        raise InvalidCursor(cursor) from error
    if value < -1:
        raise InvalidCursor(cursor)
    return value
