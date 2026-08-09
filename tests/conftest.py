from __future__ import annotations

import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from agent_ledger.store import EventStore
from agent_ledger.stores.memory import MemoryEventStore
from agent_ledger.stores.redis import RedisEventStore
from agent_ledger.stores.sql import SqlEventStore


def _backends() -> list[str]:
    result = ["memory", "sqlite"]
    if os.getenv("AGENT_LEDGER_REDIS_URL"):
        result.append("redis")
    if os.getenv("AGENT_LEDGER_POSTGRES_URL"):
        result.append("postgres")
    if os.getenv("AGENT_LEDGER_MYSQL_URL"):
        result.append("mysql")
    return result


@pytest.fixture(params=_backends())
async def event_store(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> AsyncIterator[EventStore]:
    backend = str(request.param)
    if backend == "memory":
        yield MemoryEventStore()
        return

    if backend == "redis":
        client = Redis.from_url(
            os.environ["AGENT_LEDGER_REDIS_URL"],
            max_connections=10,
            socket_connect_timeout=3,
            socket_timeout=3,
        )
        try:
            yield RedisEventStore(client, prefix="agent-ledger-test", operation_timeout=5)
        finally:
            await client.aclose()
        return

    engine = _sql_engine(backend, tmp_path)
    store = SqlEventStore(engine, operation_timeout=10)
    await store.initialize()
    try:
        yield store
    finally:
        await engine.dispose()


def _sql_engine(backend: str, tmp_path: Path) -> AsyncEngine:
    if backend == "sqlite":
        return create_async_engine(
            f"sqlite+aiosqlite:///{tmp_path / 'ledger.db'}",
            connect_args={"timeout": 5},
        )

    url = os.environ[f"AGENT_LEDGER_{backend.upper()}_URL"]
    connect_args: dict[str, Any]
    if backend == "postgres":
        connect_args = {"command_timeout": 5}
    else:
        connect_args = {"connect_timeout": 5}
    return create_async_engine(
        url,
        pool_size=5,
        max_overflow=0,
        pool_timeout=5,
        pool_recycle=300,
        connect_args=connect_args,
    )
