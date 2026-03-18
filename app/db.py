from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.logging import get_logger
from app.settings import Settings

_SLOW_QUERY_THRESHOLD_MS = 500

_db_logger = get_logger("app.db.slow_query")


def _before_cursor_execute(conn, cursor, statement, parameters, context, executemany):  # noqa: ANN001
    conn.info["query_start"] = time.perf_counter()


def _after_cursor_execute(conn, cursor, statement, parameters, context, executemany):  # noqa: ANN001
    elapsed_ms = (time.perf_counter() - conn.info["query_start"]) * 1000
    if elapsed_ms >= _SLOW_QUERY_THRESHOLD_MS:
        _db_logger.warning(
            "slow_query_detected",
            extra={
                "duration_ms": round(elapsed_ms, 2),
                "statement_hash": hash(statement),
                "executemany": executemany,
            },
        )


_engine = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def init_db(settings: Settings) -> None:
    global _engine, _session_factory

    if _engine is not None or not settings.postgres_dsn:
        return

    _engine = create_async_engine(
        settings.postgres_dsn,
        pool_pre_ping=True,
        future=True,
    )
    event.listen(_engine.sync_engine, "before_cursor_execute", _before_cursor_execute)
    event.listen(_engine.sync_engine, "after_cursor_execute", _after_cursor_execute)
    _session_factory = async_sessionmaker(_engine, expire_on_commit=False)


async def dispose_db() -> None:
    global _engine, _session_factory

    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _session_factory = None


async def ping_database() -> bool:
    if _engine is None:
        return False

    try:
        async with _engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    except Exception:
        return False
    return True


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession | None]:
    if _session_factory is None:
        yield None
        return

    async with _session_factory() as session:
        yield session


async def get_session() -> AsyncIterator[AsyncSession | None]:
    async with session_scope() as session:
        yield session
