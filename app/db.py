from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.settings import Settings


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

