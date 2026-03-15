from __future__ import annotations

import asyncio
import os

import asyncpg
import pytest


def require_integration_postgres() -> str:
    postgres_dsn = os.getenv("INTEGRATION_POSTGRES_DSN") or os.getenv("POSTGRES_DSN")
    if not postgres_dsn:
        pytest.skip("PostgreSQL integration DSN not configured.")

    asyncio.run(_ensure_connection(postgres_dsn))
    return postgres_dsn


async def _ensure_connection(postgres_dsn: str) -> None:
    try:
        connection = await asyncpg.connect(_normalize_asyncpg_dsn(postgres_dsn))
    except Exception as exc:  # pragma: no cover - depends on local integration env
        pytest.skip(f"PostgreSQL integration DSN is not reachable: {exc}")
    await connection.close()


def _normalize_asyncpg_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn.split("://", 1)[1]
    return dsn
