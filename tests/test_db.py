from __future__ import annotations

import pytest

import app.db as db_module


@pytest.mark.anyio
async def test_session_scope_yields_none_when_not_initialized() -> None:
    """session_scope() must yield None gracefully when no postgres_dsn is configured.

    This is intentional: repositories guard against None sessions, enabling
    database-optional operation in test and local-dev environments.
    """
    original_factory = db_module._session_factory
    try:
        db_module._session_factory = None
        async with db_module.session_scope() as session:
            assert session is None
    finally:
        db_module._session_factory = original_factory


@pytest.mark.anyio
async def test_ping_database_returns_false_if_not_initialized() -> None:
    """ping_database() must return False gracefully when engine is None."""
    original_engine = db_module._engine
    try:
        db_module._engine = None
        result = await db_module.ping_database()
        assert result is False
    finally:
        db_module._engine = original_engine
