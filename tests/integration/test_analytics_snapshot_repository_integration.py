from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest
from sqlalchemy import delete, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models import AnalyticalSnapshot
from app.repositories.analytics_snapshots import AnalyticalSnapshotRepository


@pytest.mark.integration
def test_analytical_snapshot_repository_roundtrip_with_postgres() -> None:
    postgres_dsn = os.getenv("INTEGRATION_POSTGRES_DSN") or os.getenv("POSTGRES_DSN")
    if not postgres_dsn:
        pytest.skip("PostgreSQL integration DSN not configured.")

    async def scenario() -> None:
        engine = create_async_engine(postgres_dsn, future=True)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = uuid4().hex[:8]
        scope_key = f"municipality:{suffix}"
        filters = {"municipality_code": suffix, "page": 1, "page_size": 10}
        source = "internal.analytics.municipality_report"
        snapshot_type = "municipality_report"
        table_available = False

        try:
            async with engine.connect() as connection:
                table_name = await connection.scalar(
                    text("SELECT to_regclass('public.analytical_snapshots')")
                )
            table_available = table_name is not None
            if table_name is None:
                pytest.skip(
                    "analytical_snapshots table is not available in the integration database."
                )

            async with session_factory() as session:
                repository = AnalyticalSnapshotRepository(session=session)

                inserted = await repository.upsert_snapshot(
                    snapshot_type=snapshot_type,
                    scope_key=scope_key,
                    source=source,
                    territorial_unit_id=None,
                    payload={"report_type": snapshot_type, "metadata": {"run": 1}},
                    filters=filters,
                    ttl_seconds=3600,
                    metadata={"scope": "integration"},
                )
                cached = await repository.get_fresh_snapshot(
                    snapshot_type=snapshot_type,
                    scope_key=scope_key,
                    filters=filters,
                )
                updated = await repository.upsert_snapshot(
                    snapshot_type=snapshot_type,
                    scope_key=scope_key,
                    source=source,
                    territorial_unit_id=None,
                    payload={"report_type": snapshot_type, "metadata": {"run": 2}},
                    filters=filters,
                    ttl_seconds=3600,
                    metadata={"scope": "integration", "updated": True},
                )

                assert inserted is not None
                assert cached is not None
                assert updated is not None
                assert cached["payload"]["metadata"]["run"] == 1
                assert updated["payload"]["metadata"]["run"] == 2
                assert inserted["snapshot_key"] == cached["snapshot_key"] == updated["snapshot_key"]

        finally:
            if table_available:
                async with session_factory() as session:
                    await session.execute(
                        delete(AnalyticalSnapshot).where(AnalyticalSnapshot.scope_key == scope_key)
                    )
                    await session.commit()
            await engine.dispose()

    asyncio.run(scenario())
