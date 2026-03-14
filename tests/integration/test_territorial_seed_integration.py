from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models import INESeriesNormalized, TerritorialUnit, TerritorialUnitCode
from app.repositories.series import SeriesRepository
from app.repositories.territorial import (
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TerritorialRepository,
)
from app.services.territorial_seed import ensure_municipality_analytics_seed


@pytest.mark.integration
def test_municipality_analytics_seed_is_idempotent_with_postgres() -> None:
    postgres_dsn = os.getenv("INTEGRATION_POSTGRES_DSN") or os.getenv("POSTGRES_DSN")
    if not postgres_dsn:
        pytest.skip("PostgreSQL integration DSN not configured.")

    async def scenario() -> None:
        engine = create_async_engine(postgres_dsn, future=True)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = uuid4().hex[:8]
        municipality_code = f"33seed{suffix}"
        municipality_name = f"Municipio Seed {suffix}"
        territorial_unit_id: int | None = None

        try:
            async with session_factory() as session:
                first = await ensure_municipality_analytics_seed(
                    session,
                    municipality_code=municipality_code,
                    municipality_name=municipality_name,
                )
                second = await ensure_municipality_analytics_seed(
                    session,
                    municipality_code=municipality_code,
                    municipality_name=municipality_name,
                )
                territorial_unit_id = first.territorial_unit_id

                assert first.created_unit is True
                assert second.created_unit is False
                assert first.territorial_unit_id == second.territorial_unit_id
                assert first.normalized_rows_upserted == 2
                assert second.normalized_rows_upserted == 2

            async with session_factory() as session:
                territorial_repository = TerritorialRepository(session=session)
                detail = await territorial_repository.get_unit_detail_by_canonical_code(
                    unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
                    code_value=municipality_code,
                )
                assert detail is not None
                assert detail["canonical_name"] == municipality_name
                assert detail["canonical_code"]["code_value"] == municipality_code

                series_repository = SeriesRepository(session=session)
                indicators = await series_repository.list_latest_indicators_by_geography(
                    geography_code=municipality_code,
                    page=1,
                    page_size=10,
                )
                assert indicators["total"] == 2
                assert len(indicators["items"]) == 2

        finally:
            async with session_factory() as session:
                await session.execute(
                    delete(INESeriesNormalized).where(
                        INESeriesNormalized.geography_code == municipality_code
                    )
                )
                if territorial_unit_id is not None:
                    await session.execute(
                        delete(TerritorialUnitCode).where(
                            TerritorialUnitCode.territorial_unit_id == territorial_unit_id
                        )
                    )
                    await session.execute(
                        delete(TerritorialUnit).where(TerritorialUnit.id == territorial_unit_id)
                    )
                await session.commit()
            await engine.dispose()

    asyncio.run(scenario())
