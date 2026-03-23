from __future__ import annotations

import asyncio
from datetime import datetime, UTC
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models import CatastroMunicipalityAggregateCache
from app.repositories.catastro_cache import (
    CATASTRO_PROVIDER_FAMILY_URBANO,
    CatastroMunicipalityAggregateCacheRepository,
)
from tests.integration.postgres import require_integration_postgres


@pytest.mark.integration
def test_catastro_cache_repository_roundtrip_with_postgres():
    postgres_dsn = require_integration_postgres()

    async def scenario() -> None:
        engine = create_async_engine(postgres_dsn, future=True)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        municipality_code = f"33{uuid4().hex[:6]}"

        try:
            async with session_factory() as session:
                repository = CatastroMunicipalityAggregateCacheRepository(session=session)
                stored = await repository.upsert_payload(
                    provider_family=CATASTRO_PROVIDER_FAMILY_URBANO,
                    municipality_code=municipality_code,
                    reference_year="2025",
                    payload={"reference_year": "2025", "indicators": []},
                    ttl_seconds=3600,
                    metadata={"provider": "catastro"},
                    now=datetime(2026, 3, 15, 13, 0, tzinfo=UTC),
                )
                assert stored is not None

                fresh = await repository.get_fresh_payload(
                    provider_family=CATASTRO_PROVIDER_FAMILY_URBANO,
                    municipality_code=municipality_code,
                    reference_year="2025",
                    now=datetime(2026, 3, 15, 13, 1, tzinfo=UTC),
                )
                assert fresh is not None
                assert fresh["payload"]["reference_year"] == "2025"
                assert fresh["metadata"]["provider"] == "catastro"
        finally:
            async with session_factory() as session:
                await session.execute(
                    delete(CatastroMunicipalityAggregateCache).where(
                        CatastroMunicipalityAggregateCache.municipality_code == municipality_code
                    )
                )
                await session.commit()
            await engine.dispose()

    asyncio.run(scenario())
