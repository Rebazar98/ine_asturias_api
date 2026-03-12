from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models import GeocodeCache, ReverseGeocodeCache
from app.repositories.geocoding import (
    DEFAULT_REVERSE_GEOCODE_PRECISION,
    GEOCODING_PROVIDER_CARTOCIUDAD,
    GeocodingCacheRepository,
    normalize_geocode_query,
)


@pytest.mark.integration
def test_geocoding_cache_repository_roundtrip_with_postgres() -> None:
    postgres_dsn = os.getenv("INTEGRATION_POSTGRES_DSN") or os.getenv("POSTGRES_DSN")
    if not postgres_dsn:
        pytest.skip("PostgreSQL integration DSN not configured.")

    async def scenario() -> None:
        engine = create_async_engine(postgres_dsn, future=True)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = uuid4().hex[:8]
        provider = f"{GEOCODING_PROVIDER_CARTOCIUDAD}-test-{suffix}"
        query = f"Calle Uria {suffix}"
        lat = 43.3614004
        lon = -5.8493996

        try:
            async with session_factory() as session:
                repository = GeocodingCacheRepository(session=session)

                inserted_geocode = await repository.upsert_geocode_cache(
                    provider=provider,
                    query=query,
                    payload={"label": "Oviedo"},
                    ttl_seconds=3600,
                    metadata={"scope": "integration"},
                )
                cached_geocode = await repository.get_geocode_cache(
                    provider=provider,
                    query=f"  {query.lower()}  ",
                )
                updated_geocode = await repository.upsert_geocode_cache(
                    provider=provider,
                    query=query,
                    payload={"label": "Oviedo Updated"},
                    ttl_seconds=3600,
                    metadata={"scope": "integration", "updated": True},
                )

                inserted_reverse = await repository.upsert_reverse_geocode_cache(
                    provider=provider,
                    lat=lat,
                    lon=lon,
                    payload={"municipality": "Oviedo"},
                    ttl_seconds=3600,
                    metadata={"scope": "integration"},
                )
                cached_reverse = await repository.get_reverse_geocode_cache(
                    provider=provider,
                    lat=43.3614,
                    lon=-5.8494,
                )

                assert inserted_geocode is not None
                assert cached_geocode is not None
                assert updated_geocode is not None
                assert inserted_reverse is not None
                assert cached_reverse is not None

                assert cached_geocode["payload"] == {"label": "Oviedo"}
                assert updated_geocode["payload"] == {"label": "Oviedo Updated"}
                assert updated_geocode["normalized_query"] == normalize_geocode_query(query)
                assert inserted_reverse["precision_digits"] == DEFAULT_REVERSE_GEOCODE_PRECISION
                assert cached_reverse["payload"] == {"municipality": "Oviedo"}
                assert cached_reverse["coordinate_key"] == inserted_reverse["coordinate_key"]

        finally:
            async with session_factory() as session:
                await session.execute(delete(GeocodeCache).where(GeocodeCache.provider == provider))
                await session.execute(
                    delete(ReverseGeocodeCache).where(ReverseGeocodeCache.provider == provider)
                )
                await session.commit()
            await engine.dispose()

    asyncio.run(scenario())
