from __future__ import annotations

import re
from datetime import datetime, timedelta, UTC
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.core.metrics import record_persistence_batch
from app.models import GeocodeCache, ReverseGeocodeCache


GEOCODING_PROVIDER_CARTOCIUDAD = "cartociudad"
DEFAULT_REVERSE_GEOCODE_PRECISION = 6


def normalize_geocode_query(query: str) -> str:
    collapsed = re.sub(r"\s+", " ", (query or "").strip())
    return collapsed.casefold()


def normalize_reverse_geocode_coordinates(
    lat: float,
    lon: float,
    precision_digits: int = DEFAULT_REVERSE_GEOCODE_PRECISION,
) -> tuple[float, float]:
    return round(float(lat), precision_digits), round(float(lon), precision_digits)


def build_reverse_geocode_coordinate_key(
    lat: float,
    lon: float,
    precision_digits: int = DEFAULT_REVERSE_GEOCODE_PRECISION,
) -> str:
    normalized_lat, normalized_lon = normalize_reverse_geocode_coordinates(
        lat=lat,
        lon=lon,
        precision_digits=precision_digits,
    )
    return f"{normalized_lat:.{precision_digits}f},{normalized_lon:.{precision_digits}f}"


class GeocodingCacheRepository:
    def __init__(self, session: AsyncSession | None) -> None:
        self.session = session
        self.logger = get_logger("app.repositories.geocoding")

    async def get_geocode_cache(
        self,
        provider: str,
        query: str,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        if self.session is None:
            return None

        normalized_query = normalize_geocode_query(query)
        if not normalized_query:
            return None

        lookup_time = now or datetime.now(UTC)
        statement = (
            select(GeocodeCache)
            .where(
                GeocodeCache.provider == provider,
                GeocodeCache.normalized_query == normalized_query,
                GeocodeCache.expires_at > lookup_time,
            )
            .limit(1)
        )
        result = await self.session.execute(statement)
        row = result.scalars().first()
        if row is None:
            return None

        self.logger.info(
            "geocode_cache_hit_db",
            extra={"provider": provider, "normalized_query": normalized_query},
        )
        return self._serialize_geocode_row(row)

    async def upsert_geocode_cache(
        self,
        provider: str,
        query: str,
        payload: dict[str, Any] | list[Any],
        *,
        ttl_seconds: int,
        metadata: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        if self.session is None:
            return None

        normalized_query = normalize_geocode_query(query)
        if not normalized_query:
            return None

        cached_at = now or datetime.now(UTC)
        expires_at = cached_at + timedelta(seconds=ttl_seconds)
        statement = insert(GeocodeCache.__table__).values(
            {
                "provider": provider,
                "query_text": query.strip(),
                "normalized_query": normalized_query,
                "payload": payload,
                "metadata": dict(metadata or {}),
                "cached_at": cached_at,
                "expires_at": expires_at,
            }
        )
        statement = statement.on_conflict_do_update(
            index_elements=["provider", "normalized_query"],
            set_={
                "query_text": statement.excluded.query_text,
                "payload": statement.excluded.payload,
                "metadata": statement.excluded["metadata"],
                "cached_at": statement.excluded.cached_at,
                "expires_at": statement.excluded.expires_at,
            },
        )

        try:
            await self.session.execute(statement)
            await self.session.commit()
        except SQLAlchemyError:
            await self.session.rollback()
            self.logger.exception(
                "geocode_cache_upsert_failed",
                extra={"provider": provider, "normalized_query": normalized_query},
            )
            return None

        record_persistence_batch("geocoding_cache", batch_size=1, rows_inserted=1)
        self.logger.info(
            "geocode_cache_upserted",
            extra={
                "provider": provider,
                "normalized_query": normalized_query,
                "ttl_seconds": ttl_seconds,
            },
        )
        return await self._get_geocode_row_by_normalized_query(
            provider=provider,
            normalized_query=normalized_query,
        )

    async def get_reverse_geocode_cache(
        self,
        provider: str,
        lat: float,
        lon: float,
        *,
        precision_digits: int = DEFAULT_REVERSE_GEOCODE_PRECISION,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        if self.session is None:
            return None

        coordinate_key = build_reverse_geocode_coordinate_key(
            lat=lat,
            lon=lon,
            precision_digits=precision_digits,
        )
        lookup_time = now or datetime.now(UTC)
        statement = (
            select(ReverseGeocodeCache)
            .where(
                ReverseGeocodeCache.provider == provider,
                ReverseGeocodeCache.coordinate_key == coordinate_key,
                ReverseGeocodeCache.expires_at > lookup_time,
            )
            .limit(1)
        )
        result = await self.session.execute(statement)
        row = result.scalars().first()
        if row is None:
            return None

        self.logger.info(
            "reverse_geocode_cache_hit_db",
            extra={"provider": provider, "coordinate_key": coordinate_key},
        )
        return self._serialize_reverse_geocode_row(row)

    async def upsert_reverse_geocode_cache(
        self,
        provider: str,
        lat: float,
        lon: float,
        payload: dict[str, Any] | list[Any],
        *,
        ttl_seconds: int,
        metadata: dict[str, Any] | None = None,
        precision_digits: int = DEFAULT_REVERSE_GEOCODE_PRECISION,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        if self.session is None:
            return None

        normalized_lat, normalized_lon = normalize_reverse_geocode_coordinates(
            lat=lat,
            lon=lon,
            precision_digits=precision_digits,
        )
        coordinate_key = build_reverse_geocode_coordinate_key(
            lat=lat,
            lon=lon,
            precision_digits=precision_digits,
        )
        cached_at = now or datetime.now(UTC)
        expires_at = cached_at + timedelta(seconds=ttl_seconds)
        statement = insert(ReverseGeocodeCache.__table__).values(
            {
                "provider": provider,
                "latitude": normalized_lat,
                "longitude": normalized_lon,
                "coordinate_key": coordinate_key,
                "precision_digits": precision_digits,
                "payload": payload,
                "metadata": dict(metadata or {}),
                "cached_at": cached_at,
                "expires_at": expires_at,
            }
        )
        statement = statement.on_conflict_do_update(
            index_elements=["provider", "coordinate_key"],
            set_={
                "latitude": statement.excluded.latitude,
                "longitude": statement.excluded.longitude,
                "precision_digits": statement.excluded.precision_digits,
                "payload": statement.excluded.payload,
                "metadata": statement.excluded["metadata"],
                "cached_at": statement.excluded.cached_at,
                "expires_at": statement.excluded.expires_at,
            },
        )

        try:
            await self.session.execute(statement)
            await self.session.commit()
        except SQLAlchemyError:
            await self.session.rollback()
            self.logger.exception(
                "reverse_geocode_cache_upsert_failed",
                extra={"provider": provider, "coordinate_key": coordinate_key},
            )
            return None

        record_persistence_batch("geocoding_cache", batch_size=1, rows_inserted=1)
        self.logger.info(
            "reverse_geocode_cache_upserted",
            extra={
                "provider": provider,
                "coordinate_key": coordinate_key,
                "ttl_seconds": ttl_seconds,
                "precision_digits": precision_digits,
            },
        )
        return await self._get_reverse_geocode_row_by_coordinate_key(
            provider=provider,
            coordinate_key=coordinate_key,
        )

    async def _get_geocode_row_by_normalized_query(
        self,
        *,
        provider: str,
        normalized_query: str,
    ) -> dict[str, Any] | None:
        if self.session is None:
            return None

        statement = (
            select(GeocodeCache)
            .where(
                GeocodeCache.provider == provider,
                GeocodeCache.normalized_query == normalized_query,
            )
            .limit(1)
        )
        result = await self.session.execute(statement)
        row = result.scalars().first()
        if row is None:
            return None
        return self._serialize_geocode_row(row)

    async def _get_reverse_geocode_row_by_coordinate_key(
        self,
        *,
        provider: str,
        coordinate_key: str,
    ) -> dict[str, Any] | None:
        if self.session is None:
            return None

        statement = (
            select(ReverseGeocodeCache)
            .where(
                ReverseGeocodeCache.provider == provider,
                ReverseGeocodeCache.coordinate_key == coordinate_key,
            )
            .limit(1)
        )
        result = await self.session.execute(statement)
        row = result.scalars().first()
        if row is None:
            return None
        return self._serialize_reverse_geocode_row(row)

    @staticmethod
    def _serialize_geocode_row(row: GeocodeCache) -> dict[str, Any]:
        return {
            "id": row.id,
            "provider": row.provider,
            "query_text": row.query_text,
            "normalized_query": row.normalized_query,
            "payload": row.payload,
            "metadata": row.metadata_json,
            "cached_at": row.cached_at,
            "expires_at": row.expires_at,
        }

    @staticmethod
    def _serialize_reverse_geocode_row(row: ReverseGeocodeCache) -> dict[str, Any]:
        return {
            "id": row.id,
            "provider": row.provider,
            "latitude": row.latitude,
            "longitude": row.longitude,
            "coordinate_key": row.coordinate_key,
            "precision_digits": row.precision_digits,
            "payload": row.payload,
            "metadata": row.metadata_json,
            "cached_at": row.cached_at,
            "expires_at": row.expires_at,
        }
