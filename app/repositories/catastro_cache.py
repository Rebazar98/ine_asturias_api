from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.core.metrics import record_persistence_batch
from app.models import CatastroMunicipalityAggregateCache


CATASTRO_PROVIDER_FAMILY_URBANO = "catastro_urbano_municipality_aggregates"


class CatastroMunicipalityAggregateCacheRepository:
    def __init__(self, session: AsyncSession | None) -> None:
        self.session = session
        self.logger = get_logger("app.repositories.catastro_cache")

    async def get_fresh_payload(
        self,
        *,
        provider_family: str,
        municipality_code: str,
        reference_year: str,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        if self.session is None:
            return None

        lookup_time = now or datetime.now(timezone.utc)
        statement = (
            select(CatastroMunicipalityAggregateCache)
            .where(
                CatastroMunicipalityAggregateCache.provider_family == provider_family,
                CatastroMunicipalityAggregateCache.municipality_code == municipality_code,
                CatastroMunicipalityAggregateCache.reference_year == reference_year,
                CatastroMunicipalityAggregateCache.expires_at > lookup_time,
            )
            .limit(1)
        )
        result = await self.session.execute(statement)
        row = result.scalars().first()
        if row is None:
            return None

        self.logger.info(
            "catastro_cache_hit_db",
            extra={
                "provider_family": provider_family,
                "municipality_code": municipality_code,
                "reference_year": reference_year,
            },
        )
        return self._serialize_row(row)

    async def upsert_payload(
        self,
        *,
        provider_family: str,
        municipality_code: str,
        reference_year: str,
        payload: dict[str, Any] | list[Any],
        ttl_seconds: int,
        metadata: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        if self.session is None or ttl_seconds <= 0:
            return None

        cached_at = now or datetime.now(timezone.utc)
        expires_at = cached_at + timedelta(seconds=ttl_seconds)
        statement = insert(CatastroMunicipalityAggregateCache.__table__).values(
            {
                "provider_family": provider_family,
                "municipality_code": municipality_code,
                "reference_year": reference_year,
                "payload": payload,
                "metadata": dict(metadata or {}),
                "cached_at": cached_at,
                "expires_at": expires_at,
            }
        )
        statement = statement.on_conflict_do_update(
            index_elements=["provider_family", "municipality_code", "reference_year"],
            set_={
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
                "catastro_cache_upsert_failed",
                extra={
                    "provider_family": provider_family,
                    "municipality_code": municipality_code,
                    "reference_year": reference_year,
                },
            )
            return None

        record_persistence_batch("catastro_cache", batch_size=1, rows_inserted=1)
        self.logger.info(
            "catastro_cache_upserted",
            extra={
                "provider_family": provider_family,
                "municipality_code": municipality_code,
                "reference_year": reference_year,
                "ttl_seconds": ttl_seconds,
            },
        )
        return await self._get_row(
            provider_family=provider_family,
            municipality_code=municipality_code,
            reference_year=reference_year,
        )

    async def _get_row(
        self,
        *,
        provider_family: str,
        municipality_code: str,
        reference_year: str,
    ) -> dict[str, Any] | None:
        if self.session is None:
            return None

        statement = (
            select(CatastroMunicipalityAggregateCache)
            .where(
                CatastroMunicipalityAggregateCache.provider_family == provider_family,
                CatastroMunicipalityAggregateCache.municipality_code == municipality_code,
                CatastroMunicipalityAggregateCache.reference_year == reference_year,
            )
            .limit(1)
        )
        result = await self.session.execute(statement)
        row = result.scalars().first()
        if row is None:
            return None
        return self._serialize_row(row)

    @staticmethod
    def _serialize_row(row: CatastroMunicipalityAggregateCache) -> dict[str, Any]:
        return {
            "id": row.id,
            "provider_family": row.provider_family,
            "municipality_code": row.municipality_code,
            "reference_year": row.reference_year,
            "payload": row.payload,
            "metadata": row.metadata_json,
            "cached_at": row.cached_at,
            "expires_at": row.expires_at,
        }
