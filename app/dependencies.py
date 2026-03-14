from __future__ import annotations

from collections.abc import AsyncIterator

from arq.connections import ArqRedis
from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.cache import BaseAsyncCache
from app.core.jobs import BaseJobStore
from app.db import get_session
from app.repositories.analytics_snapshots import AnalyticalSnapshotRepository
from app.repositories.catalog import TableCatalogRepository
from app.repositories.geocoding import GeocodingCacheRepository
from app.repositories.ingestion import IngestionRepository
from app.repositories.series import SeriesRepository
from app.repositories.territorial import TerritorialRepository
from app.services.asturias_resolver import AsturiasResolver
from app.services.cartociudad_client import CartoCiudadClientService
from app.services.ine_client import INEClientService
from app.services.ine_operation_ingestion import INEOperationIngestionService
from app.services.territorial_analytics import TerritorialAnalyticsService
from app.settings import Settings, get_settings


async def get_db_session() -> AsyncIterator[AsyncSession | None]:
    async for session in get_session():
        yield session


async def get_cache(request: Request) -> BaseAsyncCache:
    return request.app.state.cache


async def get_job_store(request: Request) -> BaseJobStore:
    return request.app.state.job_store


async def get_arq_pool(request: Request) -> ArqRedis | None:
    return getattr(request.app.state, "arq_redis", None)


def get_ine_client_service(
    request: Request,
    settings: Settings = Depends(get_settings),
    cache: BaseAsyncCache = Depends(get_cache),
) -> INEClientService:
    return INEClientService(request.app.state.http_client, settings, cache)


def get_cartociudad_client_service(
    request: Request,
    settings: Settings = Depends(get_settings),
    cache: BaseAsyncCache = Depends(get_cache),
) -> CartoCiudadClientService:
    return CartoCiudadClientService(request.app.state.http_client, settings, cache)


def get_asturias_resolver(
    ine_client: INEClientService = Depends(get_ine_client_service),
    cache: BaseAsyncCache = Depends(get_cache),
) -> AsturiasResolver:
    return AsturiasResolver(ine_client=ine_client, cache=cache)


def get_ingestion_repository(
    session: AsyncSession | None = Depends(get_db_session),
) -> IngestionRepository:
    return IngestionRepository(session=session)


def get_series_repository(
    session: AsyncSession | None = Depends(get_db_session),
) -> SeriesRepository:
    return SeriesRepository(session=session)


def get_table_catalog_repository(
    session: AsyncSession | None = Depends(get_db_session),
) -> TableCatalogRepository:
    return TableCatalogRepository(session=session)


def get_geocoding_cache_repository(
    session: AsyncSession | None = Depends(get_db_session),
) -> GeocodingCacheRepository:
    return GeocodingCacheRepository(session=session)


def get_analytical_snapshot_repository(
    session: AsyncSession | None = Depends(get_db_session),
) -> AnalyticalSnapshotRepository:
    return AnalyticalSnapshotRepository(session=session)


def get_territorial_repository(
    session: AsyncSession | None = Depends(get_db_session),
) -> TerritorialRepository:
    return TerritorialRepository(session=session)


def get_operation_ingestion_service(
    ingestion_repo: IngestionRepository = Depends(get_ingestion_repository),
    series_repo: SeriesRepository = Depends(get_series_repository),
    catalog_repo: TableCatalogRepository = Depends(get_table_catalog_repository),
) -> INEOperationIngestionService:
    return INEOperationIngestionService(
        ingestion_repo=ingestion_repo,
        series_repo=series_repo,
        catalog_repo=catalog_repo,
    )


def get_territorial_analytics_service(
    territorial_repo: TerritorialRepository = Depends(get_territorial_repository),
    series_repo: SeriesRepository = Depends(get_series_repository),
    analytical_snapshot_repo: AnalyticalSnapshotRepository = Depends(
        get_analytical_snapshot_repository
    ),
    settings: Settings = Depends(get_settings),
) -> TerritorialAnalyticsService:
    return TerritorialAnalyticsService(
        territorial_repo=territorial_repo,
        series_repo=series_repo,
        analytical_snapshot_repo=analytical_snapshot_repo,
        analytical_snapshot_ttl_seconds=settings.analytical_snapshot_ttl_seconds,
    )


async def require_api_key(
    settings: Settings = Depends(get_settings),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> None:
    if not settings.api_key:
        return
    if x_api_key != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key.",
        )
