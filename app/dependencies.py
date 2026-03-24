from __future__ import annotations

from collections.abc import AsyncIterator

from arq.connections import ArqRedis
from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.cache import BaseAsyncCache
from app.core.jobs import BaseJobStore
from app.core.metrics import record_auth_failure, record_rate_limit_rejection
from app.core.rate_limit import BaseRateLimiter, RateLimitPolicy
from app.core.resilience import AsyncCircuitBreaker
from app.core.security import compare_api_keys, hash_sensitive_data
from app.db import get_session
from app.repositories.analytics_snapshots import AnalyticalSnapshotRepository
from app.repositories.cartographic_qa import CartographicQARepository
from app.repositories.catastro_cache import (
    CatastroMunicipalityAggregateCacheRepository,
    CatastroTerritorialAggregateCacheRepository,
)
from app.repositories.catalog import TableCatalogRepository
from app.repositories.geocoding import GeocodingCacheRepository
from app.repositories.ingestion import IngestionRepository
from app.repositories.series import SeriesRepository
from app.repositories.territorial_export_artifacts import TerritorialExportArtifactRepository
from app.repositories.territorial import TerritorialRepository
from app.services.asturias_resolver import AsturiasResolver
from app.services.catastro_client import CatastroClientService
from app.services.cartociudad_client import CartoCiudadClientService
from app.services.cartociudad_geocoding import CartoCiudadGeocodingService
from app.services.ine_client import INEClientService
from app.services.ine_operation_ingestion import INEOperationIngestionService
from app.services.territorial_analytics import TerritorialAnalyticsService
from app.services.territorial_exports import TerritorialExportService
from app.settings import Settings, get_settings


async def get_db_session() -> AsyncIterator[AsyncSession | None]:
    async for session in get_session():
        yield session


async def get_cache(request: Request) -> BaseAsyncCache:
    return request.app.state.cache


async def get_job_store(request: Request) -> BaseJobStore:
    return request.app.state.job_store


async def get_rate_limiter(request: Request) -> BaseRateLimiter:
    return request.app.state.rate_limiter


async def get_arq_pool(request: Request) -> ArqRedis | None:
    return getattr(request.app.state, "arq_redis", None)


def get_ine_client_service(
    request: Request,
    settings: Settings = Depends(get_settings),
    cache: BaseAsyncCache = Depends(get_cache),
) -> INEClientService:
    circuit_breaker: AsyncCircuitBreaker = request.app.state.ine_circuit_breaker
    return INEClientService(request.app.state.http_client, settings, cache, circuit_breaker)


def get_cartociudad_client_service(
    request: Request,
    settings: Settings = Depends(get_settings),
    cache: BaseAsyncCache = Depends(get_cache),
) -> CartoCiudadClientService:
    circuit_breaker: AsyncCircuitBreaker = request.app.state.cartociudad_circuit_breaker
    return CartoCiudadClientService(request.app.state.http_client, settings, cache, circuit_breaker)


def get_catastro_client_service(
    request: Request,
    settings: Settings = Depends(get_settings),
    cache: BaseAsyncCache = Depends(get_cache),
) -> CatastroClientService:
    circuit_breaker: AsyncCircuitBreaker = request.app.state.catastro_circuit_breaker
    return CatastroClientService(request.app.state.http_client, settings, cache, circuit_breaker)


def get_asturias_resolver(
    request: Request,
    ine_client: INEClientService = Depends(get_ine_client_service),
    cache: BaseAsyncCache = Depends(get_cache),
) -> AsturiasResolver:
    settings = request.app.state.settings
    return AsturiasResolver(
        ine_client=ine_client,
        cache=cache,
        geography_code=settings.default_geography_code,
        geography_name=settings.default_geography_name,
    )


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


def get_catastro_municipality_cache_repository(
    session: AsyncSession | None = Depends(get_db_session),
) -> CatastroMunicipalityAggregateCacheRepository:
    return CatastroMunicipalityAggregateCacheRepository(session=session)


def get_catastro_territorial_aggregate_cache_repository(
    session: AsyncSession | None = Depends(get_db_session),
) -> CatastroTerritorialAggregateCacheRepository:
    return CatastroTerritorialAggregateCacheRepository(session=session)


def get_analytical_snapshot_repository(
    session: AsyncSession | None = Depends(get_db_session),
) -> AnalyticalSnapshotRepository:
    return AnalyticalSnapshotRepository(session=session)


def get_territorial_repository(
    session: AsyncSession | None = Depends(get_db_session),
) -> TerritorialRepository:
    return TerritorialRepository(session=session)


def get_territorial_export_artifact_repository(
    session: AsyncSession | None = Depends(get_db_session),
) -> TerritorialExportArtifactRepository:
    return TerritorialExportArtifactRepository(session=session)


def get_qa_repository(
    session: AsyncSession | None = Depends(get_db_session),
) -> CartographicQARepository:
    return CartographicQARepository(session=session)


def get_operation_ingestion_service(
    ingestion_repo: IngestionRepository = Depends(get_ingestion_repository),
    series_repo: SeriesRepository = Depends(get_series_repository),
    catalog_repo: TableCatalogRepository = Depends(get_table_catalog_repository),
    settings: Settings = Depends(get_settings),
) -> INEOperationIngestionService:
    return INEOperationIngestionService(
        ingestion_repo=ingestion_repo,
        series_repo=series_repo,
        catalog_repo=catalog_repo,
        default_geography_code=settings.default_geography_code,
        default_geography_name=settings.default_geography_name,
        series_direct_max_series=settings.ine_series_direct_max_series,
        series_direct_max_errors_to_persist=settings.ine_series_direct_max_errors_to_persist,
        raw_payload_max_bytes=settings.ine_raw_payload_max_bytes,
        table_abort_threshold=settings.ine_table_abort_threshold,
        table_background_only_threshold=settings.ine_table_background_only_threshold,
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


def get_territorial_export_service(
    territorial_repo: TerritorialRepository = Depends(get_territorial_repository),
    series_repo: SeriesRepository = Depends(get_series_repository),
    analytics_service: TerritorialAnalyticsService = Depends(get_territorial_analytics_service),
    catastro_client: CatastroClientService = Depends(get_catastro_client_service),
    catastro_cache_repo: CatastroMunicipalityAggregateCacheRepository = Depends(
        get_catastro_municipality_cache_repository
    ),
    catastro_aggregate_cache_repo: CatastroTerritorialAggregateCacheRepository = Depends(
        get_catastro_territorial_aggregate_cache_repository
    ),
    ingestion_repo: IngestionRepository = Depends(get_ingestion_repository),
    artifact_repo: TerritorialExportArtifactRepository = Depends(
        get_territorial_export_artifact_repository
    ),
    settings: Settings = Depends(get_settings),
) -> TerritorialExportService:
    return TerritorialExportService(
        territorial_repo=territorial_repo,
        series_repo=series_repo,
        analytics_service=analytics_service,
        catastro_client=catastro_client,
        catastro_cache_repo=catastro_cache_repo,
        catastro_aggregate_cache_repo=catastro_aggregate_cache_repo,
        ingestion_repo=ingestion_repo,
        artifact_repo=artifact_repo,
        export_ttl_seconds=settings.territorial_export_ttl_seconds,
        catastro_cache_ttl_seconds=settings.catastro_cache_ttl_seconds,
        catastro_aggregate_cache_ttl_seconds=settings.catastro_aggregate_cache_ttl_seconds,
        catastro_aggregate_max_concurrency=settings.catastro_aggregate_max_concurrency,
    )


def get_cartociudad_geocoding_service(
    cartociudad_client: CartoCiudadClientService = Depends(get_cartociudad_client_service),
    geocoding_repo: GeocodingCacheRepository = Depends(get_geocoding_cache_repository),
    ingestion_repo: IngestionRepository = Depends(get_ingestion_repository),
    territorial_repo: TerritorialRepository = Depends(get_territorial_repository),
    settings: Settings = Depends(get_settings),
) -> CartoCiudadGeocodingService:
    return CartoCiudadGeocodingService(
        cartociudad_client=cartociudad_client,
        geocoding_repo=geocoding_repo,
        ingestion_repo=ingestion_repo,
        territorial_repo=territorial_repo,
        cache_ttl_seconds=settings.cache_ttl_seconds,
    )


async def require_api_key(
    request: Request,
    settings: Settings = Depends(get_settings),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> None:
    request.state.api_key_authenticated = False
    if not settings.requires_api_key:
        if settings.api_key and compare_api_keys(x_api_key, settings.api_key):
            request.state.api_key_authenticated = True
        return

    if not settings.api_key:
        record_auth_failure("server_api_key_not_configured")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Server API key is not configured.",
        )

    if x_api_key is None:
        record_auth_failure("missing_api_key")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key.",
            headers={"WWW-Authenticate": "X-API-Key"},
        )

    if not compare_api_keys(x_api_key, settings.api_key):
        record_auth_failure("invalid_api_key")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key.",
            headers={"WWW-Authenticate": "X-API-Key"},
        )
    request.state.api_key_authenticated = True


def build_rate_limit_dependency(policy: RateLimitPolicy):
    async def enforce_rate_limit(
        request: Request,
        settings: Settings = Depends(get_settings),
        rate_limiter: BaseRateLimiter = Depends(get_rate_limiter),
        x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    ) -> None:
        if not settings.rate_limit_enabled:
            return

        api_key_authenticated = getattr(request.state, "api_key_authenticated", False)
        if not api_key_authenticated and settings.api_key and x_api_key:
            api_key_authenticated = compare_api_keys(x_api_key, settings.api_key)

        auth_mode = "api_key" if api_key_authenticated else "anonymous"
        limit = (
            policy.authenticated_requests_per_minute
            if api_key_authenticated
            else policy.public_requests_per_minute
        )
        client_ip = _resolve_client_ip(request)
        bucket_key = f"{policy.name}:{auth_mode}:{hash_sensitive_data(client_ip)}"
        snapshot = await rate_limiter.increment(bucket_key, window_seconds=policy.window_seconds)
        if snapshot.count <= limit:
            return

        record_rate_limit_rejection(policy.name, auth_mode)
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "message": "Rate limit exceeded.",
                "policy": policy.name,
                "limit": limit,
                "window_seconds": policy.window_seconds,
                "auth_mode": auth_mode,
            },
            headers={"Retry-After": str(snapshot.retry_after_seconds)},
        )

    return enforce_rate_limit


def _resolve_client_ip(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",", 1)[0].strip()
    client = request.client
    if client and client.host:
        return client.host
    return "unknown"
