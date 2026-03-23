from __future__ import annotations

import asyncio
import json
from datetime import datetime, UTC
from time import perf_counter

from arq.connections import ArqRedis
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status

from app.core.jobs import BaseJobStore
from app.core.logging import get_logger
from app.core.metrics import record_job_duration, record_territorial_point_resolution
from app.core.rate_limit import RateLimitPolicy
from app.dependencies import (
    build_rate_limit_dependency,
    get_arq_pool,
    get_cartociudad_geocoding_service,
    get_job_store,
    get_series_repository,
    get_settings,
    get_territorial_analytics_service,
    get_territorial_export_artifact_repository,
    get_territorial_export_service,
    get_territorial_repository,
    require_api_key,
)
from app.repositories.series import SeriesRepository
from app.repositories.territorial_export_artifacts import TerritorialExportArtifactRepository
from app.repositories.territorial import (
    TERRITORIAL_UNIT_LEVEL_COUNTRY,
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
    TerritorialRepository,
)
from app.schemas import (
    BackgroundJobStatusResponse,
    ErrorResponse,
    GeocodeResponse,
    GeocodingCoordinatesResponse,
    IndicadorSeriesFiltersResponse,
    IndicadorSeriesResponse,
    IndicatorSeriesPointResponse,
    IndicatorTerritoryResponse,
    ReverseGeocodeResponse,
    TerritorialCatalogLevelCoverageResponse,
    TerritorialCatalogResourceResponse,
    TerritorialCatalogResponse,
    TerritorialCatalogSummaryResponse,
    TerritorialExportJobAcceptedResponse,
    TerritorialExportJobStatusResponse,
    TerritorialExportRequest,
    TerritorialPointResolutionCoverageResponse,
    TerritorialPointResolutionResponse,
    TerritorialPointResolutionResultResponse,
    TerritorialReportJobAcceptedResponse,
    TerritorialMunicipalitySummaryResponse,
    TerritorialUnitDetailResponse,
    TerritorialUnitListFiltersResponse,
    TerritorialUnitListResponse,
    TerritorialUnitSummaryResponse,
)
from app.services.cartociudad_geocoding import CartoCiudadGeocodingService
from app.services.catastro_client import CatastroClientError
from app.services.ign_admin_boundaries import (
    IGN_ADMIN_BOUNDARY_SOURCE,
    IGN_ADMIN_CATALOG_RESOURCE_KEY,
)
from app.services.territorial_analytics import (
    MUNICIPALITY_REPORT_TYPE,
    TerritorialAnalyticsService,
)
from app.services.territorial_exports import TERRITORIAL_EXPORT_JOB_TYPE, TerritorialExportService
from app.settings import Settings


router = APIRouter(tags=["territorial"], dependencies=[Depends(require_api_key)])
logger = get_logger("app.api.routes_territorial")
MUNICIPALITY_REPORT_JOB_TYPE = "territorial_municipality_report"
TERRITORIAL_CATALOG_SOURCE = "internal.catalog.territorial"
TERRITORIAL_EXPORT_CONTENT_TYPE = "application/zip"
GEOCODE_RATE_LIMIT = build_rate_limit_dependency(
    RateLimitPolicy(
        name="geocode",
        public_requests_per_minute=100,
        authenticated_requests_per_minute=1000,
    )
)
REVERSE_GEOCODE_RATE_LIMIT = build_rate_limit_dependency(
    RateLimitPolicy(
        name="reverse_geocode",
        public_requests_per_minute=100,
        authenticated_requests_per_minute=1000,
    )
)
POINT_RESOLUTION_RATE_LIMIT = build_rate_limit_dependency(
    RateLimitPolicy(
        name="territorial_resolve_point",
        public_requests_per_minute=100,
        authenticated_requests_per_minute=1000,
    )
)
TERRITORIAL_EXPORT_RATE_LIMIT = build_rate_limit_dependency(
    RateLimitPolicy(
        name="territorial_export",
        public_requests_per_minute=10,
        authenticated_requests_per_minute=1000,
    )
)
TERRITORIAL_CATALOG_LEVEL_PATHS = {
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY: {
        "list_path": "/territorios/comunidades-autonomas",
        "detail_path": None,
        "summary_path": None,
        "report_job_path": None,
    },
    TERRITORIAL_UNIT_LEVEL_PROVINCE: {
        "list_path": "/territorios/provincias",
        "detail_path": None,
        "summary_path": None,
        "report_job_path": None,
    },
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY: {
        "list_path": None,
        "detail_path": "/municipio/{codigo_ine}",
        "summary_path": "/territorios/municipio/{codigo_ine}/resumen",
        "report_job_path": "/territorios/municipio/{codigo_ine}/informe",
    },
}


def _build_territorial_catalog_resources() -> list[TerritorialCatalogResourceResponse]:
    return [
        TerritorialCatalogResourceResponse(
            resource_key="territorial.autonomous_communities.list",
            title="Autonomous communities list",
            category="territorial_read",
            method="GET",
            path="/territorios/comunidades-autonomas",
            summary="List autonomous communities from the internal territorial model.",
            unit_levels=[TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY],
            query_params=["page", "page_size", "active_only"],
            response_contract="TerritorialUnitListResponse",
            supports_pagination=True,
        ),
        TerritorialCatalogResourceResponse(
            resource_key="territorial.provinces.list",
            title="Provinces list",
            category="territorial_read",
            method="GET",
            path="/territorios/provincias",
            summary="List provinces, optionally filtered by autonomous community code.",
            unit_levels=[TERRITORIAL_UNIT_LEVEL_PROVINCE],
            query_params=["autonomous_community_code", "page", "page_size", "active_only"],
            response_contract="TerritorialUnitListResponse",
            supports_pagination=True,
        ),
        TerritorialCatalogResourceResponse(
            resource_key="territorial.municipality.detail",
            title="Municipality detail",
            category="territorial_read",
            method="GET",
            path="/municipio/{codigo_ine}",
            summary="Get territorial detail for a municipality by canonical INE code.",
            unit_levels=[TERRITORIAL_UNIT_LEVEL_MUNICIPALITY],
            path_params=["codigo_ine"],
            response_contract="TerritorialUnitDetailResponse",
        ),
        TerritorialCatalogResourceResponse(
            resource_key="territorial.geocode.query",
            title="Semantic geocode query",
            category="territorial_read",
            method="GET",
            path="/geocode",
            summary="Resolve a text query through the internal semantic geocoding contract over CartoCiudad.",
            unit_levels=[
                TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
                TERRITORIAL_UNIT_LEVEL_PROVINCE,
                TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
            ],
            query_params=["query"],
            response_contract="GeocodeResponse",
        ),
        TerritorialCatalogResourceResponse(
            resource_key="territorial.reverse_geocode.query",
            title="Semantic reverse geocode query",
            category="territorial_read",
            method="GET",
            path="/reverse_geocode",
            summary="Resolve coordinates through the internal semantic reverse geocoding contract over CartoCiudad.",
            unit_levels=[
                TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
                TERRITORIAL_UNIT_LEVEL_PROVINCE,
                TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
            ],
            query_params=["lat", "lon"],
            response_contract="ReverseGeocodeResponse",
        ),
        TerritorialCatalogResourceResponse(
            resource_key="territorial.resolve_point.query",
            title="Territorial point resolution",
            category="territorial_read",
            method="GET",
            path="/territorios/resolve-point",
            summary=(
                "Resolve a coordinate pair against internal administrative boundary coverage "
                "without exposing raw geometry."
            ),
            unit_levels=[
                TERRITORIAL_UNIT_LEVEL_COUNTRY,
                TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
                TERRITORIAL_UNIT_LEVEL_PROVINCE,
                TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
            ],
            query_params=["lat", "lon"],
            response_contract="TerritorialPointResolutionResponse",
        ),
        TerritorialCatalogResourceResponse(
            resource_key=IGN_ADMIN_CATALOG_RESOURCE_KEY,
            title="IGN administrative boundary coverage",
            category="territorial_read",
            method="GET",
            path="/territorios/catalogo",
            summary=(
                "Discover internal administrative boundary coverage loaded from IGN/CNIG "
                "without exposing raw provider payloads or geometry contracts."
            ),
            unit_levels=[
                TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
                TERRITORIAL_UNIT_LEVEL_PROVINCE,
                TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
            ],
            response_contract="TerritorialCatalogResponse",
        ),
        TerritorialCatalogResourceResponse(
            resource_key="territorial.municipality.summary",
            title="Municipality summary",
            category="territorial_analytics",
            method="GET",
            path="/territorios/municipio/{codigo_ine}/resumen",
            summary="Get a semantic municipality summary over the internal analytical contract.",
            unit_levels=[TERRITORIAL_UNIT_LEVEL_MUNICIPALITY],
            path_params=["codigo_ine"],
            query_params=[
                "operation_code",
                "variable_id",
                "period_from",
                "period_to",
                "page",
                "page_size",
            ],
            response_contract="TerritorialMunicipalitySummaryResponse",
            supports_pagination=True,
        ),
        TerritorialCatalogResourceResponse(
            resource_key="territorial.municipality.report_job",
            title="Municipality report job",
            category="territorial_analytics",
            method="POST",
            path="/territorios/municipio/{codigo_ine}/informe",
            summary="Queue a municipality analytical report job with reusable snapshot support.",
            unit_levels=[TERRITORIAL_UNIT_LEVEL_MUNICIPALITY],
            path_params=["codigo_ine"],
            query_params=[
                "operation_code",
                "variable_id",
                "period_from",
                "period_to",
                "page",
                "page_size",
            ],
            response_contract="TerritorialReportJobAcceptedResponse",
            supports_background_job=True,
            supports_snapshot_reuse=True,
        ),
        TerritorialCatalogResourceResponse(
            resource_key="territorial.jobs.status",
            title="Territorial job status",
            category="territorial_jobs",
            method="GET",
            path="/territorios/jobs/{job_id}",
            summary="Read the status and result of a previously queued territorial job.",
            path_params=["job_id"],
            response_contract="BackgroundJobStatusResponse",
        ),
        TerritorialCatalogResourceResponse(
            resource_key="territorial.export.job",
            title="Territorial export job",
            category="territorial_jobs",
            method="POST",
            path="/territorios/export",
            summary=(
                "Queue a multi-source territorial export bundle for a canonical territorial entity, "
                "including opt-in Catastro municipality aggregates."
            ),
            unit_levels=[
                TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
                TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
            ],
            response_contract="TerritorialExportJobAcceptedResponse",
            supports_background_job=True,
        ),
        TerritorialCatalogResourceResponse(
            resource_key="territorial.export.status",
            title="Territorial export status",
            category="territorial_jobs",
            method="GET",
            path="/territorios/exports/{job_id}",
            summary="Read the status and result of a previously queued territorial export job.",
            path_params=["job_id"],
            response_contract="TerritorialExportJobStatusResponse",
        ),
        TerritorialCatalogResourceResponse(
            resource_key="territorial.export.download",
            title="Territorial export download",
            category="territorial_jobs",
            method="GET",
            path="/territorios/exports/{job_id}/download",
            summary="Download the ZIP bundle produced by a completed territorial export job.",
            path_params=["job_id"],
            response_contract="application/zip",
        ),
    ]


def _build_territorial_catalog_level_coverage(
    row: dict,
) -> TerritorialCatalogLevelCoverageResponse:
    paths = TERRITORIAL_CATALOG_LEVEL_PATHS.get(row["unit_level"], {})
    return TerritorialCatalogLevelCoverageResponse(
        unit_level=row["unit_level"],
        country_code=row["country_code"],
        units_total=row["units_total"],
        active_units=row["active_units"],
        geometry_units=row.get("geometry_units", 0),
        centroid_units=row.get("centroid_units", 0),
        boundary_source=row.get("boundary_source"),
        canonical_code_strategy=row.get("canonical_code_strategy"),
        list_path=paths.get("list_path"),
        detail_path=paths.get("detail_path"),
        summary_path=paths.get("summary_path"),
        report_job_path=paths.get("report_job_path"),
    )


@router.get(
    "/territorios/jobs/{job_id}",
    response_model=BackgroundJobStatusResponse,
    tags=["territorial-jobs"],
    summary="Get the status of a territorial background job",
)
async def get_territorial_job_status(
    job_id: str,
    job_store: BaseJobStore = Depends(get_job_store),
) -> BackgroundJobStatusResponse:
    job = await job_store.get_job(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"message": "Job not found.", "job_id": job_id},
        )
    return BackgroundJobStatusResponse(**job)


@router.post(
    "/territorios/export",
    response_model=TerritorialExportJobAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["territorial-jobs"],
    summary="Queue a multi-source territorial export bundle",
    responses={
        422: {"model": ErrorResponse, "description": "Validation error"},
        503: {
            "model": ErrorResponse,
            "description": "Catastro service or circuit breaker unavailable",
        },
    },
)
async def create_territorial_export_job(
    request: Request,
    export_request: TerritorialExportRequest,
    _: None = Depends(TERRITORIAL_EXPORT_RATE_LIMIT),
    job_store: BaseJobStore = Depends(get_job_store),
    export_service: TerritorialExportService = Depends(get_territorial_export_service),
    arq_pool: ArqRedis | None = Depends(get_arq_pool),
    settings: Settings = Depends(get_settings),
) -> TerritorialExportJobAcceptedResponse:
    job_params = export_request.model_dump(mode="json")
    job = await job_store.create_job(job_type=TERRITORIAL_EXPORT_JOB_TYPE, params=job_params)
    job_id = job["job_id"]

    try:
        if arq_pool is not None:
            await arq_pool.enqueue_job(
                "run_territorial_export_job",
                job_id,
                job_params,
                _job_id=job_id,
                _queue_name=settings.job_queue_name,
            )
        else:
            task = asyncio.create_task(
                _run_territorial_export_job_inline(
                    job_store=job_store,
                    export_service=export_service,
                    job_id=job_id,
                    export_request=export_request,
                )
            )
            request.app.state.inline_job_tasks = getattr(
                request.app.state, "inline_job_tasks", set()
            )
            request.app.state.inline_job_tasks.add(task)
            task.add_done_callback(
                lambda completed: request.app.state.inline_job_tasks.discard(completed)
            )
    except Exception as exc:
        await job_store.fail_job(
            job_id,
            {
                "message": "Could not enqueue the territorial export job.",
                "unit_level": export_request.unit_level,
                "code_value": export_request.code_value,
                "error": str(exc),
            },
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"message": "Background queue unavailable.", "job_id": job_id},
        )

    logger.info(
        "territorial_export_job_queued",
        extra={
            "job_id": job_id,
            "unit_level": export_request.unit_level,
            "code_value": export_request.code_value,
            "providers": export_request.include_providers,
        },
    )
    return TerritorialExportJobAcceptedResponse(
        job_id=job_id,
        status=job["status"],
        status_path=f"/territorios/exports/{job_id}",
        params=export_request,
    )


@router.get(
    "/territorios/exports/{job_id}",
    response_model=TerritorialExportJobStatusResponse,
    tags=["territorial-jobs"],
    summary="Get the status of a territorial export job",
)
async def get_territorial_export_status(
    job_id: str,
    job_store: BaseJobStore = Depends(get_job_store),
) -> TerritorialExportJobStatusResponse:
    job = await job_store.get_job(job_id)
    if job is None or job.get("job_type") != TERRITORIAL_EXPORT_JOB_TYPE:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"message": "Territorial export job not found.", "job_id": job_id},
        )
    return TerritorialExportJobStatusResponse(**job)


@router.get(
    "/territorios/exports/{job_id}/download",
    tags=["territorial-jobs"],
    summary="Download a completed territorial export bundle",
)
async def download_territorial_export(
    job_id: str,
    job_store: BaseJobStore = Depends(get_job_store),
    artifact_repo: TerritorialExportArtifactRepository = Depends(
        get_territorial_export_artifact_repository
    ),
) -> Response:
    job = await job_store.get_job(job_id)
    if job is None or job.get("job_type") != TERRITORIAL_EXPORT_JOB_TYPE:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"message": "Territorial export job not found.", "job_id": job_id},
        )
    if job.get("status") != "completed":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "message": "Territorial export artifact is not available yet.",
                "job_id": job_id,
            },
        )

    result = job.get("result") or {}
    export_id = result.get("export_id")
    if not isinstance(export_id, int):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"message": "Territorial export artifact was not found.", "job_id": job_id},
        )

    artifact = await artifact_repo.get_by_export_id(export_id)
    if artifact is None or artifact["expires_at"] <= datetime.now(UTC):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"message": "Territorial export artifact was not found.", "job_id": job_id},
        )

    return Response(
        content=artifact["payload_bytes"],
        media_type=artifact.get("content_type") or TERRITORIAL_EXPORT_CONTENT_TYPE,
        headers={
            "Content-Disposition": f'attachment; filename="{artifact["filename"]}"',
        },
    )


@router.get(
    "/geocode",
    response_model=GeocodeResponse,
    tags=["territorial-semantic"],
    summary="Geocode a textual territorial query",
    description=(
        "Semantic geocoding endpoint over CartoCiudad with persistent cache fallback. "
        "The public contract is internal to this API and does not expose the raw provider payload."
    ),
)
async def geocode(
    query: str = Query(..., min_length=1, max_length=512),
    _: None = Depends(GEOCODE_RATE_LIMIT),
    geocoding_service: CartoCiudadGeocodingService = Depends(get_cartociudad_geocoding_service),
) -> GeocodeResponse:
    return await geocoding_service.geocode(query)


@router.get(
    "/reverse_geocode",
    response_model=ReverseGeocodeResponse,
    tags=["territorial-semantic"],
    summary="Reverse geocode a coordinate pair",
    description=(
        "Semantic reverse geocoding endpoint over CartoCiudad with persistent cache fallback. "
        "The public contract is internal to this API and does not expose the raw provider payload."
    ),
)
async def reverse_geocode(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    _: None = Depends(REVERSE_GEOCODE_RATE_LIMIT),
    geocoding_service: CartoCiudadGeocodingService = Depends(get_cartociudad_geocoding_service),
) -> ReverseGeocodeResponse:
    return await geocoding_service.reverse_geocode(lat, lon)


@router.get(
    "/territorios/resolve-point",
    response_model=TerritorialPointResolutionResponse,
    tags=["territorial-semantic"],
    summary="Resolve a point against internal territorial boundary coverage",
    description=(
        "Semantic territorial resolution over internal administrative boundaries. "
        "This endpoint returns the best internal territorial match and hierarchy "
        "without exposing public geometry contracts."
    ),
)
async def resolve_territorial_point(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    _: None = Depends(POINT_RESOLUTION_RATE_LIMIT),
    territorial_repo: TerritorialRepository = Depends(get_territorial_repository),
) -> TerritorialPointResolutionResponse:
    started_at = perf_counter()
    resolution = await territorial_repo.resolve_point(lat=lat, lon=lon)

    if resolution is None:
        duration_seconds = perf_counter() - started_at
        record_territorial_point_resolution("no_coverage", duration_seconds)
        logger.info(
            "territorial_point_resolution_unavailable",
            extra={
                "lat_hint": round(lat, 2),
                "lon_hint": round(lon, 2),
                "duration_ms": round(duration_seconds * 1000, 2),
            },
        )
        return TerritorialPointResolutionResponse(
            query_coordinates=GeocodingCoordinatesResponse(lat=lat, lon=lon),
            result=None,
            metadata={"reason": "no_boundary_coverage_loaded"},
        )

    coverage = resolution["coverage"]
    duration_seconds = perf_counter() - started_at
    if resolution["best_match"] is None:
        outcome = "no_coverage" if coverage["boundary_source"] is None else "no_match"
        reason = (
            "no_boundary_coverage_loaded"
            if coverage["boundary_source"] is None
            else "outside_loaded_coverage"
        )
        record_territorial_point_resolution(outcome, duration_seconds)
        logger.info(
            "territorial_point_resolution_no_match",
            extra={
                "lat_hint": round(lat, 2),
                "lon_hint": round(lon, 2),
                "levels_considered": coverage["levels_considered"],
                "duration_ms": round(duration_seconds * 1000, 2),
            },
        )
        return TerritorialPointResolutionResponse(
            query_coordinates=GeocodingCoordinatesResponse(lat=lat, lon=lon),
            result=None,
            metadata={"reason": reason},
        )

    ambiguity_detected = bool(resolution.get("ambiguity_detected"))
    record_territorial_point_resolution(
        "ambiguous_match" if ambiguity_detected else "matched",
        duration_seconds,
    )
    logger.info(
        "territorial_point_resolution_response_ready",
        extra={
            "lat_hint": round(lat, 2),
            "lon_hint": round(lon, 2),
            "best_match_unit_level": resolution["best_match"]["unit_level"],
            "levels_matched": coverage["levels_matched"],
            "ambiguity_detected": ambiguity_detected,
            "duration_ms": round(duration_seconds * 1000, 2),
        },
    )
    return TerritorialPointResolutionResponse(
        query_coordinates=GeocodingCoordinatesResponse(lat=lat, lon=lon),
        result=TerritorialPointResolutionResultResponse(
            matched_by="geometry_cover",
            best_match=TerritorialUnitSummaryResponse(**resolution["best_match"]),
            hierarchy=[TerritorialUnitSummaryResponse(**item) for item in resolution["hierarchy"]],
            coverage=TerritorialPointResolutionCoverageResponse(**coverage),
        ),
        metadata={
            "ambiguity_detected": ambiguity_detected,
            "ambiguity_by_level": resolution.get("ambiguity_by_level", {}),
        },
    )


@router.get(
    "/territorios/comunidades-autonomas",
    response_model=TerritorialUnitListResponse,
    tags=["territorial-read"],
    summary="List autonomous communities from the internal territorial model",
)
async def list_autonomous_communities(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    active_only: bool = Query(default=True),
    territorial_repo: TerritorialRepository = Depends(get_territorial_repository),
) -> TerritorialUnitListResponse:
    result = await territorial_repo.list_units(
        unit_level=TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
        page=page,
        page_size=page_size,
        country_code="ES",
        active_only=active_only,
    )
    return TerritorialUnitListResponse(
        items=[TerritorialUnitSummaryResponse(**item) for item in result["items"]],
        total=result["total"],
        page=result["page"],
        page_size=result["page_size"],
        pages=result["pages"],
        has_next=result["has_next"],
        has_previous=result["has_previous"],
        filters=TerritorialUnitListFiltersResponse(**result["filters"]),
    )


@router.get(
    "/territorios/provincias",
    response_model=TerritorialUnitListResponse,
    tags=["territorial-read"],
    summary="List provinces from the internal territorial model",
)
async def list_provinces(
    autonomous_community_code: str | None = Query(default=None, min_length=1),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    active_only: bool = Query(default=True),
    territorial_repo: TerritorialRepository = Depends(get_territorial_repository),
) -> TerritorialUnitListResponse:
    parent_id = None
    if autonomous_community_code is not None:
        parent_lookup = await territorial_repo.get_unit_by_canonical_code(
            unit_level=TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
            code_value=autonomous_community_code,
        )
        if parent_lookup is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "message": "Autonomous community code was not found.",
                    "autonomous_community_code": autonomous_community_code,
                },
            )
        parent_id = parent_lookup["id"]

    result = await territorial_repo.list_units(
        unit_level=TERRITORIAL_UNIT_LEVEL_PROVINCE,
        page=page,
        page_size=page_size,
        country_code="ES",
        parent_id=parent_id,
        active_only=active_only,
    )
    return TerritorialUnitListResponse(
        items=[TerritorialUnitSummaryResponse(**item) for item in result["items"]],
        total=result["total"],
        page=result["page"],
        page_size=result["page_size"],
        pages=result["pages"],
        has_next=result["has_next"],
        has_previous=result["has_previous"],
        filters=TerritorialUnitListFiltersResponse(**result["filters"]),
    )


@router.get(
    "/territorios/catalogo",
    response_model=TerritorialCatalogResponse,
    tags=["territorial-discovery"],
    summary="Discover published territorial semantic resources",
    description=(
        "Minimal discovery catalog for automations and programmatic clients. "
        "It exposes stable internal resources, territorial levels and basic coverage "
        "without exposing internal tables or provider raw payloads."
    ),
)
async def get_territorial_catalog(
    territorial_repo: TerritorialRepository = Depends(get_territorial_repository),
) -> TerritorialCatalogResponse:
    resources = _build_territorial_catalog_resources()
    coverage_rows = await territorial_repo.get_catalog_coverage(country_code="ES")
    territorial_levels = [_build_territorial_catalog_level_coverage(row) for row in coverage_rows]
    return TerritorialCatalogResponse(
        source=TERRITORIAL_CATALOG_SOURCE,
        generated_at=datetime.now(UTC),
        summary=TerritorialCatalogSummaryResponse(
            resources_total=len(resources),
            territorial_levels_total=len(territorial_levels),
            read_resources_total=sum(
                1 for resource in resources if resource.category == "territorial_read"
            ),
            analytics_resources_total=sum(
                1 for resource in resources if resource.category == "territorial_analytics"
            ),
            job_resources_total=sum(
                1 for resource in resources if resource.category == "territorial_jobs"
            ),
        ),
        territorial_levels=territorial_levels,
        resources=resources,
        metadata={
            "default_country_code": "ES",
            "intended_consumers": ["n8n", "agents", "programmatic_clients"],
            "raw_provider_contracts_exposed": False,
            "discovery_scope": "published_territorial_resources",
            "official_sources": [
                "ine",
                "cartociudad",
                IGN_ADMIN_BOUNDARY_SOURCE,
                "catastro_urbano",
            ],
        },
    )


@router.get(
    "/municipio/{codigo_ine}",
    response_model=TerritorialUnitDetailResponse,
    tags=["territorial-read"],
    summary="Get a municipality by canonical INE code",
)
async def get_municipality_by_ine_code(
    codigo_ine: str,
    territorial_repo: TerritorialRepository = Depends(get_territorial_repository),
) -> TerritorialUnitDetailResponse:
    unit = await territorial_repo.get_unit_detail_by_canonical_code(
        unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
        code_value=codigo_ine,
    )
    if unit is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "message": "Municipality code was not found.",
                "codigo_ine": codigo_ine,
            },
        )

    return TerritorialUnitDetailResponse(**unit)


@router.get(
    "/territorios/municipio/{codigo_ine}/resumen",
    response_model=TerritorialMunicipalitySummaryResponse,
    tags=["territorial-analytics"],
    summary="Get a semantic municipality summary",
    description=(
        "Analytical municipality summary that combines internal territorial detail with "
        "latest normalized INE indicators. The public contract is semantic and stable."
    ),
)
async def get_municipality_summary(
    codigo_ine: str,
    operation_code: str | None = Query(default=None, min_length=1),
    variable_id: str | None = Query(default=None, min_length=1),
    period_from: str | None = Query(default=None, min_length=1),
    period_to: str | None = Query(default=None, min_length=1),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    analytics_service: TerritorialAnalyticsService = Depends(get_territorial_analytics_service),
) -> TerritorialMunicipalitySummaryResponse:
    if period_from and period_to and period_from > period_to:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "message": "period_from cannot be greater than period_to.",
            },
        )

    summary = await analytics_service.build_municipality_summary(
        municipality_code=codigo_ine,
        operation_code=operation_code,
        variable_id=variable_id,
        period_from=period_from,
        period_to=period_to,
        page=page,
        page_size=page_size,
    )
    if summary is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "message": "Municipality code was not found.",
                "codigo_ine": codigo_ine,
            },
        )

    return summary


@router.post(
    "/territorios/municipio/{codigo_ine}/informe",
    response_model=TerritorialReportJobAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["territorial-analytics"],
    summary="Queue a municipality analytical report job",
)
async def create_municipality_report_job(
    request: Request,
    codigo_ine: str,
    operation_code: str | None = Query(default=None, min_length=1),
    variable_id: str | None = Query(default=None, min_length=1),
    period_from: str | None = Query(default=None, min_length=1),
    period_to: str | None = Query(default=None, min_length=1),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    analytics_service: TerritorialAnalyticsService = Depends(get_territorial_analytics_service),
    job_store: BaseJobStore = Depends(get_job_store),
    arq_pool: ArqRedis | None = Depends(get_arq_pool),
    settings: Settings = Depends(get_settings),
) -> TerritorialReportJobAcceptedResponse:
    if period_from and period_to and period_from > period_to:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "message": "period_from cannot be greater than period_to.",
            },
        )

    job_params = {
        "municipality_code": codigo_ine,
        "operation_code": operation_code,
        "variable_id": variable_id,
        "period_from": period_from,
        "period_to": period_to,
        "page": page,
        "page_size": page_size,
    }
    job = await job_store.create_job(job_type=MUNICIPALITY_REPORT_JOB_TYPE, params=job_params)
    job_id = job["job_id"]
    try:
        if arq_pool is not None:
            await arq_pool.enqueue_job(
                "run_municipality_report_job",
                job_id,
                job_params,
                _job_id=job_id,
                _queue_name=settings.job_queue_name,
            )
        else:
            task = asyncio.create_task(
                _run_municipality_report_job_inline(
                    job_store=job_store,
                    analytics_service=analytics_service,
                    job_id=job_id,
                    municipality_code=codigo_ine,
                    operation_code=operation_code,
                    variable_id=variable_id,
                    period_from=period_from,
                    period_to=period_to,
                    page=page,
                    page_size=page_size,
                )
            )
            request.app.state.inline_job_tasks = getattr(
                request.app.state, "inline_job_tasks", set()
            )
            request.app.state.inline_job_tasks.add(task)
            task.add_done_callback(
                lambda completed: request.app.state.inline_job_tasks.discard(completed)
            )
    except Exception as exc:
        await job_store.fail_job(
            job_id,
            {
                "message": "Could not enqueue the territorial report job.",
                "municipality_code": codigo_ine,
                "error": str(exc),
            },
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"message": "Background queue unavailable.", "job_id": job_id},
        )

    logger.info(
        "municipality_report_job_queued",
        extra={"municipality_code": codigo_ine, "job_id": job_id},
    )
    return TerritorialReportJobAcceptedResponse(
        job_id=job_id,
        job_type=MUNICIPALITY_REPORT_JOB_TYPE,
        status=job["status"],
        municipality_code=codigo_ine,
        status_path=f"/territorios/jobs/{job_id}",
        params=job_params,
    )


@router.get(
    "/municipios",
    response_model=TerritorialUnitListResponse,
    tags=["territorial-read"],
    summary="List municipalities from the internal territorial model",
)
async def list_municipalities(
    province_code: str | None = Query(default=None, min_length=1),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    active_only: bool = Query(default=True),
    territorial_repo: TerritorialRepository = Depends(get_territorial_repository),
) -> TerritorialUnitListResponse:
    parent_id = None
    if province_code is not None:
        parent_lookup = await territorial_repo.get_unit_by_canonical_code(
            unit_level=TERRITORIAL_UNIT_LEVEL_PROVINCE,
            code_value=province_code,
        )
        if parent_lookup is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "message": "Province code was not found.",
                    "province_code": province_code,
                },
            )
        parent_id = parent_lookup["id"]

    result = await territorial_repo.list_units(
        unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
        page=page,
        page_size=page_size,
        country_code="ES",
        parent_id=parent_id,
        active_only=active_only,
    )
    return TerritorialUnitListResponse(
        items=[TerritorialUnitSummaryResponse(**item) for item in result["items"]],
        total=result["total"],
        page=result["page"],
        page_size=result["page_size"],
        pages=result["pages"],
        has_next=result["has_next"],
        has_previous=result["has_previous"],
        filters=TerritorialUnitListFiltersResponse(**result["filters"]),
    )


@router.get(
    "/estadisticas/{indicador}/{codigo_territorial}",
    response_model=IndicadorSeriesResponse,
    tags=["territorial-analytics"],
    summary="Get time series for an indicator at a territorial unit",
    description=(
        "Returns normalized INE series for a given variable identifier and INE geography code. "
        "The response is paginated and may be filtered by operation code and period range."
    ),
)
async def get_indicator_series(
    indicador: str,
    codigo_territorial: str,
    operation_code: str | None = Query(default=None, min_length=1),
    period_from: str | None = Query(default=None, min_length=1),
    period_to: str | None = Query(default=None, min_length=1),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    series_repo: SeriesRepository = Depends(get_series_repository),
) -> IndicadorSeriesResponse:
    result = await series_repo.list_normalized(
        variable_id=indicador,
        geography_code=codigo_territorial,
        operation_code=operation_code,
        period_from=period_from,
        period_to=period_to,
        page=page,
        page_size=page_size,
    )

    geography_name = codigo_territorial
    if result["items"]:
        geography_name = result["items"][0].get("geography_name") or codigo_territorial

    operation_codes = list(
        {item["operation_code"] for item in result["items"] if item.get("operation_code")}
    )

    return IndicadorSeriesResponse(
        source="ine",
        territory=IndicatorTerritoryResponse(
            code=codigo_territorial,
            name=geography_name,
        ),
        indicator=indicador,
        series=[
            IndicatorSeriesPointResponse(
                period=item["period"],
                value=item["value"],
                unit=item.get("unit"),
            )
            for item in result["items"]
        ],
        total=result["total"],
        page=result["page"],
        page_size=result["page_size"],
        pages=result["pages"],
        has_next=result["has_next"],
        has_previous=result["has_previous"],
        filters=IndicadorSeriesFiltersResponse(
            indicador=indicador,
            codigo_territorial=codigo_territorial,
            operation_code=operation_code,
            period_from=period_from,
            period_to=period_to,
            page=page,
            page_size=page_size,
        ),
        metadata={"operation_codes": sorted(operation_codes)},
    )


async def _run_municipality_report_job_inline(
    *,
    job_store: BaseJobStore,
    analytics_service: TerritorialAnalyticsService,
    job_id: str,
    municipality_code: str,
    operation_code: str | None,
    variable_id: str | None,
    period_from: str | None,
    period_to: str | None,
    page: int,
    page_size: int,
) -> None:
    started_at = perf_counter()

    async def report_progress(progress: dict[str, object]) -> None:
        await job_store.update_progress(job_id, **progress)

    try:
        await job_store.mark_running(job_id)
        await report_progress(
            {
                "stage": "starting_report",
                "municipality_code": municipality_code,
                "report_type": MUNICIPALITY_REPORT_TYPE,
            }
        )
        report = await analytics_service.build_municipality_report(
            municipality_code=municipality_code,
            operation_code=operation_code,
            variable_id=variable_id,
            period_from=period_from,
            period_to=period_to,
            page=page,
            page_size=page_size,
            progress_reporter=report_progress,
        )
        if report is None:
            await job_store.fail_job(
                job_id,
                {
                    "message": "Municipality code was not found.",
                    "codigo_ine": municipality_code,
                },
            )
            logger.warning(
                "municipality_report_inline_job_not_found",
                extra={
                    "job_id": job_id,
                    "municipality_code": municipality_code,
                    "duration_ms": round((perf_counter() - started_at) * 1000, 3),
                },
            )
            record_job_duration(
                MUNICIPALITY_REPORT_JOB_TYPE,
                "failed",
                perf_counter() - started_at,
            )
            return
        payload_result = report.model_dump(mode="json")
        await job_store.complete_job(job_id, payload_result)
        record_job_duration(
            MUNICIPALITY_REPORT_JOB_TYPE,
            "completed",
            perf_counter() - started_at,
        )
        logger.info(
            "municipality_report_inline_job_completed",
            extra={
                "job_id": job_id,
                "municipality_code": municipality_code,
                "series_count": len(report.series),
                "storage_mode": report.metadata.get("storage_mode"),
                "result_bytes": len(json.dumps(payload_result, default=str).encode("utf-8")),
                "duration_ms": round((perf_counter() - started_at) * 1000, 3),
            },
        )
    except Exception as exc:
        logger.exception(
            "municipality_report_inline_job_unexpected_error",
            extra={
                "job_id": job_id,
                "municipality_code": municipality_code,
                "duration_ms": round((perf_counter() - started_at) * 1000, 3),
            },
        )
        await job_store.fail_job(
            job_id,
            {
                "message": "Unexpected error while generating the municipality report.",
                "codigo_ine": municipality_code,
                "error": str(exc),
            },
        )
        record_job_duration(
            MUNICIPALITY_REPORT_JOB_TYPE,
            "failed",
            perf_counter() - started_at,
        )


async def _run_territorial_export_job_inline(
    *,
    job_store: BaseJobStore,
    export_service: TerritorialExportService,
    job_id: str,
    export_request: TerritorialExportRequest,
) -> None:
    started_at = perf_counter()

    async def report_progress(progress: dict[str, object]) -> None:
        await job_store.update_progress(job_id, **progress)

    try:
        await job_store.mark_running(job_id)
        await report_progress(
            {
                "stage": "starting_export",
                "unit_level": export_request.unit_level,
                "code_value": export_request.code_value,
                "format": export_request.format,
            }
        )
        result = await export_service.build_export(
            job_id=job_id,
            unit_level=export_request.unit_level,
            code_value=export_request.code_value,
            artifact_format=export_request.format,
            include_providers=export_request.include_providers,
            progress_reporter=report_progress,
        )
        if result is None:
            await job_store.fail_job(
                job_id,
                {
                    "message": "Territorial unit code was not found.",
                    "unit_level": export_request.unit_level,
                    "code_value": export_request.code_value,
                },
            )
            logger.warning(
                "territorial_export_inline_job_not_found",
                extra={
                    "job_id": job_id,
                    "unit_level": export_request.unit_level,
                    "code_value": export_request.code_value,
                },
            )
            record_job_duration(
                TERRITORIAL_EXPORT_JOB_TYPE,
                "failed",
                perf_counter() - started_at,
            )
            return

        payload_result = result.model_dump(mode="json")
        await job_store.complete_job(job_id, payload_result)
        record_job_duration(
            TERRITORIAL_EXPORT_JOB_TYPE,
            "completed",
            perf_counter() - started_at,
        )
        logger.info(
            "territorial_export_inline_job_completed",
            extra={
                "job_id": job_id,
                "unit_level": export_request.unit_level,
                "code_value": export_request.code_value,
                "export_id": result.export_id,
                "artifact_reused": result.summary.get("artifact_reused"),
                "byte_size": result.summary.get("byte_size"),
            },
        )
    except CatastroClientError as exc:
        logger.warning(
            "territorial_export_inline_job_failed",
            extra={
                "job_id": job_id,
                "unit_level": export_request.unit_level,
                "code_value": export_request.code_value,
                "error": exc.detail,
            },
        )
        await job_store.fail_job(job_id, exc.detail)
        record_job_duration(
            TERRITORIAL_EXPORT_JOB_TYPE,
            "failed",
            perf_counter() - started_at,
        )
    except Exception as exc:
        logger.exception(
            "territorial_export_inline_job_unexpected_error",
            extra={
                "job_id": job_id,
                "unit_level": export_request.unit_level,
                "code_value": export_request.code_value,
            },
        )
        await job_store.fail_job(
            job_id,
            {
                "message": "Unexpected error while generating the territorial export.",
                "unit_level": export_request.unit_level,
                "code_value": export_request.code_value,
                "error": str(exc),
            },
        )
        record_job_duration(
            TERRITORIAL_EXPORT_JOB_TYPE,
            "failed",
            perf_counter() - started_at,
        )
