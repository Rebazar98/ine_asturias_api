from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.logging import get_logger
from app.core.metrics import record_provider_cache_hit
from app.dependencies import (
    get_cartociudad_client_service,
    get_geocoding_cache_repository,
    get_territorial_repository,
    require_api_key,
)
from app.repositories.geocoding import (
    GEOCODING_PROVIDER_CARTOCIUDAD,
    GeocodingCacheRepository,
)
from app.repositories.territorial import (
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
    TerritorialRepository,
)
from app.schemas import (
    GeocodeResponse,
    ReverseGeocodeResponse,
    TerritorialUnitDetailResponse,
    TerritorialUnitListFiltersResponse,
    TerritorialUnitListResponse,
    TerritorialUnitSummaryResponse,
)
from app.services.cartociudad_client import CartoCiudadClientService
from app.services.cartociudad_normalizers import (
    attach_territorial_resolution,
    normalize_cartociudad_geocode_response,
    normalize_cartociudad_reverse_geocode_response,
)
from app.settings import Settings, get_settings


router = APIRouter(tags=["territorial"], dependencies=[Depends(require_api_key)])
logger = get_logger("app.api.routes_territorial")


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
    settings: Settings = Depends(get_settings),
    geocoding_repo: GeocodingCacheRepository = Depends(get_geocoding_cache_repository),
    cartociudad_client: CartoCiudadClientService = Depends(get_cartociudad_client_service),
    territorial_repo: TerritorialRepository = Depends(get_territorial_repository),
) -> GeocodeResponse:
    cached_row = await geocoding_repo.get_geocode_cache(
        provider=GEOCODING_PROVIDER_CARTOCIUDAD,
        query=query,
    )
    if cached_row is not None:
        record_provider_cache_hit("cartociudad", "geocode_persistent")
        logger.info(
            "geocode_persistent_cache_hit",
            extra={"provider": GEOCODING_PROVIDER_CARTOCIUDAD, "query": query},
        )
        normalized_response = normalize_cartociudad_geocode_response(
            query=query,
            payload=cached_row["payload"],
            cached=True,
            metadata={
                **cached_row.get("metadata", {}),
                "cache_scope": "persistent",
                "persistent_cache_hit": True,
            },
        )
        return await attach_territorial_resolution(normalized_response, territorial_repo)

    payload = await cartociudad_client.geocode(query)
    persisted = await geocoding_repo.upsert_geocode_cache(
        provider=GEOCODING_PROVIDER_CARTOCIUDAD,
        query=query,
        payload=payload,
        ttl_seconds=settings.cache_ttl_seconds,
        metadata={"endpoint_family": "find"},
    )
    logger.info(
        "geocode_provider_fetch_completed",
        extra={
            "provider": GEOCODING_PROVIDER_CARTOCIUDAD,
            "query": query,
            "persistent_cache_written": persisted is not None,
        },
    )
    normalized_response = normalize_cartociudad_geocode_response(
        query=query,
        payload=payload,
        cached=False,
        metadata={
            "cache_scope": "provider",
            "persistent_cache_written": persisted is not None,
            **(persisted.get("metadata", {}) if persisted else {}),
        },
    )
    return await attach_territorial_resolution(normalized_response, territorial_repo)


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
    settings: Settings = Depends(get_settings),
    geocoding_repo: GeocodingCacheRepository = Depends(get_geocoding_cache_repository),
    cartociudad_client: CartoCiudadClientService = Depends(get_cartociudad_client_service),
    territorial_repo: TerritorialRepository = Depends(get_territorial_repository),
) -> ReverseGeocodeResponse:
    cached_row = await geocoding_repo.get_reverse_geocode_cache(
        provider=GEOCODING_PROVIDER_CARTOCIUDAD,
        lat=lat,
        lon=lon,
    )
    if cached_row is not None:
        record_provider_cache_hit("cartociudad", "reverse_geocode_persistent")
        logger.info(
            "reverse_geocode_persistent_cache_hit",
            extra={
                "provider": GEOCODING_PROVIDER_CARTOCIUDAD,
                "lat": lat,
                "lon": lon,
            },
        )
        normalized_response = normalize_cartociudad_reverse_geocode_response(
            lat=lat,
            lon=lon,
            payload=cached_row["payload"],
            cached=True,
            metadata={
                **cached_row.get("metadata", {}),
                "cache_scope": "persistent",
                "persistent_cache_hit": True,
            },
        )
        return await attach_territorial_resolution(normalized_response, territorial_repo)

    payload = await cartociudad_client.reverse_geocode(lat, lon)
    persisted = await geocoding_repo.upsert_reverse_geocode_cache(
        provider=GEOCODING_PROVIDER_CARTOCIUDAD,
        lat=lat,
        lon=lon,
        payload=payload,
        ttl_seconds=settings.cache_ttl_seconds,
        metadata={"endpoint_family": "reverseGeocode"},
    )
    logger.info(
        "reverse_geocode_provider_fetch_completed",
        extra={
            "provider": GEOCODING_PROVIDER_CARTOCIUDAD,
            "lat": lat,
            "lon": lon,
            "persistent_cache_written": persisted is not None,
        },
    )
    normalized_response = normalize_cartociudad_reverse_geocode_response(
        lat=lat,
        lon=lon,
        payload=payload,
        cached=False,
        metadata={
            "cache_scope": "provider",
            "persistent_cache_written": persisted is not None,
            **(persisted.get("metadata", {}) if persisted else {}),
        },
    )
    return await attach_territorial_resolution(normalized_response, territorial_repo)


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
