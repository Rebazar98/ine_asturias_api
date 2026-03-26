from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.core.logging import get_logger
from app.core.metrics import record_provider_cache_hit
from app.repositories.geocoding import (
    GEOCODING_PROVIDER_CARTOCIUDAD,
    GeocodingCacheRepository,
)
from app.repositories.ingestion import IngestionRepository
from app.repositories.territorial import (
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
    TERRITORIAL_UNIT_LEVEL_COUNTRY,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
    TerritorialRepository,
)
from app.schemas import (
    GeocodeResponse,
    GeocodingCoordinatesResponse,
    GeocodingTerritorialContextResponse,
    GeocodeSummaryResponse,
    ReverseGeocodeResponse,
    ReverseGeocodeSummaryResponse,
    TerritorialPointResolutionDetailsResponse,
)
from app.services.cartociudad_client import CartoCiudadClientError, CartoCiudadClientService
from app.services.cartociudad_normalizers import (
    CartoCiudadNormalizationError,
    attach_territorial_resolution,
    normalize_cartociudad_geocode_response,
    normalize_cartociudad_reverse_geocode_response,
    repair_geocoding_text,
)
from app.services.geocoding_privacy import (
    build_geocode_audit_request_params,
    build_geocode_source_key,
    build_reverse_geocode_audit_request_params,
    build_reverse_geocode_source_key,
    sanitize_geocode_query_context,
    sanitize_reverse_geocode_context,
)


CARTOCIUDAD_GEOCODE_SOURCE_TYPE = "cartociudad_geocode_find"
CARTOCIUDAD_REVERSE_GEOCODE_SOURCE_TYPE = "cartociudad_reverse_geocode"

_REVERSE_GEOCODE_LEVELS = [
    TERRITORIAL_UNIT_LEVEL_COUNTRY,
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
]


def _build_geocoding_context_from_hierarchy(
    hierarchy: list[dict[str, Any]],
) -> GeocodingTerritorialContextResponse:
    payload: dict[str, str | None] = {}
    for item in hierarchy:
        unit_level = item.get("unit_level")
        canonical_name = item.get("canonical_name")
        if canonical_name is not None:
            canonical_name = repair_geocoding_text(str(canonical_name))
        canonical_code = ((item.get("canonical_code") or {}) or {}).get("code_value")
        if unit_level == TERRITORIAL_UNIT_LEVEL_COUNTRY:
            payload["country_code"] = canonical_code
            payload["country_name"] = canonical_name
        elif unit_level == TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY:
            payload["autonomous_community_code"] = canonical_code
            payload["autonomous_community_name"] = canonical_name
        elif unit_level == TERRITORIAL_UNIT_LEVEL_PROVINCE:
            payload["province_code"] = canonical_code
            payload["province_name"] = canonical_name
        elif unit_level == TERRITORIAL_UNIT_LEVEL_MUNICIPALITY:
            payload["municipality_code"] = canonical_code
            payload["municipality_name"] = canonical_name
    return GeocodingTerritorialContextResponse(**payload)


def _build_geocoding_spatial_contract(
    resolution: dict[str, Any] | None,
) -> tuple[
    GeocodingTerritorialContextResponse,
    TerritorialPointResolutionDetailsResponse,
    bool,
    bool,
]:
    coverage = dict((resolution or {}).get("coverage", {}) or {})
    levels_considered = list(coverage.get("levels_considered") or [])
    levels_matched = list(coverage.get("levels_matched") or [])
    levels_loaded = list(coverage.get("levels_loaded") or [])
    boundary_source = coverage.get("boundary_source")
    coverage_status = str(coverage.get("coverage_status") or "").strip().lower()

    if not levels_considered:
        levels_considered = list(_REVERSE_GEOCODE_LEVELS)
    if not levels_loaded and boundary_source is not None:
        levels_loaded = levels_matched.copy()
    if coverage_status not in {"none", "partial", "full"}:
        coverage_status = (
            "none"
            if not boundary_source and not levels_loaded
            else "full"
            if levels_loaded and len(levels_loaded) == len(levels_considered)
            else "partial"
        )

    levels_missing_geometry = list(coverage.get("levels_missing_geometry") or [])
    if not levels_missing_geometry:
        levels_missing_geometry = [
            level for level in levels_considered if level not in set(levels_loaded)
        ]

    hierarchy = [
        item for item in list((resolution or {}).get("hierarchy") or []) if isinstance(item, dict)
    ]
    territorial_context = _build_geocoding_context_from_hierarchy(hierarchy)
    best_match = (resolution or {}).get("best_match")
    missing_levels = [level for level in levels_considered if level not in set(levels_matched)]
    territorial_match = isinstance(best_match, dict)
    partial_resolution = territorial_match and bool(missing_levels)

    return (
        territorial_context,
        TerritorialPointResolutionDetailsResponse(
            strategy="spatial_cover",
            boundary_source=boundary_source,
            coverage_status=coverage_status,
            levels_considered=levels_considered,
            levels_loaded=levels_loaded,
            levels_missing_geometry=levels_missing_geometry,
            levels_matched=levels_matched,
            missing_levels=missing_levels,
            partial_resolution=partial_resolution,
        ),
        territorial_match,
        partial_resolution,
    )


class CartoCiudadGeocodingService:
    def __init__(
        self,
        *,
        cartociudad_client: CartoCiudadClientService,
        geocoding_repo: GeocodingCacheRepository,
        ingestion_repo: IngestionRepository,
        territorial_repo: TerritorialRepository,
        cache_ttl_seconds: int,
    ) -> None:
        self.cartociudad_client = cartociudad_client
        self.geocoding_repo = geocoding_repo
        self.ingestion_repo = ingestion_repo
        self.territorial_repo = territorial_repo
        self.cache_ttl_seconds = cache_ttl_seconds
        self.logger = get_logger("app.services.cartociudad_geocoding")

    async def geocode(self, query: str) -> GeocodeResponse:
        query_context = sanitize_geocode_query_context(query)
        cached_row = await self.geocoding_repo.get_geocode_cache(
            provider=GEOCODING_PROVIDER_CARTOCIUDAD,
            query=query,
        )
        if cached_row is not None:
            record_provider_cache_hit("cartociudad", "geocode_persistent")
            self.logger.info(
                "geocode_persistent_cache_hit",
                extra={
                    "provider": GEOCODING_PROVIDER_CARTOCIUDAD,
                    **query_context,
                },
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
            normalized_response = await attach_territorial_resolution(
                normalized_response,
                self.territorial_repo,
            )
            return await self._compose_geocode_response(normalized_response)

        payload = await self.cartociudad_client.geocode(query)
        raw_record_id = await self.ingestion_repo.save_raw(
            source_type=CARTOCIUDAD_GEOCODE_SOURCE_TYPE,
            source_key=build_geocode_source_key(query),
            request_path="/find",
            request_params=build_geocode_audit_request_params(query),
            payload=payload,
        )
        persisted = await self.geocoding_repo.upsert_geocode_cache(
            provider=GEOCODING_PROVIDER_CARTOCIUDAD,
            query=query,
            payload=payload,
            ttl_seconds=self.cache_ttl_seconds,
            metadata={"endpoint_family": "find"},
        )
        self.logger.info(
            "geocode_provider_fetch_completed",
            extra={
                "provider": GEOCODING_PROVIDER_CARTOCIUDAD,
                **query_context,
                "raw_ingestion_saved": raw_record_id is not None,
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
        normalized_response = await attach_territorial_resolution(
            normalized_response,
            self.territorial_repo,
        )
        return await self._compose_geocode_response(normalized_response)

    async def reverse_geocode(self, lat: float, lon: float) -> ReverseGeocodeResponse:
        coordinate_context = sanitize_reverse_geocode_context(lat, lon)
        cached_row = await self.geocoding_repo.get_reverse_geocode_cache(
            provider=GEOCODING_PROVIDER_CARTOCIUDAD,
            lat=lat,
            lon=lon,
        )
        if cached_row is not None:
            record_provider_cache_hit("cartociudad", "reverse_geocode_persistent")
            self.logger.info(
                "reverse_geocode_persistent_cache_hit",
                extra={
                    "provider": GEOCODING_PROVIDER_CARTOCIUDAD,
                    **coordinate_context,
                },
            )
            try:
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
                normalized_response = await attach_territorial_resolution(
                    normalized_response,
                    self.territorial_repo,
                )
            except CartoCiudadNormalizationError as exc:
                fallback_response = await self._build_reverse_geocode_fallback_response(
                    lat=lat,
                    lon=lon,
                    fallback_reason="cached_provider_response_unusable",
                    extra_metadata={"normalization_error_message": str(exc)},
                )
                if fallback_response is not None:
                    return fallback_response
                raise
            return await self._compose_reverse_geocode_response(
                normalized_response,
                lat=lat,
                lon=lon,
            )

        try:
            payload = await self.cartociudad_client.reverse_geocode(lat, lon)
        except CartoCiudadClientError as exc:
            fallback_response = await self._build_reverse_geocode_fallback_response(
                lat=lat,
                lon=lon,
                fallback_reason="provider_unavailable",
                extra_metadata={
                    "provider_error_status_code": exc.status_code,
                    "provider_error_message": str(exc),
                },
            )
            if fallback_response is not None:
                return fallback_response
            raise
        raw_record_id = await self.ingestion_repo.save_raw(
            source_type=CARTOCIUDAD_REVERSE_GEOCODE_SOURCE_TYPE,
            source_key=build_reverse_geocode_source_key(lat, lon),
            request_path="/reverseGeocode",
            request_params=build_reverse_geocode_audit_request_params(lat, lon),
            payload=payload,
        )
        persisted = await self.geocoding_repo.upsert_reverse_geocode_cache(
            provider=GEOCODING_PROVIDER_CARTOCIUDAD,
            lat=lat,
            lon=lon,
            payload=payload,
            ttl_seconds=self.cache_ttl_seconds,
            metadata={"endpoint_family": "reverseGeocode"},
        )
        self.logger.info(
            "reverse_geocode_provider_fetch_completed",
            extra={
                "provider": GEOCODING_PROVIDER_CARTOCIUDAD,
                **coordinate_context,
                "raw_ingestion_saved": raw_record_id is not None,
                "persistent_cache_written": persisted is not None,
            },
        )
        try:
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
            normalized_response = await attach_territorial_resolution(
                normalized_response,
                self.territorial_repo,
            )
        except CartoCiudadNormalizationError as exc:
            fallback_response = await self._build_reverse_geocode_fallback_response(
                lat=lat,
                lon=lon,
                fallback_reason="provider_response_unusable",
                extra_metadata={"normalization_error_message": str(exc)},
            )
            if fallback_response is not None:
                return fallback_response
            raise
        return await self._compose_reverse_geocode_response(
            normalized_response,
            lat=lat,
            lon=lon,
        )

    async def _compose_reverse_geocode_response(
        self,
        response: ReverseGeocodeResponse,
        *,
        lat: float,
        lon: float,
        fallback_reason: str | None = None,
        extra_metadata: dict[str, Any] | None = None,
    ) -> ReverseGeocodeResponse:
        point_resolution = await self.territorial_repo.resolve_point(lat=lat, lon=lon)
        (
            territorial_context,
            territorial_resolution,
            territorial_match,
            partial_resolution,
        ) = _build_geocoding_spatial_contract(point_resolution)
        provider_hit = response.result is not None

        effective_fallback_reason = fallback_reason
        fallback_used = False
        if effective_fallback_reason is not None and territorial_match:
            fallback_used = True
        elif not provider_hit and territorial_match:
            effective_fallback_reason = "provider_no_result"
            fallback_used = True

        metadata = {
            **response.metadata,
            **(extra_metadata or {}),
            "provider_name": GEOCODING_PROVIDER_CARTOCIUDAD,
            "provider_response_cached": response.cached,
            "fallback_used": fallback_used,
            "fallback_reason": effective_fallback_reason,
        }

        return response.model_copy(
            update={
                "generated_at": datetime.now(UTC),
                "territorial_context": territorial_context,
                "territorial_resolution": territorial_resolution,
                "summary": ReverseGeocodeSummaryResponse(
                    resolved=provider_hit or territorial_match,
                    provider_hit=provider_hit,
                    territorial_match=territorial_match,
                    cached=response.cached,
                    partial_resolution=partial_resolution,
                ),
                "metadata": metadata,
            }
        )

    async def _build_reverse_geocode_fallback_response(
        self,
        *,
        lat: float,
        lon: float,
        fallback_reason: str,
        extra_metadata: dict[str, Any] | None = None,
    ) -> ReverseGeocodeResponse | None:
        fallback_response = await self._compose_reverse_geocode_response(
            ReverseGeocodeResponse(
                source="cartociudad",
                query_coordinates=GeocodingCoordinatesResponse(lat=lat, lon=lon),
                cached=False,
                result=None,
                metadata=extra_metadata or {},
            ),
            lat=lat,
            lon=lon,
            fallback_reason=fallback_reason,
        )
        if not fallback_response.summary.territorial_match:
            return None

        coordinate_context = sanitize_reverse_geocode_context(lat, lon)
        self.logger.warning(
            "reverse_geocode_spatial_fallback_used",
            extra={
                "provider": GEOCODING_PROVIDER_CARTOCIUDAD,
                "fallback_reason": fallback_reason,
                **coordinate_context,
            },
        )
        return fallback_response

    async def _compose_geocode_response(self, response: GeocodeResponse) -> GeocodeResponse:
        territorial_context = GeocodingTerritorialContextResponse()
        territorial_resolution = None
        territorial_match = False
        partial_resolution = False
        if response.result is not None:
            point_resolution = await self.territorial_repo.resolve_point(
                lat=response.result.coordinates.lat,
                lon=response.result.coordinates.lon,
            )
            (
                territorial_context,
                territorial_resolution,
                territorial_match,
                partial_resolution,
            ) = _build_geocoding_spatial_contract(point_resolution)

        metadata = {
            **response.metadata,
            "provider_name": GEOCODING_PROVIDER_CARTOCIUDAD,
            "provider_response_cached": response.cached,
            "fallback_used": False,
            "fallback_reason": None,
        }
        provider_hit = response.result is not None

        return response.model_copy(
            update={
                "generated_at": datetime.now(UTC),
                "territorial_context": territorial_context,
                "territorial_resolution": territorial_resolution,
                "summary": GeocodeSummaryResponse(
                    resolved=provider_hit or territorial_match,
                    provider_hit=provider_hit,
                    territorial_match=territorial_match,
                    cached=response.cached,
                    partial_resolution=partial_resolution,
                ),
                "metadata": metadata,
            }
        )
