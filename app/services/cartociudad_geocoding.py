from __future__ import annotations

from app.core.logging import get_logger
from app.core.metrics import record_provider_cache_hit
from app.repositories.geocoding import (
    GEOCODING_PROVIDER_CARTOCIUDAD,
    GeocodingCacheRepository,
)
from app.repositories.ingestion import IngestionRepository
from app.repositories.territorial import TerritorialRepository
from app.schemas import GeocodeResponse, ReverseGeocodeResponse
from app.services.cartociudad_client import CartoCiudadClientService
from app.services.cartociudad_normalizers import (
    attach_territorial_resolution,
    normalize_cartociudad_geocode_response,
    normalize_cartociudad_reverse_geocode_response,
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
            return await attach_territorial_resolution(
                normalized_response,
                self.territorial_repo,
            )

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
        return await attach_territorial_resolution(
            normalized_response,
            self.territorial_repo,
        )

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
            return await attach_territorial_resolution(
                normalized_response,
                self.territorial_repo,
            )

        payload = await self.cartociudad_client.reverse_geocode(lat, lon)
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
        return await attach_territorial_resolution(
            normalized_response,
            self.territorial_repo,
        )
