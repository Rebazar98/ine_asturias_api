from __future__ import annotations

from typing import Any

from app.dependencies import (
    get_cartociudad_client_service,
    get_geocoding_cache_repository,
)
from app.main import app
from app.repositories.geocoding import (
    GEOCODING_PROVIDER_CARTOCIUDAD,
    build_reverse_geocode_coordinate_key,
    normalize_geocode_query,
)
from app.repositories.territorial import (
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
)
from app.services.cartociudad_client import CartoCiudadUpstreamError


class DummyGeocodingCacheRepository:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], dict[str, Any]] = {}
        self.reverse_rows: dict[tuple[str, str], dict[str, Any]] = {}

    async def get_geocode_cache(self, provider: str, query: str, *, now=None):
        return self.rows.get((provider, normalize_geocode_query(query)))

    async def upsert_geocode_cache(
        self,
        provider: str,
        query: str,
        payload: dict[str, Any] | list[Any],
        *,
        ttl_seconds: int,
        metadata: dict[str, Any] | None = None,
        now=None,
    ):
        row = {
            "id": len(self.rows) + 1,
            "provider": provider,
            "query_text": query.strip(),
            "normalized_query": normalize_geocode_query(query),
            "payload": payload,
            "metadata": dict(metadata or {}),
            "cached_at": None,
            "expires_at": None,
        }
        self.rows[(provider, row["normalized_query"])] = row
        return row

    async def get_reverse_geocode_cache(
        self,
        provider: str,
        lat: float,
        lon: float,
        *,
        precision_digits=6,
        now=None,
    ):
        key = build_reverse_geocode_coordinate_key(lat, lon, precision_digits=precision_digits)
        return self.reverse_rows.get((provider, key))

    async def upsert_reverse_geocode_cache(
        self,
        provider: str,
        lat: float,
        lon: float,
        payload: dict[str, Any] | list[Any],
        *,
        ttl_seconds: int,
        metadata: dict[str, Any] | None = None,
        precision_digits=6,
        now=None,
    ):
        key = build_reverse_geocode_coordinate_key(lat, lon, precision_digits=precision_digits)
        row = {
            "id": len(self.reverse_rows) + 1,
            "provider": provider,
            "latitude": round(float(lat), precision_digits),
            "longitude": round(float(lon), precision_digits),
            "coordinate_key": key,
            "precision_digits": precision_digits,
            "payload": payload,
            "metadata": dict(metadata or {}),
            "cached_at": None,
            "expires_at": None,
        }
        self.reverse_rows[(provider, key)] = row
        return row


class DummyCartoCiudadClientService:
    def __init__(
        self, payload: dict[str, Any] | list[Any] | None = None, error: Exception | None = None
    ) -> None:
        self.payload = payload if payload is not None else []
        self.error = error
        self.calls: list[str] = []
        self.reverse_calls: list[tuple[float, float]] = []

    async def geocode(self, query: str) -> dict[str, Any] | list[Any]:
        self.calls.append(query)
        if self.error is not None:
            raise self.error
        return self.payload

    async def reverse_geocode(self, lat: float, lon: float) -> dict[str, Any] | list[Any]:
        self.reverse_calls.append((lat, lon))
        if self.error is not None:
            raise self.error
        return self.payload


def _oviedo_point_resolution_payload() -> dict[str, Any]:
    return {
        "matched_by": "geometry_cover",
        "best_match": {
            "id": 44,
            "parent_id": 33,
            "unit_level": "municipality",
            "canonical_name": "Oviedo",
            "display_name": "Oviedo",
            "country_code": "ES",
            "is_active": True,
            "canonical_code_strategy": {
                "source_system": "ine",
                "code_type": "municipality",
            },
            "canonical_code": {
                "source_system": "ine",
                "code_type": "municipality",
                "code_value": "33044",
                "is_primary": True,
            },
        },
        "hierarchy": [
            {
                "id": 1,
                "parent_id": None,
                "unit_level": "country",
                "canonical_name": "Espana",
                "display_name": "Espana",
                "country_code": "ES",
                "is_active": True,
                "canonical_code_strategy": {
                    "source_system": "iso3166",
                    "code_type": "alpha2",
                },
                "canonical_code": {
                    "source_system": "iso3166",
                    "code_type": "alpha2",
                    "code_value": "ES",
                    "is_primary": True,
                },
            },
            {
                "id": 3,
                "parent_id": 1,
                "unit_level": "autonomous_community",
                "canonical_name": "Principado de Asturias",
                "display_name": "Principado de Asturias",
                "country_code": "ES",
                "is_active": True,
                "canonical_code_strategy": {
                    "source_system": "ine",
                    "code_type": "autonomous_community",
                },
                "canonical_code": {
                    "source_system": "ine",
                    "code_type": "autonomous_community",
                    "code_value": "03",
                    "is_primary": True,
                },
            },
            {
                "id": 33,
                "parent_id": 3,
                "unit_level": "province",
                "canonical_name": "Asturias",
                "display_name": "Asturias",
                "country_code": "ES",
                "is_active": True,
                "canonical_code_strategy": {
                    "source_system": "ine",
                    "code_type": "province",
                },
                "canonical_code": {
                    "source_system": "ine",
                    "code_type": "province",
                    "code_value": "33",
                    "is_primary": True,
                },
            },
            {
                "id": 44,
                "parent_id": 33,
                "unit_level": "municipality",
                "canonical_name": "Oviedo",
                "display_name": "Oviedo",
                "country_code": "ES",
                "is_active": True,
                "canonical_code_strategy": {
                    "source_system": "ine",
                    "code_type": "municipality",
                },
                "canonical_code": {
                    "source_system": "ine",
                    "code_type": "municipality",
                    "code_value": "33044",
                    "is_primary": True,
                },
            },
        ],
        "coverage": {
            "boundary_source": "ign_administrative_boundaries",
            "levels_considered": [
                "country",
                "autonomous_community",
                "province",
                "municipality",
            ],
            "levels_loaded": [
                "country",
                "autonomous_community",
                "province",
                "municipality",
            ],
            "levels_missing_geometry": [],
            "levels_matched": [
                "country",
                "autonomous_community",
                "province",
                "municipality",
            ],
            "coverage_status": "full",
        },
        "ambiguity_detected": False,
        "ambiguity_by_level": {},
    }


def _gijon_point_resolution_payload_with_mojibake() -> dict[str, Any]:
    payload = _oviedo_point_resolution_payload()
    payload["best_match"]["canonical_name"] = "GijÃ³n"
    payload["best_match"]["display_name"] = "GijÃ³n"
    payload["best_match"]["canonical_code"]["code_value"] = "33024"
    payload["hierarchy"][0]["canonical_name"] = "EspaÃ±a"
    payload["hierarchy"][0]["display_name"] = "EspaÃ±a"
    payload["hierarchy"][3]["canonical_name"] = "GijÃ³n"
    payload["hierarchy"][3]["display_name"] = "GijÃ³n"
    payload["hierarchy"][3]["canonical_code"]["code_value"] = "33024"
    return payload


def test_geocode_endpoint_returns_semantic_response_and_persists_cache(
    client, dummy_territorial_repo, dummy_ingestion_repo
):
    cache_repo = DummyGeocodingCacheRepository()
    cartociudad_client = DummyCartoCiudadClientService(
        payload=[
            {
                "id": "oviedo-1",
                "type": "municipality",
                "label": "Oviedo",
                "address": "Oviedo",
                "postalCode": "33001",
                "lat": 43.3614,
                "lon": -5.8494,
                "codigoMunicipio": "33044",
                "municipio": "Oviedo",
                "provincia": "Asturias",
                "comunidadAutonoma": "Principado de Asturias",
            }
        ]
    )
    dummy_territorial_repo.point_resolution_payload = _oviedo_point_resolution_payload()
    dummy_territorial_repo.by_canonical_code[(TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, "33044")] = {
        "id": 44,
        "unit_level": "municipality",
        "canonical_name": "Oviedo",
        "matched_by": "code",
        "canonical_code": {
            "source_system": "ine",
            "code_type": "municipality",
            "code_value": "33044",
            "is_primary": True,
        },
    }
    app.dependency_overrides[get_geocoding_cache_repository] = lambda: cache_repo
    app.dependency_overrides[get_cartociudad_client_service] = lambda: cartociudad_client

    response = client.get("/geocode?query=Oviedo")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "cartociudad"
    assert payload["query"] == "Oviedo"
    assert payload["cached"] is False
    assert payload["generated_at"]
    assert payload["territorial_context"] == {
        "country_code": "ES",
        "autonomous_community_code": "03",
        "province_code": "33",
        "municipality_code": "33044",
        "country_name": "Espana",
        "autonomous_community_name": "Principado de Asturias",
        "province_name": "Asturias",
        "municipality_name": "Oviedo",
    }
    assert payload["territorial_resolution"] == {
        "strategy": "spatial_cover",
        "boundary_source": "ign_administrative_boundaries",
        "coverage_status": "full",
        "levels_considered": [
            "country",
            "autonomous_community",
            "province",
            "municipality",
        ],
        "levels_loaded": [
            "country",
            "autonomous_community",
            "province",
            "municipality",
        ],
        "levels_missing_geometry": [],
        "levels_matched": [
            "country",
            "autonomous_community",
            "province",
            "municipality",
        ],
        "missing_levels": [],
        "partial_resolution": False,
    }
    assert payload["summary"] == {
        "resolved": True,
        "provider_hit": True,
        "territorial_match": True,
        "cached": False,
        "partial_resolution": False,
    }
    assert payload["result"]["label"] == "Oviedo"
    assert payload["result"]["entity_type"] == "municipality"
    assert payload["result"]["coordinates"] == {"lat": 43.3614, "lon": -5.8494}
    assert payload["result"]["territorial_context"]["municipality_name"] == "Oviedo"
    assert payload["result"]["territorial_resolution"] == {
        "territorial_unit_id": 44,
        "matched_by": "code",
        "canonical_name": "Oviedo",
        "canonical_code": "33044",
        "source_system": "ine",
        "unit_level": "municipality",
    }
    assert payload["metadata"]["cache_scope"] == "provider"
    assert payload["metadata"]["persistent_cache_written"] is True
    assert payload["metadata"]["provider_result_count"] == 1
    assert payload["metadata"]["provider_name"] == "cartociudad"
    assert payload["metadata"]["provider_response_cached"] is False
    assert payload["metadata"]["fallback_used"] is False
    assert payload["metadata"]["fallback_reason"] is None
    assert cartociudad_client.calls == ["Oviedo"]
    assert len(dummy_ingestion_repo.records) == 1
    assert dummy_ingestion_repo.records[0]["source_type"] == "cartociudad_geocode_find"
    assert "Oviedo" not in dummy_ingestion_repo.records[0]["source_key"]
    assert dummy_ingestion_repo.records[0]["request_path"] == "/find"
    assert dummy_ingestion_repo.records[0]["request_params"] == {
        "query_fingerprint": dummy_ingestion_repo.records[0]["request_params"]["query_fingerprint"],
        "query_length": 6,
        "query_terms": 1,
        "request_kind": "text_query",
        "provider_contract_exposed": False,
    }
    assert "query" not in dummy_ingestion_repo.records[0]["request_params"]
    assert (
        cache_repo.rows[(GEOCODING_PROVIDER_CARTOCIUDAD, normalize_geocode_query("Oviedo"))][
            "payload"
        ][0]["id"]
        == "oviedo-1"
    )


def test_geocode_endpoint_repairs_mojibake_in_provider_and_internal_context(
    client, dummy_territorial_repo
):
    cache_repo = DummyGeocodingCacheRepository()
    cartociudad_client = DummyCartoCiudadClientService(
        payload=[
            {
                "id": "gijon-1",
                "type": "municipality",
                "label": "GijÃ³n/XixÃ³n",
                "address": "GijÃ³n/XixÃ³n",
                "lat": 43.5351550760999,
                "lon": -5.663608052406369,
                "pais": "EspaÃ±a",
                "municipio": "GijÃ³n/XixÃ³n",
                "comunidadAutonoma": "Principado de Asturias",
            }
        ]
    )
    dummy_territorial_repo.point_resolution_payload = _gijon_point_resolution_payload_with_mojibake()
    dummy_territorial_repo.by_name[(TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, "Gijón/Xixón")] = {
        "id": 24,
        "unit_level": "municipality",
        "canonical_name": "GijÃ³n",
        "matched_by": "canonical_name",
        "canonical_code": {
            "source_system": "ine",
            "code_type": "municipality",
            "code_value": "33024",
            "is_primary": True,
        },
    }
    app.dependency_overrides[get_geocoding_cache_repository] = lambda: cache_repo
    app.dependency_overrides[get_cartociudad_client_service] = lambda: cartociudad_client

    response = client.get("/geocode?query=Gijon")

    assert response.status_code == 200
    payload = response.json()
    assert payload["territorial_context"]["country_name"] == "España"
    assert payload["territorial_context"]["municipality_name"] == "Gijón"
    assert payload["result"]["label"] == "Gijón/Xixón"
    assert payload["result"]["territorial_context"]["country_name"] == "España"
    assert payload["result"]["territorial_context"]["municipality_name"] == "Gijón/Xixón"
    assert payload["result"]["territorial_resolution"]["canonical_name"] == "Gijón"


def test_geocode_endpoint_uses_persistent_cache_before_provider(
    client, dummy_territorial_repo, dummy_ingestion_repo
):
    cache_repo = DummyGeocodingCacheRepository()
    dummy_territorial_repo.point_resolution_payload = _oviedo_point_resolution_payload()
    cache_repo.rows[(GEOCODING_PROVIDER_CARTOCIUDAD, normalize_geocode_query("Oviedo"))] = {
        "id": 1,
        "provider": GEOCODING_PROVIDER_CARTOCIUDAD,
        "query_text": "Oviedo",
        "normalized_query": normalize_geocode_query("Oviedo"),
        "payload": [
            {
                "id": "oviedo-cache",
                "type": "municipality",
                "label": "Oviedo",
                "lat": 43.3614,
                "lon": -5.8494,
                "codigoMunicipio": "33044",
                "municipio": "Oviedo",
                "provincia": "Asturias",
            }
        ],
        "metadata": {"endpoint_family": "find"},
        "cached_at": None,
        "expires_at": None,
    }
    dummy_territorial_repo.by_canonical_code[(TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, "33044")] = {
        "id": 44,
        "unit_level": "municipality",
        "canonical_name": "Oviedo",
        "matched_by": "code",
        "canonical_code": {
            "source_system": "ine",
            "code_type": "municipality",
            "code_value": "33044",
            "is_primary": True,
        },
    }
    cartociudad_client = DummyCartoCiudadClientService(payload=[])
    app.dependency_overrides[get_geocoding_cache_repository] = lambda: cache_repo
    app.dependency_overrides[get_cartociudad_client_service] = lambda: cartociudad_client

    response = client.get("/geocode?query=Oviedo")

    assert response.status_code == 200
    payload = response.json()
    assert payload["cached"] is True
    assert payload["metadata"]["cache_scope"] == "persistent"
    assert payload["metadata"]["persistent_cache_hit"] is True
    assert payload["metadata"]["provider_name"] == "cartociudad"
    assert payload["metadata"]["provider_response_cached"] is True
    assert payload["summary"] == {
        "resolved": True,
        "provider_hit": True,
        "territorial_match": True,
        "cached": True,
        "partial_resolution": False,
    }
    assert payload["territorial_context"]["municipality_code"] == "33044"
    assert payload["result"]["territorial_resolution"]["canonical_code"] == "33044"
    assert cartociudad_client.calls == []
    assert dummy_ingestion_repo.records == []


def test_geocode_endpoint_validates_query_parameter(client, dummy_territorial_repo):
    response = client.get("/geocode?query=")

    assert response.status_code == 422


def test_geocode_endpoint_maps_upstream_error(client, dummy_territorial_repo):
    cache_repo = DummyGeocodingCacheRepository()
    cartociudad_client = DummyCartoCiudadClientService(
        error=CartoCiudadUpstreamError(
            status_code=503,
            detail={"message": "The CartoCiudad service returned an error."},
        )
    )
    app.dependency_overrides[get_geocoding_cache_repository] = lambda: cache_repo
    app.dependency_overrides[get_cartociudad_client_service] = lambda: cartociudad_client

    response = client.get("/geocode?query=Oviedo")

    assert response.status_code == 503
    assert response.json()["detail"]["message"] == "The CartoCiudad service returned an error."


def test_geocode_endpoint_maps_normalization_error_without_echoing_query(
    client, dummy_territorial_repo, dummy_ingestion_repo
):
    cache_repo = DummyGeocodingCacheRepository()
    cartociudad_client = DummyCartoCiudadClientService(
        payload=[
            {
                "id": "missing-coords",
                "type": "municipality",
                "label": "Oviedo",
            }
        ]
    )
    app.dependency_overrides[get_geocoding_cache_repository] = lambda: cache_repo
    app.dependency_overrides[get_cartociudad_client_service] = lambda: cartociudad_client

    response = client.get("/geocode?query=Oviedo")

    assert response.status_code == 502
    assert response.json()["detail"]["message"] == (
        "The CartoCiudad result could not be normalized to semantic coordinates."
    )
    assert len(dummy_ingestion_repo.records) == 1


def test_reverse_geocode_endpoint_returns_semantic_response_and_persists_cache(
    client, dummy_territorial_repo, dummy_ingestion_repo
):
    cache_repo = DummyGeocodingCacheRepository()
    cartociudad_client = DummyCartoCiudadClientService(
        payload={
            "id": "reverse-1",
            "type": "address",
            "label": "Oviedo, Asturias",
            "address": "Oviedo, Asturias",
            "postalCode": "33001",
            "lat": 43.3614,
            "lon": -5.8494,
            "provincia": "Asturias",
            "comunidadAutonoma": "Principado de Asturias",
        }
    )
    dummy_territorial_repo.point_resolution_payload = _oviedo_point_resolution_payload()
    dummy_territorial_repo.by_name[(TERRITORIAL_UNIT_LEVEL_PROVINCE, "Asturias")] = {
        "id": 33,
        "unit_level": "province",
        "canonical_name": "Asturias",
        "matched_by": "canonical_name",
        "canonical_code": {
            "source_system": "ine",
            "code_type": "province",
            "code_value": "33",
            "is_primary": True,
        },
    }
    app.dependency_overrides[get_geocoding_cache_repository] = lambda: cache_repo
    app.dependency_overrides[get_cartociudad_client_service] = lambda: cartociudad_client

    response = client.get("/reverse_geocode?lat=43.3614&lon=-5.8494")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "cartociudad"
    assert payload["cached"] is False
    assert payload["generated_at"]
    assert payload["query_coordinates"] == {"lat": 43.3614, "lon": -5.8494}
    assert payload["territorial_context"] == {
        "country_code": "ES",
        "autonomous_community_code": "03",
        "province_code": "33",
        "municipality_code": "33044",
        "country_name": "Espana",
        "autonomous_community_name": "Principado de Asturias",
        "province_name": "Asturias",
        "municipality_name": "Oviedo",
    }
    assert payload["territorial_resolution"] == {
        "strategy": "spatial_cover",
        "boundary_source": "ign_administrative_boundaries",
        "coverage_status": "full",
        "levels_considered": [
            "country",
            "autonomous_community",
            "province",
            "municipality",
        ],
        "levels_loaded": [
            "country",
            "autonomous_community",
            "province",
            "municipality",
        ],
        "levels_missing_geometry": [],
        "levels_matched": [
            "country",
            "autonomous_community",
            "province",
            "municipality",
        ],
        "missing_levels": [],
        "partial_resolution": False,
    }
    assert payload["summary"] == {
        "resolved": True,
        "provider_hit": True,
        "territorial_match": True,
        "cached": False,
        "partial_resolution": False,
    }
    assert payload["result"]["label"] == "Oviedo, Asturias"
    assert payload["result"]["entity_type"] == "address"
    assert payload["result"]["coordinates"] == {"lat": 43.3614, "lon": -5.8494}
    assert payload["result"]["territorial_context"]["province_name"] == "Asturias"
    assert payload["result"]["territorial_resolution"] == {
        "territorial_unit_id": 33,
        "matched_by": "canonical_name",
        "canonical_name": "Asturias",
        "canonical_code": "33",
        "source_system": "ine",
        "unit_level": "province",
    }
    assert payload["metadata"]["cache_scope"] == "provider"
    assert payload["metadata"]["persistent_cache_written"] is True
    assert payload["metadata"]["provider_result_count"] == 1
    assert payload["metadata"]["provider_name"] == "cartociudad"
    assert payload["metadata"]["provider_response_cached"] is False
    assert payload["metadata"]["fallback_used"] is False
    assert payload["metadata"]["fallback_reason"] is None
    assert cartociudad_client.reverse_calls == [(43.3614, -5.8494)]
    assert len(dummy_ingestion_repo.records) == 1
    assert dummy_ingestion_repo.records[0]["source_type"] == "cartociudad_reverse_geocode"
    assert dummy_ingestion_repo.records[0]["request_path"] == "/reverseGeocode"
    assert dummy_ingestion_repo.records[0]["request_params"] == {
        "coordinate_hint": "43.3614,-5.8494",
        "coordinate_precision": 4,
        "request_kind": "coordinates",
        "provider_contract_exposed": False,
    }
    cache_key = build_reverse_geocode_coordinate_key(43.3614, -5.8494)
    assert (
        cache_repo.reverse_rows[(GEOCODING_PROVIDER_CARTOCIUDAD, cache_key)]["payload"]["id"]
        == "reverse-1"
    )


def test_reverse_geocode_endpoint_repairs_mojibake_in_provider_and_internal_context(
    client, dummy_territorial_repo
):
    cache_repo = DummyGeocodingCacheRepository()
    cartociudad_client = DummyCartoCiudadClientService(
        payload={
            "id": "reverse-gijon",
            "type": "portal",
            "label": "CAMPO VALDES",
            "address": "CAMPO VALDES",
            "postalCode": "33201",
            "lat": 43.545559890101366,
            "lon": -5.661852647781838,
            "pais": "EspaÃ±a",
            "municipio": "GijÃ³n/XixÃ³n",
            "comunidadAutonoma": "Principado de Asturias",
        }
    )
    dummy_territorial_repo.point_resolution_payload = _gijon_point_resolution_payload_with_mojibake()
    dummy_territorial_repo.by_name[(TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, "Gijón/Xixón")] = {
        "id": 24,
        "unit_level": "municipality",
        "canonical_name": "GijÃ³n",
        "matched_by": "canonical_name",
        "canonical_code": {
            "source_system": "ine",
            "code_type": "municipality",
            "code_value": "33024",
            "is_primary": True,
        },
    }
    app.dependency_overrides[get_geocoding_cache_repository] = lambda: cache_repo
    app.dependency_overrides[get_cartociudad_client_service] = lambda: cartociudad_client

    response = client.get("/reverse_geocode?lat=43.5453&lon=-5.6619")

    assert response.status_code == 200
    payload = response.json()
    assert payload["territorial_context"]["country_name"] == "España"
    assert payload["territorial_context"]["municipality_name"] == "Gijón"
    assert payload["result"]["territorial_context"]["municipality_name"] == "Gijón/Xixón"
    assert payload["result"]["territorial_resolution"]["canonical_name"] == "Gijón"


def test_reverse_geocode_endpoint_uses_persistent_cache_before_provider(
    client, dummy_territorial_repo, dummy_ingestion_repo
):
    cache_repo = DummyGeocodingCacheRepository()
    dummy_territorial_repo.point_resolution_payload = _oviedo_point_resolution_payload()
    cache_key = build_reverse_geocode_coordinate_key(43.3614, -5.8494)
    cache_repo.reverse_rows[(GEOCODING_PROVIDER_CARTOCIUDAD, cache_key)] = {
        "id": 1,
        "provider": GEOCODING_PROVIDER_CARTOCIUDAD,
        "latitude": 43.3614,
        "longitude": -5.8494,
        "coordinate_key": cache_key,
        "precision_digits": 6,
        "payload": {
            "id": "reverse-cache",
            "type": "address",
            "label": "Oviedo, Asturias",
            "lat": 43.3614,
            "lon": -5.8494,
            "provincia": "Asturias",
        },
        "metadata": {"endpoint_family": "reverseGeocode"},
        "cached_at": None,
        "expires_at": None,
    }
    dummy_territorial_repo.by_name[(TERRITORIAL_UNIT_LEVEL_PROVINCE, "Asturias")] = {
        "id": 33,
        "unit_level": "province",
        "canonical_name": "Asturias",
        "matched_by": "canonical_name",
        "canonical_code": {
            "source_system": "ine",
            "code_type": "province",
            "code_value": "33",
            "is_primary": True,
        },
    }
    cartociudad_client = DummyCartoCiudadClientService(payload={})
    app.dependency_overrides[get_geocoding_cache_repository] = lambda: cache_repo
    app.dependency_overrides[get_cartociudad_client_service] = lambda: cartociudad_client

    response = client.get("/reverse_geocode?lat=43.3614&lon=-5.8494")

    assert response.status_code == 200
    payload = response.json()
    assert payload["cached"] is True
    assert payload["metadata"]["cache_scope"] == "persistent"
    assert payload["metadata"]["persistent_cache_hit"] is True
    assert payload["metadata"]["provider_name"] == "cartociudad"
    assert payload["metadata"]["provider_response_cached"] is True
    assert payload["summary"] == {
        "resolved": True,
        "provider_hit": True,
        "territorial_match": True,
        "cached": True,
        "partial_resolution": False,
    }
    assert payload["territorial_context"]["municipality_code"] == "33044"
    assert payload["result"]["territorial_resolution"]["canonical_code"] == "33"
    assert cartociudad_client.reverse_calls == []
    assert dummy_ingestion_repo.records == []


def test_reverse_geocode_endpoint_validates_coordinates(client, dummy_territorial_repo):
    response = client.get("/reverse_geocode?lat=120&lon=-5.84")

    assert response.status_code == 422


def test_reverse_geocode_endpoint_maps_upstream_error(client, dummy_territorial_repo):
    cache_repo = DummyGeocodingCacheRepository()
    cartociudad_client = DummyCartoCiudadClientService(
        error=CartoCiudadUpstreamError(
            status_code=503,
            detail={"message": "The CartoCiudad service returned an error."},
        )
    )
    app.dependency_overrides[get_geocoding_cache_repository] = lambda: cache_repo
    app.dependency_overrides[get_cartociudad_client_service] = lambda: cartociudad_client

    response = client.get("/reverse_geocode?lat=43.3614&lon=-5.8494")

    assert response.status_code == 503
    assert response.json()["detail"]["message"] == "The CartoCiudad service returned an error."


def test_reverse_geocode_endpoint_falls_back_to_internal_territorial_resolution(
    client, dummy_territorial_repo
):
    cache_repo = DummyGeocodingCacheRepository()
    cartociudad_client = DummyCartoCiudadClientService(
        error=CartoCiudadUpstreamError(
            status_code=503,
            detail={"message": "The CartoCiudad service returned an error."},
        )
    )
    dummy_territorial_repo.point_resolution_payload = _oviedo_point_resolution_payload()
    app.dependency_overrides[get_geocoding_cache_repository] = lambda: cache_repo
    app.dependency_overrides[get_cartociudad_client_service] = lambda: cartociudad_client

    response = client.get("/reverse_geocode?lat=43.3614&lon=-5.8494")

    assert response.status_code == 200
    payload = response.json()
    assert payload["result"] is None
    assert payload["summary"] == {
        "resolved": True,
        "provider_hit": False,
        "territorial_match": True,
        "cached": False,
        "partial_resolution": False,
    }
    assert payload["territorial_context"]["municipality_code"] == "33044"
    assert payload["territorial_resolution"]["coverage_status"] == "full"
    assert payload["metadata"]["provider_name"] == "cartociudad"
    assert payload["metadata"]["provider_response_cached"] is False
    assert payload["metadata"]["fallback_used"] is True
    assert payload["metadata"]["fallback_reason"] == "provider_unavailable"


def test_reverse_geocode_endpoint_maps_normalization_error(
    client, dummy_territorial_repo, dummy_ingestion_repo
):
    cache_repo = DummyGeocodingCacheRepository()
    cartociudad_client = DummyCartoCiudadClientService(payload="unexpected")
    app.dependency_overrides[get_geocoding_cache_repository] = lambda: cache_repo
    app.dependency_overrides[get_cartociudad_client_service] = lambda: cartociudad_client

    response = client.get("/reverse_geocode?lat=43.3614&lon=-5.8494")

    assert response.status_code == 502
    assert (
        response.json()["detail"]["message"] == "CartoCiudad returned an unexpected payload type."
    )
    assert len(dummy_ingestion_repo.records) == 1
