from __future__ import annotations

from hashlib import sha256
from typing import Any

from app.repositories.geocoding import (
    DEFAULT_REVERSE_GEOCODE_PRECISION,
    build_reverse_geocode_coordinate_key,
    normalize_geocode_query,
)


GEOCODING_LOG_COORDINATE_PRECISION = 3
GEOCODING_AUDIT_COORDINATE_PRECISION = 4


def build_geocode_query_fingerprint(query: str) -> str:
    normalized_query = normalize_geocode_query(query)
    if not normalized_query:
        return "empty"
    return sha256(normalized_query.encode("utf-8")).hexdigest()[:16]


def sanitize_geocode_query_context(query: str) -> dict[str, Any]:
    stripped_query = (query or "").strip()
    normalized_query = normalize_geocode_query(query)
    return {
        "query_fingerprint": build_geocode_query_fingerprint(query),
        "query_length": len(stripped_query),
        "query_terms": len(normalized_query.split()) if normalized_query else 0,
    }


def sanitize_reverse_geocode_context(
    lat: float,
    lon: float,
    *,
    precision_digits: int = GEOCODING_LOG_COORDINATE_PRECISION,
) -> dict[str, Any]:
    return {
        "coordinate_hint": build_reverse_geocode_coordinate_key(
            lat=lat,
            lon=lon,
            precision_digits=precision_digits,
        ),
        "coordinate_precision": precision_digits,
    }


def build_geocode_audit_request_params(query: str) -> dict[str, Any]:
    return {
        **sanitize_geocode_query_context(query),
        "request_kind": "text_query",
        "provider_contract_exposed": False,
    }


def build_reverse_geocode_audit_request_params(lat: float, lon: float) -> dict[str, Any]:
    return {
        **sanitize_reverse_geocode_context(
            lat=lat,
            lon=lon,
            precision_digits=GEOCODING_AUDIT_COORDINATE_PRECISION,
        ),
        "request_kind": "coordinates",
        "provider_contract_exposed": False,
    }


def build_geocode_source_key(query: str) -> str:
    return f"cartociudad.find:{build_geocode_query_fingerprint(query)}"


def build_reverse_geocode_source_key(lat: float, lon: float) -> str:
    coordinate_key = build_reverse_geocode_coordinate_key(
        lat=lat,
        lon=lon,
        precision_digits=DEFAULT_REVERSE_GEOCODE_PRECISION,
    )
    fingerprint = sha256(coordinate_key.encode("utf-8")).hexdigest()[:16]
    return f"cartociudad.reverse_geocode:{fingerprint}"
