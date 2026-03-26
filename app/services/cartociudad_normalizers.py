from __future__ import annotations

from typing import Any

from app.repositories.territorial import (
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
    TERRITORIAL_UNIT_LEVEL_COUNTRY,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
    TerritorialRepository,
)
from app.schemas import (
    GeocodeResponse,
    GeocodeResultResponse,
    GeocodingCoordinatesResponse,
    GeocodingTerritorialResolutionResponse,
    GeocodingTerritorialContextResponse,
    ReverseGeocodeResponse,
    ReverseGeocodeResultResponse,
)


class CartoCiudadNormalizationError(Exception):
    def __init__(self, detail: dict[str, Any]) -> None:
        super().__init__(detail.get("message", "CartoCiudad payload could not be normalized."))
        self.status_code = 502
        self.detail = detail


_CARTOCIUDAD_MOJIBAKE_MARKERS = (
    "ÃƒÆ’",
    "Ãƒâ€š",
    "ÃƒÂ¢",
    "Ã†â€™",
    "Ã¢â€šÂ¬",
    "Ã¢â€žÂ¢",
    "Ã¯Â¿Â½",
    "Ã±",
    "Ã³",
    "Ã¡",
    "Ã©",
    "Ã­",
    "Ãº",
)


def repair_geocoding_text(value: str) -> str:
    current = value
    for _ in range(3):
        if _mojibake_score(current) == 0:
            break

        candidates = [current]
        for source_encoding in ("cp1252", "latin-1"):
            try:
                candidate = current.encode(source_encoding).decode("utf-8")
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue
            if candidate != current:
                candidates.append(candidate)

        best_candidate = min(candidates, key=_mojibake_score)
        if best_candidate == current:
            break
        if _mojibake_score(best_candidate) > _mojibake_score(current):
            break
        current = best_candidate

    return current


def normalize_cartociudad_geocode_response(
    *,
    query: str,
    payload: dict[str, Any] | list[Any],
    cached: bool,
    metadata: dict[str, Any] | None = None,
) -> GeocodeResponse:
    items = _extract_payload_items(payload)
    result_item = items[0] if items else None

    response_metadata = dict(metadata or {})
    response_metadata.setdefault("provider_result_count", len(items))

    if result_item is None:
        return GeocodeResponse(
            source="cartociudad",
            query=query,
            cached=cached,
            result=None,
            metadata=response_metadata,
        )

    lat = _coerce_float(_pick_first(result_item, "lat", "latitude", "y"))
    lon = _coerce_float(_pick_first(result_item, "lon", "lng", "longitude", "x"))
    if lat is None or lon is None:
        raise CartoCiudadNormalizationError(
            {
                "message": "The CartoCiudad result could not be normalized to semantic coordinates.",
            }
        )

    label = _pick_first(
        result_item,
        "label",
        "rotulo",
        "title",
        "direccion",
        "address",
        "nombre",
        "name",
    )
    if not label:
        label = query

    entity_type = (
        _pick_first(result_item, "entity_type", "type", "tipo", "clase", "category") or "unknown"
    )

    result = GeocodeResultResponse(
        label=label,
        entity_type=entity_type,
        coordinates=GeocodingCoordinatesResponse(lat=lat, lon=lon),
        address=_pick_first(result_item, "address", "direccion", "direccionPostal"),
        postal_code=_pick_first(
            result_item, "postal_code", "postalCode", "codPostal", "codigoPostal"
        ),
        territorial_context=GeocodingTerritorialContextResponse(
            country_code=_pick_first(result_item, "country_code", "countryCode", "codigoPais")
            or "ES",
            autonomous_community_code=_pick_first(
                result_item,
                "autonomous_community_code",
                "codigoComunidadAutonoma",
                "comunidadAutonomaCodigo",
            ),
            province_code=_pick_first(
                result_item, "province_code", "codigoProvincia", "provinciaCodigo"
            ),
            municipality_code=_pick_first(
                result_item,
                "municipality_code",
                "codigoMunicipio",
                "municipioCodigo",
                "poblacionCodigo",
            ),
            country_name=_pick_first(result_item, "country_name", "pais"),
            autonomous_community_name=_pick_first(
                result_item,
                "autonomous_community_name",
                "comunidadAutonoma",
            ),
            province_name=_pick_first(result_item, "province_name", "provincia"),
            municipality_name=_pick_first(
                result_item,
                "municipality_name",
                "municipio",
                "poblacion",
            ),
        ),
        territorial_resolution=None,
        metadata={
            "provider_id": _pick_first(result_item, "id", "place_id"),
            "provider_type": _pick_first(result_item, "type", "tipo"),
        },
    )

    return GeocodeResponse(
        source="cartociudad",
        query=query,
        cached=cached,
        result=result,
        metadata=response_metadata,
    )


def normalize_cartociudad_reverse_geocode_response(
    *,
    lat: float,
    lon: float,
    payload: dict[str, Any] | list[Any],
    cached: bool,
    metadata: dict[str, Any] | None = None,
) -> ReverseGeocodeResponse:
    items = _extract_payload_items(payload)
    result_item = items[0] if items else None

    response_metadata = dict(metadata or {})
    response_metadata.setdefault("provider_result_count", len(items))

    if result_item is None:
        return ReverseGeocodeResponse(
            source="cartociudad",
            query_coordinates=GeocodingCoordinatesResponse(lat=lat, lon=lon),
            cached=cached,
            result=None,
            metadata=response_metadata,
        )

    response_lat = _coerce_float(_pick_first(result_item, "lat", "latitude", "y"))
    response_lon = _coerce_float(_pick_first(result_item, "lon", "lng", "longitude", "x"))
    if response_lat is None or response_lon is None:
        response_lat = lat
        response_lon = lon

    label = _pick_first(
        result_item,
        "label",
        "rotulo",
        "title",
        "direccion",
        "address",
        "nombre",
        "name",
    )
    if not label:
        label = f"{lat},{lon}"

    entity_type = (
        _pick_first(result_item, "entity_type", "type", "tipo", "clase", "category") or "unknown"
    )

    result = ReverseGeocodeResultResponse(
        label=label,
        entity_type=entity_type,
        coordinates=GeocodingCoordinatesResponse(lat=response_lat, lon=response_lon),
        address=_pick_first(result_item, "address", "direccion", "direccionPostal"),
        postal_code=_pick_first(
            result_item, "postal_code", "postalCode", "codPostal", "codigoPostal"
        ),
        territorial_context=GeocodingTerritorialContextResponse(
            country_code=_pick_first(result_item, "country_code", "countryCode", "codigoPais")
            or "ES",
            autonomous_community_code=_pick_first(
                result_item,
                "autonomous_community_code",
                "codigoComunidadAutonoma",
                "comunidadAutonomaCodigo",
            ),
            province_code=_pick_first(
                result_item, "province_code", "codigoProvincia", "provinciaCodigo"
            ),
            municipality_code=_pick_first(
                result_item,
                "municipality_code",
                "codigoMunicipio",
                "municipioCodigo",
                "poblacionCodigo",
            ),
            country_name=_pick_first(result_item, "country_name", "pais"),
            autonomous_community_name=_pick_first(
                result_item,
                "autonomous_community_name",
                "comunidadAutonoma",
            ),
            province_name=_pick_first(result_item, "province_name", "provincia"),
            municipality_name=_pick_first(
                result_item,
                "municipality_name",
                "municipio",
                "poblacion",
            ),
        ),
        territorial_resolution=None,
        metadata={
            "provider_id": _pick_first(result_item, "id", "place_id"),
            "provider_type": _pick_first(result_item, "type", "tipo"),
        },
    )

    return ReverseGeocodeResponse(
        source="cartociudad",
        query_coordinates=GeocodingCoordinatesResponse(lat=lat, lon=lon),
        cached=cached,
        result=result,
        metadata=response_metadata,
    )


async def attach_territorial_resolution(
    response: GeocodeResponse | ReverseGeocodeResponse,
    territorial_repo: TerritorialRepository,
) -> GeocodeResponse | ReverseGeocodeResponse:
    if response.result is None:
        return response

    lookup = await _resolve_best_territorial_match(
        territorial_repo=territorial_repo,
        context=response.result.territorial_context.model_dump(),
    )
    if lookup is None:
        return response

    resolution = GeocodingTerritorialResolutionResponse(
        territorial_unit_id=lookup["id"],
        matched_by=lookup["matched_by"],
        canonical_name=repair_geocoding_text(str(lookup["canonical_name"])),
        canonical_code=(lookup.get("canonical_code") or {}).get("code_value"),
        source_system=(lookup.get("canonical_code") or {}).get("source_system"),
        unit_level=lookup["unit_level"],
    )
    result = response.result.model_copy(update={"territorial_resolution": resolution})
    return response.model_copy(update={"result": result})


def _extract_payload_items(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        items = [item for item in payload if isinstance(item, dict)]
        if len(items) != len(payload):
            raise CartoCiudadNormalizationError(
                {"message": "CartoCiudad returned a list with unexpected item types."}
            )
        return items

    if isinstance(payload, dict):
        return [payload]

    raise CartoCiudadNormalizationError(
        {"message": "CartoCiudad returned an unexpected payload type."}
    )


def _mojibake_score(value: str) -> int:
    return sum(value.count(marker) for marker in _CARTOCIUDAD_MOJIBAKE_MARKERS)


def _pick_first(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized:
            return repair_geocoding_text(normalized)
    return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


async def _resolve_best_territorial_match(
    *,
    territorial_repo: TerritorialRepository,
    context: dict[str, Any],
) -> dict[str, Any] | None:
    attempts: list[tuple[str, str | None, str]] = [
        (TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, context.get("municipality_code"), "code"),
        (TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, context.get("municipality_name"), "name"),
        (TERRITORIAL_UNIT_LEVEL_PROVINCE, context.get("province_code"), "code"),
        (TERRITORIAL_UNIT_LEVEL_PROVINCE, context.get("province_name"), "name"),
        (
            TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
            context.get("autonomous_community_code"),
            "code",
        ),
        (
            TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
            context.get("autonomous_community_name"),
            "name",
        ),
        (TERRITORIAL_UNIT_LEVEL_COUNTRY, context.get("country_code"), "code"),
        (TERRITORIAL_UNIT_LEVEL_COUNTRY, context.get("country_name"), "name"),
    ]

    for unit_level, candidate, match_kind in attempts:
        normalized_candidate = str(candidate).strip() if candidate is not None else ""
        if not normalized_candidate:
            continue

        if match_kind == "code":
            lookup = await territorial_repo.get_unit_by_canonical_code(
                unit_level=unit_level,
                code_value=normalized_candidate,
            )
        else:
            lookup = await territorial_repo.get_unit_by_name(
                name=normalized_candidate,
                unit_level=unit_level,
            )

        if lookup is not None:
            return lookup

    return None
