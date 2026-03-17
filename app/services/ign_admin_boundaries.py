from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from app.core.logging import get_logger
from app.core.metrics import (
    record_territorial_boundary_feature,
    record_territorial_boundary_load,
)
from app.repositories.cartographic_qa import CartographicQARepository
from app.repositories.ingestion import IngestionRepository
from app.repositories.territorial import (
    TERRITORIAL_ALIAS_TYPE_PROVIDER_NAME,
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
    TERRITORIAL_UNIT_LEVEL_COUNTRY,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
    TerritorialRepository,
)


IGN_ADMIN_BOUNDARY_SOURCE = "ign_administrative_boundaries"
IGN_ADMIN_PROVIDER_SOURCE_SYSTEM = "ign_admin"
IGN_ADMIN_SCOPE_ASTURIAS_CODE = "03"
IGN_ADMIN_CATALOG_RESOURCE_KEY = "territorial.ign_administrative_boundaries.catalog"

IGN_ADMIN_SNAPSHOT_SOURCE_TYPE = "ign_admin_boundaries_snapshot"
IGN_ADMIN_LEVEL_SOURCE_TYPES = {
    TERRITORIAL_UNIT_LEVEL_COUNTRY: "ign_admin_boundaries_country",
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY: "ign_admin_boundaries_autonomous_community",
    TERRITORIAL_UNIT_LEVEL_PROVINCE: "ign_admin_boundaries_province",
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY: "ign_admin_boundaries_municipality",
}
IGN_ADMIN_LEVEL_ORDER = {
    TERRITORIAL_UNIT_LEVEL_COUNTRY: 0,
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY: 1,
    TERRITORIAL_UNIT_LEVEL_PROVINCE: 2,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY: 3,
}
IGN_ADMIN_ALLOWED_UNIT_LEVELS = tuple(IGN_ADMIN_LEVEL_ORDER.keys())


@dataclass(frozen=True, slots=True)
class IGNAdministrativeBoundaryFeature:
    unit_level: str
    canonical_code: str
    canonical_name: str
    display_name: str
    country_code: str
    parent_unit_level: str | None
    parent_canonical_code: str | None
    autonomous_community_code: str | None
    province_code: str | None
    geometry_geojson: dict[str, Any]
    centroid_geojson: dict[str, Any] | None
    raw_feature: dict[str, Any]
    metadata: dict[str, Any]
    provider_alias: str | None = None


class IGNAdministrativeBoundariesLoaderService:
    def __init__(
        self,
        *,
        ingestion_repo: IngestionRepository,
        territorial_repo: TerritorialRepository,
        qa_repo: CartographicQARepository | None = None,
        cartographic_qa_enabled: bool = True,
    ) -> None:
        self.ingestion_repo = ingestion_repo
        self.territorial_repo = territorial_repo
        self.qa_repo = qa_repo
        self.cartographic_qa_enabled = cartographic_qa_enabled
        self.logger = get_logger("app.services.ign_admin_boundaries")

    async def load_snapshot(
        self,
        *,
        payload: dict[str, Any],
        source_path: str,
        dataset_version: str | None = None,
        country_code: str = "ES",
        autonomous_community_code: str = IGN_ADMIN_SCOPE_ASTURIAS_CODE,
    ) -> dict[str, Any]:
        started_at = perf_counter()
        normalized = normalize_ign_admin_snapshot(
            payload,
            country_code=country_code,
            autonomous_community_code=autonomous_community_code,
        )
        effective_dataset_version = (
            (dataset_version or "").strip()
            or str(((payload.get("metadata") or {}).get("dataset_version") or "")).strip()
            or "ign_admin_snapshot"
        )

        raw_records_saved = await self._persist_raw_snapshot_groups(
            payload=payload,
            features=normalized["features"],
            source_path=source_path,
            dataset_version=effective_dataset_version,
            country_code=country_code,
            autonomous_community_code=autonomous_community_code,
        )

        incidents = list(normalized["incidents"])
        levels: dict[str, dict[str, int]] = {
            level: {
                "selected": 0,
                "matched": 0,
                "upserted": 0,
                "created": 0,
                "updated": 0,
                "rejected": 0,
            }
            for level in IGN_ADMIN_ALLOWED_UNIT_LEVELS
        }
        for feature in normalized["features"]:
            levels[feature.unit_level]["selected"] += 1
            record_territorial_boundary_feature(
                IGN_ADMIN_BOUNDARY_SOURCE,
                "selected",
                feature.unit_level,
            )
        for incident in incidents:
            if incident.get("unit_level") in levels:
                levels[str(incident["unit_level"])]["rejected"] += 1

        persisted_units: dict[tuple[str, str], int] = {}
        session = getattr(self.territorial_repo, "session", None)

        try:
            for feature in normalized["features"]:
                parent_id = None
                if (
                    feature.parent_unit_level is not None
                    and feature.parent_canonical_code is not None
                ):
                    parent_id = persisted_units.get(
                        (feature.parent_unit_level, feature.parent_canonical_code)
                    )
                    if parent_id is None:
                        parent_lookup = await self.territorial_repo.get_unit_by_canonical_code(
                            unit_level=feature.parent_unit_level,
                            code_value=feature.parent_canonical_code,
                        )
                        if parent_lookup is not None:
                            parent_id = parent_lookup["id"]

                if feature.parent_unit_level is not None and parent_id is None:
                    incidents.append(
                        {
                            "reason": "parent_not_resolved",
                            "unit_level": feature.unit_level,
                            "canonical_code": feature.canonical_code,
                            "parent_unit_level": feature.parent_unit_level,
                            "parent_canonical_code": feature.parent_canonical_code,
                        }
                    )
                    levels[feature.unit_level]["rejected"] += 1
                    record_territorial_boundary_feature(
                        IGN_ADMIN_BOUNDARY_SOURCE,
                        "rejected",
                        feature.unit_level,
                    )
                    continue

                try:
                    if session is not None:
                        async with session.begin_nested():
                            result = await self.territorial_repo.upsert_boundary_unit(
                                unit_level=feature.unit_level,
                                canonical_code=feature.canonical_code,
                                canonical_name=feature.canonical_name,
                                display_name=feature.display_name,
                                country_code=feature.country_code,
                                parent_id=parent_id,
                                geometry_geojson=feature.geometry_geojson,
                                centroid_geojson=feature.centroid_geojson,
                                provider_source=IGN_ADMIN_PROVIDER_SOURCE_SYSTEM,
                                provider_alias=feature.provider_alias,
                                provider_alias_type=TERRITORIAL_ALIAS_TYPE_PROVIDER_NAME,
                                boundary_metadata={
                                    "source": IGN_ADMIN_BOUNDARY_SOURCE,
                                    "dataset_version": effective_dataset_version,
                                    "source_path": source_path,
                                    "scope_country_code": country_code,
                                    "scope_autonomous_community_code": autonomous_community_code,
                                    **feature.metadata,
                                },
                            )
                    else:
                        result = await self.territorial_repo.upsert_boundary_unit(
                            unit_level=feature.unit_level,
                            canonical_code=feature.canonical_code,
                            canonical_name=feature.canonical_name,
                            display_name=feature.display_name,
                            country_code=feature.country_code,
                            parent_id=parent_id,
                            geometry_geojson=feature.geometry_geojson,
                            centroid_geojson=feature.centroid_geojson,
                            provider_source=IGN_ADMIN_PROVIDER_SOURCE_SYSTEM,
                            provider_alias=feature.provider_alias,
                            provider_alias_type=TERRITORIAL_ALIAS_TYPE_PROVIDER_NAME,
                            boundary_metadata={
                                "source": IGN_ADMIN_BOUNDARY_SOURCE,
                                "dataset_version": effective_dataset_version,
                                "source_path": source_path,
                                "scope_country_code": country_code,
                                "scope_autonomous_community_code": autonomous_community_code,
                                **feature.metadata,
                            },
                        )
                except Exception as exc:
                    incidents.append(
                        {
                            "reason": "upsert_failed",
                            "unit_level": feature.unit_level,
                            "canonical_code": feature.canonical_code,
                            "error": str(exc),
                        }
                    )
                    levels[feature.unit_level]["rejected"] += 1
                    record_territorial_boundary_feature(
                        IGN_ADMIN_BOUNDARY_SOURCE,
                        "rejected",
                        feature.unit_level,
                    )
                    self.logger.warning(
                        "ign_admin_feature_upsert_failed",
                        extra={
                            "unit_level": feature.unit_level,
                            "canonical_code": feature.canonical_code,
                            "error": str(exc),
                        },
                    )
                    continue

                levels[feature.unit_level]["matched"] += 1
                levels[feature.unit_level]["upserted"] += 1
                levels[feature.unit_level]["created"] += 1 if result["created"] else 0
                levels[feature.unit_level]["updated"] += 0 if result["created"] else 1
                record_territorial_boundary_feature(
                    IGN_ADMIN_BOUNDARY_SOURCE,
                    "matched",
                    feature.unit_level,
                )
                record_territorial_boundary_feature(
                    IGN_ADMIN_BOUNDARY_SOURCE,
                    "upserted",
                    feature.unit_level,
                )
                persisted_units[(feature.unit_level, feature.canonical_code)] = result[
                    "territorial_unit_id"
                ]

            if session is not None:
                await session.commit()

            # PostGIS QA validation on the upserted units
            qa_incident_count = 0
            if (
                self.cartographic_qa_enabled
                and self.qa_repo is not None
                and session is not None
                and persisted_units
            ):
                from app.services.cartographic_qa import CartographicQAService

                qa_service = CartographicQAService(session=session)
                qa_incidents = await qa_service.validate_territorial_units(
                    list(persisted_units.values())
                )
                if qa_incidents:
                    qa_incident_count = await self.qa_repo.save_incidents(qa_incidents)
                    self.logger.info(
                        "qa_validation_completed",
                        extra={"incidents": qa_incident_count},
                    )

        except Exception:
            if session is not None:
                await session.rollback()
            record_territorial_boundary_load(
                IGN_ADMIN_BOUNDARY_SOURCE,
                "failed",
                perf_counter() - started_at,
            )
            raise

        outcome = "completed" if not incidents else "completed_with_incidents"
        duration_seconds = perf_counter() - started_at
        record_territorial_boundary_load(
            IGN_ADMIN_BOUNDARY_SOURCE,
            outcome,
            duration_seconds,
        )
        summary = {
            "source": IGN_ADMIN_BOUNDARY_SOURCE,
            "dataset_version": effective_dataset_version,
            "source_path": source_path,
            "country_code": country_code,
            "autonomous_community_code": autonomous_community_code,
            "features_found": normalized["features_found"],
            "features_selected": len(normalized["features"]),
            "features_rejected": len(incidents),
            "features_matched": sum(level["matched"] for level in levels.values()),
            "features_upserted": sum(level["upserted"] for level in levels.values()),
            "raw_records_saved": raw_records_saved,
            "levels": levels,
            "incidents": incidents,
            "duration_ms": round(duration_seconds * 1000, 2),
            "qa_incidents_detected": qa_incident_count,
        }
        self.logger.info("ign_admin_snapshot_loaded", extra=summary)
        return summary

    async def _persist_raw_snapshot_groups(
        self,
        *,
        payload: dict[str, Any],
        features: list[IGNAdministrativeBoundaryFeature],
        source_path: str,
        dataset_version: str,
        country_code: str,
        autonomous_community_code: str,
    ) -> int:
        raw_records_saved = 0
        full_snapshot_saved = await self.ingestion_repo.save_raw(
            source_type=IGN_ADMIN_SNAPSHOT_SOURCE_TYPE,
            source_key=f"{dataset_version}:{autonomous_community_code}",
            request_path=source_path,
            request_params={
                "dataset_version": dataset_version,
                "country_code": country_code,
                "autonomous_community_code": autonomous_community_code,
                "provider_contract_exposed": False,
            },
            payload=payload,
        )
        raw_records_saved += 1 if full_snapshot_saved is not None else 0

        grouped_features: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for feature in features:
            grouped_features[feature.unit_level].append(feature.raw_feature)

        for unit_level, raw_features in grouped_features.items():
            if not raw_features:
                continue
            raw_record_id = await self.ingestion_repo.save_raw(
                source_type=IGN_ADMIN_LEVEL_SOURCE_TYPES[unit_level],
                source_key=f"{dataset_version}:{unit_level}:{autonomous_community_code}",
                request_path=source_path,
                request_params={
                    "dataset_version": dataset_version,
                    "unit_level": unit_level,
                    "country_code": country_code,
                    "autonomous_community_code": autonomous_community_code,
                    "features_total": len(raw_features),
                    "provider_contract_exposed": False,
                },
                payload={
                    "type": "FeatureCollection",
                    "features": raw_features,
                    "metadata": {
                        "dataset_version": dataset_version,
                        "unit_level": unit_level,
                        "country_code": country_code,
                        "autonomous_community_code": autonomous_community_code,
                    },
                },
            )
            raw_records_saved += 1 if raw_record_id is not None else 0

        return raw_records_saved


def normalize_ign_admin_snapshot(
    payload: dict[str, Any],
    *,
    country_code: str = "ES",
    autonomous_community_code: str = IGN_ADMIN_SCOPE_ASTURIAS_CODE,
) -> dict[str, Any]:
    raw_features = payload.get("features")
    if not isinstance(raw_features, list):
        raise ValueError("IGN administrative snapshot must contain a features list.")

    incidents: list[dict[str, Any]] = []
    normalized_features: list[IGNAdministrativeBoundaryFeature] = []

    for index, raw_feature in enumerate(raw_features):
        try:
            normalized_features.append(
                _normalize_ign_admin_feature(
                    raw_feature,
                    country_code=country_code,
                )
            )
        except ValueError as exc:
            incidents.append(
                {
                    "reason": "invalid_feature",
                    "feature_index": index,
                    "error": str(exc),
                }
            )

    in_scope_provinces = {
        feature.canonical_code
        for feature in normalized_features
        if feature.unit_level == TERRITORIAL_UNIT_LEVEL_PROVINCE
        and feature.autonomous_community_code == autonomous_community_code
    }
    selected_features: list[IGNAdministrativeBoundaryFeature] = []
    for feature in normalized_features:
        if _is_feature_in_scope(
            feature,
            country_code=country_code,
            autonomous_community_code=autonomous_community_code,
            in_scope_provinces=in_scope_provinces,
        ):
            selected_features.append(feature)

    selected_features.sort(
        key=lambda feature: (
            IGN_ADMIN_LEVEL_ORDER[feature.unit_level],
            feature.canonical_code,
        )
    )
    return {
        "features_found": len(raw_features),
        "features": selected_features,
        "incidents": incidents,
    }


def _normalize_ign_admin_feature(
    raw_feature: dict[str, Any],
    *,
    country_code: str,
) -> IGNAdministrativeBoundaryFeature:
    if not isinstance(raw_feature, dict) or raw_feature.get("type") != "Feature":
        raise ValueError("Each IGN boundary entry must be a GeoJSON Feature.")

    properties = raw_feature.get("properties")
    if not isinstance(properties, dict):
        raise ValueError("Each IGN boundary feature must contain a properties object.")

    geometry = raw_feature.get("geometry")
    if not isinstance(geometry, dict):
        raise ValueError("Each IGN boundary feature must contain a geometry object.")
    geometry_type = str(geometry.get("type") or "")
    if geometry_type == "Polygon":
        geometry = {"type": "MultiPolygon", "coordinates": [geometry.get("coordinates")]}
    elif geometry_type != "MultiPolygon":
        raise ValueError(f"Unsupported geometry type for IGN boundary feature: {geometry_type}.")
    if not geometry.get("coordinates"):
        raise ValueError("IGN boundary geometry must include coordinates.")

    unit_level = _resolve_unit_level(properties)
    canonical_code = _resolve_canonical_code(
        properties, unit_level=unit_level, country_code=country_code
    )
    canonical_name = _first_non_empty_string(
        properties,
        "canonical_name",
        "NAMEUNIT",  # INSPIRE / CNIG field
        "name",
        "nombre",
        "label",
        "display_name",
    )
    if not canonical_name:
        raise ValueError("IGN boundary feature is missing canonical_name.")
    display_name = (
        _first_non_empty_string(
            properties,
            "display_name",
            "label",
            "nombre",
            "name",
        )
        or canonical_name
    )

    parent_unit_level, parent_canonical_code = _resolve_parent_reference(
        properties,
        unit_level=unit_level,
        canonical_code=canonical_code,
        country_code=country_code,
    )
    autonomous_community = _first_non_empty_string(
        properties,
        "autonomous_community_code",
        "codigo_ccaa",
        "cod_ccaa",
        "ccaa_code",
    )
    if not autonomous_community:
        # Derive CCAA from INSPIRE NATCODE: bytes 2-3 (0-indexed) hold the INE CCAA code
        natcode_raw = _first_non_empty_string(properties, "NATCODE", "natcode")
        if natcode_raw:
            natcode_len = len(natcode_raw)
            if natcode_len == 4:
                # This feature IS the CCAA level; its own code is the CCAA code
                autonomous_community = canonical_code
            elif natcode_len in (6, 11):
                ccaa_candidate = natcode_raw[2:4]
                if ccaa_candidate.isdigit():
                    autonomous_community = ccaa_candidate
    province_code = _first_non_empty_string(
        properties,
        "province_code",
        "codigo_provincia",
        "cod_prov",
        "prov_code",
    )
    if unit_level == TERRITORIAL_UNIT_LEVEL_PROVINCE:
        province_code = canonical_code
    elif unit_level == TERRITORIAL_UNIT_LEVEL_MUNICIPALITY:
        province_code = province_code or canonical_code[:2]

    centroid_geojson = raw_feature.get("centroid")
    if centroid_geojson is not None:
        if not isinstance(centroid_geojson, dict) or centroid_geojson.get("type") != "Point":
            raise ValueError("IGN boundary centroid must be a GeoJSON Point when provided.")

    metadata = {
        "provider_unit_level": unit_level,
    }
    if autonomous_community:
        metadata["autonomous_community_code"] = autonomous_community
    if province_code:
        metadata["province_code"] = province_code

    provider_alias = None
    provider_name = _first_non_empty_string(properties, "provider_name", "nombre", "label")
    if provider_name and provider_name != canonical_name:
        provider_alias = provider_name

    return IGNAdministrativeBoundaryFeature(
        unit_level=unit_level,
        canonical_code=canonical_code,
        canonical_name=canonical_name,
        display_name=display_name,
        country_code=country_code,
        parent_unit_level=parent_unit_level,
        parent_canonical_code=parent_canonical_code,
        autonomous_community_code=autonomous_community,
        province_code=province_code,
        geometry_geojson=geometry,
        centroid_geojson=centroid_geojson,
        raw_feature=raw_feature,
        metadata=metadata,
        provider_alias=provider_alias,
    )


def _resolve_unit_level(properties: dict[str, Any]) -> str:
    raw_level = _first_non_empty_string(properties, "unit_level", "nivel", "level", "admin_level")
    normalized_level = _normalize_level(raw_level)
    if normalized_level:
        return normalized_level

    # INSPIRE / CNIG NATCODE heuristic — level inferred from code length:
    #   2  → country  |  4 → autonomous_community  |  6 → province  |  11 → municipality
    natcode = _first_non_empty_string(properties, "NATCODE", "natcode")
    if natcode:
        natcode_len = len(natcode)
        if natcode_len == 2:
            return TERRITORIAL_UNIT_LEVEL_COUNTRY
        if natcode_len == 4:
            return TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY
        if natcode_len == 6:
            return TERRITORIAL_UNIT_LEVEL_PROVINCE
        if natcode_len == 11:
            return TERRITORIAL_UNIT_LEVEL_MUNICIPALITY

    if _first_non_empty_string(
        properties,
        "municipality_code",
        "codigo_municipio",
        "cod_mun",
        "ine_municipality_code",
    ):
        return TERRITORIAL_UNIT_LEVEL_MUNICIPALITY
    if _first_non_empty_string(
        properties,
        "province_code",
        "codigo_provincia",
        "cod_prov",
        "prov_code",
    ):
        return TERRITORIAL_UNIT_LEVEL_PROVINCE
    if _first_non_empty_string(
        properties,
        "autonomous_community_code",
        "codigo_ccaa",
        "cod_ccaa",
        "ccaa_code",
    ):
        return TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY
    if _first_non_empty_string(properties, "country_code", "codigo_pais", "iso2"):
        return TERRITORIAL_UNIT_LEVEL_COUNTRY
    raise ValueError("IGN boundary feature is missing a supported unit level.")


def _normalize_level(raw_level: str | None) -> str | None:
    if not raw_level:
        return None

    normalized = raw_level.strip().lower().replace("-", "_").replace(" ", "_")
    mapping = {
        "country": TERRITORIAL_UNIT_LEVEL_COUNTRY,
        "pais": TERRITORIAL_UNIT_LEVEL_COUNTRY,
        "autonomous_community": TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
        "comunidad_autonoma": TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
        "autonomouscommunity": TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
        "province": TERRITORIAL_UNIT_LEVEL_PROVINCE,
        "provincia": TERRITORIAL_UNIT_LEVEL_PROVINCE,
        "municipality": TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
        "municipio": TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    }
    return mapping.get(normalized)


def _resolve_canonical_code(
    properties: dict[str, Any],
    *,
    unit_level: str,
    country_code: str,
) -> str:
    if unit_level == TERRITORIAL_UNIT_LEVEL_COUNTRY:
        return (
            _first_non_empty_string(properties, "country_code", "codigo_pais", "iso2")
            or country_code
        )
    if unit_level == TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY:
        code = _first_non_empty_string(
            properties,
            "autonomous_community_code",
            "codigo_ccaa",
            "cod_ccaa",
            "ccaa_code",
        )
        if code:
            return code
        # INSPIRE NATCODE length-4: last 2 digits are the INE CCAA code
        natcode = _first_non_empty_string(properties, "NATCODE", "natcode")
        if natcode and len(natcode) == 4 and natcode[-2:].isdigit():
            return natcode[-2:]
    if unit_level == TERRITORIAL_UNIT_LEVEL_PROVINCE:
        code = _first_non_empty_string(
            properties,
            "province_code",
            "codigo_provincia",
            "cod_prov",
            "prov_code",
        )
        if code:
            return code
        # INSPIRE NATCODE length-6: last 2 digits are the INE province code
        natcode = _first_non_empty_string(properties, "NATCODE", "natcode")
        if natcode and len(natcode) == 6 and natcode[-2:].isdigit():
            return natcode[-2:]
    if unit_level == TERRITORIAL_UNIT_LEVEL_MUNICIPALITY:
        code = _first_non_empty_string(
            properties,
            "municipality_code",
            "codigo_municipio",
            "cod_mun",
            "ine_municipality_code",
        )
        if code:
            return code
        # INSPIRE NATCODE length-11: last 5 digits are the INE municipality code
        ine_code = extract_ine_code(_first_non_empty_string(properties, "NATCODE", "natcode"))
        if ine_code:
            return ine_code
    generic_code = _first_non_empty_string(properties, "canonical_code", "code", "codigo")
    if generic_code:
        return generic_code
    raise ValueError("IGN boundary feature is missing canonical_code.")


def _resolve_parent_reference(
    properties: dict[str, Any],
    *,
    unit_level: str,
    canonical_code: str,
    country_code: str,
) -> tuple[str | None, str | None]:
    if unit_level == TERRITORIAL_UNIT_LEVEL_COUNTRY:
        return None, None
    if unit_level == TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY:
        return TERRITORIAL_UNIT_LEVEL_COUNTRY, (
            _first_non_empty_string(properties, "parent_code", "country_code", "codigo_pais")
            or country_code
        )
    if unit_level == TERRITORIAL_UNIT_LEVEL_PROVINCE:
        parent_code = _first_non_empty_string(
            properties,
            "parent_code",
            "autonomous_community_code",
            "codigo_ccaa",
            "cod_ccaa",
            "ccaa_code",
        )
        if not parent_code:
            # INSPIRE NATCODE length-6: bytes 2-3 hold the INE CCAA code
            natcode = _first_non_empty_string(properties, "NATCODE", "natcode")
            if natcode and len(natcode) == 6:
                ccaa_candidate = natcode[2:4]
                if ccaa_candidate.isdigit():
                    parent_code = ccaa_candidate
        return TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY, parent_code
    if unit_level == TERRITORIAL_UNIT_LEVEL_MUNICIPALITY:
        parent_code = (
            _first_non_empty_string(
                properties,
                "parent_code",
                "province_code",
                "codigo_provincia",
                "cod_prov",
                "prov_code",
            )
            or canonical_code[:2]
        )
        return TERRITORIAL_UNIT_LEVEL_PROVINCE, parent_code
    return None, None


def _is_feature_in_scope(
    feature: IGNAdministrativeBoundaryFeature,
    *,
    country_code: str,
    autonomous_community_code: str,
    in_scope_provinces: set[str],
) -> bool:
    if feature.unit_level == TERRITORIAL_UNIT_LEVEL_COUNTRY:
        return feature.canonical_code == country_code
    if feature.unit_level == TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY:
        return feature.canonical_code == autonomous_community_code
    if feature.unit_level == TERRITORIAL_UNIT_LEVEL_PROVINCE:
        return feature.autonomous_community_code == autonomous_community_code
    if feature.unit_level == TERRITORIAL_UNIT_LEVEL_MUNICIPALITY:
        if feature.autonomous_community_code == autonomous_community_code:
            return True
        return bool(feature.province_code and feature.province_code in in_scope_provinces)
    return False


def extract_ine_code(natcode: str | None) -> str | None:
    """Extract the 5-digit INE municipality code from an IGN/CNIG INSPIRE NATCODE.

    CNIG NATCODE format for Spanish municipalities (11 digits):
        <country(2)><ccaa(2)><province(2)><municipality(5)>
        e.g. "34033333044"  →  "33044"  (Oviedo, Asturias)

    Returns None when *natcode* is absent, shorter than 5 chars, or the last
    5 characters are not all digits.
    """
    if not natcode:
        return None
    cleaned = str(natcode).strip()
    if len(cleaned) < 5:
        return None
    candidate = cleaned[-5:]
    if not candidate.isdigit():
        return None
    return candidate


def _first_non_empty_string(properties: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = properties.get(key)
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized:
            return normalized
    return None
