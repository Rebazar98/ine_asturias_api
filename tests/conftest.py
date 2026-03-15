from __future__ import annotations
from copy import deepcopy
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from fastapi.testclient import TestClient

from app.core.cache import InMemoryTTLCache
from app.dependencies import (
    get_analytical_snapshot_repository,
    get_catastro_client_service,
    get_catastro_municipality_cache_repository,
    get_ingestion_repository,
    get_ine_client_service,
    get_series_repository,
    get_table_catalog_repository,
    get_territorial_export_artifact_repository,
    get_territorial_repository,
)
from app.main import app
from app.repositories.analytics_snapshots import build_snapshot_key
from app.repositories.territorial_export_artifacts import build_export_key
from app.repositories.territorial import (
    TERRITORIAL_DISCOVERY_LEVELS,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    get_canonical_code_strategy,
)
from app.repositories.series import SeriesRepository
from app.schemas import NormalizedSeriesItem
from app.services.ine_client import INEClientService
from app.settings import Settings, get_settings


DEFAULT_ANALYTICAL_MUNICIPALITY_CODE = "33044"


class DummyIngestionRepository:
    def __init__(self) -> None:
        self.records: list[dict] = []

    async def save_raw(self, **kwargs):
        self.records.append(kwargs)
        return len(self.records)


class DummyCatastroMunicipalityAggregateCacheRepository:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, str, str], dict] = {}
        self.get_calls = 0
        self.upsert_calls = 0
        self._next_id = 1

    async def get_fresh_payload(
        self,
        *,
        provider_family: str,
        municipality_code: str,
        reference_year: str,
        now: datetime | None = None,
    ):
        self.get_calls += 1
        key = (provider_family, municipality_code, reference_year)
        row = self.rows.get(key)
        if row is None:
            return None
        lookup_time = now or datetime.now(timezone.utc)
        if row["expires_at"] <= lookup_time:
            return None
        return deepcopy(row)

    async def upsert_payload(
        self,
        *,
        provider_family: str,
        municipality_code: str,
        reference_year: str,
        payload: dict | list,
        ttl_seconds: int,
        metadata: dict | None = None,
        now: datetime | None = None,
    ):
        self.upsert_calls += 1
        write_time = now or datetime.now(timezone.utc)
        key = (provider_family, municipality_code, reference_year)
        existing = self.rows.get(key)
        row = {
            "id": existing["id"] if existing is not None else self._next_id,
            "provider_family": provider_family,
            "municipality_code": municipality_code,
            "reference_year": reference_year,
            "payload": deepcopy(payload),
            "metadata": deepcopy(metadata or {}),
            "cached_at": write_time,
            "expires_at": write_time + timedelta(seconds=ttl_seconds),
        }
        self.rows[key] = deepcopy(row)
        if existing is None:
            self._next_id += 1
        return deepcopy(row)


class DummyCatastroClientService:
    def __init__(self) -> None:
        self.reference_year = "2025"
        self.calls: list[dict] = []
        self.payload: dict = {
            "reference_year": "2025",
            "province_file_code": "04133",
            "province_label": "Asturias",
            "municipality_option_value": "0043",
            "municipality_label": "Oviedo",
            "indicators": [
                {
                    "series_key": "catastro_urbano.last_valuation_year",
                    "label": "Ano ultima valoracion",
                    "value": 2013,
                    "unit": "anos",
                    "metadata": {"provider": "catastro"},
                },
                {
                    "series_key": "catastro_urbano.urban_parcels",
                    "label": "Parcelas urbanas",
                    "value": 23783,
                    "unit": "unidades",
                    "metadata": {"provider": "catastro"},
                },
                {
                    "series_key": "catastro_urbano.urban_parcel_area_hectares",
                    "label": "Superficie parcelas urbanas",
                    "value": 3277.79,
                    "unit": "hectareas",
                    "metadata": {"provider": "catastro"},
                },
                {
                    "series_key": "catastro_urbano.real_estate_assets",
                    "label": "Bienes inmuebles",
                    "value": 243583,
                    "unit": "unidades",
                    "metadata": {"provider": "catastro"},
                },
                {
                    "series_key": "catastro_urbano.cadastral_construction_value",
                    "label": "Valor catastral construccion",
                    "value": 6430733.88,
                    "unit": "miles_euros",
                    "metadata": {"provider": "catastro"},
                },
                {
                    "series_key": "catastro_urbano.cadastral_land_value",
                    "label": "Valor catastral suelo",
                    "value": 8120131.57,
                    "unit": "miles_euros",
                    "metadata": {"provider": "catastro"},
                },
                {
                    "series_key": "catastro_urbano.cadastral_total_value",
                    "label": "Valor catastral total",
                    "value": 14550865.45,
                    "unit": "miles_euros",
                    "metadata": {"provider": "catastro"},
                },
            ],
            "raw": {
                "content_type": "text/html",
                "reference_year": "2025",
                "province_file_code": "04133",
                "province_label": "Asturias",
                "municipality_option_value": "0043",
                "municipality_label": "Oviedo",
                "result_html": "<table><tr><td>Oviedo</td></tr></table>",
            },
            "metadata": {
                "provider": "catastro",
                "provider_family": "catastro_urbano",
                "reference_year": "2025",
                "province_file_code": "04133",
                "province_label": "Asturias",
                "municipality_option_value": "0043",
                "municipality_label": "Oviedo",
            },
        }
        self.raise_error: Exception | None = None

    async def get_reference_year(self) -> str:
        return self.reference_year

    async def fetch_municipality_aggregates(
        self,
        *,
        province_candidates,
        municipality_candidates,
    ):
        self.calls.append(
            {
                "province_candidates": list(province_candidates),
                "municipality_candidates": list(municipality_candidates),
            }
        )
        if self.raise_error is not None:
            raise self.raise_error
        return deepcopy(self.payload)


class DummySeriesRepository:
    def __init__(self) -> None:
        self.items: list[NormalizedSeriesItem] = []
        self.latest_indicator_calls = 0

    async def upsert_many(self, items, batch_size=500):
        self.items.extend(items)
        return len(items)

    async def list_normalized(
        self,
        operation_code=None,
        table_id=None,
        geography_code=None,
        geography_name=None,
        geography_code_system="ine",
        variable_id=None,
        period_from=None,
        period_to=None,
        page=1,
        page_size=50,
    ):
        rows = self.items
        if operation_code:
            rows = [item for item in rows if item.operation_code == operation_code]
        if table_id:
            rows = [item for item in rows if item.table_id == table_id]
        if geography_code:
            rows = [item for item in rows if item.geography_code == geography_code]
        if geography_name:
            rows = [item for item in rows if item.geography_name.lower() == geography_name.lower()]
        if variable_id:
            rows = [item for item in rows if item.variable_id == variable_id]
        if period_from:
            rows = [item for item in rows if item.period >= period_from]
        if period_to:
            rows = [item for item in rows if item.period <= period_to]

        total = len(rows)
        start = (page - 1) * page_size
        end = start + page_size
        paged = rows[start:end]
        pages = (total + page_size - 1) // page_size if total else 0
        return {
            "items": [
                {
                    "id": index + 1,
                    "operation_code": item.operation_code,
                    "table_id": item.table_id,
                    "variable_id": item.variable_id,
                    "geography_name": item.geography_name,
                    "geography_code": item.geography_code,
                    "period": item.period,
                    "value": item.value,
                    "unit": item.unit,
                    "metadata": item.metadata,
                    "inserted_at": None,
                }
                for index, item in enumerate(paged)
            ],
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "has_next": page < pages,
            "has_previous": page > 1,
            "filters": {
                "operation_code": operation_code,
                "table_id": table_id,
                "geography_code": geography_code,
                "geography_name": geography_name,
                "geography_code_system": geography_code_system,
                "variable_id": variable_id,
                "period_from": period_from,
                "period_to": period_to,
            },
        }

    async def list_latest_indicators_by_geography(
        self,
        *,
        geography_code: str,
        operation_code: str | None = None,
        variable_id: str | None = None,
        period_from: str | None = None,
        period_to: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ):
        self.latest_indicator_calls += 1
        rows = [item for item in self.items if item.geography_code == geography_code]
        if operation_code:
            rows = [item for item in rows if item.operation_code == operation_code]
        if variable_id:
            rows = [item for item in rows if item.variable_id == variable_id]
        if period_from:
            rows = [item for item in rows if item.period >= period_from]
        if period_to:
            rows = [item for item in rows if item.period <= period_to]

        latest_by_series: dict[tuple[str, str, str, str], tuple[int, NormalizedSeriesItem]] = {}
        for index, item in enumerate(rows):
            series_key = (
                item.operation_code,
                item.table_id,
                item.variable_id,
                item.geography_code,
            )
            existing = latest_by_series.get(series_key)
            candidate_rank = (item.period, index)
            if existing is None or candidate_rank > (existing[1].period, existing[0]):
                latest_by_series[series_key] = (index, item)

        latest_rows = [item for _, item in latest_by_series.values()]
        latest_rows.sort(key=lambda item: item.variable_id)
        latest_rows.sort(key=lambda item: item.table_id)
        latest_rows.sort(key=lambda item: item.operation_code)
        latest_rows.sort(key=lambda item: item.period, reverse=True)

        total = len(latest_rows)
        start = (page - 1) * page_size
        end = start + page_size
        paged = latest_rows[start:end]
        pages = (total + page_size - 1) // page_size if total else 0
        return {
            "items": [SeriesRepository.serialize_latest_indicator_item(item) for item in paged],
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "has_next": page < pages,
            "has_previous": page > 1,
            "filters": {
                "geography_code": geography_code,
                "geography_code_system": "ine",
                "operation_code": operation_code,
                "variable_id": variable_id,
                "period_from": period_from,
                "period_to": period_to,
            },
            "summary": {
                "operation_codes": sorted({item.operation_code for item in latest_rows}),
                "latest_period": latest_rows[0].period if latest_rows else None,
            },
        }


class DummyTableCatalogRepository:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], dict] = {}
        self._next_id = 1

    async def upsert_discovered_tables(
        self, operation_code, tables, request_path, resolution_context=None
    ):
        count = 0
        for table in tables:
            key = (operation_code, str(table["table_id"]))
            existing = self.rows.get(key)
            if existing is None:
                existing = {
                    "id": self._next_id,
                    "operation_code": operation_code,
                    "table_id": str(table["table_id"]),
                    "table_name": str(table.get("table_name", "")),
                    "request_path": request_path,
                    "resolution_context": dict(resolution_context or {}),
                    "has_asturias_data": None,
                    "validation_status": "unknown",
                    "normalized_rows": 0,
                    "raw_rows_retrieved": 0,
                    "filtered_rows_retrieved": 0,
                    "series_kept": 0,
                    "series_discarded": 0,
                    "last_checked_at": None,
                    "first_seen_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                    "metadata": dict(table.get("metadata", {})),
                    "notes": "",
                    "last_warning": "",
                }
                self.rows[key] = existing
                self._next_id += 1
            else:
                existing.update(
                    {
                        "table_name": str(table.get("table_name", "")),
                        "request_path": request_path,
                        "resolution_context": dict(resolution_context or {}),
                        "metadata": dict(table.get("metadata", {})),
                        "updated_at": datetime.now(timezone.utc),
                    }
                )
            count += 1
        return count

    async def update_table_status(
        self,
        operation_code,
        table_id,
        table_name,
        request_path,
        resolution_context=None,
        has_asturias_data=None,
        validation_status="unknown",
        normalized_rows=0,
        raw_rows_retrieved=0,
        filtered_rows_retrieved=0,
        series_kept=0,
        series_discarded=0,
        metadata=None,
        notes="",
        last_warning="",
    ):
        key = (operation_code, str(table_id))
        existing = self.rows.get(key)
        if existing is None:
            await self.upsert_discovered_tables(
                operation_code,
                [{"table_id": table_id, "table_name": table_name, "metadata": metadata or {}}],
                request_path,
                resolution_context,
            )
            existing = self.rows[key]
        existing.update(
            {
                "table_name": table_name,
                "request_path": request_path,
                "resolution_context": dict(resolution_context or {}),
                "has_asturias_data": has_asturias_data,
                "validation_status": validation_status,
                "normalized_rows": normalized_rows,
                "raw_rows_retrieved": raw_rows_retrieved,
                "filtered_rows_retrieved": filtered_rows_retrieved,
                "series_kept": series_kept,
                "series_discarded": series_discarded,
                "last_checked_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
                "metadata": dict(metadata or existing.get("metadata", {})),
                "notes": notes,
                "last_warning": last_warning,
            }
        )
        return True

    async def list_by_operation(self, operation_code):
        return [self.rows[key] for key in sorted(self.rows) if key[0] == operation_code]

    async def get_operation_summary(self, operation_code):
        rows = await self.list_by_operation(operation_code)
        return {
            "operation_code": operation_code,
            "total_tables": len(rows),
            "has_data": sum(1 for row in rows if row["validation_status"] == "has_data"),
            "no_data": sum(1 for row in rows if row["validation_status"] == "no_data"),
            "failed": sum(1 for row in rows if row["validation_status"] == "failed"),
            "unknown": sum(1 for row in rows if row["validation_status"] == "unknown"),
        }

    async def get_known_no_data_table_ids(self, operation_code):
        return {
            row["table_id"]
            for row in (await self.list_by_operation(operation_code))
            if row["validation_status"] == "no_data" and row["has_asturias_data"] is False
        }


class DummyTerritorialRepository:
    def __init__(self) -> None:
        self.by_name: dict[str, dict] = {}
        self.by_canonical_code: dict[tuple[str, str], dict] = {}
        self.detail_by_canonical_code: dict[tuple[str, str], dict] = {}
        self.detail_by_id: dict[int, dict] = {}
        self.hierarchy_by_unit_id: dict[int, list[dict]] = {}
        self.units_by_level: dict[str, list[dict]] = {}
        self.upsert_boundary_calls: list[dict[str, object]] = []
        self.point_resolution_payload: dict | None = None

    async def get_unit_by_name(
        self, name: str, source_system=None, alias_type=None, unit_level=None
    ):
        if unit_level is not None:
            return self.by_name.get((unit_level, name))
        return self.by_name.get(name)

    async def get_unit_by_canonical_code(self, unit_level: str, code_value: str):
        return self.by_canonical_code.get((unit_level, code_value))

    async def get_unit_detail_by_canonical_code(self, *, unit_level: str, code_value: str):
        return self.detail_by_canonical_code.get((unit_level, code_value))

    async def get_unit_detail_by_id(self, territorial_unit_id: int):
        return self.detail_by_id.get(territorial_unit_id)

    async def list_hierarchy(self, territorial_unit_id: int):
        return deepcopy(self.hierarchy_by_unit_id.get(territorial_unit_id, []))

    async def list_units(
        self,
        *,
        unit_level: str,
        page: int = 1,
        page_size: int = 50,
        country_code: str | None = None,
        parent_id: int | None = None,
        active_only: bool = True,
    ):
        rows = list(self.units_by_level.get(unit_level, []))
        if country_code:
            rows = [row for row in rows if row.get("country_code") == country_code]
        if parent_id is not None:
            rows = [row for row in rows if row.get("parent_id") == parent_id]
        if active_only:
            rows = [row for row in rows if row.get("is_active", True)]

        total = len(rows)
        start = (page - 1) * page_size
        end = start + page_size
        paged = rows[start:end]
        pages = (total + page_size - 1) // page_size if total else 0
        return {
            "items": paged,
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "has_next": page < pages,
            "has_previous": page > 1,
            "filters": {
                "unit_level": unit_level,
                "country_code": country_code,
                "parent_id": parent_id,
                "active_only": active_only,
            },
        }

    async def get_catalog_coverage(self, *, country_code: str = "ES"):
        coverage = []
        for unit_level in TERRITORIAL_DISCOVERY_LEVELS:
            rows = [
                row
                for row in self.units_by_level.get(unit_level, [])
                if not country_code or row.get("country_code") == country_code
            ]
            coverage.append(
                {
                    "unit_level": unit_level,
                    "country_code": country_code,
                    "units_total": len(rows),
                    "active_units": sum(1 for row in rows if row.get("is_active", True)),
                    "geometry_units": sum(1 for row in rows if row.get("has_geometry", False)),
                    "centroid_units": sum(1 for row in rows if row.get("has_centroid", False)),
                    "boundary_source": (
                        "ign_administrative_boundaries"
                        if any(row.get("has_geometry", False) for row in rows)
                        else None
                    ),
                    "canonical_code_strategy": get_canonical_code_strategy(unit_level),
                }
            )
        return coverage

    async def upsert_boundary_unit(
        self,
        *,
        unit_level: str,
        canonical_code: str,
        canonical_name: str,
        display_name: str,
        country_code: str,
        parent_id: int | None,
        geometry_geojson: dict,
        centroid_geojson: dict | None,
        provider_source: str,
        provider_alias: str | None = None,
        provider_alias_type: str = "provider_name",
        boundary_metadata: dict | None = None,
    ):
        self.upsert_boundary_calls.append(
            {
                "unit_level": unit_level,
                "canonical_code": canonical_code,
                "canonical_name": canonical_name,
                "display_name": display_name,
                "country_code": country_code,
                "parent_id": parent_id,
                "geometry_geojson": geometry_geojson,
                "centroid_geojson": centroid_geojson,
                "provider_source": provider_source,
                "provider_alias": provider_alias,
                "provider_alias_type": provider_alias_type,
                "boundary_metadata": deepcopy(boundary_metadata or {}),
            }
        )
        unit_id = len(self.upsert_boundary_calls)
        row = {
            "id": unit_id,
            "parent_id": parent_id,
            "unit_level": unit_level,
            "canonical_name": canonical_name,
            "display_name": display_name,
            "country_code": country_code,
            "is_active": True,
            "has_geometry": True,
            "has_centroid": True,
            "canonical_code": {
                "source_system": get_canonical_code_strategy(unit_level)["source_system"],
                "code_type": get_canonical_code_strategy(unit_level)["code_type"],
                "code_value": canonical_code,
                "is_primary": True,
            },
            "canonical_code_strategy": get_canonical_code_strategy(unit_level),
        }
        self.by_canonical_code[(unit_level, canonical_code)] = {
            "id": unit_id,
            "unit_level": unit_level,
            "canonical_name": canonical_name,
            "display_name": display_name,
            "country_code": country_code,
            "is_active": True,
            "canonical_code": row["canonical_code"],
        }
        self.units_by_level.setdefault(unit_level, []).append(row)
        return {
            "territorial_unit_id": unit_id,
            "unit_level": unit_level,
            "canonical_code": canonical_code,
            "created": True,
        }

    async def resolve_point(self, *, lat: float, lon: float):
        if self.point_resolution_payload is None:
            return {
                "matched_by": "geometry_cover",
                "best_match": None,
                "hierarchy": [],
                "coverage": {
                    "boundary_source": None,
                    "levels_considered": [
                        "country",
                        "autonomous_community",
                        "province",
                        "municipality",
                    ],
                    "levels_matched": [],
                },
                "ambiguity_detected": False,
                "ambiguity_by_level": {},
            }
        return deepcopy(self.point_resolution_payload)


class DummyAnalyticalSnapshotRepository:
    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}
        self.get_calls = 0
        self.upsert_calls = 0
        self._next_id = 1

    async def get_fresh_snapshot(
        self,
        *,
        snapshot_type: str,
        scope_key: str,
        filters: dict | None = None,
        now: datetime | None = None,
    ):
        self.get_calls += 1
        snapshot_key = build_snapshot_key(
            snapshot_type=snapshot_type,
            scope_key=scope_key,
            filters=filters,
        )
        row = self.rows.get(snapshot_key)
        if row is None:
            return None
        lookup_time = now or datetime.now(timezone.utc)
        if row["expires_at"] <= lookup_time:
            return None
        return deepcopy(row)

    async def upsert_snapshot(
        self,
        *,
        snapshot_type: str,
        scope_key: str,
        source: str,
        payload: dict | list,
        ttl_seconds: int,
        territorial_unit_id: int | None = None,
        filters: dict | None = None,
        metadata: dict | None = None,
        generated_at: datetime | None = None,
        now: datetime | None = None,
    ):
        self.upsert_calls += 1
        write_time = now or datetime.now(timezone.utc)
        snapshot_key = build_snapshot_key(
            snapshot_type=snapshot_type,
            scope_key=scope_key,
            filters=filters,
        )
        existing = self.rows.get(snapshot_key)
        row = {
            "id": existing["id"] if existing is not None else self._next_id,
            "snapshot_key": snapshot_key,
            "snapshot_type": snapshot_type,
            "scope_key": scope_key,
            "source": source,
            "territorial_unit_id": territorial_unit_id,
            "filters": deepcopy(filters or {}),
            "payload": deepcopy(payload),
            "metadata": deepcopy(metadata or {}),
            "generated_at": generated_at or write_time,
            "created_at": existing["created_at"] if existing is not None else write_time,
            "updated_at": write_time,
            "expires_at": write_time + timedelta(seconds=ttl_seconds),
        }
        self.rows[snapshot_key] = deepcopy(row)
        if existing is None:
            self._next_id += 1
        return deepcopy(row)


class DummyTerritorialExportArtifactRepository:
    def __init__(self) -> None:
        self.rows_by_key: dict[str, dict] = {}
        self.rows_by_id: dict[int, dict] = {}
        self.get_calls = 0
        self.upsert_calls = 0
        self._next_id = 1

    async def get_fresh_artifact(
        self,
        *,
        unit_level: str,
        code_value: str,
        artifact_format: str,
        include_providers: list[str] | tuple[str, ...] | None = None,
        now: datetime | None = None,
    ):
        self.get_calls += 1
        export_key = build_export_key(
            unit_level=unit_level,
            code_value=code_value,
            artifact_format=artifact_format,
            include_providers=include_providers,
        )
        row = self.rows_by_key.get(export_key)
        if row is None:
            return None
        lookup_time = now or datetime.now(timezone.utc)
        if row["expires_at"] <= lookup_time:
            return None
        return deepcopy(row)

    async def get_by_export_id(self, export_id: int):
        row = self.rows_by_id.get(export_id)
        if row is None:
            return None
        return deepcopy(row)

    async def upsert_artifact(
        self,
        *,
        territorial_unit_id: int | None,
        unit_level: str,
        code_value: str,
        artifact_format: str,
        content_type: str,
        filename: str,
        payload_bytes: bytes,
        ttl_seconds: int,
        include_providers: list[str] | tuple[str, ...] | None = None,
        metadata: dict | None = None,
        now: datetime | None = None,
    ):
        self.upsert_calls += 1
        write_time = now or datetime.now(timezone.utc)
        export_key = build_export_key(
            unit_level=unit_level,
            code_value=code_value,
            artifact_format=artifact_format,
            include_providers=include_providers,
        )
        existing = self.rows_by_key.get(export_key)
        row = {
            "export_id": existing["export_id"] if existing is not None else self._next_id,
            "export_key": export_key,
            "territorial_unit_id": territorial_unit_id,
            "unit_level": unit_level,
            "code_value": code_value,
            "artifact_format": artifact_format,
            "content_type": content_type,
            "filename": filename,
            "payload_bytes": bytes(payload_bytes),
            "payload_sha256": "dummy",
            "byte_size": len(payload_bytes),
            "metadata": deepcopy(metadata or {}),
            "created_at": existing["created_at"] if existing is not None else write_time,
            "updated_at": write_time,
            "expires_at": write_time + timedelta(seconds=ttl_seconds),
        }
        self.rows_by_key[export_key] = deepcopy(row)
        self.rows_by_id[row["export_id"]] = deepcopy(row)
        if existing is None:
            self._next_id += 1
        return deepcopy(row)


def seed_municipality_analytics_context(
    territorial_repo: DummyTerritorialRepository,
    series_repo: DummySeriesRepository,
    *,
    municipality_code: str = DEFAULT_ANALYTICAL_MUNICIPALITY_CODE,
) -> dict:
    territorial_repo.detail_by_canonical_code[
        (TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, municipality_code)
    ] = {
        "id": int(municipality_code),
        "parent_id": 33,
        "unit_level": "municipality",
        "canonical_name": "Oviedo",
        "display_name": "Oviedo",
        "country_code": "ES",
        "is_active": True,
        "canonical_code_strategy": {"source_system": "ine", "code_type": "municipality"},
        "canonical_code": {
            "source_system": "ine",
            "code_type": "municipality",
            "code_value": municipality_code,
            "is_primary": True,
        },
        "codes": [
            {
                "source_system": "ine",
                "code_type": "municipality",
                "code_value": municipality_code,
                "is_primary": True,
            }
        ],
        "aliases": [],
        "attributes": {"population_scope": "municipal"},
    }
    territorial_repo.detail_by_id[int(municipality_code)] = deepcopy(
        territorial_repo.detail_by_canonical_code[
            (TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, municipality_code)
        ]
    )
    series_repo.items.extend(
        [
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP_TOTAL",
                geography_name="Oviedo",
                geography_code=municipality_code,
                period="2024",
                value=220543,
                unit="personas",
                metadata={"series_name": "Poblacion total"},
            ),
            NormalizedSeriesItem(
                operation_code="22",
                table_id="3901",
                variable_id="AGEING_INDEX",
                geography_name="Oviedo",
                geography_code=municipality_code,
                period="2024M01",
                value=142.5,
                unit="indice",
                metadata={"series_name": "Indice de envejecimiento"},
            ),
        ]
    )
    return {"municipality_code": municipality_code, "canonical_name": "Oviedo"}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("POSTGRES_DSN", "")
    monkeypatch.setenv("REDIS_URL", "")
    monkeypatch.setenv("API_KEY", "")
    get_settings.cache_clear()
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()
    get_settings.cache_clear()


@pytest.fixture
def dummy_ingestion_repo() -> DummyIngestionRepository:
    repo = DummyIngestionRepository()
    app.dependency_overrides[get_ingestion_repository] = lambda: repo
    return repo


@pytest.fixture
def dummy_catastro_cache_repo() -> DummyCatastroMunicipalityAggregateCacheRepository:
    repo = DummyCatastroMunicipalityAggregateCacheRepository()
    app.dependency_overrides[get_catastro_municipality_cache_repository] = lambda: repo
    return repo


@pytest.fixture
def dummy_catastro_client_service() -> DummyCatastroClientService:
    service = DummyCatastroClientService()
    app.dependency_overrides[get_catastro_client_service] = lambda: service
    return service


@pytest.fixture
def dummy_series_repo() -> DummySeriesRepository:
    repo = DummySeriesRepository()
    app.dependency_overrides[get_series_repository] = lambda: repo
    return repo


@pytest.fixture
def dummy_catalog_repo() -> DummyTableCatalogRepository:
    repo = DummyTableCatalogRepository()
    app.dependency_overrides[get_table_catalog_repository] = lambda: repo
    return repo


@pytest.fixture
def dummy_territorial_repo() -> DummyTerritorialRepository:
    repo = DummyTerritorialRepository()
    app.dependency_overrides[get_territorial_repository] = lambda: repo
    return repo


@pytest.fixture
def dummy_analytical_snapshot_repo() -> DummyAnalyticalSnapshotRepository:
    repo = DummyAnalyticalSnapshotRepository()
    app.dependency_overrides[get_analytical_snapshot_repository] = lambda: repo
    return repo


@pytest.fixture
def dummy_territorial_export_artifact_repo() -> DummyTerritorialExportArtifactRepository:
    repo = DummyTerritorialExportArtifactRepository()
    app.dependency_overrides[get_territorial_export_artifact_repository] = lambda: repo
    return repo


def override_ine_service(
    handler: Callable[[httpx.Request], httpx.Response | Awaitable[httpx.Response]],
    enable_cache: bool = True,
) -> None:
    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(transport=transport)
    settings = Settings(
        ine_base_url="https://mocked.ine",
        enable_cache=enable_cache,
        cache_ttl_seconds=60,
    )
    cache = InMemoryTTLCache(enabled=enable_cache, default_ttl_seconds=60)
    service = INEClientService(http_client=http_client, settings=settings, cache=cache)
    app.dependency_overrides[get_ine_client_service] = lambda: service
