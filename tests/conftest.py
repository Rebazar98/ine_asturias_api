from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

import httpx
import pytest
from fastapi.testclient import TestClient

from app.core.cache import InMemoryTTLCache
from app.dependencies import (
    get_ingestion_repository,
    get_ine_client_service,
    get_series_repository,
    get_table_catalog_repository,
    get_territorial_repository,
)
from app.main import app
from app.schemas import NormalizedSeriesItem
from app.services.ine_client import INEClientService
from app.settings import Settings, get_settings


class DummyIngestionRepository:
    def __init__(self) -> None:
        self.records: list[dict] = []

    async def save_raw(self, **kwargs):
        self.records.append(kwargs)
        return len(self.records)


class DummySeriesRepository:
    def __init__(self) -> None:
        self.items: list[NormalizedSeriesItem] = []

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
        self.units_by_level: dict[str, list[dict]] = {}

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
