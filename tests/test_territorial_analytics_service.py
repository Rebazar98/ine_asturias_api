import asyncio
from datetime import datetime, timezone

from app.repositories.territorial import TERRITORIAL_UNIT_LEVEL_MUNICIPALITY
from app.schemas import NormalizedSeriesItem
from app.services.territorial_analytics import TerritorialAnalyticsService
from tests.conftest import (
    DummyAnalyticalSnapshotRepository,
    DummySeriesRepository,
    DummyTerritorialRepository,
)


def _build_seeded_repositories() -> tuple[DummyTerritorialRepository, DummySeriesRepository]:
    territorial_repo = DummyTerritorialRepository()
    territorial_repo.detail_by_canonical_code[(TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, "33044")] = {
        "id": 33044,
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
            "code_value": "33044",
            "is_primary": True,
        },
        "codes": [
            {
                "source_system": "ine",
                "code_type": "municipality",
                "code_value": "33044",
                "is_primary": True,
            }
        ],
        "aliases": [],
        "attributes": {"population_scope": "municipal"},
    }
    series_repo = DummySeriesRepository()
    series_repo.items.extend(
        [
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP_TOTAL",
                geography_name="Oviedo",
                geography_code="33044",
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
                geography_code="33044",
                period="2024M01",
                value=142.5,
                unit="indice",
                metadata={"series_name": "Indice de envejecimiento"},
            ),
        ]
    )
    return territorial_repo, series_repo


def test_build_municipality_report_returns_structured_result():
    territorial_repo, series_repo = _build_seeded_repositories()
    service = TerritorialAnalyticsService(
        territorial_repo=territorial_repo,
        series_repo=series_repo,
        now_factory=lambda: datetime(2026, 3, 14, 14, 0, tzinfo=timezone.utc),
    )

    report = asyncio.run(
        service.build_municipality_report(
            municipality_code="33044",
            page=1,
            page_size=10,
        )
    )

    assert report is not None
    assert report.report_type == "municipality_report"
    assert report.territorial_unit.canonical_name == "Oviedo"
    assert report.summary.indicators_total == 2
    assert [section.section_key for section in report.sections] == [
        "territorial_profile",
        "latest_indicators",
    ]
    assert report.sections[1].series[0].series_key.startswith("ine.")
    assert report.metadata["storage_mode"] == "job_store_only"


def test_build_municipality_report_persists_and_reuses_snapshot():
    territorial_repo, series_repo = _build_seeded_repositories()
    snapshot_repo = DummyAnalyticalSnapshotRepository()
    service = TerritorialAnalyticsService(
        territorial_repo=territorial_repo,
        series_repo=series_repo,
        analytical_snapshot_repo=snapshot_repo,
        analytical_snapshot_ttl_seconds=3600,
        now_factory=lambda: datetime(2026, 3, 14, 14, 0, tzinfo=timezone.utc),
    )

    first_report = asyncio.run(
        service.build_municipality_report(
            municipality_code="33044",
            page=1,
            page_size=10,
        )
    )
    second_report = asyncio.run(
        service.build_municipality_report(
            municipality_code="33044",
            page=1,
            page_size=10,
        )
    )

    assert first_report is not None
    assert second_report is not None
    assert first_report.metadata["storage_mode"] == "persistent_snapshot"
    assert first_report.metadata["snapshot_reused"] is False
    assert second_report.metadata["storage_mode"] == "persistent_snapshot"
    assert second_report.metadata["snapshot_reused"] is True
    assert first_report.metadata["snapshot_key"] == second_report.metadata["snapshot_key"]
    assert snapshot_repo.upsert_calls == 1
    assert series_repo.latest_indicator_calls == 1


def test_build_municipality_report_returns_none_when_municipality_is_unknown():
    service = TerritorialAnalyticsService(
        territorial_repo=DummyTerritorialRepository(),
        series_repo=DummySeriesRepository(),
    )

    report = asyncio.run(
        service.build_municipality_report(
            municipality_code="99999",
            page=1,
            page_size=10,
        )
    )

    assert report is None
