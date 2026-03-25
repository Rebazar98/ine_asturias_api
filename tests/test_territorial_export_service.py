import asyncio
import io
import json
from datetime import datetime, UTC
from zipfile import ZipFile

import pytest

from app.repositories.territorial import (
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
)
from app.schemas import NormalizedSeriesItem
from app.services.territorial_analytics import TerritorialAnalyticsService
from app.services.territorial_exports import TerritorialExportService
from tests.conftest import (
    DummyAnalyticalSnapshotRepository,
    DummyCatastroClientService,
    DummyCatastroMunicipalityAggregateCacheRepository,
    DummyCatastroTerritorialAggregateCacheRepository,
    DummyIngestionRepository,
    DummySeriesRepository,
    DummyTerritorialExportArtifactRepository,
    DummyTerritorialRepository,
)


def _territorial_summary(
    *,
    unit_id: int,
    parent_id: int | None,
    unit_level: str,
    canonical_name: str,
    display_name: str,
    code_type: str,
    code_value: str,
    source_system: str = "ine",
) -> dict:
    return {
        "id": unit_id,
        "parent_id": parent_id,
        "unit_level": unit_level,
        "canonical_name": canonical_name,
        "display_name": display_name,
        "country_code": "ES",
        "is_active": True,
        "canonical_code_strategy": {
            "source_system": source_system,
            "code_type": code_type,
        },
        "canonical_code": {
            "source_system": source_system,
            "code_type": code_type,
            "code_value": code_value,
            "is_primary": True,
        },
    }


def _territorial_detail(
    *,
    unit_id: int,
    parent_id: int | None,
    unit_level: str,
    canonical_name: str,
    display_name: str,
    code_type: str,
    code_value: str,
    source_system: str = "ine",
) -> dict:
    payload = _territorial_summary(
        unit_id=unit_id,
        parent_id=parent_id,
        unit_level=unit_level,
        canonical_name=canonical_name,
        display_name=display_name,
        code_type=code_type,
        code_value=code_value,
        source_system=source_system,
    )
    payload["codes"] = [payload["canonical_code"]]
    payload["aliases"] = []
    payload["attributes"] = {
        "population_scope": "municipal" if unit_level == "municipality" else "regional"
    }
    return payload


def _build_municipality_export_repositories() -> tuple[
    DummyTerritorialRepository,
    DummySeriesRepository,
    DummyTerritorialExportArtifactRepository,
    DummyAnalyticalSnapshotRepository,
]:
    territorial_repo = DummyTerritorialRepository()
    series_repo = DummySeriesRepository()
    artifact_repo = DummyTerritorialExportArtifactRepository()
    snapshot_repo = DummyAnalyticalSnapshotRepository()

    municipality_detail = _territorial_detail(
        unit_id=33044,
        parent_id=33,
        unit_level="municipality",
        canonical_name="Oviedo",
        display_name="Oviedo",
        code_type="municipality",
        code_value="33044",
    )
    territorial_repo.detail_by_canonical_code[(TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, "33044")] = (
        municipality_detail
    )
    territorial_repo.detail_by_id[33044] = municipality_detail
    territorial_repo.hierarchy_by_unit_id[33044] = [
        _territorial_summary(
            unit_id=1,
            parent_id=None,
            unit_level="country",
            canonical_name="Espana",
            display_name="Espana",
            code_type="alpha2",
            code_value="ES",
            source_system="iso3166",
        ),
        _territorial_summary(
            unit_id=2,
            parent_id=1,
            unit_level="autonomous_community",
            canonical_name="Asturias",
            display_name="Principado de Asturias",
            code_type="autonomous_community",
            code_value="03",
        ),
        _territorial_summary(
            unit_id=33,
            parent_id=2,
            unit_level="province",
            canonical_name="Asturias",
            display_name="Asturias",
            code_type="province",
            code_value="33",
        ),
        _territorial_summary(
            unit_id=33044,
            parent_id=33,
            unit_level="municipality",
            canonical_name="Oviedo",
            display_name="Oviedo",
            code_type="municipality",
            code_value="33044",
        ),
    ]

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
    return territorial_repo, series_repo, artifact_repo, snapshot_repo


def _build_autonomous_community_export_repositories() -> tuple[
    DummyTerritorialRepository,
    DummySeriesRepository,
    DummyTerritorialExportArtifactRepository,
    DummyAnalyticalSnapshotRepository,
]:
    territorial_repo = DummyTerritorialRepository()
    series_repo = DummySeriesRepository()
    artifact_repo = DummyTerritorialExportArtifactRepository()
    snapshot_repo = DummyAnalyticalSnapshotRepository()

    community_detail = _territorial_detail(
        unit_id=2,
        parent_id=1,
        unit_level="autonomous_community",
        canonical_name="Asturias",
        display_name="Principado de Asturias",
        code_type="autonomous_community",
        code_value="03",
    )
    territorial_repo.detail_by_canonical_code[
        (TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY, "03")
    ] = community_detail
    territorial_repo.detail_by_id[2] = community_detail
    territorial_repo.hierarchy_by_unit_id[2] = [
        _territorial_summary(
            unit_id=1,
            parent_id=None,
            unit_level="country",
            canonical_name="Espana",
            display_name="Espana",
            code_type="alpha2",
            code_value="ES",
            source_system="iso3166",
        ),
        _territorial_summary(
            unit_id=2,
            parent_id=1,
            unit_level="autonomous_community",
            canonical_name="Asturias",
            display_name="Principado de Asturias",
            code_type="autonomous_community",
            code_value="03",
        ),
    ]
    series_repo.items.extend(
        [
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP_TOTAL",
                geography_name="Asturias",
                geography_code="03",
                period="2024",
                value=1011792,
                unit="personas",
                metadata={"series_name": "Poblacion total"},
            ),
            NormalizedSeriesItem(
                operation_code="22",
                table_id="3901",
                variable_id="AGEING_INDEX",
                geography_name="Asturias",
                geography_code="03",
                period="2024M01",
                value=142.5,
                unit="indice",
                metadata={"series_name": "Indice de envejecimiento"},
            ),
        ]
    )
    return territorial_repo, series_repo, artifact_repo, snapshot_repo


def _build_service(
    territorial_repo: DummyTerritorialRepository,
    series_repo: DummySeriesRepository,
    artifact_repo: DummyTerritorialExportArtifactRepository,
    snapshot_repo: DummyAnalyticalSnapshotRepository,
    catastro_client: DummyCatastroClientService | None = None,
    catastro_cache_repo: DummyCatastroMunicipalityAggregateCacheRepository | None = None,
    catastro_aggregate_cache_repo: DummyCatastroTerritorialAggregateCacheRepository | None = None,
    ingestion_repo: DummyIngestionRepository | None = None,
) -> TerritorialExportService:
    analytics_service = TerritorialAnalyticsService(
        territorial_repo=territorial_repo,
        series_repo=series_repo,
        analytical_snapshot_repo=snapshot_repo,
        analytical_snapshot_ttl_seconds=3600,
        now_factory=lambda: datetime(2026, 3, 15, 13, 0, tzinfo=UTC),
    )
    return TerritorialExportService(
        territorial_repo=territorial_repo,
        series_repo=series_repo,
        analytics_service=analytics_service,
        catastro_client=catastro_client or DummyCatastroClientService(),
        catastro_cache_repo=catastro_cache_repo
        or DummyCatastroMunicipalityAggregateCacheRepository(),
        catastro_aggregate_cache_repo=catastro_aggregate_cache_repo
        or DummyCatastroTerritorialAggregateCacheRepository(),
        ingestion_repo=ingestion_repo or DummyIngestionRepository(),
        artifact_repo=artifact_repo,
        export_ttl_seconds=86400,
        catastro_cache_ttl_seconds=604800,
        catastro_aggregate_cache_ttl_seconds=86400,
        catastro_aggregate_max_concurrency=4,
        now_factory=lambda: datetime(2026, 3, 15, 13, 0, tzinfo=UTC),
    )


def test_build_export_returns_municipality_zip_bundle():
    territorial_repo, series_repo, artifact_repo, snapshot_repo = (
        _build_municipality_export_repositories()
    )
    service = _build_service(territorial_repo, series_repo, artifact_repo, snapshot_repo)

    result = asyncio.run(
        service.build_export(
            job_id="job-export-1",
            unit_level="municipality",
            code_value="33044",
            include_providers=["territorial", "ine", "analytics"],
        )
    )

    assert result is not None
    assert result.summary["artifact_reused"] is False
    artifact = asyncio.run(artifact_repo.get_by_export_id(result.export_id))
    assert artifact is not None

    with ZipFile(io.BytesIO(artifact["payload_bytes"])) as archive:
        names = sorted(archive.namelist())
        assert names == [
            "datasets/analytics_municipality_report.json",
            "datasets/analytics_municipality_summary.json",
            "datasets/ine_series.ndjson",
            "datasets/territorial_hierarchy.json",
            "datasets/territorial_unit.json",
            "manifest.json",
        ]
        manifest = json.loads(archive.read("manifest.json"))
        assert manifest["source"] == "internal.export.territorial_bundle"
        assert manifest["providers_requested"] == ["territorial", "ine", "analytics"]
        assert set(manifest["providers_included"]) == {"territorial", "ine", "analytics"}
        territorial_unit_payload = archive.read("datasets/territorial_unit.json").decode("utf-8")
        assert "geometry" not in territorial_unit_payload
        assert "centroid" not in territorial_unit_payload
        ine_lines = archive.read("datasets/ine_series.ndjson").decode("utf-8").strip().splitlines()
        assert len(ine_lines) == 2
        assert all("raw_payload" not in line for line in ine_lines)


def test_build_export_marks_analytics_as_not_applicable_for_autonomous_community():
    territorial_repo, series_repo, artifact_repo, snapshot_repo = (
        _build_autonomous_community_export_repositories()
    )
    service = _build_service(territorial_repo, series_repo, artifact_repo, snapshot_repo)

    result = asyncio.run(
        service.build_export(
            job_id="job-export-2",
            unit_level="autonomous_community",
            code_value="03",
            include_providers=["territorial", "ine", "analytics"],
        )
    )

    assert result is not None
    artifact = asyncio.run(artifact_repo.get_by_export_id(result.export_id))
    assert artifact is not None

    with ZipFile(io.BytesIO(artifact["payload_bytes"])) as archive:
        names = sorted(archive.namelist())
        assert names == [
            "datasets/ine_series.ndjson",
            "datasets/territorial_hierarchy.json",
            "datasets/territorial_unit.json",
            "manifest.json",
        ]
        manifest = json.loads(archive.read("manifest.json"))
        assert manifest["providers_requested"] == ["territorial", "ine", "analytics"]
        assert manifest["providers_included"] == ["ine", "territorial"]
        datasets_by_key = {item["dataset_key"]: item for item in manifest["datasets"]}
        assert datasets_by_key["analytics_municipality_summary"]["applicable"] is False
        assert datasets_by_key["analytics_municipality_summary"]["relative_path"] is None
        assert datasets_by_key["analytics_municipality_report"]["applicable"] is False


def test_build_export_adds_catastro_dataset_for_municipality():
    territorial_repo, series_repo, artifact_repo, snapshot_repo = (
        _build_municipality_export_repositories()
    )
    catastro_client = DummyCatastroClientService()
    catastro_cache_repo = DummyCatastroMunicipalityAggregateCacheRepository()
    ingestion_repo = DummyIngestionRepository()
    service = _build_service(
        territorial_repo,
        series_repo,
        artifact_repo,
        snapshot_repo,
        catastro_client=catastro_client,
        catastro_cache_repo=catastro_cache_repo,
        ingestion_repo=ingestion_repo,
    )

    result = asyncio.run(
        service.build_export(
            job_id="job-export-catastro-1",
            unit_level="municipality",
            code_value="33044",
            include_providers=["territorial", "ine", "analytics", "catastro"],
        )
    )

    assert result is not None
    artifact = asyncio.run(artifact_repo.get_by_export_id(result.export_id))
    assert artifact is not None
    assert len(catastro_client.calls) == 1
    assert len(ingestion_repo.records) == 1
    assert catastro_cache_repo.upsert_calls == 1

    with ZipFile(io.BytesIO(artifact["payload_bytes"])) as archive:
        names = sorted(archive.namelist())
        assert "datasets/catastro_municipality_aggregates.json" in names
        manifest = json.loads(archive.read("manifest.json"))
        assert manifest["providers_requested"] == ["territorial", "ine", "analytics", "catastro"]
        assert set(manifest["providers_included"]) == {
            "territorial",
            "ine",
            "analytics",
            "catastro",
        }
        catastro_payload = json.loads(
            archive.read("datasets/catastro_municipality_aggregates.json").decode("utf-8")
        )
        assert catastro_payload["source"] == "catastro.municipality.aggregates"
        assert catastro_payload["filters"]["reference_year"] == "2025"
        assert catastro_payload["summary"]["indicators_total"] == 7
        assert all("result_html" not in json.dumps(item) for item in catastro_payload["series"])


def test_build_export_adds_catastro_territorial_aggregate_for_autonomous_community():
    territorial_repo, series_repo, artifact_repo, snapshot_repo = (
        _build_autonomous_community_export_repositories()
    )
    catastro_client = DummyCatastroClientService()
    catastro_cache_repo = DummyCatastroMunicipalityAggregateCacheRepository()
    catastro_aggregate_cache_repo = DummyCatastroTerritorialAggregateCacheRepository()
    ingestion_repo = DummyIngestionRepository()

    # seed one descendant municipality with a fresh cache entry so no upstream call is needed
    seeded_at = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)
    asyncio.run(
        catastro_cache_repo.upsert_payload(
            provider_family="catastro_urbano_municipality_aggregates",
            municipality_code="33044",
            reference_year="2025",
            payload={
                "reference_year": "2025",
                "province_file_code": "04133",
                "province_label": "Asturias",
                "municipality_option_value": "0043",
                "municipality_label": "Oviedo",
                "indicators": catastro_client.payload["indicators"],
            },
            ttl_seconds=7200,
            metadata=catastro_client.payload["metadata"],
            now=seeded_at,
        )
    )
    territorial_repo.descendants_by_unit_id[2] = [
        {
            "territorial_unit_id": 33044,
            "municipality_code": "33044",
            "canonical_name": "Oviedo",
            "display_name": "Oviedo",
            "province_territorial_unit_id": 33,
            "province_code": "33",
            "province_canonical_name": "Asturias",
            "province_display_name": "Asturias",
            "autonomous_community_territorial_unit_id": 2,
            "autonomous_community_code": "03",
            "autonomous_community_canonical_name": "Asturias",
        }
    ]

    service = _build_service(
        territorial_repo,
        series_repo,
        artifact_repo,
        snapshot_repo,
        catastro_client=catastro_client,
        catastro_cache_repo=catastro_cache_repo,
        catastro_aggregate_cache_repo=catastro_aggregate_cache_repo,
        ingestion_repo=ingestion_repo,
    )

    result = asyncio.run(
        service.build_export(
            job_id="job-export-catastro-territorial-1",
            unit_level="autonomous_community",
            code_value="03",
            include_providers=["territorial", "ine", "catastro"],
        )
    )

    assert result is not None
    artifact = asyncio.run(artifact_repo.get_by_export_id(result.export_id))
    assert artifact is not None
    # no upstream calls needed because municipality was already cached
    assert catastro_client.calls == []
    assert catastro_aggregate_cache_repo.upsert_calls == 1

    with ZipFile(io.BytesIO(artifact["payload_bytes"])) as archive:
        names = sorted(archive.namelist())
        assert "datasets/catastro_autonomous_community_aggregates.json" in names
        manifest = json.loads(archive.read("manifest.json"))
        datasets_by_key = {item["dataset_key"]: item for item in manifest["datasets"]}
        assert datasets_by_key["catastro_autonomous_community_aggregates"]["applicable"] is True
        catastro_payload = json.loads(
            archive.read("datasets/catastro_autonomous_community_aggregates.json").decode("utf-8")
        )
        assert catastro_payload["source"] == "catastro.autonomous_community.aggregates"
        assert catastro_payload["filters"]["unit_level"] == "autonomous_community"
        assert catastro_payload["summary"]["municipalities_expected"] == 1
        assert catastro_payload["summary"]["municipalities_included"] == 1
        assert catastro_payload["metadata"]["cache_status"] == "miss"


def test_build_export_adds_catastro_territorial_aggregate_for_province():
    territorial_repo, series_repo, artifact_repo, snapshot_repo = (
        _build_autonomous_community_export_repositories()
    )
    # swap to province-level unit
    province_detail = _territorial_detail(
        unit_id=33,
        parent_id=2,
        unit_level="province",
        canonical_name="Asturias",
        display_name="Asturias",
        code_type="province",
        code_value="33",
    )
    territorial_repo.detail_by_canonical_code[("province", "33")] = province_detail
    territorial_repo.detail_by_id[33] = province_detail
    territorial_repo.hierarchy_by_unit_id[33] = [
        _territorial_summary(
            unit_id=1,
            parent_id=None,
            unit_level="country",
            canonical_name="Espana",
            display_name="Espana",
            code_type="alpha2",
            code_value="ES",
            source_system="iso3166",
        ),
        _territorial_summary(
            unit_id=2,
            parent_id=1,
            unit_level="autonomous_community",
            canonical_name="Asturias",
            display_name="Principado de Asturias",
            code_type="autonomous_community",
            code_value="03",
        ),
        _territorial_summary(
            unit_id=33,
            parent_id=2,
            unit_level="province",
            canonical_name="Asturias",
            display_name="Asturias",
            code_type="province",
            code_value="33",
        ),
    ]

    catastro_client = DummyCatastroClientService()
    catastro_cache_repo = DummyCatastroMunicipalityAggregateCacheRepository()
    catastro_aggregate_cache_repo = DummyCatastroTerritorialAggregateCacheRepository()
    ingestion_repo = DummyIngestionRepository()

    seeded_at = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)
    asyncio.run(
        catastro_cache_repo.upsert_payload(
            provider_family="catastro_urbano_municipality_aggregates",
            municipality_code="33044",
            reference_year="2025",
            payload={
                "reference_year": "2025",
                "province_file_code": "04133",
                "province_label": "Asturias",
                "municipality_option_value": "0043",
                "municipality_label": "Oviedo",
                "indicators": catastro_client.payload["indicators"],
            },
            ttl_seconds=7200,
            metadata=catastro_client.payload["metadata"],
            now=seeded_at,
        )
    )
    territorial_repo.descendants_by_unit_id[33] = [
        {
            "territorial_unit_id": 33044,
            "municipality_code": "33044",
            "canonical_name": "Oviedo",
            "display_name": "Oviedo",
            "province_territorial_unit_id": 33,
            "province_code": "33",
            "province_canonical_name": "Asturias",
            "province_display_name": "Asturias",
            "autonomous_community_territorial_unit_id": 2,
            "autonomous_community_code": "03",
            "autonomous_community_canonical_name": "Asturias",
        }
    ]

    service = _build_service(
        territorial_repo,
        series_repo,
        artifact_repo,
        snapshot_repo,
        catastro_client=catastro_client,
        catastro_cache_repo=catastro_cache_repo,
        catastro_aggregate_cache_repo=catastro_aggregate_cache_repo,
        ingestion_repo=ingestion_repo,
    )

    result = asyncio.run(
        service.build_export(
            job_id="job-export-catastro-province-1",
            unit_level="province",
            code_value="33",
            include_providers=["territorial", "catastro"],
        )
    )

    assert result is not None
    artifact = asyncio.run(artifact_repo.get_by_export_id(result.export_id))
    assert artifact is not None
    assert catastro_client.calls == []
    assert catastro_aggregate_cache_repo.upsert_calls == 1

    with ZipFile(io.BytesIO(artifact["payload_bytes"])) as archive:
        names = sorted(archive.namelist())
        assert "datasets/catastro_province_aggregates.json" in names
        catastro_payload = json.loads(
            archive.read("datasets/catastro_province_aggregates.json").decode("utf-8")
        )
        assert catastro_payload["source"] == "catastro.province.aggregates"
        assert catastro_payload["filters"]["unit_level"] == "province"
        assert catastro_payload["summary"]["municipalities_included"] == 1


def test_build_export_reuses_catastro_territorial_aggregate_cache():
    territorial_repo, series_repo, artifact_repo, snapshot_repo = (
        _build_autonomous_community_export_repositories()
    )
    catastro_client = DummyCatastroClientService()
    catastro_aggregate_cache_repo = DummyCatastroTerritorialAggregateCacheRepository()
    ingestion_repo = DummyIngestionRepository()

    # pre-seed the territorial aggregate cache so no aggregation is needed
    seeded_at = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)
    cached_payload = {
        "source": "catastro.autonomous_community.aggregates",
        "generated_at": seeded_at.isoformat(),
        "territorial_context": {},
        "filters": {
            "reference_year": "2025",
            "unit_level": "autonomous_community",
            "code_value": "03",
        },
        "summary": {
            "municipalities_expected": 1,
            "municipalities_included": 1,
            "municipalities_missing": 0,
            "coverage_ratio": 1.0,
            "reference_year": "2025",
            "parcelas_urbanas": 23783,
            "bienes_inmuebles": 243583,
            "valor_catastral_total_miles_euros": 14550865.45,
        },
        "series": catastro_client.payload["indicators"],
        "metadata": {
            "provider": "catastro",
            "provider_family": "catastro_urbano",
            "coverage": "autonomous_community",
            "coverage_status": "complete",
            "cache_status": "miss",
            "cache_hits": 1,
            "live_fetches": 0,
            "missing_municipality_codes": [],
            "missing_municipality_names": [],
            "reference_year": "2025",
        },
    }
    asyncio.run(
        catastro_aggregate_cache_repo.upsert_payload(
            provider_family="catastro_urbano_territorial_aggregates",
            unit_level="autonomous_community",
            code_value="03",
            reference_year="2025",
            payload=cached_payload,
            ttl_seconds=7200,
            metadata={"provider": "catastro", "coverage_status": "complete"},
            now=seeded_at,
        )
    )
    upsert_calls_after_seed = catastro_aggregate_cache_repo.upsert_calls

    service = _build_service(
        territorial_repo,
        series_repo,
        artifact_repo,
        snapshot_repo,
        catastro_client=catastro_client,
        catastro_aggregate_cache_repo=catastro_aggregate_cache_repo,
        ingestion_repo=ingestion_repo,
    )

    result = asyncio.run(
        service.build_export(
            job_id="job-export-catastro-territorial-cache",
            unit_level="autonomous_community",
            code_value="03",
            include_providers=["territorial", "catastro"],
        )
    )

    assert result is not None
    assert catastro_client.calls == []
    assert catastro_aggregate_cache_repo.upsert_calls == upsert_calls_after_seed

    artifact = asyncio.run(artifact_repo.get_by_export_id(result.export_id))
    assert artifact is not None
    with ZipFile(io.BytesIO(artifact["payload_bytes"])) as archive:
        catastro_payload = json.loads(
            archive.read("datasets/catastro_autonomous_community_aggregates.json").decode("utf-8")
        )
        assert catastro_payload["metadata"]["cache_status"] == "hit"


def test_build_export_marks_catastro_territorial_aggregate_as_partial_when_some_municipalities_fail():
    from app.services.catastro_client import CatastroUpstreamError

    territorial_repo, series_repo, artifact_repo, snapshot_repo = (
        _build_autonomous_community_export_repositories()
    )
    catastro_client = DummyCatastroClientService()
    catastro_client.raise_error = CatastroUpstreamError(
        status_code=503,
        detail={
            "message": "The Catastro service is temporarily unavailable.",
            "path": "/jaxi/tabla.do",
            "retryable": True,
        },
    )
    catastro_cache_repo = DummyCatastroMunicipalityAggregateCacheRepository()
    catastro_aggregate_cache_repo = DummyCatastroTerritorialAggregateCacheRepository()
    ingestion_repo = DummyIngestionRepository()

    seeded_at = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)
    asyncio.run(
        catastro_cache_repo.upsert_payload(
            provider_family="catastro_urbano_municipality_aggregates",
            municipality_code="33044",
            reference_year="2025",
            payload={
                "reference_year": "2025",
                "province_file_code": "04133",
                "province_label": "Asturias",
                "municipality_option_value": "0043",
                "municipality_label": "Oviedo",
                "indicators": catastro_client.payload["indicators"],
            },
            ttl_seconds=7200,
            metadata=catastro_client.payload["metadata"],
            now=seeded_at,
        )
    )
    territorial_repo.descendants_by_unit_id[2] = [
        {
            "territorial_unit_id": 33044,
            "municipality_code": "33044",
            "canonical_name": "Oviedo",
            "display_name": "Oviedo",
            "province_territorial_unit_id": 33,
            "province_code": "33",
            "province_canonical_name": "Asturias",
            "province_display_name": "Asturias",
            "autonomous_community_territorial_unit_id": 2,
            "autonomous_community_code": "03",
            "autonomous_community_canonical_name": "Asturias",
        },
        {
            "territorial_unit_id": 33024,
            "municipality_code": "33024",
            "canonical_name": "Gijon",
            "display_name": "Gijon",
            "province_territorial_unit_id": 33,
            "province_code": "33",
            "province_canonical_name": "Asturias",
            "province_display_name": "Asturias",
            "autonomous_community_territorial_unit_id": 2,
            "autonomous_community_code": "03",
            "autonomous_community_canonical_name": "Asturias",
        },
    ]

    service = _build_service(
        territorial_repo,
        series_repo,
        artifact_repo,
        snapshot_repo,
        catastro_client=catastro_client,
        catastro_cache_repo=catastro_cache_repo,
        catastro_aggregate_cache_repo=catastro_aggregate_cache_repo,
        ingestion_repo=ingestion_repo,
    )

    result = asyncio.run(
        service.build_export(
            job_id="job-export-catastro-territorial-partial",
            unit_level="autonomous_community",
            code_value="03",
            include_providers=["territorial", "catastro"],
        )
    )

    assert result is not None
    assert result.summary["provider_coverage"]["catastro"]["coverage_status"] == "partial"
    assert result.summary["provider_coverage"]["catastro"]["municipalities_expected"] == 2
    assert result.summary["provider_coverage"]["catastro"]["municipalities_included"] == 1
    assert result.summary["provider_coverage"]["catastro"]["municipalities_missing"] == 1

    artifact = asyncio.run(artifact_repo.get_by_export_id(result.export_id))
    assert artifact is not None
    with ZipFile(io.BytesIO(artifact["payload_bytes"])) as archive:
        catastro_payload = json.loads(
            archive.read("datasets/catastro_autonomous_community_aggregates.json").decode("utf-8")
        )
        assert catastro_payload["summary"]["municipalities_expected"] == 2
        assert catastro_payload["summary"]["municipalities_included"] == 1
        assert catastro_payload["summary"]["municipalities_missing"] == 1
        assert catastro_payload["metadata"]["coverage_status"] == "partial"
        assert catastro_payload["metadata"]["missing_municipality_codes"] == ["33024"]
        assert catastro_payload["metadata"]["missing_municipality_names"] == ["Gijon"]


def test_build_export_fails_when_catastro_territorial_aggregate_has_zero_coverage():
    from app.services.catastro_client import CatastroUpstreamError

    territorial_repo, series_repo, artifact_repo, snapshot_repo = (
        _build_autonomous_community_export_repositories()
    )
    catastro_client = DummyCatastroClientService()
    catastro_client.raise_error = CatastroUpstreamError(
        status_code=503,
        detail={
            "message": "The Catastro service is temporarily unavailable.",
            "path": "/jaxi/tabla.do",
            "retryable": True,
        },
    )
    territorial_repo.descendants_by_unit_id[2] = [
        {
            "territorial_unit_id": 33024,
            "municipality_code": "33024",
            "canonical_name": "Gijon",
            "display_name": "Gijon",
            "province_territorial_unit_id": 33,
            "province_code": "33",
            "province_canonical_name": "Asturias",
            "province_display_name": "Asturias",
            "autonomous_community_territorial_unit_id": 2,
            "autonomous_community_code": "03",
            "autonomous_community_canonical_name": "Asturias",
        }
    ]

    service = _build_service(
        territorial_repo,
        series_repo,
        artifact_repo,
        snapshot_repo,
        catastro_client=catastro_client,
    )

    with pytest.raises(CatastroUpstreamError) as exc_info:
        asyncio.run(
            service.build_export(
                job_id="job-export-catastro-territorial-zero-coverage",
                unit_level=TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
                code_value="03",
                include_providers=["territorial", "catastro"],
            )
        )

    assert exc_info.value.detail["message"] == (
        "Catastro aggregates could not be built from municipality coverage."
    )


def test_build_export_reuses_catastro_cache_without_upstream_call():
    territorial_repo, series_repo, artifact_repo, snapshot_repo = (
        _build_municipality_export_repositories()
    )
    catastro_client = DummyCatastroClientService()
    catastro_cache_repo = DummyCatastroMunicipalityAggregateCacheRepository()
    ingestion_repo = DummyIngestionRepository()
    seeded_at = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)
    asyncio.run(
        catastro_cache_repo.upsert_payload(
            provider_family="catastro_urbano_municipality_aggregates",
            municipality_code="33044",
            reference_year="2025",
            payload={
                "reference_year": "2025",
                "province_file_code": "04133",
                "province_label": "Asturias",
                "municipality_option_value": "0043",
                "municipality_label": "Oviedo",
                "indicators": catastro_client.payload["indicators"],
            },
            ttl_seconds=7200,
            metadata=catastro_client.payload["metadata"],
            now=seeded_at,
        )
    )
    upsert_calls_after_seed = catastro_cache_repo.upsert_calls
    service = _build_service(
        territorial_repo,
        series_repo,
        artifact_repo,
        snapshot_repo,
        catastro_client=catastro_client,
        catastro_cache_repo=catastro_cache_repo,
        ingestion_repo=ingestion_repo,
    )

    result = asyncio.run(
        service.build_export(
            job_id="job-export-catastro-cache",
            unit_level="municipality",
            code_value="33044",
            include_providers=["territorial", "catastro"],
        )
    )

    assert result is not None
    assert catastro_client.calls == []
    assert ingestion_repo.records == []
    assert catastro_cache_repo.upsert_calls == upsert_calls_after_seed
    artifact = asyncio.run(artifact_repo.get_by_export_id(result.export_id))
    assert artifact is not None
    with ZipFile(io.BytesIO(artifact["payload_bytes"])) as archive:
        catastro_payload = json.loads(
            archive.read("datasets/catastro_municipality_aggregates.json").decode("utf-8")
        )
        assert catastro_payload["metadata"]["cache_status"] == "hit"


def test_build_export_reuses_fresh_artifact():
    territorial_repo, series_repo, artifact_repo, snapshot_repo = (
        _build_municipality_export_repositories()
    )
    service = _build_service(territorial_repo, series_repo, artifact_repo, snapshot_repo)

    first = asyncio.run(
        service.build_export(
            job_id="job-export-1",
            unit_level="municipality",
            code_value="33044",
            include_providers=["territorial", "ine", "analytics"],
        )
    )
    second = asyncio.run(
        service.build_export(
            job_id="job-export-2",
            unit_level="municipality",
            code_value="33044",
            include_providers=["analytics", "territorial", "ine"],
        )
    )

    assert first is not None
    assert second is not None
    assert first.export_key == second.export_key
    assert second.summary["artifact_reused"] is True
    assert artifact_repo.upsert_calls == 1


def test_build_export_returns_none_when_territorial_unit_is_unknown():
    service = _build_service(
        DummyTerritorialRepository(),
        DummySeriesRepository(),
        DummyTerritorialExportArtifactRepository(),
        DummyAnalyticalSnapshotRepository(),
    )

    result = asyncio.run(
        service.build_export(
            job_id="job-export-missing",
            unit_level="municipality",
            code_value="99999",
            include_providers=["territorial", "ine", "analytics"],
        )
    )

    assert result is None
