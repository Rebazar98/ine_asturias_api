from __future__ import annotations

import io
import json
import re
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

from app.core.logging import get_logger
from app.repositories.series import SeriesRepository
from app.repositories.territorial import (
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TerritorialRepository,
)
from app.repositories.territorial_export_artifacts import (
    TerritorialExportArtifactRepository,
    build_export_key,
    normalize_export_provider_keys,
)
from app.schemas import (
    AnalyticalTerritorialContextResponse,
    TerritorialExportResultResponse,
    TerritorialMunicipalityReportResponse,
    TerritorialMunicipalitySummaryResponse,
    TerritorialUnitDetailResponse,
    TerritorialUnitSummaryResponse,
)
from app.services.territorial_analytics import TerritorialAnalyticsService


ProgressReporter = Callable[[dict[str, object]], Awaitable[None]]
TERRITORIAL_EXPORT_SOURCE = "internal.export.territorial_bundle"
TERRITORIAL_EXPORT_JOB_TYPE = "territorial_export"
TERRITORIAL_EXPORT_FORMAT = "zip"
EXPORT_PROVIDER_TERRITORIAL = "territorial"
EXPORT_PROVIDER_INE = "ine"
EXPORT_PROVIDER_ANALYTICS = "analytics"
EXPORT_DEFAULT_PROVIDERS = [
    EXPORT_PROVIDER_TERRITORIAL,
    EXPORT_PROVIDER_INE,
    EXPORT_PROVIDER_ANALYTICS,
]
EXPORT_SUPPORTED_UNIT_LEVELS = [
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
]
EXPORT_INE_PAGE_SIZE = 1000
EXPORT_ANALYTICS_PAGE_SIZE = 10000


@dataclass(slots=True)
class ExportDataset:
    dataset_key: str
    provider: str
    relative_path: str | None
    content_type: str
    record_count: int
    applicable: bool
    payload_bytes: bytes | None = None

    def manifest_entry(self) -> dict[str, Any]:
        return {
            "dataset_key": self.dataset_key,
            "provider": self.provider,
            "relative_path": self.relative_path,
            "record_count": self.record_count,
            "content_type": self.content_type,
            "applicable": self.applicable,
        }


@dataclass(slots=True)
class ExportContext:
    unit: TerritorialUnitDetailResponse
    hierarchy: list[TerritorialUnitSummaryResponse]
    territorial_context: AnalyticalTerritorialContextResponse


class TerritorialExportProvider(ABC):
    provider_key: str

    @abstractmethod
    def supports(self, unit_level: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def build_datasets(
        self,
        *,
        context: ExportContext,
    ) -> list[ExportDataset]:
        raise NotImplementedError

    def build_not_applicable_datasets(self, *, context: ExportContext) -> list[ExportDataset]:
        return []


class TerritorialMetadataExportProvider(TerritorialExportProvider):
    provider_key = EXPORT_PROVIDER_TERRITORIAL

    def supports(self, unit_level: str) -> bool:
        return unit_level in EXPORT_SUPPORTED_UNIT_LEVELS

    async def build_datasets(self, *, context: ExportContext) -> list[ExportDataset]:
        unit_payload = context.unit.model_dump(mode="json")
        hierarchy_payload = [item.model_dump(mode="json") for item in context.hierarchy]
        return [
            ExportDataset(
                dataset_key="territorial_unit",
                provider=self.provider_key,
                relative_path="datasets/territorial_unit.json",
                content_type="application/json",
                record_count=1,
                applicable=True,
                payload_bytes=_json_bytes(unit_payload),
            ),
            ExportDataset(
                dataset_key="territorial_hierarchy",
                provider=self.provider_key,
                relative_path="datasets/territorial_hierarchy.json",
                content_type="application/json",
                record_count=len(hierarchy_payload),
                applicable=True,
                payload_bytes=_json_bytes(hierarchy_payload),
            ),
        ]


class INESeriesExportProvider(TerritorialExportProvider):
    provider_key = EXPORT_PROVIDER_INE

    def __init__(self, series_repo: SeriesRepository) -> None:
        self.series_repo = series_repo

    def supports(self, unit_level: str) -> bool:
        return unit_level in EXPORT_SUPPORTED_UNIT_LEVELS

    async def build_datasets(self, *, context: ExportContext) -> list[ExportDataset]:
        canonical_code = context.unit.canonical_code
        geography_code = canonical_code.code_value if canonical_code is not None else ""
        if not geography_code:
            raise ValueError("Territorial export requires a canonical INE code.")

        payload_buffer = io.BytesIO()
        page = 1
        total_records = 0
        while True:
            page_payload = await self.series_repo.list_normalized(
                geography_code=geography_code,
                geography_code_system="ine",
                page=page,
                page_size=EXPORT_INE_PAGE_SIZE,
            )
            items = page_payload["items"]
            for item in items:
                payload_buffer.write(_ndjson_line(item))
            total_records += len(items)
            if not page_payload["has_next"]:
                break
            page += 1

        return [
            ExportDataset(
                dataset_key="ine_series",
                provider=self.provider_key,
                relative_path="datasets/ine_series.ndjson",
                content_type="application/x-ndjson",
                record_count=total_records,
                applicable=True,
                payload_bytes=payload_buffer.getvalue(),
            )
        ]


class AnalyticsTerritorialExportProvider(TerritorialExportProvider):
    provider_key = EXPORT_PROVIDER_ANALYTICS

    def __init__(self, analytics_service: TerritorialAnalyticsService) -> None:
        self.analytics_service = analytics_service

    def supports(self, unit_level: str) -> bool:
        return unit_level == TERRITORIAL_UNIT_LEVEL_MUNICIPALITY

    async def build_datasets(self, *, context: ExportContext) -> list[ExportDataset]:
        municipality_code = context.territorial_context.municipality_code
        if not municipality_code:
            raise ValueError("Municipality analytics export requires municipality_code.")

        summary = await self.analytics_service.build_municipality_summary(
            municipality_code=municipality_code,
            page=1,
            page_size=EXPORT_ANALYTICS_PAGE_SIZE,
            emit_observability=False,
        )
        report = await self.analytics_service.build_municipality_report(
            municipality_code=municipality_code,
            page=1,
            page_size=EXPORT_ANALYTICS_PAGE_SIZE,
        )
        if summary is None or report is None:
            raise ValueError("Municipality analytics export could not be built.")

        return [
            self._build_json_dataset(
                dataset_key="analytics_municipality_summary",
                relative_path="datasets/analytics_municipality_summary.json",
                payload=summary,
            ),
            self._build_json_dataset(
                dataset_key="analytics_municipality_report",
                relative_path="datasets/analytics_municipality_report.json",
                payload=report,
            ),
        ]

    def build_not_applicable_datasets(self, *, context: ExportContext) -> list[ExportDataset]:
        return [
            ExportDataset(
                dataset_key="analytics_municipality_summary",
                provider=self.provider_key,
                relative_path=None,
                content_type="application/json",
                record_count=0,
                applicable=False,
            ),
            ExportDataset(
                dataset_key="analytics_municipality_report",
                provider=self.provider_key,
                relative_path=None,
                content_type="application/json",
                record_count=0,
                applicable=False,
            ),
        ]

    def _build_json_dataset(
        self,
        *,
        dataset_key: str,
        relative_path: str,
        payload: TerritorialMunicipalitySummaryResponse | TerritorialMunicipalityReportResponse,
    ) -> ExportDataset:
        return ExportDataset(
            dataset_key=dataset_key,
            provider=self.provider_key,
            relative_path=relative_path,
            content_type="application/json",
            record_count=1,
            applicable=True,
            payload_bytes=_json_bytes(payload.model_dump(mode="json")),
        )


class TerritorialExportService:
    def __init__(
        self,
        *,
        territorial_repo: TerritorialRepository,
        series_repo: SeriesRepository,
        analytics_service: TerritorialAnalyticsService,
        artifact_repo: TerritorialExportArtifactRepository,
        export_ttl_seconds: int,
        now_factory: Callable[[], datetime] | None = None,
    ) -> None:
        self.territorial_repo = territorial_repo
        self.series_repo = series_repo
        self.analytics_service = analytics_service
        self.artifact_repo = artifact_repo
        self.export_ttl_seconds = max(0, export_ttl_seconds)
        self.now_factory = now_factory or (lambda: datetime.now(timezone.utc))
        self.logger = get_logger("app.services.territorial_exports")
        self.providers: dict[str, TerritorialExportProvider] = {
            EXPORT_PROVIDER_TERRITORIAL: TerritorialMetadataExportProvider(),
            EXPORT_PROVIDER_INE: INESeriesExportProvider(series_repo=series_repo),
            EXPORT_PROVIDER_ANALYTICS: AnalyticsTerritorialExportProvider(
                analytics_service=analytics_service
            ),
        }

    async def build_export(
        self,
        *,
        job_id: str,
        unit_level: str,
        code_value: str,
        artifact_format: str = TERRITORIAL_EXPORT_FORMAT,
        include_providers: Sequence[str] | None = None,
        progress_reporter: ProgressReporter | None = None,
    ) -> TerritorialExportResultResponse | None:
        normalized_providers = normalize_export_provider_keys(list(include_providers or []))
        if artifact_format != TERRITORIAL_EXPORT_FORMAT:
            raise ValueError("Unsupported export format.")
        if not normalized_providers:
            raise ValueError("At least one export provider is required.")

        if progress_reporter is not None:
            await progress_reporter(
                {
                    "stage": "resolving_territorial_unit",
                    "unit_level": unit_level,
                    "code_value": code_value,
                }
            )

        unit_payload = await self.territorial_repo.get_unit_detail_by_canonical_code(
            unit_level=unit_level,
            code_value=code_value,
        )
        if unit_payload is None:
            return None

        unit = TerritorialUnitDetailResponse(**unit_payload)
        hierarchy_payload = await self.territorial_repo.list_hierarchy(unit.id)
        hierarchy = [TerritorialUnitSummaryResponse(**item) for item in hierarchy_payload]
        context = ExportContext(
            unit=unit,
            hierarchy=hierarchy,
            territorial_context=self._build_territorial_context(unit, hierarchy),
        )
        export_key = build_export_key(
            unit_level=unit_level,
            code_value=code_value,
            artifact_format=artifact_format,
            include_providers=normalized_providers,
        )

        if progress_reporter is not None:
            await progress_reporter(
                {
                    "stage": "checking_export_artifact",
                    "export_key": export_key,
                    "unit_level": unit_level,
                    "code_value": code_value,
                }
            )

        cached_artifact = await self.artifact_repo.get_fresh_artifact(
            unit_level=unit_level,
            code_value=code_value,
            artifact_format=artifact_format,
            include_providers=normalized_providers,
            now=self.now_factory(),
        )
        if cached_artifact is not None:
            self.logger.info(
                "territorial_export_artifact_reused",
                extra={
                    "job_id": job_id,
                    "export_key": export_key,
                    "unit_level": unit_level,
                    "code_value": code_value,
                },
            )
            return self._build_public_result(
                artifact=cached_artifact,
                job_id=job_id,
                artifact_reused=True,
            )

        datasets: list[ExportDataset] = []
        for provider_key in normalized_providers:
            provider = self.providers[provider_key]
            if progress_reporter is not None:
                await progress_reporter(
                    {
                        "stage": "building_provider_dataset",
                        "provider": provider_key,
                        "unit_level": unit_level,
                        "code_value": code_value,
                    }
                )
            if provider.supports(unit_level):
                datasets.extend(await provider.build_datasets(context=context))
            else:
                datasets.extend(provider.build_not_applicable_datasets(context=context))

        generated_at = self.now_factory()
        manifest = self._build_manifest(
            generated_at=generated_at,
            context=context,
            export_key=export_key,
            providers_requested=normalized_providers,
            datasets=datasets,
        )

        if progress_reporter is not None:
            await progress_reporter(
                {
                    "stage": "packaging_export_bundle",
                    "export_key": export_key,
                    "datasets_total": len(datasets),
                }
            )

        archive_bytes = self._build_zip_bundle(manifest=manifest, datasets=datasets)
        filename = self._build_filename(unit_level=unit_level, code_value=code_value)
        artifact = await self.artifact_repo.upsert_artifact(
            territorial_unit_id=unit.id,
            unit_level=unit_level,
            code_value=code_value,
            artifact_format=artifact_format,
            content_type="application/zip",
            filename=filename,
            payload_bytes=archive_bytes,
            ttl_seconds=self.export_ttl_seconds,
            include_providers=normalized_providers,
            metadata=manifest,
            now=generated_at,
        )
        if artifact is None:
            raise RuntimeError("Territorial export artifact storage is unavailable.")

        self.logger.info(
            "territorial_export_artifact_built",
            extra={
                "job_id": job_id,
                "export_key": export_key,
                "unit_level": unit_level,
                "code_value": code_value,
                "datasets_total": len(datasets),
                "byte_size": artifact["byte_size"],
            },
        )
        return self._build_public_result(
            artifact=artifact,
            job_id=job_id,
            artifact_reused=False,
        )

    def _build_manifest(
        self,
        *,
        generated_at: datetime,
        context: ExportContext,
        export_key: str,
        providers_requested: list[str],
        datasets: list[ExportDataset],
    ) -> dict[str, Any]:
        providers_included = sorted(
            {
                dataset.provider
                for dataset in datasets
                if dataset.applicable
                and dataset.relative_path
                and dataset.payload_bytes is not None
            }
        )
        applicable_datasets = [dataset for dataset in datasets if dataset.applicable]
        return {
            "source": TERRITORIAL_EXPORT_SOURCE,
            "generated_at": generated_at.isoformat(),
            "territorial_context": context.territorial_context.model_dump(mode="json"),
            "providers_requested": providers_requested,
            "providers_included": providers_included,
            "datasets": [dataset.manifest_entry() for dataset in datasets],
            "summary": {
                "datasets_total": len(datasets),
                "datasets_written": len([dataset for dataset in datasets if dataset.payload_bytes]),
                "records_total": sum(dataset.record_count for dataset in applicable_datasets),
            },
            "metadata": {
                "format": TERRITORIAL_EXPORT_FORMAT,
                "export_key": export_key,
                "future_provider_extension": "additive",
            },
        }

    def _build_public_result(
        self,
        *,
        artifact: dict[str, Any],
        job_id: str,
        artifact_reused: bool,
    ) -> TerritorialExportResultResponse:
        manifest = dict(artifact.get("metadata") or {})
        summary = dict(manifest.get("summary") or {})
        summary["artifact_reused"] = artifact_reused
        summary["providers_requested"] = list(manifest.get("providers_requested") or [])
        summary["providers_included"] = list(manifest.get("providers_included") or [])
        summary["byte_size"] = artifact["byte_size"]
        return TerritorialExportResultResponse(
            export_id=artifact["export_id"],
            export_key=artifact["export_key"],
            format=artifact["artifact_format"],
            territorial_context=AnalyticalTerritorialContextResponse(
                **(manifest.get("territorial_context") or {})
            ),
            summary=summary,
            download_path=f"/territorios/exports/{job_id}/download",
            expires_at=artifact["expires_at"],
        )

    @staticmethod
    def _build_zip_bundle(*, manifest: dict[str, Any], datasets: list[ExportDataset]) -> bytes:
        buffer = io.BytesIO()
        with ZipFile(buffer, mode="w", compression=ZIP_DEFLATED) as archive:
            archive.writestr("manifest.json", _json_text(manifest))
            for dataset in datasets:
                if (
                    not dataset.applicable
                    or not dataset.relative_path
                    or dataset.payload_bytes is None
                ):
                    continue
                archive.writestr(dataset.relative_path, dataset.payload_bytes)
        return buffer.getvalue()

    @staticmethod
    def _build_territorial_context(
        unit: TerritorialUnitDetailResponse,
        hierarchy: list[TerritorialUnitSummaryResponse],
    ) -> AnalyticalTerritorialContextResponse:
        by_level = {item.unit_level: item for item in hierarchy}
        canonical_code = unit.canonical_code
        return AnalyticalTerritorialContextResponse(
            territorial_unit_id=unit.id,
            unit_level=unit.unit_level,
            canonical_code=canonical_code.code_value if canonical_code else None,
            canonical_name=unit.canonical_name,
            display_name=unit.display_name,
            source_system=canonical_code.source_system if canonical_code else None,
            country_code=unit.country_code,
            autonomous_community_code=_canonical_code_value(by_level.get("autonomous_community")),
            province_code=_canonical_code_value(by_level.get("province")),
            municipality_code=_canonical_code_value(by_level.get("municipality")),
        )

    @staticmethod
    def _build_filename(*, unit_level: str, code_value: str) -> str:
        safe_code = re.sub(r"[^a-zA-Z0-9._-]+", "_", code_value.strip()) or "unknown"
        return f"territorial_export_{unit_level}_{safe_code}.zip"


def _canonical_code_value(unit: TerritorialUnitSummaryResponse | None) -> str | None:
    if unit is None or unit.canonical_code is None:
        return None
    return unit.canonical_code.code_value


def _json_bytes(value: Any) -> bytes:
    return _json_text(value).encode("utf-8")


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, default=str)


def _ndjson_line(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, default=str) + "\n").encode(
        "utf-8"
    )
