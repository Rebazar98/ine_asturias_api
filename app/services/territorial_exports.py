from __future__ import annotations

import asyncio
import io
import json
import re
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

from app.core.logging import get_logger
from app.core.metrics import record_provider_cache_hit
from app.repositories.catastro_cache import (
    CATASTRO_PROVIDER_FAMILY_URBANO,
    CATASTRO_PROVIDER_FAMILY_URBANO_TERRITORIAL,
    CatastroMunicipalityAggregateCacheRepository,
    CatastroTerritorialAggregateCacheRepository,
)
from app.repositories.ingestion import IngestionRepository
from app.repositories.series import SeriesRepository
from app.repositories.territorial import (
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
    normalize_territorial_name,
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
from app.services.catastro_client import (
    CatastroClientError,
    CatastroClientService,
    CatastroSelectionError,
    CatastroUpstreamError,
)
from app.services.territorial_analytics import TerritorialAnalyticsService


ProgressReporter = Callable[[dict[str, object]], Awaitable[None]]
TERRITORIAL_EXPORT_SOURCE = "internal.export.territorial_bundle"
TERRITORIAL_EXPORT_JOB_TYPE = "territorial_export"
TERRITORIAL_EXPORT_FORMAT = "zip"
EXPORT_PROVIDER_TERRITORIAL = "territorial"
EXPORT_PROVIDER_INE = "ine"
EXPORT_PROVIDER_ANALYTICS = "analytics"
EXPORT_PROVIDER_CATASTRO = "catastro"
EXPORT_DEFAULT_PROVIDERS = [
    EXPORT_PROVIDER_TERRITORIAL,
    EXPORT_PROVIDER_INE,
    EXPORT_PROVIDER_ANALYTICS,
]
EXPORT_SUPPORTED_UNIT_LEVELS = [
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
]
EXPORT_INE_PAGE_SIZE = 1000
EXPORT_ANALYTICS_PAGE_SIZE = 10000
CATASTRO_EXPORT_SOURCE_BY_LEVEL = {
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY: "catastro.municipality.aggregates",
    TERRITORIAL_UNIT_LEVEL_PROVINCE: "catastro.province.aggregates",
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY: "catastro.autonomous_community.aggregates",
}
CATASTRO_DATASET_PATH_BY_LEVEL = {
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY: "datasets/catastro_municipality_aggregates.json",
    TERRITORIAL_UNIT_LEVEL_PROVINCE: "datasets/catastro_province_aggregates.json",
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY: (
        "datasets/catastro_autonomous_community_aggregates.json"
    ),
}
CATASTRO_DATASET_KEY_BY_LEVEL = {
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY: "catastro_municipality_aggregates",
    TERRITORIAL_UNIT_LEVEL_PROVINCE: "catastro_province_aggregates",
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY: "catastro_autonomous_community_aggregates",
}
CATASTRO_SUMMABLE_SERIES_KEYS = (
    "catastro_urbano.urban_parcels",
    "catastro_urbano.urban_parcel_area_hectares",
    "catastro_urbano.real_estate_assets",
    "catastro_urbano.cadastral_construction_value",
    "catastro_urbano.cadastral_land_value",
    "catastro_urbano.cadastral_total_value",
)
CATASTRO_LAST_VALUATION_SERIES_KEY = "catastro_urbano.last_valuation_year"


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


class CatastroExportProvider(TerritorialExportProvider):
    provider_key = EXPORT_PROVIDER_CATASTRO

    def __init__(
        self,
        *,
        territorial_repo: TerritorialRepository,
        catastro_client: CatastroClientService,
        catastro_cache_repo: CatastroMunicipalityAggregateCacheRepository,
        catastro_aggregate_cache_repo: CatastroTerritorialAggregateCacheRepository,
        ingestion_repo: IngestionRepository,
        cache_ttl_seconds: int,
        aggregate_cache_ttl_seconds: int,
        aggregate_max_concurrency: int,
        now_factory: Callable[[], datetime],
    ) -> None:
        self.territorial_repo = territorial_repo
        self.catastro_client = catastro_client
        self.catastro_cache_repo = catastro_cache_repo
        self.catastro_aggregate_cache_repo = catastro_aggregate_cache_repo
        self.ingestion_repo = ingestion_repo
        self.cache_ttl_seconds = max(0, cache_ttl_seconds)
        self.aggregate_cache_ttl_seconds = max(0, aggregate_cache_ttl_seconds)
        self.aggregate_max_concurrency = max(1, aggregate_max_concurrency)
        self.now_factory = now_factory
        self.logger = get_logger("app.services.territorial_exports.catastro")

    def supports(self, unit_level: str) -> bool:
        return unit_level in {
            TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
            TERRITORIAL_UNIT_LEVEL_PROVINCE,
            TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
        }

    async def build_datasets(self, *, context: ExportContext) -> list[ExportDataset]:
        if context.unit.unit_level == TERRITORIAL_UNIT_LEVEL_MUNICIPALITY:
            dataset_payload = await self._build_municipality_dataset_payload(context=context)
        else:
            dataset_payload = await self._build_territorial_aggregate_dataset_payload(
                context=context
            )

        unit_level = context.unit.unit_level
        return [
            ExportDataset(
                dataset_key=CATASTRO_DATASET_KEY_BY_LEVEL[unit_level],
                provider=self.provider_key,
                relative_path=CATASTRO_DATASET_PATH_BY_LEVEL[unit_level],
                content_type="application/json",
                record_count=len(dataset_payload.get("series") or []),
                applicable=True,
                payload_bytes=_json_bytes(dataset_payload),
            )
        ]

    async def _build_municipality_dataset_payload(
        self, *, context: ExportContext
    ) -> dict[str, Any]:
        municipality_code = context.territorial_context.municipality_code
        if not municipality_code:
            raise ValueError("Catastro municipality export requires municipality_code.")

        province_candidates = _build_hierarchy_level_name_candidates(
            context.hierarchy,
            TERRITORIAL_UNIT_LEVEL_PROVINCE,
        )
        if not province_candidates:
            raise ValueError("Catastro municipality export requires province context.")

        reference_year = await self.catastro_client.get_reference_year()
        payload, provider_metadata, cache_status = await self._load_municipality_payload(
            municipality_code=municipality_code,
            province_candidates=province_candidates,
            municipality_candidates=_build_unit_name_candidates(context.unit),
            reference_year=reference_year,
        )

        dataset_payload = self._build_dataset_payload(
            context=context,
            code_value=municipality_code,
            payload=payload,
            metadata=provider_metadata,
            cache_status=cache_status,
        )
        dataset_payload["metadata"]["coverage_status"] = "complete"
        dataset_payload["metadata"]["cache_hits"] = 1 if cache_status == "hit" else 0
        dataset_payload["metadata"]["live_fetches"] = 1 if cache_status == "miss" else 0
        dataset_payload["metadata"]["missing_municipality_codes"] = []
        dataset_payload["metadata"]["missing_municipality_names"] = []
        self.logger.info(
            "catastro_export_dataset_built",
            extra={
                "municipality_code": municipality_code,
                "reference_year": payload["reference_year"],
                "cache_status": cache_status,
                "series_count": len(dataset_payload["series"]),
            },
        )
        return dataset_payload

    def build_not_applicable_datasets(self, *, context: ExportContext) -> list[ExportDataset]:
        return []

    async def _build_territorial_aggregate_dataset_payload(
        self,
        *,
        context: ExportContext,
    ) -> dict[str, Any]:
        unit_level = context.unit.unit_level
        canonical_code = (
            context.unit.canonical_code.code_value if context.unit.canonical_code else ""
        )
        if not canonical_code:
            raise ValueError("Catastro territorial aggregate export requires a canonical code.")

        reference_year = await self.catastro_client.get_reference_year()
        cached = await self.catastro_aggregate_cache_repo.get_fresh_payload(
            provider_family=CATASTRO_PROVIDER_FAMILY_URBANO_TERRITORIAL,
            unit_level=unit_level,
            code_value=canonical_code,
            reference_year=reference_year,
            now=self.now_factory(),
        )
        if cached is not None:
            record_provider_cache_hit("catastro", "territorial_aggregates")
            dataset_payload = deepcopy(cached["payload"])
            dataset_payload["generated_at"] = self.now_factory().isoformat()
            dataset_metadata = dict(dataset_payload.get("metadata") or {})
            dataset_metadata["cache_status"] = "hit"
            dataset_payload["metadata"] = dataset_metadata
            return dataset_payload

        descendants = await self.territorial_repo.list_descendant_municipalities(
            unit_level=unit_level,
            territorial_unit_id=context.unit.id,
        )
        if not descendants:
            raise CatastroSelectionError(
                status_code=404,
                detail={
                    "message": "No descendant municipalities were found for the requested unit.",
                    "unit_level": unit_level,
                    "code_value": canonical_code,
                },
            )

        aggregated = await self._aggregate_descendant_municipalities(
            descendants=descendants,
            reference_year=reference_year,
        )
        if aggregated["municipalities_included"] <= 0:
            raise CatastroUpstreamError(
                status_code=503,
                detail={
                    "message": "Catastro aggregates could not be built from municipality coverage.",
                    "unit_level": unit_level,
                    "code_value": canonical_code,
                    "reference_year": reference_year,
                    "retryable": True,
                },
            )

        dataset_payload = {
            "source": CATASTRO_EXPORT_SOURCE_BY_LEVEL[unit_level],
            "generated_at": self.now_factory().isoformat(),
            "territorial_context": context.territorial_context.model_dump(mode="json"),
            "filters": {
                "reference_year": reference_year,
                "unit_level": unit_level,
                "code_value": canonical_code,
            },
            "summary": {
                "municipalities_expected": aggregated["municipalities_expected"],
                "municipalities_included": aggregated["municipalities_included"],
                "municipalities_missing": aggregated["municipalities_missing"],
                "coverage_ratio": aggregated["coverage_ratio"],
                "reference_year": reference_year,
                "parcelas_urbanas": aggregated["summary_values"].get(
                    "catastro_urbano.urban_parcels"
                ),
                "bienes_inmuebles": aggregated["summary_values"].get(
                    "catastro_urbano.real_estate_assets"
                ),
                "valor_catastral_total_miles_euros": aggregated["summary_values"].get(
                    "catastro_urbano.cadastral_total_value"
                ),
            },
            "series": aggregated["series"],
            "metadata": {
                "provider": "catastro",
                "provider_family": "catastro_urbano",
                "coverage": unit_level,
                "coverage_status": aggregated["coverage_status"],
                "cache_status": "miss",
                "cache_hits": aggregated["cache_hits"],
                "live_fetches": aggregated["live_fetches"],
                "missing_municipality_codes": aggregated["missing_municipality_codes"],
                "missing_municipality_names": aggregated["missing_municipality_names"],
                "reference_year": reference_year,
            },
        }
        await self.catastro_aggregate_cache_repo.upsert_payload(
            provider_family=CATASTRO_PROVIDER_FAMILY_URBANO_TERRITORIAL,
            unit_level=unit_level,
            code_value=canonical_code,
            reference_year=reference_year,
            payload=dataset_payload,
            ttl_seconds=self.aggregate_cache_ttl_seconds,
            metadata={
                "provider": "catastro",
                "provider_family": "catastro_urbano",
                "coverage_status": aggregated["coverage_status"],
            },
            now=self.now_factory(),
        )
        self.logger.info(
            "catastro_territorial_export_dataset_built",
            extra={
                "unit_level": unit_level,
                "code_value": canonical_code,
                "reference_year": reference_year,
                "municipalities_expected": aggregated["municipalities_expected"],
                "municipalities_included": aggregated["municipalities_included"],
                "coverage_status": aggregated["coverage_status"],
            },
        )
        return dataset_payload

    async def _aggregate_descendant_municipalities(
        self,
        *,
        descendants: Sequence[dict[str, Any]],
        reference_year: str,
    ) -> dict[str, Any]:
        municipality_payloads: list[dict[str, Any]] = []
        cache_hits = 0
        live_fetches = 0
        missing_codes: list[str] = []
        missing_names: list[str] = []
        missing_descendants: list[dict[str, Any]] = []

        for descendant in descendants:
            municipality_code = str(descendant.get("municipality_code") or "").strip()
            municipality_name = _best_descendant_name(descendant)
            if not municipality_code:
                missing_codes.append("")
                missing_names.append(municipality_name)
                missing_descendants.append(descendant)
                continue
            cached = await self.catastro_cache_repo.get_fresh_payload(
                provider_family=CATASTRO_PROVIDER_FAMILY_URBANO,
                municipality_code=municipality_code,
                reference_year=reference_year,
                now=self.now_factory(),
            )
            if cached is None:
                missing_descendants.append(descendant)
                continue
            record_provider_cache_hit("catastro", "municipality_aggregates")
            municipality_payloads.append(dict(cached["payload"]))
            cache_hits += 1

        fetch_results = await self._fetch_missing_descendants(
            descendants=missing_descendants,
            reference_year=reference_year,
        )
        for descendant, fetched, error in fetch_results:
            municipality_code = str(descendant.get("municipality_code") or "").strip()
            municipality_name = _best_descendant_name(descendant)
            if error is not None:
                if municipality_code:
                    missing_codes.append(municipality_code)
                missing_names.append(municipality_name)
                self.logger.warning(
                    "catastro_territorial_aggregate_municipality_missing",
                    extra={
                        "municipality_code": municipality_code or None,
                        "municipality_name": municipality_name,
                        "error": error.detail,
                    },
                )
                continue

            payload = {
                "reference_year": fetched["reference_year"],
                "province_file_code": fetched["province_file_code"],
                "province_label": fetched["province_label"],
                "municipality_option_value": fetched["municipality_option_value"],
                "municipality_label": fetched["municipality_label"],
                "indicators": fetched["indicators"],
            }
            await self.ingestion_repo.save_raw(
                source_type="catastro_urbano_municipality_aggregates",
                source_key=f"{municipality_code}:{fetched['reference_year']}",
                request_path="/jaxi/tabla.do",
                request_params={
                    "reference_year": fetched["reference_year"],
                    "province_file_code": fetched["province_file_code"],
                    "municipality_code": municipality_code,
                    "municipality_option_value": fetched["municipality_option_value"],
                },
                payload=fetched["raw"],
            )
            await self.catastro_cache_repo.upsert_payload(
                provider_family=CATASTRO_PROVIDER_FAMILY_URBANO,
                municipality_code=municipality_code,
                reference_year=fetched["reference_year"],
                payload=payload,
                ttl_seconds=self.cache_ttl_seconds,
                metadata=fetched["metadata"],
                now=self.now_factory(),
            )
            municipality_payloads.append(payload)
            live_fetches += 1

        expected = len(descendants)
        included = len(municipality_payloads)
        missing = max(expected - included, 0)
        coverage_status = "complete" if included == expected and expected > 0 else "partial"
        series, summary_values = _aggregate_catastro_series(municipality_payloads)
        return {
            "series": series,
            "summary_values": summary_values,
            "municipalities_expected": expected,
            "municipalities_included": included,
            "municipalities_missing": missing,
            "coverage_ratio": round(included / expected, 4) if expected else 0.0,
            "coverage_status": coverage_status,
            "cache_hits": cache_hits,
            "live_fetches": live_fetches,
            "missing_municipality_codes": missing_codes,
            "missing_municipality_names": missing_names,
        }

    async def _fetch_missing_descendants(
        self,
        *,
        descendants: Sequence[dict[str, Any]],
        reference_year: str,
    ) -> list[tuple[dict[str, Any], dict[str, Any] | None, CatastroClientError | None]]:
        if not descendants:
            return []

        semaphore = asyncio.Semaphore(self.aggregate_max_concurrency)

        async def _fetch_one(
            descendant: dict[str, Any],
        ) -> tuple[dict[str, Any], dict[str, Any] | None, CatastroClientError | None]:
            province_candidates = _build_descendant_province_candidates(descendant)
            municipality_candidates = _build_descendant_municipality_candidates(descendant)
            try:
                async with semaphore:
                    payload = await self.catastro_client.fetch_municipality_aggregates(
                        province_candidates=province_candidates,
                        municipality_candidates=municipality_candidates,
                        reference_year=reference_year,
                    )
            except CatastroClientError as exc:
                return descendant, None, exc
            return descendant, payload, None

        return list(await asyncio.gather(*(_fetch_one(descendant) for descendant in descendants)))

    async def _load_municipality_payload(
        self,
        *,
        municipality_code: str,
        province_candidates: Sequence[str],
        municipality_candidates: Sequence[str],
        reference_year: str,
    ) -> tuple[dict[str, Any], dict[str, Any], str]:
        cached = await self.catastro_cache_repo.get_fresh_payload(
            provider_family=CATASTRO_PROVIDER_FAMILY_URBANO,
            municipality_code=municipality_code,
            reference_year=reference_year,
            now=self.now_factory(),
        )
        if cached is not None:
            record_provider_cache_hit("catastro", "municipality_aggregates")
            return dict(cached["payload"]), dict(cached.get("metadata") or {}), "hit"

        fetched = await self.catastro_client.fetch_municipality_aggregates(
            province_candidates=province_candidates,
            municipality_candidates=municipality_candidates,
            reference_year=reference_year,
        )
        payload = {
            "reference_year": fetched["reference_year"],
            "province_file_code": fetched["province_file_code"],
            "province_label": fetched["province_label"],
            "municipality_option_value": fetched["municipality_option_value"],
            "municipality_label": fetched["municipality_label"],
            "indicators": fetched["indicators"],
        }
        provider_metadata = dict(fetched["metadata"])
        await self.ingestion_repo.save_raw(
            source_type="catastro_urbano_municipality_aggregates",
            source_key=f"{municipality_code}:{fetched['reference_year']}",
            request_path="/jaxi/tabla.do",
            request_params={
                "reference_year": fetched["reference_year"],
                "province_file_code": fetched["province_file_code"],
                "municipality_code": municipality_code,
                "municipality_option_value": fetched["municipality_option_value"],
            },
            payload=fetched["raw"],
        )
        await self.catastro_cache_repo.upsert_payload(
            provider_family=CATASTRO_PROVIDER_FAMILY_URBANO,
            municipality_code=municipality_code,
            reference_year=fetched["reference_year"],
            payload=payload,
            ttl_seconds=self.cache_ttl_seconds,
            metadata=provider_metadata,
            now=self.now_factory(),
        )
        return payload, provider_metadata, "miss"

    def _build_dataset_payload(
        self,
        *,
        context: ExportContext,
        code_value: str,
        payload: dict[str, Any],
        metadata: dict[str, Any],
        cache_status: str,
    ) -> dict[str, Any]:
        indicators = list(payload.get("indicators") or [])
        summary = {
            "indicators_total": len(indicators),
            "reference_year": payload.get("reference_year"),
            "code_value": code_value,
            "parcelas_urbanas": _indicator_value(indicators, "catastro_urbano.urban_parcels"),
            "bienes_inmuebles": _indicator_value(indicators, "catastro_urbano.real_estate_assets"),
            "valor_catastral_total_miles_euros": _indicator_value(
                indicators, "catastro_urbano.cadastral_total_value"
            ),
        }
        return {
            "source": CATASTRO_EXPORT_SOURCE_BY_LEVEL[context.unit.unit_level],
            "generated_at": self.now_factory().isoformat(),
            "territorial_context": context.territorial_context.model_dump(mode="json"),
            "filters": {
                "reference_year": payload.get("reference_year"),
                "unit_level": context.unit.unit_level,
                "code_value": code_value,
            },
            "summary": summary,
            "series": indicators,
            "metadata": {
                "provider": "catastro",
                "provider_family": metadata.get("provider_family", "catastro_urbano"),
                "coverage": "municipality",
                "reference_year": payload.get("reference_year"),
                "province_file_code": payload.get("province_file_code"),
                "province_label": payload.get("province_label"),
                "municipality_label": payload.get("municipality_label"),
                "cache_status": cache_status,
            },
        }


class TerritorialExportService:
    def __init__(
        self,
        *,
        territorial_repo: TerritorialRepository,
        series_repo: SeriesRepository,
        analytics_service: TerritorialAnalyticsService,
        catastro_client: CatastroClientService,
        catastro_cache_repo: CatastroMunicipalityAggregateCacheRepository,
        catastro_aggregate_cache_repo: CatastroTerritorialAggregateCacheRepository,
        ingestion_repo: IngestionRepository,
        artifact_repo: TerritorialExportArtifactRepository,
        export_ttl_seconds: int,
        catastro_cache_ttl_seconds: int,
        catastro_aggregate_cache_ttl_seconds: int,
        catastro_aggregate_max_concurrency: int,
        now_factory: Callable[[], datetime] | None = None,
    ) -> None:
        self.territorial_repo = territorial_repo
        self.series_repo = series_repo
        self.analytics_service = analytics_service
        self.catastro_client = catastro_client
        self.catastro_cache_repo = catastro_cache_repo
        self.catastro_aggregate_cache_repo = catastro_aggregate_cache_repo
        self.ingestion_repo = ingestion_repo
        self.artifact_repo = artifact_repo
        self.export_ttl_seconds = max(0, export_ttl_seconds)
        self.catastro_cache_ttl_seconds = max(0, catastro_cache_ttl_seconds)
        self.catastro_aggregate_cache_ttl_seconds = max(0, catastro_aggregate_cache_ttl_seconds)
        self.catastro_aggregate_max_concurrency = max(1, catastro_aggregate_max_concurrency)
        self.now_factory = now_factory or (lambda: datetime.now(UTC))
        self.logger = get_logger("app.services.territorial_exports")
        self.providers: dict[str, TerritorialExportProvider] = {
            EXPORT_PROVIDER_TERRITORIAL: TerritorialMetadataExportProvider(),
            EXPORT_PROVIDER_INE: INESeriesExportProvider(series_repo=series_repo),
            EXPORT_PROVIDER_ANALYTICS: AnalyticsTerritorialExportProvider(
                analytics_service=analytics_service
            ),
            EXPORT_PROVIDER_CATASTRO: CatastroExportProvider(
                territorial_repo=territorial_repo,
                catastro_client=catastro_client,
                catastro_cache_repo=catastro_cache_repo,
                catastro_aggregate_cache_repo=catastro_aggregate_cache_repo,
                ingestion_repo=ingestion_repo,
                cache_ttl_seconds=self.catastro_cache_ttl_seconds,
                aggregate_cache_ttl_seconds=self.catastro_aggregate_cache_ttl_seconds,
                aggregate_max_concurrency=self.catastro_aggregate_max_concurrency,
                now_factory=self.now_factory,
            ),
        }

    async def build_catastro_dataset_payload(
        self,
        *,
        unit_level: str,
        code_value: str,
    ) -> dict[str, Any] | None:
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
        provider = self.providers[EXPORT_PROVIDER_CATASTRO]
        if not isinstance(provider, CatastroExportProvider):
            raise ValueError("Catastro export provider is not configured.")

        if unit.unit_level == TERRITORIAL_UNIT_LEVEL_MUNICIPALITY:
            return await provider._build_municipality_dataset_payload(context=context)
        return await provider._build_territorial_aggregate_dataset_payload(context=context)

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
        normalized_providers = normalize_export_provider_keys(
            list(include_providers) if include_providers is not None else None
        )
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
            self.logger.error(
                "territorial_export_artifact_storage_failed",
                extra={"job_id": job_id, "export_key": export_key},
            )
            return None

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
                "provider_coverage": _build_provider_coverage_summary(datasets),
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


def _build_unit_name_candidates(unit: TerritorialUnitDetailResponse) -> list[str]:
    candidates = [unit.canonical_name, unit.display_name]
    candidates.extend(alias.alias for alias in unit.aliases)
    unique_candidates: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = normalize_territorial_name(candidate)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique_candidates.append(candidate)
    return unique_candidates


def _build_hierarchy_level_name_candidates(
    hierarchy: Sequence[TerritorialUnitSummaryResponse],
    unit_level: str,
) -> list[str]:
    candidates: list[str] = []
    for item in hierarchy:
        if item.unit_level != unit_level:
            continue
        for candidate in (item.canonical_name, item.display_name):
            normalized = normalize_territorial_name(candidate)
            if normalized and candidate not in candidates:
                candidates.append(candidate)
    return candidates


def _indicator_value(indicators: Sequence[dict[str, Any]], series_key: str) -> Any:
    for indicator in indicators:
        if indicator.get("series_key") == series_key:
            return indicator.get("value")
    return None


def _build_descendant_province_candidates(descendant: dict[str, Any]) -> list[str]:
    candidates = [
        descendant.get("province_canonical_name"),
        descendant.get("province_display_name"),
    ]
    return _deduplicate_candidates(candidates)


def _build_descendant_municipality_candidates(descendant: dict[str, Any]) -> list[str]:
    candidates = [
        descendant.get("canonical_name"),
        descendant.get("display_name"),
    ]
    return _deduplicate_candidates(candidates)


def _best_descendant_name(descendant: dict[str, Any]) -> str:
    return str(descendant.get("display_name") or descendant.get("canonical_name") or "").strip()


def _deduplicate_candidates(candidates: Sequence[Any]) -> list[str]:
    unique_candidates: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = normalize_territorial_name(str(candidate or ""))
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique_candidates.append(str(candidate))
    return unique_candidates


def _aggregate_catastro_series(
    municipality_payloads: Sequence[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    templates: dict[str, dict[str, Any]] = {}
    summed_values: dict[str, float] = {
        series_key: 0.0 for series_key in CATASTRO_SUMMABLE_SERIES_KEYS
    }
    summed_presence: dict[str, bool] = {
        series_key: False for series_key in CATASTRO_SUMMABLE_SERIES_KEYS
    }
    valuation_years: list[int] = []

    for payload in municipality_payloads:
        for indicator in payload.get("indicators") or []:
            series_key = str(indicator.get("series_key") or "")
            if series_key not in templates:
                templates[series_key] = dict(indicator)
            value = indicator.get("value")
            if series_key in CATASTRO_SUMMABLE_SERIES_KEYS and value is not None:
                summed_values[series_key] += float(value)
                summed_presence[series_key] = True
            elif series_key == CATASTRO_LAST_VALUATION_SERIES_KEY and value is not None:
                valuation_years.append(int(value))

    series: list[dict[str, Any]] = []
    summary_values: dict[str, Any] = {}
    for series_key in CATASTRO_SUMMABLE_SERIES_KEYS:
        template = dict(templates.get(series_key) or {})
        aggregated_value: int | float | None
        if not summed_presence[series_key]:
            aggregated_value = None
        elif template.get("unit") == "unidades":
            aggregated_value = int(round(summed_values[series_key]))
        else:
            aggregated_value = round(summed_values[series_key], 2)
        summary_values[series_key] = aggregated_value
        series.append(
            {
                "series_key": series_key,
                "label": template.get("label") or series_key,
                "value": aggregated_value,
                "unit": template.get("unit"),
                "metadata": {
                    **dict(template.get("metadata") or {}),
                    "provider": "catastro",
                    "provider_family": "catastro_urbano",
                    "aggregation_method": "sum",
                },
            }
        )

    valuation_template = dict(templates.get(CATASTRO_LAST_VALUATION_SERIES_KEY) or {})
    if valuation_years:
        series.extend(
            [
                {
                    "series_key": "catastro_urbano.last_valuation_year_min",
                    "label": "Ano ultima valoracion minimo",
                    "value": min(valuation_years),
                    "unit": valuation_template.get("unit") or "anos",
                    "metadata": {
                        **dict(valuation_template.get("metadata") or {}),
                        "provider": "catastro",
                        "provider_family": "catastro_urbano",
                        "aggregation_method": "min",
                    },
                },
                {
                    "series_key": "catastro_urbano.last_valuation_year_max",
                    "label": "Ano ultima valoracion maximo",
                    "value": max(valuation_years),
                    "unit": valuation_template.get("unit") or "anos",
                    "metadata": {
                        **dict(valuation_template.get("metadata") or {}),
                        "provider": "catastro",
                        "provider_family": "catastro_urbano",
                        "aggregation_method": "max",
                    },
                },
            ]
        )

    return series, summary_values


def _build_provider_coverage_summary(datasets: Sequence[ExportDataset]) -> dict[str, Any]:
    provider_coverage: dict[str, Any] = {}
    for dataset in datasets:
        if (
            dataset.provider != EXPORT_PROVIDER_CATASTRO
            or not dataset.applicable
            or dataset.payload_bytes is None
        ):
            continue
        try:
            payload = json.loads(dataset.payload_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        metadata = dict(payload.get("metadata") or {})
        summary = dict(payload.get("summary") or {})
        provider_coverage[dataset.provider] = {
            "coverage_status": metadata.get("coverage_status", "complete"),
            "reference_year": payload.get("filters", {}).get("reference_year"),
            "municipalities_expected": summary.get("municipalities_expected"),
            "municipalities_included": summary.get("municipalities_included"),
            "municipalities_missing": summary.get("municipalities_missing"),
        }
    return provider_coverage


def _json_bytes(value: Any) -> bytes:
    return _json_text(value).encode("utf-8")


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, default=str)


def _ndjson_line(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, default=str) + "\n").encode(
        "utf-8"
    )
