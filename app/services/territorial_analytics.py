from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from time import perf_counter
from typing import Any

from app.core.logging import get_logger
from app.core.metrics import record_analytical_flow, record_analytical_snapshot_event
from app.repositories.analytics_snapshots import AnalyticalSnapshotRepository
from app.repositories.series import SeriesRepository
from app.repositories.territorial import (
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TerritorialRepository,
)
from app.schemas import (
    AnalyticalSeriesItemResponse,
    AnalyticalTerritorialContextResponse,
    TerritorialMunicipalityReportResponse,
    TerritorialMunicipalitySummaryFiltersResponse,
    TerritorialMunicipalitySummaryMetricsResponse,
    TerritorialMunicipalitySummaryResponse,
    TerritorialMunicipalitySummarySeriesItemResponse,
    TerritorialReportSectionResponse,
    TerritorialUnitDetailResponse,
)


ProgressReporter = Callable[[dict[str, object]], Awaitable[None]]
MUNICIPALITY_SUMMARY_SOURCE = "internal.analytics.municipality_summary"
MUNICIPALITY_REPORT_TYPE = "municipality_report"
MUNICIPALITY_SUMMARY_FLOW = "municipality_summary"


class TerritorialAnalyticsService:
    def __init__(
        self,
        territorial_repo: TerritorialRepository,
        series_repo: SeriesRepository,
        analytical_snapshot_repo: AnalyticalSnapshotRepository | None = None,
        analytical_snapshot_ttl_seconds: int = 0,
        *,
        now_factory: Callable[[], datetime] | None = None,
    ) -> None:
        self.territorial_repo = territorial_repo
        self.series_repo = series_repo
        self.analytical_snapshot_repo = analytical_snapshot_repo
        self.analytical_snapshot_ttl_seconds = max(0, analytical_snapshot_ttl_seconds)
        self.now_factory = now_factory or (lambda: datetime.now(timezone.utc))
        self.logger = get_logger("app.services.territorial_analytics")

    async def build_municipality_summary(
        self,
        *,
        municipality_code: str,
        operation_code: str | None = None,
        variable_id: str | None = None,
        period_from: str | None = None,
        period_to: str | None = None,
        page: int = 1,
        page_size: int = 50,
        emit_observability: bool = True,
    ) -> TerritorialMunicipalitySummaryResponse | None:
        started_at = perf_counter()
        outcome = "failed"
        storage_mode = "direct"
        series_count = 0
        result_bytes = 0

        try:
            unit_payload = await self.territorial_repo.get_unit_detail_by_canonical_code(
                unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
                code_value=municipality_code,
            )
            if unit_payload is None:
                outcome = "not_found"
                storage_mode = "none"
                if emit_observability:
                    self.logger.info(
                        "municipality_summary_not_found",
                        extra={"municipality_code": municipality_code},
                    )
                return None

            unit = TerritorialUnitDetailResponse(**unit_payload)
            indicators = await self.series_repo.list_latest_indicators_by_geography(
                geography_code=municipality_code,
                operation_code=operation_code,
                variable_id=variable_id,
                period_from=period_from,
                period_to=period_to,
                page=page,
                page_size=page_size,
            )
            summary = TerritorialMunicipalitySummaryResponse(
                source=MUNICIPALITY_SUMMARY_SOURCE,
                generated_at=self.now_factory(),
                territorial_context=self._build_analytical_territorial_context(unit),
                territorial_unit=unit,
                filters=TerritorialMunicipalitySummaryFiltersResponse(
                    municipality_code=municipality_code,
                    operation_code=operation_code,
                    variable_id=variable_id,
                    period_from=period_from,
                    period_to=period_to,
                    page=page,
                    page_size=page_size,
                ),
                summary=TerritorialMunicipalitySummaryMetricsResponse(
                    indicators_total=indicators["total"],
                    indicators_returned=len(indicators["items"]),
                    operation_codes=indicators["summary"]["operation_codes"],
                    latest_period=indicators["summary"]["latest_period"],
                ),
                series=[
                    TerritorialMunicipalitySummarySeriesItemResponse(**item)
                    for item in indicators["items"]
                ],
                metadata={
                    "dataset": "ine_series_normalized",
                    "analytical_scope": "municipality_summary",
                },
                pagination={
                    "total": indicators["total"],
                    "page": indicators["page"],
                    "page_size": indicators["page_size"],
                    "pages": indicators["pages"],
                    "has_next": indicators["has_next"],
                    "has_previous": indicators["has_previous"],
                },
            )
            outcome = "completed"
            series_count = len(summary.series)
            result_bytes = self._estimate_payload_size_bytes(summary.model_dump(mode="json"))
            if emit_observability:
                self.logger.info(
                    "municipality_summary_built",
                    extra={
                        "municipality_code": municipality_code,
                        "operation_code": operation_code,
                        "variable_id": variable_id,
                        "total_indicators": indicators["total"],
                        "returned_indicators": series_count,
                        "storage_mode": storage_mode,
                        "result_bytes": result_bytes,
                        "duration_ms": self._duration_ms(started_at),
                    },
                )
            return summary
        except Exception:
            if emit_observability:
                self.logger.exception(
                    "municipality_summary_build_failed",
                    extra={
                        "municipality_code": municipality_code,
                        "operation_code": operation_code,
                        "variable_id": variable_id,
                    },
                )
            raise
        finally:
            if emit_observability:
                record_analytical_flow(
                    flow=MUNICIPALITY_SUMMARY_FLOW,
                    outcome=outcome,
                    storage_mode=storage_mode,
                    duration_seconds=perf_counter() - started_at,
                    series_count=series_count,
                    result_bytes=result_bytes,
                )

    async def build_municipality_report(
        self,
        *,
        municipality_code: str,
        operation_code: str | None = None,
        variable_id: str | None = None,
        period_from: str | None = None,
        period_to: str | None = None,
        page: int = 1,
        page_size: int = 50,
        progress_reporter: ProgressReporter | None = None,
    ) -> TerritorialMunicipalityReportResponse | None:
        started_at = perf_counter()
        outcome = "failed"
        storage_mode = "job_store_only"
        series_count = 0
        result_bytes = 0

        report_filters = self._build_municipality_report_filters(
            municipality_code=municipality_code,
            operation_code=operation_code,
            variable_id=variable_id,
            period_from=period_from,
            period_to=period_to,
            page=page,
            page_size=page_size,
        )

        try:
            if progress_reporter is not None and self._snapshot_persistence_enabled:
                await progress_reporter(
                    {
                        "stage": "checking_snapshot",
                        "municipality_code": municipality_code,
                        "report_type": MUNICIPALITY_REPORT_TYPE,
                    }
                )

            cached_report = await self._get_persisted_municipality_report(
                municipality_code=municipality_code,
                filters=report_filters,
            )
            if cached_report is not None:
                outcome = "completed"
                storage_mode = self._report_storage_mode(cached_report)
                series_count = len(cached_report.series)
                result_bytes = self._estimate_payload_size_bytes(
                    cached_report.model_dump(mode="json")
                )
                if progress_reporter is not None:
                    await progress_reporter(
                        {
                            "stage": "snapshot_hit",
                            "municipality_code": municipality_code,
                            "report_type": MUNICIPALITY_REPORT_TYPE,
                        }
                    )
                self.logger.info(
                    "municipality_report_built",
                    extra={
                        "municipality_code": municipality_code,
                        "operation_code": operation_code,
                        "variable_id": variable_id,
                        "series_count": series_count,
                        "storage_mode": storage_mode,
                        "snapshot_reused": True,
                        "result_bytes": result_bytes,
                        "duration_ms": self._duration_ms(started_at),
                    },
                )
                return cached_report

            if progress_reporter is not None:
                await progress_reporter(
                    {
                        "stage": "building_summary",
                        "municipality_code": municipality_code,
                        "report_type": MUNICIPALITY_REPORT_TYPE,
                    }
                )

            summary = await self.build_municipality_summary(
                municipality_code=municipality_code,
                operation_code=operation_code,
                variable_id=variable_id,
                period_from=period_from,
                period_to=period_to,
                page=page,
                page_size=page_size,
                emit_observability=False,
            )
            if summary is None:
                outcome = "not_found"
                storage_mode = "none"
                self.logger.info(
                    "municipality_report_not_found",
                    extra={"municipality_code": municipality_code},
                )
                return None

            if progress_reporter is not None:
                await progress_reporter(
                    {
                        "stage": "assembling_report",
                        "municipality_code": municipality_code,
                        "indicators_total": summary.summary.indicators_total,
                    }
                )

            profile_section = TerritorialReportSectionResponse(
                section_key="territorial_profile",
                title="Territorial profile",
                summary={
                    "canonical_name": summary.territorial_unit.canonical_name,
                    "display_name": summary.territorial_unit.display_name,
                    "unit_level": summary.territorial_unit.unit_level,
                    "country_code": summary.territorial_unit.country_code,
                    "codes_count": len(summary.territorial_unit.codes),
                    "aliases_count": len(summary.territorial_unit.aliases),
                },
                metadata={"section_type": "context"},
            )
            indicators_section = TerritorialReportSectionResponse(
                section_key="latest_indicators",
                title="Latest indicators",
                summary=summary.summary.model_dump(),
                series=[
                    AnalyticalSeriesItemResponse(**item.model_dump()) for item in summary.series
                ],
                metadata={
                    "dataset": "ine_series_normalized",
                    "section_type": "indicator_snapshot",
                },
            )

            report = TerritorialMunicipalityReportResponse(
                report_type=MUNICIPALITY_REPORT_TYPE,
                source="internal.analytics.municipality_report",
                generated_at=self.now_factory(),
                territorial_context=summary.territorial_context,
                territorial_unit=summary.territorial_unit,
                filters=summary.filters,
                summary=summary.summary,
                series=summary.series,
                sections=[profile_section, indicators_section],
                metadata={
                    **summary.metadata,
                    "report_scope": "municipality",
                    "storage_mode": "job_store_only",
                    "persistence": "disabled_or_unavailable",
                },
                pagination=summary.pagination,
            )

            persisted_report = await self._persist_municipality_report(
                report=report,
                municipality_code=municipality_code,
                filters=report_filters,
            )
            if persisted_report is not None:
                report = persisted_report
                if progress_reporter is not None:
                    await progress_reporter(
                        {
                            "stage": "snapshot_persisted",
                            "municipality_code": municipality_code,
                            "report_type": MUNICIPALITY_REPORT_TYPE,
                        }
                    )

            outcome = "completed"
            storage_mode = self._report_storage_mode(report)
            series_count = len(report.series)
            result_bytes = self._estimate_payload_size_bytes(report.model_dump(mode="json"))

            if progress_reporter is not None:
                await progress_reporter(
                    {
                        "stage": "report_completed",
                        "municipality_code": municipality_code,
                        "report_type": MUNICIPALITY_REPORT_TYPE,
                    }
                )

            self.logger.info(
                "municipality_report_built",
                extra={
                    "municipality_code": municipality_code,
                    "operation_code": operation_code,
                    "variable_id": variable_id,
                    "series_count": series_count,
                    "storage_mode": storage_mode,
                    "snapshot_reused": bool(report.metadata.get("snapshot_reused")),
                    "result_bytes": result_bytes,
                    "duration_ms": self._duration_ms(started_at),
                },
            )
            return report
        except Exception:
            self.logger.exception(
                "municipality_report_build_failed",
                extra={
                    "municipality_code": municipality_code,
                    "operation_code": operation_code,
                    "variable_id": variable_id,
                },
            )
            raise
        finally:
            record_analytical_flow(
                flow=MUNICIPALITY_REPORT_TYPE,
                outcome=outcome,
                storage_mode=storage_mode,
                duration_seconds=perf_counter() - started_at,
                series_count=series_count,
                result_bytes=result_bytes,
            )

    @staticmethod
    def _build_analytical_territorial_context(
        unit: TerritorialUnitDetailResponse,
    ) -> AnalyticalTerritorialContextResponse:
        canonical_code = unit.canonical_code
        municipality_code = (
            canonical_code.code_value
            if canonical_code is not None and unit.unit_level == TERRITORIAL_UNIT_LEVEL_MUNICIPALITY
            else None
        )
        return AnalyticalTerritorialContextResponse(
            territorial_unit_id=unit.id,
            unit_level=unit.unit_level,
            canonical_code=canonical_code.code_value if canonical_code else None,
            canonical_name=unit.canonical_name,
            display_name=unit.display_name,
            source_system=canonical_code.source_system if canonical_code else None,
            country_code=unit.country_code,
            municipality_code=municipality_code,
        )

    @property
    def _snapshot_persistence_enabled(self) -> bool:
        return (
            self.analytical_snapshot_repo is not None and self.analytical_snapshot_ttl_seconds > 0
        )

    async def _get_persisted_municipality_report(
        self,
        *,
        municipality_code: str,
        filters: dict[str, Any],
    ) -> TerritorialMunicipalityReportResponse | None:
        if not self._snapshot_persistence_enabled:
            return None

        snapshot_row = await self.analytical_snapshot_repo.get_fresh_snapshot(
            snapshot_type=MUNICIPALITY_REPORT_TYPE,
            scope_key=self._build_municipality_report_scope_key(municipality_code),
            filters=filters,
            now=self.now_factory(),
        )
        if snapshot_row is None:
            record_analytical_snapshot_event(MUNICIPALITY_REPORT_TYPE, "miss")
            return None

        record_analytical_snapshot_event(MUNICIPALITY_REPORT_TYPE, "hit")
        self.logger.info(
            "municipality_report_snapshot_reused",
            extra={
                "municipality_code": municipality_code,
                "snapshot_key": snapshot_row["snapshot_key"],
            },
        )
        return self._hydrate_snapshot_report(snapshot_row, reused=True)

    async def _persist_municipality_report(
        self,
        *,
        report: TerritorialMunicipalityReportResponse,
        municipality_code: str,
        filters: dict[str, Any],
    ) -> TerritorialMunicipalityReportResponse | None:
        if not self._snapshot_persistence_enabled:
            return None

        snapshot_row = await self.analytical_snapshot_repo.upsert_snapshot(
            snapshot_type=MUNICIPALITY_REPORT_TYPE,
            scope_key=self._build_municipality_report_scope_key(municipality_code),
            source=report.source,
            territorial_unit_id=report.territorial_context.territorial_unit_id,
            payload=report.model_dump(mode="json"),
            filters=filters,
            ttl_seconds=self.analytical_snapshot_ttl_seconds,
            metadata={
                "report_type": MUNICIPALITY_REPORT_TYPE,
                "report_scope": "municipality",
                "municipality_code": municipality_code,
            },
            generated_at=report.generated_at,
            now=self.now_factory(),
        )
        if snapshot_row is None:
            return None

        record_analytical_snapshot_event(MUNICIPALITY_REPORT_TYPE, "persisted")
        self.logger.info(
            "municipality_report_snapshot_persisted",
            extra={
                "municipality_code": municipality_code,
                "snapshot_key": snapshot_row["snapshot_key"],
            },
        )
        return self._hydrate_snapshot_report(snapshot_row, reused=False)

    def _hydrate_snapshot_report(
        self,
        snapshot_row: dict[str, Any],
        *,
        reused: bool,
    ) -> TerritorialMunicipalityReportResponse:
        payload = self._clone_jsonish(snapshot_row["payload"])
        metadata = dict(payload.get("metadata", {}))
        metadata.update(
            {
                "storage_mode": "persistent_snapshot",
                "persistence": "analytical_snapshots",
                "snapshot_reused": reused,
                "snapshot_key": snapshot_row["snapshot_key"],
                "snapshot_scope_key": snapshot_row["scope_key"],
                "snapshot_expires_at": self._serialize_datetime(snapshot_row["expires_at"]),
            }
        )
        payload["metadata"] = metadata
        return TerritorialMunicipalityReportResponse.model_validate(payload)

    @staticmethod
    def _build_municipality_report_scope_key(municipality_code: str) -> str:
        return f"municipality:{municipality_code}"

    @staticmethod
    def _build_municipality_report_filters(
        *,
        municipality_code: str,
        operation_code: str | None,
        variable_id: str | None,
        period_from: str | None,
        period_to: str | None,
        page: int,
        page_size: int,
    ) -> dict[str, Any]:
        return {
            "municipality_code": municipality_code,
            "operation_code": operation_code,
            "variable_id": variable_id,
            "period_from": period_from,
            "period_to": period_to,
            "page": page,
            "page_size": page_size,
        }

    @staticmethod
    def _serialize_datetime(value: datetime | str | None) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

    @staticmethod
    def _clone_jsonish(value: Any) -> Any:
        return json.loads(json.dumps(value, default=str))

    @staticmethod
    def _report_storage_mode(report: TerritorialMunicipalityReportResponse) -> str:
        return str(report.metadata.get("storage_mode") or "unknown")

    @staticmethod
    def _estimate_payload_size_bytes(payload: Any) -> int:
        return len(json.dumps(payload, default=str, ensure_ascii=True).encode("utf-8"))

    @staticmethod
    def _duration_ms(started_at: float) -> float:
        return round((perf_counter() - started_at) * 1000, 3)
