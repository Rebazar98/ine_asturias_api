from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from app.core.logging import get_logger
from app.repositories.series import SeriesRepository
from app.repositories.territorial import (
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
    TerritorialRepository,
)
from app.schemas import (
    AnalyticalSeriesItemResponse,
    TerritorialDossierCatastroSectionResponse,
    TerritorialDossierCatastroSummaryResponse,
    TerritorialDossierContextResponse,
    TerritorialDossierGeometrySectionResponse,
    TerritorialDossierIdentitySectionResponse,
    TerritorialDossierINEIndicatorResponse,
    TerritorialDossierINESectionResponse,
    TerritorialDossierINESummaryResponse,
    TerritorialDossierResponse,
    TerritorialDossierSectionsResponse,
    TerritorialDossierSummaryResponse,
    TerritorialGeometrySummaryResponse,
    TerritorialUnitDetailResponse,
    TerritorialUnitSummaryResponse,
)
from app.services.catastro_client import CatastroClientError
from app.services.territorial_exports import TerritorialExportService


TERRITORIAL_DOSSIER_SOURCE = "internal.territorial.dossier"
DOSSIER_SUPPORTED_UNIT_LEVELS = {
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
}
DOSSIER_PRIORITIZED_INE_OPERATIONS = ("71", "22", "33")
DOSSIER_INE_PAGE_SIZE_PER_OPERATION = 5


class TerritorialDossierService:
    def __init__(
        self,
        *,
        territorial_repo: TerritorialRepository,
        series_repo: SeriesRepository,
        territorial_export_service: TerritorialExportService,
        now_factory: Callable[[], datetime] | None = None,
    ) -> None:
        self.territorial_repo = territorial_repo
        self.series_repo = series_repo
        self.territorial_export_service = territorial_export_service
        self.now_factory = now_factory or (lambda: datetime.now(UTC))
        self.logger = get_logger("app.services.territorial_dossier")

    async def build_dossier(
        self,
        *,
        unit_level: str,
        code_value: str,
        include_geometry: bool = True,
        include_ine: bool = True,
        include_catastro: bool = True,
    ) -> TerritorialDossierResponse | None:
        unit_payload = await self.territorial_repo.get_unit_detail_by_canonical_code(
            unit_level=unit_level,
            code_value=code_value,
        )
        if unit_payload is None:
            return None

        unit = TerritorialUnitDetailResponse(**unit_payload)
        hierarchy_payload = await self.territorial_repo.list_hierarchy(unit.id)
        hierarchy = [TerritorialUnitSummaryResponse(**item) for item in hierarchy_payload]

        sections = TerritorialDossierSectionsResponse(
            identity=TerritorialDossierIdentitySectionResponse(
                unit=unit,
                hierarchy=hierarchy,
                metadata={"status": "complete"},
            )
        )
        partial_section_keys: list[str] = []
        included_sections = ["identity"]

        if include_geometry:
            included_sections.append("geometry")
            sections.geometry = await self._build_geometry_section(
                unit_level=unit_level,
                code_value=code_value,
            )
            if not sections.geometry.summary.has_geometry:
                partial_section_keys.append("geometry")

        if include_ine:
            included_sections.append("ine")
            sections.ine = await self._build_ine_section(unit=unit)
            if sections.ine.summary.coverage_status != "full":
                partial_section_keys.append("ine")

        if include_catastro:
            included_sections.append("catastro")
            sections.catastro = await self._build_catastro_section(
                unit_level=unit_level,
                code_value=code_value,
            )
            if sections.catastro.summary.coverage_status != "complete":
                partial_section_keys.append("catastro")

        has_geometry = bool(sections.geometry and sections.geometry.summary.has_geometry)
        has_ine_data = bool(sections.ine and sections.ine.summary.indicators_returned > 0)
        has_catastro_data = bool(
            sections.catastro
            and sections.catastro.summary.coverage_status in {"partial", "complete"}
            and (
                sections.catastro.summary.indicators_total > 0
                or sections.catastro.summary.municipalities_included not in (None, 0)
            )
        )

        return TerritorialDossierResponse(
            generated_at=self.now_factory(),
            territorial_context=TerritorialDossierContextResponse(
                territorial_unit_id=unit.id,
                unit_level=unit.unit_level,
                canonical_code=unit.canonical_code,
                canonical_name=unit.canonical_name,
                display_name=unit.display_name,
                country_code=unit.country_code,
                hierarchy=hierarchy,
            ),
            summary=TerritorialDossierSummaryResponse(
                resolved=True,
                has_geometry=has_geometry,
                geometry_coverage_status="available" if has_geometry else "none",
                has_ine_data=has_ine_data,
                has_catastro_data=has_catastro_data,
                partial_sections=bool(partial_section_keys),
                section_count=sum(
                    section is not None
                    for section in (
                        sections.identity,
                        sections.geometry,
                        sections.ine,
                        sections.catastro,
                    )
                ),
            ),
            sections=sections,
            metadata={
                "included_sections": included_sections,
                "partial_section_keys": partial_section_keys,
                "future_extension_mode": "additive",
            },
        )

    async def _build_geometry_section(
        self,
        *,
        unit_level: str,
        code_value: str,
    ) -> TerritorialDossierGeometrySectionResponse:
        geometry_payload = await self.territorial_repo.get_unit_geometry_by_canonical_code(
            unit_level=unit_level,
            code_value=code_value,
        )
        if geometry_payload is None:
            return TerritorialDossierGeometrySectionResponse(
                summary=TerritorialGeometrySummaryResponse(
                    has_geometry=False,
                    has_centroid=False,
                    geometry_type=None,
                    srid=None,
                    boundary_source=None,
                ),
                geometry=None,
                centroid=None,
                metadata={"status": "unavailable", "reason": "geometry_not_loaded"},
            )

        return TerritorialDossierGeometrySectionResponse(
            summary=TerritorialGeometrySummaryResponse(**geometry_payload["summary"]),
            geometry=geometry_payload.get("geometry"),
            centroid=geometry_payload.get("centroid"),
            metadata={"status": "complete", **dict(geometry_payload.get("metadata") or {})},
        )

    async def _build_ine_section(
        self,
        *,
        unit: TerritorialUnitDetailResponse,
    ) -> TerritorialDossierINESectionResponse:
        geography_code = unit.canonical_code.code_value if unit.canonical_code is not None else None
        if not geography_code:
            return TerritorialDossierINESectionResponse(
                summary=TerritorialDossierINESummaryResponse(
                    coverage_status="none",
                    operations_considered=list(DOSSIER_PRIORITIZED_INE_OPERATIONS),
                ),
                metadata={"status": "unavailable", "reason": "missing_canonical_code"},
            )

        combined_items: list[dict[str, Any]] = []
        operations_present: list[str] = []
        latest_period: str | None = None
        indicators_total = 0

        for operation_code in DOSSIER_PRIORITIZED_INE_OPERATIONS:
            payload = await self.series_repo.list_latest_indicators_by_geography(
                geography_code=geography_code,
                operation_code=operation_code,
                page=1,
                page_size=DOSSIER_INE_PAGE_SIZE_PER_OPERATION,
            )
            indicators_total += int(payload["total"])
            if payload["total"] > 0:
                operations_present.append(operation_code)
                combined_items.extend(payload["items"])
                candidate_period = payload["summary"]["latest_period"]
                if candidate_period and (latest_period is None or candidate_period > latest_period):
                    latest_period = candidate_period

        combined_items.sort(
            key=lambda item: (
                str(item.get("period") or ""),
                str(item.get("operation_code") or ""),
                str(item.get("table_id") or ""),
                str(item.get("variable_id") or ""),
            ),
            reverse=True,
        )
        if not operations_present:
            coverage_status = "none"
        elif len(operations_present) == len(DOSSIER_PRIORITIZED_INE_OPERATIONS):
            coverage_status = "full"
        else:
            coverage_status = "partial"

        return TerritorialDossierINESectionResponse(
            summary=TerritorialDossierINESummaryResponse(
                coverage_status=coverage_status,
                operations_considered=list(DOSSIER_PRIORITIZED_INE_OPERATIONS),
                operations_present=operations_present,
                indicators_total=indicators_total,
                indicators_returned=len(combined_items),
                latest_period=latest_period,
            ),
            series=[TerritorialDossierINEIndicatorResponse(**item) for item in combined_items],
            metadata={
                "status": "complete" if operations_present else "unavailable",
                "dataset": "ine_series_normalized",
                "geography_code_system": "ine",
            },
        )

    async def _build_catastro_section(
        self,
        *,
        unit_level: str,
        code_value: str,
    ) -> TerritorialDossierCatastroSectionResponse:
        try:
            payload = await self.territorial_export_service.build_catastro_dataset_payload(
                unit_level=unit_level,
                code_value=code_value,
            )
        except CatastroClientError as exc:
            detail = exc.detail if isinstance(exc.detail, dict) else {"message": str(exc.detail)}
            self.logger.warning(
                "territorial_dossier_catastro_unavailable",
                extra={
                    "unit_level": unit_level,
                    "code_value": code_value,
                    "error": detail,
                },
            )
            return TerritorialDossierCatastroSectionResponse(
                summary=TerritorialDossierCatastroSummaryResponse(coverage_status="unavailable"),
                metadata={
                    "status": "unavailable",
                    "error": detail,
                    "retryable": bool(detail.get("retryable")),
                },
            )

        if payload is None:
            return TerritorialDossierCatastroSectionResponse(
                summary=TerritorialDossierCatastroSummaryResponse(coverage_status="none"),
                metadata={"status": "unavailable", "reason": "unit_not_found"},
            )

        summary_payload = dict(payload.get("summary") or {})
        metadata_payload = dict(payload.get("metadata") or {})
        coverage_status = str(metadata_payload.get("coverage_status") or "").strip().lower()
        if coverage_status not in {"partial", "complete"}:
            coverage_status = "complete" if summary_payload.get("indicators_total") else "none"

        return TerritorialDossierCatastroSectionResponse(
            source=str(payload.get("source") or ""),
            summary=TerritorialDossierCatastroSummaryResponse(
                coverage_status=coverage_status,
                reference_year=summary_payload.get("reference_year"),
                indicators_total=int(
                    summary_payload.get("indicators_total") or len(payload.get("series") or [])
                ),
                municipalities_expected=summary_payload.get("municipalities_expected"),
                municipalities_included=summary_payload.get("municipalities_included"),
                municipalities_missing=summary_payload.get("municipalities_missing"),
                coverage_ratio=summary_payload.get("coverage_ratio"),
                parcelas_urbanas=summary_payload.get("parcelas_urbanas"),
                bienes_inmuebles=summary_payload.get("bienes_inmuebles"),
                valor_catastral_total_miles_euros=summary_payload.get(
                    "valor_catastral_total_miles_euros"
                ),
            ),
            series=[
                AnalyticalSeriesItemResponse(
                    series_key=str(item.get("series_key") or ""),
                    label=str(item.get("label") or ""),
                    value=item.get("value"),
                    unit=item.get("unit"),
                    period=item.get("period"),
                    metadata=dict(item.get("metadata") or {}),
                )
                for item in list(payload.get("series") or [])
            ],
            metadata={
                "status": "complete" if coverage_status == "complete" else "partial",
                **metadata_payload,
            },
        )
