from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from fastapi import Query
from pydantic import BaseModel, Field, RootModel, field_validator

from app.repositories.territorial_export_artifacts import (
    DEFAULT_EXPORT_PROVIDERS,
    normalize_export_provider_keys,
)


class HealthResponse(BaseModel):
    status: Literal["ok"]


class ReadinessComponentResponse(BaseModel):
    status: Literal["ok", "error", "disabled"]
    details: dict[str, Any] = Field(default_factory=dict)


class ReadinessResponse(BaseModel):
    status: Literal["ok", "degraded"]
    app_env: str
    components: dict[str, ReadinessComponentResponse]


class JSONPayload(RootModel[dict[str, Any] | list[Any]]):
    pass


class NormalizedSeriesItem(BaseModel):
    operation_code: str = ""
    table_id: str = ""
    variable_id: str = ""
    territorial_unit_id: int | None = None
    geography_name: str = ""
    geography_code: str = ""
    period: str
    value: float | None = None
    unit: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    raw_payload: dict[str, Any] = Field(default_factory=dict)
    source_provider: str = "ine"


class AsturiasResolutionResult(BaseModel):
    geo_variable_id: str
    asturias_value_id: str | None = None
    variable_name: str | None = None
    asturias_label: str | None = None
    name_based_fallback: bool = False


class AsturiasOperationQueryParams(BaseModel):
    """Query parameters for GET /ine/operation/{op_code}/asturias.

    Injected via FastAPI Depends() to group the 9 business-logic query
    params and keep the route signature concise.
    """

    geo_variable_id: str | None = Field(Query(default=None))
    asturias_value_id: str | None = Field(Query(default=None))
    nult: int | None = Field(Query(default=None, ge=1))
    det: int | None = Field(Query(default=None, ge=0, le=2))
    tip: Literal["A", "M", "AM"] | None = Field(Query(default=None))
    periodicidad: str | None = Field(Query(default=None))
    max_tables: int | None = Field(Query(default=None, ge=1, le=500))
    max_series: int | None = Field(Query(default=None, ge=1, le=5000))
    background: bool | None = Field(Query(default=None))
    skip_known_no_data: bool = Field(Query(default=False))
    skip_known_processed: bool = Field(Query(default=False))


class BackgroundJobAcceptedResponse(BaseModel):
    job_id: str
    job_type: str
    status: Literal["queued", "running", "completed", "failed"]
    background: Literal[True] = True
    background_forced: bool = False
    background_reason: Literal["heavy_operation_requires_background"] | None = None
    operation_code: str
    status_path: str
    params: dict[str, Any] = Field(default_factory=dict)


class BackgroundJobStatusResponse(BaseModel):
    job_id: str
    job_type: str
    status: Literal["queued", "running", "completed", "failed"]
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    background_forced: bool = False
    background_reason: Literal["heavy_operation_requires_background"] | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    progress: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] | list[Any] | None = None
    error: Any = None


class INESyncOperationCatalogItemResponse(BaseModel):
    operation_code: str
    execution_profile: Literal["scheduled", "background_only", "manual_only", "discarded"]
    schedule_enabled: bool
    background_required: bool = False
    decision_reason: str
    decision_source: str
    profile_origin: Literal["baseline", "override"] = "baseline"
    override_active: bool = False
    override_execution_profile: (
        Literal["scheduled", "background_only", "manual_only", "discarded"] | None
    ) = None
    override_schedule_enabled: bool | None = None
    override_decision_reason: str | None = None
    override_decision_source: str | None = None
    override_applied_at: datetime | None = None
    baseline_execution_profile: Literal["scheduled", "background_only", "manual_only", "discarded"]
    baseline_schedule_enabled: bool
    last_job_id: str | None = None
    last_run_status: Literal["queued", "running", "completed", "failed"] | None = None
    last_trigger_mode: str | None = None
    last_background_forced: bool = False
    last_background_reason: str | None = None
    last_run_started_at: datetime | None = None
    last_run_finished_at: datetime | None = None
    last_duration_ms: int | None = None
    last_tables_found: int | None = None
    last_tables_selected: int | None = None
    last_tables_succeeded: int | None = None
    last_tables_failed: int | None = None
    last_tables_skipped_catalog: int | None = None
    last_normalized_rows: int | None = None
    last_warning_count: int | None = None
    last_error_message: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class INESyncOperationCatalogFiltersResponse(BaseModel):
    operation_code: str | None = None
    execution_profile: (
        Literal["scheduled", "background_only", "manual_only", "discarded"] | None
    ) = None
    last_run_status: Literal["queued", "running", "completed", "failed"] | None = None
    schedule_enabled: bool | None = None
    include_unclassified: bool = True
    page: int
    page_size: int


class INESyncOperationCatalogSummaryResponse(BaseModel):
    operations_total: int
    scheduled_total: int
    background_only_total: int
    manual_only_total: int
    discarded_total: int
    schedule_enabled_total: int
    with_last_run_total: int


class INESyncOperationCatalogResponse(BaseModel):
    source: Literal["internal.sync.ine_operation_catalog"] = "internal.sync.ine_operation_catalog"
    generated_at: datetime
    summary: INESyncOperationCatalogSummaryResponse
    items: list[INESyncOperationCatalogItemResponse] = Field(default_factory=list)
    filters: INESyncOperationCatalogFiltersResponse
    pagination: AnalyticalPaginationResponse
    metadata: dict[str, Any] = Field(default_factory=dict)


class INESyncOperationOverrideRequest(BaseModel):
    execution_profile: Literal["scheduled", "background_only", "manual_only", "discarded"]
    decision_reason: str = Field(min_length=1)
    schedule_enabled: bool | None = None


class INESyncOperationHistoryItemResponse(BaseModel):
    event_id: int | None = None
    event_type: Literal["override_set", "override_updated", "override_cleared"]
    operation_code: str
    effective_execution_profile_before: (
        Literal["scheduled", "background_only", "manual_only", "discarded"] | None
    ) = None
    effective_execution_profile_after: (
        Literal["scheduled", "background_only", "manual_only", "discarded"] | None
    ) = None
    schedule_enabled_before: bool | None = None
    schedule_enabled_after: bool | None = None
    background_required_before: bool | None = None
    background_required_after: bool | None = None
    override_active_before: bool | None = None
    override_active_after: bool | None = None
    decision_reason: str | None = None
    decision_source: str | None = None
    override_decision_reason: str | None = None
    override_decision_source: str | None = None
    occurred_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class INESyncOperationHistorySummaryResponse(BaseModel):
    events_total: int
    override_set_total: int
    override_updated_total: int
    override_cleared_total: int


class INESyncOperationHistoryResponse(BaseModel):
    source: Literal["internal.sync.ine_operation_history"] = "internal.sync.ine_operation_history"
    generated_at: datetime
    operation_code: str
    summary: INESyncOperationHistorySummaryResponse
    items: list[INESyncOperationHistoryItemResponse] = Field(default_factory=list)
    pagination: AnalyticalPaginationResponse
    metadata: dict[str, Any] = Field(default_factory=dict)


class CatalogTableItemResponse(BaseModel):
    id: int
    operation_code: str
    table_id: str
    table_name: str
    request_path: str
    resolution_context: dict[str, Any] = Field(default_factory=dict)
    has_asturias_data: bool | None = None
    validation_status: Literal["unknown", "has_data", "no_data", "failed"]
    normalized_rows: int = 0
    raw_rows_retrieved: int = 0
    filtered_rows_retrieved: int = 0
    series_kept: int = 0
    series_discarded: int = 0
    last_checked_at: datetime | None = None
    first_seen_at: datetime | None = None
    updated_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    notes: str = ""
    last_warning: str = ""


class CatalogOperationSummaryResponse(BaseModel):
    operation_code: str
    total_tables: int
    has_data: int
    no_data: int
    failed: int
    unknown: int


class INESeriesListItemResponse(BaseModel):
    id: int
    operation_code: str
    table_id: str
    variable_id: str
    geography_name: str
    geography_code: str
    period: str
    value: float | None = None
    unit: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    inserted_at: datetime | None = None


class INESeriesFiltersResponse(BaseModel):
    operation_code: str | None = None
    table_id: str | None = None
    geography_code: str | None = None
    geography_name: str | None = None
    geography_code_system: Literal["ine"] = "ine"
    variable_id: str | None = None
    period_from: str | None = None
    period_to: str | None = None


class INESeriesTerritorialResolutionResponse(BaseModel):
    input_name: str
    resolved_geography_code: str | None = None
    matched_by: Literal["code", "alias", "canonical_name"] | None = None
    canonical_name: str | None = None
    source_system: str | None = None


class INESeriesListResponse(BaseModel):
    items: list[INESeriesListItemResponse] = Field(default_factory=list)
    total: int
    page: int
    page_size: int
    pages: int
    has_next: bool
    has_previous: bool
    filters: INESeriesFiltersResponse
    territorial_resolution: INESeriesTerritorialResolutionResponse | None = None


class AnalyticalTerritorialContextResponse(BaseModel):
    territorial_unit_id: int | None = None
    unit_level: str | None = None
    canonical_code: str | None = None
    canonical_name: str | None = None
    display_name: str | None = None
    source_system: str | None = None
    country_code: str | None = None
    autonomous_community_code: str | None = None
    province_code: str | None = None
    municipality_code: str | None = None


class AnalyticalSeriesItemResponse(BaseModel):
    series_key: str
    label: str
    value: float | int | str | bool | None = None
    unit: str | None = None
    period: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class AnalyticalPaginationResponse(BaseModel):
    total: int
    page: int
    page_size: int
    pages: int
    has_next: bool
    has_previous: bool


class AnalyticalErrorDetailResponse(BaseModel):
    code: str
    message: str
    retryable: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class AnalyticalErrorResponse(BaseModel):
    detail: AnalyticalErrorDetailResponse


class AnalyticalResponse(BaseModel):
    source: str = Field(description="Semantic source identifier of the analytical output.")
    generated_at: datetime = Field(
        description="Timestamp when the analytical output was generated."
    )
    territorial_context: AnalyticalTerritorialContextResponse = Field(
        default_factory=AnalyticalTerritorialContextResponse
    )
    filters: dict[str, Any] = Field(
        default_factory=dict,
        description="Semantic filters applied to produce the output.",
    )
    summary: dict[str, Any] = Field(
        default_factory=dict,
        description="Compact summary metrics for automation and reports.",
    )
    series: list[AnalyticalSeriesItemResponse] = Field(
        default_factory=list,
        description="Semantic analytical observations or indicators.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Non-provider-specific metadata useful for consumers.",
    )
    pagination: AnalyticalPaginationResponse | None = None


class TerritorialCanonicalCodeStrategyResponse(BaseModel):
    source_system: str
    code_type: str


class TerritorialUnitCodeResponse(BaseModel):
    source_system: str
    code_type: str
    code_value: str
    is_primary: bool = False


class TerritorialUnitAliasResponse(BaseModel):
    source_system: str
    alias: str
    normalized_alias: str
    alias_type: str


class TerritorialUnitLookupResponse(BaseModel):
    id: int
    parent_id: int | None = None
    unit_level: str
    canonical_name: str
    display_name: str
    country_code: str
    is_active: bool
    canonical_code_strategy: TerritorialCanonicalCodeStrategyResponse | None = None
    canonical_code: TerritorialUnitCodeResponse | None = None
    matched_by: Literal["code", "alias", "canonical_name"]
    matched_code: TerritorialUnitCodeResponse | None = None
    matched_alias: TerritorialUnitAliasResponse | None = None


class TerritorialUnitSummaryResponse(BaseModel):
    id: int
    parent_id: int | None = None
    unit_level: str
    canonical_name: str
    display_name: str
    country_code: str
    is_active: bool
    canonical_code_strategy: TerritorialCanonicalCodeStrategyResponse | None = None
    canonical_code: TerritorialUnitCodeResponse | None = None


class TerritorialUnitDetailResponse(TerritorialUnitSummaryResponse):
    codes: list[TerritorialUnitCodeResponse] = Field(default_factory=list)
    aliases: list[TerritorialUnitAliasResponse] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)


class TerritorialMunicipalitySummaryFiltersResponse(BaseModel):
    municipality_code: str
    geography_code_system: Literal["ine"] = "ine"
    operation_code: str | None = None
    variable_id: str | None = None
    period_from: str | None = None
    period_to: str | None = None
    page: int
    page_size: int


class TerritorialMunicipalitySummaryMetricsResponse(BaseModel):
    indicators_total: int
    indicators_returned: int
    operation_codes: list[str] = Field(default_factory=list)
    latest_period: str | None = None


class TerritorialMunicipalitySummarySeriesItemResponse(AnalyticalSeriesItemResponse):
    operation_code: str
    table_id: str
    variable_id: str
    geography_code: str
    geography_name: str


class TerritorialMunicipalitySummaryResponse(AnalyticalResponse):
    territorial_unit: TerritorialUnitDetailResponse
    filters: TerritorialMunicipalitySummaryFiltersResponse
    summary: TerritorialMunicipalitySummaryMetricsResponse
    series: list[TerritorialMunicipalitySummarySeriesItemResponse] = Field(default_factory=list)


class TerritorialReportSectionResponse(BaseModel):
    section_key: str
    title: str
    summary: dict[str, Any] = Field(default_factory=dict)
    series: list[AnalyticalSeriesItemResponse] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class TerritorialMunicipalityReportResponse(AnalyticalResponse):
    report_type: Literal["municipality_report"] = "municipality_report"
    territorial_unit: TerritorialUnitDetailResponse
    filters: TerritorialMunicipalitySummaryFiltersResponse
    summary: TerritorialMunicipalitySummaryMetricsResponse
    series: list[TerritorialMunicipalitySummarySeriesItemResponse] = Field(default_factory=list)
    sections: list[TerritorialReportSectionResponse] = Field(default_factory=list)


class TerritorialReportJobAcceptedResponse(BaseModel):
    job_id: str
    job_type: str
    report_type: Literal["municipality_report"] = "municipality_report"
    status: Literal["queued", "running", "completed", "failed"]
    background: Literal[True] = True
    municipality_code: str
    status_path: str
    params: dict[str, Any] = Field(default_factory=dict)


class TerritorialExportRequest(BaseModel):
    unit_level: Literal["municipality", "province", "autonomous_community"]
    code_value: str = Field(min_length=1, max_length=128)
    format: Literal["zip"] = "zip"
    include_providers: list[Literal["territorial", "ine", "analytics", "catastro"]] = Field(
        default_factory=lambda: list(DEFAULT_EXPORT_PROVIDERS)
    )

    @field_validator("code_value", mode="before")
    @classmethod
    def strip_code_value(cls, value: str) -> str:
        return str(value).strip()

    @field_validator("include_providers", mode="before")
    @classmethod
    def normalize_providers(cls, value: list[str] | None) -> list[str]:
        if value is None:
            return list(DEFAULT_EXPORT_PROVIDERS)

        providers = normalize_export_provider_keys(value)
        if not providers:
            raise ValueError("include_providers must include at least one supported provider.")
        return providers


class TerritorialExportResultResponse(BaseModel):
    export_id: int
    export_key: str
    format: Literal["zip"] = "zip"
    territorial_context: AnalyticalTerritorialContextResponse
    summary: dict[str, Any] = Field(default_factory=dict)
    download_path: str
    expires_at: datetime


class TerritorialExportJobAcceptedResponse(BaseModel):
    job_id: str
    job_type: Literal["territorial_export"] = "territorial_export"
    status: Literal["queued", "running", "completed", "failed"]
    background: Literal[True] = True
    status_path: str
    params: TerritorialExportRequest


class TerritorialExportJobStatusResponse(BaseModel):
    job_id: str
    job_type: Literal["territorial_export"] = "territorial_export"
    status: Literal["queued", "running", "completed", "failed"]
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    params: TerritorialExportRequest
    progress: dict[str, Any] = Field(default_factory=dict)
    result: TerritorialExportResultResponse | None = None
    error: Any = None


class TerritorialUnitListFiltersResponse(BaseModel):
    unit_level: str
    country_code: str | None = None
    parent_id: int | None = None
    active_only: bool = True


class IndicatorTerritoryResponse(BaseModel):
    code: str
    name: str
    code_system: str = "ine"


class IndicatorSeriesPointResponse(BaseModel):
    period: str
    value: float | None = None
    unit: str | None = None


class IndicadorSeriesFiltersResponse(BaseModel):
    indicador: str
    codigo_territorial: str
    code_system: str = "ine"
    operation_code: str | None = None
    period_from: str | None = None
    period_to: str | None = None
    page: int
    page_size: int


class IndicadorSeriesResponse(BaseModel):
    source: str = "ine"
    territory: IndicatorTerritoryResponse
    indicator: str
    series: list[IndicatorSeriesPointResponse] = Field(default_factory=list)
    total: int
    page: int
    page_size: int
    pages: int
    has_next: bool
    has_previous: bool
    filters: IndicadorSeriesFiltersResponse
    metadata: dict[str, Any] = Field(default_factory=dict)


class TerritorialUnitListResponse(BaseModel):
    items: list[TerritorialUnitSummaryResponse] = Field(default_factory=list)
    total: int
    page: int
    page_size: int
    pages: int
    has_next: bool
    has_previous: bool
    filters: TerritorialUnitListFiltersResponse


class TerritorialCatalogSummaryResponse(BaseModel):
    resources_total: int
    territorial_levels_total: int
    read_resources_total: int
    analytics_resources_total: int
    job_resources_total: int


class TerritorialCatalogLevelCoverageResponse(BaseModel):
    unit_level: str
    country_code: str
    units_total: int
    active_units: int
    geometry_units: int = 0
    centroid_units: int = 0
    boundary_source: str | None = None
    canonical_code_strategy: TerritorialCanonicalCodeStrategyResponse | None = None
    list_path: str | None = None
    detail_path: str | None = None
    summary_path: str | None = None
    report_job_path: str | None = None


class TerritorialCatalogResourceResponse(BaseModel):
    resource_key: str
    title: str
    category: Literal["territorial_read", "territorial_analytics", "territorial_jobs"]
    method: Literal["GET", "POST"]
    path: str
    summary: str
    unit_levels: list[str] = Field(default_factory=list)
    path_params: list[str] = Field(default_factory=list)
    query_params: list[str] = Field(default_factory=list)
    response_contract: str
    supports_pagination: bool = False
    supports_background_job: bool = False
    supports_snapshot_reuse: bool = False


class TerritorialCatalogResponse(BaseModel):
    source: Literal["internal.catalog.territorial"] = "internal.catalog.territorial"
    generated_at: datetime
    summary: TerritorialCatalogSummaryResponse
    territorial_levels: list[TerritorialCatalogLevelCoverageResponse] = Field(default_factory=list)
    resources: list[TerritorialCatalogResourceResponse] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class GeocodingCoordinatesResponse(BaseModel):
    lat: float
    lon: float


class TerritorialPointResolutionCoverageResponse(BaseModel):
    boundary_source: str | None = None
    levels_considered: list[str] = Field(default_factory=list)
    levels_matched: list[str] = Field(default_factory=list)


class TerritorialPointResolutionResultResponse(BaseModel):
    matched_by: Literal["geometry_cover"] = "geometry_cover"
    best_match: TerritorialUnitSummaryResponse
    hierarchy: list[TerritorialUnitSummaryResponse] = Field(default_factory=list)
    coverage: TerritorialPointResolutionCoverageResponse


class TerritorialPointResolutionResponse(BaseModel):
    source: Literal["internal.territorial.point_resolution"] = (
        "internal.territorial.point_resolution"
    )
    query_coordinates: GeocodingCoordinatesResponse
    result: TerritorialPointResolutionResultResponse | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class GeocodingTerritorialContextResponse(BaseModel):
    country_code: str | None = None
    autonomous_community_code: str | None = None
    province_code: str | None = None
    municipality_code: str | None = None
    country_name: str | None = None
    autonomous_community_name: str | None = None
    province_name: str | None = None
    municipality_name: str | None = None


class GeocodingTerritorialResolutionResponse(BaseModel):
    territorial_unit_id: int | None = None
    matched_by: Literal["code", "alias", "canonical_name"] | None = None
    canonical_name: str | None = None
    canonical_code: str | None = None
    source_system: str | None = None
    unit_level: str | None = None


class GeocodeResultResponse(BaseModel):
    label: str
    entity_type: str
    coordinates: GeocodingCoordinatesResponse
    address: str | None = None
    postal_code: str | None = None
    territorial_context: GeocodingTerritorialContextResponse = Field(
        default_factory=GeocodingTerritorialContextResponse
    )
    territorial_resolution: GeocodingTerritorialResolutionResponse | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class GeocodeResponse(BaseModel):
    source: str
    query: str
    cached: bool = False
    result: GeocodeResultResponse | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ReverseGeocodeResultResponse(BaseModel):
    label: str
    entity_type: str
    coordinates: GeocodingCoordinatesResponse
    address: str | None = None
    postal_code: str | None = None
    territorial_context: GeocodingTerritorialContextResponse = Field(
        default_factory=GeocodingTerritorialContextResponse
    )
    territorial_resolution: GeocodingTerritorialResolutionResponse | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ReverseGeocodeResponse(BaseModel):
    source: str
    query_coordinates: GeocodingCoordinatesResponse
    cached: bool = False
    result: ReverseGeocodeResultResponse | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class QAIncidentItem(BaseModel):
    id: int
    layer: str
    entity_id: str
    error_type: str
    severity: str
    description: str
    source_provider: str
    detected_at: datetime
    resolved: bool
    resolved_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class QAIncidentsResponse(BaseModel):
    items: list[QAIncidentItem]
    total: int
    page: int
    page_size: int
    pages: int
    has_next: bool
    has_previous: bool
    filters: dict[str, Any] = Field(default_factory=dict)


class ErrorDetail(BaseModel):
    message: str
    request_id: str | None = None


class ErrorResponse(BaseModel):
    detail: ErrorDetail
