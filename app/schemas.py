from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, RootModel


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


class AsturiasResolutionResult(BaseModel):
    geo_variable_id: str
    asturias_value_id: str
    variable_name: str | None = None
    asturias_label: str | None = None


class BackgroundJobAcceptedResponse(BaseModel):
    job_id: str
    job_type: str
    status: Literal["queued", "running", "completed", "failed"]
    background: Literal[True] = True
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
    params: dict[str, Any] = Field(default_factory=dict)
    progress: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] | list[Any] | None = None
    error: Any = None


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


class TerritorialUnitListFiltersResponse(BaseModel):
    unit_level: str
    country_code: str | None = None
    parent_id: int | None = None
    active_only: bool = True


class TerritorialUnitListResponse(BaseModel):
    items: list[TerritorialUnitSummaryResponse] = Field(default_factory=list)
    total: int
    page: int
    page_size: int
    pages: int
    has_next: bool
    has_previous: bool
    filters: TerritorialUnitListFiltersResponse


class GeocodingCoordinatesResponse(BaseModel):
    lat: float
    lon: float


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
    territorial_context: GeocodingTerritorialContextResponse = Field(default_factory=GeocodingTerritorialContextResponse)
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
    territorial_context: GeocodingTerritorialContextResponse = Field(default_factory=GeocodingTerritorialContextResponse)
    territorial_resolution: GeocodingTerritorialResolutionResponse | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ReverseGeocodeResponse(BaseModel):
    source: str
    query_coordinates: GeocodingCoordinatesResponse
    cached: bool = False
    result: ReverseGeocodeResultResponse | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
