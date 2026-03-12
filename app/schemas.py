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


class INESeriesListResponse(BaseModel):
    items: list[INESeriesListItemResponse] = Field(default_factory=list)
    total: int
    page: int
    page_size: int
