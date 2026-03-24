from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, Query

from app.core.logging import get_logger
from app.core.jobs import BaseJobStore
from app.dependencies import (
    get_ine_operation_governance_repository,
    get_job_store,
    get_settings,
    require_api_key,
)
from app.repositories.ine_operation_governance import INEOperationGovernanceRepository
from app.schemas import (
    INESyncOperationCatalogFiltersResponse,
    INESyncOperationCatalogItemResponse,
    INESyncOperationCatalogResponse,
    INESyncOperationCatalogSummaryResponse,
)
from app.services.ine_operation_governance import (
    filter_ine_operation_profiles,
    merge_ine_operation_profiles,
    paginate_ine_operation_profiles,
    summarize_ine_operation_profiles,
)
from app.settings import Settings


router = APIRouter(tags=["sync"], dependencies=[Depends(require_api_key)])
logger = get_logger("app.api.routes_sync")


async def _load_ine_operation_profiles(
    settings: Settings,
    ine_governance_repo: INEOperationGovernanceRepository,
    *,
    log_event: str,
) -> list[dict[str, Any]]:
    try:
        persisted_operation_profiles = await ine_governance_repo.list_all()
    except Exception:
        logger.exception(log_event)
        persisted_operation_profiles = []
    return merge_ine_operation_profiles(settings, persisted_operation_profiles)


@router.get(
    "/sync/status",
    summary="Get sync schedule and worker status",
    description=(
        "Reports the configured synchronization sources, their cron schedules, "
        "and the live worker heartbeat status."
    ),
)
async def get_sync_status(
    settings: Settings = Depends(get_settings),
    job_store: BaseJobStore = Depends(get_job_store),
    ine_governance_repo: INEOperationGovernanceRepository = Depends(
        get_ine_operation_governance_repository
    ),
) -> dict[str, Any]:
    worker_status = await job_store.get_worker_status(settings.job_queue_name)
    operation_profiles = await _load_ine_operation_profiles(
        settings,
        ine_governance_repo,
        log_event="sync_status_governance_lookup_failed",
    )

    sources = [
        {
            "source": "ine",
            "job_type": "operation_asturias_ingestion",
            "schedule": "daily 03:00 UTC",
            "operations": list(settings.scheduled_ine_operations),
            "execution_profiles_available": len(operation_profiles),
            "enabled": bool(settings.scheduled_ine_operations),
        },
        {
            "source": "sadei",
            "job_type": "sadei_sync",
            "schedule": "daily 05:00 UTC",
            "datasets": list(settings.sadei_sync_datasets),
            "enabled": bool(settings.sadei_sync_datasets),
        },
        {
            "source": "ideas",
            "job_type": "ideas_sync",
            "schedule": "monday 04:30 UTC",
            "layers": list(settings.ideas_sync_layers),
            "enabled": bool(settings.ideas_sync_layers),
        },
        {
            "source": "territorial",
            "job_type": "territorial_boundary_sync",
            "schedule": "monday 04:00 UTC",
            "enabled": settings.scheduled_territorial_sync_enabled,
        },
    ]

    return {
        "worker": worker_status,
        "sources": sources,
        "operation_profiles": operation_profiles,
    }


@router.get(
    "/sync/ine/operations",
    response_model=INESyncOperationCatalogResponse,
    summary="Get the governed operational catalog for INE operations",
    description=(
        "Returns the operational profile and latest observed execution state for INE "
        "Asturias ingestion operations."
    ),
)
async def get_ine_operation_catalog(
    operation_code: str | None = Query(default=None),
    execution_profile: str | None = Query(
        default=None,
        pattern="^(scheduled|background_only|manual_only|discarded)$",
    ),
    last_run_status: str | None = Query(
        default=None,
        pattern="^(queued|running|completed|failed)$",
    ),
    schedule_enabled: bool | None = Query(default=None),
    include_unclassified: bool = Query(default=True),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    settings: Settings = Depends(get_settings),
    ine_governance_repo: INEOperationGovernanceRepository = Depends(
        get_ine_operation_governance_repository
    ),
) -> INESyncOperationCatalogResponse:
    operation_profiles = await _load_ine_operation_profiles(
        settings,
        ine_governance_repo,
        log_event="sync_ine_catalog_governance_lookup_failed",
    )
    filtered_profiles = filter_ine_operation_profiles(
        operation_profiles,
        operation_code=operation_code,
        execution_profile=execution_profile,
        last_run_status=last_run_status,
        schedule_enabled=schedule_enabled,
        include_unclassified=include_unclassified,
    )
    paginated_profiles, pagination = paginate_ine_operation_profiles(
        filtered_profiles,
        page=page,
        page_size=page_size,
    )
    configured_profiles = filter_ine_operation_profiles(
        operation_profiles,
        include_unclassified=False,
    )
    return INESyncOperationCatalogResponse(
        generated_at=datetime.now(UTC),
        summary=INESyncOperationCatalogSummaryResponse(
            **summarize_ine_operation_profiles(filtered_profiles)
        ),
        items=[INESyncOperationCatalogItemResponse(**item) for item in paginated_profiles],
        filters=INESyncOperationCatalogFiltersResponse(
            operation_code=operation_code,
            execution_profile=execution_profile,
            last_run_status=last_run_status,
            schedule_enabled=schedule_enabled,
            include_unclassified=include_unclassified,
            page=page,
            page_size=page_size,
        ),
        pagination=pagination,
        metadata={
            "configured_operations_total": len(configured_profiles),
            "merged_operations_total": len(operation_profiles),
        },
    )
