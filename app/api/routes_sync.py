from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.logging import get_logger
from app.core.jobs import BaseJobStore
from app.dependencies import (
    get_ine_operation_governance_repository,
    get_ine_operation_governance_history_repository,
    get_job_store,
    get_settings,
    require_api_key,
)
from app.repositories.ine_operation_governance import INEOperationGovernanceRepository
from app.repositories.ine_operation_governance_history import (
    INEOperationGovernanceHistoryRepository,
)
from app.schemas import (
    INESyncOperationCatalogFiltersResponse,
    INESyncOperationCatalogItemResponse,
    INESyncOperationCatalogResponse,
    INESyncOperationCatalogSummaryResponse,
    INESyncOperationHistoryItemResponse,
    INESyncOperationHistoryResponse,
    INESyncOperationHistorySummaryResponse,
    INESyncOperationOverrideRequest,
)
from app.services.ine_operation_governance import (
    INE_EXECUTION_PROFILE_SCHEDULED,
    INE_MANUAL_OVERRIDE_DECISION_SOURCE,
    build_ine_operation_history_event,
    derive_schedule_enabled_for_profile,
    filter_ine_operation_profiles,
    merge_ine_operation_profiles,
    paginate_ine_operation_history_events,
    paginate_ine_operation_profiles,
    resolve_effective_ine_operation_profile,
    summarize_ine_operation_history_events,
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


async def _load_ine_operation_profile_item(
    settings: Settings,
    ine_governance_repo: INEOperationGovernanceRepository,
    *,
    operation_code: str,
    log_event: str,
) -> dict[str, Any]:
    try:
        persisted_profile = await ine_governance_repo.get_by_operation_code(operation_code)
    except Exception:
        logger.exception(log_event, extra={"operation_code": operation_code})
        persisted_profile = None
    return resolve_effective_ine_operation_profile(
        settings,
        operation_code,
        persisted_profile,
    )


async def _commit_repo_session_or_rollback(repo: Any) -> None:
    session = getattr(repo, "session", None)
    if session is None:
        return
    try:
        await session.commit()
    except Exception:
        await session.rollback()
        raise


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


@router.post(
    "/sync/ine/operations/{operation_code}/override",
    response_model=INESyncOperationCatalogItemResponse,
    summary="Persist a manual operational override for an INE operation",
)
async def set_ine_operation_override(
    operation_code: str,
    request: INESyncOperationOverrideRequest,
    settings: Settings = Depends(get_settings),
    ine_governance_repo: INEOperationGovernanceRepository = Depends(
        get_ine_operation_governance_repository
    ),
    ine_governance_history_repo: INEOperationGovernanceHistoryRepository = Depends(
        get_ine_operation_governance_history_repository
    ),
) -> INESyncOperationCatalogItemResponse:
    before_item = await _load_ine_operation_profile_item(
        settings,
        ine_governance_repo,
        operation_code=operation_code,
        log_event="sync_ine_override_before_lookup_failed",
    )
    decision_reason = request.decision_reason.strip()
    if not decision_reason:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "message": "decision_reason must not be blank.",
                "operation_code": operation_code,
            },
        )
    effective_schedule_enabled = request.schedule_enabled
    if effective_schedule_enabled is None:
        effective_schedule_enabled = derive_schedule_enabled_for_profile(request.execution_profile)
    if (
        request.execution_profile != INE_EXECUTION_PROFILE_SCHEDULED
        and effective_schedule_enabled is True
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "message": "schedule_enabled=true is only valid for execution_profile='scheduled'.",
                "operation_code": operation_code,
            },
        )
    if (
        request.execution_profile == INE_EXECUTION_PROFILE_SCHEDULED
        and effective_schedule_enabled is False
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "message": "schedule_enabled=false is not valid for execution_profile='scheduled'.",
                "operation_code": operation_code,
            },
        )
    try:
        persisted = await ine_governance_repo.set_override(
            operation_code=operation_code,
            execution_profile=request.execution_profile,
            schedule_enabled=effective_schedule_enabled,
            decision_reason=decision_reason,
            decision_source=INE_MANUAL_OVERRIDE_DECISION_SOURCE,
            commit=False,
        )
        item = resolve_effective_ine_operation_profile(settings, operation_code, persisted)
        await ine_governance_history_repo.append_event(
            **build_ine_operation_history_event(operation_code, before_item, item),
            commit=False,
        )
        await _commit_repo_session_or_rollback(ine_governance_repo)
    except Exception:
        session = getattr(ine_governance_repo, "session", None)
        if session is not None:
            await session.rollback()
        raise
    return INESyncOperationCatalogItemResponse(**item)


@router.delete(
    "/sync/ine/operations/{operation_code}/override",
    response_model=INESyncOperationCatalogItemResponse,
    summary="Clear a manual operational override for an INE operation",
)
async def clear_ine_operation_override(
    operation_code: str,
    settings: Settings = Depends(get_settings),
    ine_governance_repo: INEOperationGovernanceRepository = Depends(
        get_ine_operation_governance_repository
    ),
    ine_governance_history_repo: INEOperationGovernanceHistoryRepository = Depends(
        get_ine_operation_governance_history_repository
    ),
) -> INESyncOperationCatalogItemResponse:
    before_item = await _load_ine_operation_profile_item(
        settings,
        ine_governance_repo,
        operation_code=operation_code,
        log_event="sync_ine_override_before_clear_lookup_failed",
    )
    try:
        persisted = await ine_governance_repo.clear_override(operation_code, commit=False)
        if persisted is None:
            item = before_item
        else:
            item = resolve_effective_ine_operation_profile(settings, operation_code, persisted)
            if before_item.get("override_active"):
                await ine_governance_history_repo.append_event(
                    **build_ine_operation_history_event(operation_code, before_item, item),
                    commit=False,
                )
                await _commit_repo_session_or_rollback(ine_governance_repo)
    except Exception:
        session = getattr(ine_governance_repo, "session", None)
        if session is not None:
            await session.rollback()
        raise
    if persisted is None:
        item = before_item
    return INESyncOperationCatalogItemResponse(**item)


@router.get(
    "/sync/ine/operations/{operation_code}/history",
    response_model=INESyncOperationHistoryResponse,
    summary="Get override history for a governed INE operation",
)
async def get_ine_operation_history(
    operation_code: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    ine_governance_history_repo: INEOperationGovernanceHistoryRepository = Depends(
        get_ine_operation_governance_history_repository
    ),
) -> INESyncOperationHistoryResponse:
    events = await ine_governance_history_repo.list_by_operation_code(
        operation_code,
        page=page,
        page_size=page_size,
    )
    summary_data = await ine_governance_history_repo.summarize_by_operation_code(operation_code)
    pagination = paginate_ine_operation_history_events(
        total=summary_data["events_total"],
        page=page,
        page_size=page_size,
    )
    return INESyncOperationHistoryResponse(
        generated_at=datetime.now(UTC),
        operation_code=operation_code,
        summary=INESyncOperationHistorySummaryResponse(
            **summarize_ine_operation_history_events(
                events,
                events_total=summary_data["events_total"],
            )
            | {
                "override_set_total": summary_data["override_set_total"],
                "override_updated_total": summary_data["override_updated_total"],
                "override_cleared_total": summary_data["override_cleared_total"],
            }
        ),
        items=[INESyncOperationHistoryItemResponse(**item) for item in events],
        pagination=pagination,
        metadata={"returned_events": len(events)},
    )
