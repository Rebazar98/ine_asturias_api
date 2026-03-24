from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from app.core.logging import get_logger
from app.core.jobs import BaseJobStore
from app.dependencies import (
    get_ine_operation_governance_repository,
    get_job_store,
    get_settings,
    require_api_key,
)
from app.repositories.ine_operation_governance import INEOperationGovernanceRepository
from app.services.ine_operation_governance import merge_ine_operation_profiles
from app.settings import Settings


router = APIRouter(tags=["sync"], dependencies=[Depends(require_api_key)])
logger = get_logger("app.api.routes_sync")


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
    try:
        persisted_operation_profiles = await ine_governance_repo.list_all()
    except Exception:
        logger.exception("sync_status_governance_lookup_failed")
        persisted_operation_profiles = []
    operation_profiles = merge_ine_operation_profiles(settings, persisted_operation_profiles)

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
