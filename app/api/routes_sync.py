from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from app.core.jobs import BaseJobStore
from app.dependencies import get_job_store, get_settings, require_api_key
from app.settings import Settings


router = APIRouter(tags=["sync"], dependencies=[Depends(require_api_key)])


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
) -> dict[str, Any]:
    worker_status = await job_store.get_worker_status(settings.job_queue_name)

    sources = [
        {
            "source": "ine",
            "job_type": "operation_asturias_ingestion",
            "schedule": "daily 03:00 UTC",
            "operations": list(settings.scheduled_ine_operations),
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
    }
