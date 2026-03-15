from __future__ import annotations

from fastapi import APIRouter, Depends, Request, Response, status

from app.core.jobs import InMemoryJobStore
from app.core.logging import get_logger
from app.core.metrics import merge_metrics_payloads, metrics_content_type, metrics_payload
from app.db import ping_database
from app.dependencies import require_api_key
from app.schemas import HealthResponse, ReadinessComponentResponse, ReadinessResponse


router = APIRouter(tags=["health"])
logger = get_logger("app.api.routes_health")
WORKER_METRICS_TIMEOUT_SECONDS = 2.0


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    return HealthResponse(status="ok")


@router.get("/health/ready", response_model=ReadinessResponse)
async def readiness_check(request: Request) -> Response | ReadinessResponse:
    settings = request.app.state.settings
    job_store = request.app.state.job_store

    components: dict[str, ReadinessComponentResponse] = {}
    overall_ready = True

    if settings.postgres_dsn:
        db_ok = await ping_database()
        components["postgres"] = ReadinessComponentResponse(
            status="ok" if db_ok else "error",
            details={"configured": True},
        )
        overall_ready = overall_ready and db_ok
    else:
        components["postgres"] = ReadinessComponentResponse(
            status="disabled",
            details={"configured": False},
        )

    redis_configured = bool(settings.redis_url)
    if redis_configured:
        redis_ok = await job_store.ping()
        components["redis"] = ReadinessComponentResponse(
            status="ok" if redis_ok else "error",
            details={"configured": True},
        )
        overall_ready = overall_ready and redis_ok

        worker_status = await job_store.get_worker_status(settings.job_queue_name)
        worker_ok = worker_status.get("status") == "ok"
        components["worker"] = ReadinessComponentResponse(
            status="ok" if worker_ok else "error",
            details=worker_status,
        )
        overall_ready = overall_ready and worker_ok
    else:
        components["redis"] = ReadinessComponentResponse(
            status="disabled",
            details={"configured": False},
        )
        worker_mode = "disabled"
        if isinstance(job_store, InMemoryJobStore):
            worker_mode = "disabled"
        components["worker"] = ReadinessComponentResponse(
            status="disabled",
            details={"mode": worker_mode},
        )

    response = ReadinessResponse(
        status="ok" if overall_ready else "degraded",
        app_env=settings.app_env,
        components=components,
    )
    status_code = status.HTTP_200_OK if overall_ready else status.HTTP_503_SERVICE_UNAVAILABLE
    return Response(
        content=response.model_dump_json(),
        status_code=status_code,
        media_type="application/json",
    )


@router.get("/metrics")
async def get_metrics(
    request: Request,
    _: None = Depends(require_api_key),
) -> Response:
    settings = request.app.state.settings
    api_payload = metrics_payload()
    worker_payloads: list[bytes] = []

    if settings.worker_metrics_url:
        try:
            response = await request.app.state.http_client.get(
                settings.worker_metrics_url,
                timeout=min(settings.http_timeout_seconds, WORKER_METRICS_TIMEOUT_SECONDS),
            )
            response.raise_for_status()
            worker_payloads.append(response.content)
        except Exception as exc:
            logger.warning(
                "worker_metrics_fetch_failed",
                extra={"worker_metrics_url": settings.worker_metrics_url, "error": str(exc)},
            )

    merged_payload = merge_metrics_payloads(api_payload, worker_payloads)
    return Response(content=merged_payload, media_type=metrics_content_type())
