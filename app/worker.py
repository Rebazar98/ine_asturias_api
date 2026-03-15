from __future__ import annotations

import asyncio
import json
import socket
from time import perf_counter
from typing import Any

import httpx
from prometheus_client import start_http_server
from redis.asyncio import Redis

from app.core.cache import InMemoryTTLCache, LayeredCache, RedisTTLCache
from app.core.jobs import RedisJobStore
from app.core.logging import configure_logging, get_logger
from app.core.metrics import record_job_duration
from app.core.redis import redis_settings_from_url
from app.core.resilience import AsyncCircuitBreaker
from app.db import dispose_db, init_db, session_scope
from app.repositories.analytics_snapshots import AnalyticalSnapshotRepository
from app.repositories.catalog import TableCatalogRepository
from app.repositories.ingestion import IngestionRepository
from app.repositories.series import SeriesRepository
from app.repositories.territorial_export_artifacts import TerritorialExportArtifactRepository
from app.repositories.territorial import TerritorialRepository
from app.services.asturias_resolver import AsturiasResolutionError, AsturiasResolver
from app.services.ine_client import INEClientError, INEClientService
from app.services.ine_operation_ingestion import INEOperationIngestionService
from app.services.territorial_analytics import (
    MUNICIPALITY_REPORT_TYPE,
    TerritorialAnalyticsService,
)
from app.services.territorial_exports import TERRITORIAL_EXPORT_JOB_TYPE, TerritorialExportService
from app.settings import get_settings


logger = get_logger("app.worker")


async def run_operation_asturias_job(
    ctx: dict[str, Any], job_id: str, payload: dict[str, Any]
) -> dict[str, Any] | None:
    settings = ctx["settings"]
    job_store: RedisJobStore = ctx["job_store"]
    ine_client: INEClientService = ctx["ine_client"]
    resolver: AsturiasResolver = ctx["resolver"]

    op_code = payload["operation_code"]
    started_at = perf_counter()

    async def report_progress(progress: dict[str, Any]) -> None:
        await job_store.update_progress(job_id, **progress)

    try:
        await job_store.mark_running(job_id)
        await report_progress({"stage": "resolving_asturias", "operation_code": op_code})
        resolution = await resolver.resolve(
            op_code=op_code,
            geo_variable_id=payload.get("geo_variable_id"),
            asturias_value_id=payload.get("asturias_value_id"),
        )
        await report_progress(
            {
                "stage": "resolution_completed",
                "geo_variable_id": resolution.geo_variable_id,
                "asturias_value_id": resolution.asturias_value_id,
            }
        )

        async with session_scope() as session:
            ingestion_repo = IngestionRepository(session=session)
            series_repo = SeriesRepository(session=session)
            catalog_repo = TableCatalogRepository(session=session)
            ingestion_service = INEOperationIngestionService(
                ingestion_repo=ingestion_repo,
                series_repo=series_repo,
                catalog_repo=catalog_repo,
            )
            result = await ingestion_service.ingest_asturias_operation(
                op_code=op_code,
                resolution=resolution,
                nult=payload.get("nult"),
                det=payload.get("det"),
                tip=payload.get("tip"),
                periodicidad=payload.get("periodicidad"),
                max_tables=payload.get("max_tables"),
                skip_known_no_data=payload.get("skip_known_no_data", False),
                ine_client=ine_client,
                max_concurrent_table_fetches=settings.max_concurrent_table_fetches,
                progress_reporter=report_progress,
            )

        await job_store.complete_job(job_id, result)
        record_job_duration(
            "operation_asturias_ingestion",
            "completed",
            perf_counter() - started_at,
        )
        logger.info(
            "asturias_worker_job_completed",
            extra={"job_id": job_id, "operation_code": op_code, "app_env": settings.app_env},
        )
        return result
    except (AsturiasResolutionError, INEClientError) as exc:
        await job_store.fail_job(job_id, exc.detail)
        record_job_duration(
            "operation_asturias_ingestion",
            "failed",
            perf_counter() - started_at,
        )
        logger.warning(
            "asturias_worker_job_failed",
            extra={"job_id": job_id, "operation_code": op_code, "error": exc.detail},
        )
    except Exception as exc:
        logger.exception(
            "asturias_worker_job_unexpected_error",
            extra={"job_id": job_id, "operation_code": op_code},
        )
        detail = (
            exc.args[0]
            if exc.args and isinstance(exc.args[0], dict)
            else {
                "message": "Unexpected error while processing Asturias operation data.",
                "operation_code": op_code,
                "error": str(exc),
            }
        )
        await job_store.fail_job(job_id, detail)
        record_job_duration(
            "operation_asturias_ingestion",
            "failed",
            perf_counter() - started_at,
        )
    return None


async def run_municipality_report_job(
    ctx: dict[str, Any], job_id: str, payload: dict[str, Any]
) -> dict[str, Any] | None:
    settings = ctx["settings"]
    job_store: RedisJobStore = ctx["job_store"]
    municipality_code = payload["municipality_code"]
    started_at = perf_counter()

    async def report_progress(progress: dict[str, Any]) -> None:
        await job_store.update_progress(job_id, **progress)

    try:
        await job_store.mark_running(job_id)
        await report_progress(
            {
                "stage": "starting_report",
                "municipality_code": municipality_code,
                "report_type": MUNICIPALITY_REPORT_TYPE,
            }
        )

        async with session_scope() as session:
            territorial_repo = TerritorialRepository(session=session)
            series_repo = SeriesRepository(session=session)
            analytical_snapshot_repo = AnalyticalSnapshotRepository(session=session)
            analytics_service = TerritorialAnalyticsService(
                territorial_repo=territorial_repo,
                series_repo=series_repo,
                analytical_snapshot_repo=analytical_snapshot_repo,
                analytical_snapshot_ttl_seconds=settings.analytical_snapshot_ttl_seconds,
            )
            report = await analytics_service.build_municipality_report(
                municipality_code=municipality_code,
                operation_code=payload.get("operation_code"),
                variable_id=payload.get("variable_id"),
                period_from=payload.get("period_from"),
                period_to=payload.get("period_to"),
                page=payload.get("page", 1),
                page_size=payload.get("page_size", 50),
                progress_reporter=report_progress,
            )

        if report is None:
            await job_store.fail_job(
                job_id,
                {
                    "message": "Municipality code was not found.",
                    "codigo_ine": municipality_code,
                },
            )
            logger.warning(
                "municipality_report_worker_job_not_found",
                extra={
                    "job_id": job_id,
                    "municipality_code": municipality_code,
                    "duration_ms": round((perf_counter() - started_at) * 1000, 3),
                },
            )
            return None

        payload_result = report.model_dump(mode="json")
        await job_store.complete_job(job_id, payload_result)
        record_job_duration(
            "territorial_municipality_report",
            "completed",
            perf_counter() - started_at,
        )
        logger.info(
            "municipality_report_worker_job_completed",
            extra={
                "job_id": job_id,
                "municipality_code": municipality_code,
                "series_count": len(report.series),
                "storage_mode": report.metadata.get("storage_mode"),
                "result_bytes": len(json.dumps(payload_result, default=str).encode("utf-8")),
                "duration_ms": round((perf_counter() - started_at) * 1000, 3),
            },
        )
        return payload_result
    except Exception as exc:
        logger.exception(
            "municipality_report_worker_job_unexpected_error",
            extra={
                "job_id": job_id,
                "municipality_code": municipality_code,
                "duration_ms": round((perf_counter() - started_at) * 1000, 3),
            },
        )
        await job_store.fail_job(
            job_id,
            {
                "message": "Unexpected error while generating the municipality report.",
                "codigo_ine": municipality_code,
                "error": str(exc),
            },
        )
        record_job_duration(
            "territorial_municipality_report",
            "failed",
            perf_counter() - started_at,
        )
    return None


async def run_territorial_export_job(
    ctx: dict[str, Any], job_id: str, payload: dict[str, Any]
) -> dict[str, Any] | None:
    settings = ctx["settings"]
    job_store: RedisJobStore = ctx["job_store"]
    started_at = perf_counter()

    async def report_progress(progress: dict[str, Any]) -> None:
        await job_store.update_progress(job_id, **progress)

    try:
        await job_store.mark_running(job_id)
        await report_progress(
            {
                "stage": "starting_export",
                "unit_level": payload["unit_level"],
                "code_value": payload["code_value"],
                "format": payload.get("format", "zip"),
            }
        )

        async with session_scope() as session:
            territorial_repo = TerritorialRepository(session=session)
            series_repo = SeriesRepository(session=session)
            analytical_snapshot_repo = AnalyticalSnapshotRepository(session=session)
            artifact_repo = TerritorialExportArtifactRepository(session=session)
            analytics_service = TerritorialAnalyticsService(
                territorial_repo=territorial_repo,
                series_repo=series_repo,
                analytical_snapshot_repo=analytical_snapshot_repo,
                analytical_snapshot_ttl_seconds=settings.analytical_snapshot_ttl_seconds,
            )
            export_service = TerritorialExportService(
                territorial_repo=territorial_repo,
                series_repo=series_repo,
                analytics_service=analytics_service,
                artifact_repo=artifact_repo,
                export_ttl_seconds=settings.territorial_export_ttl_seconds,
            )
            result = await export_service.build_export(
                job_id=job_id,
                unit_level=payload["unit_level"],
                code_value=payload["code_value"],
                artifact_format=payload.get("format", "zip"),
                include_providers=payload.get("include_providers"),
                progress_reporter=report_progress,
            )

        if result is None:
            await job_store.fail_job(
                job_id,
                {
                    "message": "Territorial unit code was not found.",
                    "unit_level": payload["unit_level"],
                    "code_value": payload["code_value"],
                },
            )
            logger.warning(
                "territorial_export_worker_job_not_found",
                extra={
                    "job_id": job_id,
                    "unit_level": payload["unit_level"],
                    "code_value": payload["code_value"],
                },
            )
            record_job_duration(
                TERRITORIAL_EXPORT_JOB_TYPE,
                "failed",
                perf_counter() - started_at,
            )
            return None

        payload_result = result.model_dump(mode="json")
        await job_store.complete_job(job_id, payload_result)
        record_job_duration(
            TERRITORIAL_EXPORT_JOB_TYPE,
            "completed",
            perf_counter() - started_at,
        )
        logger.info(
            "territorial_export_worker_job_completed",
            extra={
                "job_id": job_id,
                "unit_level": payload["unit_level"],
                "code_value": payload["code_value"],
                "export_id": result.export_id,
                "byte_size": result.summary.get("byte_size"),
                "artifact_reused": result.summary.get("artifact_reused"),
            },
        )
        return payload_result
    except Exception as exc:
        logger.exception(
            "territorial_export_worker_job_unexpected_error",
            extra={
                "job_id": job_id,
                "unit_level": payload.get("unit_level"),
                "code_value": payload.get("code_value"),
            },
        )
        await job_store.fail_job(
            job_id,
            {
                "message": "Unexpected error while generating the territorial export.",
                "unit_level": payload.get("unit_level"),
                "code_value": payload.get("code_value"),
                "error": str(exc),
            },
        )
        record_job_duration(
            TERRITORIAL_EXPORT_JOB_TYPE,
            "failed",
            perf_counter() - started_at,
        )
    return None


async def startup(ctx: dict[str, Any]) -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    init_db(settings)

    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    job_store = RedisJobStore(redis=redis, settings=settings)
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(
            settings.http_timeout_seconds, connect=min(settings.http_timeout_seconds, 5.0)
        ),
    )
    local_cache = InMemoryTTLCache(
        enabled=settings.enable_cache,
        default_ttl_seconds=settings.cache_ttl_seconds,
    )
    cache = LayeredCache(
        local_cache=local_cache,
        shared_cache=RedisTTLCache(
            redis=redis,
            enabled=settings.enable_cache,
            default_ttl_seconds=settings.cache_ttl_seconds,
            namespace="provider-cache",
        ),
    )
    ine_client = INEClientService(
        http_client=http_client,
        settings=settings,
        cache=cache,
        circuit_breaker=AsyncCircuitBreaker(
            provider="ine",
            fail_max=settings.provider_circuit_breaker_failures,
            reset_timeout_seconds=settings.provider_circuit_breaker_recovery_seconds,
            half_open_sample_size=settings.provider_circuit_breaker_half_open_sample_size,
            success_threshold=settings.provider_circuit_breaker_success_threshold,
        ),
    )
    resolver = AsturiasResolver(ine_client=ine_client, cache=cache)

    ctx["settings"] = settings
    ctx["redis"] = redis
    ctx["job_store"] = job_store
    ctx["http_client"] = http_client
    ctx["cache"] = cache
    ctx["ine_client"] = ine_client
    ctx["resolver"] = resolver
    ctx["worker_id"] = f"{socket.gethostname()}:{settings.job_queue_name}"
    ctx["heartbeat_task"] = asyncio.create_task(
        _heartbeat_loop(
            job_store,
            settings.job_queue_name,
            ctx["worker_id"],
            settings.worker_heartbeat_ttl_seconds,
        )
    )
    ctx["metrics_server"] = _start_worker_metrics_server(settings.worker_metrics_port)
    logger.info("worker_started", extra={"queue_name": settings.job_queue_name})


async def shutdown(ctx: dict[str, Any]) -> None:
    heartbeat_task: asyncio.Task[Any] | None = ctx.get("heartbeat_task")
    if heartbeat_task is not None:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass

    metrics_server = ctx.get("metrics_server")
    if metrics_server is not None:
        try:
            metrics_server.shutdown()
            metrics_server.server_close()
        except Exception:
            logger.warning("worker_metrics_server_shutdown_failed")

    http_client: httpx.AsyncClient | None = ctx.get("http_client")
    if http_client is not None:
        await http_client.aclose()

    redis: Redis | None = ctx.get("redis")
    if redis is not None:
        await redis.aclose()

    await dispose_db()
    logger.info("worker_stopped")


async def _heartbeat_loop(
    job_store: RedisJobStore, queue_name: str, worker_id: str, ttl_seconds: int
) -> None:
    interval = max(ttl_seconds / 2, 5)
    while True:
        try:
            await job_store.record_worker_heartbeat(queue_name=queue_name, worker_id=worker_id)
        except Exception:
            logger.warning(
                "worker_heartbeat_loop_iteration_failed",
                extra={"queue_name": queue_name, "worker_id": worker_id},
            )
        await asyncio.sleep(interval)


def _start_worker_metrics_server(port: int):
    try:
        server, _thread = start_http_server(port, addr="0.0.0.0")
        logger.info("worker_metrics_server_started", extra={"port": port})
        return server
    except Exception:
        logger.warning("worker_metrics_server_failed", extra={"port": port})
        return None


class WorkerSettings:
    functions = [
        run_operation_asturias_job,
        run_municipality_report_job,
        run_territorial_export_job,
    ]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = redis_settings_from_url(get_settings().redis_url or "redis://localhost:6379/0")
    queue_name = get_settings().job_queue_name
