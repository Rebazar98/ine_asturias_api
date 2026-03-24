from __future__ import annotations

import asyncio
import json
import socket
from datetime import UTC, datetime
from time import perf_counter
from typing import Any

import httpx
from arq import create_pool
from arq.cron import cron
from prometheus_client import start_http_server
from redis.asyncio import Redis

from app.core.cache import InMemoryTTLCache, LayeredCache, RedisTTLCache
from app.core.jobs import RedisJobStore
from app.core.logging import configure_logging, get_logger, request_id_var
from app.core.metrics import record_ine_operation_execution, record_job_duration
from app.core.redis import redis_settings_from_url
from app.core.resilience import AsyncCircuitBreaker
from app.db import dispose_db, init_db, session_scope
from app.repositories.analytics_snapshots import AnalyticalSnapshotRepository
from app.repositories.catastro_cache import (
    CatastroMunicipalityAggregateCacheRepository,
    CatastroTerritorialAggregateCacheRepository,
)
from app.repositories.catalog import TableCatalogRepository
from app.repositories.ingestion import IngestionRepository
from app.repositories.ine_operation_governance import INEOperationGovernanceRepository
from app.repositories.series import SeriesRepository
from app.repositories.territorial_export_artifacts import TerritorialExportArtifactRepository
from app.repositories.territorial import TerritorialRepository
from app.services.asturias_resolver import AsturiasResolutionError, AsturiasResolver
from app.services.catastro_client import CatastroClientError, CatastroClientService
from app.services.ideas_wfs_client import IDEASWFSClientError, IDEASWFSClientService
from app.services.ine_client import INEClientError, INEClientService
from app.services.ine_operation_governance import (
    list_effective_scheduled_ine_operation_codes,
    resolve_effective_ine_operation_profile,
)
from app.services.ine_operation_ingestion import INEOperationIngestionService
from app.services.sadei_client import SADEIClientError, SADEIClientService
from app.services.sadei_normalizers import normalize_sadei_dataset
from app.services.territorial_analytics import (
    MUNICIPALITY_REPORT_TYPE,
    TerritorialAnalyticsService,
)
from app.services.territorial_exports import TERRITORIAL_EXPORT_JOB_TYPE, TerritorialExportService
from app.settings import get_settings


logger = get_logger("app.worker")


def _ine_trigger_mode(payload: dict[str, Any]) -> str:
    return str(payload.get("_trigger_mode") or "background")


async def _resolve_effective_ine_governance_context(
    repo: INEOperationGovernanceRepository,
    settings,
    op_code: str,
) -> dict[str, Any]:
    persisted = await repo.get_by_operation_code(op_code)
    return resolve_effective_ine_operation_profile(settings, op_code, persisted)


async def _load_scheduled_ine_operation_codes(settings) -> list[str]:
    try:
        async with session_scope() as session:
            if session is None:
                return list(settings.scheduled_ine_operations)
            repo = INEOperationGovernanceRepository(session=session)
            persisted_profiles = await repo.list_all()
        return list_effective_scheduled_ine_operation_codes(settings, persisted_profiles)
    except Exception:
        logger.warning("scheduled_ine_update_governance_lookup_failed")
        return list(settings.scheduled_ine_operations)


def _extract_error_message(error: Any) -> str:
    if isinstance(error, dict):
        message = error.get("message")
        if message:
            return str(message)
        return json.dumps(error, default=str)
    if error is None:
        return ""
    return str(error)


async def _mark_ine_governance_running(
    *,
    settings,
    op_code: str,
    job_id: str,
    trigger_mode: str,
    background_forced: bool,
    background_reason: str | None,
) -> None:
    try:
        async with session_scope() as session:
            if session is None:
                return
            repo = INEOperationGovernanceRepository(session=session)
            governance = await _resolve_effective_ine_governance_context(repo, settings, op_code)
            await repo.mark_running(
                operation_code=op_code,
                execution_profile=governance["execution_profile"],
                schedule_enabled=governance["schedule_enabled"],
                decision_reason=governance["decision_reason"],
                decision_source=governance["decision_source"],
                metadata=governance["metadata"],
                job_id=job_id,
                trigger_mode=trigger_mode,
                background_forced=background_forced,
                background_reason=background_reason,
                started_at=datetime.now(UTC),
            )
    except Exception:
        logger.warning(
            "ine_operation_governance_running_failed",
            extra={"operation_code": op_code, "job_id": job_id},
        )


async def _mark_ine_governance_queued(
    *,
    settings,
    op_code: str,
    job_id: str,
    trigger_mode: str,
    background_forced: bool,
    background_reason: str | None,
) -> None:
    try:
        async with session_scope() as session:
            if session is None:
                return
            repo = INEOperationGovernanceRepository(session=session)
            governance = await _resolve_effective_ine_governance_context(repo, settings, op_code)
            await repo.mark_queued(
                operation_code=op_code,
                execution_profile=governance["execution_profile"],
                schedule_enabled=governance["schedule_enabled"],
                decision_reason=governance["decision_reason"],
                decision_source=governance["decision_source"],
                metadata=governance["metadata"],
                job_id=job_id,
                trigger_mode=trigger_mode,
                background_forced=background_forced,
                background_reason=background_reason,
            )
    except Exception:
        logger.warning(
            "ine_operation_governance_queued_failed",
            extra={"operation_code": op_code, "job_id": job_id},
        )


async def _mark_ine_governance_completed(
    *,
    settings,
    op_code: str,
    job_id: str,
    trigger_mode: str,
    background_forced: bool,
    background_reason: str | None,
    duration_ms: int,
    summary: dict[str, Any],
) -> None:
    try:
        async with session_scope() as session:
            if session is None:
                return
            repo = INEOperationGovernanceRepository(session=session)
            governance = await _resolve_effective_ine_governance_context(repo, settings, op_code)
            await repo.mark_completed(
                operation_code=op_code,
                execution_profile=governance["execution_profile"],
                schedule_enabled=governance["schedule_enabled"],
                decision_reason=governance["decision_reason"],
                decision_source=governance["decision_source"],
                metadata=governance["metadata"],
                job_id=job_id,
                trigger_mode=trigger_mode,
                background_forced=background_forced,
                background_reason=background_reason,
                finished_at=datetime.now(UTC),
                duration_ms=duration_ms,
                summary=summary,
            )
    except Exception:
        logger.warning(
            "ine_operation_governance_completed_failed",
            extra={"operation_code": op_code, "job_id": job_id},
        )


async def _mark_ine_governance_failed(
    *,
    settings,
    op_code: str,
    job_id: str,
    trigger_mode: str,
    background_forced: bool,
    background_reason: str | None,
    duration_ms: int,
    error: Any,
) -> None:
    try:
        async with session_scope() as session:
            if session is None:
                return
            repo = INEOperationGovernanceRepository(session=session)
            governance = await _resolve_effective_ine_governance_context(repo, settings, op_code)
            await repo.mark_failed(
                operation_code=op_code,
                execution_profile=governance["execution_profile"],
                schedule_enabled=governance["schedule_enabled"],
                decision_reason=governance["decision_reason"],
                decision_source=governance["decision_source"],
                metadata=governance["metadata"],
                job_id=job_id,
                trigger_mode=trigger_mode,
                background_forced=background_forced,
                background_reason=background_reason,
                finished_at=datetime.now(UTC),
                duration_ms=duration_ms,
                error_message=_extract_error_message(error),
            )
    except Exception:
        logger.warning(
            "ine_operation_governance_failed_recording_failed",
            extra={"operation_code": op_code, "job_id": job_id},
        )


async def run_operation_asturias_job(
    ctx: dict[str, Any], job_id: str, payload: dict[str, Any]
) -> dict[str, Any] | None:
    settings = ctx["settings"]
    job_store: RedisJobStore = ctx["job_store"]
    ine_client: INEClientService = ctx["ine_client"]
    resolver: AsturiasResolver = ctx["resolver"]

    _rid_token = request_id_var.set(payload.get("_request_id"))
    op_code = payload["operation_code"]
    started_at = perf_counter()
    trigger_mode = _ine_trigger_mode(payload)
    background_forced = bool(payload.get("background_forced", False))
    background_reason = payload.get("background_reason")

    async def report_progress(progress: dict[str, Any]) -> None:
        await job_store.update_progress(job_id, **progress)

    try:
        await job_store.mark_running(job_id)
        await _mark_ine_governance_running(
            settings=settings,
            op_code=op_code,
            job_id=job_id,
            trigger_mode=trigger_mode,
            background_forced=background_forced,
            background_reason=background_reason,
        )
        await report_progress({"stage": "resolving_asturias", "operation_code": op_code})
        resolution: Any = None
        try:
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
        except AsturiasResolutionError:
            logger.info(
                "asturias_worker_resolver_failed",
                extra={"op_code": op_code, "job_id": job_id},
            )

        async with session_scope() as session:
            ingestion_repo = IngestionRepository(session=session)
            series_repo = SeriesRepository(session=session)
            catalog_repo = TableCatalogRepository(session=session)
            ingestion_service = INEOperationIngestionService(
                ingestion_repo=ingestion_repo,
                series_repo=series_repo,
                catalog_repo=catalog_repo,
                default_geography_code=settings.default_geography_code,
                default_geography_name=settings.default_geography_name,
                series_direct_max_series=settings.ine_series_direct_max_series,
                series_direct_max_errors_to_persist=settings.ine_series_direct_max_errors_to_persist,
                raw_payload_max_bytes=settings.ine_raw_payload_max_bytes,
                table_abort_threshold=settings.ine_table_abort_threshold,
                table_background_only_threshold=settings.ine_table_background_only_threshold,
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
                skip_known_processed=payload.get("skip_known_processed", False),
                ine_client=ine_client,
                max_concurrent_table_fetches=settings.max_concurrent_table_fetches,
                progress_reporter=report_progress,
                max_series=payload.get("max_series"),
                max_concurrent_series_fetches=settings.max_concurrent_series_fetches,
                background_mode=True,
            )

        await job_store.complete_job(job_id, result)
        duration_seconds = perf_counter() - started_at
        await _mark_ine_governance_completed(
            settings=settings,
            op_code=op_code,
            job_id=job_id,
            trigger_mode=trigger_mode,
            background_forced=background_forced,
            background_reason=background_reason,
            duration_ms=round(duration_seconds * 1000),
            summary=result.get("summary", {}),
        )
        record_ine_operation_execution(
            op_code,
            trigger_mode,
            "completed",
            duration_seconds,
        )
        record_job_duration(
            "operation_asturias_ingestion",
            "completed",
            duration_seconds,
        )
        logger.info(
            "asturias_worker_job_completed",
            extra={"job_id": job_id, "operation_code": op_code, "app_env": settings.app_env},
        )
        return result
    except (AsturiasResolutionError, INEClientError) as exc:
        await job_store.fail_job(job_id, exc.detail)
        duration_seconds = perf_counter() - started_at
        await _mark_ine_governance_failed(
            settings=settings,
            op_code=op_code,
            job_id=job_id,
            trigger_mode=trigger_mode,
            background_forced=background_forced,
            background_reason=background_reason,
            duration_ms=round(duration_seconds * 1000),
            error=exc.detail,
        )
        record_ine_operation_execution(
            op_code,
            trigger_mode,
            "failed",
            duration_seconds,
        )
        record_job_duration(
            "operation_asturias_ingestion",
            "failed",
            duration_seconds,
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
        duration_seconds = perf_counter() - started_at
        await _mark_ine_governance_failed(
            settings=settings,
            op_code=op_code,
            job_id=job_id,
            trigger_mode=trigger_mode,
            background_forced=background_forced,
            background_reason=background_reason,
            duration_ms=round(duration_seconds * 1000),
            error=detail,
        )
        record_ine_operation_execution(
            op_code,
            trigger_mode,
            "failed",
            duration_seconds,
        )
        record_job_duration(
            "operation_asturias_ingestion",
            "failed",
            duration_seconds,
        )
    finally:
        request_id_var.reset(_rid_token)
    return None


async def run_municipality_report_job(
    ctx: dict[str, Any], job_id: str, payload: dict[str, Any]
) -> dict[str, Any] | None:
    settings = ctx["settings"]
    job_store: RedisJobStore = ctx["job_store"]
    _rid_token = request_id_var.set(payload.get("_request_id"))
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
    finally:
        request_id_var.reset(_rid_token)
    return None


async def run_territorial_export_job(
    ctx: dict[str, Any], job_id: str, payload: dict[str, Any]
) -> dict[str, Any] | None:
    settings = ctx["settings"]
    job_store: RedisJobStore = ctx["job_store"]
    _rid_token = request_id_var.set(payload.get("_request_id"))
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
            catastro_cache_repo = CatastroMunicipalityAggregateCacheRepository(session=session)
            catastro_aggregate_cache_repo = CatastroTerritorialAggregateCacheRepository(
                session=session
            )
            artifact_repo = TerritorialExportArtifactRepository(session=session)
            ingestion_repo = IngestionRepository(session=session)
            analytics_service = TerritorialAnalyticsService(
                territorial_repo=territorial_repo,
                series_repo=series_repo,
                analytical_snapshot_repo=analytical_snapshot_repo,
                analytical_snapshot_ttl_seconds=settings.analytical_snapshot_ttl_seconds,
            )
            catastro_client = CatastroClientService(
                http_client=ctx["http_client"],
                settings=settings,
                cache=ctx["cache"],
                circuit_breaker=ctx["catastro_circuit_breaker"],
            )
            export_service = TerritorialExportService(
                territorial_repo=territorial_repo,
                series_repo=series_repo,
                analytics_service=analytics_service,
                catastro_client=catastro_client,
                catastro_cache_repo=catastro_cache_repo,
                catastro_aggregate_cache_repo=catastro_aggregate_cache_repo,
                ingestion_repo=ingestion_repo,
                artifact_repo=artifact_repo,
                export_ttl_seconds=settings.territorial_export_ttl_seconds,
                catastro_cache_ttl_seconds=settings.catastro_cache_ttl_seconds,
                catastro_aggregate_cache_ttl_seconds=settings.catastro_aggregate_cache_ttl_seconds,
                catastro_aggregate_max_concurrency=settings.catastro_aggregate_max_concurrency,
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
    except CatastroClientError as exc:
        await job_store.fail_job(job_id, exc.detail)
        record_job_duration(
            TERRITORIAL_EXPORT_JOB_TYPE,
            "failed",
            perf_counter() - started_at,
        )
        logger.warning(
            "territorial_export_worker_job_failed",
            extra={
                "job_id": job_id,
                "unit_level": payload.get("unit_level"),
                "code_value": payload.get("code_value"),
                "error": exc.detail,
            },
        )
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
    finally:
        request_id_var.reset(_rid_token)
    return None


async def run_sadei_sync_job(
    ctx: dict[str, Any], job_id: str, payload: dict[str, Any]
) -> dict[str, Any] | None:
    job_store: RedisJobStore = ctx["job_store"]
    sadei_client: SADEIClientService = ctx["sadei_client"]
    dataset_id: str = payload["dataset_id"]
    started_at = perf_counter()

    async def report_progress(progress: dict[str, Any]) -> None:
        await job_store.update_progress(job_id, **progress)

    try:
        await job_store.mark_running(job_id)
        await report_progress({"stage": "fetching_sadei", "dataset_id": dataset_id})

        rows = await sadei_client.fetch_dataset(dataset_id)
        items = normalize_sadei_dataset(rows, dataset_id)

        await report_progress({"stage": "upserting", "rows": len(items)})

        upserted = 0
        if items:
            async with session_scope() as session:
                series_repo = SeriesRepository(session=session)
                upserted = await series_repo.upsert_many(items)

        result = {
            "dataset_id": dataset_id,
            "rows_fetched": len(rows),
            "rows_normalized": len(items),
            "rows_upserted": upserted,
        }
        await job_store.complete_job(job_id, result)
        record_job_duration("sadei_sync", "completed", perf_counter() - started_at)
        logger.info("sadei_sync_job_completed", extra={"job_id": job_id, **result})
        return result
    except SADEIClientError as exc:
        await job_store.fail_job(job_id, {"message": exc.detail, "dataset_id": dataset_id})
        record_job_duration("sadei_sync", "failed", perf_counter() - started_at)
        logger.warning(
            "sadei_sync_job_failed",
            extra={"job_id": job_id, "dataset_id": dataset_id, "error": exc.detail},
        )
    except Exception as exc:
        logger.exception(
            "sadei_sync_job_unexpected_error", extra={"job_id": job_id, "dataset_id": dataset_id}
        )
        await job_store.fail_job(
            job_id,
            {
                "message": "Unexpected error in SADEI sync.",
                "dataset_id": dataset_id,
                "error": str(exc),
            },
        )
        record_job_duration("sadei_sync", "failed", perf_counter() - started_at)
    return None


async def run_ideas_sync_job(
    ctx: dict[str, Any], job_id: str, payload: dict[str, Any]
) -> dict[str, Any] | None:
    job_store: RedisJobStore = ctx["job_store"]
    ideas_client: IDEASWFSClientService = ctx["ideas_client"]
    layer_name: str = payload["layer_name"]
    started_at = perf_counter()

    async def report_progress(progress: dict[str, Any]) -> None:
        await job_store.update_progress(job_id, **progress)

    try:
        await job_store.mark_running(job_id)
        await report_progress({"stage": "fetching_ideas_wfs", "layer_name": layer_name})

        geojson = await ideas_client.fetch_layer(layer_name=layer_name)
        features = geojson.get("features", [])

        result = {
            "layer_name": layer_name,
            "features_fetched": len(features),
        }
        await job_store.complete_job(job_id, result)
        record_job_duration("ideas_sync", "completed", perf_counter() - started_at)
        logger.info("ideas_sync_job_completed", extra={"job_id": job_id, **result})
        return result
    except IDEASWFSClientError as exc:
        await job_store.fail_job(job_id, {"message": exc.detail, "layer_name": layer_name})
        record_job_duration("ideas_sync", "failed", perf_counter() - started_at)
        logger.warning(
            "ideas_sync_job_failed",
            extra={"job_id": job_id, "layer_name": layer_name, "error": exc.detail},
        )
    except Exception as exc:
        logger.exception(
            "ideas_sync_job_unexpected_error", extra={"job_id": job_id, "layer_name": layer_name}
        )
        await job_store.fail_job(
            job_id,
            {
                "message": "Unexpected error in IDEAS sync.",
                "layer_name": layer_name,
                "error": str(exc),
            },
        )
        record_job_duration("ideas_sync", "failed", perf_counter() - started_at)
    return None


async def scheduled_ine_update(ctx: dict[str, Any]) -> None:
    """Cron job: enqueue INE ingestion for each configured operation code."""
    settings = ctx["settings"]
    job_store: RedisJobStore = ctx["job_store"]
    arq_pool = ctx["arq_pool"]
    operation_codes = await _load_scheduled_ine_operation_codes(settings)
    for op_code in operation_codes:
        try:
            job_record = await job_store.create_job(
                "operation_asturias_ingestion",
                {"operation_code": op_code, "skip_known_processed": True},
            )
            job_id = job_record["job_id"]
            await _mark_ine_governance_queued(
                settings=settings,
                op_code=op_code,
                job_id=job_id,
                trigger_mode="scheduled",
                background_forced=False,
                background_reason=None,
            )
            await arq_pool.enqueue_job(
                "run_operation_asturias_job",
                job_id=job_id,
                payload={
                    "operation_code": op_code,
                    "skip_known_processed": True,
                    "_trigger_mode": "scheduled",
                },
            )
            logger.info(
                "scheduled_ine_update_enqueued",
                extra={
                    "operation_code": op_code,
                    "job_id": job_id,
                    "skip_known_processed": True,
                },
            )
        except Exception:
            logger.exception(
                "scheduled_ine_update_enqueue_failed", extra={"operation_code": op_code}
            )


async def scheduled_territorial_sync(ctx: dict[str, Any]) -> None:
    """Cron job: placeholder for IGN administrative boundaries weekly sync."""
    settings = ctx["settings"]
    if not settings.scheduled_territorial_sync_enabled:
        logger.info("scheduled_territorial_sync_disabled")
        return
    logger.info(
        "scheduled_territorial_sync_triggered",
        extra={"note": "IGN sync job not yet implemented — Fase B"},
    )


async def scheduled_sadei_sync(ctx: dict[str, Any]) -> None:
    """Cron job: enqueue SADEI sync for each configured dataset."""
    settings = ctx["settings"]
    job_store: RedisJobStore = ctx["job_store"]
    arq_pool = ctx["arq_pool"]
    for dataset_id in settings.sadei_sync_datasets:
        try:
            job_record = await job_store.create_job("sadei_sync", {"dataset_id": dataset_id})
            job_id = job_record["job_id"]
            await arq_pool.enqueue_job(
                "run_sadei_sync_job", job_id=job_id, payload={"dataset_id": dataset_id}
            )
            logger.info(
                "scheduled_sadei_sync_enqueued",
                extra={"dataset_id": dataset_id, "job_id": job_id},
            )
        except Exception:
            logger.exception(
                "scheduled_sadei_sync_enqueue_failed", extra={"dataset_id": dataset_id}
            )


async def scheduled_ideas_sync(ctx: dict[str, Any]) -> None:
    """Cron job: enqueue IDEAS/SITPA WFS sync for each configured layer."""
    settings = ctx["settings"]
    job_store: RedisJobStore = ctx["job_store"]
    arq_pool = ctx["arq_pool"]
    for layer_name in settings.ideas_sync_layers:
        try:
            job_record = await job_store.create_job("ideas_sync", {"layer_name": layer_name})
            job_id = job_record["job_id"]
            await arq_pool.enqueue_job(
                "run_ideas_sync_job", job_id=job_id, payload={"layer_name": layer_name}
            )
            logger.info(
                "scheduled_ideas_sync_enqueued",
                extra={"layer_name": layer_name, "job_id": job_id},
            )
        except Exception:
            logger.exception(
                "scheduled_ideas_sync_enqueue_failed", extra={"layer_name": layer_name}
            )


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
        follow_redirects=True,
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
    resolver = AsturiasResolver(
        ine_client=ine_client,
        cache=cache,
        geography_code=settings.default_geography_code,
        geography_name=settings.default_geography_name,
    )
    sadei_client = SADEIClientService(http_client=http_client, settings=settings)
    ideas_client = IDEASWFSClientService(settings=settings)
    catastro_circuit_breaker = AsyncCircuitBreaker(
        provider="catastro",
        fail_max=settings.provider_circuit_breaker_failures,
        reset_timeout_seconds=settings.provider_circuit_breaker_recovery_seconds,
        half_open_sample_size=settings.provider_circuit_breaker_half_open_sample_size,
        success_threshold=settings.provider_circuit_breaker_success_threshold,
    )

    arq_pool = await create_pool(redis_settings_from_url(settings.redis_url))

    ctx["settings"] = settings
    ctx["redis"] = redis
    ctx["job_store"] = job_store
    ctx["arq_pool"] = arq_pool
    ctx["http_client"] = http_client
    ctx["cache"] = cache
    ctx["ine_client"] = ine_client
    ctx["resolver"] = resolver
    ctx["sadei_client"] = sadei_client
    ctx["ideas_client"] = ideas_client
    ctx["catastro_circuit_breaker"] = catastro_circuit_breaker
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

    arq_pool = ctx.get("arq_pool")
    if arq_pool is not None:
        await arq_pool.aclose()

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
        run_sadei_sync_job,
        run_ideas_sync_job,
    ]
    cron_jobs = [
        cron(scheduled_ine_update, hour={3}, minute={0}),  # daily 03:00
        cron(scheduled_territorial_sync, weekday={1}, hour={4}),  # Monday 04:00
        cron(scheduled_sadei_sync, hour={5}, minute={0}),  # daily 05:00
        cron(scheduled_ideas_sync, weekday={1}, hour={4}, minute={30}),  # Monday 04:30
    ]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = redis_settings_from_url(get_settings().redis_url or "redis://localhost:6379/0")
    queue_name = get_settings().job_queue_name
