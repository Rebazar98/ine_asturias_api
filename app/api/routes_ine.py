from __future__ import annotations

import asyncio
from typing import Any, Literal

from arq.connections import ArqRedis
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse

from app.core.jobs import BaseJobStore
from app.core.logging import get_logger
from app.dependencies import (
    get_arq_pool,
    get_asturias_resolver,
    get_ingestion_repository,
    get_ine_client_service,
    get_job_store,
    get_operation_ingestion_service,
    get_series_repository,
    get_table_catalog_repository,
    require_api_key,
)
from app.repositories.catalog import TableCatalogRepository
from app.repositories.ingestion import IngestionRepository
from app.repositories.series import SeriesRepository
from app.schemas import (
    BackgroundJobAcceptedResponse,
    BackgroundJobStatusResponse,
    CatalogOperationSummaryResponse,
    CatalogTableItemResponse,
    INESeriesListItemResponse,
    INESeriesListResponse,
    JSONPayload,
)
from app.services.asturias_resolver import AsturiasResolutionError, AsturiasResolver
from app.services.ine_client import INEClientError, INEClientService
from app.services.ine_operation_ingestion import INEOperationIngestionService
from app.settings import Settings, get_settings


router = APIRouter(tags=["ine"], dependencies=[Depends(require_api_key)])
logger = get_logger("app.api.routes_ine")
DEV_MAX_TABLES_DEFAULT = 3
BACKGROUND_JOB_TYPE = "operation_asturias_ingestion"


@router.get("/ine/table/{table_id}", response_model=JSONPayload)
async def get_table_data(
    table_id: str,
    nult: int | None = Query(default=None, ge=1),
    det: int | None = Query(default=None, ge=0, le=2),
    tip: Literal["A", "M", "AM"] | None = Query(default=None),
    date: str | None = Query(default=None),
    ine_client: INEClientService = Depends(get_ine_client_service),
    ingestion_repo: IngestionRepository = Depends(get_ingestion_repository),
    ingestion_service: INEOperationIngestionService = Depends(get_operation_ingestion_service),
) -> JSONPayload:
    params = _build_query_params(nult=nult, det=det, tip=tip, date=date)
    payload = await ine_client.get_table(table_id, params)

    await ingestion_repo.save_raw(
        source_type="table",
        source_key=table_id,
        request_path=f"DATOS_TABLA/{table_id}",
        request_params=params,
        payload=payload,
    )
    await ingestion_service.normalize_and_store_table(payload=payload, table_id=table_id)
    return JSONPayload(root=payload)


@router.get("/ine/operation/{op_code}/variables", response_model=JSONPayload)
async def get_operation_variables(
    op_code: str,
    ine_client: INEClientService = Depends(get_ine_client_service),
    ingestion_repo: IngestionRepository = Depends(get_ingestion_repository),
) -> JSONPayload:
    payload = await ine_client.get_operation_variables(op_code)
    await ingestion_repo.save_raw(
        source_type="operation_variables",
        source_key=op_code,
        request_path=f"VARIABLES_OPERACION/{op_code}",
        request_params={},
        payload=payload,
    )
    return JSONPayload(root=payload)


@router.get("/ine/operation/{op_code}/variable/{variable_id}/values", response_model=JSONPayload)
async def get_variable_values(
    op_code: str,
    variable_id: str,
    ine_client: INEClientService = Depends(get_ine_client_service),
    ingestion_repo: IngestionRepository = Depends(get_ingestion_repository),
) -> JSONPayload:
    payload = await ine_client.get_variable_values(op_code, variable_id)
    await ingestion_repo.save_raw(
        source_type="variable_values",
        source_key=f"{op_code}:{variable_id}",
        request_path=f"VALORES_VARIABLEOPERACION/{variable_id}/{op_code}",
        request_params={},
        payload=payload,
    )
    return JSONPayload(root=payload)


@router.get("/ine/catalog/operation/{op_code}", response_model=list[CatalogTableItemResponse])
async def get_catalog_for_operation(
    op_code: str,
    catalog_repo: TableCatalogRepository = Depends(get_table_catalog_repository),
) -> list[CatalogTableItemResponse]:
    rows = await catalog_repo.list_by_operation(op_code)
    return [CatalogTableItemResponse(**row) for row in rows]


@router.get(
    "/ine/catalog/operation/{op_code}/summary", response_model=CatalogOperationSummaryResponse
)
async def get_catalog_summary_for_operation(
    op_code: str,
    catalog_repo: TableCatalogRepository = Depends(get_table_catalog_repository),
) -> CatalogOperationSummaryResponse:
    summary = await catalog_repo.get_operation_summary(op_code)
    return CatalogOperationSummaryResponse(**summary)


@router.get("/ine/jobs/{job_id}", response_model=BackgroundJobStatusResponse)
async def get_job_status(
    job_id: str,
    job_store: BaseJobStore = Depends(get_job_store),
) -> BackgroundJobStatusResponse:
    job = await job_store.get_job(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"message": "Job not found.", "job_id": job_id},
        )
    return BackgroundJobStatusResponse(**job)


@router.get("/ine/series", response_model=INESeriesListResponse)
async def list_normalized_ine_series(
    operation_code: str | None = Query(default=None),
    table_id: str | None = Query(default=None),
    geography_code: str | None = Query(default=None),
    variable_id: str | None = Query(default=None),
    period_from: str | None = Query(default=None),
    period_to: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    series_repo: SeriesRepository = Depends(get_series_repository),
) -> INESeriesListResponse:
    result = await series_repo.list_normalized(
        operation_code=operation_code,
        table_id=table_id,
        geography_code=geography_code,
        variable_id=variable_id,
        period_from=period_from,
        period_to=period_to,
        page=page,
        page_size=page_size,
    )
    return INESeriesListResponse(
        items=[INESeriesListItemResponse(**item) for item in result["items"]],
        total=result["total"],
        page=result["page"],
        page_size=result["page_size"],
    )


@router.get(
    "/ine/operation/{op_code}/asturias",
    response_model=JSONPayload,
    responses={202: {"model": BackgroundJobAcceptedResponse}},
)
async def get_asturias_operation_data(
    request: Request,
    op_code: str,
    geo_variable_id: str | None = Query(default=None),
    asturias_value_id: str | None = Query(default=None),
    nult: int | None = Query(default=None, ge=1),
    det: int | None = Query(default=None, ge=0, le=2),
    tip: Literal["A", "M", "AM"] | None = Query(default=None),
    periodicidad: str | None = Query(default=None),
    max_tables: int | None = Query(default=None, ge=1, le=25),
    background: bool | None = Query(default=None),
    skip_known_no_data: bool = Query(default=False),
    settings: Settings = Depends(get_settings),
    ine_client: INEClientService = Depends(get_ine_client_service),
    resolver: AsturiasResolver = Depends(get_asturias_resolver),
    operation_service: INEOperationIngestionService = Depends(get_operation_ingestion_service),
    job_store: BaseJobStore = Depends(get_job_store),
    arq_pool: ArqRedis | None = Depends(get_arq_pool),
) -> JSONPayload | JSONResponse:
    effective_max_tables = _resolve_max_tables(max_tables, settings)
    background_mode = _resolve_background_mode(background, settings)
    logger.info(
        "asturias_operation_lookup_started",
        extra={
            "operation_code": op_code,
            "max_tables_requested": max_tables,
            "max_tables_effective": effective_max_tables,
            "background": background_mode,
            "skip_known_no_data": skip_known_no_data,
            "app_env": settings.app_env,
        },
    )

    job_params = _build_job_params(
        operation_code=op_code,
        geo_variable_id=geo_variable_id,
        asturias_value_id=asturias_value_id,
        nult=nult,
        det=det,
        tip=tip,
        periodicidad=periodicidad,
        max_tables=effective_max_tables,
        skip_known_no_data=skip_known_no_data,
    )

    if background_mode:
        job = await job_store.create_job(job_type=BACKGROUND_JOB_TYPE, params=job_params)
        job_id = job["job_id"]
        try:
            if arq_pool is not None:
                await arq_pool.enqueue_job(
                    "run_operation_asturias_job",
                    job_id,
                    job_params,
                    _job_id=job_id,
                    _queue_name=settings.job_queue_name,
                )
            else:
                task = asyncio.create_task(
                    _run_asturias_operation_job_inline(
                        job_store=job_store,
                        op_code=op_code,
                        geo_variable_id=geo_variable_id,
                        asturias_value_id=asturias_value_id,
                        nult=nult,
                        det=det,
                        tip=tip,
                        periodicidad=periodicidad,
                        max_tables=effective_max_tables,
                        skip_known_no_data=skip_known_no_data,
                        ine_client=ine_client,
                        resolver=resolver,
                        operation_service=operation_service,
                        job_id=job_id,
                    )
                )
                request.app.state.inline_job_tasks = getattr(
                    request.app.state, "inline_job_tasks", set()
                )
                request.app.state.inline_job_tasks.add(task)
                task.add_done_callback(
                    lambda completed: request.app.state.inline_job_tasks.discard(completed)
                )
        except Exception as exc:
            await job_store.fail_job(
                job_id,
                {
                    "message": "Could not enqueue the background job.",
                    "operation_code": op_code,
                    "error": str(exc),
                },
            )
            raise HTTPException(
                status_code=503,
                detail={"message": "Background queue unavailable.", "job_id": job_id},
            )

        accepted = BackgroundJobAcceptedResponse(
            job_id=job_id,
            job_type=BACKGROUND_JOB_TYPE,
            status=job["status"],
            operation_code=op_code,
            status_path=f"/ine/jobs/{job_id}",
            params=job_params,
        )
        logger.info(
            "asturias_background_job_queued",
            extra={"operation_code": op_code, "job_id": job_id, "max_tables": effective_max_tables},
        )
        return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=accepted.model_dump())

    resolution = await resolver.resolve(
        op_code=op_code,
        geo_variable_id=geo_variable_id,
        asturias_value_id=asturias_value_id,
    )
    logger.info(
        "asturias_operation_resolution_ready",
        extra={
            "operation_code": op_code,
            "geo_variable_id": resolution.geo_variable_id,
            "asturias_value_id": resolution.asturias_value_id,
        },
    )

    payload = await operation_service.ingest_asturias_operation(
        op_code=op_code,
        resolution=resolution,
        nult=nult,
        det=det,
        tip=tip,
        periodicidad=periodicidad,
        max_tables=effective_max_tables,
        skip_known_no_data=skip_known_no_data,
        ine_client=ine_client,
    )
    return JSONPayload(root=payload)


async def _run_asturias_operation_job_inline(
    job_store: BaseJobStore,
    op_code: str,
    geo_variable_id: str | None,
    asturias_value_id: str | None,
    nult: int | None,
    det: Literal[0, 1, 2] | None,
    tip: Literal["A", "M", "AM"] | None,
    periodicidad: str | None,
    max_tables: int | None,
    skip_known_no_data: bool,
    ine_client: INEClientService,
    resolver: AsturiasResolver,
    operation_service: INEOperationIngestionService,
    job_id: str,
) -> None:
    async def report_progress(progress: dict[str, Any]) -> None:
        await job_store.update_progress(job_id, **progress)

    try:
        await job_store.mark_running(job_id)
        await report_progress({"stage": "resolving_asturias", "operation_code": op_code})
        resolution = await resolver.resolve(
            op_code=op_code,
            geo_variable_id=geo_variable_id,
            asturias_value_id=asturias_value_id,
        )
        await report_progress(
            {
                "stage": "resolution_completed",
                "geo_variable_id": resolution.geo_variable_id,
                "asturias_value_id": resolution.asturias_value_id,
            }
        )
        payload = await operation_service.ingest_asturias_operation(
            op_code=op_code,
            resolution=resolution,
            nult=nult,
            det=det,
            tip=tip,
            periodicidad=periodicidad,
            max_tables=max_tables,
            skip_known_no_data=skip_known_no_data,
            ine_client=ine_client,
            progress_reporter=report_progress,
        )
        await job_store.complete_job(job_id, payload)
    except (AsturiasResolutionError, INEClientError) as exc:
        await job_store.fail_job(job_id, exc.detail)
    except Exception as exc:
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


def _resolve_max_tables(max_tables: int | None, settings: Settings) -> int | None:
    if max_tables is not None:
        return max_tables
    if settings.is_local_env:
        return DEV_MAX_TABLES_DEFAULT
    return None


def _resolve_background_mode(background: bool | None, settings: Settings) -> bool:
    if not settings.is_local_env:
        return True
    if background is not None:
        return background
    return True


def _build_job_params(**kwargs: Any) -> dict[str, Any]:
    return {key: value for key, value in kwargs.items() if value is not None}


def _build_query_params(**kwargs: Any) -> dict[str, Any]:
    return {key: value for key, value in kwargs.items() if value is not None}
