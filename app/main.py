from __future__ import annotations

import asyncio
import math
import time
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import httpx
from arq.connections import create_pool
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from redis.asyncio import Redis

from app.api.qa import router as qa_router
from app.api.routes_health import router as health_router
from app.api.routes_ine import router as ine_router
from app.api.routes_territorial import router as territorial_router
from app.core.cache import InMemoryTTLCache, LayeredCache, RedisTTLCache
from app.core.jobs import InMemoryJobStore, RedisJobStore
from app.core.logging import configure_logging, get_logger, request_id_var
from app.core.metrics import record_http_request
from app.core.rate_limit import InMemoryRateLimiter, RedisRateLimiter
from app.core.redis import redis_settings_from_url
from app.core.resilience import AsyncCircuitBreaker, CircuitBreakerOpenError
from app.core.security import sanitize_query_params_for_logging
from app.db import dispose_db, init_db
from app.services.asturias_resolver import AsturiasResolutionError
from app.services.catastro_client import CatastroClientError
from app.services.cartociudad_client import CartoCiudadClientError
from app.services.cartociudad_normalizers import CartoCiudadNormalizationError
from app.services.ine_client import INEClientError
from app.settings import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    logger = get_logger("app.lifecycle")

    app.state.settings = settings
    local_cache = InMemoryTTLCache(
        enabled=settings.enable_cache,
        default_ttl_seconds=settings.cache_ttl_seconds,
    )
    app.state.cache = local_cache
    app.state.http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(
            settings.http_timeout_seconds, connect=min(settings.http_timeout_seconds, 5.0)
        ),
        follow_redirects=True,
    )
    app.state.redis = None
    app.state.arq_redis = None
    app.state.job_store = InMemoryJobStore() if settings.job_store_backend == "memory" else None
    app.state.inline_job_tasks = set()
    app.state.rate_limiter = InMemoryRateLimiter()
    app.state.ine_circuit_breaker = AsyncCircuitBreaker(
        provider="ine",
        fail_max=settings.provider_circuit_breaker_failures,
        reset_timeout_seconds=settings.provider_circuit_breaker_recovery_seconds,
        half_open_sample_size=settings.provider_circuit_breaker_half_open_sample_size,
        success_threshold=settings.provider_circuit_breaker_success_threshold,
    )
    app.state.cartociudad_circuit_breaker = AsyncCircuitBreaker(
        provider="cartociudad",
        fail_max=settings.provider_circuit_breaker_failures,
        reset_timeout_seconds=settings.provider_circuit_breaker_recovery_seconds,
        half_open_sample_size=settings.provider_circuit_breaker_half_open_sample_size,
        success_threshold=settings.provider_circuit_breaker_success_threshold,
    )
    app.state.catastro_circuit_breaker = AsyncCircuitBreaker(
        provider="catastro",
        fail_max=settings.provider_circuit_breaker_failures,
        reset_timeout_seconds=settings.provider_circuit_breaker_recovery_seconds,
        half_open_sample_size=settings.provider_circuit_breaker_half_open_sample_size,
        success_threshold=settings.provider_circuit_breaker_success_threshold,
    )

    init_db(settings)

    if settings.redis_url:
        try:
            app.state.redis = Redis.from_url(settings.redis_url, decode_responses=True)
            await app.state.redis.ping()
            app.state.arq_redis = await create_pool(redis_settings_from_url(settings.redis_url))
            app.state.job_store = RedisJobStore(redis=app.state.redis, settings=settings)
            app.state.rate_limiter = RedisRateLimiter(redis=app.state.redis)
            app.state.cache = LayeredCache(
                local_cache=local_cache,
                shared_cache=RedisTTLCache(
                    redis=app.state.redis,
                    enabled=settings.enable_cache,
                    default_ttl_seconds=settings.cache_ttl_seconds,
                    namespace="provider-cache",
                ),
            )
            logger.info("redis_ready", extra={"queue_name": settings.job_queue_name})
        except Exception:
            if settings.is_local_env:
                logger.exception("redis_initialization_failed_degraded")
                if app.state.arq_redis is not None:
                    await app.state.arq_redis.aclose()
                    app.state.arq_redis = None
                if app.state.redis is not None:
                    await app.state.redis.aclose()
                    app.state.redis = None
                # job_store stays None → /health/ready reports worker as degraded
            else:
                logger.exception("redis_initialization_failed")
                await app.state.http_client.aclose()
                await dispose_db()
                raise
    else:
        logger.warning("redis_disabled", extra={"reason": "REDIS_URL not configured"})

    logger.info(
        "app_started",
        extra={
            "app_name": settings.app_name,
            "app_version": settings.app_version,
            "app_env": settings.app_env,
            "cache_enabled": settings.enable_cache,
            "api_key_required": settings.requires_api_key,
            "rate_limit_enabled": settings.rate_limit_enabled,
            "job_store": type(app.state.job_store).__name__,
        },
    )
    try:
        yield
    finally:
        inline_tasks = list(getattr(app.state, "inline_job_tasks", set()))
        for task in inline_tasks:
            task.cancel()
        for task in inline_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("inline_job_shutdown_failed")
        if app.state.arq_redis is not None:
            await app.state.arq_redis.aclose()
        if app.state.redis is not None:
            await app.state.redis.aclose()
        await app.state.http_client.aclose()
        await dispose_db()
        logger.info(
            "app_stopped",
            extra={
                "app_name": settings.app_name,
                "app_version": settings.app_version,
                "app_env": settings.app_env,
            },
        )


def _error_response(
    status_code: int,
    detail: Any,
    *,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    rid = request_id_var.get()
    if isinstance(detail, dict):
        body = {"detail": {**detail, "request_id": rid}}
    else:
        body = {"detail": detail}
    return JSONResponse(status_code=status_code, content=body, headers=headers)


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description="FastAPI ingestion and proxy API for INE data focused on Asturias.",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allowed_origins,
        allow_credentials=settings.cors_allow_credentials,
        allow_methods=settings.cors_allowed_methods,
        allow_headers=settings.cors_allowed_headers,
    )

    app.include_router(health_router)
    app.include_router(ine_router)
    app.include_router(territorial_router)
    app.include_router(qa_router)

    @app.middleware("http")
    async def request_timing_middleware(request: Request, call_next):
        logger = get_logger("app.access")
        started_at = time.perf_counter()
        path_template = getattr(request.scope.get("route"), "path", request.url.path)

        request_id = request.headers.get("X-Request-ID") or str(uuid4())
        token = request_id_var.set(request_id)

        try:
            response = await call_next(request)
        except Exception:
            duration_seconds = time.perf_counter() - started_at
            logger.exception(
                "request_failed",
                extra={
                    "method": request.method,
                    "path": request.url.path,
                    "path_template": path_template,
                    **sanitize_query_params_for_logging(request.query_params),
                    "duration_ms": round(duration_seconds * 1000, 2),
                    "request_id": request_id,
                },
            )
            record_http_request(request.method, path_template, 500, duration_seconds)
            return _error_response(500, {"message": "Internal server error."})
        finally:
            request_id_var.reset(token)

        duration_seconds = time.perf_counter() - started_at
        response.headers["X-Process-Time-Ms"] = str(round(duration_seconds * 1000, 2))
        response.headers["X-Request-ID"] = request_id
        logger.info(
            "request_completed",
            extra={
                "method": request.method,
                "path": request.url.path,
                "path_template": path_template,
                "status_code": response.status_code,
                "duration_ms": round(duration_seconds * 1000, 2),
                "request_id": request_id,
            },
        )
        record_http_request(request.method, path_template, response.status_code, duration_seconds)
        return response

    @app.exception_handler(INEClientError)
    async def ine_client_error_handler(_: Request, exc: INEClientError) -> JSONResponse:
        return _error_response(exc.status_code, exc.detail)

    @app.exception_handler(CartoCiudadClientError)
    async def cartociudad_client_error_handler(
        _: Request, exc: CartoCiudadClientError
    ) -> JSONResponse:
        return _error_response(exc.status_code, exc.detail)

    @app.exception_handler(CartoCiudadNormalizationError)
    async def cartociudad_normalization_error_handler(
        _: Request, exc: CartoCiudadNormalizationError
    ) -> JSONResponse:
        return _error_response(exc.status_code, exc.detail)

    @app.exception_handler(CatastroClientError)
    async def catastro_client_error_handler(_: Request, exc: CatastroClientError) -> JSONResponse:
        return _error_response(exc.status_code, exc.detail)

    @app.exception_handler(AsturiasResolutionError)
    async def asturias_resolution_error_handler(
        _: Request, exc: AsturiasResolutionError
    ) -> JSONResponse:
        return _error_response(exc.status_code, exc.detail)

    @app.exception_handler(CircuitBreakerOpenError)
    async def circuit_breaker_open_error_handler(
        _: Request, exc: CircuitBreakerOpenError
    ) -> JSONResponse:
        return _error_response(
            503,
            {
                "message": "Service temporarily unavailable. Please retry later.",
                "provider": exc.provider,
                "retry_after_seconds": exc.retry_after_seconds,
            },
            headers={"Retry-After": str(math.ceil(exc.retry_after_seconds))},
        )

    @app.exception_handler(RequestValidationError)
    async def request_validation_error_handler(
        _: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return _error_response(422, {"message": "Validation error.", "errors": exc.errors()})

    return app


app = create_app()
