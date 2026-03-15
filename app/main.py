from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager

import httpx
from arq.connections import create_pool
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis.asyncio import Redis

from app.api.routes_health import router as health_router
from app.api.routes_ine import router as ine_router
from app.api.routes_territorial import router as territorial_router
from app.core.cache import InMemoryTTLCache, LayeredCache, RedisTTLCache
from app.core.jobs import InMemoryJobStore, RedisJobStore
from app.core.logging import configure_logging, get_logger
from app.core.metrics import record_http_request
from app.core.rate_limit import InMemoryRateLimiter, RedisRateLimiter
from app.core.redis import redis_settings_from_url
from app.core.resilience import AsyncCircuitBreaker
from app.core.security import sanitize_query_params_for_logging
from app.db import dispose_db, init_db
from app.services.asturias_resolver import AsturiasResolutionError
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
    )
    app.state.redis = None
    app.state.arq_redis = None
    app.state.job_store = InMemoryJobStore()
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
                logger.exception("redis_initialization_failed_fallback_in_memory")
                if app.state.arq_redis is not None:
                    await app.state.arq_redis.aclose()
                    app.state.arq_redis = None
                if app.state.redis is not None:
                    await app.state.redis.aclose()
                    app.state.redis = None
                app.state.job_store = InMemoryJobStore()
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


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description="FastAPI ingestion and proxy API for INE data focused on Asturias.",
        lifespan=lifespan,
    )

    app.include_router(health_router)
    app.include_router(ine_router)
    app.include_router(territorial_router)

    @app.middleware("http")
    async def request_timing_middleware(request: Request, call_next):
        logger = get_logger("app.access")
        started_at = time.perf_counter()
        path_template = getattr(request.scope.get("route"), "path", request.url.path)

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
                },
            )
            record_http_request(request.method, path_template, 500, duration_seconds)
            raise

        duration_seconds = time.perf_counter() - started_at
        response.headers["X-Process-Time-Ms"] = str(round(duration_seconds * 1000, 2))
        logger.info(
            "request_completed",
            extra={
                "method": request.method,
                "path": request.url.path,
                "path_template": path_template,
                "status_code": response.status_code,
                "duration_ms": round(duration_seconds * 1000, 2),
            },
        )
        record_http_request(request.method, path_template, response.status_code, duration_seconds)
        return response

    @app.exception_handler(INEClientError)
    async def ine_client_error_handler(_: Request, exc: INEClientError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    @app.exception_handler(CartoCiudadClientError)
    async def cartociudad_client_error_handler(
        _: Request, exc: CartoCiudadClientError
    ) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    @app.exception_handler(CartoCiudadNormalizationError)
    async def cartociudad_normalization_error_handler(
        _: Request, exc: CartoCiudadNormalizationError
    ) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    @app.exception_handler(AsturiasResolutionError)
    async def asturias_resolution_error_handler(
        _: Request, exc: AsturiasResolutionError
    ) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    return app


app = create_app()
