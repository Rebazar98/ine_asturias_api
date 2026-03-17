from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import httpx

from app.core.cache import BaseAsyncCache
from app.core.logging import get_logger
from app.core.metrics import (
    record_provider_cache_hit,
    record_provider_request,
    record_provider_retry,
)
from app.core.resilience import AsyncCircuitBreaker, CircuitBreakerOpenError
from app.settings import Settings


class INEClientError(Exception):
    def __init__(self, status_code: int, detail: Any) -> None:
        super().__init__(str(detail))
        self.status_code = status_code
        self.detail = detail


class INEUpstreamError(INEClientError):
    pass


class INEInvalidPayloadError(INEClientError):
    pass


class INEClientService:
    def __init__(
        self,
        http_client: httpx.AsyncClient,
        settings: Settings,
        cache: BaseAsyncCache,
        circuit_breaker: AsyncCircuitBreaker | None = None,
    ) -> None:
        self.http_client = http_client
        self.settings = settings
        self.cache = cache
        self.circuit_breaker = circuit_breaker
        self.logger = get_logger("app.services.ine_client")

    async def get_table(self, table_id: str, params: dict[str, Any]) -> dict[str, Any] | list[Any]:
        return await self._fetch_json(
            f"DATOS_TABLA/{table_id}",
            params=params,
            cache_scope="table",
        )

    async def get_operation_variables(self, op_code: str) -> dict[str, Any] | list[Any]:
        return await self._fetch_json(
            f"VARIABLES_OPERACION/{op_code}",
            cache_scope="operation_variables",
        )

    async def get_variable_values(
        self, op_code: str, variable_id: str
    ) -> dict[str, Any] | list[Any]:
        return await self._fetch_json(
            f"VALORES_VARIABLEOPERACION/{variable_id}/{op_code}",
            cache_scope="variable_values",
        )

    async def get_operation_tables(self, op_code: str) -> dict[str, Any] | list[Any]:
        return await self._fetch_json(
            f"TABLAS_OPERACION/{op_code}",
            cache_scope="operation_tables",
        )

    async def _fetch_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        cache_scope: str = "ine",
    ) -> dict[str, Any] | list[Any]:
        cache_key = self._build_cache_key(cache_scope, path, params)
        endpoint_family = self._endpoint_family(path)
        cached = await self.cache.get(cache_key)
        if cached is not None:
            record_provider_cache_hit("ine", cache_scope)
            self.logger.info(
                "ine_cache_hit",
                extra={"path": path, "cache_scope": cache_scope},
            )
            return cached

        started_at = time.perf_counter()
        url = f"{self.settings.ine_base_url.rstrip('/')}/{path}"
        response = await self._request_with_resilience(
            url=url,
            path=path,
            params=params,
            endpoint_family=endpoint_family,
            started_at=started_at,
        )

        try:
            payload = response.json()
        except ValueError as exc:
            duration_seconds = time.perf_counter() - started_at
            record_provider_request("ine", endpoint_family, "invalid_json", duration_seconds)
            if self.circuit_breaker is not None:
                await self.circuit_breaker.record_failure(reason="invalid_json")
            self.logger.error(
                "ine_invalid_json",
                extra={"path": path, "params": params or {}},
            )
            raise INEInvalidPayloadError(
                status_code=502,
                detail={
                    "message": "The INE service returned invalid JSON.",
                    "path": path,
                    "params": params or {},
                },
            ) from exc

        if not isinstance(payload, (list, dict)):
            duration_seconds = time.perf_counter() - started_at
            record_provider_request("ine", endpoint_family, "unexpected_payload", duration_seconds)
            if self.circuit_breaker is not None:
                await self.circuit_breaker.record_failure(reason="unexpected_payload")
            self.logger.error(
                "ine_unexpected_payload",
                extra={
                    "path": path,
                    "params": params or {},
                    "payload_type": type(payload).__name__,
                },
            )
            raise INEInvalidPayloadError(
                status_code=502,
                detail={
                    "message": "The INE service returned an unexpected JSON format.",
                    "path": path,
                    "params": params or {},
                },
            )

        duration_seconds = time.perf_counter() - started_at
        record_provider_request("ine", endpoint_family, "success", duration_seconds)
        if self.circuit_breaker is not None:
            await self.circuit_breaker.record_success()
        self.logger.info(
            "ine_request_completed",
            extra={
                "path": path,
                "params": params or {},
                "status_code": response.status_code,
                "duration_ms": round(duration_seconds * 1000, 2),
            },
        )

        await self.cache.set(cache_key, payload)
        return payload

    async def _request_with_resilience(
        self,
        *,
        url: str,
        path: str,
        params: dict[str, Any] | None,
        endpoint_family: str,
        started_at: float,
    ) -> httpx.Response:
        if self.circuit_breaker is not None:
            try:
                await self.circuit_breaker.before_call()
            except CircuitBreakerOpenError as exc:
                record_provider_request("ine", endpoint_family, "circuit_open", 0.0)
                self.logger.warning(
                    "ine_circuit_breaker_open",
                    extra={
                        "path": path,
                        "params": params or {},
                        "retry_after_seconds": round(exc.retry_after_seconds, 3),
                    },
                )
                raise INEUpstreamError(
                    status_code=503,
                    detail={
                        "message": "The INE service is temporarily unavailable.",
                        "path": path,
                        "params": params or {},
                        "retryable": True,
                    },
                ) from exc

        deadline = started_at + self.settings.provider_total_timeout_seconds
        backoff_seconds = self.settings.http_retry_backoff_seconds
        max_attempts = self.settings.http_retry_max_attempts

        for attempt in range(1, max_attempts + 1):
            try:
                response = await self.http_client.get(url, params=params)
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as exc:
                retryable = self._is_retryable_status(exc.response.status_code)
                if self._can_retry(attempt, deadline, retryable, backoff_seconds):
                    await self._log_retry(
                        endpoint_family=endpoint_family,
                        path=path,
                        params=params,
                        attempt=attempt,
                        reason=f"http_{exc.response.status_code}",
                        backoff_seconds=backoff_seconds,
                    )
                    await asyncio.sleep(backoff_seconds)
                    backoff_seconds *= 2
                    continue

                duration_seconds = time.perf_counter() - started_at
                record_provider_request("ine", endpoint_family, "http_error", duration_seconds)
                if self.circuit_breaker is not None:
                    await self.circuit_breaker.record_failure(
                        reason=f"http_{exc.response.status_code}"
                    )
                self.logger.warning(
                    "ine_upstream_status_error",
                    extra={
                        "path": path,
                        "params": params or {},
                        "status_code": exc.response.status_code,
                        "duration_ms": round(duration_seconds * 1000, 2),
                        "attempt": attempt,
                    },
                )
                raise INEUpstreamError(
                    status_code=exc.response.status_code,
                    detail={
                        "message": "The INE service returned an error.",
                        "path": path,
                        "params": params or {},
                        "status_code": exc.response.status_code,
                        "retryable": retryable,
                    },
                ) from exc
            except httpx.RequestError as exc:
                if self._can_retry(attempt, deadline, True, backoff_seconds):
                    await self._log_retry(
                        endpoint_family=endpoint_family,
                        path=path,
                        params=params,
                        attempt=attempt,
                        reason="request_error",
                        backoff_seconds=backoff_seconds,
                    )
                    await asyncio.sleep(backoff_seconds)
                    backoff_seconds *= 2
                    continue

                duration_seconds = time.perf_counter() - started_at
                record_provider_request("ine", endpoint_family, "request_error", duration_seconds)
                if self.circuit_breaker is not None:
                    await self.circuit_breaker.record_failure(reason="request_error")
                self.logger.error(
                    "ine_request_error",
                    extra={
                        "path": path,
                        "params": params or {},
                        "duration_ms": round(duration_seconds * 1000, 2),
                        "error": str(exc),
                        "attempt": attempt,
                    },
                )
                raise INEUpstreamError(
                    status_code=502,
                    detail={
                        "message": "Could not connect to the INE service.",
                        "path": path,
                        "params": params or {},
                        "retryable": True,
                    },
                ) from exc

        raise RuntimeError("Retry loop exhausted without returning a response.")

    async def _log_retry(
        self,
        *,
        endpoint_family: str,
        path: str,
        params: dict[str, Any] | None,
        attempt: int,
        reason: str,
        backoff_seconds: float,
    ) -> None:
        record_provider_retry("ine", endpoint_family, reason)
        self.logger.warning(
            "ine_retry_scheduled",
            extra={
                "path": path,
                "params": params or {},
                "attempt": attempt,
                "max_attempts": self.settings.http_retry_max_attempts,
                "backoff_seconds": round(backoff_seconds, 3),
                "reason": reason,
            },
        )

    def _can_retry(
        self,
        attempt: int,
        deadline: float,
        retryable: bool,
        backoff_seconds: float,
    ) -> bool:
        if not retryable:
            return False
        if attempt >= self.settings.http_retry_max_attempts:
            return False
        return time.perf_counter() + backoff_seconds < deadline

    @staticmethod
    def _build_cache_key(
        scope: str,
        path: str,
        params: dict[str, Any] | None,
    ) -> str:
        serialized_params = json.dumps(params or {}, sort_keys=True, default=str)
        return f"{scope}:{path}:{serialized_params}"

    @staticmethod
    def _endpoint_family(path: str) -> str:
        return path.split("/", 1)[0]

    @staticmethod
    def _is_retryable_status(status_code: int) -> bool:
        return status_code in {408, 429, 500, 502, 503, 504}
