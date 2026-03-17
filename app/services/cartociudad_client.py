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
from app.services.geocoding_privacy import (
    build_geocode_audit_request_params,
    build_reverse_geocode_audit_request_params,
)
from app.settings import Settings


class CartoCiudadClientError(Exception):
    def __init__(self, status_code: int, detail: Any) -> None:
        super().__init__(str(detail))
        self.status_code = status_code
        self.detail = detail


class CartoCiudadUpstreamError(CartoCiudadClientError):
    pass


class CartoCiudadInvalidPayloadError(CartoCiudadClientError):
    pass


class CartoCiudadClientService:
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
        self.logger = get_logger("app.services.cartociudad_client")

    async def geocode(self, query: str) -> dict[str, Any] | list[Any]:
        return await self._fetch_json(
            "find",
            params={"q": query},
            cache_scope="geocode",
        )

    async def reverse_geocode(self, lat: float, lon: float) -> dict[str, Any] | list[Any]:
        return await self._fetch_json(
            "reverseGeocode",
            params={"lat": lat, "lon": lon},
            cache_scope="reverse_geocode",
        )

    async def _fetch_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        cache_scope: str = "cartociudad",
    ) -> dict[str, Any] | list[Any]:
        cache_key = self._build_cache_key(cache_scope, path, params)
        cached = await self.cache.get(cache_key)
        if cached is not None:
            record_provider_cache_hit("cartociudad", cache_scope)
            self.logger.info(
                "cartociudad_cache_hit",
                extra={"path": path, "cache_scope": cache_scope},
            )
            return cached

        started_at = time.perf_counter()
        endpoint_family = self._endpoint_family(path)
        url = f"{self.settings.cartociudad_base_url.rstrip('/')}/{path}"
        safe_params = self._sanitize_params(path, params)
        response = await self._request_with_resilience(
            url=url,
            path=path,
            params=params,
            safe_params=safe_params,
            endpoint_family=endpoint_family,
            started_at=started_at,
        )

        try:
            payload = response.json()
        except ValueError as exc:
            duration_seconds = time.perf_counter() - started_at
            record_provider_request(
                "cartociudad", endpoint_family, "invalid_json", duration_seconds
            )
            if self.circuit_breaker is not None:
                await self.circuit_breaker.record_failure(reason="invalid_json")
            self.logger.error(
                "cartociudad_invalid_json",
                extra={"path": path, "request_context": safe_params},
            )
            raise CartoCiudadInvalidPayloadError(
                status_code=502,
                detail={
                    "message": "The CartoCiudad service returned invalid JSON.",
                    "path": path,
                    "request_context": safe_params,
                },
            ) from exc

        if not isinstance(payload, (list, dict)):
            duration_seconds = time.perf_counter() - started_at
            record_provider_request(
                "cartociudad", endpoint_family, "unexpected_payload", duration_seconds
            )
            if self.circuit_breaker is not None:
                await self.circuit_breaker.record_failure(reason="unexpected_payload")
            self.logger.error(
                "cartociudad_unexpected_payload",
                extra={
                    "path": path,
                    "request_context": safe_params,
                    "payload_type": type(payload).__name__,
                },
            )
            raise CartoCiudadInvalidPayloadError(
                status_code=502,
                detail={
                    "message": "The CartoCiudad service returned an unexpected JSON format.",
                    "path": path,
                    "request_context": safe_params,
                },
            )

        duration_seconds = time.perf_counter() - started_at
        record_provider_request("cartociudad", endpoint_family, "success", duration_seconds)
        if self.circuit_breaker is not None:
            await self.circuit_breaker.record_success()
        self.logger.info(
            "cartociudad_request_completed",
            extra={
                "path": path,
                "request_context": safe_params,
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
        safe_params: dict[str, Any],
        endpoint_family: str,
        started_at: float,
    ) -> httpx.Response:
        if self.circuit_breaker is not None:
            try:
                await self.circuit_breaker.before_call()
            except CircuitBreakerOpenError as exc:
                record_provider_request("cartociudad", endpoint_family, "circuit_open", 0.0)
                self.logger.warning(
                    "cartociudad_circuit_breaker_open",
                    extra={
                        "path": path,
                        "request_context": safe_params,
                        "retry_after_seconds": round(exc.retry_after_seconds, 3),
                    },
                )
                raise CartoCiudadUpstreamError(
                    status_code=503,
                    detail={
                        "message": "The CartoCiudad service is temporarily unavailable.",
                        "path": path,
                        "request_context": safe_params,
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
                        safe_params=safe_params,
                        attempt=attempt,
                        reason=f"http_{exc.response.status_code}",
                        backoff_seconds=backoff_seconds,
                    )
                    await asyncio.sleep(backoff_seconds)
                    backoff_seconds *= 2
                    continue

                duration_seconds = time.perf_counter() - started_at
                record_provider_request(
                    "cartociudad", endpoint_family, "http_error", duration_seconds
                )
                if self.circuit_breaker is not None:
                    await self.circuit_breaker.record_failure(
                        reason=f"http_{exc.response.status_code}"
                    )
                self.logger.warning(
                    "cartociudad_upstream_status_error",
                    extra={
                        "path": path,
                        "request_context": safe_params,
                        "status_code": exc.response.status_code,
                        "duration_ms": round(duration_seconds * 1000, 2),
                        "attempt": attempt,
                    },
                )
                raise CartoCiudadUpstreamError(
                    status_code=exc.response.status_code,
                    detail={
                        "message": "The CartoCiudad service returned an error.",
                        "path": path,
                        "request_context": safe_params,
                        "status_code": exc.response.status_code,
                        "retryable": retryable,
                    },
                ) from exc
            except httpx.RequestError as exc:
                if self._can_retry(attempt, deadline, True, backoff_seconds):
                    await self._log_retry(
                        endpoint_family=endpoint_family,
                        path=path,
                        safe_params=safe_params,
                        attempt=attempt,
                        reason="request_error",
                        backoff_seconds=backoff_seconds,
                    )
                    await asyncio.sleep(backoff_seconds)
                    backoff_seconds *= 2
                    continue

                duration_seconds = time.perf_counter() - started_at
                record_provider_request(
                    "cartociudad", endpoint_family, "request_error", duration_seconds
                )
                if self.circuit_breaker is not None:
                    await self.circuit_breaker.record_failure(reason="request_error")
                self.logger.error(
                    "cartociudad_request_error",
                    extra={
                        "path": path,
                        "request_context": safe_params,
                        "duration_ms": round(duration_seconds * 1000, 2),
                        "error": str(exc),
                        "attempt": attempt,
                    },
                )
                raise CartoCiudadUpstreamError(
                    status_code=502,
                    detail={
                        "message": "Could not connect to the CartoCiudad service.",
                        "path": path,
                        "request_context": safe_params,
                        "retryable": True,
                    },
                ) from exc

        raise RuntimeError("Retry loop exhausted without returning a response.")

    async def _log_retry(
        self,
        *,
        endpoint_family: str,
        path: str,
        safe_params: dict[str, Any],
        attempt: int,
        reason: str,
        backoff_seconds: float,
    ) -> None:
        record_provider_retry("cartociudad", endpoint_family, reason)
        self.logger.warning(
            "cartociudad_retry_scheduled",
            extra={
                "path": path,
                "request_context": safe_params,
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
    def _build_cache_key(scope: str, path: str, params: dict[str, Any] | None) -> str:
        serialized_params = json.dumps(params or {}, sort_keys=True, default=str)
        return f"{scope}:{path}:{serialized_params}"

    @staticmethod
    def _endpoint_family(path: str) -> str:
        return path.split("/", 1)[0]

    @staticmethod
    def _sanitize_params(path: str, params: dict[str, Any] | None) -> dict[str, Any]:
        safe_params = dict(params or {})
        if path == "find":
            return build_geocode_audit_request_params(str(safe_params.get("q") or ""))
        if path == "reverseGeocode":
            lat = float(safe_params.get("lat") or 0.0)
            lon = float(safe_params.get("lon") or 0.0)
            return build_reverse_geocode_audit_request_params(lat, lon)
        return {}

    @staticmethod
    def _is_retryable_status(status_code: int) -> bool:
        return status_code in {408, 429, 500, 502, 503, 504}
