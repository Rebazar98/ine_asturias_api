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


_INE_MOJIBAKE_MARKERS = ("Ã", "Â", "â", "ƒ", "€", "™", "�")


def _mojibake_score(value: str) -> int:
    return sum(value.count(marker) for marker in _INE_MOJIBAKE_MARKERS)


def _repair_ine_string(value: str) -> str:
    current = value
    for _ in range(3):
        if _mojibake_score(current) == 0:
            break

        candidates: list[str] = []
        for source_encoding in ("cp1252", "latin-1"):
            try:
                candidate = current.encode(source_encoding).decode("utf-8")
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue
            if candidate != current:
                candidates.append(candidate)

        if not candidates:
            break

        best_candidate = min(candidates, key=_mojibake_score)
        if _mojibake_score(best_candidate) > _mojibake_score(current):
            break
        current = best_candidate

    return current


def _fix_ine_encoding(obj: Any) -> Any:
    """Reverse INE's server-side mojibake: UTF-8 bytes stored as Latin-1 codepoints."""
    if isinstance(obj, str):
        return _repair_ine_string(obj)
    if isinstance(obj, list):
        return [_fix_ine_encoding(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _fix_ine_encoding(v) for k, v in obj.items()}
    return obj


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
        try:
            return await self._fetch_json(
                f"VARIABLES_OPERACION/{op_code}",
                cache_scope="operation_variables",
            )
        except INEInvalidPayloadError:
            self.logger.warning(
                "ine_operation_variables_empty_body",
                extra={"op_code": op_code},
            )
            return []

    async def get_variable_values(
        self, op_code: str, variable_id: str
    ) -> dict[str, Any] | list[Any]:
        try:
            return await self._fetch_json(
                f"VALORES_VARIABLEOPERACION/{variable_id}/{op_code}",
                cache_scope="variable_values",
            )
        except INEInvalidPayloadError:
            # Some operations return an empty body for geographic variable values
            # (HTTP 200 but no JSON). Treat as empty list so the resolver can
            # activate name-based fallback instead of failing.
            self.logger.warning(
                "ine_variable_values_empty_body",
                extra={"op_code": op_code, "variable_id": variable_id},
            )
            return []

    async def get_operation_tables(self, op_code: str) -> dict[str, Any] | list[Any]:
        return await self._fetch_json(
            f"TABLAS_OPERACION/{op_code}",
            cache_scope="operation_tables",
        )

    async def get_operation_series(self, op_code: str, page: int = 1) -> dict[str, Any] | list[Any]:
        return await self._fetch_json(
            f"SERIES_OPERACION/{op_code}",
            params={"det": 2, "page": page},
            cache_scope="operation_series",
        )

    async def get_serie_data(
        self, cod_serie: str, nult: int | None = None
    ) -> dict[str, Any] | list[Any]:
        params: dict[str, Any] = {}
        if nult is not None:
            params["nult"] = nult
        return await self._fetch_json(
            f"DATOS_SERIE/{cod_serie}",
            params=params or None,
            cache_scope="serie_data",
        )

    async def get_variables(self) -> list[Any]:
        """Return all INE statistical variables from the /VARIABLES endpoint.

        Useful for discovering known geographic variable IDs (e.g. Id=3 for CCAA)
        without depending on per-operation metadata.  Returns [] on empty body.
        """
        try:
            result = await self._fetch_json("VARIABLES", cache_scope="variables")
        except INEInvalidPayloadError:
            self.logger.warning("ine_variables_empty_body")
            return []
        return result if isinstance(result, list) else []

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
            payload = self._decode_json_response(response)
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

    @staticmethod
    def _decode_json_response(response: httpx.Response) -> Any:
        try:
            return _fix_ine_encoding(response.json())
        except ValueError:
            if not response.content:
                raise

            for encoding in ("utf-8-sig", "utf-8"):
                try:
                    decoded_text = response.content.decode(encoding)
                    return _fix_ine_encoding(json.loads(decoded_text))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
            raise

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
                if self.circuit_breaker is not None and exc.response.status_code >= 500:
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

        raise INEUpstreamError(
            status_code=500,
            detail={
                "message": "INE retry loop completed without a result. This is a logic error.",
                "retryable": False,
            },
        )

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
