from __future__ import annotations

import json
import time
from typing import Any

import httpx

from app.core.cache import InMemoryTTLCache
from app.core.logging import get_logger
from app.core.metrics import record_provider_cache_hit, record_provider_request
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
        cache: InMemoryTTLCache,
    ) -> None:
        self.http_client = http_client
        self.settings = settings
        self.cache = cache
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

    async def get_variable_values(self, op_code: str, variable_id: str) -> dict[str, Any] | list[Any]:
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

        try:
            response = await self.http_client.get(url, params=params)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            duration_seconds = time.perf_counter() - started_at
            record_provider_request("ine", endpoint_family, "http_error", duration_seconds)
            self.logger.warning(
                "ine_upstream_status_error",
                extra={
                    "path": path,
                    "params": params or {},
                    "status_code": exc.response.status_code,
                    "duration_ms": round(duration_seconds * 1000, 2),
                },
            )
            raise INEUpstreamError(
                status_code=exc.response.status_code,
                detail={
                    "message": "The INE service returned an error.",
                    "path": path,
                    "params": params or {},
                    "status_code": exc.response.status_code,
                },
            ) from exc
        except httpx.RequestError as exc:
            duration_seconds = time.perf_counter() - started_at
            record_provider_request("ine", endpoint_family, "request_error", duration_seconds)
            self.logger.error(
                "ine_request_error",
                extra={
                    "path": path,
                    "params": params or {},
                    "duration_ms": round(duration_seconds * 1000, 2),
                    "error": str(exc),
                },
            )
            raise INEUpstreamError(
                status_code=502,
                detail={
                    "message": "Could not connect to the INE service.",
                    "path": path,
                    "params": params or {},
                },
            ) from exc

        try:
            payload = response.json()
        except ValueError as exc:
            duration_seconds = time.perf_counter() - started_at
            record_provider_request("ine", endpoint_family, "invalid_json", duration_seconds)
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
