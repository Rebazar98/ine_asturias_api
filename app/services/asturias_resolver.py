from __future__ import annotations

import json
import unicodedata
from typing import Any

from app.core.cache import InMemoryTTLCache
from app.core.logging import get_logger
from app.schemas import AsturiasResolutionResult
from app.services.ine_client import INEClientService


_GEO_KEYWORDS = {
    "comunidad": 10,
    "autonoma": 10,
    "ccaa": 9,
    "territorial": 7,
    "geo": 6,
    "geografica": 6,
    "provincia": 5,
    "municipio": 4,
}


class AsturiasResolutionError(Exception):
    def __init__(self, detail: Any, status_code: int = 422) -> None:
        super().__init__(str(detail))
        self.status_code = status_code
        self.detail = detail


class AsturiasResolver:
    def __init__(self, ine_client: INEClientService, cache: InMemoryTTLCache) -> None:
        self.ine_client = ine_client
        self.cache = cache
        self.logger = get_logger("app.services.asturias_resolver")

    async def resolve(
        self,
        op_code: str,
        geo_variable_id: str | None = None,
        asturias_value_id: str | None = None,
    ) -> AsturiasResolutionResult:
        cache_key = self._cache_key(op_code, geo_variable_id, asturias_value_id)
        cached = await self.cache.get(cache_key)
        if cached is not None:
            return AsturiasResolutionResult.model_validate(cached)

        if geo_variable_id and asturias_value_id:
            result = AsturiasResolutionResult(
                geo_variable_id=geo_variable_id,
                asturias_value_id=asturias_value_id,
            )
            await self.cache.set(cache_key, result.model_dump())
            return result

        geo_candidate = {"id": geo_variable_id, "name": None} if geo_variable_id else None
        if geo_candidate is None:
            variables_payload = await self.ine_client.get_operation_variables(op_code)
            geo_candidate = self._detect_geo_variable(variables_payload)

        if not geo_candidate:
            raise AsturiasResolutionError(
                detail={
                    "message": "Could not resolve the geographic variable for this operation.",
                    "hint": "Provide geo_variable_id manually.",
                    "operation_code": op_code,
                }
            )

        asturias_candidate = {"id": asturias_value_id, "name": None} if asturias_value_id else None
        if asturias_candidate is None:
            values_payload = await self.ine_client.get_variable_values(op_code, geo_candidate["id"])
            asturias_candidate = self._detect_asturias_value(values_payload)

        if not asturias_candidate:
            raise AsturiasResolutionError(
                detail={
                    "message": "Could not resolve Asturias within the geographic variable values.",
                    "hint": "Provide asturias_value_id manually.",
                    "operation_code": op_code,
                    "geo_variable_id": geo_candidate["id"],
                }
            )

        result = AsturiasResolutionResult(
            geo_variable_id=geo_candidate["id"],
            asturias_value_id=asturias_candidate["id"],
            variable_name=geo_candidate.get("name"),
            asturias_label=asturias_candidate.get("name"),
        )
        await self.cache.set(cache_key, result.model_dump())
        self.logger.info(
            "asturias_resolution_completed",
            extra={
                "operation_code": op_code,
                "geo_variable_id": result.geo_variable_id,
                "asturias_value_id": result.asturias_value_id,
            },
        )
        return result

    def _detect_geo_variable(self, payload: dict[str, Any] | list[Any]) -> dict[str, str] | None:
        candidates: list[tuple[int, dict[str, str]]] = []
        for record in self._iter_records(payload):
            record_id = self._pick_first(record, ("Id", "id", "Codigo", "codigo"))
            if not record_id:
                continue
            name = self._pick_first(record, ("Nombre", "name", "Variable", "Descripcion", "description"))
            normalized_name = self._normalize_text(name)
            score = max((weight for key, weight in _GEO_KEYWORDS.items() if key in normalized_name), default=0)
            if score > 0:
                candidates.append((score, {"id": str(record_id), "name": name}))

        if not candidates:
            return None

        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    def _detect_asturias_value(self, payload: dict[str, Any] | list[Any]) -> dict[str, str] | None:
        candidates: list[tuple[int, dict[str, str]]] = []
        for record in self._iter_records(payload):
            record_id = self._pick_first(record, ("Id", "id", "Codigo", "codigo"))
            if not record_id:
                continue
            name = self._pick_first(record, ("Nombre", "name", "Descripcion", "description", "Valor"))
            normalized_name = self._normalize_text(name)
            if "asturias" not in normalized_name:
                continue
            score = 20 if normalized_name == "principado de asturias" else 10
            candidates.append((score, {"id": str(record_id), "name": name}))

        if not candidates:
            return None

        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    def _iter_records(self, payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            return [payload]
        return []

    @staticmethod
    def _pick_first(payload: dict[str, Any], keys: tuple[str, ...]) -> str:
        for key in keys:
            value = payload.get(key)
            if value not in (None, ""):
                return str(value)
        return ""

    @staticmethod
    def _normalize_text(value: str) -> str:
        normalized = unicodedata.normalize("NFKD", value or "")
        return normalized.encode("ascii", "ignore").decode("ascii").lower().strip()

    @staticmethod
    def _cache_key(
        op_code: str,
        geo_variable_id: str | None,
        asturias_value_id: str | None,
    ) -> str:
        payload = {
            "op_code": op_code,
            "geo_variable_id": geo_variable_id,
            "asturias_value_id": asturias_value_id,
        }
        return f"asturias_resolution:{json.dumps(payload, sort_keys=True)}"
