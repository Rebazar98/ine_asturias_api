from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import Sequence
from html import unescape
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlencode

import httpx

from app.core.cache import BaseAsyncCache
from app.core.logging import get_logger
from app.core.metrics import (
    record_provider_cache_hit,
    record_provider_request,
    record_provider_retry,
)
from app.core.resilience import AsyncCircuitBreaker, CircuitBreakerOpenError
from app.repositories.territorial import normalize_territorial_name
from app.settings import Settings


CATASTRO_PROVIDER = "catastro"
CATASTRO_URBANO_STATS_PAGE_PATH = "es-ES/estadisticas_1.html"
CATASTRO_URBANO_STATS_PAGE_CACHE_SCOPE = "catastro_stats_page"
CATASTRO_URBANO_YEAR_CACHE_SCOPE = "catastro_urbano_year"
CATASTRO_URBANO_TABLE_DEFINITION_SCOPE = "catastro_table_definition"
CATASTRO_URBANO_PROVINCE_SELECT_ID = "select_URB_4"
CATASTRO_URBANO_YEAR_SELECT_ID = "urbano"
CATASTRO_MUNICIPALITIES_SELECT_NAME = "cri1"
CATASTRO_VARIABLES_SELECT_NAME = "cri2"

CATASTRO_VARIABLE_DEFINITIONS = (
    {
        "option_value": "0000",
        "series_key": "catastro_urbano.last_valuation_year",
        "label": "Ano ultima valoracion",
        "unit": "anos",
        "value_kind": "year",
    },
    {
        "option_value": "0001",
        "series_key": "catastro_urbano.urban_parcels",
        "label": "Parcelas urbanas",
        "unit": "unidades",
        "value_kind": "count",
    },
    {
        "option_value": "0002",
        "series_key": "catastro_urbano.urban_parcel_area_hectares",
        "label": "Superficie parcelas urbanas",
        "unit": "hectareas",
        "value_kind": "float",
    },
    {
        "option_value": "0003",
        "series_key": "catastro_urbano.real_estate_assets",
        "label": "Bienes inmuebles",
        "unit": "unidades",
        "value_kind": "count",
    },
    {
        "option_value": "0004",
        "series_key": "catastro_urbano.cadastral_construction_value",
        "label": "Valor catastral construccion",
        "unit": "miles_euros",
        "value_kind": "float",
    },
    {
        "option_value": "0005",
        "series_key": "catastro_urbano.cadastral_land_value",
        "label": "Valor catastral suelo",
        "unit": "miles_euros",
        "value_kind": "float",
    },
    {
        "option_value": "0006",
        "series_key": "catastro_urbano.cadastral_total_value",
        "label": "Valor catastral total",
        "unit": "miles_euros",
        "value_kind": "float",
    },
)


class CatastroClientError(Exception):
    def __init__(self, status_code: int, detail: Any) -> None:
        super().__init__(str(detail))
        self.status_code = status_code
        self.detail = detail


class CatastroUpstreamError(CatastroClientError):
    pass


class CatastroInvalidPayloadError(CatastroClientError):
    pass


class CatastroSelectionError(CatastroClientError):
    pass


class CatastroClientService:
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
        self.logger = get_logger("app.services.catastro_client")

    async def get_reference_year(self) -> str:
        if self.settings.catastro_urbano_year:
            return self.settings.catastro_urbano_year

        cache_key = self._build_cache_key(CATASTRO_URBANO_YEAR_CACHE_SCOPE, "latest", None)
        cached_year = await self.cache.get(cache_key)
        if cached_year:
            record_provider_cache_hit(CATASTRO_PROVIDER, CATASTRO_URBANO_YEAR_CACHE_SCOPE)
            return str(cached_year)

        stats_page_html = await self._fetch_stats_page_html()
        reference_year = _extract_selected_or_latest_year(stats_page_html)
        if not reference_year:
            raise CatastroInvalidPayloadError(
                status_code=502,
                detail={
                    "message": "The Catastro stats page did not expose a reference year.",
                    "path": CATASTRO_URBANO_STATS_PAGE_PATH,
                },
            )

        await self.cache.set(cache_key, reference_year)
        return reference_year

    async def fetch_municipality_aggregates(
        self,
        *,
        province_candidates: Sequence[str],
        municipality_candidates: Sequence[str],
        reference_year: str | None = None,
    ) -> dict[str, Any]:
        reference_year = reference_year or await self.get_reference_year()
        stats_page_html = await self._fetch_stats_page_html()
        province_option = _match_option(
            _extract_select_options(stats_page_html, CATASTRO_URBANO_PROVINCE_SELECT_ID),
            province_candidates,
        )
        if province_option is None:
            raise CatastroSelectionError(
                status_code=404,
                detail={
                    "message": "No Catastro province dataset was found for the requested municipality.",
                    "reference_year": reference_year,
                },
            )

        table_definition_html = await self._fetch_table_definition_html(
            reference_year=reference_year,
            province_file_code=province_option["value"],
        )
        municipality_option = _match_option(
            _extract_select_options(table_definition_html, CATASTRO_MUNICIPALITIES_SELECT_NAME),
            municipality_candidates,
        )
        if municipality_option is None:
            raise CatastroSelectionError(
                status_code=404,
                detail={
                    "message": "The municipality is not available in the Catastro urban dataset.",
                    "reference_year": reference_year,
                    "province_label": province_option["label"],
                },
            )

        result_html = await self._fetch_table_result_html(
            reference_year=reference_year,
            province_file_code=province_option["value"],
            municipality_option_value=municipality_option["value"],
        )
        row_label, raw_values = _extract_result_row(result_html)
        if len(raw_values) != len(CATASTRO_VARIABLE_DEFINITIONS):
            raise CatastroInvalidPayloadError(
                status_code=502,
                detail={
                    "message": "The Catastro result table returned an unexpected number of values.",
                    "reference_year": reference_year,
                    "province_file_code": province_option["value"],
                    "municipality_label": municipality_option["label"],
                    "values_found": len(raw_values),
                },
            )

        indicators: list[dict[str, Any]] = []
        for definition, raw_value in zip(CATASTRO_VARIABLE_DEFINITIONS, raw_values, strict=True):
            parsed_value = _parse_spanish_numeric_value(raw_value, definition["value_kind"])
            indicators.append(
                {
                    "series_key": definition["series_key"],
                    "label": definition["label"],
                    "value": parsed_value,
                    "unit": definition["unit"],
                    "metadata": {
                        "provider": CATASTRO_PROVIDER,
                        "reference_year": reference_year,
                        "variable_label": definition["label"],
                        "provider_value": raw_value,
                        "provider_scope": "municipality",
                        "municipality_label": row_label or municipality_option["label"],
                    },
                }
            )

        return {
            "reference_year": reference_year,
            "province_file_code": province_option["value"],
            "province_label": province_option["label"],
            "municipality_option_value": municipality_option["value"],
            "municipality_label": row_label or municipality_option["label"],
            "indicators": indicators,
            "raw": {
                "content_type": "text/html",
                "reference_year": reference_year,
                "province_file_code": province_option["value"],
                "province_label": province_option["label"],
                "municipality_option_value": municipality_option["value"],
                "municipality_label": row_label or municipality_option["label"],
                "result_html": result_html,
            },
            "metadata": {
                "provider": CATASTRO_PROVIDER,
                "provider_family": "catastro_urbano",
                "reference_year": reference_year,
                "province_file_code": province_option["value"],
                "province_label": province_option["label"],
                "municipality_option_value": municipality_option["value"],
                "municipality_label": row_label or municipality_option["label"],
                "stats_page_path": CATASTRO_URBANO_STATS_PAGE_PATH,
                "table_definition_path": "/jaxi/tabla.do",
            },
        }

    async def _fetch_stats_page_html(self) -> str:
        cache_key = self._build_cache_key(
            CATASTRO_URBANO_STATS_PAGE_CACHE_SCOPE, "stats_page", None
        )
        cached = await self.cache.get(cache_key)
        if cached is not None:
            record_provider_cache_hit(CATASTRO_PROVIDER, CATASTRO_URBANO_STATS_PAGE_CACHE_SCOPE)
            self.logger.info(
                "catastro_stats_page_cache_hit", extra={"path": CATASTRO_URBANO_STATS_PAGE_PATH}
            )
            return str(cached)

        html = await self._request_text_with_resilience(
            method="GET",
            url=self._build_url(CATASTRO_URBANO_STATS_PAGE_PATH),
            path=CATASTRO_URBANO_STATS_PAGE_PATH,
            endpoint_family="stats_page",
        )
        await self.cache.set(cache_key, html)
        return html

    async def _fetch_table_definition_html(
        self,
        *,
        reference_year: str,
        province_file_code: str,
    ) -> str:
        params = self._build_table_params(
            reference_year=reference_year, province_file_code=province_file_code
        )
        cache_key = self._build_cache_key(
            CATASTRO_URBANO_TABLE_DEFINITION_SCOPE,
            province_file_code,
            {"reference_year": reference_year},
        )
        cached = await self.cache.get(cache_key)
        if cached is not None:
            record_provider_cache_hit(CATASTRO_PROVIDER, CATASTRO_URBANO_TABLE_DEFINITION_SCOPE)
            self.logger.info(
                "catastro_table_definition_cache_hit",
                extra={"reference_year": reference_year, "province_file_code": province_file_code},
            )
            return str(cached)

        html = await self._request_text_with_resilience(
            method="GET",
            url=self._build_url("/jaxi/tabla.do"),
            path="/jaxi/tabla.do",
            params=params,
            endpoint_family="table_definition",
        )
        await self.cache.set(cache_key, html)
        return html

    async def _fetch_table_result_html(
        self,
        *,
        reference_year: str,
        province_file_code: str,
        municipality_option_value: str,
    ) -> str:
        body = self._build_result_form_body(
            reference_year=reference_year,
            province_file_code=province_file_code,
            municipality_option_value=municipality_option_value,
        )
        return await self._request_text_with_resilience(
            method="POST",
            url=self._build_url("/jaxi/tabla.do"),
            path="/jaxi/tabla.do",
            data=body,
            endpoint_family="municipality_aggregates",
        )

    async def _request_text_with_resilience(
        self,
        *,
        method: str,
        url: str,
        path: str,
        endpoint_family: str,
        params: dict[str, Any] | None = None,
        data: str | None = None,
    ) -> str:
        started_at = time.perf_counter()
        if self.circuit_breaker is not None:
            try:
                await self.circuit_breaker.before_call()
            except CircuitBreakerOpenError as exc:
                record_provider_request(CATASTRO_PROVIDER, endpoint_family, "circuit_open", 0.0)
                raise CatastroUpstreamError(
                    status_code=503,
                    detail={
                        "message": "The Catastro service is temporarily unavailable.",
                        "path": path,
                        "retryable": True,
                        "retry_after_seconds": round(exc.retry_after_seconds, 3),
                    },
                ) from exc

        deadline = started_at + self.settings.provider_total_timeout_seconds
        backoff_seconds = self.settings.http_retry_backoff_seconds
        max_attempts = self.settings.http_retry_max_attempts

        for attempt in range(1, max_attempts + 1):
            try:
                response = await self.http_client.request(
                    method,
                    url,
                    params=params,
                    content=data,
                    headers=(
                        {"Content-Type": "application/x-www-form-urlencoded"}
                        if data is not None
                        else None
                    ),
                    timeout=self.settings.catastro_timeout_seconds,
                )
                response.raise_for_status()
                payload = response.text
                if not payload.strip():
                    raise CatastroInvalidPayloadError(
                        status_code=502,
                        detail={
                            "message": "The Catastro service returned an empty response.",
                            "path": path,
                        },
                    )
                duration_seconds = time.perf_counter() - started_at
                record_provider_request(
                    CATASTRO_PROVIDER, endpoint_family, "success", duration_seconds
                )
                if self.circuit_breaker is not None:
                    await self.circuit_breaker.record_success()
                self.logger.info(
                    "catastro_request_completed",
                    extra={
                        "method": method,
                        "path": path,
                        "status_code": response.status_code,
                        "duration_ms": round(duration_seconds * 1000, 2),
                    },
                )
                return payload
            except CatastroInvalidPayloadError:
                duration_seconds = time.perf_counter() - started_at
                record_provider_request(
                    CATASTRO_PROVIDER, endpoint_family, "invalid_payload", duration_seconds
                )
                if self.circuit_breaker is not None:
                    await self.circuit_breaker.record_failure(reason="invalid_payload")
                raise
            except httpx.HTTPStatusError as exc:
                retryable = self._is_retryable_status(exc.response.status_code)
                if self._can_retry(attempt, deadline, retryable, backoff_seconds):
                    await self._log_retry(
                        endpoint_family=endpoint_family,
                        path=path,
                        attempt=attempt,
                        reason=f"http_{exc.response.status_code}",
                        backoff_seconds=backoff_seconds,
                    )
                    await asyncio.sleep(backoff_seconds)
                    backoff_seconds *= 2
                    continue

                duration_seconds = time.perf_counter() - started_at
                record_provider_request(
                    CATASTRO_PROVIDER, endpoint_family, "http_error", duration_seconds
                )
                if self.circuit_breaker is not None:
                    await self.circuit_breaker.record_failure(
                        reason=f"http_{exc.response.status_code}"
                    )
                raise CatastroUpstreamError(
                    status_code=exc.response.status_code,
                    detail={
                        "message": "The Catastro service returned an error.",
                        "path": path,
                        "status_code": exc.response.status_code,
                        "retryable": retryable,
                    },
                ) from exc
            except httpx.RequestError as exc:
                if self._can_retry(attempt, deadline, True, backoff_seconds):
                    await self._log_retry(
                        endpoint_family=endpoint_family,
                        path=path,
                        attempt=attempt,
                        reason="request_error",
                        backoff_seconds=backoff_seconds,
                    )
                    await asyncio.sleep(backoff_seconds)
                    backoff_seconds *= 2
                    continue

                duration_seconds = time.perf_counter() - started_at
                record_provider_request(
                    CATASTRO_PROVIDER, endpoint_family, "request_error", duration_seconds
                )
                if self.circuit_breaker is not None:
                    await self.circuit_breaker.record_failure(reason="request_error")
                raise CatastroUpstreamError(
                    status_code=502,
                    detail={
                        "message": "Could not connect to the Catastro service.",
                        "path": path,
                        "retryable": True,
                    },
                ) from exc

        raise RuntimeError("Retry loop exhausted without returning a Catastro response.")

    async def _log_retry(
        self,
        *,
        endpoint_family: str,
        path: str,
        attempt: int,
        reason: str,
        backoff_seconds: float,
    ) -> None:
        record_provider_retry(CATASTRO_PROVIDER, endpoint_family, reason)
        self.logger.warning(
            "catastro_retry_scheduled",
            extra={
                "path": path,
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
    def _is_retryable_status(status_code: int) -> bool:
        return status_code in {408, 429, 500, 502, 503, 504}

    def _build_url(self, path: str) -> str:
        base_url = self.settings.catastro_base_url.rstrip("/")
        normalized_path = path if path.startswith("/") else f"/{path}"
        return f"{base_url}{normalized_path}"

    @staticmethod
    def _build_table_params(*, reference_year: str, province_file_code: str) -> dict[str, Any]:
        return {
            "path": f"/est{reference_year}/catastro/urbano/",
            "file": f"{province_file_code}.px",
            "type": "pcaxis",
            "L": 0,
        }

    @staticmethod
    def _build_result_form_body(
        *,
        reference_year: str,
        province_file_code: str,
        municipality_option_value: str,
    ) -> str:
        pairs = [
            ("type", "pcaxis"),
            ("path", f"/est{reference_year}/catastro/urbano/"),
            ("file", f"{province_file_code}.px"),
            ("divi", ""),
            ("per", ""),
            ("idtab", ""),
            ("accion", "html"),
            ("numCri", "2"),
            ("sel_1", "1"),
            ("sel_2", str(len(CATASTRO_VARIABLE_DEFINITIONS))),
            ("NumCeldas", str(len(CATASTRO_VARIABLE_DEFINITIONS))),
            ("cri1", municipality_option_value),
        ]
        for definition in CATASTRO_VARIABLE_DEFINITIONS:
            pairs.append(("cri2", definition["option_value"]))
        pairs.extend(
            [
                ("rows", "Municipios"),
                ("columns", "Variables Catastro"),
            ]
        )
        return urlencode(pairs, doseq=True)

    @staticmethod
    def _build_cache_key(scope: str, path: str, params: dict[str, Any] | None) -> str:
        serialized_params = json.dumps(params or {}, sort_keys=True, default=str)
        return f"{scope}:{path}:{serialized_params}"


def _extract_selected_or_latest_year(html: str) -> str | None:
    options = _extract_select_options(html, CATASTRO_URBANO_YEAR_SELECT_ID)
    if not options:
        return None
    selected = next((option["value"] for option in options if option["selected"]), None)
    return selected or options[-1]["value"]


def _extract_select_options(html: str, select_name_or_id: str) -> list[dict[str, Any]]:
    parser = _SelectOptionsParser(targets={select_name_or_id})
    parser.feed(html)
    parser.close()
    return parser.options_by_target.get(select_name_or_id, [])


def _match_option(
    options: Sequence[dict[str, Any]],
    candidates: Sequence[str],
) -> dict[str, Any] | None:
    normalized_candidates: set[str] = set()
    for candidate in candidates:
        normalized_candidates.update(_build_name_candidates(candidate))

    for option in options:
        option_keys = _build_name_candidates(str(option["label"]))
        if option_keys & normalized_candidates:
            return option
    return None


def _build_name_candidates(value: str) -> set[str]:
    normalized = normalize_territorial_name(value)
    if not normalized:
        return set()

    candidates = {normalized}
    match_parenthetical = re.fullmatch(r"(.+)\s+(el|la|los|las)", normalized)
    if match_parenthetical:
        base, article = match_parenthetical.groups()
        candidates.add(f"{article} {base}".strip())
    match_leading = re.fullmatch(r"(el|la|los|las)\s+(.+)", normalized)
    if match_leading:
        article, base = match_leading.groups()
        candidates.add(f"{base} {article}".strip())
    return {candidate.strip() for candidate in candidates if candidate.strip()}


def _extract_result_row(html: str) -> tuple[str | None, list[str]]:
    row_label_match = re.search(
        r'<td[^>]*class="tableCellGr"[^>]*nowrap[^>]*>\s*(?P<label>.*?)\s*</td>',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if row_label_match is None:
        row_label_match = re.search(
            r'<td[^>]*nowrap[^>]*class="tableCellGr"[^>]*>\s*(?P<label>.*?)\s*</td>',
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )

    row_label = _strip_html(row_label_match.group("label")) if row_label_match else None
    values = [
        _strip_html(match.group("value"))
        for match in re.finditer(
            r'<td[^>]*class="dataCell"[^>]*>\s*(?P<value>.*?)\s*</td>',
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
    ]
    cleaned_values = [value for value in values if value]
    return row_label, cleaned_values


def _strip_html(value: str) -> str:
    without_tags = re.sub(r"<[^>]+>", " ", value or "")
    normalized = re.sub(r"\s+", " ", unescape(without_tags))
    return normalized.strip()


def _parse_spanish_numeric_value(raw_value: str, value_kind: str) -> int | float | None:
    normalized = raw_value.strip()
    if not normalized:
        return None

    canonical = normalized.replace(".", "").replace(",", ".")
    try:
        numeric_value = float(canonical)
    except ValueError as exc:
        raise CatastroInvalidPayloadError(
            status_code=502,
            detail={
                "message": "The Catastro service returned a non-numeric aggregate value.",
                "value": raw_value,
            },
        ) from exc

    if value_kind in {"year", "count"}:
        return int(round(numeric_value))
    return numeric_value


class _SelectOptionsParser(HTMLParser):
    def __init__(self, *, targets: set[str]) -> None:
        super().__init__(convert_charrefs=True)
        self.targets = targets
        self.current_target: str | None = None
        self.current_option: dict[str, Any] | None = None
        self.options_by_target: dict[str, list[dict[str, Any]]] = {target: [] for target in targets}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)
        if tag == "select":
            target = attr_map.get("id") or attr_map.get("name")
            self.current_target = target if target in self.targets else None
            return

        if tag == "option" and self.current_target is not None:
            self.current_option = {
                "value": attr_map.get("value", ""),
                "label": "",
                "selected": "selected" in attr_map or attr_map.get("selected") is not None,
            }

    def handle_data(self, data: str) -> None:
        if self.current_option is None:
            return
        self.current_option["label"] += data

    def handle_endtag(self, tag: str) -> None:
        if tag == "option" and self.current_target is not None and self.current_option is not None:
            option = dict(self.current_option)
            option["label"] = re.sub(r"\s+", " ", option["label"]).strip()
            self.options_by_target[self.current_target].append(option)
            self.current_option = None
            return

        if tag == "select":
            self.current_target = None
