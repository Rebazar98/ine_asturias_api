from __future__ import annotations

import httpx
import pytest

from app.core.cache import InMemoryTTLCache
from app.core.resilience import AsyncCircuitBreaker
from app.services.ine_client import (
    INEClientService,
    INEInvalidPayloadError,
    INEUpstreamError,
    _fix_ine_encoding,
)
from app.settings import Settings


def build_service(
    handler,
    *,
    circuit_breaker: AsyncCircuitBreaker | None = None,
    **settings_overrides,
) -> INEClientService:
    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(transport=transport)
    defaults = {
        "http_retry_max_attempts": 3,
        "http_retry_backoff_seconds": 0.001,
        "provider_total_timeout_seconds": 1.0,
    }
    defaults.update(settings_overrides)
    settings = Settings(
        ine_base_url="https://mocked.ine",
        enable_cache=True,
        cache_ttl_seconds=60,
        **defaults,
    )
    cache = InMemoryTTLCache(enabled=True, default_ttl_seconds=60)
    return INEClientService(
        http_client=http_client,
        settings=settings,
        cache=cache,
        circuit_breaker=circuit_breaker,
    )


def _double_mojibake(value: str) -> str:
    return value.encode("utf-8").decode("latin-1").encode("utf-8").decode("cp1252")


@pytest.mark.anyio
async def test_request_error_retries_before_succeeding() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise httpx.ReadTimeout("provider timeout", request=request)
        return httpx.Response(200, json=[{"Id": "22"}], request=request)

    service = build_service(handler)

    payload = await service.get_operation_tables("22")

    assert payload == [{"Id": "22"}]
    assert calls == 3


@pytest.mark.anyio
async def test_circuit_breaker_opens_after_repeated_failures() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"detail": "service unavailable"}, request=request)

    breaker = AsyncCircuitBreaker(
        provider="ine",
        fail_max=1,
        reset_timeout_seconds=30,
        half_open_sample_size=5,
        success_threshold=0.8,
    )
    service = build_service(
        handler,
        circuit_breaker=breaker,
        http_retry_max_attempts=1,
    )

    with pytest.raises(INEUpstreamError) as first_error:
        await service.get_operation_tables("22")

    with pytest.raises(INEUpstreamError) as second_error:
        await service.get_operation_tables("22")

    assert first_error.value.status_code == 503
    assert second_error.value.status_code == 503
    assert second_error.value.detail["message"] == "The INE service is temporarily unavailable."
    assert second_error.value.detail["retryable"] is True


@pytest.mark.anyio
async def test_get_operation_variables_returns_empty_list_on_empty_body() -> None:
    """INE returns HTTP 200 with empty body for non-existent operation codes."""

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"", request=request)

    service = build_service(handler)
    result = await service.get_operation_variables("9999")
    assert result == []


@pytest.mark.anyio
async def test_cache_hit_skips_http_call() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=[{"Id": "22"}], request=request)

    service = build_service(handler)

    first = await service.get_operation_tables("22")
    second = await service.get_operation_tables("22")

    assert first == second == [{"Id": "22"}]
    assert calls == 1


@pytest.mark.anyio
async def test_invalid_json_response_raises_invalid_payload_error() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not json", request=request)

    service = build_service(handler)

    with pytest.raises(INEInvalidPayloadError) as exc_info:
        await service.get_operation_tables("22")

    assert exc_info.value.status_code == 502
    assert "invalid JSON" in exc_info.value.detail["message"]


@pytest.mark.anyio
async def test_unexpected_json_type_raises_invalid_payload_error() -> None:
    """INE returns a JSON scalar (e.g. a number) instead of list/dict."""

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"42", request=request)

    service = build_service(handler)

    with pytest.raises(INEInvalidPayloadError) as exc_info:
        await service.get_operation_tables("22")

    assert exc_info.value.status_code == 502
    assert "unexpected JSON format" in exc_info.value.detail["message"]


@pytest.mark.anyio
async def test_non_retryable_4xx_fails_immediately() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(404, json={"detail": "not found"}, request=request)

    service = build_service(handler, http_retry_max_attempts=3)

    with pytest.raises(INEUpstreamError) as exc_info:
        await service.get_operation_tables("22")

    assert exc_info.value.status_code == 404
    assert calls == 1


@pytest.mark.anyio
async def test_all_retries_exhausted_raises_upstream_error() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("always fails", request=request)

    service = build_service(handler, http_retry_max_attempts=3)

    with pytest.raises(INEUpstreamError) as exc_info:
        await service.get_operation_tables("22")

    assert exc_info.value.status_code == 502
    assert calls == 3


@pytest.mark.anyio
async def test_get_variable_values_returns_empty_list_on_empty_body() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"", request=request)

    service = build_service(handler)
    result = await service.get_variable_values("22", "349")
    assert result == []


@pytest.mark.anyio
async def test_get_table_returns_payload() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[{"Nombre": "Serie 1", "Data": []}], request=request)

    service = build_service(handler)
    result = await service.get_table("2852", {"g1": "349:19"})
    assert result == [{"Nombre": "Serie 1", "Data": []}]


@pytest.mark.anyio
async def test_get_operation_series_passes_pagination_params() -> None:
    received_params: dict = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        received_params.update(dict(request.url.params))
        return httpx.Response(200, json=[{"COD": "IPC333"}], request=request)

    service = build_service(handler)
    result = await service.get_operation_series("25", page=2)

    assert result == [{"COD": "IPC333"}]
    assert received_params.get("page") == "2"
    assert received_params.get("det") == "2"


@pytest.mark.anyio
async def test_get_serie_data_without_nult() -> None:
    received_params: dict = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        received_params.update(dict(request.url.params))
        return httpx.Response(200, json={"COD": "IPC333", "Data": []}, request=request)

    service = build_service(handler)
    result = await service.get_serie_data("IPC333")

    assert result == {"COD": "IPC333", "Data": []}
    assert "nult" not in received_params


@pytest.mark.anyio
async def test_get_serie_data_with_nult() -> None:
    received_params: dict = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        received_params.update(dict(request.url.params))
        return httpx.Response(200, json={"COD": "IPC333", "Data": []}, request=request)

    service = build_service(handler)
    await service.get_serie_data("IPC333", nult=12)

    assert received_params.get("nult") == "12"


@pytest.mark.anyio
async def test_get_operation_tables_fixes_mojibake_encoding() -> None:
    """When INE returns UTF-8 bytes misencoded as Latin-1 in string fields, the
    client must return the corrected Unicode strings (no mojibake in output)."""

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                {"Id": "2852", "Nombre": "Tasa Bruta de InmigraciÃ³n procedente del extranjero"},
                {"Id": "2853", "Nombre": "Nacidos vivos segÃºn edad de la madre"},
                {"Id": "2854", "Nombre": "PoblaciÃ³n por sexo y edad"},
            ],
            request=request,
        )

    service = build_service(handler)
    result = await service.get_operation_tables("33")

    assert result[0]["Nombre"] == "Tasa Bruta de Inmigración procedente del extranjero"
    assert result[1]["Nombre"] == "Nacidos vivos según edad de la madre"
    assert result[2]["Nombre"] == "Población por sexo y edad"


@pytest.mark.anyio
async def test_get_variables_fixes_double_mojibake_encoding() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                {
                    "Id": 3,
                    "Nombre": _double_mojibake("Comunidades y Ciudades Autónomas"),
                    "Codigo": "CCAA",
                }
            ],
            request=request,
        )

    service = build_service(handler)
    result = await service.get_variables()

    assert result == [{"Id": 3, "Nombre": "Comunidades y Ciudades Autónomas", "Codigo": "CCAA"}]


def test_fix_ine_encoding_corrects_mojibake() -> None:
    """UTF-8 bytes stored as Latin-1 codepoints must be round-tripped correctly."""
    assert _fix_ine_encoding("PoblaciÃ³n") == "Población"
    assert _fix_ine_encoding("segÃºn") == "según"
    assert _fix_ine_encoding({"Nombre": "PoblaciÃ³n"}) == {"Nombre": "Población"}
    assert _fix_ine_encoding(["PoblaciÃ³n", "segÃºn"]) == ["Población", "según"]


def test_fix_ine_encoding_corrects_double_mojibake() -> None:
    broken = _double_mojibake("Comunidades y Ciudades Autónomas")
    assert _fix_ine_encoding(broken) == "Comunidades y Ciudades Autónomas"


def test_fix_ine_encoding_passes_through_clean_strings() -> None:
    assert _fix_ine_encoding("Tasa de Natalidad") == "Tasa de Natalidad"
    assert _fix_ine_encoding(42) == 42
    assert _fix_ine_encoding(None) is None


# ---------------------------------------------------------------------------
# get_variables
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_get_variables_returns_list_of_variables() -> None:
    """Happy path: /VARIABLES returns a JSON list of {Id, Nombre, Codigo} objects."""

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                {"Id": 3, "Nombre": "Comunidades y Ciudades Autónomas", "Codigo": "CCAA"},
                {"Id": 13, "Nombre": "Municipios", "Codigo": "MUN"},
            ],
            request=request,
        )

    service = build_service(handler)
    result = await service.get_variables()

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["Id"] == 3
    assert result[1]["Nombre"] == "Municipios"


@pytest.mark.anyio
async def test_get_variables_hits_variables_endpoint() -> None:
    """The request URL must target the VARIABLES path (no operation code)."""
    captured_path: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        captured_path.append(request.url.path)
        return httpx.Response(200, json=[], request=request)

    service = build_service(handler)
    await service.get_variables()

    assert captured_path[0].endswith("/VARIABLES")


@pytest.mark.anyio
async def test_get_variables_returns_empty_list_on_empty_body() -> None:
    """INE sometimes returns HTTP 200 with an empty body — should return []."""

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"", request=request)

    service = build_service(handler)
    result = await service.get_variables()

    assert result == []


@pytest.mark.anyio
async def test_get_variables_returns_empty_list_when_response_is_dict() -> None:
    """If INE unexpectedly returns a JSON object instead of a list, return []."""

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"error": "unexpected"}, request=request)

    service = build_service(handler)
    result = await service.get_variables()

    assert result == []
