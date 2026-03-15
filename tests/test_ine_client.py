from __future__ import annotations

import httpx
import pytest

from app.core.cache import InMemoryTTLCache
from app.core.resilience import AsyncCircuitBreaker
from app.services.ine_client import INEClientService, INEUpstreamError
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
