from __future__ import annotations

import httpx
import pytest

from app.core.cache import InMemoryTTLCache
from app.services.cartociudad_client import (
    CartoCiudadClientService,
    CartoCiudadInvalidPayloadError,
    CartoCiudadUpstreamError,
)
from app.settings import Settings


def build_service(handler, enable_cache: bool = True) -> CartoCiudadClientService:
    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(transport=transport)
    settings = Settings(
        cartociudad_base_url="https://mocked.cartociudad/geocoder/api/geocoder",
        enable_cache=enable_cache,
        cache_ttl_seconds=60,
    )
    cache = InMemoryTTLCache(enabled=enable_cache, default_ttl_seconds=60)
    return CartoCiudadClientService(http_client=http_client, settings=settings, cache=cache)


@pytest.mark.anyio
async def test_geocode_calls_find_endpoint_with_query_param() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/find")
        assert request.url.params["q"] == "Oviedo"
        return httpx.Response(200, json=[{"id": "oviedo"}])

    service = build_service(handler)
    payload = await service.geocode("Oviedo")

    assert payload == [{"id": "oviedo"}]


@pytest.mark.anyio
async def test_reverse_geocode_calls_reverse_endpoint_with_coordinates() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/reverseGeocode")
        assert request.url.params["lat"] == "43.3614"
        assert request.url.params["lon"] == "-5.8494"
        return httpx.Response(200, json={"label": "Oviedo"})

    service = build_service(handler)
    payload = await service.reverse_geocode(lat=43.3614, lon=-5.8494)

    assert payload == {"label": "Oviedo"}


@pytest.mark.anyio
async def test_geocode_uses_cache_when_enabled() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=[{"id": "oviedo"}])

    service = build_service(handler, enable_cache=True)

    first = await service.geocode("Oviedo")
    second = await service.geocode("Oviedo")

    assert first == second == [{"id": "oviedo"}]
    assert calls == 1


@pytest.mark.anyio
async def test_http_error_raises_cartociudad_upstream_error() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"detail": "service unavailable"})

    service = build_service(handler)

    with pytest.raises(CartoCiudadUpstreamError) as exc_info:
        await service.geocode("Oviedo")

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail["request_context"]["query_length"] == 6
    assert "params" not in exc_info.value.detail


@pytest.mark.anyio
async def test_invalid_json_raises_cartociudad_invalid_payload_error() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="not-json", headers={"content-type": "application/json"})

    service = build_service(handler)

    with pytest.raises(CartoCiudadInvalidPayloadError) as exc_info:
        await service.geocode("Oviedo")

    assert exc_info.value.status_code == 502
    assert exc_info.value.detail["request_context"]["query_terms"] == 1


@pytest.mark.anyio
async def test_unexpected_json_type_raises_cartociudad_invalid_payload_error() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json="unexpected")

    service = build_service(handler)

    with pytest.raises(CartoCiudadInvalidPayloadError) as exc_info:
        await service.geocode("Oviedo")

    assert exc_info.value.status_code == 502
    assert exc_info.value.detail["request_context"]["query_fingerprint"]
