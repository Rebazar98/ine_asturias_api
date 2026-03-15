import asyncio

import httpx
import pytest

from app.core.cache import InMemoryTTLCache
from app.services.catastro_client import CatastroClientService, CatastroInvalidPayloadError
from app.settings import Settings


STATS_PAGE_HTML = """
<html>
  <select id="urbano">
    <option value="2024">2024</option>
    <option value="2025" selected>2025</option>
  </select>
  <select id="select_URB_4">
    <option value="04133">Asturias</option>
  </select>
</html>
"""

TABLE_DEFINITION_HTML = """
<html>
  <select name="cri1">
    <option value="0043">Oviedo</option>
  </select>
  <select name="cri2" multiple="multiple">
    <option value="0000">Ano ultima valoracion</option>
    <option value="0001">Parcelas urbanas</option>
    <option value="0002">Superficie parcelas urbanas</option>
    <option value="0003">Bienes inmuebles</option>
    <option value="0004">Valor catastral construccion</option>
    <option value="0005">Valor catastral suelo</option>
    <option value="0006">Valor catastral total</option>
  </select>
</html>
"""

RESULT_HTML = """
<html>
  <table border="0">
    <tr>
      <td class="tableCellGr"></td>
      <td class="tableCellGr">Ano ultima valoracion</td>
      <td class="tableCellGr">Parcelas urbanas</td>
      <td class="tableCellGr">Superficie parcelas urbanas</td>
      <td class="tableCellGr">Bienes inmuebles</td>
      <td class="tableCellGr">Valor catastral construccion</td>
      <td class="tableCellGr">Valor catastral suelo</td>
      <td class="tableCellGr">Valor catastral total</td>
    </tr>
    <tr>
      <td nowrap class="tableCellGr">Oviedo</td>
      <td class="dataCell">2.013</td>
      <td class="dataCell">23.783</td>
      <td class="dataCell">3.277,79</td>
      <td class="dataCell">243.583</td>
      <td class="dataCell">6.430.733,88</td>
      <td class="dataCell">8.120.131,57</td>
      <td class="dataCell">14.550.865,45</td>
    </tr>
  </table>
</html>
"""

INVALID_RESULT_HTML = """
<html>
  <table border="0">
    <tr>
      <td nowrap class="tableCellGr">Oviedo</td>
      <td class="dataCell">2.013</td>
      <td class="dataCell">23.783</td>
    </tr>
  </table>
</html>
"""


def _build_service(
    *,
    handler,
    catastro_urbano_year: str | None = None,
) -> tuple[CatastroClientService, httpx.AsyncClient]:
    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(transport=transport)
    settings = Settings(
        catastro_base_url="https://mocked.catastro",
        enable_cache=True,
        cache_ttl_seconds=60,
        catastro_timeout_seconds=10,
        catastro_urbano_year=catastro_urbano_year,
    )
    cache = InMemoryTTLCache(enabled=True, default_ttl_seconds=60)
    return CatastroClientService(
        http_client=http_client, settings=settings, cache=cache
    ), http_client


def test_catastro_client_fetches_municipality_aggregates_and_discovers_year():
    call_history: list[tuple[str, str, bytes]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_history.append((request.method, request.url.path, request.url.query))
        if request.method == "GET" and request.url.path == "/es-ES/estadisticas_1.html":
            return httpx.Response(200, text=STATS_PAGE_HTML)
        if request.method == "GET" and request.url.path == "/jaxi/tabla.do":
            assert request.url.params["file"] == "04133.px"
            return httpx.Response(200, text=TABLE_DEFINITION_HTML)
        if request.method == "POST" and request.url.path == "/jaxi/tabla.do":
            body = request.content.decode("utf-8")
            assert "cri1=0043" in body
            assert body.count("cri2=") == 7
            return httpx.Response(200, text=RESULT_HTML)
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    service, http_client = _build_service(handler=handler)

    result = asyncio.run(
        service.fetch_municipality_aggregates(
            province_candidates=["Asturias"],
            municipality_candidates=["Oviedo"],
        )
    )

    assert result["reference_year"] == "2025"
    assert result["province_file_code"] == "04133"
    assert result["municipality_label"] == "Oviedo"
    assert len(result["indicators"]) == 7
    assert result["indicators"][0]["value"] == 2013
    assert result["indicators"][-1]["value"] == 14550865.45
    assert call_history[0] == ("GET", "/es-ES/estadisticas_1.html", b"")

    second_year = asyncio.run(service.get_reference_year())
    assert second_year == "2025"
    assert len([call for call in call_history if call[1] == "/es-ES/estadisticas_1.html"]) == 1

    asyncio.run(http_client.aclose())


def test_catastro_client_uses_configured_reference_year_without_discovery():
    call_history: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_history.append((request.method, request.url.path))
        if request.method == "GET" and request.url.path == "/es-ES/estadisticas_1.html":
            return httpx.Response(200, text=STATS_PAGE_HTML)
        if request.method == "GET" and request.url.path == "/jaxi/tabla.do":
            assert request.url.params["path"] == "/est2023/catastro/urbano/"
            assert request.url.params["file"] == "04133.px"
            return httpx.Response(200, text=TABLE_DEFINITION_HTML)
        if request.method == "POST" and request.url.path == "/jaxi/tabla.do":
            body = request.content.decode("utf-8")
            assert "path=%2Fest2023%2Fcatastro%2Furbano%2F" in body
            return httpx.Response(200, text=RESULT_HTML)
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    service, http_client = _build_service(handler=handler, catastro_urbano_year="2023")

    result = asyncio.run(
        service.fetch_municipality_aggregates(
            province_candidates=["Asturias"],
            municipality_candidates=["Oviedo"],
        )
    )

    assert result["reference_year"] == "2023"
    assert call_history == [
        ("GET", "/es-ES/estadisticas_1.html"),
        ("GET", "/jaxi/tabla.do"),
        ("POST", "/jaxi/tabla.do"),
    ]

    asyncio.run(http_client.aclose())


def test_catastro_client_raises_invalid_payload_when_result_shape_is_incomplete():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/es-ES/estadisticas_1.html":
            return httpx.Response(200, text=STATS_PAGE_HTML)
        if request.method == "GET" and request.url.path == "/jaxi/tabla.do":
            return httpx.Response(200, text=TABLE_DEFINITION_HTML)
        if request.method == "POST" and request.url.path == "/jaxi/tabla.do":
            return httpx.Response(200, text=INVALID_RESULT_HTML)
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    service, http_client = _build_service(handler=handler)

    with pytest.raises(CatastroInvalidPayloadError) as excinfo:
        asyncio.run(
            service.fetch_municipality_aggregates(
                province_candidates=["Asturias"],
                municipality_candidates=["Oviedo"],
            )
        )

    assert excinfo.value.status_code == 502
    assert excinfo.value.detail["values_found"] == 2

    asyncio.run(http_client.aclose())
