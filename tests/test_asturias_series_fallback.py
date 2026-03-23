"""Tests para el fallback de series directas (SERIES_OPERACION / DATOS_SERIE).

Cubre:
- ingest_asturias_operation_via_series: paginación, concurrencia, upsert, errores.
- ingest_asturias_operation: activación automática del fallback cuando TABLAS_OPERACION vacía.
- AsturiasResolver: name_based_fallback cuando VALORES_VARIABLEOPERACION devuelve [].
- INEClientService: get_operation_series, get_serie_data, get_variable_values con body vacío.
"""
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from app.core.cache import InMemoryTTLCache
from app.schemas import AsturiasResolutionResult
from app.services.asturias_resolver import AsturiasResolutionError, AsturiasResolver
from app.services.ine_client import INEClientService
from app.services.ine_operation_ingestion import INEOperationIngestionService
from app.settings import Settings
from tests.conftest import (
    DummyIngestionRepository,
    DummySeriesRepository,
    DummyTableCatalogRepository,
    override_ine_service,
)


# ---------------------------------------------------------------------------
# Payloads de ejemplo
# ---------------------------------------------------------------------------

_SERIE_META_PAGE1 = [
    {"Id": 101, "COD": "EPOB0101", "Nombre": "Asturias. Hombres. 0 anos."},
    {"Id": 102, "COD": "EPOB0102", "Nombre": "Asturias. Mujeres. 0 anos."},
]

_SERIE_META_PAGE2 = [
    {"Id": 103, "COD": "EPOB0103", "Nombre": "Asturias. Total. 0 anos."},
]

_SERIE_DATA_101 = {
    "COD": "EPOB0101",
    "Nombre": "Asturias. Hombres. 0 anos.",
    "FK_Unidad": 3,
    "Data": [
        {"Anyo": 2022, "FK_Periodo": 28, "Valor": 1200.0, "Secreto": False},
    ],
}

_SERIE_DATA_102 = {
    "COD": "EPOB0102",
    "Nombre": "Asturias. Mujeres. 0 anos.",
    "FK_Unidad": 3,
    "Data": [
        {"Anyo": 2022, "FK_Periodo": 28, "Valor": 1150.0, "Secreto": False},
    ],
}

_SERIE_DATA_103 = {
    "COD": "EPOB0103",
    "Nombre": "Asturias. Total. 0 anos.",
    "FK_Unidad": 3,
    "Data": [
        {"Anyo": 2022, "FK_Periodo": 28, "Valor": 2350.0, "Secreto": False},
    ],
}


# ---------------------------------------------------------------------------
# Helper: construye la instancia de servicio con repos dummy
# ---------------------------------------------------------------------------

def _build_service(ingestion_repo=None, series_repo=None, catalog_repo=None):
    return INEOperationIngestionService(
        ingestion_repo=ingestion_repo or DummyIngestionRepository(),
        series_repo=series_repo or DummySeriesRepository(),
        catalog_repo=catalog_repo or DummyTableCatalogRepository(),
    )


def _mock_client(series_pages: list[list[dict]], serie_data: dict[str, Any]) -> Any:
    """Construye un cliente INE mock con get_operation_series paginado y get_serie_data."""
    client = MagicMock()

    async def get_operation_series(op_code, page=1):
        idx = page - 1
        if idx < len(series_pages):
            return series_pages[idx]
        return []

    async def get_serie_data(cod_serie, nult=None):
        if cod_serie in serie_data:
            return serie_data[cod_serie]
        raise RuntimeError(f"Serie no encontrada: {cod_serie}")

    client.get_operation_series = get_operation_series
    client.get_serie_data = get_serie_data
    return client


# ---------------------------------------------------------------------------
# ingest_asturias_operation_via_series — tests unitarios
# ---------------------------------------------------------------------------


def test_via_series_happy_path_single_page():
    """Una sola página de series → todas descargadas y normalizadas."""
    ingestion_repo = DummyIngestionRepository()
    series_repo = DummySeriesRepository()
    service = _build_service(ingestion_repo=ingestion_repo, series_repo=series_repo)

    client = _mock_client(
        series_pages=[_SERIE_META_PAGE1, []],
        serie_data={"101": _SERIE_DATA_101, "102": _SERIE_DATA_102},
    )

    async def run():
        return await service.ingest_asturias_operation_via_series(
            op_code="21",
            nult=None,
            max_series=None,
            max_concurrent_series_fetches=3,
            progress_reporter=None,
            ine_client=client,
        )

    result = asyncio.run(run())

    assert result["ingestion_mode"] == "series_direct"
    assert result["series_index_total"] == 2
    assert result["series_failed"] == 0
    assert result["normalized_rows"] == 2
    assert len(series_repo.items) == 2
    assert all(item.geography_code == "33" for item in series_repo.items)


def test_via_series_pagination_multiple_pages():
    """Dos páginas antes de la página vacía → serie_index contiene las 3 series."""
    series_repo = DummySeriesRepository()
    service = _build_service(series_repo=series_repo)

    client = _mock_client(
        series_pages=[_SERIE_META_PAGE1, _SERIE_META_PAGE2, []],
        serie_data={
            "101": _SERIE_DATA_101,
            "102": _SERIE_DATA_102,
            "103": _SERIE_DATA_103,
        },
    )

    async def run():
        return await service.ingest_asturias_operation_via_series(
            op_code="21",
            nult=None,
            max_series=None,
            max_concurrent_series_fetches=5,
            progress_reporter=None,
            ine_client=client,
        )

    result = asyncio.run(run())

    assert result["series_index_total"] == 3
    assert result["normalized_rows"] == 3


def test_via_series_max_series_truncates_index():
    """max_series=1 limita el índice a 1 serie aunque haya 2 en la primera página."""
    series_repo = DummySeriesRepository()
    service = _build_service(series_repo=series_repo)

    client = _mock_client(
        series_pages=[_SERIE_META_PAGE1],
        serie_data={"101": _SERIE_DATA_101},
    )

    async def run():
        return await service.ingest_asturias_operation_via_series(
            op_code="21",
            nult=None,
            max_series=1,
            max_concurrent_series_fetches=3,
            progress_reporter=None,
            ine_client=client,
        )

    result = asyncio.run(run())

    assert result["series_index_total"] == 1
    assert result["summary"]["max_series_effective"] == 1


def test_via_series_empty_index_raises_resolution_error():
    """SERIES_OPERACION devuelve página vacía desde el inicio → AsturiasResolutionError(404)."""
    service = _build_service()
    client = _mock_client(series_pages=[[]], serie_data={})

    async def run():
        await service.ingest_asturias_operation_via_series(
            op_code="21",
            nult=None,
            max_series=None,
            max_concurrent_series_fetches=3,
            progress_reporter=None,
            ine_client=client,
        )

    with pytest.raises(AsturiasResolutionError) as exc_info:
        asyncio.run(run())

    assert exc_info.value.status_code == 404
    assert "No series found" in exc_info.value.detail["message"]


def test_via_series_one_fetch_fails_partial_success():
    """Un get_serie_data falla → se registra el error, el resto upsertea bien."""
    ingestion_repo = DummyIngestionRepository()
    series_repo = DummySeriesRepository()
    service = _build_service(ingestion_repo=ingestion_repo, series_repo=series_repo)

    async def failing_get_serie_data(cod_serie, nult=None):
        if cod_serie == "101":
            raise RuntimeError("upstream error")
        return _SERIE_DATA_102

    client = MagicMock()
    client.get_operation_series = _mock_client(
        [_SERIE_META_PAGE1, []], {}
    ).get_operation_series
    client.get_serie_data = failing_get_serie_data

    async def run():
        return await service.ingest_asturias_operation_via_series(
            op_code="21",
            nult=None,
            max_series=None,
            max_concurrent_series_fetches=3,
            progress_reporter=None,
            ine_client=client,
        )

    result = asyncio.run(run())

    assert result["series_failed"] == 1
    assert result["series_fetched"] == 1
    assert len(result["errors"]) == 1
    assert result["errors"][0]["cod_serie"] == "EPOB0101"
    assert len(series_repo.items) == 1


def test_via_series_zero_normalized_rows_raises():
    """Series encontradas pero Data vacía → normalized_rows=0 → AsturiasResolutionError(404)."""
    service = _build_service()

    series_with_empty_data = [{"Id": 101, "COD": "EPOB0101", "Nombre": "Asturias. Total."}]
    serie_no_data = {"COD": "EPOB0101", "Nombre": "Asturias. Total.", "Data": []}

    client = _mock_client(
        series_pages=[series_with_empty_data, []],
        serie_data={"101": serie_no_data},
    )

    async def run():
        await service.ingest_asturias_operation_via_series(
            op_code="21",
            nult=None,
            max_series=None,
            max_concurrent_series_fetches=3,
            progress_reporter=None,
            ine_client=client,
        )

    with pytest.raises(AsturiasResolutionError) as exc_info:
        asyncio.run(run())

    assert exc_info.value.status_code == 404
    assert "no data could be normalized" in exc_info.value.detail["message"]


def test_via_series_progress_reporter_called():
    """progress_reporter debe recibir el evento series_index_ready."""
    service = _build_service()
    client = _mock_client(
        series_pages=[_SERIE_META_PAGE1, []],
        serie_data={"101": _SERIE_DATA_101, "102": _SERIE_DATA_102},
    )
    events: list[dict] = []

    async def reporter(event):
        events.append(event)

    async def run():
        await service.ingest_asturias_operation_via_series(
            op_code="21",
            nult=None,
            max_series=None,
            max_concurrent_series_fetches=3,
            progress_reporter=reporter,
            ine_client=client,
        )

    asyncio.run(run())

    stages = [e["stage"] for e in events]
    assert "series_index_ready" in stages


def test_via_series_uses_numeric_id_as_serie_key():
    """Cuando la serie tiene Id numérico se usa como clave, no el COD."""
    series_repo = DummySeriesRepository()
    service = _build_service(series_repo=series_repo)

    fetched_keys: list[str] = []

    async def tracking_get_serie_data(cod_serie, nult=None):
        fetched_keys.append(cod_serie)
        return _SERIE_DATA_101

    client = MagicMock()
    client.get_operation_series = _mock_client(
        [[{"Id": 999, "COD": "EPOB0101", "Nombre": "Asturias."}], []], {}
    ).get_operation_series
    client.get_serie_data = tracking_get_serie_data

    asyncio.run(
        service.ingest_asturias_operation_via_series(
            op_code="21",
            nult=None,
            max_series=None,
            max_concurrent_series_fetches=3,
            progress_reporter=None,
            ine_client=client,
        )
    )

    assert "999" in fetched_keys


def test_via_series_nult_forwarded_to_get_serie_data():
    """El parámetro nult se pasa correctamente a get_serie_data."""
    series_repo = DummySeriesRepository()
    service = _build_service(series_repo=series_repo)

    received_nult: list[Any] = []

    async def capturing_get_serie_data(cod_serie, nult=None):
        received_nult.append(nult)
        return _SERIE_DATA_101

    client = MagicMock()
    client.get_operation_series = _mock_client(
        [[{"Id": 101, "COD": "EPOB0101", "Nombre": "Asturias."}], []], {}
    ).get_operation_series
    client.get_serie_data = capturing_get_serie_data

    asyncio.run(
        service.ingest_asturias_operation_via_series(
            op_code="21",
            nult=5,
            max_series=None,
            max_concurrent_series_fetches=3,
            progress_reporter=None,
            ine_client=client,
        )
    )

    assert received_nult == [5]


# ---------------------------------------------------------------------------
# ingest_asturias_operation — activación automática del fallback
# ---------------------------------------------------------------------------


def test_ingest_asturias_operation_activates_series_fallback_when_no_tables():
    """Cuando TABLAS_OPERACION devuelve [] se activa el fallback de series."""
    series_repo = DummySeriesRepository()
    service = _build_service(series_repo=series_repo)

    client = _mock_client(
        series_pages=[_SERIE_META_PAGE1, []],
        serie_data={"101": _SERIE_DATA_101, "102": _SERIE_DATA_102},
    )

    # get_operation_tables devuelve lista vacía
    async def empty_tables(op_code):
        return []

    client.get_operation_tables = empty_tables

    resolution = AsturiasResolutionResult(
        geo_variable_id="115",
        asturias_value_id="33",
    )

    async def run():
        return await service.ingest_asturias_operation(
            op_code="21",
            resolution=resolution,
            nult=None,
            det=None,
            tip=None,
            periodicidad=None,
            max_tables=None,
            skip_known_no_data=False,
            ine_client=client,
            max_concurrent_table_fetches=3,
            max_series=None,
            max_concurrent_series_fetches=3,
        )

    result = asyncio.run(run())

    assert result["ingestion_mode"] == "series_direct"
    assert result["normalized_rows"] == 2


# ---------------------------------------------------------------------------
# AsturiasResolver — name_based_fallback
# ---------------------------------------------------------------------------


def test_resolver_name_based_fallback_when_variable_values_empty():
    """VALORES_VARIABLEOPERACION devuelve [] → name_based_fallback=True."""
    cache = InMemoryTTLCache(enabled=False)
    mock_ine = MagicMock()

    async def get_operation_variables(op_code):
        return [{"Id": "115", "Nombre": "Comunidad autonoma"}]

    async def get_variable_values(op_code, variable_id):
        return []  # empty — sin candidatos de valor geográfico

    mock_ine.get_operation_variables = get_operation_variables
    mock_ine.get_variable_values = get_variable_values

    resolver = AsturiasResolver(ine_client=mock_ine, cache=cache)

    async def run():
        return await resolver.resolve(op_code="OP_EMPTY")

    result = asyncio.run(run())

    assert result.name_based_fallback is True
    assert result.asturias_value_id is None
    assert result.geo_variable_id == "115"
    assert result.variable_name is None


def test_resolver_name_based_fallback_false_when_values_found():
    """VALORES_VARIABLEOPERACION devuelve Asturias → name_based_fallback=False."""
    cache = InMemoryTTLCache(enabled=False)
    mock_ine = MagicMock()

    async def get_operation_variables(op_code):
        return [{"Id": "115", "Nombre": "Comunidad autonoma"}]

    async def get_variable_values(op_code, variable_id):
        return [{"Id": "33", "Nombre": "Principado de Asturias"}]

    mock_ine.get_operation_variables = get_operation_variables
    mock_ine.get_variable_values = get_variable_values

    resolver = AsturiasResolver(ine_client=mock_ine, cache=cache)

    result = asyncio.run(resolver.resolve(op_code="OP_NORMAL"))

    assert result.name_based_fallback is False
    assert result.asturias_value_id == "33"


# ---------------------------------------------------------------------------
# INEClientService — get_operation_series, get_serie_data, get_variable_values
# ---------------------------------------------------------------------------


def _build_ine_client(handler) -> INEClientService:
    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(transport=transport)
    settings = Settings(ine_base_url="https://mocked.ine", enable_cache=False)
    cache = InMemoryTTLCache(enabled=False)
    return INEClientService(http_client=http_client, settings=settings, cache=cache)


def test_ine_client_get_operation_series_returns_list():
    """get_operation_series devuelve la lista de la página solicitada."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/SERIES_OPERACION/21"
        assert request.url.params.get("page") == "1"
        assert request.url.params.get("det") == "2"
        return httpx.Response(200, json=_SERIE_META_PAGE1)

    client = _build_ine_client(handler)

    result = asyncio.run(client.get_operation_series("21", page=1))

    assert result == _SERIE_META_PAGE1


def test_ine_client_get_serie_data_without_nult():
    """get_serie_data sin nult no añade el parámetro a la URL."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/DATOS_SERIE/EPOB0101"
        assert "nult" not in request.url.params
        return httpx.Response(200, json=_SERIE_DATA_101)

    client = _build_ine_client(handler)

    result = asyncio.run(client.get_serie_data("EPOB0101"))

    assert result["COD"] == "EPOB0101"


def test_ine_client_get_serie_data_with_nult():
    """get_serie_data con nult añade el parámetro correcto."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/DATOS_SERIE/EPOB0101"
        assert request.url.params.get("nult") == "5"
        return httpx.Response(200, json=_SERIE_DATA_101)

    client = _build_ine_client(handler)

    result = asyncio.run(client.get_serie_data("EPOB0101", nult=5))

    assert result["COD"] == "EPOB0101"


def test_ine_client_get_variable_values_empty_body_returns_list():
    """HTTP 200 con body no-JSON → INEInvalidPayloadError atrapado → devuelve []."""

    def handler(request: httpx.Request) -> httpx.Response:
        # Body vacío — el INE a veces devuelve HTTP 200 sin JSON
        return httpx.Response(200, content=b"", headers={"content-type": "application/json"})

    client = _build_ine_client(handler)

    result = asyncio.run(client.get_variable_values("21", "115"))

    assert result == []


# ---------------------------------------------------------------------------
# Endpoint-level: series fallback activado vía HTTP
# ---------------------------------------------------------------------------


def test_asturias_endpoint_activates_series_fallback_when_no_tables(
    client, dummy_ingestion_repo, dummy_series_repo
):
    """GET /ine/operation/.../asturias con TABLAS_OPERACION vacía activa fallback de series."""

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        page = request.url.params.get("page", "1")

        if path == "/VARIABLES_OPERACION/OP_NOTAB":
            return httpx.Response(200, json=[{"Id": "115", "Nombre": "Comunidad autonoma"}])
        if path == "/VALORES_VARIABLEOPERACION/115/OP_NOTAB":
            return httpx.Response(200, json=[{"Id": "33", "Nombre": "Principado de Asturias"}])
        if path == "/TABLAS_OPERACION/OP_NOTAB":
            return httpx.Response(200, json=[])  # sin tablas → fallback
        if path == "/SERIES_OPERACION/OP_NOTAB":
            if page == "1":
                return httpx.Response(200, json=_SERIE_META_PAGE1)
            return httpx.Response(200, json=[])
        if path == "/DATOS_SERIE/101":
            return httpx.Response(200, json=_SERIE_DATA_101)
        if path == "/DATOS_SERIE/102":
            return httpx.Response(200, json=_SERIE_DATA_102)
        raise AssertionError(f"Unexpected path: {path}")

    override_ine_service(handler)

    response = client.get("/ine/operation/OP_NOTAB/asturias?background=false")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ingestion_mode"] == "series_direct"
    assert payload["series_index_total"] == 2
    assert payload["normalized_rows"] == 2
    assert len(dummy_series_repo.items) == 2
    assert all(item.geography_code == "33" for item in dummy_series_repo.items)


def test_asturias_endpoint_series_fallback_max_series_param(
    client, dummy_ingestion_repo, dummy_series_repo
):
    """max_series=1 limita el índice a 1 serie aunque haya más disponibles."""

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        page = request.url.params.get("page", "1")

        if path == "/VARIABLES_OPERACION/OP_NOTAB":
            return httpx.Response(200, json=[{"Id": "115", "Nombre": "Comunidad autonoma"}])
        if path == "/VALORES_VARIABLEOPERACION/115/OP_NOTAB":
            return httpx.Response(200, json=[{"Id": "33", "Nombre": "Principado de Asturias"}])
        if path == "/TABLAS_OPERACION/OP_NOTAB":
            return httpx.Response(200, json=[])
        if path == "/SERIES_OPERACION/OP_NOTAB":
            if page == "1":
                return httpx.Response(200, json=_SERIE_META_PAGE1)
            return httpx.Response(200, json=[])
        if path == "/DATOS_SERIE/101":
            return httpx.Response(200, json=_SERIE_DATA_101)
        raise AssertionError(f"Unexpected path: {path}")

    override_ine_service(handler)

    response = client.get("/ine/operation/OP_NOTAB/asturias?background=false&max_series=1")

    assert response.status_code == 200
    payload = response.json()
    assert payload["series_index_total"] == 1
    assert payload["summary"]["max_series_effective"] == 1
    assert len(dummy_series_repo.items) == 1


def test_asturias_endpoint_series_fallback_empty_series_returns_error(
    client, dummy_ingestion_repo, dummy_series_repo
):
    """TABLAS_OPERACION vacía + SERIES_OPERACION vacía → 404 con detail claro."""

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path

        if path == "/VARIABLES_OPERACION/OP_EMPTY":
            return httpx.Response(200, json=[{"Id": "115", "Nombre": "Comunidad autonoma"}])
        if path == "/VALORES_VARIABLEOPERACION/115/OP_EMPTY":
            return httpx.Response(200, json=[{"Id": "33", "Nombre": "Principado de Asturias"}])
        if path == "/TABLAS_OPERACION/OP_EMPTY":
            return httpx.Response(200, json=[])
        if path == "/SERIES_OPERACION/OP_EMPTY":
            return httpx.Response(200, json=[])
        raise AssertionError(f"Unexpected path: {path}")

    override_ine_service(handler)

    response = client.get("/ine/operation/OP_EMPTY/asturias?background=false")

    assert response.status_code == 404
    body = response.json()
    assert "No series found" in body["detail"]["message"]


def test_asturias_endpoint_name_based_fallback_via_tables(
    client, dummy_ingestion_repo, dummy_series_repo
):
    """VALORES_VARIABLEOPERACION vacío → name_based_fallback, tablas se filtran por nombre."""

    mixed_table = [
        {
            "Nombre": "Asturias. Poblacion total.",
            "MetaData": [{"Variable": "Territorio", "Nombre": "Asturias", "Id": "33"}],
            "Data": [{"Periodo": "2024", "Valor": "1012345", "Unidad": "personas"}],
        },
        {
            "Nombre": "Madrid. Poblacion total.",
            "MetaData": [{"Variable": "Territorio", "Nombre": "Madrid", "Id": "28"}],
            "Data": [{"Periodo": "2024", "Valor": "6500000", "Unidad": "personas"}],
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path

        if path == "/VARIABLES_OPERACION/OP_NBF":
            return httpx.Response(200, json=[{"Id": "115", "Nombre": "Comunidad autonoma"}])
        if path == "/VALORES_VARIABLEOPERACION/115/OP_NBF":
            return httpx.Response(200, json=[])  # empty → name-based fallback
        if path == "/TABLAS_OPERACION/OP_NBF":
            return httpx.Response(200, json=[{"IdTabla": "601", "Nombre": "Tabla mixta"}])
        if path == "/DATOS_TABLA/601":
            # sin g1 porque asturias_value_id es None en name-based mode
            assert "g1" not in request.url.params
            return httpx.Response(200, json=mixed_table)
        raise AssertionError(f"Unexpected path: {path}")

    override_ine_service(handler)

    response = client.get("/ine/operation/OP_NBF/asturias?background=false")

    assert response.status_code == 200
    payload = response.json()
    # Sólo la serie de Asturias pasa el filtro por nombre
    assert payload["summary"]["tables_succeeded"] == 1
    assert len(dummy_series_repo.items) == 1
    assert dummy_series_repo.items[0].geography_name == "Principado de Asturias"
    assert dummy_series_repo.items[0].geography_code == "33"
