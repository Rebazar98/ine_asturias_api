"""Tests for SADEIClientService and sadei_normalizers."""

from __future__ import annotations

import io
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import openpyxl
import pytest

from app.services.sadei_client import SADEIClientError, SADEIClientService, _parse_xlsx
from app.services.sadei_normalizers import normalize_sadei_dataset, normalize_sadei_row
from app.settings import Settings


def _make_settings(**overrides: Any) -> Settings:
    defaults = {
        "POSTGRES_DSN": None,
        "API_KEY": None,
        "REDIS_URL": None,
        "WORKER_METRICS_URL": None,
        "IGN_ADMIN_SNAPSHOT_URL": None,
        "CATASTRO_URBANO_YEAR": None,
        "APP_ENV": "local",
        "SADEI_BASE_URL": "https://sadei.es",
    }
    defaults.update(overrides)
    return Settings(**defaults)


def _build_xlsx_bytes(headers: list[str], rows: list[list[Any]]) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(headers)
    for row in rows:
        ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# _parse_xlsx unit tests
# ---------------------------------------------------------------------------


def test_parse_xlsx_returns_rows():
    content = _build_xlsx_bytes(
        ["codigo_municipio", "anyo", "valor"],
        [["33001", "2022", 10500], ["33002", "2022", 8200]],
    )
    rows = _parse_xlsx(content, "padron_municipal")
    assert len(rows) == 2
    assert rows[0]["codigo_municipio"] == "33001"
    assert rows[0]["_dataset_id"] == "padron_municipal"


def test_parse_xlsx_skips_blank_rows():
    content = _build_xlsx_bytes(
        ["codigo_municipio", "anyo", "valor"],
        [["33001", "2022", 10500], [None, None, None], ["33002", "2022", 8200]],
    )
    rows = _parse_xlsx(content, "padron_municipal")
    assert len(rows) == 2


# ---------------------------------------------------------------------------
# normalize_sadei_row unit tests
# ---------------------------------------------------------------------------


def test_normalize_sadei_row_basic():
    row = {"codigo_municipio": "33001", "anyo": "2022", "valor": 10500}
    item = normalize_sadei_row(row, "padron_municipal")
    assert item is not None
    assert item.geography_code == "33001"
    assert item.period == "2022"
    assert item.value == 10500.0
    assert item.source_provider == "sadei"
    assert item.operation_code == "sadei_padron_municipal"


def test_normalize_sadei_row_pads_geography_code():
    row = {"codigo_municipio": "33", "anyo": "2023", "valor": 1}
    item = normalize_sadei_row(row, "padron_municipal")
    assert item is not None
    assert item.geography_code == "00033"


def test_normalize_sadei_row_returns_none_without_period():
    row = {"codigo_municipio": "33001", "valor": 100}
    assert normalize_sadei_row(row, "padron_municipal") is None


def test_normalize_sadei_row_returns_none_without_geography():
    row = {"anyo": "2022", "valor": 100}
    assert normalize_sadei_row(row, "padron_municipal") is None


def test_normalize_sadei_dataset_filters_invalid_rows():
    rows = [
        {"codigo_municipio": "33001", "anyo": "2022", "valor": 10500},
        {"valor": 200},  # no period, no geography → discarded
        {"codigo_municipio": "33002", "anyo": "2021", "valor": 9000},
    ]
    items = normalize_sadei_dataset(rows, "padron_municipal")
    assert len(items) == 2
    assert all(i.source_provider == "sadei" for i in items)


def test_normalize_sadei_row_pib_unit():
    row = {"codigo_municipio": "33001", "anyo": "2022", "pib": 50000.0}
    item = normalize_sadei_row(row, "pib_municipal")
    assert item is not None
    assert item.unit == "miles_euros"


# ---------------------------------------------------------------------------
# SADEIClientService integration (mocked HTTP)
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_sadei_fetch_dataset_returns_rows():
    content = _build_xlsx_bytes(
        ["codigo_municipio", "anyo", "valor"],
        [["33001", "2022", 10500], ["33002", "2022", 8200]],
    )
    settings = _make_settings()

    mock_response = MagicMock()
    mock_response.content = content
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock(spec=httpx.AsyncClient)
    mock_client.get = AsyncMock(return_value=mock_response)

    service = SADEIClientService(http_client=mock_client, settings=settings)
    rows = await service.fetch_dataset("padron_municipal")

    assert len(rows) == 2
    mock_client.get.assert_called_once()
    call_url = mock_client.get.call_args[0][0]
    assert "sadei.es" in call_url


@pytest.mark.anyio
async def test_sadei_fetch_dataset_raises_on_unknown_id():
    settings = _make_settings()
    mock_client = AsyncMock(spec=httpx.AsyncClient)
    service = SADEIClientService(http_client=mock_client, settings=settings)

    with pytest.raises(SADEIClientError, match="Unknown SADEI dataset"):
        await service.fetch_dataset("nonexistent_dataset")


@pytest.mark.anyio
async def test_sadei_fetch_dataset_raises_on_http_error():
    settings = _make_settings()

    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.raise_for_status = MagicMock(
        side_effect=httpx.HTTPStatusError("404", request=MagicMock(), response=mock_response)
    )
    mock_client = AsyncMock(spec=httpx.AsyncClient)
    mock_client.get = AsyncMock(return_value=mock_response)

    service = SADEIClientService(http_client=mock_client, settings=settings)

    with pytest.raises(SADEIClientError, match="HTTP 404"):
        await service.fetch_dataset("padron_municipal")


@pytest.mark.anyio
async def test_sadei_list_available_datasets():
    settings = _make_settings()
    mock_client = AsyncMock(spec=httpx.AsyncClient)
    service = SADEIClientService(http_client=mock_client, settings=settings)
    datasets = await service.list_available_datasets()
    ids = {d["dataset_id"] for d in datasets}
    assert "padron_municipal" in ids
    assert "pib_municipal" in ids
