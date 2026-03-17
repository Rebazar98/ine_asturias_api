"""Tests for IDEASWFSClientService."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.services.ideas_wfs_client import (
    IDEASWFSClientError,
    IDEASWFSClientService,
    _fetch_layer_sync,
)
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
        "IDEAS_WFS_BASE_URL": "https://ideas.asturias.es/wfs",
    }
    defaults.update(overrides)
    return Settings(**defaults)


_SAMPLE_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]},
            "properties": {"nombre": "Parroquia 1", "codigo": "330010001"},
        }
    ],
}


def _mock_wfs(geojson: dict) -> MagicMock:
    mock_response = MagicMock()
    mock_response.read.return_value = json.dumps(geojson).encode("utf-8")

    mock_wfs = MagicMock()
    mock_wfs.getfeature.return_value = mock_response
    mock_wfs.contents = {"limites_parroquiales": MagicMock()}
    return mock_wfs


# ---------------------------------------------------------------------------
# _fetch_layer_sync unit tests (synchronous, no event loop needed)
# ---------------------------------------------------------------------------


def test_fetch_layer_sync_returns_feature_collection():
    mock_wfs = _mock_wfs(_SAMPLE_GEOJSON)

    with patch("owslib.wfs.WebFeatureService", return_value=mock_wfs):
        result = _fetch_layer_sync(
            "https://ideas.asturias.es/wfs", "limites_parroquiales", None, 500
        )

    assert result["type"] == "FeatureCollection"
    assert len(result["features"]) == 1


def test_fetch_layer_sync_normalises_non_feature_collection():
    incomplete = {"features": [{"type": "Feature", "geometry": None, "properties": {}}]}
    mock_wfs = _mock_wfs(incomplete)

    with patch("owslib.wfs.WebFeatureService", return_value=mock_wfs):
        result = _fetch_layer_sync("https://ideas.asturias.es/wfs", "layer", None, 100)

    assert result["type"] == "FeatureCollection"


def test_fetch_layer_sync_raises_on_invalid_json():
    mock_response = MagicMock()
    mock_response.read.return_value = b"not json"
    mock_wfs = MagicMock()
    mock_wfs.getfeature.return_value = mock_response

    with patch("owslib.wfs.WebFeatureService", return_value=mock_wfs):
        with pytest.raises(IDEASWFSClientError, match="not valid JSON"):
            _fetch_layer_sync("https://ideas.asturias.es/wfs", "layer", None, 100)


def test_fetch_layer_sync_raises_on_wfs_error():
    mock_wfs = MagicMock()
    mock_wfs.getfeature.side_effect = RuntimeError("WFS unavailable")

    with patch("owslib.wfs.WebFeatureService", return_value=mock_wfs):
        with pytest.raises(IDEASWFSClientError, match="WFS getfeature failed"):
            _fetch_layer_sync("https://ideas.asturias.es/wfs", "layer", None, 100)


# ---------------------------------------------------------------------------
# IDEASWFSClientService async tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_ideas_fetch_layer_returns_feature_collection():
    settings = _make_settings()
    service = IDEASWFSClientService(settings=settings)

    mock_wfs = _mock_wfs(_SAMPLE_GEOJSON)

    with patch("owslib.wfs.WebFeatureService", return_value=mock_wfs):
        result = await service.fetch_layer("limites_parroquiales")

    assert result["type"] == "FeatureCollection"
    assert len(result["features"]) == 1


@pytest.mark.anyio
async def test_ideas_list_layers():
    settings = _make_settings()
    service = IDEASWFSClientService(settings=settings)

    mock_wfs = MagicMock()
    mock_wfs.contents = {"layer_a": MagicMock(), "layer_b": MagicMock()}

    with patch("owslib.wfs.WebFeatureService", return_value=mock_wfs):
        layers = await service.list_layers()

    assert set(layers) == {"layer_a", "layer_b"}
