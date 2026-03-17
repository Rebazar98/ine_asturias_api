from __future__ import annotations

import asyncio
from functools import partial
from typing import Any

from app.core.logging import get_logger
from app.settings import Settings


class IDEASWFSClientError(Exception):
    def __init__(self, layer_name: str, detail: str) -> None:
        super().__init__(detail)
        self.layer_name = layer_name
        self.detail = detail


class IDEASWFSClientService:
    """
    Adapter para IDEAS/SITPA via WFS 2.0.
    Devuelve GeoJSON FeatureCollection normalizado a SRID 4326.
    No escribe en base de datos.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.logger = get_logger("app.services.ideas_wfs_client")

    async def list_layers(self) -> list[str]:
        loop = asyncio.get_running_loop()
        layers = await loop.run_in_executor(
            None, partial(_list_layers_sync, self.settings.ideas_wfs_base_url)
        )
        self.logger.info("ideas_list_layers_completed", extra={"count": len(layers)})
        return layers

    async def fetch_layer(
        self,
        layer_name: str,
        bbox: tuple[float, float, float, float] | None = None,
        max_features: int = 5000,
    ) -> dict[str, Any]:
        self.logger.info(
            "ideas_fetch_layer_start",
            extra={"layer_name": layer_name, "bbox": bbox, "max_features": max_features},
        )
        loop = asyncio.get_running_loop()
        geojson = await loop.run_in_executor(
            None,
            partial(
                _fetch_layer_sync,
                self.settings.ideas_wfs_base_url,
                layer_name,
                bbox,
                max_features,
            ),
        )
        self.logger.info(
            "ideas_fetch_layer_completed",
            extra={
                "layer_name": layer_name,
                "features": len(geojson.get("features", [])),
            },
        )
        return geojson


def _list_layers_sync(wfs_url: str) -> list[str]:
    from owslib.wfs import WebFeatureService  # type: ignore[import-untyped]

    wfs = WebFeatureService(url=wfs_url, version="2.0.0")
    return list(wfs.contents.keys())


def _fetch_layer_sync(
    wfs_url: str,
    layer_name: str,
    bbox: tuple[float, float, float, float] | None,
    max_features: int,
) -> dict[str, Any]:
    from owslib.wfs import WebFeatureService  # type: ignore[import-untyped]

    wfs = WebFeatureService(url=wfs_url, version="2.0.0")

    kwargs: dict[str, Any] = {
        "typename": [layer_name],
        "outputFormat": "application/json",
        "maxfeatures": max_features,
        "srsname": "urn:ogc:def:crs:EPSG::4326",
    }
    if bbox is not None:
        kwargs["bbox"] = bbox

    try:
        response = wfs.getfeature(**kwargs)
        raw = response.read()
    except Exception as exc:
        raise IDEASWFSClientError(layer_name, f"WFS getfeature failed: {exc}") from exc

    import json

    try:
        geojson: dict[str, Any] = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise IDEASWFSClientError(layer_name, f"WFS response is not valid JSON: {exc}") from exc

    # Normalise to standard FeatureCollection shape
    if geojson.get("type") != "FeatureCollection":
        geojson = {"type": "FeatureCollection", "features": geojson.get("features", [])}

    return geojson
