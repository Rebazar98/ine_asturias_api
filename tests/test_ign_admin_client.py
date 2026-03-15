from __future__ import annotations

import io
import zipfile

import httpx
import pytest

from app.services.ign_admin_client import (
    IGNAdministrativeInvalidPayloadError,
    IGNAdministrativeSnapshotClient,
    load_ign_admin_feature_collection_from_bytes,
)
from app.settings import Settings


def build_client(handler) -> IGNAdministrativeSnapshotClient:
    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(transport=transport)
    settings = Settings(ign_admin_snapshot_url="https://mocked.ign/admin-boundaries.geojson")
    return IGNAdministrativeSnapshotClient(http_client=http_client, settings=settings)


@pytest.mark.anyio
async def test_fetch_snapshot_returns_feature_collection_from_json() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "https://mocked.ign/admin-boundaries.geojson"
        return httpx.Response(
            200,
            json={"type": "FeatureCollection", "features": []},
            request=request,
        )

    client = build_client(handler)
    payload = await client.fetch_snapshot()

    assert payload == {"type": "FeatureCollection", "features": []}


def test_load_feature_collection_from_zip_extracts_first_geojson_member() -> None:
    archive_buffer = io.BytesIO()
    with zipfile.ZipFile(archive_buffer, "w") as archive:
        archive.writestr(
            "boundaries.geojson",
            '{"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]}}]}',
        )

    payload = load_ign_admin_feature_collection_from_bytes(
        archive_buffer.getvalue(),
        source_name="snapshot.zip",
    )

    assert payload["type"] == "FeatureCollection"
    assert len(payload["features"]) == 1


def test_load_feature_collection_from_bytes_rejects_non_feature_collection() -> None:
    with pytest.raises(IGNAdministrativeInvalidPayloadError) as exc_info:
        load_ign_admin_feature_collection_from_bytes(
            b'{"type":"Point","coordinates":[0,0]}',
            source_name="snapshot.geojson",
        )

    assert exc_info.value.status_code == 502
    assert "FeatureCollection" in exc_info.value.detail["message"]
