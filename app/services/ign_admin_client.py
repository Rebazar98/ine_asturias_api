from __future__ import annotations

import json
import time
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Any

import httpx

from app.core.logging import get_logger
from app.core.metrics import record_provider_request
from app.settings import Settings


IGN_ADMIN_PROVIDER = "ign_admin"
IGN_ADMIN_ENDPOINT_FAMILY = "administrative_snapshot"


class IGNAdministrativeClientError(Exception):
    def __init__(self, status_code: int, detail: Any) -> None:
        super().__init__(str(detail))
        self.status_code = status_code
        self.detail = detail


class IGNAdministrativeUpstreamError(IGNAdministrativeClientError):
    pass


class IGNAdministrativeInvalidPayloadError(IGNAdministrativeClientError):
    pass


class IGNAdministrativeSnapshotClient:
    def __init__(
        self,
        http_client: httpx.AsyncClient,
        settings: Settings,
    ) -> None:
        self.http_client = http_client
        self.settings = settings
        self.logger = get_logger("app.services.ign_admin_client")

    async def fetch_snapshot(self, snapshot_url: str | None = None) -> dict[str, Any]:
        resolved_url = (snapshot_url or self.settings.ign_admin_snapshot_url or "").strip()
        if not resolved_url:
            raise IGNAdministrativeInvalidPayloadError(
                status_code=500,
                detail={"message": "IGN administrative snapshot URL is not configured."},
            )

        started_at = time.perf_counter()
        try:
            response = await self.http_client.get(resolved_url)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            duration_seconds = time.perf_counter() - started_at
            record_provider_request(
                IGN_ADMIN_PROVIDER,
                IGN_ADMIN_ENDPOINT_FAMILY,
                "http_error",
                duration_seconds,
            )
            self.logger.warning(
                "ign_admin_snapshot_http_error",
                extra={
                    "snapshot_url": resolved_url,
                    "status_code": exc.response.status_code,
                    "duration_ms": round(duration_seconds * 1000, 2),
                },
            )
            raise IGNAdministrativeUpstreamError(
                status_code=exc.response.status_code,
                detail={
                    "message": "The IGN administrative snapshot returned an error.",
                    "snapshot_url": resolved_url,
                    "retryable": exc.response.status_code >= 500,
                },
            ) from exc
        except httpx.RequestError as exc:
            duration_seconds = time.perf_counter() - started_at
            record_provider_request(
                IGN_ADMIN_PROVIDER,
                IGN_ADMIN_ENDPOINT_FAMILY,
                "request_error",
                duration_seconds,
            )
            self.logger.error(
                "ign_admin_snapshot_request_error",
                extra={
                    "snapshot_url": resolved_url,
                    "duration_ms": round(duration_seconds * 1000, 2),
                    "error": str(exc),
                },
            )
            raise IGNAdministrativeUpstreamError(
                status_code=502,
                detail={
                    "message": "Could not connect to the IGN administrative snapshot source.",
                    "snapshot_url": resolved_url,
                    "retryable": True,
                },
            ) from exc

        payload = load_ign_admin_feature_collection_from_bytes(
            response.content,
            source_name=resolved_url,
        )
        duration_seconds = time.perf_counter() - started_at
        record_provider_request(
            IGN_ADMIN_PROVIDER,
            IGN_ADMIN_ENDPOINT_FAMILY,
            "success",
            duration_seconds,
        )
        self.logger.info(
            "ign_admin_snapshot_fetched",
            extra={
                "snapshot_url": resolved_url,
                "features_total": len(payload.get("features") or []),
                "duration_ms": round(duration_seconds * 1000, 2),
            },
        )
        return payload


def load_ign_admin_feature_collection_from_path(path: str | Path) -> dict[str, Any]:
    snapshot_path = Path(path)
    return load_ign_admin_feature_collection_from_bytes(
        snapshot_path.read_bytes(),
        source_name=str(snapshot_path),
    )


def load_ign_admin_feature_collection_from_bytes(
    content: bytes,
    *,
    source_name: str,
) -> dict[str, Any]:
    payload_bytes = _extract_payload_bytes(content, source_name=source_name)
    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IGNAdministrativeInvalidPayloadError(
            status_code=502,
            detail={
                "message": "The IGN administrative snapshot could not be parsed as JSON.",
                "source_name": source_name,
            },
        ) from exc

    if not isinstance(payload, dict):
        raise IGNAdministrativeInvalidPayloadError(
            status_code=502,
            detail={
                "message": "The IGN administrative snapshot must decode to a JSON object.",
                "source_name": source_name,
            },
        )

    if payload.get("type") != "FeatureCollection":
        raise IGNAdministrativeInvalidPayloadError(
            status_code=502,
            detail={
                "message": "The IGN administrative snapshot must be a GeoJSON FeatureCollection.",
                "source_name": source_name,
            },
        )

    if not isinstance(payload.get("features"), list):
        raise IGNAdministrativeInvalidPayloadError(
            status_code=502,
            detail={
                "message": "The IGN administrative snapshot must contain a features array.",
                "source_name": source_name,
            },
        )

    return payload


def _extract_payload_bytes(content: bytes, *, source_name: str) -> bytes:
    if _looks_like_zip(content, source_name):
        try:
            with zipfile.ZipFile(BytesIO(content)) as archive:
                candidate_names = [
                    name
                    for name in archive.namelist()
                    if not name.endswith("/") and name.lower().endswith((".geojson", ".json"))
                ]
                if not candidate_names:
                    raise IGNAdministrativeInvalidPayloadError(
                        status_code=502,
                        detail={
                            "message": "The IGN administrative ZIP snapshot does not contain a JSON file.",
                            "source_name": source_name,
                        },
                    )
                with archive.open(candidate_names[0]) as handle:
                    return handle.read()
        except zipfile.BadZipFile as exc:
            raise IGNAdministrativeInvalidPayloadError(
                status_code=502,
                detail={
                    "message": "The IGN administrative snapshot could not be parsed as ZIP.",
                    "source_name": source_name,
                },
            ) from exc
    return content


def _looks_like_zip(content: bytes, source_name: str) -> bool:
    return source_name.lower().endswith(".zip") or content.startswith(b"PK\x03\x04")
