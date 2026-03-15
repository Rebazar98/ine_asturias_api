from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.core.metrics import record_persistence_batch
from app.models import TerritorialExportArtifact


SUPPORTED_EXPORT_PROVIDERS = ("territorial", "ine", "analytics", "catastro")
DEFAULT_EXPORT_PROVIDERS = ("territorial", "ine", "analytics")


def normalize_export_scope_key(unit_level: str, code_value: str) -> str:
    normalized_level = re.sub(r"\s+", "_", (unit_level or "").strip()).casefold()
    normalized_code = re.sub(r"\s+", "", (code_value or "").strip()).casefold()
    return f"{normalized_level}:{normalized_code}"


def normalize_export_provider_keys(
    include_providers: list[str] | tuple[str, ...] | None,
) -> list[str]:
    if include_providers is None:
        requested = set(DEFAULT_EXPORT_PROVIDERS)
    else:
        requested = {str(value or "").strip().casefold() for value in include_providers if value}
    if not requested:
        return []
    return [provider for provider in SUPPORTED_EXPORT_PROVIDERS if provider in requested]


def build_export_key(
    *,
    unit_level: str,
    code_value: str,
    artifact_format: str,
    include_providers: list[str] | tuple[str, ...] | None = None,
) -> str:
    canonical_payload = {
        "scope_key": normalize_export_scope_key(unit_level, code_value),
        "artifact_format": str(artifact_format or "").strip().casefold(),
        "providers": normalize_export_provider_keys(include_providers),
    }
    encoded = json.dumps(
        canonical_payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class TerritorialExportArtifactRepository:
    def __init__(self, session: AsyncSession | None) -> None:
        self.session = session
        self.logger = get_logger("app.repositories.territorial_export_artifacts")

    async def get_fresh_artifact(
        self,
        *,
        unit_level: str,
        code_value: str,
        artifact_format: str,
        include_providers: list[str] | tuple[str, ...] | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        if self.session is None:
            return None

        export_key = build_export_key(
            unit_level=unit_level,
            code_value=code_value,
            artifact_format=artifact_format,
            include_providers=include_providers,
        )
        lookup_time = now or datetime.now(timezone.utc)
        statement = (
            select(TerritorialExportArtifact)
            .where(
                TerritorialExportArtifact.export_key == export_key,
                TerritorialExportArtifact.expires_at > lookup_time,
            )
            .limit(1)
        )
        result = await self.session.execute(statement)
        row = result.scalars().first()
        if row is None:
            return None

        self.logger.info(
            "territorial_export_artifact_hit_db",
            extra={
                "export_key": export_key,
                "unit_level": unit_level,
                "code_value": code_value,
                "artifact_format": artifact_format,
            },
        )
        return self._serialize_row(row)

    async def get_by_export_id(self, export_id: int) -> dict[str, Any] | None:
        if self.session is None:
            return None

        statement = (
            select(TerritorialExportArtifact)
            .where(TerritorialExportArtifact.export_id == export_id)
            .limit(1)
        )
        result = await self.session.execute(statement)
        row = result.scalars().first()
        if row is None:
            return None
        return self._serialize_row(row)

    async def upsert_artifact(
        self,
        *,
        territorial_unit_id: int | None,
        unit_level: str,
        code_value: str,
        artifact_format: str,
        content_type: str,
        filename: str,
        payload_bytes: bytes,
        ttl_seconds: int,
        include_providers: list[str] | tuple[str, ...] | None = None,
        metadata: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        if self.session is None or ttl_seconds <= 0:
            return None

        export_key = build_export_key(
            unit_level=unit_level,
            code_value=code_value,
            artifact_format=artifact_format,
            include_providers=include_providers,
        )
        write_time = now or datetime.now(timezone.utc)
        expires_at = write_time + timedelta(seconds=ttl_seconds)
        payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()
        byte_size = len(payload_bytes)
        statement = insert(TerritorialExportArtifact.__table__).values(
            {
                "export_key": export_key,
                "territorial_unit_id": territorial_unit_id,
                "unit_level": unit_level,
                "code_value": code_value,
                "artifact_format": artifact_format,
                "content_type": content_type,
                "filename": filename,
                "payload_bytes": payload_bytes,
                "payload_sha256": payload_sha256,
                "byte_size": byte_size,
                "metadata": _canonicalize_json(metadata or {}),
                "expires_at": expires_at,
            }
        )
        statement = statement.on_conflict_do_update(
            index_elements=["export_key"],
            set_={
                "territorial_unit_id": statement.excluded.territorial_unit_id,
                "unit_level": statement.excluded.unit_level,
                "code_value": statement.excluded.code_value,
                "artifact_format": statement.excluded.artifact_format,
                "content_type": statement.excluded.content_type,
                "filename": statement.excluded.filename,
                "payload_bytes": statement.excluded.payload_bytes,
                "payload_sha256": statement.excluded.payload_sha256,
                "byte_size": statement.excluded.byte_size,
                "metadata": statement.excluded["metadata"],
                "expires_at": statement.excluded.expires_at,
                "updated_at": write_time,
            },
        )

        try:
            await self.session.execute(statement)
            await self.session.commit()
        except SQLAlchemyError:
            await self.session.rollback()
            self.logger.exception(
                "territorial_export_artifact_upsert_failed",
                extra={
                    "export_key": export_key,
                    "unit_level": unit_level,
                    "code_value": code_value,
                },
            )
            return None

        record_persistence_batch("territorial_export_artifacts", batch_size=1, rows_inserted=1)
        self.logger.info(
            "territorial_export_artifact_upserted",
            extra={
                "export_key": export_key,
                "unit_level": unit_level,
                "code_value": code_value,
                "artifact_format": artifact_format,
                "byte_size": byte_size,
                "ttl_seconds": ttl_seconds,
            },
        )
        return await self.get_fresh_artifact(
            unit_level=unit_level,
            code_value=code_value,
            artifact_format=artifact_format,
            include_providers=include_providers,
            now=write_time,
        )

    @staticmethod
    def _serialize_row(row: TerritorialExportArtifact) -> dict[str, Any]:
        return {
            "export_id": row.export_id,
            "export_key": row.export_key,
            "territorial_unit_id": row.territorial_unit_id,
            "unit_level": row.unit_level,
            "code_value": row.code_value,
            "artifact_format": row.artifact_format,
            "content_type": row.content_type,
            "filename": row.filename,
            "payload_bytes": row.payload_bytes,
            "payload_sha256": row.payload_sha256,
            "byte_size": row.byte_size,
            "metadata": row.metadata_json,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
            "expires_at": row.expires_at,
        }


def _canonicalize_json(value: Any) -> Any:
    if value is None:
        return None
    return json.loads(json.dumps(value, default=str, sort_keys=True))
