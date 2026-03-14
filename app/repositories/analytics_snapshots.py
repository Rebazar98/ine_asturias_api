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
from app.models import AnalyticalSnapshot


def normalize_snapshot_scope_key(scope_key: str) -> str:
    collapsed = re.sub(r"\s+", " ", (scope_key or "").strip())
    return collapsed.casefold()


def build_snapshot_key(
    *,
    snapshot_type: str,
    scope_key: str,
    filters: dict[str, Any] | None = None,
) -> str:
    canonical_payload = {
        "snapshot_type": str(snapshot_type or "").strip(),
        "scope_key": normalize_snapshot_scope_key(scope_key),
        "filters": _canonicalize_json(filters or {}),
    }
    encoded = json.dumps(
        canonical_payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class AnalyticalSnapshotRepository:
    def __init__(self, session: AsyncSession | None) -> None:
        self.session = session
        self.logger = get_logger("app.repositories.analytics_snapshots")

    async def get_fresh_snapshot(
        self,
        *,
        snapshot_type: str,
        scope_key: str,
        filters: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        if self.session is None:
            return None

        normalized_scope_key = normalize_snapshot_scope_key(scope_key)
        snapshot_key = build_snapshot_key(
            snapshot_type=snapshot_type,
            scope_key=normalized_scope_key,
            filters=filters,
        )
        lookup_time = now or datetime.now(timezone.utc)
        statement = (
            select(AnalyticalSnapshot)
            .where(
                AnalyticalSnapshot.snapshot_key == snapshot_key,
                AnalyticalSnapshot.expires_at > lookup_time,
            )
            .limit(1)
        )
        result = await self.session.execute(statement)
        row = result.scalars().first()
        if row is None:
            return None

        self.logger.info(
            "analytical_snapshot_hit_db",
            extra={
                "snapshot_type": snapshot_type,
                "scope_key": normalized_scope_key,
                "snapshot_key": snapshot_key,
            },
        )
        return self._serialize_row(row)

    async def upsert_snapshot(
        self,
        *,
        snapshot_type: str,
        scope_key: str,
        source: str,
        payload: dict[str, Any] | list[Any],
        ttl_seconds: int,
        territorial_unit_id: int | None = None,
        filters: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        generated_at: datetime | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        if self.session is None or ttl_seconds <= 0:
            return None

        normalized_scope_key = normalize_snapshot_scope_key(scope_key)
        snapshot_filters = _canonicalize_json(filters or {})
        snapshot_key = build_snapshot_key(
            snapshot_type=snapshot_type,
            scope_key=normalized_scope_key,
            filters=snapshot_filters,
        )
        write_time = now or datetime.now(timezone.utc)
        snapshot_generated_at = generated_at or write_time
        expires_at = write_time + timedelta(seconds=ttl_seconds)
        statement = insert(AnalyticalSnapshot.__table__).values(
            {
                "snapshot_key": snapshot_key,
                "snapshot_type": snapshot_type,
                "scope_key": normalized_scope_key,
                "source": source,
                "territorial_unit_id": territorial_unit_id,
                "filters": snapshot_filters,
                "payload": _canonicalize_json(payload),
                "metadata": _canonicalize_json(metadata or {}),
                "generated_at": snapshot_generated_at,
                "expires_at": expires_at,
            }
        )
        statement = statement.on_conflict_do_update(
            index_elements=["snapshot_key"],
            set_={
                "snapshot_type": statement.excluded.snapshot_type,
                "scope_key": statement.excluded.scope_key,
                "source": statement.excluded.source,
                "territorial_unit_id": statement.excluded.territorial_unit_id,
                "filters": statement.excluded["filters"],
                "payload": statement.excluded.payload,
                "metadata": statement.excluded["metadata"],
                "generated_at": statement.excluded.generated_at,
                "expires_at": statement.excluded.expires_at,
            },
        )

        try:
            await self.session.execute(statement)
            await self.session.commit()
        except SQLAlchemyError:
            await self.session.rollback()
            self.logger.exception(
                "analytical_snapshot_upsert_failed",
                extra={
                    "snapshot_type": snapshot_type,
                    "scope_key": normalized_scope_key,
                    "snapshot_key": snapshot_key,
                },
            )
            return None

        record_persistence_batch("analytical_snapshots", batch_size=1, rows_inserted=1)
        self.logger.info(
            "analytical_snapshot_upserted",
            extra={
                "snapshot_type": snapshot_type,
                "scope_key": normalized_scope_key,
                "snapshot_key": snapshot_key,
                "ttl_seconds": ttl_seconds,
            },
        )
        return await self.get_fresh_snapshot(
            snapshot_type=snapshot_type,
            scope_key=normalized_scope_key,
            filters=snapshot_filters,
            now=write_time,
        )

    @staticmethod
    def _serialize_row(row: AnalyticalSnapshot) -> dict[str, Any]:
        return {
            "id": row.id,
            "snapshot_key": row.snapshot_key,
            "snapshot_type": row.snapshot_type,
            "scope_key": row.scope_key,
            "source": row.source,
            "territorial_unit_id": row.territorial_unit_id,
            "filters": row.filters_json,
            "payload": row.payload,
            "metadata": row.metadata_json,
            "generated_at": row.generated_at,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
            "expires_at": row.expires_at,
        }


def _canonicalize_json(value: Any) -> Any:
    if value is None:
        return None
    return json.loads(json.dumps(value, default=str, sort_keys=True))
