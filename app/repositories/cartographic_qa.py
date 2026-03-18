from __future__ import annotations

from datetime import datetime, UTC
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.models import CartographicQAIncident


class CartographicQARepository:
    def __init__(self, session: AsyncSession | None) -> None:
        self.session = session
        self.logger = get_logger("app.repositories.cartographic_qa")

    async def save_incidents(self, incidents: list[dict[str, Any]]) -> int:
        if not incidents:
            return 0
        if self.session is None:
            self.logger.debug("qa_save_incidents_skipped", extra={"reason": "database_disabled"})
            return 0

        rows = [
            {
                "layer": inc["layer"],
                "entity_id": str(inc["entity_id"]),
                "error_type": inc["error_type"],
                "severity": inc.get("severity", "warning"),
                "description": inc.get("description", ""),
                "source_provider": inc.get("source_provider", ""),
                "metadata": inc.get("metadata", {}),
            }
            for inc in incidents
        ]

        stmt = insert(CartographicQAIncident.__table__).values(rows)
        try:
            await self.session.execute(stmt)
            await self.session.commit()
        except SQLAlchemyError:
            await self.session.rollback()
            self.logger.exception("qa_save_incidents_failed", extra={"count": len(rows)})
            return 0

        self.logger.info("qa_incidents_saved", extra={"count": len(rows)})
        return len(rows)

    async def list_incidents(
        self,
        *,
        layer: str | None = None,
        severity: str | None = None,
        resolved: bool = False,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        if self.session is None:
            return self._empty_page(page, page_size, layer, severity, resolved)

        stmt = select(CartographicQAIncident)
        count_stmt = select(func.count()).select_from(CartographicQAIncident)

        filters = [CartographicQAIncident.resolved == resolved]
        if layer is not None:
            filters.append(CartographicQAIncident.layer == layer)
        if severity is not None:
            filters.append(CartographicQAIncident.severity == severity)

        for condition in filters:
            stmt = stmt.where(condition)
            count_stmt = count_stmt.where(condition)

        stmt = (
            stmt.order_by(CartographicQAIncident.detected_at.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )

        total = int(await self.session.scalar(count_stmt) or 0)
        result = await self.session.execute(stmt)
        items = [self._serialize(row) for row in result.scalars().all()]
        pages = (total + page_size - 1) // page_size if total else 0

        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "has_next": page < pages,
            "has_previous": page > 1,
            "filters": {"layer": layer, "severity": severity, "resolved": resolved},
        }

    async def mark_resolved(self, incident_id: int) -> bool:
        if self.session is None:
            return False
        result = await self.session.get(CartographicQAIncident, incident_id)
        if result is None:
            return False
        result.resolved = True
        result.resolved_at = datetime.now(tz=UTC)
        try:
            await self.session.commit()
        except SQLAlchemyError:
            await self.session.rollback()
            self.logger.exception("qa_mark_resolved_failed", extra={"id": incident_id})
            return False
        return True

    @staticmethod
    def _serialize(row: CartographicQAIncident) -> dict[str, Any]:
        return {
            "id": row.id,
            "layer": row.layer,
            "entity_id": row.entity_id,
            "error_type": row.error_type,
            "severity": row.severity,
            "description": row.description,
            "source_provider": row.source_provider,
            "detected_at": row.detected_at,
            "resolved": row.resolved,
            "resolved_at": row.resolved_at,
            "metadata": row.metadata_json,
        }

    @staticmethod
    def _empty_page(
        page: int,
        page_size: int,
        layer: str | None,
        severity: str | None,
        resolved: bool,
    ) -> dict[str, Any]:
        return {
            "items": [],
            "total": 0,
            "page": page,
            "page_size": page_size,
            "pages": 0,
            "has_next": False,
            "has_previous": page > 1,
            "filters": {"layer": layer, "severity": severity, "resolved": resolved},
        }
