from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.models import SyncSchedule


class SyncScheduleRepository:
    def __init__(self, session: AsyncSession | None) -> None:
        self.session = session
        self.logger = get_logger("app.repositories.sync_schedule")

    async def list_active(self, *, org_id: str | None = None) -> list[dict[str, Any]]:
        if self.session is None:
            return []
        stmt = select(SyncSchedule).where(SyncSchedule.is_active == True)  # noqa: E712
        if org_id is not None:
            stmt = stmt.where(SyncSchedule.org_id == org_id)
        result = await self.session.execute(stmt)
        return [self._serialize(row) for row in result.scalars().all()]

    async def upsert(self, *, org_id: str, source: str, cron_expression: str) -> dict[str, Any]:
        if self.session is None:
            raise RuntimeError("No database session available")

        stmt = (
            insert(SyncSchedule)
            .values(org_id=org_id, source=source, cron_expression=cron_expression)
            .on_conflict_do_update(
                constraint="uq_sync_schedule_org_source",
                set_={"cron_expression": cron_expression, "updated_at": SyncSchedule.updated_at},
            )
            .returning(SyncSchedule)
        )
        try:
            result = await self.session.execute(stmt)
            await self.session.commit()
            row = result.scalars().one()
        except SQLAlchemyError:
            await self.session.rollback()
            self.logger.exception(
                "sync_schedule_upsert_failed",
                extra={"org_id": org_id, "source": source},
            )
            raise

        return self._serialize(row)

    async def get_by_org_source(self, *, org_id: str, source: str) -> dict[str, Any] | None:
        if self.session is None:
            return None
        stmt = select(SyncSchedule).where(
            SyncSchedule.org_id == org_id,
            SyncSchedule.source == source,
        )
        result = await self.session.execute(stmt)
        row = result.scalars().first()
        return self._serialize(row) if row else None

    @staticmethod
    def _serialize(row: SyncSchedule) -> dict[str, Any]:
        return {
            "id": row.id,
            "org_id": row.org_id,
            "source": row.source,
            "cron_expression": row.cron_expression,
            "is_active": row.is_active,
            "updated_at": row.updated_at,
        }
