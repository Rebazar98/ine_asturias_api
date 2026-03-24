from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.models import INEOperationGovernanceHistory


class INEOperationGovernanceHistoryRepository:
    def __init__(self, session: AsyncSession | None) -> None:
        self.session = session
        self.logger = get_logger("app.repositories.ine_operation_governance_history")

    async def append_event(
        self,
        *,
        operation_code: str,
        event_type: str,
        effective_execution_profile_before: str | None,
        effective_execution_profile_after: str | None,
        schedule_enabled_before: bool | None,
        schedule_enabled_after: bool | None,
        background_required_before: bool | None,
        background_required_after: bool | None,
        override_active_before: bool | None,
        override_active_after: bool | None,
        decision_reason: str | None,
        decision_source: str | None,
        override_decision_reason: str | None,
        override_decision_source: str | None,
        metadata: dict[str, Any] | None = None,
        occurred_at: datetime | None = None,
        commit: bool = True,
    ) -> dict[str, Any]:
        session = self._require_session()
        row = INEOperationGovernanceHistory(
            operation_code=operation_code,
            event_type=event_type,
            effective_execution_profile_before=effective_execution_profile_before,
            effective_execution_profile_after=effective_execution_profile_after,
            schedule_enabled_before=schedule_enabled_before,
            schedule_enabled_after=schedule_enabled_after,
            background_required_before=background_required_before,
            background_required_after=background_required_after,
            override_active_before=override_active_before,
            override_active_after=override_active_after,
            decision_reason=decision_reason,
            decision_source=decision_source,
            override_decision_reason=override_decision_reason,
            override_decision_source=override_decision_source,
            metadata_json=metadata or {},
            occurred_at=occurred_at or func_now_utc(),
        )
        try:
            session.add(row)
            if commit:
                await session.commit()
                await session.refresh(row)
        except SQLAlchemyError:
            if commit:
                await session.rollback()
            self.logger.exception(
                "ine_operation_governance_history_append_failed",
                extra={"operation_code": operation_code, "event_type": event_type},
            )
            raise
        return self._serialize(row)

    async def list_by_operation_code(
        self,
        operation_code: str,
        *,
        page: int,
        page_size: int,
    ) -> list[dict[str, Any]]:
        if self.session is None:
            return []
        offset = (page - 1) * page_size
        stmt = (
            select(INEOperationGovernanceHistory)
            .where(INEOperationGovernanceHistory.operation_code == operation_code)
            .order_by(
                INEOperationGovernanceHistory.occurred_at.desc(),
                INEOperationGovernanceHistory.id.desc(),
            )
            .offset(offset)
            .limit(page_size)
        )
        result = await self.session.execute(stmt)
        return [self._serialize(row) for row in result.scalars().all()]

    async def count_by_operation_code(self, operation_code: str) -> int:
        if self.session is None:
            return 0
        stmt = (
            select(func.count())
            .select_from(INEOperationGovernanceHistory)
            .where(INEOperationGovernanceHistory.operation_code == operation_code)
        )
        result = await self.session.execute(stmt)
        return int(result.scalar_one())

    async def summarize_by_operation_code(self, operation_code: str) -> dict[str, int]:
        if self.session is None:
            return {
                "events_total": 0,
                "override_set_total": 0,
                "override_updated_total": 0,
                "override_cleared_total": 0,
            }
        stmt = (
            select(
                INEOperationGovernanceHistory.event_type,
                func.count(),
            )
            .where(INEOperationGovernanceHistory.operation_code == operation_code)
            .group_by(INEOperationGovernanceHistory.event_type)
        )
        result = await self.session.execute(stmt)
        grouped = {str(event_type): int(count) for event_type, count in result.all()}
        return {
            "events_total": sum(grouped.values()),
            "override_set_total": grouped.get("override_set", 0),
            "override_updated_total": grouped.get("override_updated", 0),
            "override_cleared_total": grouped.get("override_cleared", 0),
        }

    def _require_session(self) -> AsyncSession:
        if self.session is None:
            raise RuntimeError("No database session available")
        return self.session

    @staticmethod
    def _serialize(row: INEOperationGovernanceHistory) -> dict[str, Any]:
        return {
            "event_id": row.id,
            "operation_code": row.operation_code,
            "event_type": row.event_type,
            "effective_execution_profile_before": row.effective_execution_profile_before,
            "effective_execution_profile_after": row.effective_execution_profile_after,
            "schedule_enabled_before": row.schedule_enabled_before,
            "schedule_enabled_after": row.schedule_enabled_after,
            "background_required_before": row.background_required_before,
            "background_required_after": row.background_required_after,
            "override_active_before": row.override_active_before,
            "override_active_after": row.override_active_after,
            "decision_reason": row.decision_reason,
            "decision_source": row.decision_source,
            "override_decision_reason": row.override_decision_reason,
            "override_decision_source": row.override_decision_source,
            "metadata": row.metadata_json or {},
            "occurred_at": row.occurred_at,
        }


def func_now_utc() -> datetime:
    return datetime.now(UTC)
