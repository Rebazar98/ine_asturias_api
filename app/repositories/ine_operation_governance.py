from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.models import INEOperationGovernance


class INEOperationGovernanceRepository:
    def __init__(self, session: AsyncSession | None) -> None:
        self.session = session
        self.logger = get_logger("app.repositories.ine_operation_governance")

    async def list_all(self) -> list[dict[str, Any]]:
        if self.session is None:
            return []
        stmt = select(INEOperationGovernance).order_by(
            INEOperationGovernance.execution_profile.asc(),
            INEOperationGovernance.operation_code.asc(),
        )
        result = await self.session.execute(stmt)
        return [self._serialize(row) for row in result.scalars().all()]

    async def get_by_operation_code(self, operation_code: str) -> dict[str, Any] | None:
        if self.session is None:
            return None
        stmt = select(INEOperationGovernance).where(
            INEOperationGovernance.operation_code == operation_code
        )
        result = await self.session.execute(stmt)
        row = result.scalars().first()
        return self._serialize(row) if row else None

    async def upsert_profile(
        self,
        *,
        operation_code: str,
        execution_profile: str,
        schedule_enabled: bool,
        decision_reason: str,
        decision_source: str = "runtime_settings",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await self._upsert(
            values={
                "operation_code": operation_code,
                "execution_profile": execution_profile,
                "schedule_enabled": schedule_enabled,
                "decision_reason": decision_reason,
                "decision_source": decision_source,
                "metadata": metadata or {},
            },
            update_fields={
                "execution_profile": execution_profile,
                "schedule_enabled": schedule_enabled,
                "decision_reason": decision_reason,
                "decision_source": decision_source,
                "metadata": metadata or {},
                "updated_at": func_now_utc(),
            },
        )

    async def mark_queued(
        self,
        *,
        operation_code: str,
        execution_profile: str,
        schedule_enabled: bool,
        decision_reason: str,
        decision_source: str,
        metadata: dict[str, Any] | None,
        job_id: str,
        trigger_mode: str,
        background_forced: bool = False,
        background_reason: str | None = None,
    ) -> dict[str, Any]:
        return await self._upsert(
            values={
                "operation_code": operation_code,
                "execution_profile": execution_profile,
                "schedule_enabled": schedule_enabled,
                "decision_reason": decision_reason,
                "decision_source": decision_source,
                "metadata": metadata or {},
                "last_job_id": job_id,
                "last_run_status": "queued",
                "last_trigger_mode": trigger_mode,
                "last_background_forced": background_forced,
                "last_background_reason": background_reason,
                "last_run_started_at": None,
                "last_run_finished_at": None,
                "last_error_message": None,
            },
            update_fields={
                "execution_profile": execution_profile,
                "schedule_enabled": schedule_enabled,
                "decision_reason": decision_reason,
                "decision_source": decision_source,
                "metadata": metadata or {},
                "last_job_id": job_id,
                "last_run_status": "queued",
                "last_trigger_mode": trigger_mode,
                "last_background_forced": background_forced,
                "last_background_reason": background_reason,
                "last_run_started_at": None,
                "last_run_finished_at": None,
                "last_error_message": None,
                "updated_at": func_now_utc(),
            },
        )

    async def mark_running(
        self,
        *,
        operation_code: str,
        execution_profile: str,
        schedule_enabled: bool,
        decision_reason: str,
        decision_source: str,
        metadata: dict[str, Any] | None,
        job_id: str,
        trigger_mode: str,
        background_forced: bool = False,
        background_reason: str | None = None,
        started_at: datetime | None = None,
    ) -> dict[str, Any]:
        effective_started_at = started_at or datetime.now(UTC)
        return await self._upsert(
            values={
                "operation_code": operation_code,
                "execution_profile": execution_profile,
                "schedule_enabled": schedule_enabled,
                "decision_reason": decision_reason,
                "decision_source": decision_source,
                "metadata": metadata or {},
                "last_job_id": job_id,
                "last_run_status": "running",
                "last_trigger_mode": trigger_mode,
                "last_background_forced": background_forced,
                "last_background_reason": background_reason,
                "last_run_started_at": effective_started_at,
                "last_run_finished_at": None,
                "last_error_message": None,
            },
            update_fields={
                "execution_profile": execution_profile,
                "schedule_enabled": schedule_enabled,
                "decision_reason": decision_reason,
                "decision_source": decision_source,
                "metadata": metadata or {},
                "last_job_id": job_id,
                "last_run_status": "running",
                "last_trigger_mode": trigger_mode,
                "last_background_forced": background_forced,
                "last_background_reason": background_reason,
                "last_run_started_at": effective_started_at,
                "last_run_finished_at": None,
                "last_error_message": None,
                "updated_at": func_now_utc(),
            },
        )

    async def mark_completed(
        self,
        *,
        operation_code: str,
        execution_profile: str,
        schedule_enabled: bool,
        decision_reason: str,
        decision_source: str,
        metadata: dict[str, Any] | None,
        job_id: str,
        trigger_mode: str,
        background_forced: bool,
        background_reason: str | None,
        finished_at: datetime | None,
        duration_ms: int,
        summary: dict[str, Any],
    ) -> dict[str, Any]:
        effective_finished_at = finished_at or datetime.now(UTC)
        warning_count = summary.get("warnings")
        if warning_count is None:
            warning_count = 0
        return await self._upsert(
            values={
                "operation_code": operation_code,
                "execution_profile": execution_profile,
                "schedule_enabled": schedule_enabled,
                "decision_reason": decision_reason,
                "decision_source": decision_source,
                "metadata": metadata or {},
                "last_job_id": job_id,
                "last_run_status": "completed",
                "last_trigger_mode": trigger_mode,
                "last_background_forced": background_forced,
                "last_background_reason": background_reason,
                "last_run_finished_at": effective_finished_at,
                "last_duration_ms": duration_ms,
                "last_tables_found": summary.get("tables_found"),
                "last_tables_selected": summary.get("tables_selected"),
                "last_tables_succeeded": summary.get("tables_succeeded"),
                "last_tables_failed": summary.get("tables_failed"),
                "last_tables_skipped_catalog": summary.get("tables_skipped_catalog"),
                "last_normalized_rows": summary.get("normalized_rows"),
                "last_warning_count": warning_count,
                "last_error_message": None,
            },
            update_fields={
                "execution_profile": execution_profile,
                "schedule_enabled": schedule_enabled,
                "decision_reason": decision_reason,
                "decision_source": decision_source,
                "metadata": metadata or {},
                "last_job_id": job_id,
                "last_run_status": "completed",
                "last_trigger_mode": trigger_mode,
                "last_background_forced": background_forced,
                "last_background_reason": background_reason,
                "last_run_finished_at": effective_finished_at,
                "last_duration_ms": duration_ms,
                "last_tables_found": summary.get("tables_found"),
                "last_tables_selected": summary.get("tables_selected"),
                "last_tables_succeeded": summary.get("tables_succeeded"),
                "last_tables_failed": summary.get("tables_failed"),
                "last_tables_skipped_catalog": summary.get("tables_skipped_catalog"),
                "last_normalized_rows": summary.get("normalized_rows"),
                "last_warning_count": warning_count,
                "last_error_message": None,
                "updated_at": func_now_utc(),
            },
        )

    async def mark_failed(
        self,
        *,
        operation_code: str,
        execution_profile: str,
        schedule_enabled: bool,
        decision_reason: str,
        decision_source: str,
        metadata: dict[str, Any] | None,
        job_id: str,
        trigger_mode: str,
        background_forced: bool,
        background_reason: str | None,
        finished_at: datetime | None,
        duration_ms: int,
        error_message: str,
    ) -> dict[str, Any]:
        effective_finished_at = finished_at or datetime.now(UTC)
        return await self._upsert(
            values={
                "operation_code": operation_code,
                "execution_profile": execution_profile,
                "schedule_enabled": schedule_enabled,
                "decision_reason": decision_reason,
                "decision_source": decision_source,
                "metadata": metadata or {},
                "last_job_id": job_id,
                "last_run_status": "failed",
                "last_trigger_mode": trigger_mode,
                "last_background_forced": background_forced,
                "last_background_reason": background_reason,
                "last_run_finished_at": effective_finished_at,
                "last_duration_ms": duration_ms,
                "last_error_message": error_message,
            },
            update_fields={
                "execution_profile": execution_profile,
                "schedule_enabled": schedule_enabled,
                "decision_reason": decision_reason,
                "decision_source": decision_source,
                "metadata": metadata or {},
                "last_job_id": job_id,
                "last_run_status": "failed",
                "last_trigger_mode": trigger_mode,
                "last_background_forced": background_forced,
                "last_background_reason": background_reason,
                "last_run_finished_at": effective_finished_at,
                "last_duration_ms": duration_ms,
                "last_error_message": error_message,
                "updated_at": func_now_utc(),
            },
        )

    async def _upsert(
        self,
        *,
        values: dict[str, Any],
        update_fields: dict[str, Any],
    ) -> dict[str, Any]:
        if self.session is None:
            raise RuntimeError("No database session available")

        stmt = (
            insert(INEOperationGovernance)
            .values(**values)
            .on_conflict_do_update(
                constraint="uq_ine_operation_governance_operation_code",
                set_=update_fields,
            )
            .returning(INEOperationGovernance)
        )
        try:
            result = await self.session.execute(stmt)
            await self.session.commit()
            row = result.scalars().one()
        except SQLAlchemyError:
            await self.session.rollback()
            self.logger.exception(
                "ine_operation_governance_upsert_failed",
                extra={"operation_code": values.get("operation_code")},
            )
            raise
        return self._serialize(row)

    @staticmethod
    def _serialize(row: INEOperationGovernance) -> dict[str, Any]:
        return {
            "id": row.id,
            "operation_code": row.operation_code,
            "execution_profile": row.execution_profile,
            "schedule_enabled": row.schedule_enabled,
            "decision_reason": row.decision_reason,
            "decision_source": row.decision_source,
            "metadata": row.metadata_json,
            "background_required": row.execution_profile == "background_only",
            "last_job_id": row.last_job_id,
            "last_run_status": row.last_run_status,
            "last_trigger_mode": row.last_trigger_mode,
            "last_background_forced": row.last_background_forced,
            "last_background_reason": row.last_background_reason,
            "last_run_started_at": row.last_run_started_at,
            "last_run_finished_at": row.last_run_finished_at,
            "last_duration_ms": row.last_duration_ms,
            "last_tables_found": row.last_tables_found,
            "last_tables_selected": row.last_tables_selected,
            "last_tables_succeeded": row.last_tables_succeeded,
            "last_tables_failed": row.last_tables_failed,
            "last_tables_skipped_catalog": row.last_tables_skipped_catalog,
            "last_normalized_rows": row.last_normalized_rows,
            "last_warning_count": row.last_warning_count,
            "last_error_message": row.last_error_message,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }


def func_now_utc() -> datetime:
    return datetime.now(UTC)
