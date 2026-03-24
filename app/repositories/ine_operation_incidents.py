from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.models import INEOperationIncident


INCIDENT_SEVERITY_ORDER = {
    "low": 0,
    "medium": 1,
    "high": 2,
}


class INEOperationIncidentRepository:
    def __init__(self, session: AsyncSession | None) -> None:
        self.session = session
        self.logger = get_logger("app.repositories.ine_operation_incidents")

    async def open_or_update_incident(
        self,
        *,
        operation_code: str,
        incident_type: str,
        severity: str,
        title: str,
        message: str,
        last_job_id: str | None,
        last_run_status: str | None,
        suggested_action: str | None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        session = self._require_session()
        now = func_now_utc()
        try:
            row = await self._get_open_incident(operation_code, incident_type)
            notification_event = "opened"
            previous_severity = None
            if row is None:
                row = INEOperationIncident(
                    operation_code=operation_code,
                    incident_type=incident_type,
                    severity=severity,
                    status="open",
                    title=title,
                    message=message,
                    first_seen_at=now,
                    last_seen_at=now,
                    last_resolved_at=None,
                    occurrence_count=1,
                    last_job_id=last_job_id,
                    last_run_status=last_run_status,
                    suggested_action=suggested_action,
                    metadata_json=metadata or {},
                )
                session.add(row)
            else:
                previous_severity = row.severity
                previous_rank = INCIDENT_SEVERITY_ORDER.get(str(previous_severity), -1)
                current_rank = INCIDENT_SEVERITY_ORDER.get(str(severity), -1)
                notification_event = (
                    "severity_escalated" if current_rank > previous_rank else "updated"
                )
                row.severity = severity
                row.title = title
                row.message = message
                row.last_seen_at = now
                row.last_job_id = last_job_id
                row.last_run_status = last_run_status
                row.suggested_action = suggested_action
                row.metadata_json = metadata or {}
                row.occurrence_count = int(row.occurrence_count or 0) + 1
                row.updated_at = now
            await session.commit()
            await session.refresh(row)
        except SQLAlchemyError:
            await session.rollback()
            self.logger.exception(
                "ine_operation_incident_open_or_update_failed",
                extra={
                    "operation_code": operation_code,
                    "incident_type": incident_type,
                },
            )
            raise
        result = self._serialize(row)
        result["notification_event"] = notification_event
        result["previous_severity"] = previous_severity
        return result

    async def resolve_open_incident(
        self,
        *,
        operation_code: str,
        incident_type: str,
        last_job_id: str | None,
        last_run_status: str | None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        session = self._require_session()
        now = func_now_utc()
        try:
            row = await self._get_open_incident(operation_code, incident_type)
            if row is None:
                return None
            row.status = "resolved"
            row.last_seen_at = now
            row.last_resolved_at = now
            row.last_job_id = last_job_id
            row.last_run_status = last_run_status
            row.metadata_json = {**(row.metadata_json or {}), **(metadata or {})}
            row.updated_at = now
            await session.commit()
            await session.refresh(row)
        except SQLAlchemyError:
            await session.rollback()
            self.logger.exception(
                "ine_operation_incident_resolve_failed",
                extra={
                    "operation_code": operation_code,
                    "incident_type": incident_type,
                },
            )
            raise
        result = self._serialize(row)
        result["notification_event"] = "resolved"
        return result

    async def merge_metadata(
        self,
        *,
        incident_id: int,
        metadata: dict[str, Any],
    ) -> dict[str, Any] | None:
        session = self._require_session()
        try:
            row = await session.get(INEOperationIncident, incident_id)
            if row is None:
                return None
            row.metadata_json = {**(row.metadata_json or {}), **metadata}
            row.updated_at = func_now_utc()
            await session.commit()
            await session.refresh(row)
        except SQLAlchemyError:
            await session.rollback()
            self.logger.exception(
                "ine_operation_incident_metadata_merge_failed",
                extra={"incident_id": incident_id},
            )
            raise
        return self._serialize(row)

    async def list_filtered(
        self,
        *,
        status: str | None = None,
        severity: str | None = None,
        operation_code: str | None = None,
        incident_type: str | None = None,
    ) -> list[dict[str, Any]]:
        if self.session is None:
            return []
        stmt = select(INEOperationIncident)
        if status is not None:
            stmt = stmt.where(INEOperationIncident.status == status)
        if severity is not None:
            stmt = stmt.where(INEOperationIncident.severity == severity)
        if operation_code is not None:
            stmt = stmt.where(INEOperationIncident.operation_code == operation_code)
        if incident_type is not None:
            stmt = stmt.where(INEOperationIncident.incident_type == incident_type)
        stmt = stmt.order_by(
            INEOperationIncident.last_seen_at.desc(),
            INEOperationIncident.id.desc(),
        )
        result = await self.session.execute(stmt)
        return [self._serialize(row) for row in result.scalars().all()]

    async def count_filtered(
        self,
        *,
        status: str | None = None,
        severity: str | None = None,
        operation_code: str | None = None,
        incident_type: str | None = None,
    ) -> int:
        if self.session is None:
            return 0
        stmt = select(func.count()).select_from(INEOperationIncident)
        if status is not None:
            stmt = stmt.where(INEOperationIncident.status == status)
        if severity is not None:
            stmt = stmt.where(INEOperationIncident.severity == severity)
        if operation_code is not None:
            stmt = stmt.where(INEOperationIncident.operation_code == operation_code)
        if incident_type is not None:
            stmt = stmt.where(INEOperationIncident.incident_type == incident_type)
        result = await self.session.execute(stmt)
        return int(result.scalar_one())

    async def summarize_filtered(
        self,
        *,
        status: str | None = None,
        severity: str | None = None,
        operation_code: str | None = None,
        incident_type: str | None = None,
    ) -> dict[str, int]:
        items = await self.list_filtered(
            status=status,
            severity=severity,
            operation_code=operation_code,
            incident_type=incident_type,
        )
        return {
            "incidents_total": len(items),
            "open_total": sum(1 for item in items if item["status"] == "open"),
            "resolved_total": sum(1 for item in items if item["status"] == "resolved"),
            "high_total": sum(1 for item in items if item["severity"] == "high"),
            "medium_total": sum(1 for item in items if item["severity"] == "medium"),
            "low_total": sum(1 for item in items if item["severity"] == "low"),
        }

    def _require_session(self) -> AsyncSession:
        if self.session is None:
            raise RuntimeError("No database session available")
        return self.session

    async def _get_open_incident(
        self,
        operation_code: str,
        incident_type: str,
    ) -> INEOperationIncident | None:
        session = self._require_session()
        stmt = select(INEOperationIncident).where(
            INEOperationIncident.operation_code == operation_code,
            INEOperationIncident.incident_type == incident_type,
            INEOperationIncident.status == "open",
        )
        result = await session.execute(stmt)
        return result.scalars().first()

    @staticmethod
    def _serialize(row: INEOperationIncident) -> dict[str, Any]:
        return {
            "incident_id": row.id,
            "operation_code": row.operation_code,
            "incident_type": row.incident_type,
            "severity": row.severity,
            "status": row.status,
            "title": row.title,
            "message": row.message,
            "first_seen_at": row.first_seen_at,
            "last_seen_at": row.last_seen_at,
            "last_resolved_at": row.last_resolved_at,
            "occurrence_count": row.occurrence_count,
            "last_job_id": row.last_job_id,
            "last_run_status": row.last_run_status,
            "suggested_action": row.suggested_action,
            "metadata": row.metadata_json or {},
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }


def func_now_utc() -> datetime:
    return datetime.now(UTC)
