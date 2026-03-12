from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.core.metrics import record_catalog_event
from app.models import INETableCatalog


class TableCatalogRepository:
    def __init__(self, session: AsyncSession | None) -> None:
        self.session = session
        self.logger = get_logger("app.repositories.catalog")

    async def upsert_discovered_tables(
        self,
        operation_code: str,
        tables: list[dict[str, Any]],
        request_path: str,
        resolution_context: dict[str, Any] | None = None,
    ) -> int:
        if not tables:
            return 0

        if self.session is None:
            self.logger.debug(
                "catalog_discovery_skipped",
                extra={"reason": "database_disabled", "operation_code": operation_code},
            )
            return 0

        values = [
            {
                "operation_code": operation_code,
                "table_id": str(table.get("table_id", "")),
                "table_name": str(table.get("table_name", "")),
                "request_path": request_path,
                "resolution_context": dict(resolution_context or {}),
                "validation_status": "unknown",
                "metadata": dict(table.get("metadata", {})),
            }
            for table in tables
            if table.get("table_id")
        ]
        if not values:
            return 0

        statement = insert(INETableCatalog.__table__).values(values)
        statement = statement.on_conflict_do_update(
            index_elements=["operation_code", "table_id"],
            set_={
                "table_name": statement.excluded.table_name,
                "request_path": statement.excluded.request_path,
                "resolution_context": statement.excluded.resolution_context,
                "metadata": statement.excluded["metadata"],
                "updated_at": datetime.now(timezone.utc),
            },
        )

        try:
            await self.session.execute(statement)
            await self.session.commit()
        except SQLAlchemyError:
            await self.session.rollback()
            self.logger.exception(
                "catalog_discovery_failed",
                extra={"operation_code": operation_code, "tables": len(values)},
            )
            return 0

        record_catalog_event("discovered")
        self.logger.info(
            "catalog_tables_discovered",
            extra={"operation_code": operation_code, "tables_registered": len(values)},
        )
        return len(values)

    async def update_table_status(
        self,
        operation_code: str,
        table_id: str,
        table_name: str,
        request_path: str,
        resolution_context: dict[str, Any] | None = None,
        has_asturias_data: bool | None = None,
        validation_status: str = "unknown",
        normalized_rows: int = 0,
        raw_rows_retrieved: int = 0,
        filtered_rows_retrieved: int = 0,
        series_kept: int = 0,
        series_discarded: int = 0,
        metadata: dict[str, Any] | None = None,
        notes: str = "",
        last_warning: str = "",
    ) -> bool:
        if self.session is None:
            self.logger.debug(
                "catalog_status_update_skipped",
                extra={"reason": "database_disabled", "operation_code": operation_code, "table_id": table_id},
            )
            return False

        now = datetime.now(timezone.utc)
        statement = insert(INETableCatalog.__table__).values(
            {
                "operation_code": operation_code,
                "table_id": table_id,
                "table_name": table_name,
                "request_path": request_path,
                "resolution_context": dict(resolution_context or {}),
                "has_asturias_data": has_asturias_data,
                "validation_status": validation_status,
                "normalized_rows": normalized_rows,
                "raw_rows_retrieved": raw_rows_retrieved,
                "filtered_rows_retrieved": filtered_rows_retrieved,
                "series_kept": series_kept,
                "series_discarded": series_discarded,
                "last_checked_at": now,
                "updated_at": now,
                "metadata": dict(metadata or {}),
                "notes": notes,
                "last_warning": last_warning,
            }
        )
        statement = statement.on_conflict_do_update(
            index_elements=["operation_code", "table_id"],
            set_={
                "table_name": statement.excluded.table_name,
                "request_path": statement.excluded.request_path,
                "resolution_context": statement.excluded.resolution_context,
                "has_asturias_data": statement.excluded.has_asturias_data,
                "validation_status": statement.excluded.validation_status,
                "normalized_rows": statement.excluded.normalized_rows,
                "raw_rows_retrieved": statement.excluded.raw_rows_retrieved,
                "filtered_rows_retrieved": statement.excluded.filtered_rows_retrieved,
                "series_kept": statement.excluded.series_kept,
                "series_discarded": statement.excluded.series_discarded,
                "last_checked_at": statement.excluded.last_checked_at,
                "updated_at": statement.excluded.updated_at,
                "metadata": statement.excluded["metadata"],
                "notes": statement.excluded.notes,
                "last_warning": statement.excluded.last_warning,
            },
        )

        try:
            await self.session.execute(statement)
            await self.session.commit()
        except SQLAlchemyError:
            await self.session.rollback()
            self.logger.exception(
                "catalog_status_update_failed",
                extra={
                    "operation_code": operation_code,
                    "table_id": table_id,
                    "validation_status": validation_status,
                },
            )
            return False

        record_catalog_event(validation_status)
        self.logger.info(
            "catalog_status_updated",
            extra={
                "operation_code": operation_code,
                "table_id": table_id,
                "validation_status": validation_status,
                "has_asturias_data": has_asturias_data,
                "normalized_rows": normalized_rows,
            },
        )
        return True

    async def list_by_operation(self, operation_code: str) -> list[dict[str, Any]]:
        if self.session is None:
            return []

        statement = (
            select(INETableCatalog)
            .where(INETableCatalog.operation_code == operation_code)
            .order_by(INETableCatalog.table_id.asc())
        )
        result = await self.session.execute(statement)
        return [self._serialize_row(row) for row in result.scalars().all()]

    async def get_operation_summary(self, operation_code: str) -> dict[str, Any]:
        rows = await self.list_by_operation(operation_code)
        return {
            "operation_code": operation_code,
            "total_tables": len(rows),
            "has_data": sum(1 for row in rows if row["validation_status"] == "has_data"),
            "no_data": sum(1 for row in rows if row["validation_status"] == "no_data"),
            "failed": sum(1 for row in rows if row["validation_status"] == "failed"),
            "unknown": sum(1 for row in rows if row["validation_status"] == "unknown"),
        }

    async def get_known_no_data_table_ids(self, operation_code: str) -> set[str]:
        if self.session is None:
            return set()

        statement = select(INETableCatalog.table_id).where(
            INETableCatalog.operation_code == operation_code,
            INETableCatalog.validation_status == "no_data",
            INETableCatalog.has_asturias_data.is_(False),
        )
        result = await self.session.execute(statement)
        return {value for value in result.scalars().all() if value}

    @staticmethod
    def _serialize_row(row: INETableCatalog) -> dict[str, Any]:
        return {
            "id": row.id,
            "operation_code": row.operation_code,
            "table_id": row.table_id,
            "table_name": row.table_name,
            "request_path": row.request_path,
            "resolution_context": row.resolution_context,
            "has_asturias_data": row.has_asturias_data,
            "validation_status": row.validation_status,
            "normalized_rows": row.normalized_rows,
            "raw_rows_retrieved": row.raw_rows_retrieved,
            "filtered_rows_retrieved": row.filtered_rows_retrieved,
            "series_kept": row.series_kept,
            "series_discarded": row.series_discarded,
            "last_checked_at": row.last_checked_at,
            "first_seen_at": row.first_seen_at,
            "updated_at": row.updated_at,
            "metadata": row.metadata_json,
            "notes": row.notes,
            "last_warning": row.last_warning,
        }
