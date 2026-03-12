from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.core.metrics import record_persistence_batch
from app.models import INESeriesNormalized
from app.schemas import NormalizedSeriesItem


UPSERT_COLUMN_NAMES = {
    "operation_code",
    "table_id",
    "variable_id",
    "geography_name",
    "geography_code",
    "period",
    "value",
    "unit",
    "metadata",
    "raw_payload",
}

CONFLICT_COLUMNS = [
    "operation_code",
    "table_id",
    "variable_id",
    "geography_name",
    "geography_code",
    "period",
]

DEFAULT_BATCH_SIZE = 500


class SeriesRepository:
    def __init__(self, session: AsyncSession | None) -> None:
        self.session = session
        self.logger = get_logger("app.repositories.series")

    async def upsert_many(
        self,
        items: Sequence[NormalizedSeriesItem | dict[str, Any]],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> int:
        if not items:
            self.logger.debug("normalized_upsert_skipped", extra={"reason": "empty_items"})
            return 0

        if self.session is None:
            self.logger.debug(
                "normalized_upsert_skipped",
                extra={"reason": "database_disabled", "rows": len(items)},
            )
            return 0

        preview = self._items_preview(items)
        self.logger.info("normalized_upsert_input_preview", extra=preview)

        total_inserted = 0
        prepared_rows = 0
        first_logged = False

        for batch_index, items_batch in enumerate(self._chunked(items, batch_size), start=1):
            batch = self.prepare_upsert_rows(items_batch)
            if not batch:
                self.logger.warning(
                    "normalized_upsert_empty_batch",
                    extra={"batch_index": batch_index, "items_in_batch": len(items_batch)},
                )
                continue

            if not first_logged:
                sample = batch[0]
                self.logger.info(
                    "normalized_upsert_prepared",
                    extra={
                        "rows": len(items),
                        "sample_keys": sorted(sample.keys()),
                        "sample_table_id": sample.get("table_id", ""),
                        "sample_period": sample.get("period", ""),
                        "batch_size": batch_size,
                    },
                )
                first_logged = True

            prepared_rows += len(batch)
            table_id = batch[0].get("table_id", "")
            statement = insert(INESeriesNormalized.__table__).values(batch)
            statement = statement.on_conflict_do_update(
                index_elements=CONFLICT_COLUMNS,
                set_={
                    "value": statement.excluded.value,
                    "unit": statement.excluded.unit,
                    "metadata": statement.excluded["metadata"],
                    "raw_payload": statement.excluded.raw_payload,
                },
            )

            try:
                await self.session.execute(statement)
                await self.session.commit()
            except SQLAlchemyError:
                await self.session.rollback()
                self.logger.exception(
                    "normalized_upsert_batch_failed",
                    extra={
                        "batch_index": batch_index,
                        "batch_size": len(batch),
                        "table_id": table_id,
                    },
                )
                break

            total_inserted += len(batch)
            record_persistence_batch("series", len(batch), len(batch))
            self.logger.info(
                "normalized_upsert_batch_completed",
                extra={
                    "batch_index": batch_index,
                    "batch_size": len(batch),
                    "rows_inserted": len(batch),
                    "table_id": table_id,
                },
            )

        if prepared_rows == 0:
            self.logger.warning(
                "normalized_upsert_empty_after_serialization",
                extra={"rows": len(items)},
            )
            return 0

        self.logger.info(
            "normalized_upsert_completed",
            extra={"rows": total_inserted, "prepared_rows": prepared_rows, "batch_size": batch_size},
        )
        return total_inserted

    async def list_normalized(
        self,
        operation_code: str | None = None,
        table_id: str | None = None,
        geography_code: str | None = None,
        variable_id: str | None = None,
        period_from: str | None = None,
        period_to: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        if self.session is None:
            return {"items": [], "total": 0, "page": page, "page_size": page_size}

        statement = select(INESeriesNormalized)
        count_statement = select(func.count()).select_from(INESeriesNormalized)

        filters = []
        if operation_code:
            filters.append(INESeriesNormalized.operation_code == operation_code)
        if table_id:
            filters.append(INESeriesNormalized.table_id == table_id)
        if geography_code:
            filters.append(INESeriesNormalized.geography_code == geography_code)
        if variable_id:
            filters.append(INESeriesNormalized.variable_id == variable_id)
        if period_from:
            filters.append(INESeriesNormalized.period >= period_from)
        if period_to:
            filters.append(INESeriesNormalized.period <= period_to)

        for condition in filters:
            statement = statement.where(condition)
            count_statement = count_statement.where(condition)

        statement = statement.order_by(
            INESeriesNormalized.operation_code.asc(),
            INESeriesNormalized.table_id.asc(),
            INESeriesNormalized.period.desc(),
            INESeriesNormalized.id.asc(),
        ).offset((page - 1) * page_size).limit(page_size)

        total = await self.session.scalar(count_statement)
        result = await self.session.execute(statement)
        items = [self._serialize_row(row) for row in result.scalars().all()]
        return {
            "items": items,
            "total": int(total or 0),
            "page": page,
            "page_size": page_size,
        }

    @staticmethod
    def prepare_upsert_rows(
        items: Sequence[NormalizedSeriesItem | dict[str, Any]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []

        for item in items:
            row = SeriesRepository._coerce_item_to_row(item)
            if row is not None:
                rows.append(row)

        return rows

    @staticmethod
    def _coerce_item_to_row(item: NormalizedSeriesItem | dict[str, Any]) -> dict[str, Any] | None:
        if isinstance(item, NormalizedSeriesItem):
            payload = item.model_dump()
        elif isinstance(item, dict):
            payload = dict(item)
        else:
            payload = {
                key: getattr(item, key)
                for key in dir(item)
                if not key.startswith("_") and not callable(getattr(item, key))
            }

        row = {
            "operation_code": SeriesRepository._string_value(payload.get("operation_code")),
            "table_id": SeriesRepository._string_value(payload.get("table_id")),
            "variable_id": SeriesRepository._string_value(payload.get("variable_id")),
            "geography_name": SeriesRepository._string_value(payload.get("geography_name")),
            "geography_code": SeriesRepository._string_value(payload.get("geography_code")),
            "period": SeriesRepository._string_value(payload.get("period")),
            "value": SeriesRepository._float_value(payload.get("value")),
            "unit": SeriesRepository._string_value(payload.get("unit")),
            "metadata": SeriesRepository._json_value(
                payload.get("metadata", payload.get("metadata_json", {}))
            ),
            "raw_payload": SeriesRepository._json_value(payload.get("raw_payload", {})),
        }

        filtered_row = {key: value for key, value in row.items() if key in UPSERT_COLUMN_NAMES}
        if not filtered_row.get("period"):
            return None
        return filtered_row

    @staticmethod
    def _items_preview(items: Sequence[NormalizedSeriesItem | dict[str, Any]]) -> dict[str, Any]:
        first_item = items[0]
        if isinstance(first_item, NormalizedSeriesItem):
            first_keys = sorted(first_item.model_dump().keys())
        elif isinstance(first_item, dict):
            first_keys = sorted(first_item.keys())
        else:
            first_keys = []

        return {
            "items_type": type(items).__name__,
            "items_length": len(items),
            "first_item_type": type(first_item).__name__,
            "first_item_keys": first_keys,
        }

    @staticmethod
    def _chunked(values: Sequence[NormalizedSeriesItem | dict[str, Any]], batch_size: int) -> list[Sequence[NormalizedSeriesItem | dict[str, Any]]]:
        return [values[index : index + batch_size] for index in range(0, len(values), batch_size)]

    @staticmethod
    def _string_value(value: Any) -> str:
        if value is None:
            return ""
        return str(value)

    @staticmethod
    def _float_value(value: Any) -> float | None:
        if value in (None, ""):
            return None
        if isinstance(value, bool):
            return float(value)
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(str(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _json_value(value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return json.loads(json.dumps(value, default=str))
        if isinstance(value, list):
            return {"items": json.loads(json.dumps(value, default=str))}
        if isinstance(value, (str, int, float, bool)):
            return {"value": value}
        return {"value": str(value)}

    @staticmethod
    def _serialize_row(row: INESeriesNormalized) -> dict[str, Any]:
        return {
            "id": row.id,
            "operation_code": row.operation_code,
            "table_id": row.table_id,
            "variable_id": row.variable_id,
            "geography_name": row.geography_name,
            "geography_code": row.geography_code,
            "period": row.period,
            "value": row.value,
            "unit": row.unit,
            "metadata": row.metadata_json,
            "inserted_at": row.inserted_at,
        }
