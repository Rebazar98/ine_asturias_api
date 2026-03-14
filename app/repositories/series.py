from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
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
    "territorial_unit_id",
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
                    "territorial_unit_id": statement.excluded.territorial_unit_id,
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
            extra={
                "rows": total_inserted,
                "prepared_rows": prepared_rows,
                "batch_size": batch_size,
            },
        )
        return total_inserted

    async def list_normalized(
        self,
        operation_code: str | None = None,
        table_id: str | None = None,
        geography_code: str | None = None,
        geography_name: str | None = None,
        geography_code_system: str = "ine",
        variable_id: str | None = None,
        period_from: str | None = None,
        period_to: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        if self.session is None:
            return {
                "items": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "pages": 0,
                "has_next": False,
                "has_previous": page > 1,
                "filters": self._serialize_filters(
                    operation_code=operation_code,
                    table_id=table_id,
                    geography_code=geography_code,
                    geography_name=geography_name,
                    geography_code_system=geography_code_system,
                    variable_id=variable_id,
                    period_from=period_from,
                    period_to=period_to,
                ),
            }

        statement = select(INESeriesNormalized)
        count_statement = select(func.count()).select_from(INESeriesNormalized)

        filters = []
        if operation_code:
            filters.append(INESeriesNormalized.operation_code == operation_code)
        if table_id:
            filters.append(INESeriesNormalized.table_id == table_id)
        if geography_code:
            filters.append(INESeriesNormalized.geography_code == geography_code)
        if geography_name:
            filters.append(func.lower(INESeriesNormalized.geography_name) == geography_name.lower())
        if variable_id:
            filters.append(INESeriesNormalized.variable_id == variable_id)
        if period_from:
            filters.append(INESeriesNormalized.period >= period_from)
        if period_to:
            filters.append(INESeriesNormalized.period <= period_to)

        for condition in filters:
            statement = statement.where(condition)
            count_statement = count_statement.where(condition)

        statement = (
            statement.order_by(
                INESeriesNormalized.operation_code.asc(),
                INESeriesNormalized.table_id.asc(),
                INESeriesNormalized.period.desc(),
                INESeriesNormalized.id.asc(),
            )
            .offset((page - 1) * page_size)
            .limit(page_size)
        )

        total = await self.session.scalar(count_statement)
        result = await self.session.execute(statement)
        items = [self._serialize_row(row) for row in result.scalars().all()]
        total_count = int(total or 0)
        pages = (total_count + page_size - 1) // page_size if total_count else 0
        return {
            "items": items,
            "total": total_count,
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "has_next": page < pages,
            "has_previous": page > 1,
            "filters": self._serialize_filters(
                operation_code=operation_code,
                table_id=table_id,
                geography_code=geography_code,
                geography_name=geography_name,
                geography_code_system=geography_code_system,
                variable_id=variable_id,
                period_from=period_from,
                period_to=period_to,
            ),
        }

    async def list_latest_indicators_by_geography(
        self,
        *,
        geography_code: str,
        operation_code: str | None = None,
        variable_id: str | None = None,
        period_from: str | None = None,
        period_to: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        if self.session is None:
            return {
                "items": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "pages": 0,
                "has_next": False,
                "has_previous": page > 1,
                "filters": self._serialize_latest_indicator_filters(
                    geography_code=geography_code,
                    operation_code=operation_code,
                    variable_id=variable_id,
                    period_from=period_from,
                    period_to=period_to,
                ),
                "summary": {
                    "operation_codes": [],
                    "latest_period": None,
                },
            }

        filters = [INESeriesNormalized.geography_code == geography_code]
        if operation_code:
            filters.append(INESeriesNormalized.operation_code == operation_code)
        if variable_id:
            filters.append(INESeriesNormalized.variable_id == variable_id)
        if period_from:
            filters.append(INESeriesNormalized.period >= period_from)
        if period_to:
            filters.append(INESeriesNormalized.period <= period_to)

        ranked_rows = (
            select(
                INESeriesNormalized.id.label("id"),
                INESeriesNormalized.operation_code.label("operation_code"),
                INESeriesNormalized.table_id.label("table_id"),
                INESeriesNormalized.variable_id.label("variable_id"),
                INESeriesNormalized.territorial_unit_id.label("territorial_unit_id"),
                INESeriesNormalized.geography_name.label("geography_name"),
                INESeriesNormalized.geography_code.label("geography_code"),
                INESeriesNormalized.period.label("period"),
                INESeriesNormalized.value.label("value"),
                INESeriesNormalized.unit.label("unit"),
                INESeriesNormalized.metadata_json.label("metadata"),
                INESeriesNormalized.inserted_at.label("inserted_at"),
                func.row_number()
                .over(
                    partition_by=(
                        INESeriesNormalized.operation_code,
                        INESeriesNormalized.table_id,
                        INESeriesNormalized.variable_id,
                        INESeriesNormalized.geography_code,
                    ),
                    order_by=(
                        INESeriesNormalized.period.desc(),
                        INESeriesNormalized.id.desc(),
                    ),
                )
                .label("series_rank"),
            )
            .where(*filters)
            .subquery()
        )
        latest_rows = (
            select(
                ranked_rows.c.id,
                ranked_rows.c.operation_code,
                ranked_rows.c.table_id,
                ranked_rows.c.variable_id,
                ranked_rows.c.territorial_unit_id,
                ranked_rows.c.geography_name,
                ranked_rows.c.geography_code,
                ranked_rows.c.period,
                ranked_rows.c.value,
                ranked_rows.c.unit,
                ranked_rows.c.metadata,
                ranked_rows.c.inserted_at,
            )
            .where(ranked_rows.c.series_rank == 1)
            .subquery()
        )

        count_statement = select(func.count()).select_from(latest_rows)
        operations_statement = (
            select(latest_rows.c.operation_code)
            .distinct()
            .order_by(latest_rows.c.operation_code.asc())
        )
        latest_period_statement = select(func.max(latest_rows.c.period))
        statement = (
            select(latest_rows)
            .order_by(
                latest_rows.c.period.desc(),
                latest_rows.c.operation_code.asc(),
                latest_rows.c.table_id.asc(),
                latest_rows.c.variable_id.asc(),
            )
            .offset((page - 1) * page_size)
            .limit(page_size)
        )

        total = int(await self.session.scalar(count_statement) or 0)
        operations_result = await self.session.execute(operations_statement)
        latest_period = await self.session.scalar(latest_period_statement)
        result = await self.session.execute(statement)
        items = [self.serialize_latest_indicator_item(row) for row in result.mappings().all()]
        pages = (total + page_size - 1) // page_size if total else 0
        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "has_next": page < pages,
            "has_previous": page > 1,
            "filters": self._serialize_latest_indicator_filters(
                geography_code=geography_code,
                operation_code=operation_code,
                variable_id=variable_id,
                period_from=period_from,
                period_to=period_to,
            ),
            "summary": {
                "operation_codes": list(operations_result.scalars().all()),
                "latest_period": latest_period,
            },
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
        payload = SeriesRepository._coerce_payload(item)

        row = {
            "operation_code": SeriesRepository._string_value(payload.get("operation_code")),
            "table_id": SeriesRepository._string_value(payload.get("table_id")),
            "variable_id": SeriesRepository._string_value(payload.get("variable_id")),
            "territorial_unit_id": SeriesRepository._int_value(payload.get("territorial_unit_id")),
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
    def serialize_latest_indicator_item(item: Any) -> dict[str, Any]:
        payload = SeriesRepository._coerce_payload(item)
        metadata = SeriesRepository._json_value(
            payload.get("metadata", payload.get("metadata_json", {}))
        )
        operation_code = SeriesRepository._string_value(payload.get("operation_code"))
        table_id = SeriesRepository._string_value(payload.get("table_id"))
        variable_id = SeriesRepository._string_value(payload.get("variable_id"))

        return {
            "series_key": SeriesRepository._build_series_key(
                operation_code=operation_code,
                table_id=table_id,
                variable_id=variable_id,
            ),
            "label": SeriesRepository._build_indicator_label(
                variable_id=variable_id,
                metadata=metadata,
            ),
            "value": payload.get("value"),
            "unit": SeriesRepository._nullable_string_value(payload.get("unit")),
            "period": SeriesRepository._nullable_string_value(payload.get("period")),
            "metadata": metadata,
            "operation_code": operation_code,
            "table_id": table_id,
            "variable_id": variable_id,
            "geography_code": SeriesRepository._string_value(payload.get("geography_code")),
            "geography_name": SeriesRepository._string_value(payload.get("geography_name")),
        }

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
    def _chunked(
        values: Sequence[NormalizedSeriesItem | dict[str, Any]], batch_size: int
    ) -> list[Sequence[NormalizedSeriesItem | dict[str, Any]]]:
        return [values[index : index + batch_size] for index in range(0, len(values), batch_size)]

    @staticmethod
    def _string_value(value: Any) -> str:
        if value is None:
            return ""
        return str(value)

    @staticmethod
    def _nullable_string_value(value: Any) -> str | None:
        normalized = SeriesRepository._string_value(value)
        return normalized or None

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
    def _int_value(value: Any) -> int | None:
        if value in (None, ""):
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        try:
            return int(str(value))
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

    @staticmethod
    def _serialize_filters(
        operation_code: str | None,
        table_id: str | None,
        geography_code: str | None,
        geography_name: str | None,
        geography_code_system: str,
        variable_id: str | None,
        period_from: str | None,
        period_to: str | None,
    ) -> dict[str, Any]:
        return {
            "operation_code": operation_code,
            "table_id": table_id,
            "geography_code": geography_code,
            "geography_name": geography_name,
            "geography_code_system": geography_code_system,
            "variable_id": variable_id,
            "period_from": period_from,
            "period_to": period_to,
        }

    @staticmethod
    def _serialize_latest_indicator_filters(
        *,
        geography_code: str,
        operation_code: str | None,
        variable_id: str | None,
        period_from: str | None,
        period_to: str | None,
    ) -> dict[str, Any]:
        return {
            "geography_code": geography_code,
            "geography_code_system": "ine",
            "operation_code": operation_code,
            "variable_id": variable_id,
            "period_from": period_from,
            "period_to": period_to,
        }

    @staticmethod
    def _coerce_payload(item: Any) -> dict[str, Any]:
        if isinstance(item, NormalizedSeriesItem):
            return item.model_dump()
        if isinstance(item, Mapping):
            return dict(item)
        return {
            key: getattr(item, key)
            for key in dir(item)
            if not key.startswith("_") and not callable(getattr(item, key))
        }

    @staticmethod
    def _build_series_key(*, operation_code: str, table_id: str, variable_id: str) -> str:
        return ".".join(
            [
                "ine",
                operation_code or "unknown_operation",
                table_id or "unknown_table",
                variable_id or "unknown_variable",
            ]
        )

    @staticmethod
    def _build_indicator_label(*, variable_id: str, metadata: dict[str, Any]) -> str:
        series_name = SeriesRepository._string_value(metadata.get("series_name"))
        if series_name:
            return series_name

        series_code = SeriesRepository._string_value(metadata.get("series_code"))
        if series_code:
            return series_code

        if variable_id:
            return variable_id

        return "indicator"
