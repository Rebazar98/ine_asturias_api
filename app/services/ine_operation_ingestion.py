from __future__ import annotations

import asyncio
import unicodedata
from collections.abc import Awaitable, Callable
from typing import Any, Literal

from app.core.logging import get_logger
from app.core.metrics import record_normalization
from app.repositories.catalog import TableCatalogRepository
from app.repositories.ingestion import IngestionRepository
from app.repositories.series import SeriesRepository
from app.services.asturias_resolver import AsturiasResolutionError
from app.services.normalizers import (
    inspect_payload_shape,
    normalize_asturias_payload_with_stats,
    normalize_table_payload_with_stats,
)


LARGE_TABLE_WARNING_THRESHOLD = 50000
ProgressReporter = Callable[[dict[str, Any]], Awaitable[None]]


class INEOperationIngestionService:
    def __init__(
        self,
        ingestion_repo: IngestionRepository,
        series_repo: SeriesRepository,
        catalog_repo: TableCatalogRepository,
    ) -> None:
        self.ingestion_repo = ingestion_repo
        self.series_repo = series_repo
        self.catalog_repo = catalog_repo
        self.logger = get_logger("app.services.ine_operation_ingestion")

    async def normalize_and_store_table(
        self,
        payload: dict[str, Any] | list[Any],
        table_id: str,
    ) -> int:
        payload_stats = inspect_payload_shape(payload)

        try:
            outcome = await asyncio.to_thread(normalize_table_payload_with_stats, payload, table_id)
        except Exception:
            self.logger.exception(
                "table_normalization_failed",
                extra={"table_id": table_id, **payload_stats},
            )
            return 0

        if not outcome.items:
            record_normalization("table", 0, outcome.discarded_counts)
            self.logger.warning(
                "table_normalization_empty",
                extra={
                    "table_id": table_id,
                    **payload_stats,
                    "rows_generated": 0,
                    "discarded_counts": outcome.discarded_counts,
                },
            )
            return 0

        record_normalization("table", len(outcome.items), outcome.discarded_counts)
        self.logger.info(
            "table_normalization_prepared",
            extra={
                "table_id": table_id,
                **payload_stats,
                "rows_generated": len(outcome.items),
                "rows_sent_to_upsert": len(outcome.items),
                "discarded_counts": outcome.discarded_counts,
                "first_row": outcome.items[0].model_dump(),
            },
        )
        return await self.series_repo.upsert_many(outcome.items)

    async def normalize_and_store_asturias(
        self,
        payload: dict[str, Any] | list[Any],
        op_code: str,
        geography_name: str,
        geography_code: str,
        table_id: str,
    ) -> int:
        payload_stats = inspect_payload_shape(payload)

        try:
            outcome = await asyncio.to_thread(
                normalize_asturias_payload_with_stats,
                payload,
                op_code,
                geography_name,
                geography_code,
                table_id,
            )
        except Exception:
            self.logger.exception(
                "asturias_normalization_failed",
                extra={"op_code": op_code, "table_id": table_id, **payload_stats},
            )
            return 0

        if not outcome.items:
            record_normalization("asturias", 0, outcome.discarded_counts)
            self.logger.warning(
                "asturias_normalization_empty",
                extra={
                    "op_code": op_code,
                    "table_id": table_id,
                    **payload_stats,
                    "rows_generated": 0,
                    "discarded_counts": outcome.discarded_counts,
                },
            )
            return 0

        record_normalization("asturias", len(outcome.items), outcome.discarded_counts)
        self.logger.info(
            "asturias_normalization_prepared",
            extra={
                "op_code": op_code,
                "table_id": table_id,
                **payload_stats,
                "rows_generated": len(outcome.items),
                "rows_sent_to_upsert": len(outcome.items),
                "discarded_counts": outcome.discarded_counts,
                "first_row": outcome.items[0].model_dump(),
            },
        )
        return await self.series_repo.upsert_many(outcome.items)

    async def ingest_asturias_operation(
        self,
        op_code: str,
        resolution: Any,
        nult: int | None,
        det: Literal[0, 1, 2] | None,
        tip: Literal["A", "M", "AM"] | None,
        periodicidad: str | None,
        max_tables: int | None,
        skip_known_no_data: bool,
        ine_client: Any,
        progress_reporter: ProgressReporter | None = None,
    ) -> dict[str, Any]:
        tables_payload = await ine_client.get_operation_tables(op_code)
        await self.ingestion_repo.save_raw(
            source_type="operation_tables",
            source_key=op_code,
            request_path=f"TABLAS_OPERACION/{op_code}",
            request_params={},
            payload=tables_payload,
        )

        discovered_table_candidates = self._extract_table_candidates(tables_payload)
        self.logger.info(
            "asturias_operation_tables_discovered",
            extra={
                "operation_code": op_code,
                "tables_found": len(discovered_table_candidates),
                "table_ids": [item["table_id"] for item in discovered_table_candidates],
            },
        )

        if not discovered_table_candidates:
            raise AsturiasResolutionError(
                detail={"message": "No valid tables were found for this operation.", "operation_code": op_code},
                status_code=404,
            )

        resolution_context = {
            "geo_variable_id": resolution.geo_variable_id,
            "asturias_value_id": resolution.asturias_value_id,
            "variable_name": resolution.variable_name,
            "asturias_label": resolution.asturias_label,
        }
        await self.catalog_repo.upsert_discovered_tables(
            operation_code=op_code,
            tables=discovered_table_candidates,
            request_path=f"TABLAS_OPERACION/{op_code}",
            resolution_context=resolution_context,
        )

        table_candidates = list(discovered_table_candidates)
        skipped_catalog_table_ids: list[str] = []
        if skip_known_no_data:
            skipped_catalog_table_ids = sorted(await self.catalog_repo.get_known_no_data_table_ids(op_code))
            if skipped_catalog_table_ids:
                skipped_lookup = set(skipped_catalog_table_ids)
                table_candidates = [table for table in table_candidates if table["table_id"] not in skipped_lookup]
                self.logger.info(
                    "catalog_skipped_known_no_data_tables",
                    extra={
                        "operation_code": op_code,
                        "skipped_table_ids": skipped_catalog_table_ids,
                    },
                )

        if max_tables is not None:
            table_candidates = table_candidates[:max_tables]

        table_params = self._build_query_params(
            g1=f"{resolution.geo_variable_id}:{resolution.asturias_value_id}",
            nult=nult,
            det=det,
            tip=tip,
            p=periodicidad,
        )

        selected_table_ids = [item["table_id"] for item in table_candidates]
        self.logger.info(
            "asturias_operation_tables_selected",
            extra={
                "operation_code": op_code,
                "selected_tables": selected_table_ids,
                "request_params": table_params,
                "max_tables_effective": max_tables,
                "skipped_known_no_data": skipped_catalog_table_ids,
            },
        )

        if progress_reporter is not None:
            await progress_reporter(
                {
                    "stage": "tables_selected",
                    "tables_found": len(discovered_table_candidates),
                    "tables_selected": len(selected_table_ids),
                    "selected_table_ids": selected_table_ids,
                    "tables_skipped_catalog": len(skipped_catalog_table_ids),
                }
            )

        results: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []
        normalized_rows = 0

        for table_index, table in enumerate(table_candidates, start=1):
            table_id = table["table_id"]
            table_name = table["table_name"]
            request_path = f"DATOS_TABLA/{table_id}"
            self.logger.info(
                "asturias_table_fetch_started",
                extra={
                    "operation_code": op_code,
                    "table_id": table_id,
                    "table_name": table_name,
                    "request_path": request_path,
                    "request_params": table_params,
                    "table_index": table_index,
                },
            )
            if progress_reporter is not None:
                await progress_reporter(
                    {
                        "stage": "fetching_table",
                        "table_index": table_index,
                        "tables_total": len(table_candidates),
                        "table_id": table_id,
                        "table_name": table_name,
                    }
                )

            try:
                table_payload = await ine_client.get_table(table_id, table_params)
            except Exception as exc:
                error_detail = {
                    "table_id": table_id,
                    "table_name": table_name,
                    "request_path": request_path,
                    "request_params": table_params,
                    "error": getattr(exc, "detail", str(exc)),
                }
                self.logger.warning(
                    "asturias_table_fetch_failed",
                    extra={
                        "operation_code": op_code,
                        "table_id": table_id,
                        "error": error_detail["error"],
                    },
                )
                await self.catalog_repo.update_table_status(
                    operation_code=op_code,
                    table_id=table_id,
                    table_name=table_name,
                    request_path=request_path,
                    resolution_context=resolution_context,
                    has_asturias_data=None,
                    validation_status="failed",
                    metadata=table["metadata"],
                    notes="INE upstream error while fetching table.",
                    last_warning=self._summarize_error(error_detail["error"]),
                )
                errors.append(error_detail)
                if progress_reporter is not None:
                    await progress_reporter(
                        {
                            "stage": "table_failed",
                            "table_index": table_index,
                            "tables_total": len(table_candidates),
                            "table_id": table_id,
                            "errors": len(errors),
                        }
                    )
                continue

            await self.ingestion_repo.save_raw(
                source_type="operation_asturias_table",
                source_key=f"{op_code}:{table_id}:{resolution.geo_variable_id}:{resolution.asturias_value_id}",
                request_path=request_path,
                request_params=table_params,
                payload=table_payload,
            )

            raw_rows_retrieved = self._count_retrieved_rows(table_payload)
            last_warning = ""
            if raw_rows_retrieved > LARGE_TABLE_WARNING_THRESHOLD:
                warning_detail = {
                    "table_id": table_id,
                    "table_name": table_name,
                    "warning": "large_table_detected",
                    "raw_rows_retrieved": raw_rows_retrieved,
                }
                warnings.append(warning_detail)
                last_warning = "large_table_detected"
                self.logger.warning(
                    "asturias_table_large_payload",
                    extra={
                        "operation_code": op_code,
                        "table_id": table_id,
                        "raw_rows_retrieved": raw_rows_retrieved,
                    },
                )

            filtered_payload, filtered_stats = await asyncio.to_thread(
                self._filter_payload_for_asturias,
                table_payload,
                resolution.asturias_value_id,
                resolution.asturias_label or "Asturias",
            )
            self.logger.info(
                "asturias_table_filter_completed",
                extra={
                    "operation_code": op_code,
                    "table_id": table_id,
                    "raw_rows_retrieved": raw_rows_retrieved,
                    "filtered_rows_retrieved": filtered_stats["rows_kept"],
                    "series_kept": filtered_stats["series_kept"],
                    "series_discarded": filtered_stats["series_discarded"],
                },
            )

            if filtered_stats["series_kept"] == 0:
                warning_detail = {
                    "table_id": table_id,
                    "table_name": table_name,
                    "warning": "no_asturias_rows_after_validation",
                    "raw_rows_retrieved": raw_rows_retrieved,
                }
                warnings.append(warning_detail)
                await self.catalog_repo.update_table_status(
                    operation_code=op_code,
                    table_id=table_id,
                    table_name=table_name,
                    request_path=request_path,
                    resolution_context=resolution_context,
                    has_asturias_data=False,
                    validation_status="no_data",
                    normalized_rows=0,
                    raw_rows_retrieved=raw_rows_retrieved,
                    filtered_rows_retrieved=filtered_stats["rows_kept"],
                    series_kept=filtered_stats["series_kept"],
                    series_discarded=filtered_stats["series_discarded"],
                    metadata=table["metadata"],
                    notes="Table does not contain rows valid for Asturias after validation.",
                    last_warning="no_asturias_rows_after_validation",
                )
                if progress_reporter is not None:
                    await progress_reporter(
                        {
                            "stage": "table_filtered_empty",
                            "table_index": table_index,
                            "tables_total": len(table_candidates),
                            "table_id": table_id,
                            "warnings": len(warnings),
                        }
                    )
                continue

            table_normalized_rows = await self.normalize_and_store_asturias(
                payload=filtered_payload,
                op_code=op_code,
                geography_name=resolution.asturias_label or "Asturias",
                geography_code=resolution.asturias_value_id,
                table_id=table_id,
            )
            normalized_rows += table_normalized_rows

            await self.catalog_repo.update_table_status(
                operation_code=op_code,
                table_id=table_id,
                table_name=table_name,
                request_path=request_path,
                resolution_context=resolution_context,
                has_asturias_data=True,
                validation_status="has_data",
                normalized_rows=table_normalized_rows,
                raw_rows_retrieved=raw_rows_retrieved,
                filtered_rows_retrieved=filtered_stats["rows_kept"],
                series_kept=filtered_stats["series_kept"],
                series_discarded=filtered_stats["series_discarded"],
                metadata=table["metadata"],
                notes="Table produced Asturias rows.",
                last_warning=last_warning,
            )

            results.append(
                {
                    "table_id": table_id,
                    "table_name": table_name,
                    "request_path": request_path,
                    "request_params": table_params,
                    "raw_rows_retrieved": raw_rows_retrieved,
                    "filtered_rows_retrieved": filtered_stats["rows_kept"],
                    "data": filtered_payload,
                    "table_metadata": table["metadata"],
                }
            )
            if progress_reporter is not None:
                await progress_reporter(
                    {
                        "stage": "table_completed",
                        "table_index": table_index,
                        "tables_total": len(table_candidates),
                        "table_id": table_id,
                        "tables_succeeded": len(results),
                        "tables_failed": len(errors),
                        "normalized_rows": normalized_rows,
                    }
                )

        if not results:
            raise AsturiasResolutionError(
                detail={
                    "message": "No table data could be recovered for Asturias in this operation.",
                    "operation_code": op_code,
                    "geo_variable_id": resolution.geo_variable_id,
                    "asturias_value_id": resolution.asturias_value_id,
                    "tables_found": selected_table_ids,
                    "errors": errors,
                    "warnings": warnings,
                },
                status_code=502,
            )

        aggregated_payload = {
            "operation_code": op_code,
            "resolution": resolution_context,
            "tables_found": [
                {
                    "table_id": table["table_id"],
                    "table_name": table["table_name"],
                    "metadata": table["metadata"],
                }
                for table in table_candidates
            ],
            "tables_selected": selected_table_ids,
            "results": results,
            "errors": errors,
            "warnings": warnings,
            "summary": {
                "tables_found": len(discovered_table_candidates),
                "tables_selected": len(selected_table_ids),
                "tables_succeeded": len(results),
                "tables_failed": len(errors),
                "warnings": len(warnings),
                "normalized_rows": normalized_rows,
                "max_tables_effective": max_tables,
                "tables_skipped_catalog": len(skipped_catalog_table_ids),
            },
        }

        await self.ingestion_repo.save_raw(
            source_type="operation_asturias",
            source_key=f"{op_code}:{resolution.geo_variable_id}:{resolution.asturias_value_id}",
            request_path=f"TABLAS_OPERACION/{op_code} -> DATOS_TABLA/*",
            request_params=table_params,
            payload=aggregated_payload,
        )
        self.logger.info(
            "asturias_operation_lookup_completed",
            extra={
                "operation_code": op_code,
                "tables_succeeded": len(results),
                "tables_failed": len(errors),
                "warnings": len(warnings),
                "normalized_rows": normalized_rows,
                "tables_skipped_catalog": len(skipped_catalog_table_ids),
            },
        )
        return aggregated_payload

    @staticmethod
    def _extract_table_candidates(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        seen: set[str] = set()

        for record in INEOperationIngestionService._extract_records(payload):
            table_id = INEOperationIngestionService._pick_first(
                record,
                ("IdTabla", "idTabla", "Id", "id", "Codigo", "codigo", "Cod", "cod"),
            )
            if not table_id or table_id in seen:
                continue

            seen.add(table_id)
            candidates.append(
                {
                    "table_id": table_id,
                    "table_name": INEOperationIngestionService._pick_first(
                        record,
                        ("Nombre", "name", "Descripcion", "description", "Titulo", "titulo"),
                    ),
                    "metadata": record,
                }
            )

        return candidates

    @staticmethod
    def _filter_payload_for_asturias(
        payload: dict[str, Any] | list[Any],
        asturias_value_id: str,
        asturias_label: str,
    ) -> tuple[dict[str, Any] | list[Any], dict[str, int]]:
        records = INEOperationIngestionService._extract_records(payload)
        kept_records = [
            record
            for record in records
            if INEOperationIngestionService._series_matches_asturias(record, asturias_value_id, asturias_label)
        ]
        filtered_payload = kept_records if isinstance(payload, list) else {**payload, "Data": kept_records}
        return filtered_payload, {
            "series_kept": len(kept_records),
            "series_discarded": max(len(records) - len(kept_records), 0),
            "rows_kept": INEOperationIngestionService._count_retrieved_rows(filtered_payload),
        }

    @staticmethod
    def _series_matches_asturias(
        series: dict[str, Any],
        asturias_value_id: str,
        asturias_label: str,
    ) -> bool:
        labels = {
            INEOperationIngestionService._normalize_text(asturias_label),
            "asturias",
            "asturias principado de",
            "principado de asturias",
        }

        for item in INEOperationIngestionService._ensure_list(series.get("MetaData") or series.get("metadata")):
            if not isinstance(item, dict):
                continue

            for key in ("Id", "id", "Codigo", "codigo", "Code", "code"):
                if str(item.get(key, "")) == str(asturias_value_id):
                    return True

            for key in ("Nombre", "name", "Valor", "value", "Descripcion", "description"):
                candidate = INEOperationIngestionService._normalize_text(str(item.get(key, "")))
                if candidate and any(label in candidate or candidate in label for label in labels):
                    return True

        for key in ("Nombre", "name", "Descripcion", "description", "Titulo", "titulo"):
            candidate = INEOperationIngestionService._normalize_text(str(series.get(key, "")))
            if candidate and any(label in candidate or candidate in label for label in labels):
                return True

        return False

    @staticmethod
    def _extract_records(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]

        if not isinstance(payload, dict):
            return []

        for key in ("Data", "data", "Tables", "tables", "Tablas", "tablas", "Resultados", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

        return [payload]

    @staticmethod
    def _count_retrieved_rows(payload: dict[str, Any] | list[Any]) -> int:
        total = 0
        for series in INEOperationIngestionService._extract_records(payload):
            data_points = series.get("Data") or series.get("data")
            if isinstance(data_points, list):
                total += len(data_points)
            else:
                total += 1
        return total

    @staticmethod
    def _pick_first(payload: dict[str, Any], keys: tuple[str, ...]) -> str:
        for key in keys:
            value = payload.get(key)
            if value not in (None, ""):
                return str(value)
        return ""

    @staticmethod
    def _ensure_list(value: Any) -> list[Any]:
        if isinstance(value, list):
            return value
        if value is None:
            return []
        return [value]

    @staticmethod
    def _normalize_text(value: str) -> str:
        normalized = unicodedata.normalize("NFKD", value or "")
        return normalized.encode("ascii", "ignore").decode("ascii").lower().replace(",", " ").strip()

    @staticmethod
    def _build_query_params(**kwargs: Any) -> dict[str, Any]:
        return {key: value for key, value in kwargs.items() if value is not None}

    @staticmethod
    def _summarize_error(detail: Any) -> str:
        if isinstance(detail, dict):
            return str(detail.get("message") or detail.get("error") or detail)
        return str(detail)
