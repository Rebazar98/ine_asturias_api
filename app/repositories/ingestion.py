from __future__ import annotations

import json
from typing import Any

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.core.metrics import record_raw_ingestion
from app.models import IngestionRaw


class IngestionRepository:
    def __init__(self, session: AsyncSession | None) -> None:
        self.session = session
        self.logger = get_logger("app.repositories.ingestion")

    async def save_raw(
        self,
        source_type: str,
        source_key: str,
        request_path: str,
        request_params: dict[str, Any],
        payload: dict[str, Any] | list[Any],
        max_payload_bytes: int | None = None,
    ) -> int | None:
        if self.session is None:
            self.logger.debug(
                "raw_ingestion_skipped",
                extra={"reason": "database_disabled", "source_type": source_type},
            )
            return None

        payload_to_store = payload
        if max_payload_bytes is not None and max_payload_bytes > 0:
            payload_to_store = self._truncate_payload_if_needed(
                source_type=source_type,
                source_key=source_key,
                request_path=request_path,
                request_params=request_params,
                payload=payload,
                max_payload_bytes=max_payload_bytes,
            )

        record = IngestionRaw(
            source_type=source_type,
            source_key=source_key,
            request_path=request_path,
            request_params=request_params,
            payload=payload_to_store,
        )
        self.session.add(record)

        try:
            await self.session.commit()
            await self.session.refresh(record)
        except SQLAlchemyError:
            await self.session.rollback()
            self.logger.exception(
                "raw_ingestion_failed",
                extra={"source_type": source_type, "source_key": source_key},
            )
            return None

        record_raw_ingestion(source_type)
        self.logger.info(
            "raw_ingestion_saved",
            extra={"source_type": source_type, "source_key": source_key, "record_id": record.id},
        )
        return record.id

    def _truncate_payload_if_needed(
        self,
        *,
        source_type: str,
        source_key: str,
        request_path: str,
        request_params: dict[str, Any],
        payload: dict[str, Any] | list[Any],
        max_payload_bytes: int,
    ) -> dict[str, Any] | list[Any]:
        estimated_bytes = self._estimate_payload_bytes(payload)
        if estimated_bytes <= max_payload_bytes:
            return payload

        if isinstance(payload, list):
            sample_items = payload[:5]
            payload_shape = {
                "payload_type": "list",
                "items_total": len(payload),
                "sample_items": sample_items,
            }
        else:
            payload_shape = {
                "payload_type": "dict",
                "top_level_keys": sorted(payload.keys()),
            }
            if isinstance(payload.get("summary"), dict):
                payload_shape["summary"] = payload["summary"]
            if isinstance(payload.get("warnings"), list):
                payload_shape["warnings_sample"] = payload["warnings"][:5]
            if isinstance(payload.get("errors"), list):
                payload_shape["errors_sample"] = payload["errors"][:5]

        truncated_payload = {
            "payload_truncated": True,
            "raw_payload_bytes_estimated": estimated_bytes,
            "max_payload_bytes": max_payload_bytes,
            "source_type": source_type,
            "source_key": source_key,
            "request_path": request_path,
            "request_params": request_params,
            "payload_shape": payload_shape,
        }
        self.logger.warning(
            "raw_ingestion_payload_truncated",
            extra={
                "source_type": source_type,
                "source_key": source_key,
                "estimated_bytes": estimated_bytes,
                "max_payload_bytes": max_payload_bytes,
            },
        )
        return truncated_payload

    @staticmethod
    def _estimate_payload_bytes(payload: dict[str, Any] | list[Any]) -> int:
        return len(json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"))
