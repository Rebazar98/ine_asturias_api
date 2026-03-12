from __future__ import annotations

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
    ) -> int | None:
        if self.session is None:
            self.logger.debug(
                "raw_ingestion_skipped",
                extra={"reason": "database_disabled", "source_type": source_type},
            )
            return None

        record = IngestionRaw(
            source_type=source_type,
            source_key=source_key,
            request_path=request_path,
            request_params=request_params,
            payload=payload,
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
