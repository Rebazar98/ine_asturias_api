from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.models import TerritorialUnit, TerritorialUnitAlias, TerritorialUnitCode


class TerritorialRepository:
    def __init__(self, session: AsyncSession | None) -> None:
        self.session = session
        self.logger = get_logger("app.repositories.territorial")

    async def get_unit_by_code(self, source_system: str, code_value: str) -> dict[str, Any] | None:
        if self.session is None:
            return None

        statement = (
            select(TerritorialUnit, TerritorialUnitCode)
            .join(TerritorialUnitCode, TerritorialUnitCode.territorial_unit_id == TerritorialUnit.id)
            .where(
                TerritorialUnitCode.source_system == source_system,
                TerritorialUnitCode.code_value == code_value,
            )
        )
        result = await self.session.execute(statement)
        row = result.first()
        if row is None:
            return None
        unit, code = row
        return {
            "id": unit.id,
            "parent_id": unit.parent_id,
            "unit_level": unit.unit_level,
            "canonical_name": unit.canonical_name,
            "display_name": unit.display_name,
            "country_code": unit.country_code,
            "is_active": unit.is_active,
            "code": {
                "source_system": code.source_system,
                "code_type": code.code_type,
                "code_value": code.code_value,
                "is_primary": code.is_primary,
            },
        }

    async def list_aliases(self, territorial_unit_id: int) -> list[dict[str, Any]]:
        if self.session is None:
            return []

        statement = select(TerritorialUnitAlias).where(TerritorialUnitAlias.territorial_unit_id == territorial_unit_id)
        result = await self.session.execute(statement)
        return [
            {
                "id": row.id,
                "source_system": row.source_system,
                "alias": row.alias,
                "normalized_alias": row.normalized_alias,
                "alias_type": row.alias_type,
            }
            for row in result.scalars().all()
        ]

    async def ping(self) -> bool:
        if self.session is None:
            return False
        try:
            await self.session.execute(select(TerritorialUnit.id).limit(1))
        except SQLAlchemyError:
            return False
        return True
