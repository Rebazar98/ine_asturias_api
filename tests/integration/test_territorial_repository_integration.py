from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models import TerritorialUnit, TerritorialUnitAlias, TerritorialUnitCode
from app.repositories.territorial import (
    INE_MUNICIPALITY_CODE_TYPE,
    INE_TERRITORIAL_SOURCE_SYSTEM,
    TERRITORIAL_ALIAS_TYPE_ALTERNATE_NAME,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TerritorialRepository,
    normalize_territorial_name,
)
from tests.integration.postgres import require_integration_postgres


@pytest.mark.integration
def test_territorial_repository_roundtrip_with_postgres():
    postgres_dsn = require_integration_postgres()

    async def scenario() -> None:
        engine = create_async_engine(postgres_dsn, future=True)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = uuid4().hex[:8]
        canonical_name = f"Oviedo Integration {suffix}"
        alias_name = f"Uvieu Integracion {suffix}"
        code_value = f"33044-int-{suffix}"
        unit_id: int | None = None

        try:
            async with session_factory() as session:
                unit = TerritorialUnit(
                    parent_id=None,
                    unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
                    canonical_name=canonical_name,
                    normalized_name=normalize_territorial_name(canonical_name),
                    display_name=canonical_name,
                    country_code="ES",
                    is_active=True,
                    attributes_json={},
                )
                session.add(unit)
                await session.flush()
                unit_id = unit.id
                session.add(
                    TerritorialUnitCode(
                        territorial_unit_id=unit.id,
                        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
                        code_type=INE_MUNICIPALITY_CODE_TYPE,
                        code_value=code_value,
                        is_primary=True,
                    )
                )
                session.add(
                    TerritorialUnitAlias(
                        territorial_unit_id=unit.id,
                        source_system="internal",
                        alias=alias_name,
                        normalized_alias=normalize_territorial_name(alias_name),
                        alias_type=TERRITORIAL_ALIAS_TYPE_ALTERNATE_NAME,
                    )
                )
                await session.commit()

            async with session_factory() as session:
                repository = TerritorialRepository(session=session)

                by_code = await repository.get_unit_by_canonical_code(
                    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
                    code_value,
                )
                by_name = await repository.get_unit_by_name(
                    canonical_name,
                    unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
                )
                by_alias = await repository.get_unit_by_name(
                    alias_name,
                    source_system="internal",
                    alias_type=TERRITORIAL_ALIAS_TYPE_ALTERNATE_NAME,
                    unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
                )
                aliases = await repository.list_aliases(unit_id)

                assert by_code is not None
                assert by_code["canonical_code"]["code_value"] == code_value
                assert by_code["matched_by"] == "code"

                assert by_name is not None
                assert by_name["matched_by"] == "canonical_name"
                assert by_name["canonical_name"] == canonical_name

                assert by_alias is not None
                assert by_alias["matched_by"] == "alias"
                assert by_alias["matched_alias"]["normalized_alias"] == normalize_territorial_name(
                    alias_name
                )

                assert len(aliases) == 1
                assert aliases[0]["alias_type"] == TERRITORIAL_ALIAS_TYPE_ALTERNATE_NAME

        finally:
            async with session_factory() as session:
                if unit_id is not None:
                    await session.execute(
                        delete(TerritorialUnitAlias).where(
                            TerritorialUnitAlias.territorial_unit_id == unit_id
                        )
                    )
                    await session.execute(
                        delete(TerritorialUnitCode).where(
                            TerritorialUnitCode.territorial_unit_id == unit_id
                        )
                    )
                    await session.execute(
                        delete(TerritorialUnit).where(TerritorialUnit.id == unit_id)
                    )
                    await session.commit()
            await engine.dispose()

    asyncio.run(scenario())
