from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.models import TerritorialUnit, TerritorialUnitCode
from app.repositories.series import SeriesRepository
from app.repositories.territorial import (
    INE_MUNICIPALITY_CODE_TYPE,
    INE_TERRITORIAL_SOURCE_SYSTEM,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    normalize_territorial_name,
)
from app.schemas import NormalizedSeriesItem


DEFAULT_SEED_MUNICIPALITY_NAMES = {
    "33044": "Oviedo",
}


@dataclass(frozen=True, slots=True)
class SeededMunicipalityAnalyticsContext:
    municipality_code: str
    municipality_name: str
    territorial_unit_id: int
    created_unit: bool
    normalized_rows_upserted: int


def default_seed_municipality_name(municipality_code: str) -> str:
    return DEFAULT_SEED_MUNICIPALITY_NAMES.get(
        municipality_code,
        f"Municipio {municipality_code}",
    )


async def ensure_municipality_analytics_seed(
    session: AsyncSession,
    *,
    municipality_code: str,
    municipality_name: str | None = None,
) -> SeededMunicipalityAnalyticsContext:
    logger = get_logger("app.services.territorial_seed")
    unit = await _get_municipality_unit(session, municipality_code=municipality_code)
    created_unit = False

    if unit is None:
        resolved_name = municipality_name or default_seed_municipality_name(municipality_code)
        unit = TerritorialUnit(
            parent_id=None,
            unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
            canonical_name=resolved_name,
            normalized_name=normalize_territorial_name(resolved_name),
            display_name=resolved_name,
            country_code="ES",
            is_active=True,
            attributes_json={
                "seed_source": "local_analytics_smoke",
                "population_scope": "municipal",
            },
        )
        session.add(unit)
        await session.flush()
        session.add(
            TerritorialUnitCode(
                territorial_unit_id=unit.id,
                source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
                code_type=INE_MUNICIPALITY_CODE_TYPE,
                code_value=municipality_code,
                is_primary=True,
            )
        )
        created_unit = True
    else:
        resolved_name = unit.canonical_name

    series_repository = SeriesRepository(session=session)
    normalized_rows_upserted = await series_repository.upsert_many(
        [
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP_TOTAL",
                territorial_unit_id=unit.id,
                geography_name=resolved_name,
                geography_code=municipality_code,
                period="2024",
                value=220543.0,
                unit="personas",
                metadata={
                    "series_name": "Poblacion total",
                    "seed_source": "local_analytics_smoke",
                },
                raw_payload={"seed_source": "local_analytics_smoke"},
            ),
            NormalizedSeriesItem(
                operation_code="22",
                table_id="3901",
                variable_id="AGEING_INDEX",
                territorial_unit_id=unit.id,
                geography_name=resolved_name,
                geography_code=municipality_code,
                period="2024M01",
                value=142.5,
                unit="indice",
                metadata={
                    "series_name": "Indice de envejecimiento",
                    "seed_source": "local_analytics_smoke",
                },
                raw_payload={"seed_source": "local_analytics_smoke"},
            ),
        ]
    )

    logger.info(
        "municipality_analytics_seed_ensured",
        extra={
            "municipality_code": municipality_code,
            "municipality_name": resolved_name,
            "territorial_unit_id": unit.id,
            "created_unit": created_unit,
            "normalized_rows_upserted": normalized_rows_upserted,
        },
    )
    return SeededMunicipalityAnalyticsContext(
        municipality_code=municipality_code,
        municipality_name=resolved_name,
        territorial_unit_id=unit.id,
        created_unit=created_unit,
        normalized_rows_upserted=normalized_rows_upserted,
    )


async def _get_municipality_unit(
    session: AsyncSession,
    *,
    municipality_code: str,
) -> TerritorialUnit | None:
    statement = (
        select(TerritorialUnit)
        .join(TerritorialUnitCode, TerritorialUnitCode.territorial_unit_id == TerritorialUnit.id)
        .where(
            TerritorialUnit.unit_level == TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
            TerritorialUnitCode.source_system == INE_TERRITORIAL_SOURCE_SYSTEM,
            TerritorialUnitCode.code_type == INE_MUNICIPALITY_CODE_TYPE,
            TerritorialUnitCode.code_value == municipality_code,
            TerritorialUnitCode.is_primary.is_(True),
        )
        .limit(1)
    )
    result = await session.execute(statement)
    return result.scalars().first()
