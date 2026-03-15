from __future__ import annotations

import json
import re
import unicodedata
from collections.abc import Sequence
from typing import Any

from sqlalchemy import Select, func, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.models import (
    POSTGIS_DEFAULT_SRID,
    TERRITORIAL_BOUNDARY_GEOMETRY_TYPE,
    TERRITORIAL_CENTROID_GEOMETRY_TYPE,
    TerritorialUnit,
    TerritorialUnitAlias,
    TerritorialUnitCode,
)


TERRITORIAL_UNIT_LEVEL_COUNTRY = "country"
TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY = "autonomous_community"
TERRITORIAL_UNIT_LEVEL_PROVINCE = "province"
TERRITORIAL_UNIT_LEVEL_MUNICIPALITY = "municipality"

ISO3166_TERRITORIAL_SOURCE_SYSTEM = "iso3166"
ISO3166_ALPHA2_CODE_TYPE = "alpha2"
INE_TERRITORIAL_SOURCE_SYSTEM = "ine"
INE_TERRITORIAL_CODE_TYPE = "default"
INE_AUTONOMOUS_COMMUNITY_CODE_TYPE = "autonomous_community"
INE_PROVINCE_CODE_TYPE = "province"
INE_MUNICIPALITY_CODE_TYPE = "municipality"

TERRITORIAL_ALIAS_TYPE_CANONICAL_NAME = "canonical_name"
TERRITORIAL_ALIAS_TYPE_DISPLAY_NAME = "display_name"
TERRITORIAL_ALIAS_TYPE_ALTERNATE_NAME = "alternate_name"
TERRITORIAL_ALIAS_TYPE_PROVIDER_NAME = "provider_name"
TERRITORIAL_ALIAS_TYPE_SHORT_NAME = "short_name"

TERRITORIAL_MATCHED_BY_CODE = "code"
TERRITORIAL_MATCHED_BY_ALIAS = "alias"
TERRITORIAL_MATCHED_BY_CANONICAL_NAME = "canonical_name"
TERRITORIAL_DISCOVERY_LEVELS = (
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
)

CANONICAL_TERRITORIAL_CODE_BY_LEVEL = {
    TERRITORIAL_UNIT_LEVEL_COUNTRY: {
        "source_system": ISO3166_TERRITORIAL_SOURCE_SYSTEM,
        "code_type": ISO3166_ALPHA2_CODE_TYPE,
    },
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY: {
        "source_system": INE_TERRITORIAL_SOURCE_SYSTEM,
        "code_type": INE_AUTONOMOUS_COMMUNITY_CODE_TYPE,
    },
    TERRITORIAL_UNIT_LEVEL_PROVINCE: {
        "source_system": INE_TERRITORIAL_SOURCE_SYSTEM,
        "code_type": INE_PROVINCE_CODE_TYPE,
    },
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY: {
        "source_system": INE_TERRITORIAL_SOURCE_SYSTEM,
        "code_type": INE_MUNICIPALITY_CODE_TYPE,
    },
}
POSTGIS_GEOMETRY_TYPE_NAMES = {
    TERRITORIAL_BOUNDARY_GEOMETRY_TYPE: "ST_MultiPolygon",
    TERRITORIAL_CENTROID_GEOMETRY_TYPE: "ST_Point",
}


def get_canonical_code_strategy(unit_level: str) -> dict[str, str] | None:
    return CANONICAL_TERRITORIAL_CODE_BY_LEVEL.get(unit_level)


def normalize_territorial_name(value: str) -> str:
    normalized_value = unicodedata.normalize("NFKD", value or "")
    normalized_value = "".join(
        character for character in normalized_value if not unicodedata.combining(character)
    )
    normalized_value = normalized_value.casefold().replace("_", " ")
    normalized_value = re.sub(r"[^\w\s]", " ", normalized_value)
    normalized_value = re.sub(r"\s+", " ", normalized_value)
    return normalized_value.strip()


class TerritorialRepository:
    def __init__(self, session: AsyncSession | None) -> None:
        self.session = session
        self.logger = get_logger("app.repositories.territorial")

    async def get_unit_by_code(
        self,
        source_system: str,
        code_value: str,
        code_type: str | None = None,
        require_primary: bool = False,
    ) -> dict[str, Any] | None:
        if self.session is None:
            return None

        statement = select(TerritorialUnit, TerritorialUnitCode).join(
            TerritorialUnitCode, TerritorialUnitCode.territorial_unit_id == TerritorialUnit.id
        )
        conditions = [
            TerritorialUnitCode.source_system == source_system,
            TerritorialUnitCode.code_value == code_value,
        ]
        if code_type:
            conditions.append(TerritorialUnitCode.code_type == code_type)
        if require_primary:
            conditions.append(TerritorialUnitCode.is_primary.is_(True))

        statement = statement.where(*conditions)
        result = await self.session.execute(statement)
        row = result.first()
        if row is None:
            return None
        unit, code = row
        return await self._serialize_lookup(unit=unit, matched_code=code)

    async def get_unit_by_ine_code(self, code_value: str) -> dict[str, Any] | None:
        return await self.get_unit_by_code(
            source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
            code_value=code_value,
        )

    async def get_unit_by_canonical_code(
        self,
        unit_level: str,
        code_value: str,
    ) -> dict[str, Any] | None:
        strategy = get_canonical_code_strategy(unit_level)
        if strategy is None:
            return None

        return await self.get_unit_by_code(
            source_system=strategy["source_system"],
            code_type=strategy["code_type"],
            code_value=code_value,
            require_primary=True,
        )

    async def get_unit_by_alias(
        self,
        alias_value: str | None = None,
        *,
        normalized_alias: str | None = None,
        source_system: str | None = None,
        alias_type: str | None = None,
        unit_level: str | None = None,
    ) -> dict[str, Any] | None:
        if self.session is None:
            return None

        normalized_lookup = normalized_alias or normalize_territorial_name(alias_value or "")
        if not normalized_lookup:
            return None

        statement: Select[Any] = (
            select(TerritorialUnit, TerritorialUnitAlias)
            .join(
                TerritorialUnitAlias, TerritorialUnitAlias.territorial_unit_id == TerritorialUnit.id
            )
            .where(TerritorialUnitAlias.normalized_alias == normalized_lookup)
        )
        if source_system:
            statement = statement.where(TerritorialUnitAlias.source_system == source_system)
        if alias_type:
            statement = statement.where(TerritorialUnitAlias.alias_type == alias_type)
        if unit_level:
            statement = statement.where(TerritorialUnit.unit_level == unit_level)

        result = await self.session.execute(statement)
        row = result.first()
        if row is None:
            return None

        unit, alias = row
        return await self._serialize_lookup(unit=unit, matched_alias=alias)

    async def get_unit_by_name(
        self,
        name: str,
        source_system: str | None = None,
        alias_type: str | None = None,
        unit_level: str | None = None,
    ) -> dict[str, Any] | None:
        if self.session is None:
            return None

        normalized_lookup = normalize_territorial_name(name)
        if not normalized_lookup:
            return None

        statement = select(TerritorialUnit).where(
            TerritorialUnit.normalized_name == normalized_lookup
        )
        if unit_level:
            statement = statement.where(TerritorialUnit.unit_level == unit_level)

        result = await self.session.execute(statement.limit(1))
        unit = result.scalars().first()
        if unit is not None:
            return await self._serialize_lookup(
                unit=unit,
                matched_by=TERRITORIAL_MATCHED_BY_CANONICAL_NAME,
            )

        return await self.get_unit_by_alias(
            alias_value=name,
            source_system=source_system,
            alias_type=alias_type,
            unit_level=unit_level,
        )

    async def list_codes(self, territorial_unit_id: int) -> list[dict[str, Any]]:
        if self.session is None:
            return []

        statement = (
            select(TerritorialUnitCode)
            .where(TerritorialUnitCode.territorial_unit_id == territorial_unit_id)
            .order_by(
                TerritorialUnitCode.is_primary.desc(),
                TerritorialUnitCode.source_system.asc(),
                TerritorialUnitCode.code_type.asc(),
                TerritorialUnitCode.code_value.asc(),
            )
        )
        result = await self.session.execute(statement)
        return [self._serialize_code(row) for row in result.scalars().all()]

    async def list_aliases(self, territorial_unit_id: int) -> list[dict[str, Any]]:
        if self.session is None:
            return []

        statement = (
            select(TerritorialUnitAlias)
            .where(TerritorialUnitAlias.territorial_unit_id == territorial_unit_id)
            .order_by(
                TerritorialUnitAlias.alias_type.asc(),
                TerritorialUnitAlias.source_system.asc(),
                TerritorialUnitAlias.alias.asc(),
            )
        )
        result = await self.session.execute(statement)
        return [self._serialize_alias(row, include_id=True) for row in result.scalars().all()]

    async def list_units(
        self,
        *,
        unit_level: str,
        page: int = 1,
        page_size: int = 50,
        country_code: str | None = None,
        parent_id: int | None = None,
        active_only: bool = True,
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
                "filters": {
                    "unit_level": unit_level,
                    "country_code": country_code,
                    "parent_id": parent_id,
                    "active_only": active_only,
                },
            }

        conditions = [TerritorialUnit.unit_level == unit_level]
        if country_code:
            conditions.append(TerritorialUnit.country_code == country_code)
        if parent_id is not None:
            conditions.append(TerritorialUnit.parent_id == parent_id)
        if active_only:
            conditions.append(TerritorialUnit.is_active.is_(True))

        count_statement = select(func.count(TerritorialUnit.id)).where(*conditions)
        count_result = await self.session.execute(count_statement)
        total = int(count_result.scalars().first() or 0)

        statement = (
            select(TerritorialUnit)
            .where(*conditions)
            .order_by(TerritorialUnit.canonical_name.asc(), TerritorialUnit.id.asc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        result = await self.session.execute(statement)
        units = result.scalars().all()
        canonical_codes = await self._get_primary_canonical_codes_for_units(
            unit_ids=[unit.id for unit in units],
            unit_level=unit_level,
        )
        items = [
            self._serialize_unit_summary_payload(unit, canonical_codes.get(unit.id))
            for unit in units
        ]
        pages = (total + page_size - 1) // page_size if total else 0
        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "has_next": page < pages,
            "has_previous": page > 1,
            "filters": {
                "unit_level": unit_level,
                "country_code": country_code,
                "parent_id": parent_id,
                "active_only": active_only,
            },
        }

    async def get_catalog_coverage(self, *, country_code: str = "ES") -> list[dict[str, Any]]:
        coverage_rows: list[dict[str, Any]] = []
        for unit_level in TERRITORIAL_DISCOVERY_LEVELS:
            if self.session is None:
                total = 0
                active_units = 0
                geometry_units = 0
                centroid_units = 0
            else:
                total_statement = select(func.count(TerritorialUnit.id)).where(
                    TerritorialUnit.unit_level == unit_level,
                    TerritorialUnit.country_code == country_code,
                )
                total_result = await self.session.execute(total_statement)
                total = int(total_result.scalars().first() or 0)

                active_statement = select(func.count(TerritorialUnit.id)).where(
                    TerritorialUnit.unit_level == unit_level,
                    TerritorialUnit.country_code == country_code,
                    TerritorialUnit.is_active.is_(True),
                )
                active_result = await self.session.execute(active_statement)
                active_units = int(active_result.scalars().first() or 0)

                geometry_statement = select(func.count(TerritorialUnit.id)).where(
                    TerritorialUnit.unit_level == unit_level,
                    TerritorialUnit.country_code == country_code,
                    TerritorialUnit.geometry.is_not(None),
                )
                geometry_result = await self.session.execute(geometry_statement)
                geometry_units = int(geometry_result.scalars().first() or 0)

                centroid_statement = select(func.count(TerritorialUnit.id)).where(
                    TerritorialUnit.unit_level == unit_level,
                    TerritorialUnit.country_code == country_code,
                    TerritorialUnit.centroid.is_not(None),
                )
                centroid_result = await self.session.execute(centroid_statement)
                centroid_units = int(centroid_result.scalars().first() or 0)

            coverage_rows.append(
                {
                    "unit_level": unit_level,
                    "country_code": country_code,
                    "units_total": total,
                    "active_units": active_units,
                    "geometry_units": geometry_units,
                    "centroid_units": centroid_units,
                    "boundary_source": "ign_administrative_boundaries" if geometry_units else None,
                    "canonical_code_strategy": get_canonical_code_strategy(unit_level),
                }
            )
        return coverage_rows

    async def get_unit_detail_by_canonical_code(
        self,
        *,
        unit_level: str,
        code_value: str,
    ) -> dict[str, Any] | None:
        lookup = await self.get_unit_by_canonical_code(unit_level=unit_level, code_value=code_value)
        if lookup is None or self.session is None:
            return None

        statement = select(TerritorialUnit).where(TerritorialUnit.id == lookup["id"]).limit(1)
        result = await self.session.execute(statement)
        unit = result.scalars().first()
        if unit is None:
            return None

        return await self._serialize_unit_detail(unit)

    async def upsert_boundary_unit(
        self,
        *,
        unit_level: str,
        canonical_code: str,
        canonical_name: str,
        display_name: str,
        country_code: str,
        parent_id: int | None,
        geometry_geojson: dict[str, Any],
        centroid_geojson: dict[str, Any] | None,
        provider_source: str,
        provider_alias: str | None = None,
        provider_alias_type: str = TERRITORIAL_ALIAS_TYPE_PROVIDER_NAME,
        boundary_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self.session is None:
            raise RuntimeError("TerritorialRepository requires a database session.")

        strategy = get_canonical_code_strategy(unit_level)
        if strategy is None:
            raise ValueError(f"Unsupported territorial unit level: {unit_level}.")

        unit = await self._get_unit_model_by_canonical_code(
            unit_level=unit_level,
            code_value=canonical_code,
        )
        if unit is None:
            unit = await self._get_unit_model_by_name_and_parent(
                unit_level=unit_level,
                canonical_name=canonical_name,
                parent_id=parent_id,
            )

        created = unit is None
        if unit is None:
            unit = TerritorialUnit(
                parent_id=parent_id,
                unit_level=unit_level,
                canonical_name=canonical_name,
                normalized_name=normalize_territorial_name(canonical_name),
                display_name=display_name or canonical_name,
                country_code=country_code,
                is_active=True,
                attributes_json={},
            )
            self.session.add(unit)
            await self.session.flush()
        else:
            unit.parent_id = parent_id
            unit.canonical_name = canonical_name
            unit.normalized_name = normalize_territorial_name(canonical_name)
            unit.display_name = display_name or canonical_name
            unit.country_code = country_code
            unit.is_active = True

        await self._ensure_primary_code(
            territorial_unit_id=unit.id,
            source_system=strategy["source_system"],
            code_type=strategy["code_type"],
            code_value=canonical_code,
        )
        if provider_alias:
            await self._ensure_alias(
                territorial_unit_id=unit.id,
                source_system=provider_source,
                alias=provider_alias,
                alias_type=provider_alias_type,
            )

        existing_attributes = dict(getattr(unit, "attributes_json", {}) or {})
        existing_boundary_metadata = dict(existing_attributes.get("boundary_source", {}) or {})
        unit.attributes_json = {
            **existing_attributes,
            "boundary_source": {
                **existing_boundary_metadata,
                **dict(boundary_metadata or {}),
                "provider_source": provider_source,
            },
        }
        await self.session.flush()

        geometry_expr, centroid_expr = await self._build_validated_boundary_expressions(
            geometry_geojson=geometry_geojson,
            centroid_geojson=centroid_geojson,
        )
        await self.session.execute(
            update(TerritorialUnit)
            .where(TerritorialUnit.id == unit.id)
            .values(
                geometry=geometry_expr,
                centroid=centroid_expr,
                updated_at=func.now(),
            )
        )
        await self.session.flush()

        self.logger.info(
            "territorial_boundary_unit_upserted",
            extra={
                "territorial_unit_id": unit.id,
                "unit_level": unit_level,
                "canonical_code": canonical_code,
                "created": created,
            },
        )
        return {
            "territorial_unit_id": unit.id,
            "unit_level": unit_level,
            "canonical_code": canonical_code,
            "created": created,
        }

    async def ping(self) -> bool:
        if self.session is None:
            return False
        try:
            await self.session.execute(select(TerritorialUnit.id).limit(1))
        except SQLAlchemyError:
            return False
        return True

    async def _serialize_lookup(
        self,
        unit: TerritorialUnit,
        matched_code: TerritorialUnitCode | None = None,
        matched_alias: TerritorialUnitAlias | None = None,
        matched_by: str | None = None,
    ) -> dict[str, Any]:
        strategy = get_canonical_code_strategy(unit.unit_level)
        canonical_code = await self._get_canonical_code_for_unit(unit)
        return {
            "id": unit.id,
            "parent_id": unit.parent_id,
            "unit_level": unit.unit_level,
            "canonical_name": unit.canonical_name,
            "display_name": unit.display_name,
            "country_code": unit.country_code,
            "is_active": unit.is_active,
            "canonical_code_strategy": strategy,
            "canonical_code": self._serialize_code(canonical_code),
            "matched_by": matched_by
            or (
                TERRITORIAL_MATCHED_BY_ALIAS
                if matched_alias is not None
                else TERRITORIAL_MATCHED_BY_CODE
            ),
            "matched_code": self._serialize_code(matched_code),
            "matched_alias": self._serialize_alias(matched_alias),
        }

    async def _serialize_unit_summary(self, unit: TerritorialUnit) -> dict[str, Any]:
        return self._serialize_unit_summary_payload(
            unit,
            await self._get_canonical_code_for_unit(unit),
        )

    async def _serialize_unit_detail(self, unit: TerritorialUnit) -> dict[str, Any]:
        summary = await self._serialize_unit_summary(unit)
        summary.update(
            {
                "codes": await self.list_codes(unit.id),
                "aliases": await self.list_aliases(unit.id),
                "attributes": dict(getattr(unit, "attributes_json", {}) or {}),
            }
        )
        return summary

    async def _get_canonical_code_for_unit(
        self,
        unit: TerritorialUnit,
    ) -> TerritorialUnitCode | None:
        if self.session is None:
            return None

        strategy = get_canonical_code_strategy(unit.unit_level)
        if strategy is None:
            return None

        statement = (
            select(TerritorialUnitCode)
            .where(
                TerritorialUnitCode.territorial_unit_id == unit.id,
                TerritorialUnitCode.source_system == strategy["source_system"],
                TerritorialUnitCode.code_type == strategy["code_type"],
                TerritorialUnitCode.is_primary.is_(True),
            )
            .limit(1)
        )
        result = await self.session.execute(statement)
        return result.scalars().first()

    async def _get_primary_canonical_codes_for_units(
        self,
        *,
        unit_ids: Sequence[int],
        unit_level: str,
    ) -> dict[int, TerritorialUnitCode]:
        if self.session is None or not unit_ids:
            return {}

        strategy = get_canonical_code_strategy(unit_level)
        if strategy is None:
            return {}

        statement = select(TerritorialUnitCode).where(
            TerritorialUnitCode.territorial_unit_id.in_(unit_ids),
            TerritorialUnitCode.source_system == strategy["source_system"],
            TerritorialUnitCode.code_type == strategy["code_type"],
            TerritorialUnitCode.is_primary.is_(True),
        )
        result = await self.session.execute(statement)
        return {code.territorial_unit_id: code for code in result.scalars().all()}

    async def _get_unit_model_by_canonical_code(
        self,
        *,
        unit_level: str,
        code_value: str,
    ) -> TerritorialUnit | None:
        if self.session is None:
            return None

        strategy = get_canonical_code_strategy(unit_level)
        if strategy is None:
            return None

        statement = (
            select(TerritorialUnit)
            .join(
                TerritorialUnitCode, TerritorialUnitCode.territorial_unit_id == TerritorialUnit.id
            )
            .where(
                TerritorialUnit.unit_level == unit_level,
                TerritorialUnitCode.source_system == strategy["source_system"],
                TerritorialUnitCode.code_type == strategy["code_type"],
                TerritorialUnitCode.code_value == code_value,
                TerritorialUnitCode.is_primary.is_(True),
            )
            .limit(1)
        )
        result = await self.session.execute(statement)
        return result.scalars().first()

    async def _get_unit_model_by_name_and_parent(
        self,
        *,
        unit_level: str,
        canonical_name: str,
        parent_id: int | None,
    ) -> TerritorialUnit | None:
        if self.session is None:
            return None

        statement = (
            select(TerritorialUnit)
            .where(
                TerritorialUnit.unit_level == unit_level,
                TerritorialUnit.normalized_name == normalize_territorial_name(canonical_name),
                TerritorialUnit.parent_id == parent_id,
            )
            .limit(1)
        )
        result = await self.session.execute(statement)
        return result.scalars().first()

    async def _ensure_primary_code(
        self,
        *,
        territorial_unit_id: int,
        source_system: str,
        code_type: str,
        code_value: str,
    ) -> None:
        if self.session is None:
            return

        statement = (
            select(TerritorialUnitCode)
            .where(
                TerritorialUnitCode.territorial_unit_id == territorial_unit_id,
                TerritorialUnitCode.source_system == source_system,
                TerritorialUnitCode.code_type == code_type,
            )
            .limit(1)
        )
        result = await self.session.execute(statement)
        existing = result.scalars().first()
        if existing is None:
            self.session.add(
                TerritorialUnitCode(
                    territorial_unit_id=territorial_unit_id,
                    source_system=source_system,
                    code_type=code_type,
                    code_value=code_value,
                    is_primary=True,
                )
            )
            return

        existing.code_value = code_value
        existing.is_primary = True

    async def _ensure_alias(
        self,
        *,
        territorial_unit_id: int,
        source_system: str,
        alias: str,
        alias_type: str,
    ) -> None:
        if self.session is None:
            return

        normalized_alias = normalize_territorial_name(alias)
        statement = (
            select(TerritorialUnitAlias)
            .where(
                TerritorialUnitAlias.territorial_unit_id == territorial_unit_id,
                TerritorialUnitAlias.source_system == source_system,
                TerritorialUnitAlias.normalized_alias == normalized_alias,
                TerritorialUnitAlias.alias_type == alias_type,
            )
            .limit(1)
        )
        result = await self.session.execute(statement)
        existing = result.scalars().first()
        if existing is None:
            self.session.add(
                TerritorialUnitAlias(
                    territorial_unit_id=territorial_unit_id,
                    source_system=source_system,
                    alias=alias,
                    normalized_alias=normalized_alias,
                    alias_type=alias_type,
                )
            )
            return

        existing.alias = alias

    async def _build_validated_boundary_expressions(
        self,
        *,
        geometry_geojson: dict[str, Any],
        centroid_geojson: dict[str, Any] | None,
    ) -> tuple[Any, Any]:
        if self.session is None:
            raise RuntimeError("TerritorialRepository requires a database session.")

        geometry_expr = func.ST_Multi(
            func.ST_SetSRID(
                func.ST_GeomFromGeoJSON(json.dumps(geometry_geojson, sort_keys=True)),
                POSTGIS_DEFAULT_SRID,
            )
        )
        centroid_expr = (
            func.ST_SetSRID(
                func.ST_GeomFromGeoJSON(json.dumps(centroid_geojson, sort_keys=True)),
                POSTGIS_DEFAULT_SRID,
            )
            if centroid_geojson is not None
            else func.ST_Centroid(geometry_expr)
        )
        statement = select(
            func.ST_IsValid(geometry_expr),
            func.ST_GeometryType(geometry_expr),
            func.ST_SRID(geometry_expr),
            func.ST_GeometryType(centroid_expr),
            func.ST_SRID(centroid_expr),
        )
        result = await self.session.execute(statement)
        (
            geometry_is_valid,
            geometry_type,
            geometry_srid,
            centroid_type,
            centroid_srid,
        ) = result.first()

        if not geometry_is_valid:
            raise ValueError("Boundary geometry is not valid according to PostGIS.")
        if geometry_type != POSTGIS_GEOMETRY_TYPE_NAMES[TERRITORIAL_BOUNDARY_GEOMETRY_TYPE]:
            raise ValueError(f"Boundary geometry must be {TERRITORIAL_BOUNDARY_GEOMETRY_TYPE}.")
        if int(geometry_srid or 0) != POSTGIS_DEFAULT_SRID:
            raise ValueError(f"Boundary geometry SRID must be {POSTGIS_DEFAULT_SRID}.")
        if centroid_type != POSTGIS_GEOMETRY_TYPE_NAMES[TERRITORIAL_CENTROID_GEOMETRY_TYPE]:
            raise ValueError(f"Boundary centroid must be {TERRITORIAL_CENTROID_GEOMETRY_TYPE}.")
        if int(centroid_srid or 0) != POSTGIS_DEFAULT_SRID:
            raise ValueError(f"Boundary centroid SRID must be {POSTGIS_DEFAULT_SRID}.")

        return geometry_expr, centroid_expr

    @staticmethod
    def _serialize_unit_summary_payload(
        unit: TerritorialUnit,
        canonical_code: TerritorialUnitCode | None,
    ) -> dict[str, Any]:
        return {
            "id": unit.id,
            "parent_id": unit.parent_id,
            "unit_level": unit.unit_level,
            "canonical_name": unit.canonical_name,
            "display_name": unit.display_name,
            "country_code": unit.country_code,
            "is_active": unit.is_active,
            "canonical_code_strategy": get_canonical_code_strategy(unit.unit_level),
            "canonical_code": TerritorialRepository._serialize_code(canonical_code),
        }

    @staticmethod
    def _serialize_code(code: TerritorialUnitCode | None) -> dict[str, Any] | None:
        if code is None:
            return None

        return {
            "source_system": code.source_system,
            "code_type": code.code_type,
            "code_value": code.code_value,
            "is_primary": code.is_primary,
        }

    @staticmethod
    def _serialize_alias(
        alias: TerritorialUnitAlias | None,
        *,
        include_id: bool = False,
    ) -> dict[str, Any] | None:
        if alias is None:
            return None

        payload = {
            "source_system": alias.source_system,
            "alias": alias.alias,
            "normalized_alias": alias.normalized_alias,
            "alias_type": alias.alias_type,
        }
        if include_id:
            payload["id"] = alias.id
        return payload
