from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger


LAYER_TERRITORIAL_UNITS = "territorial_units"
LAYER_IDEAS_FEATURES = "ideas_features_normalized"

ERROR_INVALID_GEOMETRY = "invalid_geometry"
ERROR_MISSING_GEOMETRY = "missing_geometry"
ERROR_OVERLAP = "overlap"
ERROR_INVALID_SRID = "invalid_srid"

SEVERITY_ERROR = "error"
SEVERITY_WARNING = "warning"


class CartographicQAService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.logger = get_logger("app.services.cartographic_qa")

    async def validate_territorial_units(self, unit_ids: list[int]) -> list[dict[str, Any]]:
        if not unit_ids:
            return []

        incidents: list[dict[str, Any]] = []
        id_list = list(unit_ids)

        # Missing geometry
        missing_result = await self.session.execute(
            text("""
                SELECT id, unit_level, canonical_code
                FROM territorial_units
                WHERE id = ANY(:ids)
                  AND geometry IS NULL
            """),
            {"ids": id_list},
        )
        for row in missing_result.mappings():
            incidents.append(
                {
                    "layer": LAYER_TERRITORIAL_UNITS,
                    "entity_id": str(row["id"]),
                    "error_type": ERROR_MISSING_GEOMETRY,
                    "severity": SEVERITY_ERROR,
                    "description": f"{row['unit_level']} {row['canonical_code']}: geometry is NULL",
                    "source_provider": "ign",
                    "metadata": {
                        "unit_level": row["unit_level"],
                        "canonical_code": row["canonical_code"],
                    },
                }
            )

        # Invalid geometry
        invalid_result = await self.session.execute(
            text("""
                SELECT id, unit_level, canonical_code
                FROM territorial_units
                WHERE id = ANY(:ids)
                  AND geometry IS NOT NULL
                  AND NOT ST_IsValid(geometry)
            """),
            {"ids": id_list},
        )
        for row in invalid_result.mappings():
            incidents.append(
                {
                    "layer": LAYER_TERRITORIAL_UNITS,
                    "entity_id": str(row["id"]),
                    "error_type": ERROR_INVALID_GEOMETRY,
                    "severity": SEVERITY_ERROR,
                    "description": (
                        f"{row['unit_level']} {row['canonical_code']}: ST_IsValid returned false"
                    ),
                    "source_provider": "ign",
                    "metadata": {
                        "unit_level": row["unit_level"],
                        "canonical_code": row["canonical_code"],
                    },
                }
            )

        # Overlapping units of the same level (within the validated set only)
        if len(id_list) > 1:
            overlap_result = await self.session.execute(
                text("""
                    SELECT a.id AS id_a, b.id AS id_b,
                           a.unit_level,
                           a.canonical_code AS code_a,
                           b.canonical_code AS code_b
                    FROM territorial_units a
                    JOIN territorial_units b
                      ON a.unit_level = b.unit_level AND a.id < b.id
                    WHERE a.id = ANY(:ids)
                      AND b.id = ANY(:ids)
                      AND a.geometry IS NOT NULL
                      AND b.geometry IS NOT NULL
                      AND ST_Overlaps(a.geometry, b.geometry)
                """),
                {"ids": id_list},
            )
            for row in overlap_result.mappings():
                incidents.append(
                    {
                        "layer": LAYER_TERRITORIAL_UNITS,
                        "entity_id": f"{row['id_a']}:{row['id_b']}",
                        "error_type": ERROR_OVERLAP,
                        "severity": SEVERITY_WARNING,
                        "description": (
                            f"{row['unit_level']} {row['code_a']} overlaps {row['code_b']}"
                        ),
                        "source_provider": "ign",
                        "metadata": {
                            "unit_level": row["unit_level"],
                            "id_a": row["id_a"],
                            "id_b": row["id_b"],
                            "code_a": row["code_a"],
                            "code_b": row["code_b"],
                        },
                    }
                )

        self.logger.info(
            "qa_territorial_units_validated",
            extra={"unit_ids": len(unit_ids), "incidents": len(incidents)},
        )
        return incidents

    async def validate_ideas_features(self, feature_ids: list[int]) -> list[dict[str, Any]]:
        if not feature_ids:
            return []

        incidents: list[dict[str, Any]] = []
        id_list = list(feature_ids)

        # Invalid geometry
        invalid_result = await self.session.execute(
            text("""
                SELECT id, layer_name, feature_id
                FROM ideas_features_normalized
                WHERE id = ANY(:ids)
                  AND geometry IS NOT NULL
                  AND NOT ST_IsValid(geometry)
            """),
            {"ids": id_list},
        )
        for row in invalid_result.mappings():
            incidents.append(
                {
                    "layer": LAYER_IDEAS_FEATURES,
                    "entity_id": str(row["id"]),
                    "error_type": ERROR_INVALID_GEOMETRY,
                    "severity": SEVERITY_ERROR,
                    "description": (
                        f"Layer {row['layer_name']} feature {row['feature_id']}: "
                        "ST_IsValid returned false"
                    ),
                    "source_provider": "ideas",
                    "metadata": {
                        "layer_name": row["layer_name"],
                        "feature_id": row["feature_id"],
                    },
                }
            )

        # Wrong SRID
        srid_result = await self.session.execute(
            text("""
                SELECT id, layer_name, feature_id, ST_SRID(geometry) AS srid
                FROM ideas_features_normalized
                WHERE id = ANY(:ids)
                  AND geometry IS NOT NULL
                  AND ST_SRID(geometry) != 4326
            """),
            {"ids": id_list},
        )
        for row in srid_result.mappings():
            incidents.append(
                {
                    "layer": LAYER_IDEAS_FEATURES,
                    "entity_id": str(row["id"]),
                    "error_type": ERROR_INVALID_SRID,
                    "severity": SEVERITY_WARNING,
                    "description": (
                        f"Layer {row['layer_name']} feature {row['feature_id']}: "
                        f"SRID is {row['srid']}, expected 4326"
                    ),
                    "source_provider": "ideas",
                    "metadata": {
                        "layer_name": row["layer_name"],
                        "feature_id": row["feature_id"],
                        "srid": row["srid"],
                    },
                }
            )

        self.logger.info(
            "qa_ideas_features_validated",
            extra={"feature_ids": len(feature_ids), "incidents": len(incidents)},
        )
        return incidents

    async def validate_attributes(self, layer: str, entity_ids: list[str]) -> list[dict[str, Any]]:
        """Attribute coherence rules: NOT NULL checks on key fields."""
        # Placeholder — attribute rules are layer-specific and extended per Fase C+ needs.
        return []
