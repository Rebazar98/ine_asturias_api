from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models import IngestionRaw, TerritorialUnit, TerritorialUnitAlias, TerritorialUnitCode
from app.repositories.ingestion import IngestionRepository
from app.repositories.territorial import TerritorialRepository
from app.services.ign_admin_boundaries import IGNAdministrativeBoundariesLoaderService
from tests.integration.postgres import require_integration_postgres


def build_snapshot_payload(suffix: str) -> dict:
    country_code = f"Z{suffix[:1].upper()}"
    community_code = f"9{suffix[:1]}"
    province_code = f"8{suffix[:1]}"
    municipality_code = f"{province_code}001"
    label_suffix = suffix.upper()
    return {
        "type": "FeatureCollection",
        "metadata": {"dataset_version": f"ign-int-{suffix}"},
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "unit_level": "country",
                    "country_code": country_code,
                    "canonical_name": f"Test Country {label_suffix}",
                    "display_name": f"Test Country {label_suffix}",
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [4, 0], [4, 4], [0, 0]]],
                },
            },
            {
                "type": "Feature",
                "properties": {
                    "unit_level": "autonomous_community",
                    "autonomous_community_code": community_code,
                    "canonical_name": f"Test Community {label_suffix}",
                    "display_name": f"Test Community {label_suffix}",
                    "country_code": country_code,
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [3, 0], [3, 3], [0, 0]]],
                },
            },
            {
                "type": "Feature",
                "properties": {
                    "unit_level": "province",
                    "province_code": province_code,
                    "autonomous_community_code": community_code,
                    "canonical_name": f"Test Province {label_suffix}",
                    "display_name": f"Test Province {label_suffix}",
                    "country_code": country_code,
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [2, 0], [2, 2], [0, 0]]],
                },
            },
            {
                "type": "Feature",
                "properties": {
                    "unit_level": "municipality",
                    "municipality_code": municipality_code,
                    "province_code": province_code,
                    "autonomous_community_code": community_code,
                    "canonical_name": f"Test Municipality {label_suffix}",
                    "display_name": f"Test Municipality {label_suffix}",
                    "provider_name": f"Proveedor {label_suffix}",
                    "country_code": country_code,
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]],
                },
            },
        ],
    }


@pytest.mark.integration
def test_ign_admin_loader_roundtrip_with_postgis():
    postgres_dsn = require_integration_postgres()

    async def scenario() -> None:
        engine = create_async_engine(postgres_dsn, future=True)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = uuid4().hex[:4]
        payload = build_snapshot_payload(suffix)
        country_code = payload["features"][0]["properties"]["country_code"]
        community_code = payload["features"][1]["properties"]["autonomous_community_code"]
        province_code = payload["features"][2]["properties"]["province_code"]
        municipality_code = payload["features"][3]["properties"]["municipality_code"]
        dataset_version = payload["metadata"]["dataset_version"]

        try:
            async with session_factory() as session:
                loader = IGNAdministrativeBoundariesLoaderService(
                    ingestion_repo=IngestionRepository(session=session),
                    territorial_repo=TerritorialRepository(session=session),
                )
                first = await loader.load_snapshot(
                    payload=payload,
                    source_path="integration://ign-boundaries",
                    dataset_version=dataset_version,
                    country_code=country_code,
                    autonomous_community_code=community_code,
                )
                second = await loader.load_snapshot(
                    payload=payload,
                    source_path="integration://ign-boundaries",
                    dataset_version=dataset_version,
                    country_code=country_code,
                    autonomous_community_code=community_code,
                )

                assert first["features_upserted"] == 4
                assert second["features_upserted"] == 4

                rows = (
                    await session.execute(
                        select(
                            TerritorialUnitCode.code_type,
                            TerritorialUnitCode.code_value,
                            func.ST_GeometryType(TerritorialUnit.geometry),
                            func.ST_SRID(TerritorialUnit.geometry),
                            func.ST_GeometryType(TerritorialUnit.centroid),
                            func.ST_SRID(TerritorialUnit.centroid),
                        )
                        .join(
                            TerritorialUnit,
                            TerritorialUnit.id == TerritorialUnitCode.territorial_unit_id,
                        )
                        .where(
                            TerritorialUnitCode.code_value.in_(
                                [
                                    country_code,
                                    community_code,
                                    province_code,
                                    municipality_code,
                                ]
                            )
                        )
                    )
                ).all()
                assert len(rows) == 4
                assert {(row[0], row[1]) for row in rows} == {
                    ("alpha2", country_code),
                    ("autonomous_community", community_code),
                    ("province", province_code),
                    ("municipality", municipality_code),
                }
                for row in rows:
                    assert row[2] == "ST_MultiPolygon"
                    assert int(row[3]) == 4326
                    assert row[4] == "ST_Point"
                    assert int(row[5]) == 4326

                point_resolution = await TerritorialRepository(session=session).resolve_point(
                    lat=0.5,
                    lon=0.5,
                )
                assert point_resolution is not None
                assert point_resolution["matched_by"] == "geometry_cover"
                assert point_resolution["coverage"]["boundary_source"] == (
                    "ign_administrative_boundaries"
                )
                assert point_resolution["coverage"]["levels_matched"] == [
                    "country",
                    "autonomous_community",
                    "province",
                    "municipality",
                ]
                assert point_resolution["best_match"]["canonical_code"]["code_value"] == (
                    municipality_code
                )
                assert [item["unit_level"] for item in point_resolution["hierarchy"]] == [
                    "country",
                    "autonomous_community",
                    "province",
                    "municipality",
                ]
                assert point_resolution["ambiguity_detected"] is False

                border_resolution = await TerritorialRepository(session=session).resolve_point(
                    lat=0.0,
                    lon=0.0,
                )
                assert border_resolution is not None
                assert border_resolution["best_match"]["canonical_code"]["code_value"] == (
                    municipality_code
                )

                alias_rows = (
                    (
                        await session.execute(
                            select(TerritorialUnitAlias.alias).where(
                                TerritorialUnitAlias.source_system == "ign_admin"
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                assert len(alias_rows) == 1
                assert alias_rows[0].startswith("Proveedor")

                raw_count = int(
                    (
                        await session.execute(
                            select(func.count(IngestionRaw.id)).where(
                                IngestionRaw.source_type.like("ign_admin_boundaries%")
                            )
                        )
                    )
                    .scalars()
                    .first()
                    or 0
                )
                assert raw_count >= 10

        finally:
            async with session_factory() as session:
                unit_ids = (
                    (
                        await session.execute(
                            select(TerritorialUnitCode.territorial_unit_id).where(
                                TerritorialUnitCode.code_value.in_(
                                    [
                                        country_code,
                                        community_code,
                                        province_code,
                                        municipality_code,
                                    ]
                                )
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                if unit_ids:
                    await session.execute(
                        delete(TerritorialUnitAlias).where(
                            TerritorialUnitAlias.territorial_unit_id.in_(unit_ids)
                        )
                    )
                    await session.execute(
                        delete(TerritorialUnitCode).where(
                            TerritorialUnitCode.territorial_unit_id.in_(unit_ids)
                        )
                    )
                    await session.execute(
                        delete(TerritorialUnit).where(TerritorialUnit.id.in_(unit_ids))
                    )
                await session.execute(
                    delete(IngestionRaw).where(IngestionRaw.source_key.like(f"{dataset_version}%"))
                )
                await session.commit()
            await engine.dispose()

    asyncio.run(scenario())
