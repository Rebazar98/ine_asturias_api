from __future__ import annotations

from copy import deepcopy

import pytest

from app.services.ign_admin_boundaries import (
    IGNAdministrativeBoundariesLoaderService,
    IGN_ADMIN_BOUNDARY_SOURCE,
    normalize_ign_admin_snapshot,
)


def build_snapshot_payload() -> dict:
    return {
        "type": "FeatureCollection",
        "metadata": {"dataset_version": "ign-asturias-v1"},
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "unit_level": "country",
                    "country_code": "ES",
                    "canonical_name": "Espana",
                    "display_name": "Espana",
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
                    "autonomous_community_code": "03",
                    "canonical_name": "Principado de Asturias",
                    "display_name": "Principado de Asturias",
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
                    "province_code": "33",
                    "autonomous_community_code": "03",
                    "canonical_name": "Asturias",
                    "display_name": "Asturias",
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
                    "municipality_code": "33044",
                    "province_code": "33",
                    "canonical_name": "Oviedo",
                    "display_name": "Oviedo",
                    "provider_name": "Uviéu",
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]],
                },
            },
            {
                "type": "Feature",
                "properties": {
                    "unit_level": "province",
                    "province_code": "28",
                    "autonomous_community_code": "13",
                    "canonical_name": "Madrid",
                    "display_name": "Madrid",
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
                    "municipality_code": "33063",
                    "province_code": "33",
                    "canonical_name": "Mieres",
                    "display_name": "Mieres",
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[0, 0], [1, 1]],
                },
            },
        ],
    }


class FakeIngestionRepository:
    def __init__(self) -> None:
        self.records: list[dict] = []

    async def save_raw(self, **kwargs):
        self.records.append(deepcopy(kwargs))
        return len(self.records)


class FakeTerritorialRepository:
    def __init__(self) -> None:
        self.session = None
        self.lookup: dict[tuple[str, str], dict] = {}
        self.upsert_calls: list[dict] = []

    async def get_unit_by_canonical_code(self, *, unit_level: str, code_value: str):
        return self.lookup.get((unit_level, code_value))

    async def upsert_boundary_unit(self, **kwargs):
        self.upsert_calls.append(deepcopy(kwargs))
        unit_id = len(self.upsert_calls)
        payload = {
            "id": unit_id,
            "unit_level": kwargs["unit_level"],
            "canonical_code": {
                "code_value": kwargs["canonical_code"],
            },
        }
        self.lookup[(kwargs["unit_level"], kwargs["canonical_code"])] = payload
        return {
            "territorial_unit_id": unit_id,
            "unit_level": kwargs["unit_level"],
            "canonical_code": kwargs["canonical_code"],
            "created": True,
        }


def test_normalize_ign_admin_snapshot_selects_asturias_hierarchy_and_wraps_polygons():
    result = normalize_ign_admin_snapshot(build_snapshot_payload())

    assert result["features_found"] == 6
    assert [feature.unit_level for feature in result["features"]] == [
        "country",
        "autonomous_community",
        "province",
        "municipality",
    ]
    assert result["features"][0].geometry_geojson["type"] == "MultiPolygon"
    assert result["features"][3].provider_alias == "Uviéu"
    assert result["incidents"][0]["reason"] == "invalid_feature"


@pytest.mark.anyio
async def test_loader_persists_raw_groups_and_upserts_selected_hierarchy() -> None:
    ingestion_repo = FakeIngestionRepository()
    territorial_repo = FakeTerritorialRepository()
    loader = IGNAdministrativeBoundariesLoaderService(
        ingestion_repo=ingestion_repo,
        territorial_repo=territorial_repo,
    )

    result = await loader.load_snapshot(
        payload=build_snapshot_payload(),
        source_path="tests://ign-asturias.geojson",
        dataset_version="ign-asturias-v1",
        country_code="ES",
        autonomous_community_code="03",
    )

    assert result["source"] == IGN_ADMIN_BOUNDARY_SOURCE
    assert result["features_selected"] == 4
    assert result["features_upserted"] == 4
    assert result["features_rejected"] == 1
    assert result["raw_records_saved"] == 5
    assert [record["source_type"] for record in ingestion_repo.records] == [
        "ign_admin_boundaries_snapshot",
        "ign_admin_boundaries_country",
        "ign_admin_boundaries_autonomous_community",
        "ign_admin_boundaries_province",
        "ign_admin_boundaries_municipality",
    ]
    assert [call["unit_level"] for call in territorial_repo.upsert_calls] == [
        "country",
        "autonomous_community",
        "province",
        "municipality",
    ]
    municipality_call = territorial_repo.upsert_calls[-1]
    assert municipality_call["canonical_code"] == "33044"
    assert municipality_call["provider_alias"] == "Uviéu"
    assert municipality_call["boundary_metadata"]["dataset_version"] == "ign-asturias-v1"
