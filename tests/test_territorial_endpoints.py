import io
import json
import time
from zipfile import ZipFile

from app.dependencies import get_arq_pool
from app.repositories.territorial import (
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
)
from app.schemas import NormalizedSeriesItem
from app.settings import get_settings


def _territorial_summary(
    *,
    unit_id: int,
    parent_id: int | None,
    unit_level: str,
    canonical_name: str,
    display_name: str,
    code_type: str,
    code_value: str,
    source_system: str = "ine",
) -> dict:
    return {
        "id": unit_id,
        "parent_id": parent_id,
        "unit_level": unit_level,
        "canonical_name": canonical_name,
        "display_name": display_name,
        "country_code": "ES",
        "is_active": True,
        "canonical_code_strategy": {
            "source_system": source_system,
            "code_type": code_type,
        },
        "canonical_code": {
            "source_system": source_system,
            "code_type": code_type,
            "code_value": code_value,
            "is_primary": True,
        },
    }


def _territorial_detail(
    *,
    unit_id: int,
    parent_id: int | None,
    unit_level: str,
    canonical_name: str,
    display_name: str,
    code_type: str,
    code_value: str,
    source_system: str = "ine",
) -> dict:
    payload = _territorial_summary(
        unit_id=unit_id,
        parent_id=parent_id,
        unit_level=unit_level,
        canonical_name=canonical_name,
        display_name=display_name,
        code_type=code_type,
        code_value=code_value,
        source_system=source_system,
    )
    payload["codes"] = [payload["canonical_code"]]
    payload["aliases"] = []
    payload["attributes"] = {
        "population_scope": "municipal" if unit_level == "municipality" else "regional"
    }
    return payload


def _seed_municipality_export_context(dummy_territorial_repo, dummy_series_repo) -> None:
    municipality_detail = _territorial_detail(
        unit_id=33044,
        parent_id=33,
        unit_level="municipality",
        canonical_name="Oviedo",
        display_name="Oviedo",
        code_type="municipality",
        code_value="33044",
    )
    dummy_territorial_repo.detail_by_canonical_code[
        (TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, "33044")
    ] = municipality_detail
    dummy_territorial_repo.detail_by_id[33044] = municipality_detail
    dummy_territorial_repo.hierarchy_by_unit_id[33044] = [
        _territorial_summary(
            unit_id=1,
            parent_id=None,
            unit_level="country",
            canonical_name="Espana",
            display_name="Espana",
            code_type="alpha2",
            code_value="ES",
            source_system="iso3166",
        ),
        _territorial_summary(
            unit_id=2,
            parent_id=1,
            unit_level="autonomous_community",
            canonical_name="Asturias",
            display_name="Principado de Asturias",
            code_type="autonomous_community",
            code_value="03",
        ),
        _territorial_summary(
            unit_id=33,
            parent_id=2,
            unit_level="province",
            canonical_name="Asturias",
            display_name="Asturias",
            code_type="province",
            code_value="33",
        ),
        _territorial_summary(
            unit_id=33044,
            parent_id=33,
            unit_level="municipality",
            canonical_name="Oviedo",
            display_name="Oviedo",
            code_type="municipality",
            code_value="33044",
        ),
    ]
    dummy_series_repo.items.extend(
        [
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP_TOTAL",
                geography_name="Oviedo",
                geography_code="33044",
                period="2024",
                value=220543,
                unit="personas",
                metadata={"series_name": "Poblacion total"},
            ),
            NormalizedSeriesItem(
                operation_code="22",
                table_id="3901",
                variable_id="AGEING_INDEX",
                geography_name="Oviedo",
                geography_code="33044",
                period="2024M01",
                value=142.5,
                unit="indice",
                metadata={"series_name": "Indice de envejecimiento"},
            ),
        ]
    )


def _seed_autonomous_community_export_context(dummy_territorial_repo, dummy_series_repo) -> None:
    community_detail = _territorial_detail(
        unit_id=2,
        parent_id=1,
        unit_level="autonomous_community",
        canonical_name="Asturias",
        display_name="Principado de Asturias",
        code_type="autonomous_community",
        code_value="03",
    )
    dummy_territorial_repo.detail_by_canonical_code[
        (TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY, "03")
    ] = community_detail
    dummy_territorial_repo.detail_by_id[2] = community_detail
    dummy_territorial_repo.hierarchy_by_unit_id[2] = [
        _territorial_summary(
            unit_id=1,
            parent_id=None,
            unit_level="country",
            canonical_name="Espana",
            display_name="Espana",
            code_type="alpha2",
            code_value="ES",
            source_system="iso3166",
        ),
        _territorial_summary(
            unit_id=2,
            parent_id=1,
            unit_level="autonomous_community",
            canonical_name="Asturias",
            display_name="Principado de Asturias",
            code_type="autonomous_community",
            code_value="03",
        ),
    ]
    dummy_series_repo.items.extend(
        [
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP_TOTAL",
                geography_name="Asturias",
                geography_code="03",
                period="2024",
                value=1011792,
                unit="personas",
                metadata={"series_name": "Poblacion total"},
            ),
            NormalizedSeriesItem(
                operation_code="22",
                table_id="3901",
                variable_id="AGEING_INDEX",
                geography_name="Asturias",
                geography_code="03",
                period="2024M01",
                value=142.5,
                unit="indice",
                metadata={"series_name": "Indice de envejecimiento"},
            ),
        ]
    )


def test_list_autonomous_communities_returns_paginated_results(client, dummy_territorial_repo):
    dummy_territorial_repo.units_by_level[TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY] = [
        {
            "id": 1,
            "parent_id": None,
            "unit_level": "autonomous_community",
            "canonical_name": "Asturias",
            "display_name": "Principado de Asturias",
            "country_code": "ES",
            "is_active": True,
            "canonical_code_strategy": {
                "source_system": "ine",
                "code_type": "autonomous_community",
            },
            "canonical_code": {
                "source_system": "ine",
                "code_type": "autonomous_community",
                "code_value": "03",
                "is_primary": True,
            },
        },
        {
            "id": 2,
            "parent_id": None,
            "unit_level": "autonomous_community",
            "canonical_name": "Madrid",
            "display_name": "Comunidad de Madrid",
            "country_code": "ES",
            "is_active": True,
            "canonical_code_strategy": {
                "source_system": "ine",
                "code_type": "autonomous_community",
            },
            "canonical_code": {
                "source_system": "ine",
                "code_type": "autonomous_community",
                "code_value": "13",
                "is_primary": True,
            },
        },
    ]

    response = client.get("/territorios/comunidades-autonomas?page=1&page_size=1")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert payload["page"] == 1
    assert payload["page_size"] == 1
    assert payload["pages"] == 2
    assert payload["has_next"] is True
    assert payload["has_previous"] is False
    assert payload["filters"] == {
        "unit_level": "autonomous_community",
        "country_code": "ES",
        "parent_id": None,
        "active_only": True,
    }
    assert len(payload["items"]) == 1
    assert payload["items"][0]["canonical_name"] == "Asturias"


def test_list_provinces_supports_autonomous_community_filter(client, dummy_territorial_repo):
    dummy_territorial_repo.by_canonical_code[
        (TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY, "03")
    ] = {
        "id": 1,
        "unit_level": "autonomous_community",
        "canonical_name": "Asturias",
        "matched_by": "code",
        "canonical_code": {
            "source_system": "ine",
            "code_type": "autonomous_community",
            "code_value": "03",
            "is_primary": True,
        },
    }
    dummy_territorial_repo.units_by_level[TERRITORIAL_UNIT_LEVEL_PROVINCE] = [
        {
            "id": 33,
            "parent_id": 1,
            "unit_level": "province",
            "canonical_name": "Asturias",
            "display_name": "Asturias",
            "country_code": "ES",
            "is_active": True,
            "canonical_code_strategy": {"source_system": "ine", "code_type": "province"},
            "canonical_code": {
                "source_system": "ine",
                "code_type": "province",
                "code_value": "33",
                "is_primary": True,
            },
        },
        {
            "id": 28,
            "parent_id": 2,
            "unit_level": "province",
            "canonical_name": "Madrid",
            "display_name": "Madrid",
            "country_code": "ES",
            "is_active": True,
            "canonical_code_strategy": {"source_system": "ine", "code_type": "province"},
            "canonical_code": {
                "source_system": "ine",
                "code_type": "province",
                "code_value": "28",
                "is_primary": True,
            },
        },
    ]

    response = client.get("/territorios/provincias?autonomous_community_code=03")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["canonical_code"]["code_value"] == "33"
    assert payload["filters"]["parent_id"] == 1


def test_list_provinces_returns_404_when_autonomous_community_is_unknown(
    client, dummy_territorial_repo
):
    response = client.get("/territorios/provincias?autonomous_community_code=99")

    assert response.status_code == 404
    assert response.json()["detail"]["message"] == "Autonomous community code was not found."


def test_get_municipio_by_codigo_ine_returns_detail(client, dummy_territorial_repo):
    dummy_territorial_repo.detail_by_canonical_code[
        (TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, "33044")
    ] = {
        "id": 33044,
        "parent_id": 33,
        "unit_level": "municipality",
        "canonical_name": "Oviedo",
        "display_name": "Oviedo",
        "country_code": "ES",
        "is_active": True,
        "canonical_code_strategy": {"source_system": "ine", "code_type": "municipality"},
        "canonical_code": {
            "source_system": "ine",
            "code_type": "municipality",
            "code_value": "33044",
            "is_primary": True,
        },
        "codes": [
            {
                "source_system": "ine",
                "code_type": "municipality",
                "code_value": "33044",
                "is_primary": True,
            }
        ],
        "aliases": [
            {
                "source_system": "internal",
                "alias": "Uvieu",
                "normalized_alias": "uvieu",
                "alias_type": "alternate_name",
            }
        ],
        "attributes": {"population_scope": "municipal"},
    }

    response = client.get("/municipio/33044")

    assert response.status_code == 200
    payload = response.json()
    assert payload["canonical_name"] == "Oviedo"
    assert payload["canonical_code"]["code_value"] == "33044"
    assert payload["codes"][0]["code_value"] == "33044"
    assert payload["aliases"][0]["alias"] == "Uvieu"
    assert payload["attributes"]["population_scope"] == "municipal"


def test_get_municipio_by_codigo_ine_returns_404_when_missing(client, dummy_territorial_repo):
    response = client.get("/municipio/99999")

    assert response.status_code == 404
    assert response.json()["detail"]["message"] == "Municipality code was not found."


def test_get_territorial_catalog_exposes_resources_and_basic_coverage(
    client, dummy_territorial_repo
):
    dummy_territorial_repo.units_by_level[TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY] = [
        {
            "id": 1,
            "parent_id": None,
            "unit_level": "autonomous_community",
            "canonical_name": "Asturias",
            "display_name": "Principado de Asturias",
            "country_code": "ES",
            "is_active": True,
        }
    ]
    dummy_territorial_repo.units_by_level[TERRITORIAL_UNIT_LEVEL_PROVINCE] = [
        {
            "id": 33,
            "parent_id": 1,
            "unit_level": "province",
            "canonical_name": "Asturias",
            "display_name": "Asturias",
            "country_code": "ES",
            "is_active": True,
            "has_geometry": True,
            "has_centroid": True,
        }
    ]
    dummy_territorial_repo.units_by_level[TERRITORIAL_UNIT_LEVEL_MUNICIPALITY] = [
        {
            "id": 33044,
            "parent_id": 33,
            "unit_level": "municipality",
            "canonical_name": "Oviedo",
            "display_name": "Oviedo",
            "country_code": "ES",
            "is_active": True,
            "has_geometry": True,
            "has_centroid": True,
        },
        {
            "id": 33024,
            "parent_id": 33,
            "unit_level": "municipality",
            "canonical_name": "Gijon",
            "display_name": "Gijon",
            "country_code": "ES",
            "is_active": False,
        },
    ]

    response = client.get("/territorios/catalogo")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "internal.catalog.territorial"
    assert payload["summary"] == {
        "resources_total": 13,
        "territorial_levels_total": 3,
        "read_resources_total": 7,
        "analytics_resources_total": 2,
        "job_resources_total": 4,
    }
    assert payload["metadata"] == {
        "default_country_code": "ES",
        "intended_consumers": ["n8n", "agents", "programmatic_clients"],
        "raw_provider_contracts_exposed": False,
        "discovery_scope": "published_territorial_resources",
        "official_sources": [
            "ine",
            "cartociudad",
            "ign_administrative_boundaries",
            "catastro_urbano",
        ],
    }
    assert [level["unit_level"] for level in payload["territorial_levels"]] == [
        "autonomous_community",
        "province",
        "municipality",
    ]
    municipality_level = next(
        level for level in payload["territorial_levels"] if level["unit_level"] == "municipality"
    )
    assert municipality_level["units_total"] == 2
    assert municipality_level["active_units"] == 1
    assert municipality_level["geometry_units"] == 1
    assert municipality_level["centroid_units"] == 1
    assert municipality_level["boundary_source"] == "ign_administrative_boundaries"
    assert municipality_level["detail_path"] == "/municipio/{codigo_ine}"
    assert municipality_level["summary_path"] == "/territorios/municipio/{codigo_ine}/resumen"
    assert municipality_level["report_job_path"] == "/territorios/municipio/{codigo_ine}/informe"
    assert municipality_level["canonical_code_strategy"] == {
        "source_system": "ine",
        "code_type": "municipality",
    }
    resource_keys = {resource["resource_key"] for resource in payload["resources"]}
    assert resource_keys == {
        "territorial.autonomous_communities.list",
        "territorial.provinces.list",
        "territorial.geocode.query",
        "territorial.reverse_geocode.query",
        "territorial.resolve_point.query",
        "territorial.ign_administrative_boundaries.catalog",
        "territorial.export.job",
        "territorial.export.status",
        "territorial.export.download",
        "territorial.municipality.detail",
        "territorial.municipality.summary",
        "territorial.municipality.report_job",
        "territorial.jobs.status",
    }
    geocode_resource = next(
        resource
        for resource in payload["resources"]
        if resource["resource_key"] == "territorial.geocode.query"
    )
    assert geocode_resource["query_params"] == ["query"]
    assert geocode_resource["response_contract"] == "GeocodeResponse"
    report_job_resource = next(
        resource
        for resource in payload["resources"]
        if resource["resource_key"] == "territorial.municipality.report_job"
    )
    assert report_job_resource["supports_background_job"] is True
    assert report_job_resource["supports_snapshot_reuse"] is True
    assert report_job_resource["query_params"] == [
        "operation_code",
        "variable_id",
        "period_from",
        "period_to",
        "page",
        "page_size",
    ]
    export_job_resource = next(
        resource
        for resource in payload["resources"]
        if resource["resource_key"] == "territorial.export.job"
    )
    assert export_job_resource["supports_background_job"] is True
    export_download_resource = next(
        resource
        for resource in payload["resources"]
        if resource["resource_key"] == "territorial.export.download"
    )
    assert export_download_resource["response_contract"] == "application/zip"


def test_resolve_point_returns_best_match_and_hierarchy(client, dummy_territorial_repo):
    dummy_territorial_repo.point_resolution_payload = {
        "matched_by": "geometry_cover",
        "best_match": {
            "id": 33044,
            "parent_id": 33,
            "unit_level": "municipality",
            "canonical_name": "Oviedo",
            "display_name": "Oviedo",
            "country_code": "ES",
            "is_active": True,
            "canonical_code_strategy": {
                "source_system": "ine",
                "code_type": "municipality",
            },
            "canonical_code": {
                "source_system": "ine",
                "code_type": "municipality",
                "code_value": "33044",
                "is_primary": True,
            },
        },
        "hierarchy": [
            {
                "id": 1,
                "parent_id": None,
                "unit_level": "country",
                "canonical_name": "Espana",
                "display_name": "Espana",
                "country_code": "ES",
                "is_active": True,
                "canonical_code_strategy": {
                    "source_system": "iso3166",
                    "code_type": "alpha2",
                },
                "canonical_code": {
                    "source_system": "iso3166",
                    "code_type": "alpha2",
                    "code_value": "ES",
                    "is_primary": True,
                },
            },
            {
                "id": 2,
                "parent_id": 1,
                "unit_level": "autonomous_community",
                "canonical_name": "Asturias",
                "display_name": "Principado de Asturias",
                "country_code": "ES",
                "is_active": True,
                "canonical_code_strategy": {
                    "source_system": "ine",
                    "code_type": "autonomous_community",
                },
                "canonical_code": {
                    "source_system": "ine",
                    "code_type": "autonomous_community",
                    "code_value": "03",
                    "is_primary": True,
                },
            },
            {
                "id": 33,
                "parent_id": 2,
                "unit_level": "province",
                "canonical_name": "Asturias",
                "display_name": "Asturias",
                "country_code": "ES",
                "is_active": True,
                "canonical_code_strategy": {
                    "source_system": "ine",
                    "code_type": "province",
                },
                "canonical_code": {
                    "source_system": "ine",
                    "code_type": "province",
                    "code_value": "33",
                    "is_primary": True,
                },
            },
            {
                "id": 33044,
                "parent_id": 33,
                "unit_level": "municipality",
                "canonical_name": "Oviedo",
                "display_name": "Oviedo",
                "country_code": "ES",
                "is_active": True,
                "canonical_code_strategy": {
                    "source_system": "ine",
                    "code_type": "municipality",
                },
                "canonical_code": {
                    "source_system": "ine",
                    "code_type": "municipality",
                    "code_value": "33044",
                    "is_primary": True,
                },
            },
        ],
        "coverage": {
            "boundary_source": "ign_administrative_boundaries",
            "levels_considered": [
                "country",
                "autonomous_community",
                "province",
                "municipality",
            ],
            "levels_matched": [
                "country",
                "autonomous_community",
                "province",
                "municipality",
            ],
        },
        "ambiguity_detected": False,
        "ambiguity_by_level": {},
    }

    response = client.get("/territorios/resolve-point?lat=43.3614&lon=-5.8494")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "internal.territorial.point_resolution"
    assert payload["query_coordinates"] == {"lat": 43.3614, "lon": -5.8494}
    assert payload["result"]["matched_by"] == "geometry_cover"
    assert payload["result"]["best_match"]["canonical_code"]["code_value"] == "33044"
    assert payload["result"]["coverage"]["boundary_source"] == "ign_administrative_boundaries"
    assert [item["unit_level"] for item in payload["result"]["hierarchy"]] == [
        "country",
        "autonomous_community",
        "province",
        "municipality",
    ]
    assert payload["metadata"] == {
        "ambiguity_detected": False,
        "ambiguity_by_level": {},
    }


def test_resolve_point_returns_no_coverage_when_no_boundaries_are_loaded(
    client, dummy_territorial_repo
):
    dummy_territorial_repo.point_resolution_payload = None

    response = client.get("/territorios/resolve-point?lat=43.3614&lon=-5.8494")

    assert response.status_code == 200
    assert response.json() == {
        "source": "internal.territorial.point_resolution",
        "query_coordinates": {"lat": 43.3614, "lon": -5.8494},
        "result": None,
        "metadata": {"reason": "no_boundary_coverage_loaded"},
    }


def test_resolve_point_returns_outside_loaded_coverage_when_point_has_no_match(
    client, dummy_territorial_repo
):
    dummy_territorial_repo.point_resolution_payload = {
        "matched_by": "geometry_cover",
        "best_match": None,
        "hierarchy": [],
        "coverage": {
            "boundary_source": "ign_administrative_boundaries",
            "levels_considered": [
                "country",
                "autonomous_community",
                "province",
                "municipality",
            ],
            "levels_matched": [],
        },
        "ambiguity_detected": False,
        "ambiguity_by_level": {},
    }

    response = client.get("/territorios/resolve-point?lat=36.0000&lon=-7.0000")

    assert response.status_code == 200
    payload = response.json()
    assert payload["result"] is None
    assert payload["metadata"] == {"reason": "outside_loaded_coverage"}


def test_resolve_point_validates_coordinate_ranges(client):
    response = client.get("/territorios/resolve-point?lat=93&lon=-5.8494")
    assert response.status_code == 422

    response = client.get("/territorios/resolve-point?lat=43.3614&lon=181")
    assert response.status_code == 422


def test_get_municipality_summary_returns_semantic_analytical_response(
    client, dummy_territorial_repo, dummy_series_repo
):
    dummy_territorial_repo.detail_by_canonical_code[
        (TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, "33044")
    ] = {
        "id": 33044,
        "parent_id": 33,
        "unit_level": "municipality",
        "canonical_name": "Oviedo",
        "display_name": "Oviedo",
        "country_code": "ES",
        "is_active": True,
        "canonical_code_strategy": {"source_system": "ine", "code_type": "municipality"},
        "canonical_code": {
            "source_system": "ine",
            "code_type": "municipality",
            "code_value": "33044",
            "is_primary": True,
        },
        "codes": [
            {
                "source_system": "ine",
                "code_type": "municipality",
                "code_value": "33044",
                "is_primary": True,
            }
        ],
        "aliases": [],
        "attributes": {"population_scope": "municipal"},
    }
    dummy_series_repo.items.extend(
        [
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP_TOTAL",
                geography_name="Oviedo",
                geography_code="33044",
                period="2023",
                value=219910,
                unit="personas",
                metadata={"series_name": "Poblacion total"},
            ),
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP_TOTAL",
                geography_name="Oviedo",
                geography_code="33044",
                period="2024",
                value=220543,
                unit="personas",
                metadata={"series_name": "Poblacion total"},
            ),
            NormalizedSeriesItem(
                operation_code="22",
                table_id="3901",
                variable_id="AGEING_INDEX",
                geography_name="Oviedo",
                geography_code="33044",
                period="2024M01",
                value=142.5,
                unit="indice",
                metadata={"series_name": "Indice de envejecimiento"},
            ),
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP_TOTAL",
                geography_name="Gijon",
                geography_code="33024",
                period="2024",
                value=268313,
                unit="personas",
                metadata={"series_name": "Poblacion total"},
            ),
        ]
    )

    response = client.get("/territorios/municipio/33044/resumen?page=1&page_size=10")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "internal.analytics.municipality_summary"
    assert payload["territorial_context"] == {
        "territorial_unit_id": 33044,
        "unit_level": "municipality",
        "canonical_code": "33044",
        "canonical_name": "Oviedo",
        "display_name": "Oviedo",
        "source_system": "ine",
        "country_code": "ES",
        "autonomous_community_code": None,
        "province_code": None,
        "municipality_code": "33044",
    }
    assert payload["territorial_unit"]["canonical_name"] == "Oviedo"
    assert payload["filters"] == {
        "municipality_code": "33044",
        "geography_code_system": "ine",
        "operation_code": None,
        "variable_id": None,
        "period_from": None,
        "period_to": None,
        "page": 1,
        "page_size": 10,
    }
    assert payload["summary"] == {
        "indicators_total": 2,
        "indicators_returned": 2,
        "operation_codes": ["22"],
        "latest_period": "2024M01",
    }
    assert payload["pagination"] == {
        "total": 2,
        "page": 1,
        "page_size": 10,
        "pages": 1,
        "has_next": False,
        "has_previous": False,
    }
    assert {item["series_key"] for item in payload["series"]} == {
        "ine.22.2852.POP_TOTAL",
        "ine.22.3901.AGEING_INDEX",
    }
    pop_total = next(item for item in payload["series"] if item["variable_id"] == "POP_TOTAL")
    assert pop_total["label"] == "Poblacion total"
    assert pop_total["value"] == 220543
    assert payload["metadata"]["dataset"] == "ine_series_normalized"


def test_get_municipality_summary_returns_404_when_municipality_is_unknown(
    client, dummy_territorial_repo
):
    response = client.get("/territorios/municipio/99999/resumen")

    assert response.status_code == 404
    assert response.json()["detail"]["message"] == "Municipality code was not found."


def test_create_municipality_report_job_completes_and_exposes_structured_result(
    client, dummy_territorial_repo, dummy_series_repo
):
    dummy_territorial_repo.detail_by_canonical_code[
        (TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, "33044")
    ] = {
        "id": 33044,
        "parent_id": 33,
        "unit_level": "municipality",
        "canonical_name": "Oviedo",
        "display_name": "Oviedo",
        "country_code": "ES",
        "is_active": True,
        "canonical_code_strategy": {"source_system": "ine", "code_type": "municipality"},
        "canonical_code": {
            "source_system": "ine",
            "code_type": "municipality",
            "code_value": "33044",
            "is_primary": True,
        },
        "codes": [
            {
                "source_system": "ine",
                "code_type": "municipality",
                "code_value": "33044",
                "is_primary": True,
            }
        ],
        "aliases": [],
        "attributes": {"population_scope": "municipal"},
    }
    dummy_series_repo.items.extend(
        [
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP_TOTAL",
                geography_name="Oviedo",
                geography_code="33044",
                period="2024",
                value=220543,
                unit="personas",
                metadata={"series_name": "Poblacion total"},
            ),
            NormalizedSeriesItem(
                operation_code="22",
                table_id="3901",
                variable_id="AGEING_INDEX",
                geography_name="Oviedo",
                geography_code="33044",
                period="2024M01",
                value=142.5,
                unit="indice",
                metadata={"series_name": "Indice de envejecimiento"},
            ),
        ]
    )

    response = client.post("/territorios/municipio/33044/informe?page=1&page_size=10")

    assert response.status_code == 202
    accepted = response.json()
    assert accepted["job_type"] == "territorial_municipality_report"
    assert accepted["report_type"] == "municipality_report"
    assert accepted["municipality_code"] == "33044"

    job_payload = None
    for _ in range(50):
        status_response = client.get(accepted["status_path"])
        assert status_response.status_code == 200
        job_payload = status_response.json()
        if job_payload["status"] == "completed":
            break
        time.sleep(0.02)

    assert job_payload is not None
    assert job_payload["status"] == "completed"
    assert job_payload["progress"]["stage"] in {"assembling_report", "report_completed"}
    assert job_payload["result"]["report_type"] == "municipality_report"
    assert job_payload["result"]["territorial_unit"]["canonical_name"] == "Oviedo"
    assert [section["section_key"] for section in job_payload["result"]["sections"]] == [
        "territorial_profile",
        "latest_indicators",
    ]
    assert job_payload["result"]["metadata"]["storage_mode"] == "job_store_only"


def test_get_territorial_job_status_returns_404_when_missing(client):
    response = client.get("/territorios/jobs/job-missing")

    assert response.status_code == 404
    assert response.json()["detail"]["message"] == "Job not found."


def test_create_municipality_report_job_fails_when_municipality_is_unknown(
    client, dummy_territorial_repo
):
    response = client.post("/territorios/municipio/99999/informe")

    assert response.status_code == 202
    accepted = response.json()
    assert accepted["report_type"] == "municipality_report"

    job_payload = None
    for _ in range(50):
        status_response = client.get(accepted["status_path"])
        assert status_response.status_code == 200
        job_payload = status_response.json()
        if job_payload["status"] == "failed":
            break
        time.sleep(0.02)

    assert job_payload is not None
    assert job_payload["status"] == "failed"
    assert job_payload["error"]["message"] == "Municipality code was not found."
    assert job_payload["error"]["codigo_ine"] == "99999"


def test_create_municipality_report_job_reuses_persisted_snapshot(
    client, dummy_territorial_repo, dummy_series_repo, dummy_analytical_snapshot_repo
):
    dummy_territorial_repo.detail_by_canonical_code[
        (TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, "33044")
    ] = {
        "id": 33044,
        "parent_id": 33,
        "unit_level": "municipality",
        "canonical_name": "Oviedo",
        "display_name": "Oviedo",
        "country_code": "ES",
        "is_active": True,
        "canonical_code_strategy": {"source_system": "ine", "code_type": "municipality"},
        "canonical_code": {
            "source_system": "ine",
            "code_type": "municipality",
            "code_value": "33044",
            "is_primary": True,
        },
        "codes": [
            {
                "source_system": "ine",
                "code_type": "municipality",
                "code_value": "33044",
                "is_primary": True,
            }
        ],
        "aliases": [],
        "attributes": {"population_scope": "municipal"},
    }
    dummy_series_repo.items.extend(
        [
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP_TOTAL",
                geography_name="Oviedo",
                geography_code="33044",
                period="2024",
                value=220543,
                unit="personas",
                metadata={"series_name": "Poblacion total"},
            ),
            NormalizedSeriesItem(
                operation_code="22",
                table_id="3901",
                variable_id="AGEING_INDEX",
                geography_name="Oviedo",
                geography_code="33044",
                period="2024M01",
                value=142.5,
                unit="indice",
                metadata={"series_name": "Indice de envejecimiento"},
            ),
        ]
    )

    first_response = client.post("/territorios/municipio/33044/informe?page=1&page_size=10")
    assert first_response.status_code == 202

    first_job = None
    for _ in range(50):
        status_response = client.get(first_response.json()["status_path"])
        assert status_response.status_code == 200
        first_job = status_response.json()
        if first_job["status"] == "completed":
            break
        time.sleep(0.02)

    second_response = client.post("/territorios/municipio/33044/informe?page=1&page_size=10")
    assert second_response.status_code == 202

    second_job = None
    for _ in range(50):
        status_response = client.get(second_response.json()["status_path"])
        assert status_response.status_code == 200
        second_job = status_response.json()
        if second_job["status"] == "completed":
            break
        time.sleep(0.02)

    assert first_job is not None
    assert second_job is not None
    assert first_job["result"]["metadata"]["storage_mode"] == "persistent_snapshot"
    assert first_job["result"]["metadata"]["snapshot_reused"] is False
    assert second_job["result"]["metadata"]["storage_mode"] == "persistent_snapshot"
    assert second_job["result"]["metadata"]["snapshot_reused"] is True
    assert (
        first_job["result"]["metadata"]["snapshot_key"]
        == second_job["result"]["metadata"]["snapshot_key"]
    )
    assert dummy_analytical_snapshot_repo.upsert_calls == 1
    assert dummy_series_repo.latest_indicator_calls == 1


def test_create_municipality_report_job_uses_configured_queue_name(
    client, dummy_territorial_repo, dummy_series_repo
):
    class DummyArqPool:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        async def enqueue_job(self, function_name: str, *args: object, **kwargs: object) -> None:
            self.calls.append(
                {
                    "function_name": function_name,
                    "args": args,
                    "kwargs": kwargs,
                }
            )

    dummy_territorial_repo.detail_by_canonical_code[
        (TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, "33044")
    ] = {
        "id": 33044,
        "parent_id": 33,
        "unit_level": "municipality",
        "canonical_name": "Oviedo",
        "display_name": "Oviedo",
        "country_code": "ES",
        "is_active": True,
        "canonical_code_strategy": {"source_system": "ine", "code_type": "municipality"},
        "canonical_code": {
            "source_system": "ine",
            "code_type": "municipality",
            "code_value": "33044",
            "is_primary": True,
        },
        "codes": [
            {
                "source_system": "ine",
                "code_type": "municipality",
                "code_value": "33044",
                "is_primary": True,
            }
        ],
        "aliases": [],
        "attributes": {"population_scope": "municipal"},
    }
    dummy_series_repo.items.append(
        NormalizedSeriesItem(
            operation_code="22",
            table_id="2852",
            variable_id="POP_TOTAL",
            geography_name="Oviedo",
            geography_code="33044",
            period="2024",
            value=220543,
            unit="personas",
            metadata={"series_name": "Poblacion total"},
        )
    )
    arq_pool = DummyArqPool()
    client.app.dependency_overrides[get_arq_pool] = lambda: arq_pool

    response = client.post("/territorios/municipio/33044/informe?page=1&page_size=10")

    assert response.status_code == 202
    assert len(arq_pool.calls) == 1
    call = arq_pool.calls[0]
    assert call["function_name"] == "run_municipality_report_job"
    assert call["kwargs"]["_queue_name"] == get_settings().job_queue_name


def test_create_territorial_export_job_completes_and_downloads_municipality_bundle(
    client,
    dummy_territorial_repo,
    dummy_series_repo,
    dummy_territorial_export_artifact_repo,
):
    _seed_municipality_export_context(dummy_territorial_repo, dummy_series_repo)

    response = client.post(
        "/territorios/export",
        json={
            "unit_level": "municipality",
            "code_value": "33044",
            "format": "zip",
            "include_providers": ["territorial", "ine", "analytics"],
        },
    )

    assert response.status_code == 202
    accepted = response.json()
    assert accepted["job_type"] == "territorial_export"
    assert accepted["status_path"].startswith("/territorios/exports/")

    job_payload = None
    for _ in range(50):
        status_response = client.get(accepted["status_path"])
        assert status_response.status_code == 200
        job_payload = status_response.json()
        if job_payload["status"] == "completed":
            break
        time.sleep(0.02)

    assert job_payload is not None
    assert job_payload["status"] == "completed"
    assert job_payload["result"]["format"] == "zip"
    assert job_payload["result"]["download_path"] == (
        f"/territorios/exports/{accepted['job_id']}/download"
    )
    assert job_payload["result"]["summary"]["artifact_reused"] is False

    download_response = client.get(job_payload["result"]["download_path"])
    assert download_response.status_code == 200
    assert download_response.headers["content-type"].startswith("application/zip")
    assert (
        "territorial_export_municipality_33044.zip"
        in download_response.headers["content-disposition"]
    )

    with ZipFile(io.BytesIO(download_response.content)) as archive:
        names = sorted(archive.namelist())
        assert names == [
            "datasets/analytics_municipality_report.json",
            "datasets/analytics_municipality_summary.json",
            "datasets/ine_series.ndjson",
            "datasets/territorial_hierarchy.json",
            "datasets/territorial_unit.json",
            "manifest.json",
        ]
        manifest = json.loads(archive.read("manifest.json"))
        assert manifest["providers_requested"] == ["territorial", "ine", "analytics"]
        assert set(manifest["providers_included"]) == {"territorial", "ine", "analytics"}
    assert dummy_territorial_export_artifact_repo.upsert_calls == 1


def test_create_territorial_export_job_completes_for_autonomous_community(
    client,
    dummy_territorial_repo,
    dummy_series_repo,
    dummy_territorial_export_artifact_repo,
):
    _seed_autonomous_community_export_context(dummy_territorial_repo, dummy_series_repo)

    response = client.post(
        "/territorios/export",
        json={
            "unit_level": "autonomous_community",
            "code_value": "03",
            "format": "zip",
            "include_providers": ["territorial", "ine", "analytics"],
        },
    )

    assert response.status_code == 202
    accepted = response.json()

    job_payload = None
    for _ in range(50):
        status_response = client.get(accepted["status_path"])
        assert status_response.status_code == 200
        job_payload = status_response.json()
        if job_payload["status"] == "completed":
            break
        time.sleep(0.02)

    assert job_payload is not None
    assert job_payload["status"] == "completed"
    assert job_payload["result"]["territorial_context"]["autonomous_community_code"] == "03"
    assert job_payload["result"]["summary"]["providers_included"] == ["ine", "territorial"]

    download_response = client.get(job_payload["result"]["download_path"])
    assert download_response.status_code == 200
    with ZipFile(io.BytesIO(download_response.content)) as archive:
        names = sorted(archive.namelist())
        assert names == [
            "datasets/ine_series.ndjson",
            "datasets/territorial_hierarchy.json",
            "datasets/territorial_unit.json",
            "manifest.json",
        ]
        manifest = json.loads(archive.read("manifest.json"))
        datasets_by_key = {item["dataset_key"]: item for item in manifest["datasets"]}
        assert datasets_by_key["analytics_municipality_summary"]["applicable"] is False
        assert datasets_by_key["analytics_municipality_report"]["applicable"] is False
    assert dummy_territorial_export_artifact_repo.upsert_calls == 1


def test_create_territorial_export_job_completes_with_catastro_bundle(
    client,
    dummy_territorial_repo,
    dummy_series_repo,
    dummy_ingestion_repo,
    dummy_catastro_cache_repo,
    dummy_catastro_client_service,
    dummy_territorial_export_artifact_repo,
):
    _seed_municipality_export_context(dummy_territorial_repo, dummy_series_repo)

    response = client.post(
        "/territorios/export",
        json={
            "unit_level": "municipality",
            "code_value": "33044",
            "format": "zip",
            "include_providers": ["territorial", "ine", "analytics", "catastro"],
        },
    )

    assert response.status_code == 202
    accepted = response.json()
    job_payload = None
    for _ in range(50):
        status_response = client.get(accepted["status_path"])
        assert status_response.status_code == 200
        job_payload = status_response.json()
        if job_payload["status"] == "completed":
            break
        time.sleep(0.02)

    assert job_payload is not None
    assert job_payload["status"] == "completed"
    download_response = client.get(job_payload["result"]["download_path"])
    assert download_response.status_code == 200
    with ZipFile(io.BytesIO(download_response.content)) as archive:
        names = sorted(archive.namelist())
        assert "datasets/catastro_municipality_aggregates.json" in names
        manifest = json.loads(archive.read("manifest.json"))
        assert manifest["providers_requested"] == ["territorial", "ine", "analytics", "catastro"]
        catastro_payload = json.loads(
            archive.read("datasets/catastro_municipality_aggregates.json").decode("utf-8")
        )
        assert catastro_payload["source"] == "catastro.municipality.aggregates"
        assert catastro_payload["metadata"]["cache_status"] == "miss"
    assert len(dummy_ingestion_repo.records) == 1
    assert dummy_catastro_cache_repo.upsert_calls == 1
    assert len(dummy_catastro_client_service.calls) == 1


def test_create_territorial_export_job_fails_when_catastro_provider_fails(
    client,
    dummy_territorial_repo,
    dummy_series_repo,
    dummy_catastro_client_service,
):
    from app.services.catastro_client import CatastroUpstreamError

    _seed_municipality_export_context(dummy_territorial_repo, dummy_series_repo)
    dummy_catastro_client_service.raise_error = CatastroUpstreamError(
        status_code=503,
        detail={
            "message": "The Catastro service is temporarily unavailable.",
            "path": "/jaxi/tabla.do",
            "retryable": True,
        },
    )

    response = client.post(
        "/territorios/export",
        json={
            "unit_level": "municipality",
            "code_value": "33044",
            "format": "zip",
            "include_providers": ["territorial", "catastro"],
        },
    )

    assert response.status_code == 202
    accepted = response.json()
    job_payload = None
    for _ in range(50):
        status_response = client.get(accepted["status_path"])
        assert status_response.status_code == 200
        job_payload = status_response.json()
        if job_payload["status"] == "failed":
            break
        time.sleep(0.02)

    assert job_payload is not None
    assert job_payload["status"] == "failed"
    assert job_payload["error"]["message"] == "The Catastro service is temporarily unavailable."


def test_create_territorial_export_job_fails_when_unit_is_unknown(client):
    response = client.post(
        "/territorios/export",
        json={
            "unit_level": "municipality",
            "code_value": "99999",
            "format": "zip",
            "include_providers": ["territorial", "ine", "analytics"],
        },
    )

    assert response.status_code == 202
    accepted = response.json()

    job_payload = None
    for _ in range(50):
        status_response = client.get(accepted["status_path"])
        assert status_response.status_code == 200
        job_payload = status_response.json()
        if job_payload["status"] == "failed":
            break
        time.sleep(0.02)

    assert job_payload is not None
    assert job_payload["status"] == "failed"
    assert job_payload["error"]["message"] == "Territorial unit code was not found."
    assert job_payload["error"]["code_value"] == "99999"


def test_download_territorial_export_returns_404_while_job_is_not_completed(
    client,
    dummy_territorial_repo,
    dummy_series_repo,
):
    class DummyArqPool:
        async def enqueue_job(self, function_name: str, *args: object, **kwargs: object) -> None:
            return None

    _seed_municipality_export_context(dummy_territorial_repo, dummy_series_repo)
    client.app.dependency_overrides[get_arq_pool] = lambda: DummyArqPool()

    response = client.post(
        "/territorios/export",
        json={
            "unit_level": "municipality",
            "code_value": "33044",
            "format": "zip",
            "include_providers": ["territorial", "ine", "analytics"],
        },
    )

    assert response.status_code == 202
    accepted = response.json()

    download_response = client.get(f"/territorios/exports/{accepted['job_id']}/download")
    assert download_response.status_code == 404
    assert download_response.json()["detail"]["message"] == (
        "Territorial export artifact is not available yet."
    )


def test_create_territorial_export_job_reuses_fresh_artifact(
    client,
    dummy_territorial_repo,
    dummy_series_repo,
    dummy_territorial_export_artifact_repo,
):
    _seed_municipality_export_context(dummy_territorial_repo, dummy_series_repo)

    first_response = client.post(
        "/territorios/export",
        json={
            "unit_level": "municipality",
            "code_value": "33044",
            "format": "zip",
            "include_providers": ["territorial", "ine", "analytics"],
        },
    )
    second_response = client.post(
        "/territorios/export",
        json={
            "unit_level": "municipality",
            "code_value": "33044",
            "format": "zip",
            "include_providers": ["analytics", "territorial", "ine"],
        },
    )

    assert first_response.status_code == 202
    assert second_response.status_code == 202

    first_job = None
    for _ in range(50):
        status_response = client.get(first_response.json()["status_path"])
        assert status_response.status_code == 200
        first_job = status_response.json()
        if first_job["status"] == "completed":
            break
        time.sleep(0.02)

    second_job = None
    for _ in range(50):
        status_response = client.get(second_response.json()["status_path"])
        assert status_response.status_code == 200
        second_job = status_response.json()
        if second_job["status"] == "completed":
            break
        time.sleep(0.02)

    assert first_job is not None
    assert second_job is not None
    assert first_job["result"]["export_key"] == second_job["result"]["export_key"]
    assert first_job["result"]["summary"]["artifact_reused"] is False
    assert second_job["result"]["summary"]["artifact_reused"] is True
    assert dummy_territorial_export_artifact_repo.upsert_calls == 1
