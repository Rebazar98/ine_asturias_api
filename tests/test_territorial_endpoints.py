import time

from app.dependencies import get_arq_pool
from app.repositories.territorial import (
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
)
from app.schemas import NormalizedSeriesItem
from app.settings import get_settings


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
        "resources_total": 9,
        "territorial_levels_total": 3,
        "read_resources_total": 6,
        "analytics_resources_total": 2,
        "job_resources_total": 1,
    }
    assert payload["metadata"] == {
        "default_country_code": "ES",
        "intended_consumers": ["n8n", "agents", "programmatic_clients"],
        "raw_provider_contracts_exposed": False,
        "discovery_scope": "published_territorial_resources",
        "official_sources": ["ine", "cartociudad", "ign_administrative_boundaries"],
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
        "territorial.ign_administrative_boundaries.catalog",
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
