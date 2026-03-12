from app.repositories.territorial import (
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
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
