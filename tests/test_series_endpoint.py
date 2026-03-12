from app.schemas import NormalizedSeriesItem


def test_ine_series_endpoint_returns_paginated_filtered_results(
    client, dummy_series_repo, dummy_territorial_repo
):
    dummy_series_repo.items.extend(
        [
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP",
                geography_name="Principado de Asturias",
                geography_code="33",
                period="2021",
                value=1011792,
                unit="personas",
                metadata={"series_name": "Poblacion"},
            ),
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP",
                geography_name="Principado de Asturias",
                geography_code="33",
                period="2020",
                value=1018784,
                unit="personas",
                metadata={"series_name": "Poblacion"},
            ),
            NormalizedSeriesItem(
                operation_code="33",
                table_id="1377",
                variable_id="IPC",
                geography_name="Madrid",
                geography_code="28",
                period="2024M01",
                value=101.5,
                unit="indice",
                metadata={"series_name": "Indice"},
            ),
        ]
    )

    response = client.get("/ine/series?operation_code=22&geography_code=33&page=1&page_size=1")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert payload["page"] == 1
    assert payload["page_size"] == 1
    assert payload["pages"] == 2
    assert payload["has_next"] is True
    assert payload["has_previous"] is False
    assert payload["filters"] == {
        "operation_code": "22",
        "table_id": None,
        "geography_code": "33",
        "geography_name": None,
        "geography_code_system": "ine",
        "variable_id": None,
        "period_from": None,
        "period_to": None,
    }
    assert len(payload["items"]) == 1
    assert payload["items"][0]["operation_code"] == "22"
    assert payload["items"][0]["geography_code"] == "33"
    assert payload["territorial_resolution"] is None


def test_ine_series_endpoint_supports_case_insensitive_geography_name_filter(
    client, dummy_series_repo, dummy_territorial_repo
):
    dummy_series_repo.items.extend(
        [
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP",
                geography_name="Principado de Asturias",
                geography_code="33",
                period="2021",
                value=1011792,
                unit="personas",
                metadata={"series_name": "Poblacion"},
            ),
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP",
                geography_name="Madrid",
                geography_code="28",
                period="2021",
                value=6666666,
                unit="personas",
                metadata={"series_name": "Poblacion"},
            ),
        ]
    )
    dummy_territorial_repo.by_name["principado de asturias"] = {
        "canonical_name": "Asturias",
        "matched_by": "alias",
        "canonical_code": {
            "source_system": "ine",
            "code_type": "province",
            "code_value": "33",
            "is_primary": True,
        },
    }

    response = client.get(
        "/ine/series?geography_name=principado%20de%20asturias&page=1&page_size=10"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["filters"]["geography_name"] is None
    assert payload["filters"]["geography_code"] == "33"
    assert payload["filters"]["geography_code_system"] == "ine"
    assert payload["items"][0]["geography_code"] == "33"
    assert payload["territorial_resolution"] == {
        "input_name": "principado de asturias",
        "resolved_geography_code": "33",
        "matched_by": "alias",
        "canonical_name": "Asturias",
        "source_system": "ine",
    }


def test_ine_series_endpoint_resolves_canonical_territorial_name(
    client, dummy_series_repo, dummy_territorial_repo
):
    dummy_series_repo.items.extend(
        [
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP",
                geography_name="Principado de Asturias",
                geography_code="33",
                period="2021",
                value=1011792,
                unit="personas",
                metadata={"series_name": "Poblacion"},
            ),
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP",
                geography_name="Madrid",
                geography_code="28",
                period="2021",
                value=6666666,
                unit="personas",
                metadata={"series_name": "Poblacion"},
            ),
        ]
    )
    dummy_territorial_repo.by_name["Asturias"] = {
        "canonical_name": "Asturias",
        "matched_by": "canonical_name",
        "canonical_code": {
            "source_system": "ine",
            "code_type": "province",
            "code_value": "33",
            "is_primary": True,
        },
    }

    response = client.get("/ine/series?geography_name=Asturias&page=1&page_size=10")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["filters"]["geography_name"] is None
    assert payload["filters"]["geography_code"] == "33"
    assert payload["territorial_resolution"] == {
        "input_name": "Asturias",
        "resolved_geography_code": "33",
        "matched_by": "canonical_name",
        "canonical_name": "Asturias",
        "source_system": "ine",
    }


def test_ine_series_endpoint_validates_period_range(client, dummy_territorial_repo):
    response = client.get("/ine/series?period_from=2024&period_to=2023")

    assert response.status_code == 422
    payload = response.json()
    assert payload["detail"]["message"] == "period_from cannot be greater than period_to."


def test_ine_series_endpoint_validates_unsupported_geography_code_system(
    client, dummy_territorial_repo
):
    response = client.get("/ine/series?geography_code_system=cartociudad")

    assert response.status_code == 422
    payload = response.json()
    assert payload["detail"]["message"] == "Unsupported geography_code_system."
    assert payload["detail"]["supported_values"] == ["ine"]


def test_ine_series_endpoint_validates_pagination_params(client, dummy_territorial_repo):
    response = client.get("/ine/series?page=0&page_size=500")

    assert response.status_code == 422
