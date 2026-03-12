from app.schemas import NormalizedSeriesItem


def test_ine_series_endpoint_returns_paginated_filtered_results(client, dummy_series_repo):
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
    assert len(payload["items"]) == 1
    assert payload["items"][0]["operation_code"] == "22"
    assert payload["items"][0]["geography_code"] == "33"


def test_ine_series_endpoint_validates_pagination_params(client):
    response = client.get("/ine/series?page=0&page_size=500")

    assert response.status_code == 422
