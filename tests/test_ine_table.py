import httpx

from tests.conftest import override_ine_service


TABLE_PAYLOAD = [
    {
        "Nombre": "Indice general Asturias",
        "MetaData": [
            {"Variable": "Comunidad autonoma", "Nombre": "Asturias", "Id": "33"},
            {"Variable": "Indicador", "Nombre": "Indice general", "Id": "IPC_GENERAL"},
        ],
        "Data": [
            {"Periodo": "2024M01", "Valor": "101,5", "Unidad": "indice"},
            {"Periodo": "2024M02", "Valor": "102,7", "Unidad": "indice"},
        ],
    }
]


def test_ine_table_returns_payload_and_normalizes(client, dummy_ingestion_repo, dummy_series_repo):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/DATOS_TABLA/12345"
        assert request.url.params["nult"] == "2"
        assert request.url.params["det"] == "2"
        assert request.url.params["tip"] == "AM"
        assert request.url.params["date"] == "2024M02"
        return httpx.Response(200, json=TABLE_PAYLOAD)

    override_ine_service(handler)

    response = client.get("/ine/table/12345?nult=2&det=2&tip=AM&date=2024M02")

    assert response.status_code == 200
    assert response.json() == TABLE_PAYLOAD
    assert len(dummy_ingestion_repo.records) == 1
    assert dummy_ingestion_repo.records[0]["source_type"] == "table"
    assert len(dummy_series_repo.items) == 2
    assert dummy_series_repo.items[0].table_id == "12345"
    assert dummy_series_repo.items[0].geography_name == "Asturias"
    assert dummy_series_repo.items[0].value == 101.5


def test_ine_table_validates_query_params(client):
    response = client.get("/ine/table/12345?nult=0&det=7&tip=INVALID")

    assert response.status_code == 422
