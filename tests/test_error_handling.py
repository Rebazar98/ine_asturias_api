import httpx

from tests.conftest import override_ine_service


def test_upstream_ine_error_is_returned(client, dummy_ingestion_repo, dummy_series_repo):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "temporarily unavailable"})

    override_ine_service(handler)

    response = client.get("/ine/table/999")

    assert response.status_code == 503
    assert response.json()["detail"]["message"] == "The INE service returned an error."
    assert dummy_ingestion_repo.records == []
    assert dummy_series_repo.items == []


def test_invalid_json_from_ine_returns_502(client, dummy_ingestion_repo, dummy_series_repo):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="not-json", headers={"content-type": "application/json"})

    override_ine_service(handler)

    response = client.get("/ine/table/999")

    assert response.status_code == 502
    assert response.json()["detail"]["message"] == "The INE service returned invalid JSON."
    assert dummy_ingestion_repo.records == []
    assert dummy_series_repo.items == []
