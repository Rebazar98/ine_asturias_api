import httpx

from app.core.resilience import CircuitBreakerOpenError
from app.dependencies import get_ine_client_service
from app.main import app
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


def test_request_id_present_in_error_response(client, dummy_ingestion_repo, dummy_series_repo):
    """Service errors must include the client-supplied X-Request-ID in the response body."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "unavailable"})

    override_ine_service(handler)

    response = client.get("/ine/table/999", headers={"X-Request-ID": "trace-error-001"})

    assert response.status_code == 503
    assert response.json()["detail"]["request_id"] == "trace-error-001"


def test_generic_500_returns_clean_json_without_traceback(client):
    """Unhandled exceptions must produce structured JSON with no traceback or internal details.

    The request_timing_middleware catches unhandled exceptions and returns a structured
    500 JSONResponse (rather than re-raising), ensuring the response body is always JSON
    and never exposes internal stack traces to clients.
    """

    class BrokenINEService:
        async def get_table(self, *args, **kwargs):
            raise RuntimeError("unexpected internal failure")

    app.dependency_overrides[get_ine_client_service] = lambda: BrokenINEService()
    try:
        response = client.get("/ine/table/999", headers={"X-Request-ID": "trace-500-test"})
    finally:
        app.dependency_overrides.pop(get_ine_client_service, None)

    assert response.status_code == 500
    body = response.json()
    assert body["detail"]["message"] == "Internal server error."
    assert body["detail"]["request_id"] == "trace-500-test"
    assert "traceback" not in str(body)
    assert "unexpected internal failure" not in str(body)


def test_pydantic_422_normalized_to_dict_format(client):
    """Pydantic validation errors must return a dict (not a list) with message, errors, request_id."""
    response = client.get(
        "/ine/series", params={"page": "not-a-number"}, headers={"X-Request-ID": "trace-422-test"}
    )

    assert response.status_code == 422
    body = response.json()
    detail = body["detail"]
    assert isinstance(detail, dict), f"detail must be a dict, got: {type(detail)}"
    assert detail["message"] == "Validation error."
    assert isinstance(detail["errors"], list)
    assert len(detail["errors"]) > 0
    assert detail["request_id"] == "trace-422-test"


def test_circuit_breaker_open_returns_503_with_retry_after(client):
    """CircuitBreakerOpenError must produce 503 + Retry-After header and structured body."""

    class CircuitOpenINEService:
        async def get_table(self, *args, **kwargs):
            raise CircuitBreakerOpenError(provider="ine", retry_after_seconds=45.7)

    app.dependency_overrides[get_ine_client_service] = lambda: CircuitOpenINEService()
    try:
        response = client.get(
            "/ine/table/999", headers={"X-Request-ID": "trace-cb-test"}
        )
    finally:
        app.dependency_overrides.pop(get_ine_client_service, None)

    assert response.status_code == 503
    assert response.headers["retry-after"] == "46"  # math.ceil(45.7)
    body = response.json()
    assert body["detail"]["message"] == "Service temporarily unavailable. Please retry later."
    assert body["detail"]["provider"] == "ine"
    assert body["detail"]["retry_after_seconds"] == 45.7
    assert body["detail"]["request_id"] == "trace-cb-test"
