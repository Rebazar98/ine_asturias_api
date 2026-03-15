from __future__ import annotations

from fastapi.testclient import TestClient
import pytest

from app.dependencies import get_series_repository
from app.main import app
from app.settings import Settings, get_settings


class StaticSeriesRepository:
    async def list_normalized(
        self,
        operation_code=None,
        table_id=None,
        geography_code=None,
        geography_name=None,
        geography_code_system="ine",
        variable_id=None,
        period_from=None,
        period_to=None,
        page=1,
        page_size=50,
    ):
        return {
            "items": [],
            "total": 0,
            "page": page,
            "page_size": page_size,
            "pages": 0,
            "has_next": False,
            "has_previous": False,
            "filters": {
                "operation_code": operation_code,
                "table_id": table_id,
                "geography_code": geography_code,
                "geography_name": geography_name,
                "geography_code_system": geography_code_system,
                "variable_id": variable_id,
                "period_from": period_from,
                "period_to": period_to,
            },
        }


def test_metrics_requires_api_key_in_staging_when_configured(monkeypatch):
    monkeypatch.setenv("APP_ENV", "staging")
    monkeypatch.setenv(
        "POSTGRES_DSN",
        "postgresql+asyncpg://postgres:super-secure-db-pass-1234@db:5432/ine_asturias",
    )
    monkeypatch.setenv("REDIS_URL", "")
    monkeypatch.setenv("API_KEY", "staging-api-key-1234567890abcdef")
    get_settings.cache_clear()

    with TestClient(app) as secured_client:
        unauthorized = secured_client.get("/metrics")
        authorized = secured_client.get(
            "/metrics",
            headers={"X-API-Key": "staging-api-key-1234567890abcdef"},
        )

    app.dependency_overrides.clear()
    get_settings.cache_clear()

    assert unauthorized.status_code == 401
    assert authorized.status_code == 200


def test_local_env_keeps_metrics_public_even_with_api_key_configured(monkeypatch):
    monkeypatch.setenv("APP_ENV", "local")
    monkeypatch.setenv("POSTGRES_DSN", "")
    monkeypatch.setenv("REDIS_URL", "")
    monkeypatch.setenv("API_KEY", "local-api-key-1234567890abcdef")
    get_settings.cache_clear()

    with TestClient(app) as local_client:
        response = local_client.get("/metrics")

    app.dependency_overrides.clear()
    get_settings.cache_clear()

    assert response.status_code == 200


def test_public_rate_limit_returns_429(client, dummy_series_repo):
    for _ in range(50):
        response = client.get("/ine/series?page=1&page_size=1")
        assert response.status_code == 200

    limited = client.get("/ine/series?page=1&page_size=1")

    assert limited.status_code == 429
    assert limited.json()["detail"]["policy"] == "ine_series"


def test_authenticated_requests_use_higher_rate_limit(monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("POSTGRES_DSN", "")
    monkeypatch.setenv("REDIS_URL", "")
    monkeypatch.setenv("API_KEY", "test-api-key-1234567890abcdef")
    get_settings.cache_clear()
    app.dependency_overrides[get_series_repository] = lambda: StaticSeriesRepository()

    with TestClient(app) as secured_client:
        for _ in range(60):
            response = secured_client.get(
                "/ine/series?page=1&page_size=1",
                headers={"X-API-Key": "test-api-key-1234567890abcdef"},
            )
            assert response.status_code == 200

    app.dependency_overrides.clear()
    get_settings.cache_clear()


def test_non_local_settings_require_strong_secrets():
    with pytest.raises(ValueError):
        Settings(
            app_env="staging",
            api_key="change-me",
            postgres_dsn="postgresql+asyncpg://postgres:postgres@db:5432/ine_asturias",
        )

    settings = Settings(
        app_env="staging",
        api_key="staging-api-key-1234567890abcdef",
        postgres_dsn="postgresql+asyncpg://postgres:super-secure-db-pass-1234@db:5432/ine_asturias",
    )

    assert settings.requires_api_key is True
    assert settings.api_key == "staging-api-key-1234567890abcdef"
