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


# ---------------------------------------------------------------------------
# Security utility functions (app/core/security.py)
# ---------------------------------------------------------------------------

from app.core.security import (
    compare_api_keys,
    ensure_secret_strength,
    extract_password_from_dsn,
    generate_api_key,
    get_api_key_from_env,
    hash_sensitive_data,
    is_weak_secret,
    sanitize_for_logging,
    sanitize_query_params_for_logging,
)


def test_generate_api_key_returns_non_empty_url_safe_string() -> None:
    key = generate_api_key()
    assert isinstance(key, str) and len(key) >= 32


def test_generate_api_key_is_unique_each_call() -> None:
    assert generate_api_key() != generate_api_key()


def test_get_api_key_from_env_returns_none_when_unset(monkeypatch) -> None:
    monkeypatch.delenv("API_KEY", raising=False)
    assert get_api_key_from_env() is None


def test_get_api_key_from_env_returns_none_for_blank_value(monkeypatch) -> None:
    monkeypatch.setenv("API_KEY", "   ")
    assert get_api_key_from_env() is None


def test_get_api_key_from_env_strips_and_returns_value(monkeypatch) -> None:
    monkeypatch.setenv("API_KEY", "  my-key  ")
    assert get_api_key_from_env() == "my-key"


def test_compare_api_keys_matching() -> None:
    assert compare_api_keys("abc", "abc") is True


def test_compare_api_keys_mismatched() -> None:
    assert compare_api_keys("abc", "xyz") is False


def test_compare_api_keys_none_inputs() -> None:
    assert compare_api_keys(None, "key") is False
    assert compare_api_keys("key", None) is False


def test_hash_sensitive_data_returns_16_char_hex() -> None:
    h = hash_sensitive_data("secret-value")
    assert len(h) == 16 and all(c in "0123456789abcdef" for c in h)


def test_hash_sensitive_data_empty_returns_sentinel() -> None:
    assert hash_sensitive_data("") == "empty"
    assert hash_sensitive_data("  ") == "empty"


def test_sanitize_for_logging_empty_returns_placeholder() -> None:
    assert sanitize_for_logging("") == "[empty]"


def test_sanitize_for_logging_truncates_long_values() -> None:
    result = sanitize_for_logging("a" * 100, max_length=50)
    assert "[+50 chars]" in result


def test_sanitize_for_logging_short_value_unchanged() -> None:
    assert sanitize_for_logging("hello") == "hello"


def test_sanitize_query_params_empty_dict() -> None:
    r = sanitize_query_params_for_logging({})
    assert r["query_params_count"] == 0 and r["query_param_keys"] == []


def test_sanitize_query_params_non_empty() -> None:
    r = sanitize_query_params_for_logging({"page": "1", "q": "oviedo"})
    assert r["query_params_count"] == 2
    assert "query_fingerprint" in r


def test_extract_password_from_dsn_none() -> None:
    assert extract_password_from_dsn(None) is None


def test_extract_password_from_dsn_empty() -> None:
    assert extract_password_from_dsn("") is None


def test_extract_password_from_dsn_no_password() -> None:
    assert extract_password_from_dsn("postgresql://user@localhost/db") is None


def test_extract_password_from_dsn_with_password() -> None:
    assert extract_password_from_dsn("postgresql://u:s3cr3t@host/db") == "s3cr3t"


def test_extract_password_from_dsn_url_encoded() -> None:
    assert extract_password_from_dsn("postgresql://u:p%40ss@host/db") == "p@ss"


def test_is_weak_secret_none() -> None:
    assert is_weak_secret(None) is True


def test_is_weak_secret_too_short() -> None:
    assert is_weak_secret("short") is True


def test_is_weak_secret_known_weak_values() -> None:
    for val in ("password", "secret", "changeme", "change-me", "example"):
        assert is_weak_secret(val, min_length=1) is True


def test_is_weak_secret_change_prefix() -> None:
    assert is_weak_secret("change-this-in-production", min_length=1) is True


def test_is_weak_secret_replace_prefix() -> None:
    assert is_weak_secret("replace-me-with-real-secret", min_length=1) is True


def test_is_weak_secret_strong_secret() -> None:
    assert is_weak_secret("xK9mP2qL8vN3wR5tY7uI0oP6aS4dF1gH") is False


def test_ensure_secret_strength_raises_for_weak() -> None:
    with pytest.raises(ValueError, match="must be configured"):
        ensure_secret_strength("password", secret_name="DB_PASS", min_length=1)


def test_ensure_secret_strength_passes_for_strong() -> None:
    ensure_secret_strength("xK9mP2qL8vN3wR5tY7uI0oP6aS4dF1gH", secret_name="KEY")
