from enum import Enum
from functools import lru_cache

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.core.security import ensure_secret_strength, extract_password_from_dsn


class Environment(str, Enum):
    LOCAL = "local"
    STAGING = "staging"
    PRODUCTION = "production"


class Settings(BaseSettings):
    app_name: str = Field(default="ine_asturias_api", alias="APP_NAME")
    app_version: str = Field(default="0.1.0-rc1", alias="APP_VERSION")
    app_env: str = Field(default="local", alias="APP_ENV")
    ine_base_url: str = Field(
        default="https://servicios.ine.es/wstempus/js/ES",
        alias="INE_BASE_URL",
    )
    cartociudad_base_url: str = Field(
        default="https://www.cartociudad.es/geocoder/api/geocoder",
        alias="CARTOCIUDAD_BASE_URL",
    )
    http_timeout_seconds: float = Field(default=15.0, alias="HTTP_TIMEOUT_SECONDS")
    provider_total_timeout_seconds: float = Field(
        default=30.0, alias="PROVIDER_TOTAL_TIMEOUT_SECONDS", gt=0
    )
    http_retry_max_attempts: int = Field(default=3, alias="HTTP_RETRY_MAX_ATTEMPTS", ge=1, le=5)
    http_retry_backoff_seconds: float = Field(default=1.0, alias="HTTP_RETRY_BACKOFF_SECONDS", gt=0)
    postgres_dsn: str | None = Field(default=None, alias="POSTGRES_DSN")
    enable_cache: bool = Field(default=True, alias="ENABLE_CACHE")
    cache_ttl_seconds: int = Field(default=300, alias="CACHE_TTL_SECONDS")
    api_key: str | None = Field(default=None, alias="API_KEY")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    redis_url: str | None = Field(default=None, alias="REDIS_URL")
    job_queue_name: str = Field(default="ine_jobs", alias="JOB_QUEUE_NAME")
    job_result_ttl_seconds: int = Field(default=86400, alias="JOB_RESULT_TTL_SECONDS")
    analytical_snapshot_ttl_seconds: int = Field(
        default=21600, alias="ANALYTICAL_SNAPSHOT_TTL_SECONDS", ge=0
    )
    max_concurrent_table_fetches: int = Field(
        default=3, alias="MAX_CONCURRENT_TABLE_FETCHES", ge=1, le=10
    )
    rate_limit_enabled: bool = Field(default=True, alias="RATE_LIMIT_ENABLED")
    provider_circuit_breaker_failures: int = Field(
        default=5, alias="PROVIDER_CIRCUIT_BREAKER_FAILURES", ge=1, le=20
    )
    provider_circuit_breaker_recovery_seconds: int = Field(
        default=30, alias="PROVIDER_CIRCUIT_BREAKER_RECOVERY_SECONDS", ge=1
    )
    provider_circuit_breaker_half_open_sample_size: int = Field(
        default=5, alias="PROVIDER_CIRCUIT_BREAKER_HALF_OPEN_SAMPLE_SIZE", ge=1, le=20
    )
    provider_circuit_breaker_success_threshold: float = Field(
        default=0.8, alias="PROVIDER_CIRCUIT_BREAKER_SUCCESS_THRESHOLD", ge=0.5, le=1.0
    )
    worker_heartbeat_ttl_seconds: int = Field(default=60, alias="WORKER_HEARTBEAT_TTL_SECONDS")
    worker_metrics_port: int = Field(default=9001, alias="WORKER_METRICS_PORT")
    worker_metrics_url: str | None = Field(default=None, alias="WORKER_METRICS_URL")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        populate_by_name=True,
        extra="ignore",
    )

    @field_validator("postgres_dsn", "api_key", "redis_url", "worker_metrics_url", mode="before")
    @classmethod
    def empty_strings_to_none(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @property
    def is_local_env(self) -> bool:
        return self.app_env.lower() in {"dev", "development", "local", "test"}

    @property
    def environment(self) -> Environment:
        normalized = self.app_env.strip().lower()
        if normalized == Environment.STAGING.value:
            return Environment.STAGING
        if normalized == Environment.PRODUCTION.value:
            return Environment.PRODUCTION
        return Environment.LOCAL

    @property
    def requires_api_key(self) -> bool:
        return not self.is_local_env

    @model_validator(mode="after")
    def validate_runtime_security(self) -> "Settings":
        if self.is_local_env:
            return self

        ensure_secret_strength(self.api_key, secret_name="API_KEY")
        ensure_secret_strength(
            extract_password_from_dsn(self.postgres_dsn),
            secret_name="POSTGRES_DSN password",
            min_length=16,
        )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
