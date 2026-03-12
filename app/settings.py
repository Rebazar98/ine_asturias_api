from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = Field(default="ine_asturias_api", alias="APP_NAME")
    app_env: str = Field(default="local", alias="APP_ENV")
    ine_base_url: str = Field(
        default="https://servicios.ine.es/wstempus/js/ES",
        alias="INE_BASE_URL",
    )
    http_timeout_seconds: float = Field(default=15.0, alias="HTTP_TIMEOUT_SECONDS")
    postgres_dsn: str | None = Field(default=None, alias="POSTGRES_DSN")
    enable_cache: bool = Field(default=True, alias="ENABLE_CACHE")
    cache_ttl_seconds: int = Field(default=300, alias="CACHE_TTL_SECONDS")
    api_key: str | None = Field(default=None, alias="API_KEY")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    redis_url: str | None = Field(default=None, alias="REDIS_URL")
    job_queue_name: str = Field(default="ine_jobs", alias="JOB_QUEUE_NAME")
    job_result_ttl_seconds: int = Field(default=86400, alias="JOB_RESULT_TTL_SECONDS")
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


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
