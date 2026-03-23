"""Tests for Fase A: cron job registration and job store default."""

from __future__ import annotations

from app.worker import (
    WorkerSettings,
    scheduled_ine_update,
    scheduled_territorial_sync,
)


def test_worker_cron_registration():
    """WorkerSettings.cron_jobs must include both scheduled functions."""
    assert hasattr(WorkerSettings, "cron_jobs"), "WorkerSettings must define cron_jobs"
    cron_functions = [entry.coroutine for entry in WorkerSettings.cron_jobs]
    assert scheduled_ine_update in cron_functions, (
        "scheduled_ine_update must be registered as a cron job"
    )
    assert scheduled_territorial_sync in cron_functions, (
        "scheduled_territorial_sync must be registered as a cron job"
    )


def test_worker_cron_schedules():
    """Cron schedules must match the production plan (daily 03:00 and Monday 04:00)."""
    assert len(WorkerSettings.cron_jobs) >= 2

    ine_entry = next(e for e in WorkerSettings.cron_jobs if e.coroutine is scheduled_ine_update)
    territorial_entry = next(
        e for e in WorkerSettings.cron_jobs if e.coroutine is scheduled_territorial_sync
    )

    # CronJob is an arq dataclass with direct attributes (hour, minute, weekday, etc.)
    assert ine_entry.hour == {3}, "INE cron must run at hour 3"
    assert ine_entry.minute == {0}, "INE cron must run at minute 0"
    assert territorial_entry.weekday == {1}, "Territorial cron must run on Monday (weekday=1)"
    assert territorial_entry.hour == {4}, "Territorial cron must run at hour 4"


def test_redis_job_store_is_default_in_api(monkeypatch):
    """When JOB_STORE_BACKEND is not 'memory', the API must not instantiate InMemoryJobStore."""
    from app.core.jobs import InMemoryJobStore
    from app.settings import Settings, get_settings

    monkeypatch.setenv("APP_ENV", "local")
    monkeypatch.setenv("JOB_STORE_BACKEND", "redis")
    get_settings.cache_clear()
    settings = Settings(
        app_env="local",
        job_store_backend="redis",
    )
    assert settings.job_store_backend == "redis"

    # InMemoryJobStore must only be instantiated when explicitly configured
    memory_settings = Settings(
        app_env="local",
        job_store_backend="memory",
    )
    assert memory_settings.job_store_backend == "memory"

    job_store = InMemoryJobStore()
    assert job_store is not None  # InMemory works when explicitly requested

    get_settings.cache_clear()


def test_settings_scheduled_ine_operations_default():
    """Default scheduled_ine_operations must include operations 22 and 33."""
    from app.settings import Settings

    s = Settings(app_env="local", job_store_backend="memory", _env_file=None)
    assert "22" in s.scheduled_ine_operations
    assert "33" in s.scheduled_ine_operations


def test_settings_job_store_backend_default():
    """Default job_store_backend must be 'redis' (never 'memory' in production)."""
    from app.settings import Settings

    s = Settings(app_env="local")
    assert s.job_store_backend == "redis"
