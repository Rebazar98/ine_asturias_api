"""Unit tests for scheduled cron job functions in app/worker.py.

All tests run synchronously via asyncio.run() — no DB, no Redis, no arq pool required.
"""

from __future__ import annotations

import asyncio
from typing import Any


from app.core.jobs import InMemoryJobStore
from app.settings import Settings
from app.worker import (
    scheduled_ideas_sync,
    scheduled_ine_update,
    scheduled_sadei_sync,
    scheduled_territorial_sync,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _settings(**overrides: Any) -> Settings:
    return Settings(app_env="local", job_store_backend="memory", **overrides)


class _MockArqPool:
    """Minimal arq pool replacement that records enqueue_job calls."""

    def __init__(self) -> None:
        self.enqueued: list[dict[str, Any]] = []

    async def enqueue_job(self, fn_name: str, **kwargs: Any) -> None:
        self.enqueued.append({"fn_name": fn_name, **kwargs})


class _FailingArqPool:
    """arq pool that always raises on enqueue_job."""

    async def enqueue_job(self, fn_name: str, **kwargs: Any) -> None:
        raise RuntimeError("arq connection refused")


# ---------------------------------------------------------------------------
# scheduled_territorial_sync
# ---------------------------------------------------------------------------


def test_scheduled_territorial_sync_returns_early_when_disabled() -> None:
    """When SCHEDULED_TERRITORIAL_SYNC_ENABLED=false, the function must not enqueue anything."""

    async def scenario() -> None:
        arq_pool = _MockArqPool()
        ctx = {
            "settings": _settings(scheduled_territorial_sync_enabled=False),
            "arq_pool": arq_pool,
        }
        await scheduled_territorial_sync(ctx)
        assert arq_pool.enqueued == []

    asyncio.run(scenario())


def test_scheduled_territorial_sync_logs_and_returns_when_enabled() -> None:
    """When enabled, scheduled_territorial_sync must complete without raising."""

    async def scenario() -> None:
        ctx = {
            "settings": _settings(scheduled_territorial_sync_enabled=True),
            "arq_pool": _MockArqPool(),
        }
        # Should not raise; placeholder implementation just logs.
        await scheduled_territorial_sync(ctx)

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# scheduled_ine_update
# ---------------------------------------------------------------------------


def test_scheduled_ine_update_enqueues_one_job_per_operation() -> None:
    """scheduled_ine_update must create and enqueue a job for each configured op_code."""

    async def scenario() -> None:
        job_store = InMemoryJobStore()
        arq_pool = _MockArqPool()
        ctx = {
            "settings": _settings(scheduled_ine_operations=["2081", "2074"]),
            "job_store": job_store,
            "arq_pool": arq_pool,
        }
        await scheduled_ine_update(ctx)

        assert len(arq_pool.enqueued) == 2
        fn_names = {e["fn_name"] for e in arq_pool.enqueued}
        assert fn_names == {"run_operation_asturias_job"}

        op_codes = {e["payload"]["operation_code"] for e in arq_pool.enqueued}
        assert op_codes == {"2081", "2074"}

    asyncio.run(scenario())


def test_scheduled_ine_update_skips_gracefully_when_enqueue_fails() -> None:
    """If arq_pool.enqueue_job raises, the exception must be caught — not propagated."""

    async def scenario() -> None:
        job_store = InMemoryJobStore()
        ctx = {
            "settings": _settings(scheduled_ine_operations=["2081"]),
            "job_store": job_store,
            "arq_pool": _FailingArqPool(),
        }
        # Must complete without raising even though enqueue fails.
        await scheduled_ine_update(ctx)

    asyncio.run(scenario())


def test_scheduled_ine_update_with_empty_operations_list() -> None:
    """When scheduled_ine_operations is empty, no jobs are created or enqueued."""

    async def scenario() -> None:
        job_store = InMemoryJobStore()
        arq_pool = _MockArqPool()
        ctx = {
            "settings": _settings(scheduled_ine_operations=[]),
            "job_store": job_store,
            "arq_pool": arq_pool,
        }
        await scheduled_ine_update(ctx)
        assert arq_pool.enqueued == []

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# scheduled_sadei_sync
# ---------------------------------------------------------------------------


def test_scheduled_sadei_sync_enqueues_one_job_per_dataset() -> None:
    """scheduled_sadei_sync must create and enqueue a job for each configured dataset_id."""

    async def scenario() -> None:
        job_store = InMemoryJobStore()
        arq_pool = _MockArqPool()
        ctx = {
            "settings": _settings(sadei_sync_datasets=["padron_municipal", "pib_municipal"]),
            "job_store": job_store,
            "arq_pool": arq_pool,
        }
        await scheduled_sadei_sync(ctx)

        assert len(arq_pool.enqueued) == 2
        dataset_ids = {e["payload"]["dataset_id"] for e in arq_pool.enqueued}
        assert dataset_ids == {"padron_municipal", "pib_municipal"}

    asyncio.run(scenario())


def test_scheduled_sadei_sync_skips_gracefully_when_enqueue_fails() -> None:
    """Enqueue failure must be caught and not propagated."""

    async def scenario() -> None:
        ctx = {
            "settings": _settings(sadei_sync_datasets=["padron_municipal"]),
            "job_store": InMemoryJobStore(),
            "arq_pool": _FailingArqPool(),
        }
        await scheduled_sadei_sync(ctx)

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# scheduled_ideas_sync
# ---------------------------------------------------------------------------


def test_scheduled_ideas_sync_enqueues_one_job_per_layer() -> None:
    """scheduled_ideas_sync must create and enqueue a job for each configured layer_name."""

    async def scenario() -> None:
        job_store = InMemoryJobStore()
        arq_pool = _MockArqPool()
        ctx = {
            "settings": _settings(ideas_sync_layers=["limites_parroquiales", "usos_suelo"]),
            "job_store": job_store,
            "arq_pool": arq_pool,
        }
        await scheduled_ideas_sync(ctx)

        assert len(arq_pool.enqueued) == 2
        layer_names = {e["payload"]["layer_name"] for e in arq_pool.enqueued}
        assert layer_names == {"limites_parroquiales", "usos_suelo"}

    asyncio.run(scenario())


def test_scheduled_ideas_sync_skips_gracefully_when_enqueue_fails() -> None:
    """Enqueue failure must be caught and not propagated."""

    async def scenario() -> None:
        ctx = {
            "settings": _settings(ideas_sync_layers=["limites_parroquiales"]),
            "job_store": InMemoryJobStore(),
            "arq_pool": _FailingArqPool(),
        }
        await scheduled_ideas_sync(ctx)

    asyncio.run(scenario())
