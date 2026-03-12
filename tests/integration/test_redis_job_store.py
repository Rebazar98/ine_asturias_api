from __future__ import annotations

import asyncio
import os

import pytest
from redis.asyncio import Redis

from app.core.jobs import RedisJobStore
from app.settings import Settings


@pytest.mark.integration
def test_redis_job_store_roundtrip_when_redis_is_available():
    redis_url = os.getenv("INTEGRATION_REDIS_URL") or os.getenv("REDIS_URL")
    if not redis_url:
        pytest.skip("Redis integration URL not configured.")

    async def scenario() -> None:
        redis = Redis.from_url(redis_url, decode_responses=True)
        settings = Settings(REDIS_URL=redis_url)
        store = RedisJobStore(redis=redis, settings=settings)
        await redis.ping()

        job = await store.create_job("integration_test", {"scope": "redis"})
        await store.mark_running(job["job_id"])
        await store.update_progress(job["job_id"], stage="integration")
        await store.complete_job(job["job_id"], {"ok": True})
        loaded = await store.get_job(job["job_id"])

        assert loaded is not None
        assert loaded["status"] == "completed"
        assert loaded["progress"]["stage"] == "integration"
        assert loaded["result"] == {"ok": True}

        await redis.delete(f"jobs:{job['job_id']}")
        await redis.aclose()

    asyncio.run(scenario())
