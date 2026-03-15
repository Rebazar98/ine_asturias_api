from __future__ import annotations

import asyncio
import os

import pytest
from redis.asyncio import Redis


def require_integration_redis() -> str:
    redis_url = os.getenv("INTEGRATION_REDIS_URL") or os.getenv("REDIS_URL")
    if not redis_url:
        pytest.skip("Redis integration URL not configured.")

    asyncio.run(_ensure_connection(redis_url))
    return redis_url


async def _ensure_connection(redis_url: str) -> None:
    redis = Redis.from_url(redis_url, decode_responses=True)
    try:
        await redis.ping()
    except Exception as exc:  # pragma: no cover - depends on local integration env
        pytest.skip(f"Redis integration URL is not reachable: {exc}")
    finally:
        await redis.aclose()
