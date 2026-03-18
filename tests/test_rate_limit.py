from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.rate_limit import BaseRateLimiter, InMemoryRateLimiter, RedisRateLimiter


@pytest.mark.anyio
async def test_inmemory_rate_limiter_increments_count() -> None:
    limiter = InMemoryRateLimiter()
    snap = await limiter.increment("ip:1.2.3.4", window_seconds=60)
    assert snap.count == 1
    snap2 = await limiter.increment("ip:1.2.3.4", window_seconds=60)
    assert snap2.count == 2


@pytest.mark.anyio
async def test_inmemory_rate_limiter_resets_after_window_expires() -> None:
    limiter = InMemoryRateLimiter()
    with patch("app.core.rate_limit.time.time", return_value=0.0):
        await limiter.increment("ip:1.2.3.4", window_seconds=60)

    # Advance time past the window
    with patch("app.core.rate_limit.time.time", return_value=61.0):
        snap = await limiter.increment("ip:1.2.3.4", window_seconds=60)
        assert snap.count == 1  # reset to 1 (new window)


@pytest.mark.anyio
async def test_inmemory_rate_limiter_does_not_accumulate_expired_entries() -> None:
    limiter = InMemoryRateLimiter()

    # Fill with 600 distinct IPs at t=0 with a 10s window
    with patch("app.core.rate_limit.time.time", return_value=0.0):
        for i in range(600):
            await limiter.increment(f"ip:10.0.{i // 256}.{i % 256}", window_seconds=10)

    # All 600 entries exist (none swept yet, threshold not crossed during fill
    # because sweep triggers on len > 500 *before* the new entry is added)
    assert len(limiter._entries) <= 600

    # Advance time past the window so all entries are expired
    # Adding one more entry (different key) triggers sweep
    with patch("app.core.rate_limit.time.time", return_value=20.0):
        await limiter.increment("ip:new", window_seconds=10)
        # After sweep all 600 old entries are gone, only "ip:new" remains
        assert len(limiter._entries) == 1


# ---------------------------------------------------------------------------
# BaseRateLimiter
# ---------------------------------------------------------------------------


def test_base_rate_limiter_increment_raises_not_implemented() -> None:
    async def _run():
        with pytest.raises(NotImplementedError):
            await BaseRateLimiter().increment("k", window_seconds=60)

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# RedisRateLimiter
# ---------------------------------------------------------------------------


def _mock_redis(*, incr_return: int, ttl_return: int) -> MagicMock:
    redis = MagicMock()
    redis.incr = AsyncMock(return_value=incr_return)
    redis.expire = AsyncMock(return_value=True)
    redis.ttl = AsyncMock(return_value=ttl_return)
    return redis


def test_redis_rate_limiter_first_increment_sets_expiry() -> None:
    """First call (count == 1) must set expiry and return count 1."""

    async def _run():
        redis = _mock_redis(incr_return=1, ttl_return=60)
        limiter = RedisRateLimiter(redis, namespace="rl")
        snap = await limiter.increment("ip:1.2.3.4", window_seconds=60)
        assert snap.count == 1
        redis.expire.assert_awaited_once_with("rl:ip:1.2.3.4", 60)
        redis.ttl.assert_not_awaited()

    asyncio.run(_run())


def test_redis_rate_limiter_subsequent_increment_with_positive_ttl() -> None:
    """Subsequent call with a live TTL must not re-expire the key."""

    async def _run():
        redis = _mock_redis(incr_return=3, ttl_return=45)
        limiter = RedisRateLimiter(redis, namespace="rl")
        snap = await limiter.increment("ip:1.2.3.4", window_seconds=60)
        assert snap.count == 3
        redis.expire.assert_not_awaited()

    asyncio.run(_run())


def test_redis_rate_limiter_subsequent_increment_with_expired_ttl() -> None:
    """Subsequent call where TTL <= 0 (key expired in Redis) must re-set expiry."""

    async def _run():
        redis = _mock_redis(incr_return=2, ttl_return=-1)
        limiter = RedisRateLimiter(redis, namespace="rl")
        snap = await limiter.increment("ip:1.2.3.4", window_seconds=60)
        assert snap.count == 2
        redis.expire.assert_awaited_once_with("rl:ip:1.2.3.4", 60)

    asyncio.run(_run())
