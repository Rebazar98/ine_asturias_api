from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from redis.asyncio import Redis


RATE_LIMIT_NAMESPACE = "rate-limit"


@dataclass(frozen=True, slots=True)
class RateLimitPolicy:
    name: str
    public_requests_per_minute: int
    authenticated_requests_per_minute: int
    window_seconds: int = 60


@dataclass(frozen=True, slots=True)
class RateLimitSnapshot:
    count: int
    reset_at: float

    @property
    def retry_after_seconds(self) -> int:
        return max(1, int(self.reset_at - time.time()))


class BaseRateLimiter:
    async def increment(self, key: str, *, window_seconds: int) -> RateLimitSnapshot:
        raise NotImplementedError


class InMemoryRateLimiter(BaseRateLimiter):
    _SWEEP_THRESHOLD = 500

    def __init__(self) -> None:
        self._entries: dict[str, tuple[int, float]] = {}
        self._lock = asyncio.Lock()

    def _sweep_expired(self) -> None:
        """Remove all expired windows. Must be called while holding self._lock."""
        now = time.time()
        expired = [k for k, (_, reset_at) in self._entries.items() if reset_at <= now]
        for k in expired:
            del self._entries[k]

    async def increment(self, key: str, *, window_seconds: int) -> RateLimitSnapshot:
        now = time.time()
        async with self._lock:
            if len(self._entries) > self._SWEEP_THRESHOLD:
                self._sweep_expired()
            count, reset_at = self._entries.get(key, (0, now + window_seconds))
            if reset_at <= now:
                count = 0
                reset_at = now + window_seconds
            count += 1
            self._entries[key] = (count, reset_at)
            return RateLimitSnapshot(count=count, reset_at=reset_at)


class RedisRateLimiter(BaseRateLimiter):
    def __init__(self, redis: Redis, *, namespace: str = RATE_LIMIT_NAMESPACE) -> None:
        self.redis = redis
        self.namespace = namespace

    async def increment(self, key: str, *, window_seconds: int) -> RateLimitSnapshot:
        redis_key = f"{self.namespace}:{key}"
        count = int(await self.redis.incr(redis_key))
        if count == 1:
            await self.redis.expire(redis_key, window_seconds)
            ttl_seconds = window_seconds
        else:
            ttl_seconds = int(await self.redis.ttl(redis_key))
            if ttl_seconds <= 0:
                await self.redis.expire(redis_key, window_seconds)
                ttl_seconds = window_seconds
        return RateLimitSnapshot(count=count, reset_at=time.time() + ttl_seconds)
