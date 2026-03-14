from __future__ import annotations

import asyncio
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from redis.asyncio import Redis


@dataclass(slots=True)
class CacheEntry:
    value: Any
    expires_at: float


class BaseAsyncCache(ABC):
    def __init__(self, enabled: bool = True, default_ttl_seconds: int = 300) -> None:
        self.enabled = enabled
        self.default_ttl_seconds = default_ttl_seconds

    @abstractmethod
    async def get(self, key: str) -> Any | None:
        raise NotImplementedError

    @abstractmethod
    async def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        raise NotImplementedError

    @abstractmethod
    async def delete(self, key: str) -> None:
        raise NotImplementedError

    async def get_or_set(
        self,
        key: str,
        factory: Callable[[], Awaitable[Any]],
        ttl_seconds: int | None = None,
    ) -> Any:
        cached = await self.get(key)
        if cached is not None:
            return cached

        value = await factory()
        await self.set(key, value, ttl_seconds=ttl_seconds)
        return value


class InMemoryTTLCache(BaseAsyncCache):
    def __init__(self, enabled: bool = True, default_ttl_seconds: int = 300) -> None:
        super().__init__(enabled=enabled, default_ttl_seconds=default_ttl_seconds)
        self._store: dict[str, CacheEntry] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any | None:
        if not self.enabled:
            return None

        async with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            if entry.expires_at <= time.monotonic():
                self._store.pop(key, None)
                return None
            return entry.value

    async def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        if not self.enabled:
            return

        ttl = ttl_seconds or self.default_ttl_seconds
        async with self._lock:
            self._store[key] = CacheEntry(value=value, expires_at=time.monotonic() + ttl)

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._store.pop(key, None)

class RedisTTLCache(BaseAsyncCache):
    def __init__(
        self,
        redis: Redis,
        *,
        enabled: bool = True,
        default_ttl_seconds: int = 300,
        namespace: str = "cache",
    ) -> None:
        super().__init__(enabled=enabled, default_ttl_seconds=default_ttl_seconds)
        self.redis = redis
        self.namespace = namespace.strip(":")

    async def get(self, key: str) -> Any | None:
        if not self.enabled:
            return None

        payload = await self.redis.get(self._key(key))
        if payload is None:
            return None

        return json.loads(payload)

    async def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        if not self.enabled:
            return

        ttl = ttl_seconds or self.default_ttl_seconds
        await self.redis.set(
            self._key(key),
            json.dumps(value, default=str),
            ex=ttl,
        )

    async def delete(self, key: str) -> None:
        await self.redis.delete(self._key(key))

    def _key(self, key: str) -> str:
        return f"{self.namespace}:{key}"


class LayeredCache(BaseAsyncCache):
    def __init__(
        self,
        local_cache: BaseAsyncCache,
        shared_cache: BaseAsyncCache,
    ) -> None:
        super().__init__(
            enabled=local_cache.enabled or shared_cache.enabled,
            default_ttl_seconds=local_cache.default_ttl_seconds,
        )
        self.local_cache = local_cache
        self.shared_cache = shared_cache

    async def get(self, key: str) -> Any | None:
        cached = await self.local_cache.get(key)
        if cached is not None:
            return cached

        cached = await self.shared_cache.get(key)
        if cached is None:
            return None

        await self.local_cache.set(key, cached)
        return cached

    async def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        await self.shared_cache.set(key, value, ttl_seconds=ttl_seconds)
        await self.local_cache.set(key, value, ttl_seconds=ttl_seconds)

    async def delete(self, key: str) -> None:
        await self.shared_cache.delete(key)
        await self.local_cache.delete(key)
