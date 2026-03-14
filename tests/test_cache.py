import pytest

from app.core.cache import InMemoryTTLCache, LayeredCache, RedisTTLCache


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.get_calls = 0
        self.set_calls: list[tuple[str, str, int | None]] = []
        self.delete_calls: list[str] = []

    async def get(self, key: str):
        self.get_calls += 1
        return self.store.get(key)

    async def set(self, key: str, value: str, ex: int | None = None):
        self.set_calls.append((key, value, ex))
        self.store[key] = value

    async def delete(self, key: str):
        self.delete_calls.append(key)
        self.store.pop(key, None)


@pytest.mark.anyio
async def test_redis_ttl_cache_roundtrips_json_payloads() -> None:
    redis = FakeRedis()
    cache = RedisTTLCache(redis=redis, default_ttl_seconds=45, namespace="provider-cache")

    payload = {"path": "DATOS_TABLA/501", "items": [{"period": "2024", "value": 1.0}]}
    await cache.set("table:501", payload)

    assert redis.set_calls == [
        (
            "provider-cache:table:501",
            '{"path": "DATOS_TABLA/501", "items": [{"period": "2024", "value": 1.0}]}',
            45,
        )
    ]
    assert await cache.get("table:501") == payload


@pytest.mark.anyio
async def test_layered_cache_warms_local_cache_after_shared_hit() -> None:
    redis = FakeRedis()
    shared_cache = RedisTTLCache(redis=redis, default_ttl_seconds=60, namespace="provider-cache")
    local_cache = InMemoryTTLCache(default_ttl_seconds=60)
    cache = LayeredCache(local_cache=local_cache, shared_cache=shared_cache)

    await shared_cache.set("operation_tables:22", [{"table_id": "2852"}])

    first = await cache.get("operation_tables:22")
    second = await cache.get("operation_tables:22")

    assert first == second == [{"table_id": "2852"}]
    assert redis.get_calls == 1
