from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from redis.asyncio import Redis

from app.core.logging import get_logger
from app.core.metrics import record_job_event, record_worker_heartbeat
from app.settings import Settings


class BaseJobStore(ABC):
    @abstractmethod
    async def create_job(
        self, job_type: str, params: dict[str, Any], job_id: str | None = None
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def mark_running(self, job_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def update_progress(self, job_id: str, **progress: Any) -> None:
        raise NotImplementedError

    @abstractmethod
    async def complete_job(self, job_id: str, result: dict[str, Any] | list[Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def fail_job(self, job_id: str, error: Any) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_job(self, job_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    async def ping(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def record_worker_heartbeat(self, queue_name: str, worker_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_worker_status(self, queue_name: str) -> dict[str, Any]:
        raise NotImplementedError


class InMemoryJobStore(BaseJobStore):
    def __init__(self) -> None:
        self._jobs: dict[str, dict[str, Any]] = {}
        self._heartbeats: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self.logger = get_logger("app.core.jobs.in_memory")

    async def create_job(
        self, job_type: str, params: dict[str, Any], job_id: str | None = None
    ) -> dict[str, Any]:
        job_id = job_id or uuid4().hex
        record = {
            "job_id": job_id,
            "job_type": job_type,
            "status": "queued",
            "created_at": _utcnow(),
            "started_at": None,
            "finished_at": None,
            "params": dict(params),
            "progress": {},
            "result": None,
            "error": None,
        }
        async with self._lock:
            self._jobs[job_id] = record
        record_job_event(job_type, "queued")
        return dict(record)

    async def mark_running(self, job_id: str) -> None:
        await self._update_job(
            job_id, {"status": "running", "started_at": _utcnow(), "error": None}
        )

    async def update_progress(self, job_id: str, **progress: Any) -> None:
        async with self._lock:
            current = self._jobs.get(job_id)
            if current is None:
                return
            next_progress = dict(current.get("progress", {}))
            next_progress.update(progress)
            current["progress"] = next_progress

    async def complete_job(self, job_id: str, result: dict[str, Any] | list[Any]) -> None:
        await self._update_job(
            job_id,
            {
                "status": "completed",
                "finished_at": _utcnow(),
                "result": result,
                "error": None,
            },
        )

    async def fail_job(self, job_id: str, error: Any) -> None:
        await self._update_job(
            job_id,
            {
                "status": "failed",
                "finished_at": _utcnow(),
                "error": error,
            },
        )

    async def get_job(self, job_id: str) -> dict[str, Any] | None:
        async with self._lock:
            record = self._jobs.get(job_id)
            if record is None:
                return None
            return _copy_job_record(record)

    async def ping(self) -> bool:
        return True

    async def record_worker_heartbeat(self, queue_name: str, worker_id: str) -> None:
        payload = {"queue_name": queue_name, "worker_id": worker_id, "updated_at": _utcnow()}
        async with self._lock:
            self._heartbeats[queue_name] = payload
        record_worker_heartbeat(queue_name)

    async def get_worker_status(self, queue_name: str) -> dict[str, Any]:
        async with self._lock:
            payload = self._heartbeats.get(queue_name)
        if payload is None:
            return {"status": "disabled", "queue_name": queue_name}
        return {"status": "ok", **payload}

    async def _update_job(self, job_id: str, changes: dict[str, Any]) -> None:
        async with self._lock:
            current = self._jobs.get(job_id)
            if current is None:
                return
            current.update(changes)
            record = dict(current)
        record_job_event(record["job_type"], record["status"])


class RedisJobStore(BaseJobStore):
    def __init__(self, redis: Redis, settings: Settings) -> None:
        self.redis = redis
        self.settings = settings
        self.logger = get_logger("app.core.jobs.redis")

    async def create_job(
        self, job_type: str, params: dict[str, Any], job_id: str | None = None
    ) -> dict[str, Any]:
        job_id = job_id or uuid4().hex
        record = {
            "job_id": job_id,
            "job_type": job_type,
            "status": "queued",
            "created_at": _utcnow(),
            "started_at": None,
            "finished_at": None,
            "params": dict(params),
            "progress": {},
            "result": None,
            "error": None,
        }
        await self._write_job(job_id, record)
        record_job_event(job_type, "queued")
        return record

    async def mark_running(self, job_id: str) -> None:
        await self._update_job(
            job_id, {"status": "running", "started_at": _utcnow(), "error": None}
        )

    async def update_progress(self, job_id: str, **progress: Any) -> None:
        current = await self.get_job(job_id)
        if current is None:
            return
        next_progress = dict(current.get("progress", {}))
        next_progress.update(progress)
        current["progress"] = next_progress
        await self._write_job(job_id, current, ttl_seconds=self._ttl_for_status(current["status"]))

    async def complete_job(self, job_id: str, result: dict[str, Any] | list[Any]) -> None:
        await self._update_job(
            job_id,
            {
                "status": "completed",
                "finished_at": _utcnow(),
                "result": result,
                "error": None,
            },
        )

    async def fail_job(self, job_id: str, error: Any) -> None:
        await self._update_job(
            job_id,
            {
                "status": "failed",
                "finished_at": _utcnow(),
                "error": error,
            },
        )

    async def get_job(self, job_id: str) -> dict[str, Any] | None:
        payload = await self.redis.get(self._job_key(job_id))
        if payload is None:
            return None
        return json.loads(payload)

    async def ping(self) -> bool:
        try:
            return bool(await self.redis.ping())
        except Exception:
            self.logger.warning("redis_job_store_ping_failed")
            return False

    async def record_worker_heartbeat(self, queue_name: str, worker_id: str) -> None:
        payload = {
            "queue_name": queue_name,
            "worker_id": worker_id,
            "updated_at": _utcnow(),
        }
        try:
            await self.redis.set(
                self._heartbeat_key(queue_name),
                json.dumps(payload, default=str),
                ex=self.settings.worker_heartbeat_ttl_seconds,
            )
        except Exception:
            self.logger.warning(
                "redis_worker_heartbeat_failed",
                extra={"queue_name": queue_name, "worker_id": worker_id},
            )
            return
        record_worker_heartbeat(queue_name)

    async def get_worker_status(self, queue_name: str) -> dict[str, Any]:
        try:
            payload = await self.redis.get(self._heartbeat_key(queue_name))
        except Exception as exc:
            return {"status": "error", "queue_name": queue_name, "message": str(exc)}
        if payload is None:
            return {
                "status": "error",
                "queue_name": queue_name,
                "message": "worker heartbeat missing",
            }
        return {"status": "ok", **json.loads(payload)}

    async def _update_job(self, job_id: str, changes: dict[str, Any]) -> None:
        current = await self.get_job(job_id)
        if current is None:
            return
        current.update(changes)
        await self._write_job(job_id, current, ttl_seconds=self._ttl_for_status(current["status"]))
        record_job_event(current["job_type"], current["status"])

    async def _write_job(
        self, job_id: str, record: dict[str, Any], ttl_seconds: int | None = None
    ) -> None:
        payload = json.dumps(record, default=str)
        await self.redis.set(self._job_key(job_id), payload, ex=ttl_seconds)

    def _ttl_for_status(self, status: str) -> int | None:
        if status in {"completed", "failed"}:
            return self.settings.job_result_ttl_seconds
        return None

    @staticmethod
    def _job_key(job_id: str) -> str:
        return f"jobs:{job_id}"

    @staticmethod
    def _heartbeat_key(queue_name: str) -> str:
        return f"jobs:worker_heartbeat:{queue_name}"


def _copy_job_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        **record,
        "params": _copy_jsonish(record.get("params", {})),
        "progress": _copy_jsonish(record.get("progress", {})),
        "result": _copy_jsonish(record.get("result")),
        "error": _copy_jsonish(record.get("error")),
    }


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _copy_jsonish(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.loads(json.dumps(value, default=str))
    return value
