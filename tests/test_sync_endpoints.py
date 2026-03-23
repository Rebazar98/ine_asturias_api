"""Tests for GET /sync/status endpoint."""
from __future__ import annotations

from fastapi.testclient import TestClient

from app.core.jobs import InMemoryJobStore
from app.dependencies import get_job_store, get_settings
from app.main import create_app
from app.settings import Settings


def _make_client(worker_heartbeat: dict | None = None):
    """Build a TestClient with an InMemoryJobStore pre-seeded with optional heartbeat."""

    class DummyJobStore(InMemoryJobStore):
        def __init__(self, heartbeat: dict | None):
            super().__init__()
            self._heartbeat = heartbeat

        async def get_worker_status(self, queue_name: str) -> dict:
            if self._heartbeat is None:
                return {"status": "error", "queue_name": queue_name, "message": "worker heartbeat missing"}
            return {"status": "ok", "queue_name": queue_name, **self._heartbeat}

    settings = Settings(
        API_KEY="test-key",
        SCHEDULED_INE_OPERATIONS=["22", "30"],
        SADEI_SYNC_DATASETS=["padron_municipal"],
        IDEAS_SYNC_LAYERS=["limites_parroquiales"],
        SCHEDULED_TERRITORIAL_SYNC_ENABLED=True,
    )

    app = create_app()
    store = DummyJobStore(heartbeat=worker_heartbeat)
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_job_store] = lambda: store
    return TestClient(app, headers={"X-API-Key": "test-key"})


def test_sync_status_returns_200():
    client = _make_client(worker_heartbeat={"worker_id": "w-1"})
    response = client.get("/sync/status")
    assert response.status_code == 200


def test_sync_status_worker_ok():
    client = _make_client(worker_heartbeat={"worker_id": "w-1"})
    data = client.get("/sync/status").json()
    assert data["worker"]["status"] == "ok"
    assert data["worker"]["worker_id"] == "w-1"


def test_sync_status_worker_missing_heartbeat():
    client = _make_client(worker_heartbeat=None)
    data = client.get("/sync/status").json()
    assert data["worker"]["status"] == "error"
    assert "heartbeat" in data["worker"]["message"]


def test_sync_status_sources_structure():
    client = _make_client()
    data = client.get("/sync/status").json()
    sources = data["sources"]
    source_names = {s["source"] for s in sources}
    assert source_names == {"ine", "sadei", "ideas", "territorial"}


def test_sync_status_ine_source():
    client = _make_client()
    data = client.get("/sync/status").json()
    ine = next(s for s in data["sources"] if s["source"] == "ine")
    assert ine["enabled"] is True
    assert "22" in ine["operations"]
    assert "30" in ine["operations"]
    assert "03:00" in ine["schedule"]


def test_sync_status_sadei_source():
    client = _make_client()
    data = client.get("/sync/status").json()
    sadei = next(s for s in data["sources"] if s["source"] == "sadei")
    assert sadei["enabled"] is True
    assert "padron_municipal" in sadei["datasets"]
    assert "05:00" in sadei["schedule"]


def test_sync_status_ideas_source():
    client = _make_client()
    data = client.get("/sync/status").json()
    ideas = next(s for s in data["sources"] if s["source"] == "ideas")
    assert ideas["enabled"] is True
    assert "limites_parroquiales" in ideas["layers"]
    assert "04:30" in ideas["schedule"]


def test_sync_status_territorial_source():
    client = _make_client()
    data = client.get("/sync/status").json()
    territorial = next(s for s in data["sources"] if s["source"] == "territorial")
    assert territorial["enabled"] is True
    assert "04:00" in territorial["schedule"]


def test_sync_status_territorial_disabled():
    settings = Settings(
        API_KEY="test-key",
        SCHEDULED_TERRITORIAL_SYNC_ENABLED=False,
    )
    app = create_app()
    store = InMemoryJobStore()
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_job_store] = lambda: store
    client = TestClient(app, headers={"X-API-Key": "test-key"})

    data = client.get("/sync/status").json()
    territorial = next(s for s in data["sources"] if s["source"] == "territorial")
    assert territorial["enabled"] is False


def test_sync_status_requires_api_key_in_staging():
    """In staging env, missing API key returns 401."""
    settings = Settings(
        API_KEY="a-valid-staging-key-123456789",
        APP_ENV="staging",
        POSTGRES_DSN="postgresql+asyncpg://postgres:super-secure-db-pass-1234@db:5432/ine_asturias",
    )
    app = create_app()
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_job_store] = lambda: InMemoryJobStore()
    client = TestClient(app)  # no API key header
    response = client.get("/sync/status")
    assert response.status_code == 401


def test_sync_status_accessible_in_local_env_without_key():
    """In local/dev env, auth is not enforced — endpoint is accessible without key."""
    settings = Settings(API_KEY="secret", APP_ENV="local")
    app = create_app()
    store = InMemoryJobStore()
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_job_store] = lambda: store
    client = TestClient(app)  # no API key header
    response = client.get("/sync/status")
    assert response.status_code == 200
