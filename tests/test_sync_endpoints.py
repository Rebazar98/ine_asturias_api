"""Tests for GET /sync/status endpoint."""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.core.jobs import InMemoryJobStore
from app.dependencies import (
    get_ine_operation_governance_repository,
    get_job_store,
    get_settings,
)
from app.main import create_app
from app.settings import Settings


class DummyGovernanceRepository:
    def __init__(self, rows: list[dict] | None = None):
        self._rows = rows or []

    async def list_all(self) -> list[dict]:
        return list(self._rows)


def _make_client(
    worker_heartbeat: dict | None = None,
    governance_rows: list[dict] | None = None,
):
    """Build a TestClient with an InMemoryJobStore pre-seeded with optional heartbeat."""

    class DummyJobStore(InMemoryJobStore):
        def __init__(self, heartbeat: dict | None):
            super().__init__()
            self._heartbeat = heartbeat

        async def get_worker_status(self, queue_name: str) -> dict:
            if self._heartbeat is None:
                return {
                    "status": "error",
                    "queue_name": queue_name,
                    "message": "worker heartbeat missing",
                }
            return {"status": "ok", "queue_name": queue_name, **self._heartbeat}

    settings = Settings(
        API_KEY="test-key",
        SCHEDULED_INE_OPERATIONS=["71", "22", "33"],
        HEAVY_INE_OPERATIONS=["23"],
        MANUAL_ONLY_INE_OPERATIONS=["353"],
        DISCARDED_INE_OPERATIONS=["10", "21", "30", "72", "293"],
        SADEI_SYNC_DATASETS=["padron_municipal"],
        IDEAS_SYNC_LAYERS=["limites_parroquiales"],
        SCHEDULED_TERRITORIAL_SYNC_ENABLED=True,
    )

    app = create_app()
    store = DummyJobStore(heartbeat=worker_heartbeat)
    governance_repo = DummyGovernanceRepository(rows=governance_rows)
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_job_store] = lambda: store
    app.dependency_overrides[get_ine_operation_governance_repository] = lambda: governance_repo
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
    assert "71" in ine["operations"]
    assert "22" in ine["operations"]
    assert "33" in ine["operations"]
    assert "03:00" in ine["schedule"]
    assert ine["execution_profiles_available"] == 10


def test_sync_status_exposes_governed_operation_profiles():
    client = _make_client()
    data = client.get("/sync/status").json()
    profiles = {item["operation_code"]: item for item in data["operation_profiles"]}

    assert profiles["71"]["execution_profile"] == "scheduled"
    assert profiles["71"]["schedule_enabled"] is True
    assert profiles["23"]["execution_profile"] == "background_only"
    assert profiles["23"]["background_required"] is True
    assert profiles["353"]["execution_profile"] == "manual_only"
    assert profiles["21"]["execution_profile"] == "discarded"


def test_sync_status_overlays_last_run_state_from_governance_repo():
    client = _make_client(
        governance_rows=[
            {
                "operation_code": "71",
                "execution_profile": "scheduled",
                "schedule_enabled": True,
                "decision_reason": "scheduled_shortlist_campaign_v2",
                "decision_source": "runtime_settings",
                "metadata": {"configured": True},
                "background_required": False,
                "last_job_id": "job-71",
                "last_run_status": "completed",
                "last_trigger_mode": "scheduled",
                "last_background_forced": False,
                "last_background_reason": None,
                "last_run_started_at": None,
                "last_run_finished_at": None,
                "last_duration_ms": 4200,
                "last_tables_found": 171,
                "last_tables_selected": 3,
                "last_tables_succeeded": 2,
                "last_tables_failed": 0,
                "last_tables_skipped_catalog": 5,
                "last_normalized_rows": 10958,
                "last_warning_count": 1,
                "last_error_message": None,
                "created_at": None,
                "updated_at": None,
            }
        ]
    )
    data = client.get("/sync/status").json()
    profiles = {item["operation_code"]: item for item in data["operation_profiles"]}
    assert profiles["71"]["last_job_id"] == "job-71"
    assert profiles["71"]["last_run_status"] == "completed"
    assert profiles["71"]["last_normalized_rows"] == 10958


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
    app.dependency_overrides[get_ine_operation_governance_repository] = lambda: (
        DummyGovernanceRepository()
    )
    client = TestClient(app, headers={"X-API-Key": "test-key"})

    data = client.get("/sync/status").json()
    territorial = next(s for s in data["sources"] if s["source"] == "territorial")
    assert territorial["enabled"] is False


def test_sync_status_requires_api_key_in_staging():
    """In staging env, missing API key returns 401."""
    settings = Settings(
        API_KEY="a-valid-staging-key-123456789",
        APP_ENV="staging",
        POSTGRES_DSN=(
            "postgresql+asyncpg://postgres:super-secure-db-pass-1234@db:5432/ine_asturias"
        ),
    )
    app = create_app()
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_job_store] = lambda: InMemoryJobStore()
    app.dependency_overrides[get_ine_operation_governance_repository] = lambda: (
        DummyGovernanceRepository()
    )
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
    app.dependency_overrides[get_ine_operation_governance_repository] = lambda: (
        DummyGovernanceRepository()
    )
    client = TestClient(app)  # no API key header
    response = client.get("/sync/status")
    assert response.status_code == 200
