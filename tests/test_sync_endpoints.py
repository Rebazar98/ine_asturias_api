"""Tests for GET /sync/status endpoint."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.core.jobs import InMemoryJobStore
from app.dependencies import (
    get_ine_operation_governance_repository,
    get_ine_operation_governance_history_repository,
    get_job_store,
    get_settings,
)
from app.main import create_app
from app.settings import Settings


class DummyGovernanceRepository:
    def __init__(self, rows: list[dict] | None = None):
        self._rows = {row["operation_code"]: dict(row) for row in (rows or [])}

    async def list_all(self) -> list[dict]:
        return list(self._rows.values())

    async def get_by_operation_code(self, operation_code: str) -> dict | None:
        row = self._rows.get(operation_code)
        return dict(row) if row else None

    async def set_override(
        self,
        *,
        operation_code: str,
        execution_profile: str,
        schedule_enabled: bool,
        decision_reason: str,
        decision_source: str,
        commit: bool = True,
    ) -> dict:
        row = dict(self._rows.get(operation_code, {"operation_code": operation_code}))
        row.update(
            {
                "execution_profile": execution_profile,
                "schedule_enabled": schedule_enabled,
                "decision_reason": decision_reason,
                "decision_source": decision_source,
                "metadata": row.get("metadata", {}),
                "override_active": True,
                "override_execution_profile": execution_profile,
                "override_schedule_enabled": schedule_enabled,
                "override_decision_reason": decision_reason,
                "override_decision_source": decision_source,
                "override_applied_at": None,
            }
        )
        self._rows[operation_code] = row
        return dict(row)

    async def clear_override(self, operation_code: str, *, commit: bool = True) -> dict | None:
        row = self._rows.get(operation_code)
        if row is None:
            return None
        row = dict(row)
        row.update(
            {
                "override_active": False,
                "override_execution_profile": None,
                "override_schedule_enabled": None,
                "override_decision_reason": None,
                "override_decision_source": None,
                "override_applied_at": None,
            }
        )
        self._rows[operation_code] = row
        return dict(row)


class DummyGovernanceHistoryRepository:
    def __init__(self) -> None:
        self._events: list[dict] = []
        self._next_id = 1

    async def append_event(self, **kwargs) -> dict:
        event = dict(kwargs)
        event.setdefault("occurred_at", datetime(2026, 3, 24, tzinfo=UTC))
        event["event_id"] = self._next_id
        self._next_id += 1
        self._events.append(event)
        return dict(event)

    async def list_by_operation_code(
        self, operation_code: str, *, page: int, page_size: int
    ) -> list[dict]:
        items = [e for e in self._events if e["operation_code"] == operation_code]
        items = list(reversed(items))
        start = (page - 1) * page_size
        end = start + page_size
        return [dict(item) for item in items[start:end]]

    async def count_by_operation_code(self, operation_code: str) -> int:
        return len([e for e in self._events if e["operation_code"] == operation_code])

    async def summarize_by_operation_code(self, operation_code: str) -> dict[str, int]:
        items = [e for e in self._events if e["operation_code"] == operation_code]
        return {
            "events_total": len(items),
            "override_set_total": len([e for e in items if e["event_type"] == "override_set"]),
            "override_updated_total": len(
                [e for e in items if e["event_type"] == "override_updated"]
            ),
            "override_cleared_total": len(
                [e for e in items if e["event_type"] == "override_cleared"]
            ),
        }


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
    history_repo = DummyGovernanceHistoryRepository()
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_job_store] = lambda: store
    app.dependency_overrides[get_ine_operation_governance_repository] = lambda: governance_repo
    app.dependency_overrides[get_ine_operation_governance_history_repository] = lambda: history_repo
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


def test_sync_ine_operations_returns_semantic_catalog_contract():
    client = _make_client()
    data = client.get("/sync/ine/operations").json()

    assert data["source"] == "internal.sync.ine_operation_catalog"
    assert "generated_at" in data
    assert "summary" in data
    assert "items" in data
    assert "filters" in data
    assert "pagination" in data
    assert data["summary"]["operations_total"] == 10
    assert data["pagination"]["page"] == 1


def test_sync_ine_operations_filters_by_execution_profile():
    client = _make_client()
    data = client.get("/sync/ine/operations?execution_profile=background_only").json()

    assert data["summary"]["operations_total"] == 1
    assert [item["operation_code"] for item in data["items"]] == ["23"]


def test_sync_ine_operations_filters_by_last_run_status():
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
    data = client.get("/sync/ine/operations?last_run_status=completed").json()

    assert data["summary"]["operations_total"] == 1
    assert data["items"][0]["operation_code"] == "71"
    assert data["items"][0]["last_run_status"] == "completed"


def test_sync_ine_operations_excludes_unclassified_when_requested():
    client = _make_client(
        governance_rows=[
            {
                "operation_code": "999",
                "execution_profile": "manual_only",
                "schedule_enabled": False,
                "decision_reason": "manual_review_only",
                "decision_source": "operator_override",
                "metadata": {"configured": False},
                "background_required": False,
                "last_job_id": None,
                "last_run_status": None,
                "last_trigger_mode": None,
                "last_background_forced": False,
                "last_background_reason": None,
                "last_run_started_at": None,
                "last_run_finished_at": None,
                "last_duration_ms": None,
                "last_tables_found": None,
                "last_tables_selected": None,
                "last_tables_succeeded": None,
                "last_tables_failed": None,
                "last_tables_skipped_catalog": None,
                "last_normalized_rows": None,
                "last_warning_count": None,
                "last_error_message": None,
                "created_at": None,
                "updated_at": None,
            }
        ]
    )
    data = client.get("/sync/ine/operations?include_unclassified=false").json()
    operation_codes = {item["operation_code"] for item in data["items"]}

    assert "999" not in operation_codes
    assert data["metadata"]["configured_operations_total"] == 10


def test_sync_ine_operations_supports_pagination():
    client = _make_client()
    data = client.get("/sync/ine/operations?page=2&page_size=3").json()

    assert data["pagination"]["page"] == 2
    assert data["pagination"]["page_size"] == 3
    assert data["pagination"]["has_previous"] is True
    assert len(data["items"]) == 3


def test_sync_ine_operations_exposes_override_origin_fields():
    client = _make_client(
        governance_rows=[
            {
                "operation_code": "353",
                "execution_profile": "scheduled",
                "schedule_enabled": True,
                "decision_reason": "manual_override_active",
                "decision_source": "manual_override_api",
                "metadata": {"configured": True},
                "override_active": True,
                "override_execution_profile": "scheduled",
                "override_schedule_enabled": True,
                "override_decision_reason": "promoted_temporarily",
                "override_decision_source": "manual_override_api",
                "override_applied_at": None,
            }
        ]
    )

    data = client.get("/sync/ine/operations?operation_code=353").json()

    assert data["items"][0]["profile_origin"] == "override"
    assert data["items"][0]["override_active"] is True
    assert data["items"][0]["override_execution_profile"] == "scheduled"
    assert data["items"][0]["baseline_execution_profile"] == "manual_only"


def test_sync_ine_operations_override_endpoint_persists_manual_override():
    client = _make_client()

    response = client.post(
        "/sync/ine/operations/353/override",
        json={
            "execution_profile": "scheduled",
            "decision_reason": "promoted_temporarily",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["operation_code"] == "353"
    assert data["execution_profile"] == "scheduled"
    assert data["profile_origin"] == "override"
    assert data["override_active"] is True
    assert data["schedule_enabled"] is True


def test_sync_ine_operations_override_endpoint_rejects_inconsistent_schedule_enabled():
    client = _make_client()

    response = client.post(
        "/sync/ine/operations/23/override",
        json={
            "execution_profile": "background_only",
            "decision_reason": "keep_manual",
            "schedule_enabled": True,
        },
    )

    assert response.status_code == 422


def test_sync_ine_operations_clear_override_restores_baseline():
    client = _make_client(
        governance_rows=[
            {
                "operation_code": "353",
                "execution_profile": "scheduled",
                "schedule_enabled": True,
                "decision_reason": "manual_override_active",
                "decision_source": "manual_override_api",
                "metadata": {"configured": True},
                "override_active": True,
                "override_execution_profile": "scheduled",
                "override_schedule_enabled": True,
                "override_decision_reason": "promoted_temporarily",
                "override_decision_source": "manual_override_api",
                "override_applied_at": None,
            }
        ]
    )

    response = client.delete("/sync/ine/operations/353/override")

    assert response.status_code == 200
    data = response.json()
    assert data["execution_profile"] == "manual_only"
    assert data["profile_origin"] == "baseline"
    assert data["override_active"] is False


def test_sync_ine_operation_history_returns_events_for_override_lifecycle():
    client = _make_client()

    first = client.post(
        "/sync/ine/operations/353/override",
        json={
            "execution_profile": "scheduled",
            "decision_reason": "promoted_temporarily",
        },
    )
    assert first.status_code == 200

    second = client.post(
        "/sync/ine/operations/353/override",
        json={
            "execution_profile": "background_only",
            "decision_reason": "downgraded_to_background",
        },
    )
    assert second.status_code == 200

    third = client.delete("/sync/ine/operations/353/override")
    assert third.status_code == 200

    response = client.get("/sync/ine/operations/353/history")
    assert response.status_code == 200
    data = response.json()

    assert data["source"] == "internal.sync.ine_operation_history"
    assert data["operation_code"] == "353"
    assert data["summary"]["events_total"] == 3
    assert data["summary"]["override_set_total"] == 1
    assert data["summary"]["override_updated_total"] == 1
    assert data["summary"]["override_cleared_total"] == 1
    assert [item["event_type"] for item in data["items"]] == [
        "override_cleared",
        "override_updated",
        "override_set",
    ]


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


def test_sync_ine_operations_requires_api_key_in_staging():
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
    client = TestClient(app)

    response = client.get("/sync/ine/operations")
    assert response.status_code == 401


def test_sync_ine_operations_accessible_in_local_env_without_key():
    settings = Settings(API_KEY="secret", APP_ENV="local")
    app = create_app()
    store = InMemoryJobStore()
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_job_store] = lambda: store
    app.dependency_overrides[get_ine_operation_governance_repository] = lambda: (
        DummyGovernanceRepository()
    )
    client = TestClient(app)

    response = client.get("/sync/ine/operations")
    assert response.status_code == 200
