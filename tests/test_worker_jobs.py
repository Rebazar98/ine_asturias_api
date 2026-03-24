"""Unit tests for worker job functions — no DB, no Redis, no HTTP required."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from app.core.jobs import InMemoryJobStore
from app.core.logging import request_id_var
from app.schemas import AnalyticalResponse
from app.schemas import AnalyticalTerritorialContextResponse, TerritorialExportResultResponse
from app.services.asturias_resolver import AsturiasResolutionError
from app.services.catastro_client import CatastroUpstreamError
from app.services.ine_client import INEUpstreamError
from app.services.ideas_wfs_client import IDEASWFSClientError
from app.services.sadei_client import SADEIClientError
from app.settings import Settings
from app.worker import (
    _heartbeat_loop,
    _start_worker_metrics_server,
    run_municipality_report_job,
    run_ideas_sync_job,
    run_operation_asturias_job,
    run_sadei_sync_job,
    run_territorial_export_job,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _settings() -> Settings:
    return Settings(app_env="local", job_store_backend="memory")


def _base_ctx(job_store: InMemoryJobStore) -> dict[str, Any]:
    return {"settings": _settings(), "job_store": job_store}


@asynccontextmanager
async def _failing_session_scope():
    """Async context manager that always raises — simulates an uninitialised DB."""
    raise RuntimeError("DB engine not initialised")
    yield  # pragma: no cover


# ---------------------------------------------------------------------------
# run_operation_asturias_job
# ---------------------------------------------------------------------------


def test_resolution_error_marks_job_failed() -> None:
    """AsturiasResolutionError in resolver is caught; job fails only if ingestion also fails."""

    class FailingResolver:
        async def resolve(self, **kwargs: Any) -> Any:
            raise AsturiasResolutionError(detail={"message": "Cannot resolve operation"})

    class FailingIngestionService:
        def __init__(self, **kwargs: Any) -> None:
            pass

        async def ingest_asturias_operation(self, **kwargs: Any) -> Any:
            raise AsturiasResolutionError(
                detail={"message": "Cannot resolve operation"}, status_code=404
            )

    @asynccontextmanager
    async def _mock_session():
        yield object()

    async def scenario() -> None:
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)
        ctx["resolver"] = FailingResolver()
        ctx["ine_client"] = None

        job = await job_store.create_job("test", {"operation_code": "22"})

        with (
            patch("app.worker.session_scope", new=_mock_session),
            patch("app.worker.INEOperationIngestionService", new=FailingIngestionService),
        ):
            result = await run_operation_asturias_job(ctx, job["job_id"], {"operation_code": "22"})

        assert result is None
        record = await job_store.get_job(job["job_id"])
        assert record is not None
        assert record["status"] == "failed"
        assert record["error"]["message"] == "Cannot resolve operation"

    asyncio.run(scenario())


def test_ine_client_error_marks_job_failed() -> None:
    """INEClientError raised inside the ingestion service must fail the job."""

    class OkResolver:
        async def resolve(self, **kwargs: Any) -> Any:
            return SimpleNamespace(geo_variable_id="v1", asturias_value_id="a1")

    class FailingIngestionService:
        def __init__(self, **kwargs: Any) -> None:
            pass

        async def ingest_asturias_operation(self, **kwargs: Any) -> Any:
            raise INEUpstreamError(status_code=503, detail={"message": "INE service unavailable"})

    @asynccontextmanager
    async def _mock_session():
        yield object()  # bare session; repos won't be used

    async def scenario() -> None:
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)
        ctx["resolver"] = OkResolver()
        ctx["ine_client"] = None

        job = await job_store.create_job("test", {"operation_code": "22"})

        with (
            patch("app.worker.session_scope", new=_mock_session),
            patch(
                "app.worker.INEOperationIngestionService",
                new=FailingIngestionService,
            ),
        ):
            result = await run_operation_asturias_job(ctx, job["job_id"], {"operation_code": "22"})

        assert result is None
        record = await job_store.get_job(job["job_id"])
        assert record is not None
        assert record["status"] == "failed"
        assert record["error"]["message"] == "INE service unavailable"

    asyncio.run(scenario())


def test_operation_job_request_id_set_during_execution_and_cleared_after() -> None:
    """request_id_var must be set to payload _request_id during execution and reset after."""
    captured: list[str | None] = []

    class CapturingResolver:
        async def resolve(self, **kwargs: Any) -> Any:
            captured.append(request_id_var.get())
            raise AsturiasResolutionError(detail={"message": "stop"})

    async def scenario() -> None:
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)
        ctx["resolver"] = CapturingResolver()
        ctx["ine_client"] = None

        job = await job_store.create_job("test", {})
        await run_operation_asturias_job(
            ctx,
            job["job_id"],
            {"operation_code": "22", "_request_id": "test-rid-worker"},
        )

    asyncio.run(scenario())

    assert captured == ["test-rid-worker"]
    assert request_id_var.get() is None  # reset in finally


def test_operation_job_generic_exception_marks_failed() -> None:
    """An unexpected Exception not in the typed hierarchy must still fail the job."""

    class ExplodingResolver:
        async def resolve(self, **kwargs: Any) -> Any:
            raise ValueError("something went very wrong")

    async def scenario() -> None:
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)
        ctx["resolver"] = ExplodingResolver()
        ctx["ine_client"] = None

        job = await job_store.create_job("test", {"operation_code": "30"})
        result = await run_operation_asturias_job(ctx, job["job_id"], {"operation_code": "30"})

        assert result is None
        record = await job_store.get_job(job["job_id"])
        assert record is not None
        assert record["status"] == "failed"
        assert "something went very wrong" in str(record["error"])

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# run_municipality_report_job
# ---------------------------------------------------------------------------


def test_municipality_report_db_failure_marks_job_failed() -> None:
    """When session_scope raises (DB down), the job must be marked failed."""

    async def scenario() -> None:
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)

        job = await job_store.create_job("test", {"municipality_code": "33044"})

        with patch("app.worker.session_scope", new=_failing_session_scope):
            result = await run_municipality_report_job(
                ctx, job["job_id"], {"municipality_code": "33044"}
            )

        assert result is None
        record = await job_store.get_job(job["job_id"])
        assert record is not None
        assert record["status"] == "failed"
        assert "33044" in str(record["error"])

    asyncio.run(scenario())


def test_municipality_report_request_id_cleared_after_db_failure() -> None:
    """request_id_var must be reset to None even when session_scope raises."""

    async def scenario() -> None:
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)

        job = await job_store.create_job("test", {})

        with patch("app.worker.session_scope", new=_failing_session_scope):
            await run_municipality_report_job(
                ctx,
                job["job_id"],
                {"municipality_code": "33001", "_request_id": "rid-muni"},
            )

        assert request_id_var.get() is None

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# run_territorial_export_job
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _ok_session_scope():
    """Session scope mock that yields a bare object — enough for service construction."""
    yield object()


def test_territorial_export_catastro_error_marks_job_failed() -> None:
    """CatastroClientError raised by TerritorialExportService must fail the job."""

    class FailingExportService:
        def __init__(self, **kwargs):
            pass

        async def build_export(self, **kwargs):
            raise CatastroUpstreamError(status_code=503, detail={"message": "Catastro unavailable"})

    async def scenario():
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)
        ctx.update({"http_client": None, "cache": None, "catastro_circuit_breaker": None})

        job = await job_store.create_job(
            "test", {"unit_level": "municipality", "code_value": "33044"}
        )
        with (
            patch("app.worker.session_scope", new=_ok_session_scope),
            patch("app.worker.TerritorialExportService", new=FailingExportService),
        ):
            result = await run_territorial_export_job(
                ctx, job["job_id"], {"unit_level": "municipality", "code_value": "33044"}
            )

        assert result is None
        record = await job_store.get_job(job["job_id"])
        assert record is not None
        assert record["status"] == "failed"
        assert record["error"]["message"] == "Catastro unavailable"

    asyncio.run(scenario())


def test_territorial_export_generic_exception_marks_job_failed() -> None:
    """An unexpected Exception must be caught and the job marked failed."""

    async def scenario():
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)
        ctx.update({"http_client": None, "cache": None, "catastro_circuit_breaker": None})

        job = await job_store.create_job(
            "test", {"unit_level": "municipality", "code_value": "33044"}
        )
        with patch("app.worker.session_scope", new=_failing_session_scope):
            result = await run_territorial_export_job(
                ctx, job["job_id"], {"unit_level": "municipality", "code_value": "33044"}
            )

        assert result is None
        record = await job_store.get_job(job["job_id"])
        assert record is not None
        assert record["status"] == "failed"
        assert "error" in record

    asyncio.run(scenario())


def test_territorial_export_none_result_marks_job_failed_with_unit_not_found() -> None:
    """When build_export returns None the job fails with unit-not-found detail."""

    class NoneExportService:
        def __init__(self, **kwargs):
            pass

        async def build_export(self, **kwargs):
            return None

    async def scenario():
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)
        ctx.update({"http_client": None, "cache": None, "catastro_circuit_breaker": None})

        job = await job_store.create_job("test", {"unit_level": "ccaa", "code_value": "99"})
        with (
            patch("app.worker.session_scope", new=_ok_session_scope),
            patch("app.worker.TerritorialExportService", new=NoneExportService),
        ):
            result = await run_territorial_export_job(
                ctx, job["job_id"], {"unit_level": "ccaa", "code_value": "99"}
            )

        assert result is None
        record = await job_store.get_job(job["job_id"])
        assert record is not None
        assert record["status"] == "failed"
        assert "unit_level" in record["error"]
        assert record["error"]["code_value"] == "99"

    asyncio.run(scenario())


def test_territorial_export_request_id_set_and_cleared() -> None:
    """request_id_var must be propagated from payload and reset after job ends."""
    captured: list[str | None] = []

    class CapturingExportService:
        def __init__(self, **kwargs):
            pass

        async def build_export(self, **kwargs):
            captured.append(request_id_var.get())
            raise CatastroUpstreamError(status_code=503, detail={"message": "stop"})

    async def scenario():
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)
        ctx.update({"http_client": None, "cache": None, "catastro_circuit_breaker": None})

        job = await job_store.create_job("test", {})
        with (
            patch("app.worker.session_scope", new=_ok_session_scope),
            patch("app.worker.TerritorialExportService", new=CapturingExportService),
        ):
            await run_territorial_export_job(
                ctx,
                job["job_id"],
                {"unit_level": "municipality", "code_value": "33044", "_request_id": "rid-export"},
            )

    asyncio.run(scenario())

    assert captured == ["rid-export"]
    assert request_id_var.get() is None


def test_sadei_sync_client_error_marks_job_failed() -> None:
    """SADEIClientError must fail the job with the error detail."""

    class FailingSADEIClient:
        async def fetch_dataset(self, dataset_id):
            raise SADEIClientError(dataset_id=dataset_id, detail="SADEI timeout")

    async def scenario():
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)
        ctx["sadei_client"] = FailingSADEIClient()

        job = await job_store.create_job("test", {"dataset_id": "padron_municipal"})
        result = await run_sadei_sync_job(ctx, job["job_id"], {"dataset_id": "padron_municipal"})

        assert result is None
        record = await job_store.get_job(job["job_id"])
        assert record is not None
        assert record["status"] == "failed"
        assert "padron_municipal" in str(record["error"])

    asyncio.run(scenario())


def test_sadei_sync_generic_exception_marks_job_failed() -> None:
    """An unexpected Exception must be caught and the job marked failed."""

    class ExplodingSADEIClient:
        async def fetch_dataset(self, dataset_id):
            raise ConnectionError("network down")

    async def scenario():
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)
        ctx["sadei_client"] = ExplodingSADEIClient()

        job = await job_store.create_job("test", {"dataset_id": "pib_municipal"})
        result = await run_sadei_sync_job(ctx, job["job_id"], {"dataset_id": "pib_municipal"})

        assert result is None
        record = await job_store.get_job(job["job_id"])
        assert record is not None
        assert record["status"] == "failed"
        assert "network down" in str(record["error"])

    asyncio.run(scenario())


def test_sadei_sync_success_completes_job() -> None:
    """When fetch_dataset and upsert_many succeed, the job must be marked completed."""

    class OkSADEIClient:
        async def fetch_dataset(self, dataset_id):
            return [{"period": "2024", "value": "1.0", "municipality": "33044"}]

    class OkSeriesRepo:
        def __init__(self, **kwargs):
            pass

        async def upsert_many(self, items):
            return len(items)

    async def scenario():
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)
        ctx["sadei_client"] = OkSADEIClient()

        job = await job_store.create_job("test", {"dataset_id": "padron_municipal"})

        with (
            patch("app.worker.session_scope", new=_ok_session_scope),
            patch("app.worker.SeriesRepository", new=OkSeriesRepo),
            patch("app.worker.normalize_sadei_dataset", return_value=[object()]),
        ):
            result = await run_sadei_sync_job(
                ctx, job["job_id"], {"dataset_id": "padron_municipal"}
            )

        assert result is not None
        assert result["rows_fetched"] == 1
        record = await job_store.get_job(job["job_id"])
        assert record is not None
        assert record["status"] == "completed"

    asyncio.run(scenario())


def test_ideas_sync_client_error_marks_job_failed() -> None:
    """IDEASWFSClientError must fail the job with the error detail."""

    class FailingIDEASClient:
        async def fetch_layer(self, layer_name):
            raise IDEASWFSClientError(layer_name=layer_name, detail="WFS timeout")

    async def scenario():
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)
        ctx["ideas_client"] = FailingIDEASClient()

        job = await job_store.create_job("test", {"layer_name": "limites_parroquiales"})
        result = await run_ideas_sync_job(
            ctx, job["job_id"], {"layer_name": "limites_parroquiales"}
        )

        assert result is None
        record = await job_store.get_job(job["job_id"])
        assert record is not None
        assert record["status"] == "failed"
        assert "limites_parroquiales" in str(record["error"])

    asyncio.run(scenario())


def test_ideas_sync_success_completes_job() -> None:
    """When fetch_layer returns a valid GeoJSON, the job must be marked completed."""

    class OkIDEASClient:
        async def fetch_layer(self, layer_name):
            return {"type": "FeatureCollection", "features": [{}, {}]}

    async def scenario():
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)
        ctx["ideas_client"] = OkIDEASClient()

        job = await job_store.create_job("test", {"layer_name": "usos_suelo"})
        result = await run_ideas_sync_job(ctx, job["job_id"], {"layer_name": "usos_suelo"})

        assert result is not None
        assert result["features_fetched"] == 2
        record = await job_store.get_job(job["job_id"])
        assert record is not None
        assert record["status"] == "completed"

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Success paths -- run_operation_asturias_job
# ---------------------------------------------------------------------------


def test_operation_asturias_job_success_completes_job() -> None:
    class OkResolver:
        async def resolve(self, **kwargs):
            return SimpleNamespace(geo_variable_id="v1", asturias_value_id="a1")

    class OkIngestionService:
        def __init__(self, **kwargs):
            pass

        async def ingest_asturias_operation(self, **kwargs):
            return {"summary": {"tables_succeeded": 2, "tables_failed": 0}}

    async def scenario():
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)
        ctx["resolver"] = OkResolver()
        ctx["ine_client"] = None

        job = await job_store.create_job("test", {"operation_code": "22"})
        with (
            patch("app.worker.session_scope", new=_ok_session_scope),
            patch("app.worker.INEOperationIngestionService", new=OkIngestionService),
        ):
            result = await run_operation_asturias_job(ctx, job["job_id"], {"operation_code": "22"})

        assert result is not None
        assert result["summary"]["tables_succeeded"] == 2
        record = await job_store.get_job(job["job_id"])
        assert record["status"] == "completed"

    asyncio.run(scenario())


def test_operation_asturias_job_success_evaluates_incidents() -> None:
    incident_calls: list[dict[str, Any]] = []

    class OkResolver:
        async def resolve(self, **kwargs):
            return SimpleNamespace(geo_variable_id="v1", asturias_value_id="a1")

    class OkIngestionService:
        def __init__(self, **kwargs):
            pass

        async def ingest_asturias_operation(self, **kwargs):
            return {
                "warnings": [],
                "summary": {
                    "tables_succeeded": 2,
                    "tables_failed": 0,
                    "normalized_rows": 10,
                    "warnings": 0,
                },
            }

    async def fake_mark_completed(**kwargs):
        return {
            "operation_code": "22",
            "execution_profile": "scheduled",
            "schedule_enabled": True,
            "decision_reason": "scheduled_shortlist_campaign_v2",
            "decision_source": "runtime_settings",
            "metadata": {"configured": True},
            "failure_streak": 0,
            "no_data_streak": 0,
            "last_tables_succeeded": 2,
            "last_normalized_rows": 10,
            "last_warning_count": 0,
        }

    async def fake_evaluate(**kwargs):
        incident_calls.append(kwargs)

    async def scenario():
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)
        ctx["resolver"] = OkResolver()
        ctx["ine_client"] = None

        job = await job_store.create_job("test", {"operation_code": "22"})
        with (
            patch("app.worker.session_scope", new=_ok_session_scope),
            patch("app.worker.INEOperationIngestionService", new=OkIngestionService),
            patch("app.worker._mark_ine_governance_completed", new=fake_mark_completed),
            patch("app.worker.evaluate_ine_operation_incidents", new=fake_evaluate),
        ):
            await run_operation_asturias_job(ctx, job["job_id"], {"operation_code": "22"})

        assert incident_calls
        assert incident_calls[0]["run_status"] == "completed"
        assert incident_calls[0]["operation_code"] == "22"

    asyncio.run(scenario())


def test_operation_asturias_job_failure_evaluates_incidents() -> None:
    incident_calls: list[dict[str, Any]] = []

    class OkResolver:
        async def resolve(self, **kwargs):
            return SimpleNamespace(geo_variable_id="v1", asturias_value_id="a1")

    class FailingIngestionService:
        def __init__(self, **kwargs):
            pass

        async def ingest_asturias_operation(self, **kwargs):
            raise INEUpstreamError(
                status_code=503,
                detail={"message": "INE service unavailable", "warnings": []},
            )

    async def fake_mark_failed(**kwargs):
        return {
            "operation_code": "22",
            "execution_profile": "scheduled",
            "schedule_enabled": True,
            "decision_reason": "scheduled_shortlist_campaign_v2",
            "decision_source": "runtime_settings",
            "metadata": {"configured": True},
            "failure_streak": 2,
            "no_data_streak": 0,
            "last_tables_succeeded": 0,
            "last_normalized_rows": 0,
            "last_warning_count": 0,
            "last_error_message": "INE service unavailable",
        }

    async def fake_evaluate(**kwargs):
        incident_calls.append(kwargs)

    async def scenario():
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)
        ctx["resolver"] = OkResolver()
        ctx["ine_client"] = None

        job = await job_store.create_job("test", {"operation_code": "22"})
        with (
            patch("app.worker.session_scope", new=_ok_session_scope),
            patch("app.worker.INEOperationIngestionService", new=FailingIngestionService),
            patch("app.worker._mark_ine_governance_failed", new=fake_mark_failed),
            patch("app.worker.evaluate_ine_operation_incidents", new=fake_evaluate),
        ):
            await run_operation_asturias_job(ctx, job["job_id"], {"operation_code": "22"})

        assert incident_calls
        assert incident_calls[0]["run_status"] == "failed"
        assert incident_calls[0]["operation_code"] == "22"

    asyncio.run(scenario())


def test_municipality_report_job_success_completes_job() -> None:
    report = AnalyticalResponse(source="test", generated_at=datetime.now(tz=UTC))

    class OkAnalyticsService:
        def __init__(self, **kwargs):
            pass

        async def build_municipality_report(self, **kwargs):
            return report

    async def scenario():
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)

        job = await job_store.create_job("test", {"municipality_code": "33044"})
        with (
            patch("app.worker.session_scope", new=_ok_session_scope),
            patch("app.worker.TerritorialAnalyticsService", new=OkAnalyticsService),
        ):
            result = await run_municipality_report_job(
                ctx, job["job_id"], {"municipality_code": "33044"}
            )

        assert result is not None
        record = await job_store.get_job(job["job_id"])
        assert record["status"] == "completed"

    asyncio.run(scenario())


def test_municipality_report_job_none_result_marks_failed() -> None:
    class NoneAnalyticsService:
        def __init__(self, **kwargs):
            pass

        async def build_municipality_report(self, **kwargs):
            return None

    async def scenario():
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)

        job = await job_store.create_job("test", {"municipality_code": "99999"})
        with (
            patch("app.worker.session_scope", new=_ok_session_scope),
            patch("app.worker.TerritorialAnalyticsService", new=NoneAnalyticsService),
        ):
            result = await run_municipality_report_job(
                ctx, job["job_id"], {"municipality_code": "99999"}
            )

        assert result is None
        record = await job_store.get_job(job["job_id"])
        assert record["status"] == "failed"
        assert record["error"]["message"] == "Municipality code was not found."

    asyncio.run(scenario())


def test_territorial_export_job_success_completes_job() -> None:
    export_result = TerritorialExportResultResponse(
        export_id=1,
        export_key="test/export.zip",
        territorial_context=AnalyticalTerritorialContextResponse(),
        download_path="/exports/test.zip",
        expires_at=datetime.now(tz=UTC),
    )

    class OkExportService:
        def __init__(self, **kwargs):
            pass

        async def build_export(self, **kwargs):
            return export_result

    async def scenario():
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)
        ctx.update({"http_client": None, "cache": None, "catastro_circuit_breaker": None})

        job = await job_store.create_job(
            "test", {"unit_level": "municipality", "code_value": "33044"}
        )
        with (
            patch("app.worker.session_scope", new=_ok_session_scope),
            patch("app.worker.TerritorialExportService", new=OkExportService),
        ):
            result = await run_territorial_export_job(
                ctx, job["job_id"], {"unit_level": "municipality", "code_value": "33044"}
            )

        assert result is not None
        assert result["export_id"] == 1
        record = await job_store.get_job(job["job_id"])
        assert record["status"] == "completed"

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Generic exception -- run_ideas_sync_job
# ---------------------------------------------------------------------------


def test_ideas_sync_generic_exception_marks_job_failed() -> None:
    class ExplodingIDEASClient:
        async def fetch_layer(self, layer_name):
            raise ConnectionError("WFS unreachable")

    async def scenario():
        job_store = InMemoryJobStore()
        ctx = _base_ctx(job_store)
        ctx["ideas_client"] = ExplodingIDEASClient()

        job = await job_store.create_job("test", {"layer_name": "limites_parroquiales"})
        result = await run_ideas_sync_job(
            ctx, job["job_id"], {"layer_name": "limites_parroquiales"}
        )

        assert result is None
        record = await job_store.get_job(job["job_id"])
        assert record["status"] == "failed"
        assert "WFS unreachable" in str(record["error"])

    asyncio.run(scenario())


def test_heartbeat_loop_calls_record_heartbeat() -> None:
    call_count = 0

    class MockJobStore:
        async def record_worker_heartbeat(self, **kwargs):
            nonlocal call_count
            call_count += 1

    async def scenario():
        # Capture the real asyncio.sleep BEFORE the patch replaces it on the shared
        # asyncio module object. patch("app.worker.asyncio.sleep") patches the attribute
        # on the module itself, so all callers (including this test) would see fake_sleep
        # — causing infinite recursion if fake_sleep calls asyncio.sleep directly.
        real_sleep = asyncio.sleep

        async def fake_sleep(_):
            await real_sleep(0)

        with patch("app.worker.asyncio.sleep", new=fake_sleep):
            task = asyncio.create_task(
                _heartbeat_loop(MockJobStore(), "test-queue", "worker-1", ttl_seconds=10)
            )
            for _ in range(5):
                await real_sleep(0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    asyncio.run(scenario())
    assert call_count >= 1


def test_heartbeat_loop_swallows_exceptions() -> None:
    class FailingJobStore:
        async def record_worker_heartbeat(self, **kwargs):
            raise ConnectionError("Redis down")

    async def scenario():
        real_sleep = asyncio.sleep

        async def fake_sleep(_):
            await real_sleep(0)

        with patch("app.worker.asyncio.sleep", new=fake_sleep):
            task = asyncio.create_task(_heartbeat_loop(FailingJobStore(), "q", "w", ttl_seconds=10))
            for _ in range(5):
                await real_sleep(0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    asyncio.run(scenario())


def test_start_worker_metrics_server_returns_none_on_failure() -> None:
    with patch("app.worker.start_http_server", side_effect=OSError("port in use")):
        result = _start_worker_metrics_server(9090)
    assert result is None


def test_start_worker_metrics_server_returns_server_on_success() -> None:
    mock_server = object()
    with patch("app.worker.start_http_server", return_value=(mock_server, None)):
        result = _start_worker_metrics_server(9090)
    assert result is mock_server
