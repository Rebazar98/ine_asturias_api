from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models import INEOperationGovernance
from app.repositories.ine_operation_governance import INEOperationGovernanceRepository


def _make_row(operation_code: str = "71") -> INEOperationGovernance:
    row = INEOperationGovernance(
        operation_code=operation_code,
        execution_profile="scheduled",
        schedule_enabled=True,
        decision_reason="scheduled_shortlist_campaign_v2",
        decision_source="runtime_settings",
        metadata_json={"configured": True},
        override_active=False,
        override_execution_profile=None,
        override_schedule_enabled=None,
        override_decision_reason=None,
        override_decision_source=None,
        override_applied_at=None,
        last_job_id="job-71",
        last_run_status="completed",
        last_trigger_mode="scheduled",
        last_background_forced=False,
        last_background_reason=None,
        last_duration_ms=4200,
        last_tables_found=171,
        last_tables_selected=3,
        last_tables_succeeded=2,
        last_tables_failed=0,
        last_tables_skipped_catalog=5,
        last_normalized_rows=10958,
        last_warning_count=1,
        last_error_message=None,
    )
    row.id = 1
    row.created_at = datetime(2026, 3, 24, tzinfo=UTC)
    row.updated_at = datetime(2026, 3, 24, tzinfo=UTC)
    return row


@pytest.mark.anyio
async def test_list_all_returns_empty_without_session() -> None:
    repo = INEOperationGovernanceRepository(session=None)
    assert await repo.list_all() == []


@pytest.mark.anyio
async def test_get_by_operation_code_returns_none_without_session() -> None:
    repo = INEOperationGovernanceRepository(session=None)
    assert await repo.get_by_operation_code("71") is None


@pytest.mark.anyio
async def test_upsert_profile_raises_without_session() -> None:
    repo = INEOperationGovernanceRepository(session=None)
    with pytest.raises(RuntimeError, match="No database session"):
        await repo.upsert_profile(
            operation_code="71",
            execution_profile="scheduled",
            schedule_enabled=True,
            decision_reason="scheduled_shortlist_campaign_v2",
        )


@pytest.mark.anyio
async def test_set_override_raises_without_session() -> None:
    repo = INEOperationGovernanceRepository(session=None)
    with pytest.raises(RuntimeError, match="No database session"):
        await repo.set_override(
            operation_code="353",
            execution_profile="scheduled",
            schedule_enabled=True,
            decision_reason="promoted_temporarily",
            decision_source="manual_override_api",
        )


@pytest.mark.anyio
async def test_clear_override_raises_without_session() -> None:
    repo = INEOperationGovernanceRepository(session=None)
    with pytest.raises(RuntimeError, match="No database session"):
        await repo.clear_override("353")


@pytest.mark.anyio
async def test_list_all_serializes_rows() -> None:
    row = _make_row()
    session = AsyncMock()
    fake_result = MagicMock()
    fake_result.scalars.return_value.all.return_value = [row]
    session.execute = AsyncMock(return_value=fake_result)

    repo = INEOperationGovernanceRepository(session=session)
    result = await repo.list_all()

    assert result[0]["operation_code"] == "71"
    assert result[0]["execution_profile"] == "scheduled"
    assert result[0]["last_run_status"] == "completed"
    assert result[0]["last_normalized_rows"] == 10958


def test_serialize_sets_background_required_from_profile() -> None:
    row = _make_row(operation_code="23")
    row.execution_profile = "background_only"
    row.schedule_enabled = False

    serialized = INEOperationGovernanceRepository._serialize(row)

    assert serialized["background_required"] is True
    assert serialized["operation_code"] == "23"


@pytest.mark.anyio
async def test_set_override_persists_override_fields() -> None:
    session = AsyncMock()
    fake_missing_result = MagicMock()
    fake_missing_result.scalars.return_value.first.return_value = None
    session.execute = AsyncMock(return_value=fake_missing_result)
    session.add = MagicMock()
    session.refresh = AsyncMock()

    repo = INEOperationGovernanceRepository(session=session)
    result = await repo.set_override(
        operation_code="353",
        execution_profile="scheduled",
        schedule_enabled=True,
        decision_reason="promoted_temporarily",
        decision_source="manual_override_api",
    )

    assert result["operation_code"] == "353"
    assert result["override_active"] is True
    assert result["override_execution_profile"] == "scheduled"
    assert result["override_schedule_enabled"] is True
    session.commit.assert_awaited()
    session.refresh.assert_awaited()


@pytest.mark.anyio
async def test_clear_override_clears_override_fields() -> None:
    row = _make_row(operation_code="353")
    row.override_active = True
    row.override_execution_profile = "scheduled"
    row.override_schedule_enabled = True
    row.override_decision_reason = "promoted_temporarily"
    row.override_decision_source = "manual_override_api"
    session = AsyncMock()
    fake_result = MagicMock()
    fake_result.scalars.return_value.first.return_value = row
    session.execute = AsyncMock(return_value=fake_result)
    session.refresh = AsyncMock()

    repo = INEOperationGovernanceRepository(session=session)
    result = await repo.clear_override("353")

    assert result is not None
    assert result["override_active"] is False
    assert result["override_execution_profile"] is None
    assert result["override_schedule_enabled"] is None
