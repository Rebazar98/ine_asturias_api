from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models import INEOperationIncident
from app.repositories.ine_operation_incidents import INEOperationIncidentRepository


def _make_row(
    operation_code: str = "71", incident_type: str = "repeated_failures"
) -> INEOperationIncident:
    row = INEOperationIncident(
        operation_code=operation_code,
        incident_type=incident_type,
        severity="high",
        status="open",
        title="Operation is failing repeatedly",
        message="failure streak",
        first_seen_at=datetime(2026, 3, 24, tzinfo=UTC),
        last_seen_at=datetime(2026, 3, 24, tzinfo=UTC),
        last_resolved_at=None,
        occurrence_count=1,
        last_job_id="job-71",
        last_run_status="failed",
        suggested_action="review_manual",
        metadata_json={"configured": True},
    )
    row.id = 1
    row.created_at = datetime(2026, 3, 24, tzinfo=UTC)
    row.updated_at = datetime(2026, 3, 24, tzinfo=UTC)
    return row


@pytest.mark.anyio
async def test_list_filtered_returns_empty_without_session() -> None:
    repo = INEOperationIncidentRepository(session=None)
    assert await repo.list_filtered() == []


@pytest.mark.anyio
async def test_open_or_update_incident_raises_without_session() -> None:
    repo = INEOperationIncidentRepository(session=None)
    with pytest.raises(RuntimeError, match="No database session"):
        await repo.open_or_update_incident(
            operation_code="71",
            incident_type="repeated_failures",
            severity="high",
            title="x",
            message="x",
            last_job_id="job-71",
            last_run_status="failed",
            suggested_action="review_manual",
        )


@pytest.mark.anyio
async def test_open_or_update_incident_creates_new_row() -> None:
    session = AsyncMock()
    fake_result = MagicMock()
    fake_result.scalars.return_value.first.return_value = None
    session.execute = AsyncMock(return_value=fake_result)
    session.add = MagicMock()
    session.refresh = AsyncMock()

    repo = INEOperationIncidentRepository(session=session)
    result = await repo.open_or_update_incident(
        operation_code="71",
        incident_type="repeated_failures",
        severity="high",
        title="Operation 71 is failing repeatedly",
        message="failure streak",
        last_job_id="job-71",
        last_run_status="failed",
        suggested_action="review_manual",
    )

    assert result["operation_code"] == "71"
    assert result["incident_type"] == "repeated_failures"
    assert result["notification_event"] == "opened"
    session.add.assert_called_once()
    session.commit.assert_awaited()


@pytest.mark.anyio
async def test_open_or_update_incident_updates_existing_open_row() -> None:
    row = _make_row()
    session = AsyncMock()
    fake_result = MagicMock()
    fake_result.scalars.return_value.first.return_value = row
    session.execute = AsyncMock(return_value=fake_result)
    session.refresh = AsyncMock()

    repo = INEOperationIncidentRepository(session=session)
    result = await repo.open_or_update_incident(
        operation_code="71",
        incident_type="repeated_failures",
        severity="high",
        title="Operation 71 is failing repeatedly",
        message="failure streak",
        last_job_id="job-72",
        last_run_status="failed",
        suggested_action="review_manual",
    )

    assert result["occurrence_count"] == 2
    assert row.last_job_id == "job-72"
    assert result["notification_event"] == "updated"


@pytest.mark.anyio
async def test_open_or_update_incident_marks_severity_escalation() -> None:
    row = _make_row()
    row.severity = "medium"
    session = AsyncMock()
    fake_result = MagicMock()
    fake_result.scalars.return_value.first.return_value = row
    session.execute = AsyncMock(return_value=fake_result)
    session.refresh = AsyncMock()

    repo = INEOperationIncidentRepository(session=session)
    result = await repo.open_or_update_incident(
        operation_code="71",
        incident_type="repeated_failures",
        severity="high",
        title="Operation 71 is failing repeatedly",
        message="failure streak",
        last_job_id="job-72",
        last_run_status="failed",
        suggested_action="review_manual",
    )

    assert result["notification_event"] == "severity_escalated"
    assert result["previous_severity"] == "medium"


@pytest.mark.anyio
async def test_resolve_open_incident_marks_row_resolved() -> None:
    row = _make_row()
    session = AsyncMock()
    fake_result = MagicMock()
    fake_result.scalars.return_value.first.return_value = row
    session.execute = AsyncMock(return_value=fake_result)
    session.refresh = AsyncMock()

    repo = INEOperationIncidentRepository(session=session)
    result = await repo.resolve_open_incident(
        operation_code="71",
        incident_type="repeated_failures",
        last_job_id="job-73",
        last_run_status="completed",
    )

    assert result is not None
    assert result["status"] == "resolved"
    assert row.last_run_status == "completed"
    assert result["notification_event"] == "resolved"


@pytest.mark.anyio
async def test_merge_metadata_updates_existing_incident_metadata() -> None:
    row = _make_row()
    session = AsyncMock()
    session.get = AsyncMock(return_value=row)
    session.refresh = AsyncMock()

    repo = INEOperationIncidentRepository(session=session)
    result = await repo.merge_metadata(
        incident_id=1,
        metadata={"last_notified_at": "2026-03-24T03:00:00Z", "notification_channels": ["slack"]},
    )

    assert result is not None
    assert result["metadata"]["last_notified_at"] == "2026-03-24T03:00:00Z"
    assert result["metadata"]["notification_channels"] == ["slack"]


@pytest.mark.anyio
async def test_summarize_filtered_aggregates_counts() -> None:
    row_one = _make_row(operation_code="71", incident_type="repeated_failures")
    row_two = _make_row(operation_code="23", incident_type="series_direct_blocked")
    row_two.id = 2
    row_two.severity = "medium"
    row_two.status = "resolved"
    session = AsyncMock()
    fake_result = MagicMock()
    fake_result.scalars.return_value.all.return_value = [row_one, row_two]
    session.execute = AsyncMock(return_value=fake_result)

    repo = INEOperationIncidentRepository(session=session)
    summary = await repo.summarize_filtered()

    assert summary["incidents_total"] == 2
    assert summary["open_total"] == 1
    assert summary["resolved_total"] == 1
    assert summary["high_total"] == 1
    assert summary["medium_total"] == 1
