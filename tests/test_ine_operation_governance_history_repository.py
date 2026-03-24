from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models import INEOperationGovernanceHistory
from app.repositories.ine_operation_governance_history import (
    INEOperationGovernanceHistoryRepository,
)


def _make_row(operation_code: str = "353") -> INEOperationGovernanceHistory:
    row = INEOperationGovernanceHistory(
        operation_code=operation_code,
        event_type="override_set",
        effective_execution_profile_before="manual_only",
        effective_execution_profile_after="scheduled",
        schedule_enabled_before=False,
        schedule_enabled_after=True,
        background_required_before=False,
        background_required_after=False,
        override_active_before=False,
        override_active_after=True,
        decision_reason="promoted_temporarily",
        decision_source="manual_override_api",
        override_decision_reason="promoted_temporarily",
        override_decision_source="manual_override_api",
        metadata_json={"profile_origin_before": "baseline"},
        occurred_at=datetime(2026, 3, 24, tzinfo=UTC),
    )
    row.id = 1
    return row


@pytest.mark.anyio
async def test_append_event_raises_without_session() -> None:
    repo = INEOperationGovernanceHistoryRepository(session=None)
    with pytest.raises(RuntimeError, match="No database session"):
        await repo.append_event(
            operation_code="353",
            event_type="override_set",
            effective_execution_profile_before="manual_only",
            effective_execution_profile_after="scheduled",
            schedule_enabled_before=False,
            schedule_enabled_after=True,
            background_required_before=False,
            background_required_after=False,
            override_active_before=False,
            override_active_after=True,
            decision_reason="promoted_temporarily",
            decision_source="manual_override_api",
            override_decision_reason="promoted_temporarily",
            override_decision_source="manual_override_api",
        )


@pytest.mark.anyio
async def test_list_and_count_return_empty_without_session() -> None:
    repo = INEOperationGovernanceHistoryRepository(session=None)
    assert await repo.list_by_operation_code("353", page=1, page_size=10) == []
    assert await repo.count_by_operation_code("353") == 0
    assert (await repo.summarize_by_operation_code("353"))["events_total"] == 0


@pytest.mark.anyio
async def test_append_event_serializes_row() -> None:
    session = AsyncMock()
    session.add = MagicMock()
    session.refresh = AsyncMock()
    repo = INEOperationGovernanceHistoryRepository(session=session)

    result = await repo.append_event(
        operation_code="353",
        event_type="override_set",
        effective_execution_profile_before="manual_only",
        effective_execution_profile_after="scheduled",
        schedule_enabled_before=False,
        schedule_enabled_after=True,
        background_required_before=False,
        background_required_after=False,
        override_active_before=False,
        override_active_after=True,
        decision_reason="promoted_temporarily",
        decision_source="manual_override_api",
        override_decision_reason="promoted_temporarily",
        override_decision_source="manual_override_api",
        metadata={"profile_origin_before": "baseline"},
    )

    assert result["operation_code"] == "353"
    assert result["event_type"] == "override_set"
    assert result["override_active_after"] is True


@pytest.mark.anyio
async def test_list_by_operation_code_serializes_rows() -> None:
    row = _make_row()
    session = AsyncMock()
    fake_result = MagicMock()
    fake_result.scalars.return_value.all.return_value = [row]
    session.execute = AsyncMock(return_value=fake_result)

    repo = INEOperationGovernanceHistoryRepository(session=session)
    result = await repo.list_by_operation_code("353", page=1, page_size=10)

    assert result[0]["event_id"] == 1
    assert result[0]["event_type"] == "override_set"
    assert result[0]["decision_reason"] == "promoted_temporarily"


@pytest.mark.anyio
async def test_summarize_by_operation_code_groups_events() -> None:
    session = AsyncMock()
    fake_result = MagicMock()
    fake_result.all.return_value = [
        ("override_set", 1),
        ("override_updated", 2),
        ("override_cleared", 1),
    ]
    session.execute = AsyncMock(return_value=fake_result)

    repo = INEOperationGovernanceHistoryRepository(session=session)
    summary = await repo.summarize_by_operation_code("353")

    assert summary["events_total"] == 4
    assert summary["override_set_total"] == 1
    assert summary["override_updated_total"] == 2
    assert summary["override_cleared_total"] == 1
