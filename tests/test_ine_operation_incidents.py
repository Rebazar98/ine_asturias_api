from __future__ import annotations

import pytest

from app.services.ine_operation_incidents import (
    INE_INCIDENT_TYPE_HEAVY_TABLE_THRESHOLD_ABORT,
    INE_INCIDENT_TYPE_REPEATED_FAILURES,
    INE_INCIDENT_TYPE_REPEATED_NO_DATA,
    INE_INCIDENT_TYPE_SERIES_DIRECT_BLOCKED,
    attach_ine_incident_profiles,
    build_ine_incident_signal_map,
    evaluate_ine_operation_incidents,
    filter_ine_operation_incidents,
    paginate_ine_operation_incidents,
    summarize_ine_operation_incidents,
)
from app.settings import Settings


class DummyIncidentRepo:
    def __init__(self) -> None:
        self.opened: list[dict] = []
        self.resolved: list[dict] = []

    async def open_or_update_incident(self, **kwargs):
        self.opened.append(dict(kwargs))
        return kwargs

    async def resolve_open_incident(self, **kwargs):
        self.resolved.append(dict(kwargs))
        if kwargs.get("incident_type") == INE_INCIDENT_TYPE_REPEATED_FAILURES:
            return kwargs
        return None


def _settings(**overrides) -> Settings:
    return Settings(app_env="local", job_store_backend="memory", **overrides)


def _profile(**overrides):
    data = {
        "operation_code": "71",
        "execution_profile": "scheduled",
        "schedule_enabled": True,
        "background_required": False,
    }
    data.update(overrides)
    return data


def _governance(**overrides):
    data = {
        "operation_code": "71",
        "last_warning_count": 0,
        "failure_streak": 0,
        "no_data_streak": 0,
        "last_tables_succeeded": 1,
        "last_normalized_rows": 1,
        "last_error_message": None,
    }
    data.update(overrides)
    return data


def test_build_ine_incident_signal_map_opens_repeated_failures() -> None:
    settings = _settings(INE_INCIDENT_FAILURE_STREAK_THRESHOLD=2)

    signal_map = build_ine_incident_signal_map(
        settings=settings,
        operation_code="71",
        effective_profile=_profile(),
        governance_state=_governance(failure_streak=2, last_error_message="boom"),
        run_status="failed",
        payload={"message": "boom"},
        background_forced=False,
        background_reason=None,
    )

    assert INE_INCIDENT_TYPE_REPEATED_FAILURES in signal_map
    assert signal_map[INE_INCIDENT_TYPE_REPEATED_FAILURES]["severity"] == "high"


def test_build_ine_incident_signal_map_opens_repeated_no_data() -> None:
    settings = _settings(INE_INCIDENT_NO_DATA_STREAK_THRESHOLD=3)

    signal_map = build_ine_incident_signal_map(
        settings=settings,
        operation_code="22",
        effective_profile=_profile(operation_code="22"),
        governance_state=_governance(
            operation_code="22",
            no_data_streak=3,
            last_tables_succeeded=0,
            last_normalized_rows=0,
        ),
        run_status="completed",
        payload={"summary": {"normalized_rows": 0}},
        background_forced=False,
        background_reason=None,
    )

    assert INE_INCIDENT_TYPE_REPEATED_NO_DATA in signal_map
    assert signal_map[INE_INCIDENT_TYPE_REPEATED_NO_DATA]["severity"] == "low"


def test_build_ine_incident_signal_map_detects_heavy_table_and_series_direct() -> None:
    settings = _settings()

    signal_map = build_ine_incident_signal_map(
        settings=settings,
        operation_code="23",
        effective_profile=_profile(
            operation_code="23",
            execution_profile="background_only",
            schedule_enabled=False,
            background_required=True,
        ),
        governance_state=_governance(operation_code="23"),
        run_status="failed",
        payload={
            "message": "Series direct fallback blocked by configured cardinality limit.",
            "warnings": [{"warning": "table_processing_aborted_by_threshold"}],
            "series_direct_max_series": 5000,
        },
        background_forced=False,
        background_reason=None,
    )

    assert INE_INCIDENT_TYPE_HEAVY_TABLE_THRESHOLD_ABORT in signal_map
    assert INE_INCIDENT_TYPE_SERIES_DIRECT_BLOCKED in signal_map


@pytest.mark.anyio
async def test_evaluate_ine_operation_incidents_resolves_and_opens_expected_signals() -> None:
    repo = DummyIncidentRepo()
    settings = _settings()

    transitions = await evaluate_ine_operation_incidents(
        repo=repo,
        settings=settings,
        operation_code="71",
        effective_profile=_profile(),
        governance_state=_governance(
            failure_streak=2,
            no_data_streak=0,
            last_tables_succeeded=1,
            last_normalized_rows=5,
        ),
        run_status="completed",
        job_id="job-71",
        payload={"warnings": [{"warning": "table_processing_aborted_by_threshold"}]},
        background_forced=False,
        background_reason=None,
    )

    assert any(
        item["incident_type"] == INE_INCIDENT_TYPE_HEAVY_TABLE_THRESHOLD_ABORT
        for item in repo.opened
    )
    assert any(
        item["incident_type"] == INE_INCIDENT_TYPE_REPEATED_FAILURES for item in repo.resolved
    )
    assert len(transitions) == 2


def test_incident_helpers_attach_filter_paginate_and_summarize() -> None:
    incidents = attach_ine_incident_profiles(
        [
            {
                "incident_id": 1,
                "operation_code": "71",
                "incident_type": "repeated_failures",
                "severity": "high",
                "status": "open",
                "title": "x",
                "message": "x",
                "metadata": {},
            },
            {
                "incident_id": 2,
                "operation_code": "23",
                "incident_type": "series_direct_blocked",
                "severity": "medium",
                "status": "resolved",
                "title": "y",
                "message": "y",
                "metadata": {},
            },
        ],
        [
            _profile(operation_code="71"),
            _profile(
                operation_code="23",
                execution_profile="background_only",
                schedule_enabled=False,
                background_required=True,
            ),
        ],
    )

    filtered = filter_ine_operation_incidents(
        incidents,
        status="open",
        execution_profile="scheduled",
    )
    paginated, pagination = paginate_ine_operation_incidents(filtered, page=1, page_size=10)
    summary = summarize_ine_operation_incidents(filtered)

    assert len(paginated) == 1
    assert paginated[0]["operation_code"] == "71"
    assert pagination["total"] == 1
    assert summary["open_total"] == 1
    assert paginated[0]["suggested_override_profile"] == "background_only"
    assert paginated[0]["requires_manual_confirmation"] is True
    assert "failing repeatedly" in paginated[0]["recommended_reason"]
