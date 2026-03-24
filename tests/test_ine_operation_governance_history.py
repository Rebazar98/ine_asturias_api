from __future__ import annotations

from app.services.ine_operation_governance import (
    build_ine_operation_history_event,
    paginate_ine_operation_history_events,
    summarize_ine_operation_history_events,
)


def test_build_ine_operation_history_event_for_override_set() -> None:
    before = {
        "operation_code": "353",
        "execution_profile": "manual_only",
        "schedule_enabled": False,
        "background_required": False,
        "profile_origin": "baseline",
        "override_active": False,
        "override_decision_reason": None,
        "override_decision_source": None,
    }
    after = {
        "operation_code": "353",
        "execution_profile": "scheduled",
        "schedule_enabled": True,
        "background_required": False,
        "profile_origin": "override",
        "override_active": True,
        "override_decision_reason": "promoted_temporarily",
        "override_decision_source": "manual_override_api",
        "baseline_execution_profile": "manual_only",
        "baseline_schedule_enabled": False,
    }

    event = build_ine_operation_history_event("353", before, after)

    assert event["event_type"] == "override_set"
    assert event["effective_execution_profile_before"] == "manual_only"
    assert event["effective_execution_profile_after"] == "scheduled"
    assert event["metadata"]["profile_origin_before"] == "baseline"
    assert event["metadata"]["profile_origin_after"] == "override"


def test_build_ine_operation_history_event_for_override_updated() -> None:
    before = {
        "operation_code": "353",
        "execution_profile": "scheduled",
        "schedule_enabled": True,
        "background_required": False,
        "profile_origin": "override",
        "override_active": True,
        "override_decision_reason": "promoted_temporarily",
        "override_decision_source": "manual_override_api",
    }
    after = {
        "operation_code": "353",
        "execution_profile": "background_only",
        "schedule_enabled": False,
        "background_required": True,
        "profile_origin": "override",
        "override_active": True,
        "override_decision_reason": "downgraded_to_background",
        "override_decision_source": "manual_override_api",
        "baseline_execution_profile": "manual_only",
        "baseline_schedule_enabled": False,
    }

    event = build_ine_operation_history_event("353", before, after)

    assert event["event_type"] == "override_updated"
    assert event["background_required_after"] is True
    assert event["override_decision_reason"] == "downgraded_to_background"


def test_build_ine_operation_history_event_for_override_cleared() -> None:
    before = {
        "operation_code": "353",
        "execution_profile": "scheduled",
        "schedule_enabled": True,
        "background_required": False,
        "profile_origin": "override",
        "override_active": True,
        "override_decision_reason": "promoted_temporarily",
        "override_decision_source": "manual_override_api",
    }
    after = {
        "operation_code": "353",
        "execution_profile": "manual_only",
        "schedule_enabled": False,
        "background_required": False,
        "decision_reason": "manual_exploration_only",
        "profile_origin": "baseline",
        "override_active": False,
        "override_decision_reason": None,
        "override_decision_source": None,
        "baseline_execution_profile": "manual_only",
        "baseline_schedule_enabled": False,
    }

    event = build_ine_operation_history_event("353", before, after)

    assert event["event_type"] == "override_cleared"
    assert event["override_decision_reason"] == "promoted_temporarily"
    assert event["decision_reason"] == "manual_exploration_only"


def test_summarize_and_paginate_ine_operation_history_events() -> None:
    events = [
        {"event_type": "override_set"},
        {"event_type": "override_updated"},
        {"event_type": "override_cleared"},
    ]

    summary = summarize_ine_operation_history_events(events, events_total=7)
    pagination = paginate_ine_operation_history_events(total=7, page=2, page_size=3)

    assert summary["events_total"] == 7
    assert summary["override_set_total"] == 1
    assert summary["override_updated_total"] == 1
    assert summary["override_cleared_total"] == 1
    assert pagination["page"] == 2
    assert pagination["page_size"] == 3
    assert pagination["has_previous"] is True
