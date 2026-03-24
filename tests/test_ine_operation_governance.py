from __future__ import annotations

from app.services.ine_operation_governance import (
    build_configured_ine_operation_profiles,
    filter_ine_operation_profiles,
    merge_ine_operation_profiles,
    paginate_ine_operation_profiles,
    resolve_ine_operation_profile,
    summarize_ine_operation_profiles,
)
from app.settings import Settings


def _settings(**overrides) -> Settings:
    return Settings(
        APP_ENV="local",
        SCHEDULED_INE_OPERATIONS=["71", "22", "33"],
        HEAVY_INE_OPERATIONS=["23"],
        MANUAL_ONLY_INE_OPERATIONS=["353"],
        DISCARDED_INE_OPERATIONS=["10", "21", "30", "72", "293"],
        **overrides,
    )


def test_resolve_ine_operation_profile_for_each_profile_kind() -> None:
    settings = _settings()

    assert resolve_ine_operation_profile(settings, "71")["execution_profile"] == "scheduled"
    assert resolve_ine_operation_profile(settings, "23")["execution_profile"] == "background_only"
    assert resolve_ine_operation_profile(settings, "353")["execution_profile"] == "manual_only"
    assert resolve_ine_operation_profile(settings, "21")["execution_profile"] == "discarded"


def test_resolve_ine_operation_profile_marks_background_only_as_required() -> None:
    settings = _settings()

    scheduled = resolve_ine_operation_profile(settings, "71")
    heavy = resolve_ine_operation_profile(settings, "23")

    assert scheduled["background_required"] is False
    assert heavy["background_required"] is True


def test_build_configured_ine_operation_profiles_returns_all_configured_operations() -> None:
    settings = _settings()

    profiles = build_configured_ine_operation_profiles(settings)
    operation_codes = [profile["operation_code"] for profile in profiles]

    assert operation_codes == ["22", "33", "71", "23", "353", "10", "21", "293", "30", "72"]


def test_merge_ine_operation_profiles_overlays_persisted_runtime_state() -> None:
    settings = _settings()

    merged = merge_ine_operation_profiles(
        settings,
        [
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
                "last_duration_ms": 5100,
                "last_normalized_rows": 10958,
                "last_warning_count": 1,
            }
        ],
    )

    profiles = {profile["operation_code"]: profile for profile in merged}
    assert profiles["71"]["last_job_id"] == "job-71"
    assert profiles["71"]["last_run_status"] == "completed"
    assert profiles["71"]["last_normalized_rows"] == 10958
    assert profiles["23"]["background_required"] is True


def test_merge_ine_operation_profiles_keeps_persisted_unclassified_operations() -> None:
    settings = _settings()

    merged = merge_ine_operation_profiles(
        settings,
        [
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
                "last_duration_ms": None,
                "last_normalized_rows": None,
                "last_warning_count": None,
            }
        ],
    )

    profiles = {profile["operation_code"]: profile for profile in merged}
    assert profiles["999"]["execution_profile"] == "manual_only"
    assert profiles["999"]["decision_source"] == "operator_override"


def test_filter_ine_operation_profiles_supports_profile_and_configured_filters() -> None:
    settings = _settings()
    profiles = merge_ine_operation_profiles(
        settings,
        [
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
                "last_duration_ms": None,
                "last_normalized_rows": None,
                "last_warning_count": None,
            }
        ],
    )

    background_only = filter_ine_operation_profiles(
        profiles,
        execution_profile="background_only",
    )
    configured_only = filter_ine_operation_profiles(
        profiles,
        include_unclassified=False,
    )

    assert [item["operation_code"] for item in background_only] == ["23"]
    assert "999" not in {item["operation_code"] for item in configured_only}


def test_summarize_and_paginate_ine_operation_profiles() -> None:
    settings = _settings()
    profiles = build_configured_ine_operation_profiles(settings)

    summary = summarize_ine_operation_profiles(profiles)
    items, pagination = paginate_ine_operation_profiles(profiles, page=2, page_size=3)

    assert summary["operations_total"] == 10
    assert summary["scheduled_total"] == 3
    assert summary["background_only_total"] == 1
    assert pagination["page"] == 2
    assert pagination["page_size"] == 3
    assert pagination["has_previous"] is True
    assert len(items) == 3
