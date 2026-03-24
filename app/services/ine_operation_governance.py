from __future__ import annotations

from math import ceil
from typing import Any

from app.settings import Settings


INE_EXECUTION_PROFILE_SCHEDULED = "scheduled"
INE_EXECUTION_PROFILE_BACKGROUND_ONLY = "background_only"
INE_EXECUTION_PROFILE_MANUAL_ONLY = "manual_only"
INE_EXECUTION_PROFILE_DISCARDED = "discarded"

INE_PROFILE_ORIGIN_BASELINE = "baseline"
INE_PROFILE_ORIGIN_OVERRIDE = "override"
INE_MANUAL_OVERRIDE_DECISION_SOURCE = "manual_override_api"
INE_HISTORY_EVENT_OVERRIDE_SET = "override_set"
INE_HISTORY_EVENT_OVERRIDE_UPDATED = "override_updated"
INE_HISTORY_EVENT_OVERRIDE_CLEARED = "override_cleared"

INE_EXECUTION_PROFILE_ORDER = {
    INE_EXECUTION_PROFILE_SCHEDULED: 0,
    INE_EXECUTION_PROFILE_BACKGROUND_ONLY: 1,
    INE_EXECUTION_PROFILE_MANUAL_ONLY: 2,
    INE_EXECUTION_PROFILE_DISCARDED: 3,
}


def resolve_ine_operation_profile(settings: Settings, operation_code: str) -> dict[str, Any]:
    if operation_code in settings.scheduled_ine_operations:
        execution_profile = INE_EXECUTION_PROFILE_SCHEDULED
        schedule_enabled = True
        decision_reason = "scheduled_shortlist_campaign_v2"
        configured = True
    elif operation_code in settings.heavy_ine_operations:
        execution_profile = INE_EXECUTION_PROFILE_BACKGROUND_ONLY
        schedule_enabled = False
        decision_reason = "heavy_operation_requires_background"
        configured = True
    elif operation_code in settings.manual_only_ine_operations:
        execution_profile = INE_EXECUTION_PROFILE_MANUAL_ONLY
        schedule_enabled = False
        decision_reason = "manual_exploration_only"
        configured = True
    elif operation_code in settings.discarded_ine_operations:
        execution_profile = INE_EXECUTION_PROFILE_DISCARDED
        schedule_enabled = False
        decision_reason = "discarded_by_prioritization_campaign"
        configured = True
    else:
        execution_profile = INE_EXECUTION_PROFILE_MANUAL_ONLY
        schedule_enabled = False
        decision_reason = "unclassified_operation"
        configured = False

    return {
        "operation_code": operation_code,
        "execution_profile": execution_profile,
        "schedule_enabled": schedule_enabled,
        "decision_reason": decision_reason,
        "decision_source": "runtime_settings",
        "background_required": execution_profile == INE_EXECUTION_PROFILE_BACKGROUND_ONLY,
        "profile_origin": INE_PROFILE_ORIGIN_BASELINE,
        "override_active": False,
        "override_execution_profile": None,
        "override_schedule_enabled": None,
        "override_decision_reason": None,
        "override_decision_source": None,
        "override_applied_at": None,
        "baseline_execution_profile": execution_profile,
        "baseline_schedule_enabled": schedule_enabled,
        "metadata": {
            "configured": configured,
            "override_active": False,
            "baseline_execution_profile": execution_profile,
            "baseline_schedule_enabled": schedule_enabled,
        },
    }


def derive_schedule_enabled_for_profile(execution_profile: str) -> bool:
    return execution_profile == INE_EXECUTION_PROFILE_SCHEDULED


def resolve_effective_ine_operation_profile(
    settings: Settings,
    operation_code: str,
    persisted_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    baseline = resolve_ine_operation_profile(settings, operation_code)
    persisted = persisted_profile or {}
    override_active = bool(persisted.get("override_active"))
    override_execution_profile = persisted.get("override_execution_profile")
    override_schedule_enabled = persisted.get("override_schedule_enabled")
    override_decision_reason = persisted.get("override_decision_reason")
    override_decision_source = persisted.get("override_decision_source")

    effective = {**baseline, **persisted}

    if override_active and override_execution_profile:
        schedule_enabled = (
            override_schedule_enabled
            if override_schedule_enabled is not None
            else derive_schedule_enabled_for_profile(str(override_execution_profile))
        )
        effective.update(
            {
                "execution_profile": override_execution_profile,
                "schedule_enabled": schedule_enabled,
                "decision_reason": override_decision_reason or "manual_override_active",
                "decision_source": override_decision_source or INE_MANUAL_OVERRIDE_DECISION_SOURCE,
                "background_required": (
                    override_execution_profile == INE_EXECUTION_PROFILE_BACKGROUND_ONLY
                ),
                "profile_origin": INE_PROFILE_ORIGIN_OVERRIDE,
            }
        )
    else:
        effective.update(
            {
                "execution_profile": baseline["execution_profile"],
                "schedule_enabled": baseline["schedule_enabled"],
                "decision_reason": baseline["decision_reason"],
                "decision_source": baseline["decision_source"],
                "background_required": baseline["background_required"],
                "profile_origin": INE_PROFILE_ORIGIN_BASELINE,
            }
        )

    effective.update(
        {
            "operation_code": operation_code,
            "override_active": override_active,
            "override_execution_profile": override_execution_profile if override_active else None,
            "override_schedule_enabled": override_schedule_enabled if override_active else None,
            "override_decision_reason": override_decision_reason if override_active else None,
            "override_decision_source": override_decision_source if override_active else None,
            "override_applied_at": persisted.get("override_applied_at")
            if override_active
            else None,
            "baseline_execution_profile": baseline["execution_profile"],
            "baseline_schedule_enabled": baseline["schedule_enabled"],
            "metadata": {
                **(persisted.get("metadata") or {}),
                "configured": bool(baseline["metadata"].get("configured")),
                "override_active": override_active,
                "baseline_execution_profile": baseline["execution_profile"],
                "baseline_schedule_enabled": baseline["schedule_enabled"],
            },
        }
    )
    return effective


def build_configured_ine_operation_profiles(settings: Settings) -> list[dict[str, Any]]:
    operation_codes = {
        *settings.scheduled_ine_operations,
        *settings.heavy_ine_operations,
        *settings.manual_only_ine_operations,
        *settings.discarded_ine_operations,
    }
    profiles = [
        resolve_effective_ine_operation_profile(settings, operation_code)
        for operation_code in operation_codes
    ]
    return sort_ine_operation_profiles(profiles)


def merge_ine_operation_profiles(
    settings: Settings,
    persisted_profiles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    persisted_by_operation = {
        str(profile["operation_code"]): profile for profile in persisted_profiles if profile
    }
    operation_codes = {
        *[
            profile["operation_code"]
            for profile in build_configured_ine_operation_profiles(settings)
        ],
        *persisted_by_operation.keys(),
    }
    merged = [
        resolve_effective_ine_operation_profile(
            settings,
            operation_code,
            persisted_by_operation.get(operation_code),
        )
        for operation_code in operation_codes
    ]
    return sort_ine_operation_profiles(merged)


def list_effective_scheduled_ine_operation_codes(
    settings: Settings,
    persisted_profiles: list[dict[str, Any]],
) -> list[str]:
    return [
        profile["operation_code"]
        for profile in merge_ine_operation_profiles(settings, persisted_profiles)
        if profile.get("execution_profile") == INE_EXECUTION_PROFILE_SCHEDULED
        and bool(profile.get("schedule_enabled"))
    ]


def build_ine_operation_history_event(
    operation_code: str,
    before_profile: dict[str, Any],
    after_profile: dict[str, Any],
) -> dict[str, Any]:
    before_override_active = bool(before_profile.get("override_active"))
    after_override_active = bool(after_profile.get("override_active"))
    if not before_override_active and after_override_active:
        event_type = INE_HISTORY_EVENT_OVERRIDE_SET
    elif before_override_active and after_override_active:
        event_type = INE_HISTORY_EVENT_OVERRIDE_UPDATED
    else:
        event_type = INE_HISTORY_EVENT_OVERRIDE_CLEARED

    return {
        "operation_code": operation_code,
        "event_type": event_type,
        "effective_execution_profile_before": before_profile.get("execution_profile"),
        "effective_execution_profile_after": after_profile.get("execution_profile"),
        "schedule_enabled_before": before_profile.get("schedule_enabled"),
        "schedule_enabled_after": after_profile.get("schedule_enabled"),
        "background_required_before": before_profile.get("background_required"),
        "background_required_after": after_profile.get("background_required"),
        "override_active_before": before_override_active,
        "override_active_after": after_override_active,
        "decision_reason": after_profile.get("decision_reason"),
        "decision_source": after_profile.get("decision_source"),
        "override_decision_reason": (
            after_profile.get("override_decision_reason")
            if after_override_active
            else before_profile.get("override_decision_reason")
        ),
        "override_decision_source": (
            after_profile.get("override_decision_source")
            if after_override_active
            else before_profile.get("override_decision_source")
        ),
        "metadata": {
            "profile_origin_before": before_profile.get("profile_origin"),
            "profile_origin_after": after_profile.get("profile_origin"),
            "baseline_execution_profile": after_profile.get("baseline_execution_profile"),
            "baseline_schedule_enabled": after_profile.get("baseline_schedule_enabled"),
        },
    }


def filter_ine_operation_profiles(
    profiles: list[dict[str, Any]],
    *,
    operation_code: str | None = None,
    execution_profile: str | None = None,
    last_run_status: str | None = None,
    schedule_enabled: bool | None = None,
    include_unclassified: bool = True,
) -> list[dict[str, Any]]:
    items = profiles
    if operation_code is not None:
        items = [item for item in items if item.get("operation_code") == operation_code]
    if execution_profile is not None:
        items = [item for item in items if item.get("execution_profile") == execution_profile]
    if last_run_status is not None:
        items = [item for item in items if item.get("last_run_status") == last_run_status]
    if schedule_enabled is not None:
        items = [item for item in items if bool(item.get("schedule_enabled")) is schedule_enabled]
    if not include_unclassified:
        items = [item for item in items if bool(item.get("metadata", {}).get("configured"))]
    return items


def summarize_ine_operation_profiles(
    profiles: list[dict[str, Any]],
) -> dict[str, int]:
    return {
        "operations_total": len(profiles),
        "scheduled_total": sum(
            1
            for item in profiles
            if item.get("execution_profile") == INE_EXECUTION_PROFILE_SCHEDULED
        ),
        "background_only_total": sum(
            1
            for item in profiles
            if item.get("execution_profile") == INE_EXECUTION_PROFILE_BACKGROUND_ONLY
        ),
        "manual_only_total": sum(
            1
            for item in profiles
            if item.get("execution_profile") == INE_EXECUTION_PROFILE_MANUAL_ONLY
        ),
        "discarded_total": sum(
            1
            for item in profiles
            if item.get("execution_profile") == INE_EXECUTION_PROFILE_DISCARDED
        ),
        "schedule_enabled_total": sum(1 for item in profiles if item.get("schedule_enabled")),
        "with_last_run_total": sum(1 for item in profiles if item.get("last_run_status")),
    }


def paginate_ine_operation_profiles(
    profiles: list[dict[str, Any]],
    *,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, Any]], dict[str, int | bool]]:
    total = len(profiles)
    pages = ceil(total / page_size) if total else 0
    start = (page - 1) * page_size
    end = start + page_size
    items = profiles[start:end]
    pagination = {
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": pages,
        "has_next": page < pages,
        "has_previous": page > 1 and total > 0,
    }
    return items, pagination


def summarize_ine_operation_history_events(
    events: list[dict[str, Any]],
    *,
    events_total: int | None = None,
) -> dict[str, int]:
    total = len(events) if events_total is None else events_total
    return {
        "events_total": total,
        "override_set_total": sum(
            1 for item in events if item.get("event_type") == INE_HISTORY_EVENT_OVERRIDE_SET
        ),
        "override_updated_total": sum(
            1 for item in events if item.get("event_type") == INE_HISTORY_EVENT_OVERRIDE_UPDATED
        ),
        "override_cleared_total": sum(
            1 for item in events if item.get("event_type") == INE_HISTORY_EVENT_OVERRIDE_CLEARED
        ),
    }


def paginate_ine_operation_history_events(
    *,
    total: int,
    page: int,
    page_size: int,
) -> dict[str, int | bool]:
    pages = ceil(total / page_size) if total else 0
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": pages,
        "has_next": page < pages,
        "has_previous": page > 1 and total > 0,
    }


def sort_ine_operation_profiles(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        profiles,
        key=lambda profile: (
            INE_EXECUTION_PROFILE_ORDER.get(str(profile.get("execution_profile", "")), 99),
            str(profile.get("operation_code", "")),
        ),
    )
