from __future__ import annotations

from typing import Any

from app.settings import Settings


INE_EXECUTION_PROFILE_SCHEDULED = "scheduled"
INE_EXECUTION_PROFILE_BACKGROUND_ONLY = "background_only"
INE_EXECUTION_PROFILE_MANUAL_ONLY = "manual_only"
INE_EXECUTION_PROFILE_DISCARDED = "discarded"

INE_EXECUTION_PROFILE_ORDER = {
    INE_EXECUTION_PROFILE_SCHEDULED: 0,
    INE_EXECUTION_PROFILE_BACKGROUND_ONLY: 1,
    INE_EXECUTION_PROFILE_MANUAL_ONLY: 2,
    INE_EXECUTION_PROFILE_DISCARDED: 3,
}


def resolve_ine_operation_profile(settings: Settings, operation_code: str) -> dict[str, Any]:
    if operation_code in settings.scheduled_ine_operations:
        return {
            "operation_code": operation_code,
            "execution_profile": INE_EXECUTION_PROFILE_SCHEDULED,
            "schedule_enabled": True,
            "decision_reason": "scheduled_shortlist_campaign_v2",
            "decision_source": "runtime_settings",
            "background_required": False,
            "metadata": {"configured": True},
        }

    if operation_code in settings.heavy_ine_operations:
        return {
            "operation_code": operation_code,
            "execution_profile": INE_EXECUTION_PROFILE_BACKGROUND_ONLY,
            "schedule_enabled": False,
            "decision_reason": "heavy_operation_requires_background",
            "decision_source": "runtime_settings",
            "background_required": True,
            "metadata": {"configured": True},
        }

    if operation_code in settings.manual_only_ine_operations:
        return {
            "operation_code": operation_code,
            "execution_profile": INE_EXECUTION_PROFILE_MANUAL_ONLY,
            "schedule_enabled": False,
            "decision_reason": "manual_exploration_only",
            "decision_source": "runtime_settings",
            "background_required": False,
            "metadata": {"configured": True},
        }

    if operation_code in settings.discarded_ine_operations:
        return {
            "operation_code": operation_code,
            "execution_profile": INE_EXECUTION_PROFILE_DISCARDED,
            "schedule_enabled": False,
            "decision_reason": "discarded_by_prioritization_campaign",
            "decision_source": "runtime_settings",
            "background_required": False,
            "metadata": {"configured": True},
        }

    return {
        "operation_code": operation_code,
        "execution_profile": INE_EXECUTION_PROFILE_MANUAL_ONLY,
        "schedule_enabled": False,
        "decision_reason": "unclassified_operation",
        "decision_source": "runtime_settings",
        "background_required": False,
        "metadata": {"configured": False},
    }


def build_configured_ine_operation_profiles(settings: Settings) -> list[dict[str, Any]]:
    operation_codes = {
        *settings.scheduled_ine_operations,
        *settings.heavy_ine_operations,
        *settings.manual_only_ine_operations,
        *settings.discarded_ine_operations,
    }
    profiles = [
        resolve_ine_operation_profile(settings, operation_code)
        for operation_code in operation_codes
    ]
    return sort_ine_operation_profiles(profiles)


def merge_ine_operation_profiles(
    settings: Settings,
    persisted_profiles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged = {
        profile["operation_code"]: profile
        for profile in build_configured_ine_operation_profiles(settings)
    }
    for profile in persisted_profiles:
        operation_code = profile["operation_code"]
        configured = merged.get(operation_code)
        background_required = profile.get(
            "background_required",
            configured["background_required"] if configured else False,
        )
        merged[operation_code] = {
            **(configured or resolve_ine_operation_profile(settings, operation_code)),
            **profile,
            "background_required": background_required,
        }
    return sort_ine_operation_profiles(list(merged.values()))


def sort_ine_operation_profiles(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        profiles,
        key=lambda profile: (
            INE_EXECUTION_PROFILE_ORDER.get(str(profile.get("execution_profile", "")), 99),
            str(profile.get("operation_code", "")),
        ),
    )
