from __future__ import annotations

from math import ceil
from typing import Any

from app.repositories.ine_operation_incidents import INEOperationIncidentRepository


INE_INCIDENT_STATUS_OPEN = "open"
INE_INCIDENT_STATUS_RESOLVED = "resolved"

INE_INCIDENT_TYPE_REPEATED_FAILURES = "repeated_failures"
INE_INCIDENT_TYPE_REPEATED_NO_DATA = "repeated_no_data"
INE_INCIDENT_TYPE_HEAVY_TABLE_THRESHOLD_ABORT = "heavy_table_threshold_abort"
INE_INCIDENT_TYPE_SERIES_DIRECT_BLOCKED = "series_direct_blocked"
INE_INCIDENT_TYPE_BACKGROUND_FORCED_HEAVY_OPERATION = "background_forced_heavy_operation"
INE_INCIDENT_TYPE_HIGH_WARNING_RATE = "high_warning_rate"

INE_INCIDENT_SEVERITY_HIGH = "high"
INE_INCIDENT_SEVERITY_MEDIUM = "medium"
INE_INCIDENT_SEVERITY_LOW = "low"

INE_INCIDENT_ACTION_REVIEW_MANUAL = "review_manual"
INE_INCIDENT_ACTION_DOWNGRADE_TO_BACKGROUND = "downgrade_to_background"
INE_INCIDENT_ACTION_KEEP_SCHEDULED_OBSERVE = "keep_scheduled_observe"
INE_INCIDENT_ACTION_CONSIDER_DISCARDING = "consider_discarding"
INE_INCIDENT_ACTION_PROMOTE_MANUAL_CAMPAIGN = "promote_manual_campaign"

INE_INCIDENT_TYPES = {
    INE_INCIDENT_TYPE_REPEATED_FAILURES,
    INE_INCIDENT_TYPE_REPEATED_NO_DATA,
    INE_INCIDENT_TYPE_HEAVY_TABLE_THRESHOLD_ABORT,
    INE_INCIDENT_TYPE_SERIES_DIRECT_BLOCKED,
    INE_INCIDENT_TYPE_BACKGROUND_FORCED_HEAVY_OPERATION,
    INE_INCIDENT_TYPE_HIGH_WARNING_RATE,
}


def _extract_warnings(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    warnings = payload.get("warnings")
    if isinstance(warnings, list):
        return [item for item in warnings if isinstance(item, dict)]
    detail = payload.get("detail")
    if isinstance(detail, dict):
        detail_warnings = detail.get("warnings")
        if isinstance(detail_warnings, list):
            return [item for item in detail_warnings if isinstance(item, dict)]
    return []


def _warning_names(payload: Any) -> set[str]:
    return {
        str(item.get("warning"))
        for item in _extract_warnings(payload)
        if item.get("warning") not in (None, "")
    }


def _warning_count(payload: Any, governance_state: dict[str, Any]) -> int:
    extracted = len(_extract_warnings(payload))
    if extracted:
        return extracted
    return int(governance_state.get("last_warning_count") or 0)


def _is_series_direct_blocked(error: Any) -> bool:
    if not isinstance(error, dict):
        return False
    message = str(error.get("message") or "")
    return (
        "Series direct fallback blocked by configured cardinality limit" in message
        or "series_direct_max_series" in error
    )


def _is_no_data_run(governance_state: dict[str, Any]) -> bool:
    tables_succeeded = int(governance_state.get("last_tables_succeeded") or 0)
    normalized_rows = int(governance_state.get("last_normalized_rows") or 0)
    return tables_succeeded == 0 or normalized_rows == 0


def _incident_suggested_action(
    *,
    incident_type: str,
    effective_profile: dict[str, Any],
) -> str:
    schedule_enabled = bool(effective_profile.get("schedule_enabled"))
    if incident_type == INE_INCIDENT_TYPE_HEAVY_TABLE_THRESHOLD_ABORT and schedule_enabled:
        return INE_INCIDENT_ACTION_DOWNGRADE_TO_BACKGROUND
    if incident_type == INE_INCIDENT_TYPE_REPEATED_NO_DATA and schedule_enabled:
        return INE_INCIDENT_ACTION_CONSIDER_DISCARDING
    if incident_type == INE_INCIDENT_TYPE_BACKGROUND_FORCED_HEAVY_OPERATION:
        return INE_INCIDENT_ACTION_DOWNGRADE_TO_BACKGROUND
    if incident_type == INE_INCIDENT_TYPE_REPEATED_FAILURES:
        return INE_INCIDENT_ACTION_REVIEW_MANUAL
    if incident_type == INE_INCIDENT_TYPE_SERIES_DIRECT_BLOCKED:
        return (
            INE_INCIDENT_ACTION_DOWNGRADE_TO_BACKGROUND
            if schedule_enabled
            else INE_INCIDENT_ACTION_REVIEW_MANUAL
        )
    if incident_type == INE_INCIDENT_TYPE_HIGH_WARNING_RATE:
        return (
            INE_INCIDENT_ACTION_KEEP_SCHEDULED_OBSERVE
            if schedule_enabled
            else INE_INCIDENT_ACTION_REVIEW_MANUAL
        )
    return INE_INCIDENT_ACTION_REVIEW_MANUAL


def _incident_severity(
    *,
    incident_type: str,
    effective_profile: dict[str, Any],
) -> str:
    schedule_enabled = bool(effective_profile.get("schedule_enabled"))
    if incident_type == INE_INCIDENT_TYPE_REPEATED_FAILURES:
        return INE_INCIDENT_SEVERITY_HIGH
    if incident_type == INE_INCIDENT_TYPE_SERIES_DIRECT_BLOCKED:
        return INE_INCIDENT_SEVERITY_HIGH if schedule_enabled else INE_INCIDENT_SEVERITY_MEDIUM
    if incident_type in {
        INE_INCIDENT_TYPE_HEAVY_TABLE_THRESHOLD_ABORT,
        INE_INCIDENT_TYPE_BACKGROUND_FORCED_HEAVY_OPERATION,
        INE_INCIDENT_TYPE_HIGH_WARNING_RATE,
    }:
        return INE_INCIDENT_SEVERITY_MEDIUM
    return INE_INCIDENT_SEVERITY_LOW


def _incident_title_message(
    *,
    incident_type: str,
    operation_code: str,
    effective_profile: dict[str, Any],
    governance_state: dict[str, Any],
    payload: Any,
    background_reason: str | None,
) -> tuple[str, str]:
    if incident_type == INE_INCIDENT_TYPE_REPEATED_FAILURES:
        streak = int(governance_state.get("failure_streak") or 0)
        title = f"Operation {operation_code} is failing repeatedly"
        message = (
            f"Operation {operation_code} has failed {streak} consecutive runs. "
            f"Last error: {governance_state.get('last_error_message') or 'unknown'}"
        )
        return title, message
    if incident_type == INE_INCIDENT_TYPE_REPEATED_NO_DATA:
        streak = int(governance_state.get("no_data_streak") or 0)
        title = f"Operation {operation_code} is yielding no useful Asturias data"
        message = (
            f"Operation {operation_code} completed without useful Asturias data in "
            f"{streak} consecutive runs while profile "
            f"{effective_profile.get('execution_profile')} remains active."
        )
        return title, message
    if incident_type == INE_INCIDENT_TYPE_HEAVY_TABLE_THRESHOLD_ABORT:
        title = f"Operation {operation_code} hit the heavy-table abort threshold"
        message = (
            f"Operation {operation_code} aborted at least one table because it exceeded "
            "the configured processing threshold."
        )
        return title, message
    if incident_type == INE_INCIDENT_TYPE_SERIES_DIRECT_BLOCKED:
        title = f"Operation {operation_code} hit the series-direct guardrail"
        message = (
            f"Operation {operation_code} triggered the series-direct cardinality guardrail. "
            f"Detail: {payload.get('message') if isinstance(payload, dict) else 'blocked'}"
        )
        return title, message
    if incident_type == INE_INCIDENT_TYPE_BACKGROUND_FORCED_HEAVY_OPERATION:
        title = f"Operation {operation_code} required forced background execution"
        message = (
            f"Operation {operation_code} was forced to background mode because "
            f"{background_reason or 'it is classified as heavy'}."
        )
        return title, message
    title = f"Operation {operation_code} completed with a high warning rate"
    message = (
        f"Operation {operation_code} emitted many warnings while producing limited value. "
        f"Warning count: {_warning_count(payload, governance_state)}."
    )
    return title, message


def build_ine_incident_signal_map(
    *,
    settings: Any,
    operation_code: str,
    effective_profile: dict[str, Any],
    governance_state: dict[str, Any],
    run_status: str,
    payload: Any,
    background_forced: bool,
    background_reason: str | None,
) -> dict[str, dict[str, Any]]:
    warning_names = _warning_names(payload)
    signals: dict[str, dict[str, Any]] = {}

    if run_status == "failed" and int(governance_state.get("failure_streak") or 0) >= int(
        settings.ine_incident_failure_streak_threshold
    ):
        signals[INE_INCIDENT_TYPE_REPEATED_FAILURES] = {}

    if (
        run_status == "completed"
        and bool(effective_profile.get("schedule_enabled"))
        and int(governance_state.get("no_data_streak") or 0)
        >= int(settings.ine_incident_no_data_streak_threshold)
        and _is_no_data_run(governance_state)
    ):
        signals[INE_INCIDENT_TYPE_REPEATED_NO_DATA] = {}

    if "table_processing_aborted_by_threshold" in warning_names:
        signals[INE_INCIDENT_TYPE_HEAVY_TABLE_THRESHOLD_ABORT] = {}

    if _is_series_direct_blocked(payload):
        signals[INE_INCIDENT_TYPE_SERIES_DIRECT_BLOCKED] = {}

    if (
        background_forced
        and background_reason == "heavy_operation_requires_background"
        and bool(effective_profile.get("schedule_enabled"))
    ):
        signals[INE_INCIDENT_TYPE_BACKGROUND_FORCED_HEAVY_OPERATION] = {}

    if _warning_count(payload, governance_state) >= int(
        settings.ine_incident_warning_count_threshold
    ) and _is_no_data_run(governance_state):
        signals[INE_INCIDENT_TYPE_HIGH_WARNING_RATE] = {}

    for incident_type in list(signals):
        severity = _incident_severity(
            incident_type=incident_type,
            effective_profile=effective_profile,
        )
        suggested_action = _incident_suggested_action(
            incident_type=incident_type,
            effective_profile=effective_profile,
        )
        title, message = _incident_title_message(
            incident_type=incident_type,
            operation_code=operation_code,
            effective_profile=effective_profile,
            governance_state=governance_state,
            payload=payload,
            background_reason=background_reason,
        )
        signals[incident_type] = {
            "severity": severity,
            "title": title,
            "message": message,
            "suggested_action": suggested_action,
            "metadata": {
                "execution_profile": effective_profile.get("execution_profile"),
                "schedule_enabled": effective_profile.get("schedule_enabled"),
                "background_required": effective_profile.get("background_required"),
                "failure_streak": governance_state.get("failure_streak"),
                "no_data_streak": governance_state.get("no_data_streak"),
                "last_warning_count": governance_state.get("last_warning_count"),
                "warning_names": sorted(warning_names),
                "background_forced": background_forced,
                "background_reason": background_reason,
            },
        }

    return signals


async def evaluate_ine_operation_incidents(
    *,
    repo: INEOperationIncidentRepository,
    settings: Any,
    operation_code: str,
    effective_profile: dict[str, Any],
    governance_state: dict[str, Any],
    run_status: str,
    job_id: str | None,
    payload: Any,
    background_forced: bool,
    background_reason: str | None,
) -> None:
    signal_map = build_ine_incident_signal_map(
        settings=settings,
        operation_code=operation_code,
        effective_profile=effective_profile,
        governance_state=governance_state,
        run_status=run_status,
        payload=payload,
        background_forced=background_forced,
        background_reason=background_reason,
    )

    should_resolve = set()
    if run_status == "completed":
        should_resolve.update(
            {
                INE_INCIDENT_TYPE_REPEATED_FAILURES,
                INE_INCIDENT_TYPE_REPEATED_NO_DATA,
                INE_INCIDENT_TYPE_HEAVY_TABLE_THRESHOLD_ABORT,
                INE_INCIDENT_TYPE_SERIES_DIRECT_BLOCKED,
                INE_INCIDENT_TYPE_BACKGROUND_FORCED_HEAVY_OPERATION,
                INE_INCIDENT_TYPE_HIGH_WARNING_RATE,
            }
        )
    elif run_status == "failed":
        should_resolve.update(
            {
                INE_INCIDENT_TYPE_HEAVY_TABLE_THRESHOLD_ABORT,
                INE_INCIDENT_TYPE_SERIES_DIRECT_BLOCKED,
                INE_INCIDENT_TYPE_BACKGROUND_FORCED_HEAVY_OPERATION,
                INE_INCIDENT_TYPE_HIGH_WARNING_RATE,
            }
        )

    for incident_type in should_resolve - set(signal_map):
        await repo.resolve_open_incident(
            operation_code=operation_code,
            incident_type=incident_type,
            last_job_id=job_id,
            last_run_status=run_status,
            metadata={
                "resolved_after_run_status": run_status,
                "execution_profile": effective_profile.get("execution_profile"),
            },
        )

    for incident_type, signal in signal_map.items():
        await repo.open_or_update_incident(
            operation_code=operation_code,
            incident_type=incident_type,
            severity=signal["severity"],
            title=signal["title"],
            message=signal["message"],
            last_job_id=job_id,
            last_run_status=run_status,
            suggested_action=signal["suggested_action"],
            metadata=signal["metadata"],
        )


def attach_ine_incident_profiles(
    incidents: list[dict[str, Any]],
    operation_profiles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    profiles_by_operation = {
        item["operation_code"]: item for item in operation_profiles if item.get("operation_code")
    }
    attached: list[dict[str, Any]] = []
    for item in incidents:
        profile = profiles_by_operation.get(str(item.get("operation_code")), {})
        attached.append(
            {
                **item,
                "execution_profile": profile.get("execution_profile"),
                "schedule_enabled": profile.get("schedule_enabled", False),
                "background_required": profile.get("background_required", False),
            }
        )
    return attached


def filter_ine_operation_incidents(
    incidents: list[dict[str, Any]],
    *,
    operation_code: str | None = None,
    status: str | None = None,
    severity: str | None = None,
    incident_type: str | None = None,
    execution_profile: str | None = None,
) -> list[dict[str, Any]]:
    items = incidents
    if operation_code is not None:
        items = [item for item in items if item.get("operation_code") == operation_code]
    if status is not None:
        items = [item for item in items if item.get("status") == status]
    if severity is not None:
        items = [item for item in items if item.get("severity") == severity]
    if incident_type is not None:
        items = [item for item in items if item.get("incident_type") == incident_type]
    if execution_profile is not None:
        items = [item for item in items if item.get("execution_profile") == execution_profile]
    return items


def summarize_ine_operation_incidents(incidents: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "incidents_total": len(incidents),
        "open_total": sum(
            1 for item in incidents if item.get("status") == INE_INCIDENT_STATUS_OPEN
        ),
        "resolved_total": sum(
            1 for item in incidents if item.get("status") == INE_INCIDENT_STATUS_RESOLVED
        ),
        "high_total": sum(
            1 for item in incidents if item.get("severity") == INE_INCIDENT_SEVERITY_HIGH
        ),
        "medium_total": sum(
            1 for item in incidents if item.get("severity") == INE_INCIDENT_SEVERITY_MEDIUM
        ),
        "low_total": sum(
            1 for item in incidents if item.get("severity") == INE_INCIDENT_SEVERITY_LOW
        ),
    }


def paginate_ine_operation_incidents(
    incidents: list[dict[str, Any]],
    *,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, Any]], dict[str, int | bool]]:
    total = len(incidents)
    start = (page - 1) * page_size
    end = start + page_size
    pages = ceil(total / page_size) if total else 0
    return incidents[start:end], {
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": pages,
        "has_next": page < pages,
        "has_previous": page > 1 and total > 0,
    }
