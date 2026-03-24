from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app.services.ine_operation_incidents import attach_ine_incident_profiles


NOTIFIABLE_INCIDENT_EVENTS = {"opened", "severity_escalated", "resolved"}

PAGERDUTY_SEVERITY_MAP = {
    "high": "critical",
    "medium": "warning",
    "low": "info",
}


def should_notify_ine_incident_transition(
    *,
    settings: Any,
    incident: dict[str, Any],
) -> bool:
    notification_event = str(incident.get("notification_event") or "")
    severity = str(incident.get("severity") or "")
    status = str(incident.get("status") or "")
    if notification_event not in NOTIFIABLE_INCIDENT_EVENTS:
        return False
    if notification_event == "resolved":
        return bool(settings.ine_incident_notify_on_resolved)
    if severity not in set(settings.ine_incident_notify_severities):
        return False
    return status == "open"


def build_ine_incident_notification_payload(
    *,
    incident: dict[str, Any],
) -> dict[str, Any]:
    operation_code = str(incident.get("operation_code") or "")
    payload = {
        "source": "internal.sync.ine_incident_notification",
        "generated_at": datetime.now(UTC).isoformat(),
        "incident": {
            "incident_id": incident.get("incident_id"),
            "operation_code": operation_code,
            "incident_type": incident.get("incident_type"),
            "severity": incident.get("severity"),
            "status": incident.get("status"),
            "title": incident.get("title"),
            "message": incident.get("message"),
            "occurrence_count": incident.get("occurrence_count"),
            "first_seen_at": incident.get("first_seen_at"),
            "last_seen_at": incident.get("last_seen_at"),
            "notification_event": incident.get("notification_event"),
        },
        "operation": {
            "execution_profile": incident.get("execution_profile"),
            "schedule_enabled": incident.get("schedule_enabled"),
            "background_required": incident.get("background_required"),
            "last_run_status": incident.get("last_run_status"),
            "last_job_id": incident.get("last_job_id"),
        },
        "recommendation": {
            "suggested_action": incident.get("suggested_action"),
            "reason": incident.get("recommended_reason"),
            "suggested_override_profile": incident.get("suggested_override_profile"),
            "requires_manual_confirmation": incident.get("requires_manual_confirmation", True),
        },
        "links": {
            "incidents_path": f"/sync/ine/incidents?operation_code={operation_code}",
            "operation_catalog_path": f"/sync/ine/operations?operation_code={operation_code}",
            "history_path": f"/sync/ine/operations/{operation_code}/history",
        },
        "metadata": {
            "notification_event": incident.get("notification_event"),
            "decision_source": incident.get("decision_source"),
        },
    }
    return _json_safe(payload)


def _json_safe(payload: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(payload, default=str))


def _build_slack_payload(notification: dict[str, Any]) -> dict[str, Any]:
    incident = notification["incident"]
    operation = notification["operation"]
    recommendation = notification["recommendation"]
    return {
        "text": (
            f"INE incident {incident['status']}: operation {incident['operation_code']} "
            f"({incident['incident_type']}, {incident['severity']})"
        ),
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*INE incident {incident['status']}*\n"
                        f"Operation `{incident['operation_code']}`\n"
                        f"Type: `{incident['incident_type']}`\n"
                        f"Severity: `{incident['severity']}`"
                    ),
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*Message*\n{incident['message']}\n\n"
                        f"*Profile*: `{operation['execution_profile']}`\n"
                        f"*Suggested action*: `{recommendation['suggested_action']}`\n"
                        f"*Suggested override*: `{recommendation['suggested_override_profile']}`"
                    ),
                },
            },
        ],
    }


def _build_pagerduty_payload(
    *,
    settings: Any,
    notification: dict[str, Any],
) -> dict[str, Any]:
    incident = notification["incident"]
    event_action = "resolve" if incident["status"] == "resolved" else "trigger"
    return {
        "routing_key": settings.pagerduty_key,
        "event_action": event_action,
        "dedup_key": f"ine-incident-{incident['incident_id']}",
        "payload": {
            "summary": (
                f"INE {incident['incident_type']} on operation {incident['operation_code']}"
            ),
            "source": "ine_asturias_api",
            "severity": PAGERDUTY_SEVERITY_MAP.get(str(incident["severity"]), "warning"),
            "custom_details": notification,
        },
    }


async def _send_slack_notification(
    *,
    http_client: Any,
    settings: Any,
    notification: dict[str, Any],
) -> None:
    response = await http_client.post(
        settings.slack_webhook_url,
        json=_build_slack_payload(notification),
    )
    response.raise_for_status()


async def _send_pagerduty_notification(
    *,
    http_client: Any,
    settings: Any,
    notification: dict[str, Any],
) -> None:
    response = await http_client.post(
        "https://events.pagerduty.com/v2/enqueue",
        json=_build_pagerduty_payload(settings=settings, notification=notification),
    )
    response.raise_for_status()


async def notify_ine_incident_transitions(
    *,
    settings: Any,
    http_client: Any,
    transitions: list[dict[str, Any]],
    effective_profile: dict[str, Any],
    logger: Any,
) -> list[dict[str, Any]]:
    if not transitions:
        return []
    if not settings.enable_slack_notifications and not settings.enable_pagerduty:
        return []

    reports: list[dict[str, Any]] = []
    enriched_incidents = attach_ine_incident_profiles(transitions, [effective_profile])
    for incident in enriched_incidents:
        if not should_notify_ine_incident_transition(settings=settings, incident=incident):
            continue
        if http_client is None:
            logger.warning(
                "ine_incident_notification_skipped_missing_http_client",
                extra={"incident_id": incident.get("incident_id")},
            )
            continue

        notification = build_ine_incident_notification_payload(incident=incident)
        delivered_channels: list[str] = []
        errors: list[str] = []
        attempted_at = datetime.now(UTC).isoformat()

        if settings.enable_slack_notifications and settings.slack_webhook_url:
            try:
                await _send_slack_notification(
                    http_client=http_client,
                    settings=settings,
                    notification=notification,
                )
                delivered_channels.append("slack")
            except Exception as exc:
                errors.append(f"slack:{exc}")
                logger.warning(
                    "ine_incident_slack_notification_failed",
                    extra={
                        "incident_id": incident.get("incident_id"),
                        "operation_code": incident.get("operation_code"),
                    },
                )

        if (
            settings.enable_pagerduty
            and settings.pagerduty_key
            and str(incident.get("severity")) in set(settings.ine_incident_pagerduty_severities)
        ):
            try:
                await _send_pagerduty_notification(
                    http_client=http_client,
                    settings=settings,
                    notification=notification,
                )
                delivered_channels.append("pagerduty")
            except Exception as exc:
                errors.append(f"pagerduty:{exc}")
                logger.warning(
                    "ine_incident_pagerduty_notification_failed",
                    extra={
                        "incident_id": incident.get("incident_id"),
                        "operation_code": incident.get("operation_code"),
                    },
                )

        if not delivered_channels and not errors:
            continue

        reports.append(
            {
                "incident_id": incident["incident_id"],
                "metadata": {
                    "last_notification_attempted_at": attempted_at,
                    "last_notified_at": attempted_at if delivered_channels else None,
                    "last_notified_status": incident.get("status") if delivered_channels else None,
                    "last_notified_severity": (
                        incident.get("severity") if delivered_channels else None
                    ),
                    "last_notification_event": incident.get("notification_event"),
                    "notification_channels": delivered_channels,
                    "last_notification_errors": errors,
                },
            }
        )
    return reports
