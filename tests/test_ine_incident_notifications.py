from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.ine_incident_notifications import (
    build_ine_incident_notification_payload,
    notify_ine_incident_transitions,
    should_notify_ine_incident_transition,
)
from app.settings import Settings


def _settings(**overrides) -> Settings:
    return Settings(app_env="local", job_store_backend="memory", **overrides)


def _incident(**overrides):
    item = {
        "incident_id": 1,
        "operation_code": "71",
        "incident_type": "repeated_failures",
        "severity": "high",
        "status": "open",
        "title": "Operation 71 is failing repeatedly",
        "message": "failure streak",
        "occurrence_count": 2,
        "first_seen_at": "2026-03-24T00:00:00Z",
        "last_seen_at": "2026-03-24T01:00:00Z",
        "last_job_id": "job-71",
        "last_run_status": "failed",
        "notification_event": "opened",
        "suggested_action": "review_manual",
        "execution_profile": "scheduled",
        "schedule_enabled": True,
        "background_required": False,
        "suggested_override_profile": "background_only",
        "recommended_reason": "Operation 71 is failing repeatedly while profile scheduled remains active.",
        "requires_manual_confirmation": True,
        "metadata": {},
    }
    item.update(overrides)
    return item


def test_should_notify_ine_incident_transition_for_open_high_incident() -> None:
    settings = _settings(
        ENABLE_SLACK_NOTIFICATIONS=True,
        SLACK_WEBHOOK_URL="https://example.test/slack",
    )
    assert should_notify_ine_incident_transition(settings=settings, incident=_incident()) is True


def test_should_notify_ine_incident_transition_skips_plain_updates() -> None:
    settings = _settings(
        ENABLE_SLACK_NOTIFICATIONS=True,
        SLACK_WEBHOOK_URL="https://example.test/slack",
    )
    assert (
        should_notify_ine_incident_transition(
            settings=settings,
            incident=_incident(notification_event="updated"),
        )
        is False
    )


def test_build_ine_incident_notification_payload_exposes_recommendation_and_links() -> None:
    payload = build_ine_incident_notification_payload(incident=_incident())

    assert payload["source"] == "internal.sync.ine_incident_notification"
    assert payload["incident"]["operation_code"] == "71"
    assert payload["recommendation"]["suggested_override_profile"] == "background_only"
    assert payload["links"]["operation_catalog_path"].endswith("operation_code=71")


@pytest.mark.anyio
async def test_notify_ine_incident_transitions_sends_slack_and_persists_report() -> None:
    settings = _settings(
        ENABLE_SLACK_NOTIFICATIONS=True,
        SLACK_WEBHOOK_URL="https://example.test/slack",
    )
    logger = MagicMock()

    with patch(
        "app.services.ine_incident_notifications._send_slack_notification",
        new=AsyncMock(),
    ) as slack_send:
        reports = await notify_ine_incident_transitions(
            settings=settings,
            http_client=object(),
            transitions=[_incident()],
            effective_profile={
                "operation_code": "71",
                "execution_profile": "scheduled",
                "schedule_enabled": True,
                "background_required": False,
            },
            logger=logger,
        )

    slack_send.assert_awaited_once()
    assert len(reports) == 1
    assert reports[0]["metadata"]["notification_channels"] == ["slack"]
    assert reports[0]["metadata"]["last_notification_event"] == "opened"


@pytest.mark.anyio
async def test_notify_ine_incident_transitions_records_channel_errors_without_raising() -> None:
    settings = _settings(
        ENABLE_SLACK_NOTIFICATIONS=True,
        SLACK_WEBHOOK_URL="https://example.test/slack",
    )
    logger = MagicMock()

    with patch(
        "app.services.ine_incident_notifications._send_slack_notification",
        new=AsyncMock(side_effect=RuntimeError("boom")),
    ):
        reports = await notify_ine_incident_transitions(
            settings=settings,
            http_client=object(),
            transitions=[_incident()],
            effective_profile={
                "operation_code": "71",
                "execution_profile": "scheduled",
                "schedule_enabled": True,
                "background_required": False,
            },
            logger=logger,
        )

    assert len(reports) == 1
    assert reports[0]["metadata"]["notification_channels"] == []
    assert reports[0]["metadata"]["last_notification_errors"] == ["slack:boom"]
