"""Tests for request_id traceability (X-Request-ID header + ContextVar propagation)."""
from __future__ import annotations

import logging
import uuid

import pytest

from app.core.logging import request_id_var


def test_request_id_is_generated_when_no_header_provided(client):
    """Middleware must generate a UUID request_id when none is provided by the client."""
    response = client.get("/health")

    assert response.status_code == 200
    rid = response.headers.get("X-Request-ID")
    assert rid is not None
    # Must be a valid UUID
    uuid.UUID(rid)


def test_client_provided_request_id_is_echoed_in_response(client):
    """Middleware must reflect the client-supplied X-Request-ID unchanged."""
    custom_rid = "my-trace-abc-123"
    response = client.get("/health", headers={"X-Request-ID": custom_rid})

    assert response.status_code == 200
    assert response.headers.get("X-Request-ID") == custom_rid


def test_different_requests_get_different_request_ids(client):
    """Each request without a header must receive a distinct generated request_id."""
    r1 = client.get("/health")
    r2 = client.get("/health")

    rid1 = r1.headers.get("X-Request-ID")
    rid2 = r2.headers.get("X-Request-ID")
    assert rid1 is not None
    assert rid2 is not None
    assert rid1 != rid2


def test_request_id_appears_in_log_records(client, caplog):
    """JsonFormatter must inject request_id into every log record during a request."""
    custom_rid = "trace-log-test-456"

    with caplog.at_level(logging.INFO, logger="app.access"):
        client.get("/health", headers={"X-Request-ID": custom_rid})

    matching = [r for r in caplog.records if getattr(r, "request_id", None) == custom_rid]
    assert matching, "No log records contained the expected request_id"


def test_request_id_var_is_none_outside_request_context():
    """ContextVar default must be None when no request is active."""
    assert request_id_var.get() is None


def test_request_id_var_is_reset_after_request(client):
    """ContextVar must be reset to its default after the middleware finishes."""
    client.get("/health", headers={"X-Request-ID": "ephemeral-id"})
    # After the request completes, the ContextVar should be back to default
    assert request_id_var.get() is None
