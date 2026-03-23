"""Tests for GET /qa/incidents endpoint."""

from __future__ import annotations

from datetime import datetime, UTC
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.dependencies import get_qa_repository
from app.main import app


_NOW = datetime(2026, 3, 17, 10, 0, 0, tzinfo=UTC)

_SAMPLE_INCIDENTS = [
    {
        "id": 1,
        "layer": "territorial_units",
        "entity_id": "42",
        "error_type": "invalid_geometry",
        "severity": "error",
        "description": "municipality 33001: ST_IsValid returned false",
        "source_provider": "ign",
        "detected_at": _NOW,
        "resolved": False,
        "resolved_at": None,
        "metadata": {"unit_level": "municipality"},
    },
    {
        "id": 2,
        "layer": "territorial_units",
        "entity_id": "43",
        "error_type": "missing_geometry",
        "severity": "error",
        "description": "municipality 33002: geometry is NULL",
        "source_provider": "ign",
        "detected_at": _NOW,
        "resolved": False,
        "resolved_at": None,
        "metadata": {},
    },
]


class DummyQARepository:
    def __init__(self, incidents: list[dict[str, Any]] | None = None) -> None:
        self._incidents = incidents or []

    async def list_incidents(
        self,
        *,
        layer: str | None = None,
        severity: str | None = None,
        resolved: bool = False,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        filtered = [
            inc
            for inc in self._incidents
            if inc["resolved"] == resolved
            and (layer is None or inc["layer"] == layer)
            and (severity is None or inc["severity"] == severity)
        ]
        total = len(filtered)
        start = (page - 1) * page_size
        items = filtered[start : start + page_size]
        pages = (total + page_size - 1) // page_size if total else 0
        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "has_next": page < pages,
            "has_previous": page > 1,
            "filters": {"layer": layer, "severity": severity, "resolved": resolved},
        }


@pytest.fixture
def client_with_incidents():
    dummy = DummyQARepository(_SAMPLE_INCIDENTS)
    app.dependency_overrides[get_qa_repository] = lambda: dummy
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.pop(get_qa_repository, None)


@pytest.fixture
def client_empty():
    dummy = DummyQARepository([])
    app.dependency_overrides[get_qa_repository] = lambda: dummy
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.pop(get_qa_repository, None)


def test_list_qa_incidents_returns_200(client_with_incidents):
    resp = client_with_incidents.get("/qa/incidents")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert len(body["items"]) == 2


def test_list_qa_incidents_filter_by_layer(client_with_incidents):
    resp = client_with_incidents.get("/qa/incidents?layer=territorial_units")
    assert resp.status_code == 200
    assert resp.json()["total"] == 2


def test_list_qa_incidents_filter_by_severity(client_with_incidents):
    resp = client_with_incidents.get("/qa/incidents?severity=error")
    assert resp.status_code == 200
    assert resp.json()["total"] == 2


def test_list_qa_incidents_filter_by_severity_warning(client_with_incidents):
    resp = client_with_incidents.get("/qa/incidents?severity=warning")
    assert resp.status_code == 200
    assert resp.json()["total"] == 0


def test_list_qa_incidents_empty(client_empty):
    resp = client_empty.get("/qa/incidents")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 0
    assert body["items"] == []


def test_list_qa_incidents_schema_fields(client_with_incidents):
    resp = client_with_incidents.get("/qa/incidents")
    item = resp.json()["items"][0]
    required_fields = {
        "id",
        "layer",
        "entity_id",
        "error_type",
        "severity",
        "description",
        "source_provider",
        "detected_at",
        "resolved",
    }
    assert required_fields.issubset(item.keys())


def test_list_qa_incidents_resolved_false_by_default(client_with_incidents):
    resp = client_with_incidents.get("/qa/incidents")
    assert all(not item["resolved"] for item in resp.json()["items"])


def test_list_qa_incidents_pagination_params(client_with_incidents):
    resp = client_with_incidents.get("/qa/incidents?page=1&page_size=1")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["items"]) == 1
    assert body["page_size"] == 1
