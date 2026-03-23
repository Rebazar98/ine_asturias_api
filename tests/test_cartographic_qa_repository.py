"""Tests unitarios para CartographicQARepository con sesión mockeada."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.exc import SQLAlchemyError

from app.repositories.cartographic_qa import CartographicQARepository


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_session():
    session = AsyncMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.scalar = AsyncMock(return_value=0)
    session.get = AsyncMock(return_value=None)
    return session


def _make_row(**kwargs):
    row = MagicMock()
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row


# ---------------------------------------------------------------------------
# save_incidents
# ---------------------------------------------------------------------------


def test_save_incidents_empty_list_returns_zero():
    repo = CartographicQARepository(session=_make_session())
    result = asyncio.run(repo.save_incidents([]))
    assert result == 0


def test_save_incidents_no_session_returns_zero():
    repo = CartographicQARepository(session=None)
    result = asyncio.run(repo.save_incidents([{"layer": "l", "entity_id": "1",
                                               "error_type": "missing_geometry"}]))
    assert result == 0


def test_save_incidents_happy_path():
    session = _make_session()
    repo = CartographicQARepository(session=session)
    incidents = [
        {
            "layer": "territorial_units",
            "entity_id": "5",
            "error_type": "missing_geometry",
            "severity": "error",
            "description": "No geometry",
            "source_provider": "ign",
            "metadata": {"unit_level": "municipality"},
        }
    ]

    result = asyncio.run(repo.save_incidents(incidents))

    assert result == 1
    session.execute.assert_awaited_once()
    session.commit.assert_awaited_once()


def test_save_incidents_sqlalchemy_error_returns_zero():
    session = _make_session()
    session.execute.side_effect = SQLAlchemyError("DB error")
    repo = CartographicQARepository(session=session)

    result = asyncio.run(repo.save_incidents([
        {"layer": "l", "entity_id": "1", "error_type": "e"}
    ]))

    assert result == 0
    session.rollback.assert_awaited_once()


def test_save_incidents_uses_default_severity_and_description():
    session = _make_session()
    repo = CartographicQARepository(session=session)
    # Incident with only mandatory fields — defaults apply
    incidents = [{"layer": "territorial_units", "entity_id": "9", "error_type": "overlap"}]

    result = asyncio.run(repo.save_incidents(incidents))

    assert result == 1


# ---------------------------------------------------------------------------
# list_incidents
# ---------------------------------------------------------------------------


def test_list_incidents_no_session_returns_empty_page():
    repo = CartographicQARepository(session=None)
    result = asyncio.run(repo.list_incidents())

    assert result["total"] == 0
    assert result["items"] == []
    assert result["page"] == 1
    assert result["has_next"] is False


def test_list_incidents_no_session_respects_filters():
    repo = CartographicQARepository(session=None)
    result = asyncio.run(repo.list_incidents(layer="territorial_units", severity="error"))

    assert result["filters"]["layer"] == "territorial_units"
    assert result["filters"]["severity"] == "error"


def test_list_incidents_happy_path():
    session = _make_session()
    repo = CartographicQARepository(session=session)
    row = _make_row(
        id=1,
        layer="territorial_units",
        entity_id="5",
        error_type="missing_geometry",
        severity="error",
        description="No geometry",
        source_provider="ign",
        detected_at=None,
        resolved=False,
        resolved_at=None,
        metadata_json={},
    )
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [row]
    session.scalar.return_value = 1
    session.execute.return_value = mock_result

    result = asyncio.run(repo.list_incidents())

    assert result["total"] == 1
    assert result["items"][0]["entity_id"] == "5"
    assert result["items"][0]["error_type"] == "missing_geometry"


def test_list_incidents_with_layer_filter():
    session = _make_session()
    repo = CartographicQARepository(session=session)
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    session.scalar.return_value = 0
    session.execute.return_value = mock_result

    result = asyncio.run(repo.list_incidents(layer="territorial_units"))

    assert result["filters"]["layer"] == "territorial_units"
    # Two execute calls: count + query
    assert session.execute.await_count == 1


# ---------------------------------------------------------------------------
# mark_resolved
# ---------------------------------------------------------------------------


def test_mark_resolved_no_session_returns_false():
    repo = CartographicQARepository(session=None)
    result = asyncio.run(repo.mark_resolved(1))
    assert result is False


def test_mark_resolved_not_found_returns_false():
    session = _make_session()
    session.get.return_value = None
    repo = CartographicQARepository(session=session)

    result = asyncio.run(repo.mark_resolved(999))

    assert result is False


def test_mark_resolved_happy_path():
    session = _make_session()
    incident = MagicMock()
    incident.resolved = False
    incident.resolved_at = None
    session.get.return_value = incident
    repo = CartographicQARepository(session=session)

    result = asyncio.run(repo.mark_resolved(1))

    assert result is True
    assert incident.resolved is True
    assert incident.resolved_at is not None
    session.commit.assert_awaited_once()


def test_mark_resolved_sqlalchemy_error_returns_false():
    session = _make_session()
    incident = MagicMock()
    session.get.return_value = incident
    session.commit.side_effect = SQLAlchemyError("DB error")
    repo = CartographicQARepository(session=session)

    result = asyncio.run(repo.mark_resolved(1))

    assert result is False
    session.rollback.assert_awaited_once()


# ---------------------------------------------------------------------------
# _empty_page static method
# ---------------------------------------------------------------------------


def test_empty_page_has_correct_shape():
    page = CartographicQARepository._empty_page(2, 25, "territorial_units", "error", False)

    assert page["page"] == 2
    assert page["page_size"] == 25
    assert page["total"] == 0
    assert page["has_next"] is False
    assert page["has_previous"] is True  # page 2 > 1
    assert page["filters"]["layer"] == "territorial_units"
    assert page["filters"]["severity"] == "error"
