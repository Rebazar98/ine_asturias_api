"""Tests for CartographicQAService — validates PostGIS checks via mocked session."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.services.cartographic_qa import (
    ERROR_INVALID_GEOMETRY,
    ERROR_MISSING_GEOMETRY,
    ERROR_OVERLAP,
    LAYER_TERRITORIAL_UNITS,
    CartographicQAService,
)


def _make_mapping_row(**kwargs):
    """Return a MagicMock that supports dict-style access like SQLAlchemy RowMapping."""
    row = MagicMock()
    row.__getitem__ = lambda self, key: kwargs[key]
    return row


def _make_result(rows):
    result = MagicMock()
    result.mappings.return_value = rows
    return result


def _make_session(query_results: list):
    """Return an AsyncSession mock where successive execute() calls return query_results."""
    session = AsyncMock()
    session.execute = AsyncMock(side_effect=[_make_result(r) for r in query_results])
    return session


# ---------------------------------------------------------------------------
# validate_territorial_units
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_validate_territorial_units_empty_ids():
    session = AsyncMock()
    service = CartographicQAService(session=session)
    result = await service.validate_territorial_units([])
    assert result == []
    session.execute.assert_not_called()


@pytest.mark.anyio
async def test_validate_territorial_units_no_incidents():
    # All three queries return empty
    session = _make_session([[], [], []])
    service = CartographicQAService(session=session)
    incidents = await service.validate_territorial_units([1, 2, 3])
    assert incidents == []


@pytest.mark.anyio
async def test_validate_territorial_units_missing_geometry():
    missing_row = _make_mapping_row(id=5, unit_level="municipality", canonical_code="33001")
    session = _make_session([[missing_row], [], []])
    service = CartographicQAService(session=session)
    incidents = await service.validate_territorial_units([5])
    assert len(incidents) == 1
    inc = incidents[0]
    assert inc["layer"] == LAYER_TERRITORIAL_UNITS
    assert inc["error_type"] == ERROR_MISSING_GEOMETRY
    assert inc["severity"] == "error"
    assert inc["entity_id"] == "5"


@pytest.mark.anyio
async def test_validate_territorial_units_invalid_geometry():
    invalid_row = _make_mapping_row(id=7, unit_level="province", canonical_code="33")
    session = _make_session([[], [invalid_row], []])
    service = CartographicQAService(session=session)
    incidents = await service.validate_territorial_units([7])
    assert len(incidents) == 1
    assert incidents[0]["error_type"] == ERROR_INVALID_GEOMETRY
    assert incidents[0]["entity_id"] == "7"


@pytest.mark.anyio
async def test_validate_territorial_units_overlap():
    overlap_row = _make_mapping_row(
        id_a=1, id_b=2, unit_level="municipality", code_a="33001", code_b="33002"
    )
    session = _make_session([[], [], [overlap_row]])
    service = CartographicQAService(session=session)
    incidents = await service.validate_territorial_units([1, 2])
    assert len(incidents) == 1
    assert incidents[0]["error_type"] == ERROR_OVERLAP
    assert incidents[0]["severity"] == "warning"
    assert "33001" in incidents[0]["description"]


@pytest.mark.anyio
async def test_validate_territorial_units_single_id_skips_overlap():
    """With only one unit, overlap query must be skipped."""
    session = _make_session([[], []])
    service = CartographicQAService(session=session)
    incidents = await service.validate_territorial_units([42])
    # Only 2 queries (missing + invalid), no overlap query
    assert session.execute.call_count == 2
    assert incidents == []


# ---------------------------------------------------------------------------
# validate_ideas_features
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_validate_ideas_features_empty_ids():
    session = AsyncMock()
    service = CartographicQAService(session=session)
    result = await service.validate_ideas_features([])
    assert result == []
    session.execute.assert_not_called()


@pytest.mark.anyio
async def test_validate_ideas_features_invalid_geometry():
    from app.services.cartographic_qa import ERROR_INVALID_GEOMETRY, LAYER_IDEAS_FEATURES

    invalid_row = _make_mapping_row(id=10, layer_name="limites_parroquiales", feature_id="F001")
    session = _make_session([[invalid_row], []])
    service = CartographicQAService(session=session)
    incidents = await service.validate_ideas_features([10])
    assert len(incidents) == 1
    assert incidents[0]["layer"] == LAYER_IDEAS_FEATURES
    assert incidents[0]["error_type"] == ERROR_INVALID_GEOMETRY
