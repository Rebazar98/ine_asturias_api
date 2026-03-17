"""Tests that IGN loader calls QA service when cartographic_qa_enabled=True."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.ign_admin_boundaries import IGNAdministrativeBoundariesLoaderService


def _minimal_valid_snapshot() -> dict[str, Any]:
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [[[[0, 0], [1, 0], [1, 1], [0, 0]]]],
                },
                "properties": {
                    "unit_level": "country",
                    "country_code": "ES",
                    "canonical_name": "España",
                },
            }
        ],
    }


def _make_loader(
    *,
    qa_repo=None,
    cartographic_qa_enabled: bool = True,
    session=None,
) -> IGNAdministrativeBoundariesLoaderService:
    ingestion_repo = AsyncMock()
    ingestion_repo.save_raw = AsyncMock(return_value=1)

    territorial_repo = AsyncMock()
    territorial_repo.session = session
    territorial_repo.get_unit_by_canonical_code = AsyncMock(return_value=None)
    territorial_repo.upsert_boundary_unit = AsyncMock(
        return_value={"territorial_unit_id": 99, "created": True}
    )

    return IGNAdministrativeBoundariesLoaderService(
        ingestion_repo=ingestion_repo,
        territorial_repo=territorial_repo,
        qa_repo=qa_repo,
        cartographic_qa_enabled=cartographic_qa_enabled,
    )


@pytest.mark.anyio
async def test_loader_calls_qa_when_enabled():
    session = AsyncMock()
    session.begin_nested = MagicMock(
        return_value=AsyncMock(__aenter__=AsyncMock(), __aexit__=AsyncMock(return_value=False))
    )

    qa_repo = AsyncMock()
    qa_repo.save_incidents = AsyncMock(return_value=0)

    loader = _make_loader(qa_repo=qa_repo, cartographic_qa_enabled=True, session=session)

    with patch(
        "app.services.cartographic_qa.CartographicQAService.validate_territorial_units",
        new_callable=AsyncMock,
        return_value=[],
    ):
        summary = await loader.load_snapshot(
            payload=_minimal_valid_snapshot(),
            source_path="test://snapshot",
        )

    assert "qa_incidents_detected" in summary


@pytest.mark.anyio
async def test_loader_skips_qa_when_disabled():
    session = AsyncMock()
    session.begin_nested = MagicMock(
        return_value=AsyncMock(__aenter__=AsyncMock(), __aexit__=AsyncMock(return_value=False))
    )

    qa_repo = AsyncMock()
    qa_repo.save_incidents = AsyncMock(return_value=0)

    loader = _make_loader(qa_repo=qa_repo, cartographic_qa_enabled=False, session=session)

    with patch(
        "app.services.cartographic_qa.CartographicQAService.validate_territorial_units",
        new_callable=AsyncMock,
        return_value=[{"error_type": "invalid_geometry"}],
    ) as mock_validate:
        await loader.load_snapshot(
            payload=_minimal_valid_snapshot(),
            source_path="test://snapshot",
        )

    mock_validate.assert_not_called()
    qa_repo.save_incidents.assert_not_called()


@pytest.mark.anyio
async def test_loader_skips_qa_when_repo_is_none():
    session = AsyncMock()
    session.begin_nested = MagicMock(
        return_value=AsyncMock(__aenter__=AsyncMock(), __aexit__=AsyncMock(return_value=False))
    )

    loader = _make_loader(qa_repo=None, cartographic_qa_enabled=True, session=session)

    # Should not raise even without qa_repo
    summary = await loader.load_snapshot(
        payload=_minimal_valid_snapshot(),
        source_path="test://snapshot",
    )
    assert summary["qa_incidents_detected"] == 0


@pytest.mark.anyio
async def test_loader_saves_detected_qa_incidents():
    session = AsyncMock()
    session.begin_nested = MagicMock(
        return_value=AsyncMock(__aenter__=AsyncMock(), __aexit__=AsyncMock(return_value=False))
    )

    fake_incident = {
        "layer": "territorial_units",
        "entity_id": "99",
        "error_type": "invalid_geometry",
        "severity": "error",
        "description": "test",
        "source_provider": "ign",
        "metadata": {},
    }
    qa_repo = AsyncMock()
    qa_repo.save_incidents = AsyncMock(return_value=1)

    loader = _make_loader(qa_repo=qa_repo, cartographic_qa_enabled=True, session=session)

    with patch(
        "app.services.cartographic_qa.CartographicQAService.validate_territorial_units",
        new_callable=AsyncMock,
        return_value=[fake_incident],
    ):
        summary = await loader.load_snapshot(
            payload=_minimal_valid_snapshot(),
            source_path="test://snapshot",
        )

    qa_repo.save_incidents.assert_called_once()
    assert summary["qa_incidents_detected"] == 1
