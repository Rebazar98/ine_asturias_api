"""Tests for D2 — SyncSchedule model and SyncScheduleRepository.

Validates:
- SyncSchedule ORM model has required columns.
- Repository with None session returns safe defaults.
- Repository.upsert with None session raises RuntimeError.
- Serialization produces the expected dict shape.
- A second org_id can coexist with the geonalon seed (multi-tenant readiness).
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.models import SyncSchedule
from app.repositories.sync_schedule import SyncScheduleRepository


# ---------------------------------------------------------------------------
# Model structure
# ---------------------------------------------------------------------------


class TestSyncScheduleModel:
    def test_has_required_columns(self):
        cols = {c.name for c in SyncSchedule.__table__.c}
        assert {"id", "org_id", "source", "cron_expression", "is_active", "updated_at"} <= cols

    def test_unique_constraint_org_source(self):
        constraints = {c.name for c in SyncSchedule.__table__.constraints}
        assert "uq_sync_schedule_org_source" in constraints

    def test_org_id_indexed(self):
        index_cols = {
            col.name
            for idx in SyncSchedule.__table__.indexes
            for col in idx.columns
        }
        assert "org_id" in index_cols

    def test_is_active_server_default_true(self):
        col = SyncSchedule.__table__.c["is_active"]
        assert "true" in str(col.server_default.arg).lower()

    def test_instantiation(self):
        obj = SyncSchedule(
            org_id="geonalon",
            source="ine",
            cron_expression="0 3 * * *",
        )
        assert obj.org_id == "geonalon"
        assert obj.source == "ine"
        assert obj.cron_expression == "0 3 * * *"


# ---------------------------------------------------------------------------
# Repository — None session (safe degraded mode)
# ---------------------------------------------------------------------------


class TestSyncScheduleRepositoryNoneSession:
    @pytest.mark.anyio
    async def test_list_active_returns_empty(self):
        repo = SyncScheduleRepository(session=None)
        result = await repo.list_active()
        assert result == []

    @pytest.mark.anyio
    async def test_list_active_with_org_id_returns_empty(self):
        repo = SyncScheduleRepository(session=None)
        result = await repo.list_active(org_id="geonalon")
        assert result == []

    @pytest.mark.anyio
    async def test_get_by_org_source_returns_none(self):
        repo = SyncScheduleRepository(session=None)
        result = await repo.get_by_org_source(org_id="geonalon", source="ine")
        assert result is None

    @pytest.mark.anyio
    async def test_upsert_raises_with_none_session(self):
        repo = SyncScheduleRepository(session=None)
        with pytest.raises(RuntimeError, match="No database session"):
            await repo.upsert(org_id="geonalon", source="ine", cron_expression="0 3 * * *")


# ---------------------------------------------------------------------------
# Repository — mocked session
# ---------------------------------------------------------------------------


class TestSyncScheduleRepositoryMockedSession:
    def _make_row(self, org_id="geonalon", source="ine", cron="0 3 * * *"):
        row = SyncSchedule(
            org_id=org_id,
            source=source,
            cron_expression=cron,
            is_active=True,
        )
        row.id = 1
        row.updated_at = datetime(2026, 3, 17, tzinfo=timezone.utc)
        return row

    @pytest.mark.anyio
    async def test_list_active_returns_serialized_rows(self):
        row = self._make_row()
        session = AsyncMock()
        fake_result = MagicMock()
        fake_result.scalars.return_value.all.return_value = [row]
        session.execute = AsyncMock(return_value=fake_result)

        repo = SyncScheduleRepository(session=session)
        result = await repo.list_active(org_id="geonalon")

        assert len(result) == 1
        assert result[0]["org_id"] == "geonalon"
        assert result[0]["source"] == "ine"
        assert result[0]["cron_expression"] == "0 3 * * *"
        assert result[0]["is_active"] is True

    @pytest.mark.anyio
    async def test_get_by_org_source_found(self):
        row = self._make_row(source="catastro", cron="0 5 * * 1")
        session = AsyncMock()
        fake_result = MagicMock()
        fake_result.scalars.return_value.first.return_value = row
        session.execute = AsyncMock(return_value=fake_result)

        repo = SyncScheduleRepository(session=session)
        result = await repo.get_by_org_source(org_id="geonalon", source="catastro")

        assert result is not None
        assert result["source"] == "catastro"
        assert result["cron_expression"] == "0 5 * * 1"

    @pytest.mark.anyio
    async def test_get_by_org_source_not_found(self):
        session = AsyncMock()
        fake_result = MagicMock()
        fake_result.scalars.return_value.first.return_value = None
        session.execute = AsyncMock(return_value=fake_result)

        repo = SyncScheduleRepository(session=session)
        result = await repo.get_by_org_source(org_id="unknown_org", source="ine")

        assert result is None


# ---------------------------------------------------------------------------
# Multi-tenant readiness
# ---------------------------------------------------------------------------


class TestSyncScheduleMultiTenant:
    def test_second_org_id_model_instantiation(self):
        """A second client's schedule can be created without any code change."""
        obj = SyncSchedule(
            org_id="test_client",
            source="ine",
            cron_expression="0 6 * * *",
            is_active=True,
        )
        assert obj.org_id == "test_client"
        assert obj.source == "ine"

    def test_serialize_includes_org_id(self):
        row = SyncSchedule(
            org_id="another_org",
            source="sadei",
            cron_expression="0 5 * * *",
            is_active=False,
        )
        row.id = 99
        row.updated_at = datetime(2026, 3, 17, tzinfo=timezone.utc)
        result = SyncScheduleRepository._serialize(row)

        assert result["org_id"] == "another_org"
        assert result["source"] == "sadei"
        assert result["is_active"] is False
        assert result["id"] == 99
