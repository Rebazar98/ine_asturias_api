"""Tests for D1 — org_id column on IngestionRaw, INESeriesNormalized, TerritorialUnit.

Validates that:
- All three ORM models expose an `org_id` attribute.
- The default value is "geonalon".
- The field is nullable (None is accepted).
- Adding org_id does not break existing model instantiation.
"""

from __future__ import annotations

import pytest

from app.models import INESeriesNormalized, IngestionRaw, TerritorialUnit


class TestOrgIdFieldPresence:
    def test_ingestion_raw_has_org_id(self):
        col = IngestionRaw.__table__.c.get("org_id")
        assert col is not None, "ingestion_raw.org_id column missing"

    def test_ine_series_normalized_has_org_id(self):
        col = INESeriesNormalized.__table__.c.get("org_id")
        assert col is not None, "ine_series_normalized.org_id column missing"

    def test_territorial_units_has_org_id(self):
        col = TerritorialUnit.__table__.c.get("org_id")
        assert col is not None, "territorial_units.org_id column missing"


class TestOrgIdServerDefault:
    def test_ingestion_raw_server_default_geonalon(self):
        col = IngestionRaw.__table__.c["org_id"]
        assert "geonalon" in str(col.server_default.arg)

    def test_ine_series_normalized_server_default_geonalon(self):
        col = INESeriesNormalized.__table__.c["org_id"]
        assert "geonalon" in str(col.server_default.arg)

    def test_territorial_units_server_default_geonalon(self):
        col = TerritorialUnit.__table__.c["org_id"]
        assert "geonalon" in str(col.server_default.arg)


class TestOrgIdNullable:
    def test_ingestion_raw_org_id_nullable(self):
        col = IngestionRaw.__table__.c["org_id"]
        assert col.nullable is True

    def test_ine_series_normalized_org_id_nullable(self):
        col = INESeriesNormalized.__table__.c["org_id"]
        assert col.nullable is True

    def test_territorial_units_org_id_nullable(self):
        col = TerritorialUnit.__table__.c["org_id"]
        assert col.nullable is True


class TestOrgIdDoesNotBreakInstantiation:
    def test_ingestion_raw_instantiation_with_org_id(self):
        obj = IngestionRaw(
            source_type="test",
            source_key="key",
            request_path="/test",
            payload={},
            org_id="geonalon",
        )
        assert obj.org_id == "geonalon"

    def test_ingestion_raw_instantiation_without_org_id(self):
        obj = IngestionRaw(
            source_type="test",
            source_key="key",
            request_path="/test",
            payload={},
        )
        assert obj.org_id is None  # Python-side default before DB write

    def test_ine_series_normalized_instantiation_with_org_id(self):
        obj = INESeriesNormalized(
            operation_code="22",
            table_id="tbl1",
            variable_id="var1",
            geography_name="Oviedo",
            geography_code="33044",
            period="2024",
            org_id="test_client",
        )
        assert obj.org_id == "test_client"

    def test_territorial_unit_instantiation_with_org_id(self):
        obj = TerritorialUnit(
            unit_level="municipality",
            canonical_name="Oviedo",
            normalized_name="oviedo",
            org_id="geonalon",
        )
        assert obj.org_id == "geonalon"

    def test_org_id_accepts_none(self):
        obj = TerritorialUnit(
            unit_level="municipality",
            canonical_name="Oviedo",
            normalized_name="oviedo",
            org_id=None,
        )
        assert obj.org_id is None
