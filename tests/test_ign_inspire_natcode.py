"""Tests for INSPIRE/CNIG NATCODE extraction and normalization.

CNIG NATCODE structure (11 digits for municipalities):
    <country(2)><ccaa(2)><province(2)><municipality(5)>
    e.g. "34033333044" → INE code "33044" (Oviedo, Asturias)
"""

from __future__ import annotations


from app.repositories.territorial import (
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
)
from app.services.ign_admin_boundaries import (
    IGN_ADMIN_SCOPE_ASTURIAS_CODE,
    extract_ine_code,
    normalize_ign_admin_snapshot,
)


# ---------------------------------------------------------------------------
# extract_ine_code unit tests
# ---------------------------------------------------------------------------


def test_extract_ine_code_municipality_natcode():
    assert extract_ine_code("34033333044") == "33044"


def test_extract_ine_code_different_municipality():
    # Madrid (28079): country(34) + ccaa(13) + province(28) + municipality(28079)
    assert extract_ine_code("34132828079") == "28079"


def test_extract_ine_code_exactly_five_digits():
    assert extract_ine_code("33044") == "33044"


def test_extract_ine_code_none_input():
    assert extract_ine_code(None) is None


def test_extract_ine_code_empty_string():
    assert extract_ine_code("") is None


def test_extract_ine_code_too_short():
    assert extract_ine_code("3304") is None


def test_extract_ine_code_non_digit_suffix():
    # last 5 chars contain letters → invalid
    assert extract_ine_code("3403abc33") is None


def test_extract_ine_code_whitespace_stripped():
    assert extract_ine_code("  34033333044  ") == "33044"


# ---------------------------------------------------------------------------
# Helpers for building minimal INSPIRE GeoJSON features
# ---------------------------------------------------------------------------

_MULTIPOLYGON = {
    "type": "MultiPolygon",
    "coordinates": [[[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]]],
}


def _inspire_feature(natcode: str, nameunit: str, extra_props: dict | None = None) -> dict:
    props = {"NATCODE": natcode, "NAMEUNIT": nameunit}
    if extra_props:
        props.update(extra_props)
    return {"type": "Feature", "geometry": _MULTIPOLYGON, "properties": props}


def _snapshot(*features) -> dict:
    return {"type": "FeatureCollection", "features": list(features)}


# ---------------------------------------------------------------------------
# normalize_ign_admin_snapshot with INSPIRE features
# ---------------------------------------------------------------------------


def test_inspire_municipality_canonical_code():
    feat = _inspire_feature("34033333044", "Oviedo")
    result = normalize_ign_admin_snapshot(
        _snapshot(feat),
        country_code="ES",
        autonomous_community_code=IGN_ADMIN_SCOPE_ASTURIAS_CODE,
    )
    normalized = result["features"]
    assert len(normalized) == 1
    muni = normalized[0]
    assert muni.canonical_code == "33044"
    assert muni.unit_level == TERRITORIAL_UNIT_LEVEL_MUNICIPALITY


def test_inspire_municipality_canonical_name():
    feat = _inspire_feature("34033333044", "Oviedo")
    result = normalize_ign_admin_snapshot(_snapshot(feat), country_code="ES")
    assert result["features"][0].canonical_name == "Oviedo"


def test_inspire_municipality_province_code_derived():
    feat = _inspire_feature("34033333044", "Oviedo")
    result = normalize_ign_admin_snapshot(_snapshot(feat), country_code="ES")
    muni = result["features"][0]
    # province_code must be the first 2 digits of the INE municipality code
    assert muni.province_code == "33"


def test_inspire_municipality_autonomous_community_derived():
    feat = _inspire_feature("34033333044", "Oviedo")
    result = normalize_ign_admin_snapshot(_snapshot(feat), country_code="ES")
    muni = result["features"][0]
    assert muni.autonomous_community_code == "03"


def test_inspire_province_canonical_code():
    feat = _inspire_feature("340333", "Asturias")
    result = normalize_ign_admin_snapshot(_snapshot(feat), country_code="ES")
    normalized = result["features"]
    assert len(normalized) == 1
    prov = normalized[0]
    assert prov.canonical_code == "33"
    assert prov.unit_level == TERRITORIAL_UNIT_LEVEL_PROVINCE


def test_inspire_province_parent_is_ccaa():
    feat = _inspire_feature("340333", "Asturias")
    result = normalize_ign_admin_snapshot(_snapshot(feat), country_code="ES")
    prov = result["features"][0]
    assert prov.parent_unit_level == TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY
    assert prov.parent_canonical_code == "03"


def test_inspire_ccaa_canonical_code():
    feat = _inspire_feature("3403", "Principado de Asturias")
    result = normalize_ign_admin_snapshot(
        _snapshot(feat),
        country_code="ES",
        autonomous_community_code=IGN_ADMIN_SCOPE_ASTURIAS_CODE,
    )
    normalized = result["features"]
    assert len(normalized) == 1
    ccaa = normalized[0]
    assert ccaa.canonical_code == "03"
    assert ccaa.unit_level == TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY


def test_inspire_missing_natcode_raises_incident():
    # Feature with no NATCODE and no other recognized fields → rejected as incident
    bad_feat = {
        "type": "Feature",
        "geometry": _MULTIPOLYGON,
        "properties": {"NAMEUNIT": "Unknown"},
    }
    result = normalize_ign_admin_snapshot(_snapshot(bad_feat), country_code="ES")
    assert len(result["features"]) == 0
    assert len(result["incidents"]) == 1
    assert result["incidents"][0]["reason"] == "invalid_feature"


def test_inspire_municipality_scoped_to_asturias():
    oviedo = _inspire_feature("34033333044", "Oviedo")
    madrid = _inspire_feature("34132828079", "Madrid")
    result = normalize_ign_admin_snapshot(
        _snapshot(oviedo, madrid),
        country_code="ES",
        autonomous_community_code=IGN_ADMIN_SCOPE_ASTURIAS_CODE,
    )
    codes = [f.canonical_code for f in result["features"]]
    assert "33044" in codes
    assert "28079" not in codes
