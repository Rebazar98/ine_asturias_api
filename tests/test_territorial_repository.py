import asyncio
from types import SimpleNamespace

from app.repositories.territorial import (
    CANONICAL_TERRITORIAL_CODE_BY_LEVEL,
    INE_AUTONOMOUS_COMMUNITY_CODE_TYPE,
    INE_MUNICIPALITY_CODE_TYPE,
    INE_PROVINCE_CODE_TYPE,
    INE_TERRITORIAL_SOURCE_SYSTEM,
    ISO3166_ALPHA2_CODE_TYPE,
    ISO3166_TERRITORIAL_SOURCE_SYSTEM,
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
    TERRITORIAL_UNIT_LEVEL_COUNTRY,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
    TERRITORIAL_MATCHED_BY_CANONICAL_NAME,
    TerritorialRepository,
    get_canonical_code_strategy,
    normalize_territorial_name,
)


class FakeScalarResult:
    def __init__(self, values):
        self._values = values

    def all(self):
        return self._values

    def first(self):
        return self._values[0] if self._values else None


class FakeExecuteResult:
    def __init__(self, first_value=None, scalar_values=None, all_values=None):
        self._first_value = first_value
        self._scalar_values = scalar_values or []
        self._all_values = all_values or []

    def first(self):
        return self._first_value

    def scalars(self):
        return FakeScalarResult(self._scalar_values)

    def all(self):
        return self._all_values


class FakeSession:
    def __init__(self, *results):
        self._results = list(results)
        self.statements = []

    async def execute(self, statement):
        self.statements.append(statement)
        return self._results.pop(0)


async def unexpected_canonical_code_lookup(_unit):
    raise AssertionError("list_units should batch canonical code lookups")


def test_canonical_territorial_code_strategy_is_defined_per_supported_level():
    assert CANONICAL_TERRITORIAL_CODE_BY_LEVEL == {
        TERRITORIAL_UNIT_LEVEL_COUNTRY: {
            "source_system": ISO3166_TERRITORIAL_SOURCE_SYSTEM,
            "code_type": ISO3166_ALPHA2_CODE_TYPE,
        },
        TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY: {
            "source_system": INE_TERRITORIAL_SOURCE_SYSTEM,
            "code_type": INE_AUTONOMOUS_COMMUNITY_CODE_TYPE,
        },
        TERRITORIAL_UNIT_LEVEL_PROVINCE: {
            "source_system": INE_TERRITORIAL_SOURCE_SYSTEM,
            "code_type": INE_PROVINCE_CODE_TYPE,
        },
        TERRITORIAL_UNIT_LEVEL_MUNICIPALITY: {
            "source_system": INE_TERRITORIAL_SOURCE_SYSTEM,
            "code_type": INE_MUNICIPALITY_CODE_TYPE,
        },
    }


def test_get_canonical_code_strategy_returns_none_for_unknown_level():
    assert get_canonical_code_strategy("district") is None


def test_normalize_territorial_name_removes_accents_punctuation_and_whitespace():
    assert normalize_territorial_name("  Asturias, Principado de  ") == "asturias principado de"
    assert normalize_territorial_name("Uviéu") == "uvieu"


def test_get_unit_by_canonical_code_returns_none_without_database_session():
    repository = TerritorialRepository(session=None)

    result = repository.get_unit_by_canonical_code(
        unit_level=TERRITORIAL_UNIT_LEVEL_PROVINCE,
        code_value="33",
    )

    assert asyncio.run(result) is None


def test_get_unit_geometry_by_canonical_code_returns_none_without_database_session():
    repository = TerritorialRepository(session=None)

    result = repository.get_unit_geometry_by_canonical_code(
        unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
        code_value="33044",
    )

    assert asyncio.run(result) is None


def test_get_catalog_coverage_returns_zero_rows_per_supported_level_without_database_session():
    repository = TerritorialRepository(session=None)

    result = asyncio.run(repository.get_catalog_coverage(country_code="ES"))

    assert result == [
        {
            "unit_level": TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
            "country_code": "ES",
            "units_total": 0,
            "active_units": 0,
            "geometry_units": 0,
            "centroid_units": 0,
            "boundary_source": None,
            "canonical_code_strategy": {
                "source_system": INE_TERRITORIAL_SOURCE_SYSTEM,
                "code_type": INE_AUTONOMOUS_COMMUNITY_CODE_TYPE,
            },
        },
        {
            "unit_level": TERRITORIAL_UNIT_LEVEL_PROVINCE,
            "country_code": "ES",
            "units_total": 0,
            "active_units": 0,
            "geometry_units": 0,
            "centroid_units": 0,
            "boundary_source": None,
            "canonical_code_strategy": {
                "source_system": INE_TERRITORIAL_SOURCE_SYSTEM,
                "code_type": INE_PROVINCE_CODE_TYPE,
            },
        },
        {
            "unit_level": TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
            "country_code": "ES",
            "units_total": 0,
            "active_units": 0,
            "geometry_units": 0,
            "centroid_units": 0,
            "boundary_source": None,
            "canonical_code_strategy": {
                "source_system": INE_TERRITORIAL_SOURCE_SYSTEM,
                "code_type": INE_MUNICIPALITY_CODE_TYPE,
            },
        },
    ]


def test_get_catalog_coverage_returns_counts_per_supported_level():
    session = FakeSession(
        FakeExecuteResult(scalar_values=[1]),
        FakeExecuteResult(scalar_values=[1]),
        FakeExecuteResult(scalar_values=[1]),
        FakeExecuteResult(scalar_values=[1]),
        FakeExecuteResult(scalar_values=[1]),
        FakeExecuteResult(scalar_values=[1]),
        FakeExecuteResult(scalar_values=[1]),
        FakeExecuteResult(scalar_values=[1]),
        FakeExecuteResult(scalar_values=[2]),
        FakeExecuteResult(scalar_values=[1]),
        FakeExecuteResult(scalar_values=[1]),
        FakeExecuteResult(scalar_values=[1]),
    )
    repository = TerritorialRepository(session=session)

    result = asyncio.run(repository.get_catalog_coverage(country_code="ES"))

    assert result == [
        {
            "unit_level": TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
            "country_code": "ES",
            "units_total": 1,
            "active_units": 1,
            "geometry_units": 1,
            "centroid_units": 1,
            "boundary_source": "ign_administrative_boundaries",
            "canonical_code_strategy": {
                "source_system": INE_TERRITORIAL_SOURCE_SYSTEM,
                "code_type": INE_AUTONOMOUS_COMMUNITY_CODE_TYPE,
            },
        },
        {
            "unit_level": TERRITORIAL_UNIT_LEVEL_PROVINCE,
            "country_code": "ES",
            "units_total": 1,
            "active_units": 1,
            "geometry_units": 1,
            "centroid_units": 1,
            "boundary_source": "ign_administrative_boundaries",
            "canonical_code_strategy": {
                "source_system": INE_TERRITORIAL_SOURCE_SYSTEM,
                "code_type": INE_PROVINCE_CODE_TYPE,
            },
        },
        {
            "unit_level": TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
            "country_code": "ES",
            "units_total": 2,
            "active_units": 1,
            "geometry_units": 1,
            "centroid_units": 1,
            "boundary_source": "ign_administrative_boundaries",
            "canonical_code_strategy": {
                "source_system": INE_TERRITORIAL_SOURCE_SYSTEM,
                "code_type": INE_MUNICIPALITY_CODE_TYPE,
            },
        },
    ]
    assert len(session.statements) == 12


def test_get_unit_by_canonical_code_serializes_lookup_with_primary_code():
    unit = SimpleNamespace(
        id=7,
        parent_id=3,
        unit_level=TERRITORIAL_UNIT_LEVEL_PROVINCE,
        canonical_name="Asturias",
        display_name="Principado de Asturias",
        country_code="ES",
        is_active=True,
    )
    code = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_PROVINCE_CODE_TYPE,
        code_value="33",
        is_primary=True,
    )
    session = FakeSession(
        FakeExecuteResult(first_value=(unit, code)),
        FakeExecuteResult(scalar_values=[code]),
    )
    repository = TerritorialRepository(session=session)

    result = asyncio.run(
        repository.get_unit_by_canonical_code(
            unit_level=TERRITORIAL_UNIT_LEVEL_PROVINCE,
            code_value="33",
        )
    )

    assert result == {
        "id": 7,
        "parent_id": 3,
        "unit_level": TERRITORIAL_UNIT_LEVEL_PROVINCE,
        "canonical_name": "Asturias",
        "display_name": "Principado de Asturias",
        "country_code": "ES",
        "is_active": True,
        "canonical_code_strategy": {
            "source_system": INE_TERRITORIAL_SOURCE_SYSTEM,
            "code_type": INE_PROVINCE_CODE_TYPE,
        },
        "canonical_code": {
            "source_system": INE_TERRITORIAL_SOURCE_SYSTEM,
            "code_type": INE_PROVINCE_CODE_TYPE,
            "code_value": "33",
            "is_primary": True,
        },
        "matched_by": "code",
        "matched_code": {
            "source_system": INE_TERRITORIAL_SOURCE_SYSTEM,
            "code_type": INE_PROVINCE_CODE_TYPE,
            "code_value": "33",
            "is_primary": True,
        },
        "matched_alias": None,
    }


def test_get_unit_by_alias_serializes_lookup_with_alias_and_canonical_code():
    unit = SimpleNamespace(
        id=11,
        parent_id=5,
        unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
        canonical_name="Oviedo",
        display_name="Oviedo",
        country_code="ES",
        is_active=True,
    )
    alias = SimpleNamespace(
        source_system="internal",
        alias="Uvieu",
        normalized_alias="uvieu",
        alias_type="name",
    )
    canonical_code = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_MUNICIPALITY_CODE_TYPE,
        code_value="33044",
        is_primary=True,
    )
    session = FakeSession(
        FakeExecuteResult(first_value=(unit, alias)),
        FakeExecuteResult(scalar_values=[canonical_code]),
    )
    repository = TerritorialRepository(session=session)

    result = asyncio.run(
        repository.get_unit_by_alias(
            normalized_alias="uvieu",
            source_system="internal",
            alias_type="name",
            unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
        )
    )

    assert result["matched_by"] == "alias"
    assert result["matched_alias"] == {
        "source_system": "internal",
        "alias": "Uvieu",
        "normalized_alias": "uvieu",
        "alias_type": "name",
    }
    assert result["canonical_code"] == {
        "source_system": INE_TERRITORIAL_SOURCE_SYSTEM,
        "code_type": INE_MUNICIPALITY_CODE_TYPE,
        "code_value": "33044",
        "is_primary": True,
    }


def test_get_unit_geometry_by_canonical_code_serializes_geojson_summary():
    unit = SimpleNamespace(
        id=33044,
        parent_id=33,
        unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
        canonical_name="Oviedo",
        display_name="Oviedo",
        country_code="ES",
        is_active=True,
        attributes_json={
            "boundary_source": {
                "source": "ign_administrative_boundaries",
                "dataset_version": "ign-admin-v1",
                "provider_source": "ign_admin",
            }
        },
    )
    canonical_code = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_MUNICIPALITY_CODE_TYPE,
        code_value="33044",
        is_primary=True,
    )
    session = FakeSession(
        FakeExecuteResult(
            first_value=(
                unit,
                canonical_code,
                '{"type":"MultiPolygon","coordinates":[[[[0,0],[1,0],[1,1],[0,0]]]]}',
                '{"type":"Point","coordinates":[0.5,0.5]}',
                "ST_MultiPolygon",
                4326,
            )
        )
    )
    repository = TerritorialRepository(session=session)

    result = asyncio.run(
        repository.get_unit_geometry_by_canonical_code(
            unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
            code_value="33044",
        )
    )

    assert result == {
        "territorial_context": {
            "id": 33044,
            "parent_id": 33,
            "unit_level": TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
            "canonical_name": "Oviedo",
            "display_name": "Oviedo",
            "country_code": "ES",
            "is_active": True,
            "canonical_code_strategy": {
                "source_system": INE_TERRITORIAL_SOURCE_SYSTEM,
                "code_type": INE_MUNICIPALITY_CODE_TYPE,
            },
            "canonical_code": {
                "source_system": INE_TERRITORIAL_SOURCE_SYSTEM,
                "code_type": INE_MUNICIPALITY_CODE_TYPE,
                "code_value": "33044",
                "is_primary": True,
            },
        },
        "summary": {
            "has_geometry": True,
            "has_centroid": True,
            "geometry_type": "MultiPolygon",
            "srid": 4326,
            "boundary_source": "ign_administrative_boundaries",
        },
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [[[[0, 0], [1, 0], [1, 1], [0, 0]]]],
        },
        "centroid": {"type": "Point", "coordinates": [0.5, 0.5]},
        "metadata": {
            "boundary_dataset_version": "ign-admin-v1",
            "provider_source": "ign_admin",
        },
    }


def test_get_unit_by_name_matches_canonical_name_before_alias():
    unit = SimpleNamespace(
        id=21,
        parent_id=7,
        unit_level=TERRITORIAL_UNIT_LEVEL_PROVINCE,
        canonical_name="Asturias",
        normalized_name="asturias",
        display_name="Principado de Asturias",
        country_code="ES",
        is_active=True,
    )
    canonical_code = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_PROVINCE_CODE_TYPE,
        code_value="33",
        is_primary=True,
    )
    session = FakeSession(
        FakeExecuteResult(scalar_values=[unit]),
        FakeExecuteResult(scalar_values=[canonical_code]),
    )
    repository = TerritorialRepository(session=session)

    result = asyncio.run(
        repository.get_unit_by_name(
            name=" Asturias ",
            unit_level=TERRITORIAL_UNIT_LEVEL_PROVINCE,
        )
    )

    assert result["matched_by"] == TERRITORIAL_MATCHED_BY_CANONICAL_NAME
    assert result["matched_alias"] is None
    assert len(session.statements) == 2


def test_get_unit_by_name_falls_back_to_normalized_alias_lookup():
    unit = SimpleNamespace(
        id=11,
        parent_id=5,
        unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
        canonical_name="Oviedo",
        normalized_name="oviedo",
        display_name="Oviedo",
        country_code="ES",
        is_active=True,
    )
    alias = SimpleNamespace(
        id=4,
        source_system="internal",
        alias="Uviéu",
        normalized_alias="uvieu",
        alias_type="alternate_name",
    )
    canonical_code = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_MUNICIPALITY_CODE_TYPE,
        code_value="33044",
        is_primary=True,
    )
    session = FakeSession(
        FakeExecuteResult(scalar_values=[]),
        FakeExecuteResult(first_value=(unit, alias)),
        FakeExecuteResult(scalar_values=[canonical_code]),
    )
    repository = TerritorialRepository(session=session)

    result = asyncio.run(
        repository.get_unit_by_name(
            name=" Uviéu ",
            source_system="internal",
            alias_type="alternate_name",
            unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
        )
    )

    assert result["matched_by"] == "alias"
    assert result["matched_alias"] == {
        "source_system": "internal",
        "alias": "Uviéu",
        "normalized_alias": "uvieu",
        "alias_type": "alternate_name",
    }


def test_list_codes_returns_serialized_codes():
    rows = [
        SimpleNamespace(
            source_system="ine",
            code_type="province",
            code_value="33",
            is_primary=False,
        ),
        SimpleNamespace(
            source_system="ine",
            code_type="province",
            code_value="033",
            is_primary=True,
        ),
    ]
    session = FakeSession(FakeExecuteResult(scalar_values=rows))
    repository = TerritorialRepository(session=session)

    result = asyncio.run(repository.list_codes(territorial_unit_id=3))

    assert result == [
        {
            "source_system": "ine",
            "code_type": "province",
            "code_value": "33",
            "is_primary": False,
        },
        {
            "source_system": "ine",
            "code_type": "province",
            "code_value": "033",
            "is_primary": True,
        },
    ]


def test_list_units_returns_paginated_serialized_results():
    units = [
        SimpleNamespace(
            id=1,
            parent_id=None,
            unit_level=TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
            canonical_name="Asturias",
            display_name="Principado de Asturias",
            country_code="ES",
            is_active=True,
        ),
    ]
    code_1 = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_AUTONOMOUS_COMMUNITY_CODE_TYPE,
        code_value="03",
        is_primary=True,
    )
    code_2 = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_AUTONOMOUS_COMMUNITY_CODE_TYPE,
        code_value="13",
        is_primary=True,
        territorial_unit_id=2,
    )
    units.append(
        SimpleNamespace(
            id=2,
            parent_id=None,
            unit_level=TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
            canonical_name="Madrid",
            display_name="Comunidad de Madrid",
            country_code="ES",
            is_active=True,
        )
    )
    code_1.territorial_unit_id = 1
    session = FakeSession(
        FakeExecuteResult(scalar_values=[2]),
        FakeExecuteResult(scalar_values=units),
        FakeExecuteResult(scalar_values=[code_1, code_2]),
    )
    repository = TerritorialRepository(session=session)
    repository._get_canonical_code_for_unit = unexpected_canonical_code_lookup

    result = asyncio.run(
        repository.list_units(
            unit_level=TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
            page=1,
            page_size=2,
            country_code="ES",
        )
    )

    assert result["total"] == 2
    assert result["page"] == 1
    assert result["page_size"] == 2
    assert result["pages"] == 1
    assert result["has_next"] is False
    assert result["filters"]["unit_level"] == TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY
    assert result["items"][0]["canonical_name"] == "Asturias"
    assert result["items"][0]["canonical_code"]["code_value"] == "03"
    assert result["items"][1]["canonical_code"]["code_value"] == "13"
    assert len(session.statements) == 3


def test_get_unit_detail_by_canonical_code_returns_codes_aliases_and_attributes():
    unit = SimpleNamespace(
        id=44,
        parent_id=33,
        unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
        canonical_name="Oviedo",
        display_name="Oviedo",
        country_code="ES",
        is_active=True,
        attributes_json={"population_scope": "municipal"},
    )
    canonical_code = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_MUNICIPALITY_CODE_TYPE,
        code_value="33044",
        is_primary=True,
    )
    alternate_code = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_MUNICIPALITY_CODE_TYPE,
        code_value="330440000",
        is_primary=False,
    )
    alias = SimpleNamespace(
        id=1,
        source_system="internal",
        alias="Uvieu",
        normalized_alias="uvieu",
        alias_type="alternate_name",
    )
    session = FakeSession(
        FakeExecuteResult(first_value=(unit, canonical_code)),
        FakeExecuteResult(scalar_values=[canonical_code]),
        FakeExecuteResult(scalar_values=[unit]),
        FakeExecuteResult(scalar_values=[canonical_code]),
        FakeExecuteResult(scalar_values=[canonical_code, alternate_code]),
        FakeExecuteResult(scalar_values=[alias]),
    )
    repository = TerritorialRepository(session=session)

    result = asyncio.run(
        repository.get_unit_detail_by_canonical_code(
            unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
            code_value="33044",
        )
    )

    assert result is not None
    assert result["canonical_name"] == "Oviedo"
    assert result["canonical_code"]["code_value"] == "33044"
    assert len(result["codes"]) == 2
    assert result["aliases"][0]["alias"] == "Uvieu"
    assert result["attributes"] == {"population_scope": "municipal"}


def test_get_unit_detail_by_id_returns_codes_aliases_and_attributes():
    unit = SimpleNamespace(
        id=44,
        parent_id=33,
        unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
        canonical_name="Oviedo",
        display_name="Oviedo",
        country_code="ES",
        is_active=True,
        attributes_json={"population_scope": "municipal"},
    )
    canonical_code = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_MUNICIPALITY_CODE_TYPE,
        code_value="33044",
        is_primary=True,
    )
    alternate_code = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_MUNICIPALITY_CODE_TYPE,
        code_value="330440000",
        is_primary=False,
    )
    alias = SimpleNamespace(
        id=1,
        source_system="internal",
        alias="Uvieu",
        normalized_alias="uvieu",
        alias_type="alternate_name",
    )
    session = FakeSession(
        FakeExecuteResult(scalar_values=[unit]),
        FakeExecuteResult(scalar_values=[canonical_code]),
        FakeExecuteResult(scalar_values=[canonical_code, alternate_code]),
        FakeExecuteResult(scalar_values=[alias]),
    )
    repository = TerritorialRepository(session=session)

    result = asyncio.run(repository.get_unit_detail_by_id(44))

    assert result is not None
    assert result["id"] == 44
    assert result["canonical_code"]["code_value"] == "33044"
    assert len(result["codes"]) == 2
    assert result["aliases"][0]["alias"] == "Uvieu"
    assert result["attributes"] == {"population_scope": "municipal"}


def test_list_hierarchy_returns_root_to_leaf_order():
    country = SimpleNamespace(
        id=1,
        parent_id=None,
        unit_level=TERRITORIAL_UNIT_LEVEL_COUNTRY,
        canonical_name="Espana",
        display_name="Espana",
        country_code="ES",
        is_active=True,
    )
    province = SimpleNamespace(
        id=33,
        parent_id=1,
        unit_level=TERRITORIAL_UNIT_LEVEL_PROVINCE,
        canonical_name="Asturias",
        display_name="Asturias",
        country_code="ES",
        is_active=True,
    )
    municipality = SimpleNamespace(
        id=44,
        parent_id=33,
        unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
        canonical_name="Oviedo",
        display_name="Oviedo",
        country_code="ES",
        is_active=True,
    )
    country_code = SimpleNamespace(
        source_system=ISO3166_TERRITORIAL_SOURCE_SYSTEM,
        code_type=ISO3166_ALPHA2_CODE_TYPE,
        code_value="ES",
        is_primary=True,
    )
    province_code = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_PROVINCE_CODE_TYPE,
        code_value="33",
        is_primary=True,
    )
    municipality_code = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_MUNICIPALITY_CODE_TYPE,
        code_value="33044",
        is_primary=True,
    )
    session = FakeSession(
        FakeExecuteResult(scalar_values=[municipality]),
        FakeExecuteResult(scalar_values=[province]),
        FakeExecuteResult(scalar_values=[country]),
        FakeExecuteResult(scalar_values=[country_code]),
        FakeExecuteResult(scalar_values=[province_code]),
        FakeExecuteResult(scalar_values=[municipality_code]),
    )
    repository = TerritorialRepository(session=session)

    result = asyncio.run(repository.list_hierarchy(44))

    assert [item["unit_level"] for item in result] == [
        TERRITORIAL_UNIT_LEVEL_COUNTRY,
        TERRITORIAL_UNIT_LEVEL_PROVINCE,
        TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    ]
    assert [item["canonical_code"]["code_value"] for item in result] == ["ES", "33", "33044"]


def test_resolve_point_returns_hierarchy_and_best_match():
    country = SimpleNamespace(
        id=1,
        parent_id=None,
        unit_level=TERRITORIAL_UNIT_LEVEL_COUNTRY,
        canonical_name="Espana",
        display_name="Espana",
        country_code="ES",
        is_active=True,
    )
    community = SimpleNamespace(
        id=2,
        parent_id=1,
        unit_level=TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
        canonical_name="Asturias",
        display_name="Principado de Asturias",
        country_code="ES",
        is_active=True,
    )
    province = SimpleNamespace(
        id=3,
        parent_id=2,
        unit_level=TERRITORIAL_UNIT_LEVEL_PROVINCE,
        canonical_name="Asturias",
        display_name="Asturias",
        country_code="ES",
        is_active=True,
    )
    municipality = SimpleNamespace(
        id=4,
        parent_id=3,
        unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
        canonical_name="Oviedo",
        display_name="Oviedo",
        country_code="ES",
        is_active=True,
    )
    country_code = SimpleNamespace(
        source_system=ISO3166_TERRITORIAL_SOURCE_SYSTEM,
        code_type=ISO3166_ALPHA2_CODE_TYPE,
        code_value="ES",
        is_primary=True,
    )
    community_code = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_AUTONOMOUS_COMMUNITY_CODE_TYPE,
        code_value="03",
        is_primary=True,
    )
    province_code = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_PROVINCE_CODE_TYPE,
        code_value="33",
        is_primary=True,
    )
    municipality_code = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_MUNICIPALITY_CODE_TYPE,
        code_value="33044",
        is_primary=True,
    )
    session = FakeSession(
        FakeExecuteResult(
            all_values=[
                (TERRITORIAL_UNIT_LEVEL_COUNTRY, 1),
                (TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY, 1),
                (TERRITORIAL_UNIT_LEVEL_PROVINCE, 1),
                (TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, 1),
            ]
        ),
        FakeExecuteResult(all_values=[(country, country_code)]),
        FakeExecuteResult(all_values=[(community, community_code)]),
        FakeExecuteResult(all_values=[(province, province_code)]),
        FakeExecuteResult(all_values=[(municipality, municipality_code)]),
    )
    repository = TerritorialRepository(session=session)

    result = asyncio.run(repository.resolve_point(lat=43.3614, lon=-5.8494))

    assert result["matched_by"] == "geometry_cover"
    assert result["coverage"] == {
        "boundary_source": "ign_administrative_boundaries",
        "levels_considered": ["country", "autonomous_community", "province", "municipality"],
        "levels_loaded": ["country", "autonomous_community", "province", "municipality"],
        "levels_missing_geometry": [],
        "levels_matched": ["country", "autonomous_community", "province", "municipality"],
        "coverage_status": "full",
    }
    assert result["best_match"]["canonical_code"]["code_value"] == "33044"
    assert [item["unit_level"] for item in result["hierarchy"]] == [
        "country",
        "autonomous_community",
        "province",
        "municipality",
    ]
    assert result["ambiguity_detected"] is False


def test_resolve_point_returns_no_coverage_when_no_boundaries_are_loaded():
    session = FakeSession(
        FakeExecuteResult(all_values=[]),
        FakeExecuteResult(all_values=[]),
        FakeExecuteResult(all_values=[]),
        FakeExecuteResult(all_values=[]),
        FakeExecuteResult(all_values=[]),
    )
    repository = TerritorialRepository(session=session)

    result = asyncio.run(repository.resolve_point(lat=43.3614, lon=-5.8494))

    assert result["best_match"] is None
    assert result["hierarchy"] == []
    assert result["coverage"] == {
        "boundary_source": None,
        "levels_considered": ["country", "autonomous_community", "province", "municipality"],
        "levels_loaded": [],
        "levels_missing_geometry": ["country", "autonomous_community", "province", "municipality"],
        "levels_matched": [],
        "coverage_status": "none",
    }
    assert result["ambiguity_detected"] is False


def test_resolve_point_marks_ambiguity_when_multiple_units_cover_same_level():
    municipality_a = SimpleNamespace(
        id=4,
        parent_id=3,
        unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
        canonical_name="Oviedo",
        display_name="Oviedo",
        country_code="ES",
        is_active=True,
    )
    municipality_b = SimpleNamespace(
        id=5,
        parent_id=3,
        unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
        canonical_name="Llanera",
        display_name="Llanera",
        country_code="ES",
        is_active=True,
    )
    municipality_code_a = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_MUNICIPALITY_CODE_TYPE,
        code_value="33044",
        is_primary=True,
    )
    municipality_code_b = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_MUNICIPALITY_CODE_TYPE,
        code_value="33035",
        is_primary=True,
    )
    session = FakeSession(
        FakeExecuteResult(all_values=[(TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, 2)]),
        FakeExecuteResult(all_values=[]),
        FakeExecuteResult(all_values=[]),
        FakeExecuteResult(all_values=[]),
        FakeExecuteResult(
            all_values=[
                (municipality_a, municipality_code_a),
                (municipality_b, municipality_code_b),
            ]
        ),
    )
    repository = TerritorialRepository(session=session)

    result = asyncio.run(repository.resolve_point(lat=43.4, lon=-5.8))

    assert result["best_match"]["canonical_code"]["code_value"] == "33044"
    assert result["ambiguity_detected"] is True
    assert result["ambiguity_by_level"]["municipality"] == [
        {
            "territorial_unit_id": 4,
            "canonical_name": "Oviedo",
            "canonical_code": "33044",
        },
        {
            "territorial_unit_id": 5,
            "canonical_name": "Llanera",
            "canonical_code": "33035",
        },
    ]


def test_resolve_point_returns_deepest_available_match_when_municipality_is_not_loaded():
    country = SimpleNamespace(
        id=1,
        parent_id=None,
        unit_level=TERRITORIAL_UNIT_LEVEL_COUNTRY,
        canonical_name="Espana",
        display_name="Espana",
        country_code="ES",
        is_active=True,
    )
    community = SimpleNamespace(
        id=2,
        parent_id=1,
        unit_level=TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
        canonical_name="Asturias",
        display_name="Principado de Asturias",
        country_code="ES",
        is_active=True,
    )
    province = SimpleNamespace(
        id=3,
        parent_id=2,
        unit_level=TERRITORIAL_UNIT_LEVEL_PROVINCE,
        canonical_name="Asturias",
        display_name="Asturias",
        country_code="ES",
        is_active=True,
    )
    country_code = SimpleNamespace(
        source_system=ISO3166_TERRITORIAL_SOURCE_SYSTEM,
        code_type=ISO3166_ALPHA2_CODE_TYPE,
        code_value="ES",
        is_primary=True,
    )
    community_code = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_AUTONOMOUS_COMMUNITY_CODE_TYPE,
        code_value="03",
        is_primary=True,
    )
    province_code = SimpleNamespace(
        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
        code_type=INE_PROVINCE_CODE_TYPE,
        code_value="33",
        is_primary=True,
    )
    session = FakeSession(
        FakeExecuteResult(
            all_values=[
                (TERRITORIAL_UNIT_LEVEL_COUNTRY, 1),
                (TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY, 1),
                (TERRITORIAL_UNIT_LEVEL_PROVINCE, 1),
            ]
        ),
        FakeExecuteResult(all_values=[(country, country_code)]),
        FakeExecuteResult(all_values=[(community, community_code)]),
        FakeExecuteResult(all_values=[(province, province_code)]),
        FakeExecuteResult(all_values=[]),
    )
    repository = TerritorialRepository(session=session)

    result = asyncio.run(repository.resolve_point(lat=43.1, lon=-5.8))

    assert result["best_match"]["unit_level"] == TERRITORIAL_UNIT_LEVEL_PROVINCE
    assert result["best_match"]["canonical_code"]["code_value"] == "33"
    assert result["coverage"] == {
        "boundary_source": "ign_administrative_boundaries",
        "levels_considered": ["country", "autonomous_community", "province", "municipality"],
        "levels_loaded": ["country", "autonomous_community", "province"],
        "levels_missing_geometry": ["municipality"],
        "levels_matched": ["country", "autonomous_community", "province"],
        "coverage_status": "partial",
    }
    assert [item["unit_level"] for item in result["hierarchy"]] == [
        "country",
        "autonomous_community",
        "province",
    ]
