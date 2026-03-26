from app.repositories.territorial import (
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
)
from app.schemas import NormalizedSeriesItem


def _territorial_summary(
    *,
    unit_id: int,
    parent_id: int | None,
    unit_level: str,
    canonical_name: str,
    display_name: str,
    code_type: str,
    code_value: str,
    source_system: str = "ine",
) -> dict:
    return {
        "id": unit_id,
        "parent_id": parent_id,
        "unit_level": unit_level,
        "canonical_name": canonical_name,
        "display_name": display_name,
        "country_code": "ES",
        "is_active": True,
        "canonical_code_strategy": {
            "source_system": source_system,
            "code_type": code_type,
        },
        "canonical_code": {
            "source_system": source_system,
            "code_type": code_type,
            "code_value": code_value,
            "is_primary": True,
        },
    }


def _territorial_detail(
    *,
    unit_id: int,
    parent_id: int | None,
    unit_level: str,
    canonical_name: str,
    display_name: str,
    code_type: str,
    code_value: str,
    source_system: str = "ine",
) -> dict:
    payload = _territorial_summary(
        unit_id=unit_id,
        parent_id=parent_id,
        unit_level=unit_level,
        canonical_name=canonical_name,
        display_name=display_name,
        code_type=code_type,
        code_value=code_value,
        source_system=source_system,
    )
    payload["codes"] = [payload["canonical_code"]]
    payload["aliases"] = []
    payload["attributes"] = {
        "population_scope": "municipal" if unit_level == "municipality" else "regional"
    }
    return payload


def _seed_municipality_dossier_context(dummy_territorial_repo, dummy_series_repo) -> None:
    municipality_detail = _territorial_detail(
        unit_id=33044,
        parent_id=33,
        unit_level="municipality",
        canonical_name="Oviedo",
        display_name="Oviedo",
        code_type="municipality",
        code_value="33044",
    )
    dummy_territorial_repo.detail_by_canonical_code[
        (TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, "33044")
    ] = municipality_detail
    dummy_territorial_repo.detail_by_id[33044] = municipality_detail
    dummy_territorial_repo.hierarchy_by_unit_id[33044] = [
        _territorial_summary(
            unit_id=1,
            parent_id=None,
            unit_level="country",
            canonical_name="España",
            display_name="España",
            code_type="alpha2",
            code_value="ES",
            source_system="iso3166",
        ),
        _territorial_summary(
            unit_id=2,
            parent_id=1,
            unit_level="autonomous_community",
            canonical_name="Asturias",
            display_name="Principado de Asturias",
            code_type="autonomous_community",
            code_value="03",
        ),
        _territorial_summary(
            unit_id=33,
            parent_id=2,
            unit_level="province",
            canonical_name="Asturias",
            display_name="Asturias",
            code_type="province",
            code_value="33",
        ),
        _territorial_summary(
            unit_id=33044,
            parent_id=33,
            unit_level="municipality",
            canonical_name="Oviedo",
            display_name="Oviedo",
            code_type="municipality",
            code_value="33044",
        ),
    ]
    dummy_territorial_repo.geometry_by_canonical_code[
        (TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, "33044")
    ] = {
        "territorial_context": _territorial_summary(
            unit_id=33044,
            parent_id=33,
            unit_level="municipality",
            canonical_name="Oviedo",
            display_name="Oviedo",
            code_type="municipality",
            code_value="33044",
        ),
        "summary": {
            "has_geometry": True,
            "has_centroid": True,
            "geometry_type": "MultiPolygon",
            "srid": 4326,
            "boundary_source": "ign_administrative_boundaries",
        },
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [[[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]]],
        },
        "centroid": {"type": "Point", "coordinates": [0.5, 0.5]},
        "metadata": {
            "boundary_dataset_version": "ign-admin-v1",
            "provider_source": "ign_admin",
        },
    }
    dummy_series_repo.items.extend(
        [
            NormalizedSeriesItem(
                operation_code="71",
                table_id="24369",
                variable_id="MIG_NET",
                geography_name="Oviedo",
                geography_code="33044",
                period="2024",
                value=150.0,
                unit="personas",
                metadata={"series_name": "Saldo migratorio"},
            ),
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP_TOTAL",
                geography_name="Oviedo",
                geography_code="33044",
                period="2024",
                value=220543,
                unit="personas",
                metadata={"series_name": "Poblacion total"},
            ),
            NormalizedSeriesItem(
                operation_code="33",
                table_id="3901",
                variable_id="AGEING_INDEX",
                geography_name="Oviedo",
                geography_code="33044",
                period="2024M01",
                value=142.5,
                unit="indice",
                metadata={"series_name": "Indice de envejecimiento"},
            ),
        ]
    )


def _seed_province_dossier_context(dummy_territorial_repo, dummy_series_repo) -> None:
    province_detail = _territorial_detail(
        unit_id=33,
        parent_id=2,
        unit_level="province",
        canonical_name="Asturias",
        display_name="Asturias",
        code_type="province",
        code_value="33",
    )
    dummy_territorial_repo.detail_by_canonical_code[(TERRITORIAL_UNIT_LEVEL_PROVINCE, "33")] = (
        province_detail
    )
    dummy_territorial_repo.detail_by_id[33] = province_detail
    dummy_territorial_repo.hierarchy_by_unit_id[33] = [
        _territorial_summary(
            unit_id=1,
            parent_id=None,
            unit_level="country",
            canonical_name="España",
            display_name="España",
            code_type="alpha2",
            code_value="ES",
            source_system="iso3166",
        ),
        _territorial_summary(
            unit_id=2,
            parent_id=1,
            unit_level="autonomous_community",
            canonical_name="Asturias",
            display_name="Principado de Asturias",
            code_type="autonomous_community",
            code_value="03",
        ),
        _territorial_summary(
            unit_id=33,
            parent_id=2,
            unit_level="province",
            canonical_name="Asturias",
            display_name="Asturias",
            code_type="province",
            code_value="33",
        ),
    ]
    dummy_series_repo.items.extend(
        [
            NormalizedSeriesItem(
                operation_code="71",
                table_id="24369",
                variable_id="MIG_NET",
                geography_name="Asturias",
                geography_code="33",
                period="2024",
                value=900.0,
                unit="personas",
                metadata={"series_name": "Saldo migratorio"},
            ),
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP_TOTAL",
                geography_name="Asturias",
                geography_code="33",
                period="2024",
                value=1011792,
                unit="personas",
                metadata={"series_name": "Poblacion total"},
            ),
        ]
    )


def test_get_territorial_dossier_returns_composite_municipality_payload(
    client,
    dummy_territorial_repo,
    dummy_series_repo,
    dummy_catastro_client_service,
):
    _seed_municipality_dossier_context(dummy_territorial_repo, dummy_series_repo)

    response = client.get("/territorios/municipality/33044/dossier")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "internal.territorial.dossier"
    assert payload["territorial_context"]["canonical_code"]["code_value"] == "33044"
    assert payload["summary"] == {
        "resolved": True,
        "has_geometry": True,
        "geometry_coverage_status": "available",
        "has_ine_data": True,
        "has_catastro_data": True,
        "partial_sections": False,
        "section_count": 4,
    }
    assert payload["sections"]["identity"]["unit"]["canonical_name"] == "Oviedo"
    assert payload["sections"]["geometry"]["summary"]["boundary_source"] == (
        "ign_administrative_boundaries"
    )
    assert payload["sections"]["ine"]["summary"]["coverage_status"] == "full"
    assert payload["sections"]["ine"]["summary"]["operations_present"] == ["71", "22", "33"]
    assert payload["sections"]["catastro"]["source"] == "catastro.municipality.aggregates"
    assert payload["sections"]["catastro"]["summary"]["coverage_status"] == "complete"
    assert payload["sections"]["catastro"]["summary"]["reference_year"] == "2025"
    assert payload["metadata"]["included_sections"] == ["identity", "geometry", "ine", "catastro"]
    assert len(dummy_catastro_client_service.calls) == 1


def test_get_territorial_dossier_respects_include_flags_for_province(
    client,
    dummy_territorial_repo,
    dummy_series_repo,
    dummy_catastro_client_service,
):
    _seed_province_dossier_context(dummy_territorial_repo, dummy_series_repo)

    response = client.get(
        "/territorios/province/33/dossier?include_geometry=false&include_catastro=false"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["territorial_context"]["unit_level"] == "province"
    assert payload["summary"]["section_count"] == 2
    assert payload["summary"]["has_geometry"] is False
    assert payload["summary"]["has_ine_data"] is True
    assert payload["summary"]["has_catastro_data"] is False
    assert payload["sections"]["geometry"] is None
    assert payload["sections"]["catastro"] is None
    assert payload["sections"]["ine"]["summary"]["coverage_status"] == "partial"
    assert payload["metadata"]["included_sections"] == ["identity", "ine"]
    assert dummy_catastro_client_service.calls == []


def test_get_territorial_dossier_returns_404_when_unit_is_unknown(client):
    response = client.get("/territorios/municipality/99999/dossier")

    assert response.status_code == 404
    assert response.json()["detail"] == {
        "message": "Territorial unit code was not found.",
        "unit_level": "municipality",
        "code_value": "99999",
    }
