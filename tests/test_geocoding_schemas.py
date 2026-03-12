from app.schemas import (
    GeocodeResponse,
    ReverseGeocodeResponse,
)


def test_geocode_response_schema_supports_semantic_contract() -> None:
    response = GeocodeResponse.model_validate(
        {
            "source": "cartociudad",
            "query": "Oviedo",
            "cached": True,
            "result": {
                "label": "Oviedo",
                "entity_type": "municipality",
                "coordinates": {"lat": 43.3614, "lon": -5.8494},
                "territorial_context": {
                    "country_code": "ES",
                    "autonomous_community_code": "33",
                    "province_code": "33",
                    "municipality_code": "33044",
                    "autonomous_community_name": "Principado de Asturias",
                    "province_name": "Asturias",
                    "municipality_name": "Oviedo",
                },
                "territorial_resolution": {
                    "territorial_unit_id": 44,
                    "matched_by": "canonical_name",
                    "canonical_name": "Oviedo",
                    "canonical_code": "33044",
                    "source_system": "ine",
                    "unit_level": "municipality",
                },
            },
        }
    )

    assert response.source == "cartociudad"
    assert response.cached is True
    assert response.result is not None
    assert response.result.coordinates.lat == 43.3614
    assert response.result.territorial_context.municipality_code == "33044"
    assert response.result.territorial_resolution is not None
    assert response.result.territorial_resolution.canonical_code == "33044"


def test_reverse_geocode_response_schema_supports_semantic_contract() -> None:
    response = ReverseGeocodeResponse.model_validate(
        {
            "source": "cartociudad",
            "query_coordinates": {"lat": 43.3614, "lon": -5.8494},
            "result": {
                "label": "Oviedo, Asturias",
                "entity_type": "address",
                "coordinates": {"lat": 43.3614, "lon": -5.8494},
                "address": "Oviedo, Asturias",
                "territorial_context": {
                    "country_code": "ES",
                    "autonomous_community_code": "33",
                    "province_code": "33",
                    "municipality_code": "33044",
                },
            },
        }
    )

    assert response.source == "cartociudad"
    assert response.query_coordinates.lon == -5.8494
    assert response.result is not None
    assert response.result.address == "Oviedo, Asturias"
    assert response.result.territorial_context.country_code == "ES"
