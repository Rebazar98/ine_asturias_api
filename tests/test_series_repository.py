import asyncio

from app.repositories.series import SeriesRepository
from app.schemas import NormalizedSeriesItem


def test_prepare_upsert_rows_returns_plain_insertable_dicts():
    items = [
        NormalizedSeriesItem(
            operation_code="22",
            table_id="2852",
            variable_id="70",
            territorial_unit_id=44,
            geography_name="Principado de Asturias",
            geography_code="8999",
            period="2024M01",
            value=101.5,
            unit="indice",
            metadata={"source": "test"},
            raw_payload={"nested": {"value": 1}},
        )
    ]

    rows = SeriesRepository.prepare_upsert_rows(items)

    assert len(rows) == 1
    assert rows[0] == {
        "operation_code": "22",
        "table_id": "2852",
        "variable_id": "70",
        "territorial_unit_id": 44,
        "geography_name": "Principado de Asturias",
        "geography_code": "8999",
        "period": "2024M01",
        "value": 101.5,
        "unit": "indice",
        "metadata": {"source": "test"},
        "raw_payload": {"nested": {"value": 1}},
        "source_provider": "ine",
    }


def test_prepare_upsert_rows_filters_non_serializable_or_extra_values():
    raw_object = object()
    items = [
        {
            "operation_code": 22,
            "table_id": 2852,
            "variable_id": None,
            "territorial_unit_id": "44",
            "geography_name": "Asturias",
            "geography_code": 8999,
            "period": "2024",
            "value": "101.5",
            "unit": None,
            "metadata_json": {"note": raw_object},
            "raw_payload": [1, 2, 3],
            "unexpected": "ignored",
        }
    ]

    rows = SeriesRepository.prepare_upsert_rows(items)

    assert len(rows) == 1
    assert set(rows[0].keys()) == {
        "operation_code",
        "table_id",
        "variable_id",
        "territorial_unit_id",
        "geography_name",
        "geography_code",
        "period",
        "value",
        "unit",
        "metadata",
        "raw_payload",
        "source_provider",
    }
    assert rows[0]["operation_code"] == "22"
    assert rows[0]["table_id"] == "2852"
    assert rows[0]["variable_id"] == ""
    assert rows[0]["territorial_unit_id"] == 44
    assert rows[0]["value"] == 101.5
    assert rows[0]["metadata"] == {"note": str(raw_object)}
    assert rows[0]["raw_payload"] == {"items": [1, 2, 3]}


def test_serialize_latest_indicator_item_builds_semantic_payload():
    item = {
        "operation_code": "22",
        "table_id": "2852",
        "variable_id": "POP_TOTAL",
        "geography_name": "Oviedo",
        "geography_code": "33044",
        "period": "2024",
        "value": 220543,
        "unit": "personas",
        "metadata": {"series_name": "Poblacion total"},
    }

    payload = SeriesRepository.serialize_latest_indicator_item(item)

    assert payload == {
        "series_key": "ine.22.2852.POP_TOTAL",
        "label": "Poblacion total",
        "value": 220543,
        "unit": "personas",
        "period": "2024",
        "metadata": {"series_name": "Poblacion total"},
        "operation_code": "22",
        "table_id": "2852",
        "variable_id": "POP_TOTAL",
        "geography_code": "33044",
        "geography_name": "Oviedo",
    }


def test_list_latest_indicators_by_geography_returns_empty_when_database_disabled():
    repository = SeriesRepository(session=None)

    result = asyncio.run(
        repository.list_latest_indicators_by_geography(
            geography_code="33044",
            page=1,
            page_size=10,
        )
    )

    assert result == {
        "items": [],
        "total": 0,
        "page": 1,
        "page_size": 10,
        "pages": 0,
        "has_next": False,
        "has_previous": False,
        "filters": {
            "geography_code": "33044",
            "geography_code_system": "ine",
            "operation_code": None,
            "variable_id": None,
            "period_from": None,
            "period_to": None,
        },
        "summary": {
            "operation_codes": [],
            "latest_period": None,
        },
    }
