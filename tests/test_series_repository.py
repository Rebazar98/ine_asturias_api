from app.repositories.series import SeriesRepository
from app.schemas import NormalizedSeriesItem


def test_prepare_upsert_rows_returns_plain_insertable_dicts():
    items = [
        NormalizedSeriesItem(
            operation_code="22",
            table_id="2852",
            variable_id="70",
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
        "geography_name": "Principado de Asturias",
        "geography_code": "8999",
        "period": "2024M01",
        "value": 101.5,
        "unit": "indice",
        "metadata": {"source": "test"},
        "raw_payload": {"nested": {"value": 1}},
    }


def test_prepare_upsert_rows_filters_non_serializable_or_extra_values():
    raw_object = object()
    items = [
        {
            "operation_code": 22,
            "table_id": 2852,
            "variable_id": None,
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
        "geography_name",
        "geography_code",
        "period",
        "value",
        "unit",
        "metadata",
        "raw_payload",
    }
    assert rows[0]["operation_code"] == "22"
    assert rows[0]["table_id"] == "2852"
    assert rows[0]["variable_id"] == ""
    assert rows[0]["value"] == 101.5
    assert rows[0]["metadata"] == {"note": str(raw_object)}
    assert rows[0]["raw_payload"] == {"items": [1, 2, 3]}
