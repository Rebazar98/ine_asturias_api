"""Test that repeated SADEI ingestion of the same data is idempotent.

Uses the in-memory SeriesRepository (session=None) to verify normalization
round-trips without touching a real database. For the full upsert idempotency
(ON CONFLICT DO NOTHING / DO UPDATE), the repository integration tests cover
that at the SQL level; here we verify that the prepare_upsert_rows helper
produces stable, deduplicated rows from identical input.
"""

from __future__ import annotations

from app.repositories.series import SeriesRepository
from app.services.sadei_normalizers import normalize_sadei_dataset


_ROWS = [
    {"codigo_municipio": "33001", "anyo": "2022", "valor": 10500.0},
    {"codigo_municipio": "33002", "anyo": "2022", "valor": 8200.0},
    {"codigo_municipio": "33003", "anyo": "2022", "valor": 5100.0},
]


def test_prepare_rows_stable_across_two_ingestions():
    items_first = normalize_sadei_dataset(_ROWS, "padron_municipal")
    items_second = normalize_sadei_dataset(_ROWS, "padron_municipal")

    rows_first = SeriesRepository.prepare_upsert_rows(items_first)
    rows_second = SeriesRepository.prepare_upsert_rows(items_second)

    assert len(rows_first) == 3
    assert rows_first == rows_second


def test_source_provider_is_sadei_in_prepared_rows():
    items = normalize_sadei_dataset(_ROWS, "padron_municipal")
    rows = SeriesRepository.prepare_upsert_rows(items)
    assert all(row["source_provider"] == "sadei" for row in rows)


def test_ine_rows_default_to_ine_source_provider():
    from app.schemas import NormalizedSeriesItem

    item = NormalizedSeriesItem(
        operation_code="22",
        table_id="IPC123",
        variable_id="v1",
        geography_code="33001",
        geography_name="Oviedo",
        period="2022",
        value=1.5,
        unit="indice",
        metadata_json={},
        raw_payload={},
        # source_provider defaults to "ine"
    )
    rows = SeriesRepository.prepare_upsert_rows([item])
    assert rows[0]["source_provider"] == "ine"


def test_sadei_and_ine_rows_are_distinct_by_operation_code():
    sadei_items = normalize_sadei_dataset(
        [{"codigo_municipio": "33001", "anyo": "2022", "valor": 10500.0}],
        "padron_municipal",
    )

    from app.schemas import NormalizedSeriesItem

    ine_item = NormalizedSeriesItem(
        operation_code="22",
        table_id="IPC123",
        variable_id="v1",
        geography_code="33001",
        geography_name="Oviedo",
        period="2022",
        value=1.5,
        unit="indice",
        metadata_json={},
        raw_payload={},
    )

    sadei_rows = SeriesRepository.prepare_upsert_rows(sadei_items)
    ine_rows = SeriesRepository.prepare_upsert_rows([ine_item])

    # Different operation_code → different logical key → no conflict
    assert sadei_rows[0]["operation_code"] != ine_rows[0]["operation_code"]
    assert sadei_rows[0]["source_provider"] == "sadei"
    assert ine_rows[0]["source_provider"] == "ine"
