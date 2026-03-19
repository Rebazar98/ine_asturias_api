import asyncio
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.exc import SQLAlchemyError

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


# ---------------------------------------------------------------------------
# upsert_many — con sesión mockeada
# ---------------------------------------------------------------------------


def _make_session():
    session = AsyncMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


def _sample_item(**overrides) -> NormalizedSeriesItem:
    defaults = dict(
        operation_code="22",
        table_id="501",
        variable_id="POP",
        geography_name="Asturias",
        geography_code="33",
        period="2024",
        value=1000.0,
        unit="personas",
    )
    defaults.update(overrides)
    return NormalizedSeriesItem(**defaults)


def test_upsert_many_empty_items_returns_zero():
    repo = SeriesRepository(session=_make_session())
    result = asyncio.run(repo.upsert_many([]))
    assert result == 0


def test_upsert_many_no_session_returns_zero():
    repo = SeriesRepository(session=None)
    result = asyncio.run(repo.upsert_many([_sample_item()]))
    assert result == 0


def test_upsert_many_happy_path_returns_count():
    session = _make_session()
    repo = SeriesRepository(session=session)
    items = [_sample_item(period="2023"), _sample_item(period="2024")]

    result = asyncio.run(repo.upsert_many(items))

    assert result == 2
    session.execute.assert_awaited_once()
    session.commit.assert_awaited_once()


def test_upsert_many_sqlalchemy_error_returns_zero():
    session = _make_session()
    session.execute.side_effect = SQLAlchemyError("DB error")
    repo = SeriesRepository(session=session)

    result = asyncio.run(repo.upsert_many([_sample_item()]))

    assert result == 0
    session.rollback.assert_awaited_once()


def test_upsert_many_small_batch_size_splits_correctly():
    """batch_size=1 con 2 items → 2 execute + 2 commit, retorna 2."""
    session = _make_session()
    repo = SeriesRepository(session=session)
    items = [_sample_item(period="2022"), _sample_item(period="2023")]

    result = asyncio.run(repo.upsert_many(items, batch_size=1))

    assert result == 2
    assert session.execute.await_count == 2
    assert session.commit.await_count == 2


def test_upsert_many_item_without_period_produces_empty_batch():
    """Item sin period → prepare_upsert_rows lo descarta → warning logged, returns 0."""
    session = _make_session()
    repo = SeriesRepository(session=session)
    # Dict sin 'period' → descartado por prepare_upsert_rows
    bad_item = {
        "operation_code": "22",
        "table_id": "501",
        "variable_id": "X",
        "geography_name": "Asturias",
        "geography_code": "33",
        "value": 1.0,
        "unit": "u",
    }

    result = asyncio.run(repo.upsert_many([bad_item]))

    assert result == 0
    session.execute.assert_not_awaited()
