"""Tests unitarios para TableCatalogRepository con sesión mockeada."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.exc import SQLAlchemyError

from app.repositories.catalog import TableCatalogRepository


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_session():
    session = AsyncMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


def _make_row(**kwargs):
    row = MagicMock()
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row


# ---------------------------------------------------------------------------
# upsert_discovered_tables
# ---------------------------------------------------------------------------


def test_upsert_discovered_tables_no_session_returns_zero():
    repo = TableCatalogRepository(session=None)
    result = asyncio.run(
        repo.upsert_discovered_tables("22", [{"table_id": "501"}], "TABLAS/22")
    )
    assert result == 0


def test_upsert_discovered_tables_empty_tables_returns_zero():
    repo = TableCatalogRepository(session=_make_session())
    result = asyncio.run(repo.upsert_discovered_tables("22", [], "TABLAS/22"))
    assert result == 0


def test_upsert_discovered_tables_all_missing_table_id_returns_zero():
    repo = TableCatalogRepository(session=_make_session())
    result = asyncio.run(
        repo.upsert_discovered_tables("22", [{"table_name": "Sin ID"}], "TABLAS/22")
    )
    assert result == 0


def test_upsert_discovered_tables_happy_path():
    session = _make_session()
    repo = TableCatalogRepository(session=session)
    tables = [
        {"table_id": "501", "table_name": "Tabla A", "metadata": {"key": "val"}},
        {"table_id": "502", "table_name": "Tabla B", "metadata": {}},
    ]

    result = asyncio.run(repo.upsert_discovered_tables("22", tables, "TABLAS/22"))

    assert result == 2
    session.execute.assert_awaited_once()
    session.commit.assert_awaited_once()


def test_upsert_discovered_tables_sqlalchemy_error_returns_zero():
    session = _make_session()
    session.execute.side_effect = SQLAlchemyError("DB error")
    repo = TableCatalogRepository(session=session)

    result = asyncio.run(
        repo.upsert_discovered_tables("22", [{"table_id": "501"}], "TABLAS/22")
    )

    assert result == 0
    session.rollback.assert_awaited_once()


def test_upsert_discovered_tables_with_resolution_context():
    session = _make_session()
    repo = TableCatalogRepository(session=session)
    tables = [{"table_id": "501", "table_name": "T"}]
    ctx = {"geo_variable_id": "115", "asturias_value_id": "33"}

    result = asyncio.run(repo.upsert_discovered_tables("22", tables, "TABLAS/22", ctx))

    assert result == 1
    session.commit.assert_awaited_once()


# ---------------------------------------------------------------------------
# update_table_status
# ---------------------------------------------------------------------------


def test_update_table_status_no_session_returns_false():
    repo = TableCatalogRepository(session=None)
    result = asyncio.run(
        repo.update_table_status(
            operation_code="22",
            table_id="501",
            table_name="T",
            request_path="DATOS_TABLA/501",
        )
    )
    assert result is False


def test_update_table_status_happy_path():
    session = _make_session()
    repo = TableCatalogRepository(session=session)

    result = asyncio.run(
        repo.update_table_status(
            operation_code="22",
            table_id="501",
            table_name="Tabla A",
            request_path="DATOS_TABLA/501",
            has_asturias_data=True,
            validation_status="has_data",
            normalized_rows=10,
        )
    )

    assert result is True
    session.execute.assert_awaited_once()
    session.commit.assert_awaited_once()


def test_update_table_status_sqlalchemy_error_returns_false():
    session = _make_session()
    session.execute.side_effect = SQLAlchemyError("DB error")
    repo = TableCatalogRepository(session=session)

    result = asyncio.run(
        repo.update_table_status(
            operation_code="22",
            table_id="501",
            table_name="T",
            request_path="DATOS_TABLA/501",
        )
    )

    assert result is False
    session.rollback.assert_awaited_once()


# ---------------------------------------------------------------------------
# list_by_operation
# ---------------------------------------------------------------------------


def test_list_by_operation_no_session_returns_empty():
    repo = TableCatalogRepository(session=None)
    result = asyncio.run(repo.list_by_operation("22"))
    assert result == []


def test_list_by_operation_returns_serialized_rows():
    session = _make_session()
    repo = TableCatalogRepository(session=session)
    row = _make_row(
        id=1,
        operation_code="22",
        table_id="501",
        table_name="Tabla A",
        request_path="TABLAS/22",
        resolution_context={},
        has_asturias_data=True,
        validation_status="has_data",
        normalized_rows=5,
        raw_rows_retrieved=10,
        filtered_rows_retrieved=5,
        series_kept=1,
        series_discarded=0,
        last_checked_at=None,
        first_seen_at=None,
        updated_at=None,
        metadata_json={},
        notes="",
        last_warning="",
    )
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [row]
    session.execute.return_value = mock_result

    result = asyncio.run(repo.list_by_operation("22"))

    assert len(result) == 1
    assert result[0]["table_id"] == "501"
    assert result[0]["validation_status"] == "has_data"


# ---------------------------------------------------------------------------
# get_operation_summary
# ---------------------------------------------------------------------------


def test_get_operation_summary_aggregates_statuses():
    """get_operation_summary cuenta correctamente los estados."""
    repo = TableCatalogRepository(session=None)
    # list_by_operation returns [] when session is None → all zeros
    result = asyncio.run(repo.get_operation_summary("22"))

    assert result == {
        "operation_code": "22",
        "total_tables": 0,
        "has_data": 0,
        "no_data": 0,
        "failed": 0,
        "unknown": 0,
    }


# ---------------------------------------------------------------------------
# get_known_no_data_table_ids
# ---------------------------------------------------------------------------


def test_get_known_no_data_table_ids_no_session_returns_empty_set():
    repo = TableCatalogRepository(session=None)
    result = asyncio.run(repo.get_known_no_data_table_ids("22"))
    assert result == set()


def test_get_known_no_data_table_ids_happy_path():
    session = _make_session()
    repo = TableCatalogRepository(session=session)
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = ["501", "502"]
    session.execute.return_value = mock_result

    result = asyncio.run(repo.get_known_no_data_table_ids("22"))

    assert result == {"501", "502"}
