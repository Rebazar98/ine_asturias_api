"""Tests for INEOperationIngestionService standalone normalization methods.

Covers normalize_and_store_table, normalize_and_store_asturias, and the
private _prepare_*_normalized_items helpers including exception and empty paths.
Also covers static helpers, progress_reporter paths, and large-table warnings.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.repositories.series import SeriesRepository
from app.services.asturias_resolver import AsturiasResolutionError
from app.services.ine_operation_ingestion import (
    INEOperationIngestionService,
    LARGE_TABLE_WARNING_THRESHOLD,
)
from app.services.normalizers import NormalizationOutcome
from tests.conftest import (
    DummyIngestionRepository,
    DummySeriesRepository,
    DummyTableCatalogRepository,
)


def _make_service() -> INEOperationIngestionService:
    return INEOperationIngestionService(
        ingestion_repo=DummyIngestionRepository(),
        series_repo=DummySeriesRepository(),
        catalog_repo=DummyTableCatalogRepository(),
    )


# ---------------------------------------------------------------------------
# normalize_and_store_table
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_normalize_and_store_table_stores_items_and_returns_count() -> None:
    service = _make_service()
    payload = [
        {
            "Nombre": "Serie 1",
            "MetaData": [],
            "Data": [{"Periodo": "2022", "Valor": "100"}],
        }
    ]
    count = await service.normalize_and_store_table(payload, table_id="2852")
    assert count > 0
    assert len(service.series_repo.items) == count


@pytest.mark.anyio
async def test_normalize_and_store_table_returns_zero_on_empty_payload() -> None:
    service = _make_service()
    count = await service.normalize_and_store_table([], table_id="2852")
    assert count == 0
    assert service.series_repo.items == []


# ---------------------------------------------------------------------------
# normalize_and_store_asturias
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_normalize_and_store_asturias_stores_items_and_returns_count() -> None:
    service = _make_service()
    payload = [
        {
            "Nombre": "Asturias. Nacidos vivos.",
            "MetaData": [{"Variable": "CCAA", "Nombre": "Principado de Asturias", "Id": "33"}],
            "Data": [{"Periodo": "2022", "Valor": "4521"}],
        }
    ]
    count = await service.normalize_and_store_asturias(
        payload,
        op_code="33",
        geography_name="Principado de Asturias",
        geography_code="33",
        table_id="2852",
    )
    assert count > 0


@pytest.mark.anyio
async def test_prepare_asturias_normalized_items_canonicalizes_aliases_before_upsert() -> None:
    service = _make_service()
    payload_alias = [
        {
            "Nombre": "Asturias. Nacidos vivos.",
            "MetaData": [{"Variable": "CCAA", "Nombre": "Asturias, Principado de", "Id": "8999"}],
            "Data": [{"Periodo": "2022", "Valor": "4521"}],
        }
    ]
    payload_canonical = [
        {
            "Nombre": "Asturias. Nacidos vivos.",
            "MetaData": [{"Variable": "CCAA", "Nombre": "Principado de Asturias", "Id": "33"}],
            "Data": [{"Periodo": "2022", "Valor": "4521"}],
        }
    ]

    items_alias = await service._prepare_asturias_normalized_items(
        payload=payload_alias,
        op_code="33",
        geography_name="Asturias, Principado de",
        geography_code="8999",
        table_id="2852",
    )
    items_canonical = await service._prepare_asturias_normalized_items(
        payload=payload_canonical,
        op_code="33",
        geography_name="Principado de Asturias",
        geography_code="33",
        table_id="2852",
    )

    rows = SeriesRepository.prepare_upsert_rows([*items_alias, *items_canonical])
    conflict_keys = {
        (
            row["operation_code"],
            row["table_id"],
            row["variable_id"],
            row["geography_name"],
            row["geography_code"],
            row["period"],
        )
        for row in rows
    }

    assert len(conflict_keys) == 1
    assert {row["geography_name"] for row in rows} == {"Principado de Asturias"}
    assert {row["geography_code"] for row in rows} == {"33"}


@pytest.mark.anyio
async def test_normalize_and_store_asturias_returns_zero_on_empty_payload() -> None:
    service = _make_service()
    count = await service.normalize_and_store_asturias(
        [],
        op_code="33",
        geography_name="Principado de Asturias",
        geography_code="33",
        table_id="2852",
    )
    assert count == 0
    assert service.series_repo.items == []


# ---------------------------------------------------------------------------
# _prepare_table_normalized_items — exception and empty paths
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_prepare_table_normalized_items_returns_empty_on_normalization_exception() -> None:
    """When normalize_table_payload_with_stats raises, the method returns [] instead of propagating."""
    service = _make_service()
    with patch(
        "app.services.ine_operation_ingestion.normalize_table_payload_with_stats",
        side_effect=ValueError("malformed payload"),
    ):
        items = await service._prepare_table_normalized_items(payload=[], table_id="bad")
    assert items == []


@pytest.mark.anyio
async def test_prepare_table_normalized_items_returns_empty_when_outcome_has_no_items() -> None:
    """When the normalizer succeeds but produces no items, the method returns []."""
    service = _make_service()
    with patch(
        "app.services.ine_operation_ingestion.normalize_table_payload_with_stats",
        return_value=NormalizationOutcome(),
    ):
        items = await service._prepare_table_normalized_items(payload=[], table_id="2852")
    assert items == []


# ---------------------------------------------------------------------------
# _prepare_asturias_normalized_items — exception and empty paths
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_prepare_asturias_normalized_items_returns_empty_on_exception() -> None:
    """When normalize_asturias_payload_with_stats raises, the method returns []."""
    service = _make_service()
    with patch(
        "app.services.ine_operation_ingestion.normalize_asturias_payload_with_stats",
        side_effect=RuntimeError("unexpected"),
    ):
        items = await service._prepare_asturias_normalized_items(
            payload=[],
            op_code="33",
            geography_name="Principado de Asturias",
            geography_code="33",
            table_id="2852",
        )
    assert items == []


@pytest.mark.anyio
async def test_prepare_asturias_normalized_items_returns_empty_when_outcome_has_no_items() -> None:
    service = _make_service()
    with patch(
        "app.services.ine_operation_ingestion.normalize_asturias_payload_with_stats",
        return_value=NormalizationOutcome(),
    ):
        items = await service._prepare_asturias_normalized_items(
            payload=[],
            op_code="33",
            geography_name="Principado de Asturias",
            geography_code="33",
            table_id="2852",
        )
    assert items == []


# ---------------------------------------------------------------------------
# Helpers: _make_ine_client, _make_resolution
# ---------------------------------------------------------------------------


def _make_ine_client(**overrides) -> MagicMock:
    client = MagicMock()
    client.get_operation_tables = AsyncMock(return_value=[])
    client.get_table = AsyncMock(return_value=[])
    client.get_operation_series = AsyncMock(return_value=[])
    client.get_serie_data = AsyncMock(return_value=[])
    for attr, value in overrides.items():
        setattr(client, attr, value)
    return client


def _make_resolution(**overrides) -> MagicMock:
    res = MagicMock()
    res.geo_variable_id = "3"
    res.asturias_value_id = "33"
    res.asturias_label = "Principado de Asturias"
    res.variable_name = "Comunidades y Ciudades Autónomas"
    for attr, value in overrides.items():
        setattr(res, attr, value)
    return res


def _one_table_payload(table_id: str = "T1", name: str = "Tabla test") -> list:
    return [{"IdTabla": table_id, "Nombre": name}]


def _asturias_data_rows(n: int = 1) -> list:
    return [
        {
            "Nombre": f"Asturias. Serie {i}",
            "MetaData": [{"Id": "33", "Nombre": "Principado de Asturias"}],
            "Data": [{"Periodo": "2022", "Valor": str(i * 100)}],
        }
        for i in range(1, n + 1)
    ]


# ---------------------------------------------------------------------------
# _extract_table_candidates
# ---------------------------------------------------------------------------


class TestExtractTableCandidates:
    def test_duplicate_table_id_is_skipped(self):
        payload = [
            {"IdTabla": "T1", "Nombre": "Primera"},
            {"IdTabla": "T1", "Nombre": "Duplicada"},  # mismo ID → omitida
            {"IdTabla": "T2", "Nombre": "Segunda"},
        ]
        result = INEOperationIngestionService._extract_table_candidates(payload)
        assert len(result) == 2
        assert result[0]["table_id"] == "T1"
        assert result[1]["table_id"] == "T2"

    def test_empty_payload_returns_empty(self):
        assert INEOperationIngestionService._extract_table_candidates([]) == []

    def test_record_without_id_is_skipped(self):
        payload = [{"Nombre": "Sin id"}, {"IdTabla": "T1", "Nombre": "Con id"}]
        result = INEOperationIngestionService._extract_table_candidates(payload)
        assert len(result) == 1
        assert result[0]["table_id"] == "T1"


# ---------------------------------------------------------------------------
# _extract_records
# ---------------------------------------------------------------------------


class TestExtractRecords:
    def test_non_dict_non_list_returns_empty(self):
        assert INEOperationIngestionService._extract_records("string") == []
        assert INEOperationIngestionService._extract_records(42) == []

    def test_dict_with_data_key_returns_items(self):
        payload = {"Data": [{"Periodo": "2022"}, {"Periodo": "2023"}]}
        result = INEOperationIngestionService._extract_records(payload)
        assert len(result) == 2

    def test_dict_with_tables_key_returns_items(self):
        payload = {"Tables": [{"Id": "T1"}, {"Id": "T2"}]}
        result = INEOperationIngestionService._extract_records(payload)
        assert len(result) == 2

    def test_dict_without_known_key_returns_payload_wrapped(self):
        payload = {"operacion": "22", "codigo": "INE"}
        result = INEOperationIngestionService._extract_records(payload)
        assert result == [payload]

    def test_list_filters_non_dicts(self):
        payload = [{"Id": "1"}, "cadena", 42, {"Id": "2"}]
        result = INEOperationIngestionService._extract_records(payload)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# _count_retrieved_rows
# ---------------------------------------------------------------------------


class TestCountRetrievedRows:
    def test_series_without_data_key_counts_as_one_each(self):
        payload = [{"Nombre": "Sin datos"}, {"Nombre": "También sin datos"}]
        assert INEOperationIngestionService._count_retrieved_rows(payload) == 2

    def test_series_with_data_key_counts_data_points(self):
        payload = [{"Data": [{"Periodo": "2022"}, {"Periodo": "2023"}]}]
        assert INEOperationIngestionService._count_retrieved_rows(payload) == 2

    def test_mixed_series(self):
        payload = [
            {"Data": [{"Periodo": "2022"}]},  # 1 punto
            {"Nombre": "Sin Data"},  # cuenta como 1
        ]
        assert INEOperationIngestionService._count_retrieved_rows(payload) == 2


# ---------------------------------------------------------------------------
# _ensure_list
# ---------------------------------------------------------------------------


class TestEnsureList:
    def test_scalar_wrapped_in_list(self):
        assert INEOperationIngestionService._ensure_list("hola") == ["hola"]
        assert INEOperationIngestionService._ensure_list(42) == [42]

    def test_none_returns_empty(self):
        assert INEOperationIngestionService._ensure_list(None) == []

    def test_list_returned_as_is(self):
        lst = [1, 2, 3]
        assert INEOperationIngestionService._ensure_list(lst) is lst


# ---------------------------------------------------------------------------
# _summarize_error
# ---------------------------------------------------------------------------


class TestSummarizeError:
    def test_dict_with_message_key(self):
        result = INEOperationIngestionService._summarize_error({"message": "Not found"})
        assert result == "Not found"

    def test_dict_with_error_key_fallback(self):
        result = INEOperationIngestionService._summarize_error({"error": "timeout"})
        assert result == "timeout"

    def test_non_dict_returns_str_representation(self):
        assert INEOperationIngestionService._summarize_error("plain error") == "plain error"
        assert INEOperationIngestionService._summarize_error(503) == "503"


# ---------------------------------------------------------------------------
# _series_matches_asturias
# ---------------------------------------------------------------------------


class TestSeriesMatchesAsturias:
    def test_metadata_id_match(self):
        series = {
            "Nombre": "Alguna serie",
            "MetaData": [{"Id": "33", "Nombre": "Madrid"}],
        }
        assert INEOperationIngestionService._series_matches_asturias(
            series, "33", "Principado de Asturias"
        )

    def test_metadata_name_match(self):
        series = {
            "Nombre": "Alguna serie",
            "MetaData": [{"Id": "99", "Nombre": "Principado de Asturias"}],
        }
        assert INEOperationIngestionService._series_matches_asturias(
            series, None, "Principado de Asturias"
        )

    def test_top_level_nombre_match_when_no_metadata_match(self):
        """Line 928: top-level Nombre fallback when MetaData doesn't match."""
        series = {
            "Nombre": "Asturias. Nacidos vivos por municipio",
            "MetaData": [{"Id": "99", "Nombre": "Indicador economico"}],
        }
        assert INEOperationIngestionService._series_matches_asturias(
            series, None, "Principado de Asturias"
        )

    def test_no_match_returns_false(self):
        series = {
            "Nombre": "Madrid. Datos",
            "MetaData": [{"Id": "28", "Nombre": "Madrid"}],
        }
        assert not INEOperationIngestionService._series_matches_asturias(
            series, "33", "Principado de Asturias"
        )


# ---------------------------------------------------------------------------
# _filter_payload_for_asturias
# ---------------------------------------------------------------------------


class TestFilterPayloadForAsturias:
    def test_dict_payload_returns_dict_with_filtered_data_key(self):
        """Line 888: when payload is dict, result is {**payload, 'Data': kept}."""
        payload = {
            "operacion": "22",
            "Data": [
                {
                    "Nombre": "Asturias",
                    "MetaData": [{"Id": "33", "Nombre": "Principado de Asturias"}],
                    "Data": [{"Periodo": "2022", "Valor": "100"}],
                },
                {
                    "Nombre": "Madrid",
                    "MetaData": [{"Id": "28", "Nombre": "Madrid"}],
                    "Data": [{"Periodo": "2022", "Valor": "200"}],
                },
            ],
        }
        filtered, stats = INEOperationIngestionService._filter_payload_for_asturias(
            payload, "33", "Principado de Asturias"
        )
        assert isinstance(filtered, dict)
        assert filtered["operacion"] == "22"
        assert len(filtered["Data"]) == 1
        assert stats["series_kept"] == 1
        assert stats["series_discarded"] == 1

    def test_list_payload_returns_list(self):
        payload = [
            {
                "Nombre": "Principado de Asturias",
                "MetaData": [{"Id": "33", "Nombre": "Principado de Asturias"}],
                "Data": [{"Periodo": "2022", "Valor": "100"}],
            }
        ]
        filtered, stats = INEOperationIngestionService._filter_payload_for_asturias(
            payload, "33", "Principado de Asturias"
        )
        assert isinstance(filtered, list)
        assert stats["series_kept"] == 1


# ---------------------------------------------------------------------------
# ingest_asturias_operation — edge cases with progress_reporter
# ---------------------------------------------------------------------------


class TestIngestAsturiasOperationEdgeCases:
    @pytest.mark.anyio
    async def test_raises_when_resolution_is_none_and_tables_found(self):
        """Line 397: resolution=None after tables are discovered → raises 422."""
        service = _make_service()
        ine_client = _make_ine_client(
            get_operation_tables=AsyncMock(return_value=_one_table_payload())
        )

        with pytest.raises(AsturiasResolutionError) as exc_info:
            await service.ingest_asturias_operation(
                op_code="22",
                resolution=None,
                nult=None,
                det=None,
                tip=None,
                periodicidad=None,
                max_tables=None,
                skip_known_no_data=False,
                ine_client=ine_client,
                max_concurrent_table_fetches=1,
            )
        assert exc_info.value.status_code == 422

    @pytest.mark.anyio
    async def test_progress_reporter_called_with_series_fallback_activated(self):
        """Line 380: no tables → series fallback → progress_reporter stage reported."""
        service = _make_service()
        ine_client = _make_ine_client(
            get_operation_tables=AsyncMock(return_value=[]),
            get_operation_series=AsyncMock(return_value=[]),  # vacío → raises 404
        )
        progress_reporter = AsyncMock()

        with pytest.raises(AsturiasResolutionError):
            await service.ingest_asturias_operation(
                op_code="22",
                resolution=_make_resolution(),
                nult=None,
                det=None,
                tip=None,
                periodicidad=None,
                max_tables=None,
                skip_known_no_data=False,
                ine_client=ine_client,
                max_concurrent_table_fetches=1,
                progress_reporter=progress_reporter,
            )

        stages = [call.args[0]["stage"] for call in progress_reporter.call_args_list]
        assert "series_fallback_activated" in stages

    @pytest.mark.anyio
    async def test_progress_reporter_called_with_table_failed(self):
        """Line 516: table fetch raises → progress_reporter reports table_failed."""
        service = _make_service()
        ine_client = _make_ine_client(
            get_operation_tables=AsyncMock(return_value=_one_table_payload()),
            get_table=AsyncMock(side_effect=RuntimeError("INE upstream error")),
        )
        progress_reporter = AsyncMock()

        with pytest.raises(AsturiasResolutionError):
            await service.ingest_asturias_operation(
                op_code="22",
                resolution=_make_resolution(),
                nult=None,
                det=None,
                tip=None,
                periodicidad=None,
                max_tables=None,
                skip_known_no_data=False,
                ine_client=ine_client,
                max_concurrent_table_fetches=1,
                progress_reporter=progress_reporter,
            )

        stages = [call.args[0]["stage"] for call in progress_reporter.call_args_list]
        assert "table_failed" in stages

    @pytest.mark.anyio
    async def test_progress_reporter_called_with_table_filtered_empty(self):
        """Line 573: table fetched but no Asturias rows → progress_reporter table_filtered_empty."""
        service = _make_service()
        ine_client = _make_ine_client(
            get_operation_tables=AsyncMock(return_value=_one_table_payload()),
            get_table=AsyncMock(
                return_value=[
                    {
                        "Nombre": "Madrid. Datos",
                        "MetaData": [{"Id": "28", "Nombre": "Madrid"}],
                        "Data": [{"Periodo": "2022", "Valor": "999"}],
                    }
                ]
            ),
        )
        progress_reporter = AsyncMock()

        with pytest.raises(AsturiasResolutionError):
            await service.ingest_asturias_operation(
                op_code="22",
                resolution=_make_resolution(),
                nult=None,
                det=None,
                tip=None,
                periodicidad=None,
                max_tables=None,
                skip_known_no_data=False,
                ine_client=ine_client,
                max_concurrent_table_fetches=1,
                progress_reporter=progress_reporter,
            )

        stages = [call.args[0]["stage"] for call in progress_reporter.call_args_list]
        assert "table_filtered_empty" in stages


# ---------------------------------------------------------------------------
# _prepare_asturias_table — large_table_detected
# ---------------------------------------------------------------------------


class TestPrepareAsturiasTableLargeTable:
    @pytest.mark.anyio
    async def test_large_table_sets_large_warning(self):
        """Lines 786-793: raw_rows > LARGE_TABLE_WARNING_THRESHOLD → large_warning populated."""
        service = _make_service()
        semaphore = asyncio.Semaphore(1)

        # All 50001 entries share the same dict object — lightweight in memory
        big_data = [{"Periodo": "x"}] * (LARGE_TABLE_WARNING_THRESHOLD + 1)
        big_payload = [
            {
                "Nombre": "Principado de Asturias",
                "MetaData": [{"Id": "33", "Nombre": "Principado de Asturias"}],
                "Data": big_data,
            }
        ]
        ine_client = _make_ine_client(get_table=AsyncMock(return_value=big_payload))
        table = {"table_id": "T1", "table_name": "Gran tabla", "metadata": {}}

        prepared = await service._prepare_asturias_table(
            semaphore=semaphore,
            op_code="22",
            resolution=_make_resolution(),
            table=table,
            table_index=1,
            tables_total=1,
            table_params={},
            ine_client=ine_client,
            progress_reporter=None,
        )

        assert prepared.large_warning is not None
        assert prepared.large_warning["warning"] == "large_table_detected"
        assert prepared.raw_rows_retrieved > LARGE_TABLE_WARNING_THRESHOLD

    @pytest.mark.anyio
    async def test_normal_table_has_no_large_warning(self):
        """Table under threshold does not produce large_warning."""
        service = _make_service()
        semaphore = asyncio.Semaphore(1)
        ine_client = _make_ine_client(get_table=AsyncMock(return_value=_asturias_data_rows(3)))
        table = {"table_id": "T1", "table_name": "Tabla pequeña", "metadata": {}}

        prepared = await service._prepare_asturias_table(
            semaphore=semaphore,
            op_code="22",
            resolution=_make_resolution(),
            table=table,
            table_index=1,
            tables_total=1,
            table_params={},
            ine_client=ine_client,
            progress_reporter=None,
        )

        assert prepared.large_warning is None


# ---------------------------------------------------------------------------
# Líneas residuales: 256, 539, 913
# ---------------------------------------------------------------------------


class TestResidualLines:
    @pytest.mark.anyio
    async def test_series_without_id_and_cod_is_skipped_line256(self):
        """Line 256: serie entry with no Id and no COD is silently skipped."""
        service = _make_service()
        ine_client = _make_ine_client(
            # Primera página devuelve 1 serie sin Id/COD, la segunda vacía (fin paginación)
            get_operation_series=AsyncMock(
                side_effect=[
                    [{}],  # page 1: entry sin Id ni COD
                    [],  # page 2: vacío → fin de paginación
                ]
            ),
        )
        with pytest.raises(AsturiasResolutionError) as exc_info:
            await service.ingest_asturias_operation_via_series(
                op_code="22",
                nult=None,
                max_series=None,
                max_concurrent_series_fetches=1,
                progress_reporter=None,
                ine_client=ine_client,
            )
        # Se procesó la entrada inválida (no explotar) pero no hay datos → 404
        assert exc_info.value.status_code == 404

    @pytest.mark.anyio
    async def test_large_warning_appended_in_main_flow_line539(self):
        """Line 539: large table with Asturias rows → warnings list gets large_warning appended."""
        service = _make_service()
        # Tabla con > 50000 filas Y con filas de Asturias → pasa por el bloque 538-539
        big_data = [{"Periodo": "x"}] * (LARGE_TABLE_WARNING_THRESHOLD + 1)
        big_payload = [
            {
                "Nombre": "Principado de Asturias",
                "MetaData": [{"Id": "33", "Nombre": "Principado de Asturias"}],
                "Data": big_data,
            }
        ]
        ine_client = _make_ine_client(
            get_operation_tables=AsyncMock(return_value=_one_table_payload()),
            get_table=AsyncMock(return_value=big_payload),
        )

        result = await service.ingest_asturias_operation(
            op_code="22",
            resolution=_make_resolution(),
            nult=None,
            det=None,
            tip=None,
            periodicidad=None,
            max_tables=None,
            skip_known_no_data=False,
            ine_client=ine_client,
            max_concurrent_table_fetches=1,
        )

        assert any(w.get("warning") == "large_table_detected" for w in result.get("warnings", []))

    def test_non_dict_metadata_item_is_skipped_line913(self):
        """Line 913: MetaData list entry that is not a dict → continue, then match by name."""
        series = {
            "Nombre": "Principado de Asturias",
            "MetaData": [
                "no_es_un_dict",  # triggers line 913 continue
                {"Id": "33", "Nombre": "Principado de Asturias"},
            ],
        }
        assert INEOperationIngestionService._series_matches_asturias(
            series, "33", "Principado de Asturias"
        )


# ---------------------------------------------------------------------------
# ingest_asturias_operation_via_series — lines 219-220, 237, 257-278, 285, 333
# ---------------------------------------------------------------------------


class TestIngestAsturiasOperationViaSeriesCoverage:
    @pytest.mark.anyio
    async def test_max_series_truncates_index(self):
        """Lines 219-220: series_index is trimmed to max_series and loop breaks."""
        service = _make_service()
        ine_client = _make_ine_client(
            get_operation_series=AsyncMock(
                side_effect=[
                    [{"Id": 1, "COD": "S001"}, {"Id": 2, "COD": "S002"}, {"Id": 3, "COD": "S003"}],
                    [],
                ]
            ),
            get_serie_data=AsyncMock(return_value=[]),
        )
        with patch(
            "app.services.ine_operation_ingestion.normalize_serie_direct_payload_with_stats",
            return_value=NormalizationOutcome(),
        ):
            with pytest.raises(AsturiasResolutionError) as exc_info:
                await service.ingest_asturias_operation_via_series(
                    op_code="22",
                    nult=None,
                    max_series=2,
                    max_concurrent_series_fetches=1,
                    progress_reporter=None,
                    ine_client=ine_client,
                )
        assert exc_info.value.status_code == 404
        # Page 2 was never requested — loop broke at max_series
        assert ine_client.get_operation_series.call_count == 1

    @pytest.mark.anyio
    async def test_serie_fetch_error_is_recorded_and_does_not_propagate(self):
        """Lines 260-264: get_serie_data raises → error appended, result returned with series_failed>0.

        The method only raises when normalized_rows==0 AND errors is empty.
        When errors is non-empty (fetch failed) it returns the aggregated payload.
        """
        service = _make_service()
        ine_client = _make_ine_client(
            get_operation_series=AsyncMock(
                side_effect=[
                    [{"Id": 10, "COD": "S010"}],
                    [],
                ]
            ),
            get_serie_data=AsyncMock(side_effect=RuntimeError("upstream timeout")),
        )
        result = await service.ingest_asturias_operation_via_series(
            op_code="22",
            nult=None,
            max_series=None,
            max_concurrent_series_fetches=1,
            progress_reporter=None,
            ine_client=ine_client,
        )
        # Error swallowed → returned in payload, not raised
        assert isinstance(result, dict)
        assert result["series_failed"] == 1
        assert result["normalized_rows"] == 0

    @pytest.mark.anyio
    async def test_successful_series_ingestion_returns_payload_and_calls_reporter(self):
        """Lines 237, 257-278, 285, 333: full happy path via series."""
        from app.schemas import NormalizedSeriesItem

        dummy_item = NormalizedSeriesItem(period="2022", value=100.0)
        outcome_with_item = NormalizationOutcome(items=[dummy_item])

        service = _make_service()
        ine_client = _make_ine_client(
            get_operation_series=AsyncMock(
                side_effect=[
                    [{"Id": 10, "COD": "S010"}],
                    [],
                ]
            ),
            get_serie_data=AsyncMock(return_value=[{"Periodo": "2022", "Valor": "100"}]),
        )
        progress_reporter = AsyncMock()

        with patch(
            "app.services.ine_operation_ingestion.normalize_serie_direct_payload_with_stats",
            return_value=outcome_with_item,
        ):
            result = await service.ingest_asturias_operation_via_series(
                op_code="22",
                nult=None,
                max_series=None,
                max_concurrent_series_fetches=1,
                progress_reporter=progress_reporter,
                ine_client=ine_client,
            )

        assert isinstance(result, dict)
        assert result.get("ingestion_mode") == "series_direct"
        stages = [call.args[0]["stage"] for call in progress_reporter.call_args_list]
        assert "series_index_ready" in stages
        assert len(service.series_repo.items) == 1


# ---------------------------------------------------------------------------
# ingest_asturias_operation — skip_known_no_data (421-429), max_tables (438),
# progress_reporter table_completed (620)
# ---------------------------------------------------------------------------


class TestIngestAsturiasOperationSkipAndMaxTables:
    @pytest.mark.anyio
    async def test_skip_known_no_data_removes_flagged_table(self):
        """Lines 421-429: tables marked no_data in catalog are excluded."""
        from tests.conftest import DummyTableCatalogRepository

        catalog_repo = DummyTableCatalogRepository()
        await catalog_repo.update_table_status(
            operation_code="22",
            table_id="T1",
            table_name="Known empty",
            request_path="TABLAS_OPERACION/22",
            has_asturias_data=False,
            validation_status="no_data",
        )

        service = INEOperationIngestionService(
            ingestion_repo=DummyIngestionRepository(),
            series_repo=DummySeriesRepository(),
            catalog_repo=catalog_repo,
        )

        ine_client = _make_ine_client(
            get_operation_tables=AsyncMock(
                return_value=[
                    {"IdTabla": "T1", "Nombre": "Tabla ya conocida sin datos"},
                    {"IdTabla": "T2", "Nombre": "Tabla con datos"},
                ]
            ),
            get_table=AsyncMock(return_value=_asturias_data_rows(2)),
        )

        result = await service.ingest_asturias_operation(
            op_code="22",
            resolution=_make_resolution(),
            nult=None,
            det=None,
            tip=None,
            periodicidad=None,
            max_tables=None,
            skip_known_no_data=True,
            skip_known_processed=False,
            ine_client=ine_client,
            max_concurrent_table_fetches=1,
        )

        assert isinstance(result, dict)
        fetch_calls = [str(call.args[0]) for call in ine_client.get_table.call_args_list]
        assert "T1" not in fetch_calls
        assert "T2" in fetch_calls

    @pytest.mark.anyio
    async def test_skip_known_processed_removes_has_data_and_no_data_tables(self):
        from tests.conftest import DummyTableCatalogRepository

        catalog_repo = DummyTableCatalogRepository()
        await catalog_repo.update_table_status(
            operation_code="22",
            table_id="T1",
            table_name="Known with data",
            request_path="DATOS_TABLA/T1",
            has_asturias_data=True,
            validation_status="has_data",
        )
        await catalog_repo.update_table_status(
            operation_code="22",
            table_id="T2",
            table_name="Known empty",
            request_path="DATOS_TABLA/T2",
            has_asturias_data=False,
            validation_status="no_data",
        )

        service = INEOperationIngestionService(
            ingestion_repo=DummyIngestionRepository(),
            series_repo=DummySeriesRepository(),
            catalog_repo=catalog_repo,
        )

        ine_client = _make_ine_client(
            get_operation_tables=AsyncMock(
                return_value=[
                    {"IdTabla": "T1", "Nombre": "Tabla ya conocida con datos"},
                    {"IdTabla": "T2", "Nombre": "Tabla ya conocida sin datos"},
                    {"IdTabla": "T3", "Nombre": "Tabla nueva"},
                ]
            ),
            get_table=AsyncMock(return_value=_asturias_data_rows(2)),
        )

        result = await service.ingest_asturias_operation(
            op_code="22",
            resolution=_make_resolution(),
            nult=None,
            det=None,
            tip=None,
            periodicidad=None,
            max_tables=None,
            skip_known_no_data=False,
            skip_known_processed=True,
            ine_client=ine_client,
            max_concurrent_table_fetches=1,
        )

        assert isinstance(result, dict)
        fetch_calls = [str(call.args[0]) for call in ine_client.get_table.call_args_list]
        assert "T1" not in fetch_calls
        assert "T2" not in fetch_calls
        assert "T3" in fetch_calls
        assert result["summary"]["tables_skipped_catalog"] == 2

    @pytest.mark.anyio
    async def test_max_tables_limits_candidates(self):
        """Line 438: max_tables trims the candidate list before fetching."""
        service = _make_service()
        ine_client = _make_ine_client(
            get_operation_tables=AsyncMock(
                return_value=[
                    {"IdTabla": "T1", "Nombre": "Primera"},
                    {"IdTabla": "T2", "Nombre": "Segunda"},
                    {"IdTabla": "T3", "Nombre": "Tercera"},
                ]
            ),
            get_table=AsyncMock(return_value=_asturias_data_rows(1)),
        )

        result = await service.ingest_asturias_operation(
            op_code="22",
            resolution=_make_resolution(),
            nult=None,
            det=None,
            tip=None,
            periodicidad=None,
            max_tables=1,
            skip_known_no_data=False,
            skip_known_processed=False,
            ine_client=ine_client,
            max_concurrent_table_fetches=1,
        )

        assert isinstance(result, dict)
        assert ine_client.get_table.call_count == 1

    @pytest.mark.anyio
    async def test_progress_reporter_receives_table_completed(self):
        """Line 620: progress_reporter is called with stage='table_completed'."""
        service = _make_service()
        ine_client = _make_ine_client(
            get_operation_tables=AsyncMock(return_value=_one_table_payload()),
            get_table=AsyncMock(return_value=_asturias_data_rows(2)),
        )
        progress_reporter = AsyncMock()

        await service.ingest_asturias_operation(
            op_code="22",
            resolution=_make_resolution(),
            nult=None,
            det=None,
            tip=None,
            periodicidad=None,
            max_tables=None,
            skip_known_no_data=False,
            skip_known_processed=False,
            ine_client=ine_client,
            max_concurrent_table_fetches=1,
            progress_reporter=progress_reporter,
        )

        stages = [call.args[0]["stage"] for call in progress_reporter.call_args_list]
        assert "table_completed" in stages
