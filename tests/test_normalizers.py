from app.services.normalizers import inspect_payload_shape, normalize_asturias_payload


REALISTIC_ASTURIAS_PAYLOAD = [
    {
        "COD": "DPOP15001",
        "Nombre": "Asturias. Total. Total habitantes. Personas.",
        "FK_Unidad": 3,
        "FK_Escala": 1,
        "Data": [
            {
                "Fecha": 1609455600000,
                "FK_TipoDato": 1,
                "FK_Periodo": 28,
                "Anyo": 2021,
                "Valor": 1011792,
                "Secreto": False,
            },
            {
                "Fecha": 1577833200000,
                "FK_TipoDato": 1,
                "FK_Periodo": 28,
                "Anyo": 2020,
                "Valor": 1018784,
                "Secreto": False,
            },
        ],
    }
]


def test_inspect_payload_shape_counts_root_series_and_observations():
    stats = inspect_payload_shape(REALISTIC_ASTURIAS_PAYLOAD)

    assert stats == {
        "payload_type": "list",
        "series_detected": 1,
        "observations_total": 2,
    }


def test_normalize_asturias_payload_flattens_root_list_series_into_rows():
    rows = normalize_asturias_payload(
        payload=REALISTIC_ASTURIAS_PAYLOAD,
        op_code="22",
        geography_name="Asturias",
        geography_code="8999",
        table_id="2852",
    )

    assert len(rows) == 2
    assert rows[0].operation_code == "22"
    assert rows[0].table_id == "2852"
    assert rows[0].variable_id == "DPOP15001"
    assert rows[0].geography_name == "Asturias"
    assert rows[0].geography_code == "8999"
    assert rows[0].period == "2021"
    assert rows[0].value == 1011792.0
    assert rows[1].period == "2020"
    assert rows[1].value == 1018784.0
    assert rows[0].metadata["series_name"] == "Asturias. Total. Total habitantes. Personas."
    assert rows[0].metadata["series_code"] == "DPOP15001"
    assert rows[0].raw_payload["point"]["Anyo"] == 2021

from app.services.normalizers import (
    normalize_table_payload,
    normalize_table_payload_with_stats,
    parse_numeric_value,
)


# ---------------------------------------------------------------------------
# parse_numeric_value
# ---------------------------------------------------------------------------


def test_parse_numeric_value_bool():
    assert parse_numeric_value(True) == 1.0
    assert parse_numeric_value(False) == 0.0


def test_parse_numeric_value_int_and_float():
    assert parse_numeric_value(42) == 42.0
    assert parse_numeric_value(3.14) == 3.14


def test_parse_numeric_value_none_returns_none():
    assert parse_numeric_value(None) is None


def test_parse_numeric_value_sentinel_strings_return_none():
    for sentinel in ("", "-", "..", "...", "null", "None"):
        assert parse_numeric_value(sentinel) is None, f"expected None for {sentinel!r}"


def test_parse_numeric_value_mixed_separators_comma_decimal():
    # "1.234,56" — comma is decimal separator
    assert parse_numeric_value("1.234,56") == 1234.56


def test_parse_numeric_value_mixed_separators_dot_decimal():
    # "1,234.56" — dot is decimal separator
    assert parse_numeric_value("1,234.56") == 1234.56


def test_parse_numeric_value_comma_only_decimal():
    assert parse_numeric_value("3,14") == 3.14


def test_parse_numeric_value_percentage_stripped():
    assert parse_numeric_value("45%") == 45.0


def test_parse_numeric_value_non_numeric_returns_none():
    assert parse_numeric_value("no-es-un-numero") is None
    assert parse_numeric_value("abc") is None


def test_parse_numeric_value_bare_dot_or_dash_returns_none():
    assert parse_numeric_value(".") is None
    assert parse_numeric_value("-.") is None


# ---------------------------------------------------------------------------
# normalize_table_payload — wrapper and core paths
# ---------------------------------------------------------------------------


def test_normalize_table_payload_wrapper_returns_list():
    """normalize_table_payload must return a plain list (not a NormalizationOutcome)."""
    payload = [{"Nombre": "Serie", "Data": [{"Periodo": "2024", "Valor": "1"}]}]
    result = normalize_table_payload(payload, "T1")
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0].period == "2024"
    assert result[0].value == 1.0


def test_normalize_payload_discards_series_missing_period_and_value():
    """A series with no period and no numeric value must be counted as discarded."""
    payload = [{"Nombre": "Serie sin datos"}]
    outcome = normalize_table_payload_with_stats(payload, "T1")
    assert outcome.items == []
    assert outcome.discarded_counts.get("missing_period_and_value", 0) >= 1


def test_normalize_payload_flat_observation_without_data_key():
    """A payload item with Periodo/Valor directly (no Data key) is treated as a flat observation."""
    payload = [{"Periodo": "2023", "Valor": "5.0", "Nombre": "Indicador"}]
    result = normalize_table_payload(payload, "T1")
    assert len(result) == 1
    assert result[0].period == "2023"
    assert result[0].value == 5.0


def test_normalize_payload_dict_with_collection_key():
    """A dict payload with a 'Resultados' key must be unwrapped to its list of series."""
    payload = {
        "Resultados": [
            {"Nombre": "S1", "Data": [{"Periodo": "2024", "Valor": "3"}]}
        ]
    }
    result = normalize_table_payload(payload, "T1")
    assert len(result) == 1
    assert result[0].value == 3.0


def test_normalize_payload_dict_with_data_key():
    """A dict payload with a 'Data' key containing series list must be unwrapped."""
    payload = {
        "Data": [
            {"Nombre": "S1", "Data": [{"Periodo": "2024", "Valor": "7"}]}
        ]
    }
    result = normalize_table_payload(payload, "T1")
    assert len(result) == 1
    assert result[0].value == 7.0


# ---------------------------------------------------------------------------
# _extract_period fallback keys
# ---------------------------------------------------------------------------


def test_extract_period_uses_anyo_fallback():
    payload = [{"Anyo": "2023", "Valor": "1"}]
    result = normalize_table_payload(payload, "T1")
    assert len(result) == 1
    assert result[0].period == "2023"


def test_extract_period_uses_fecha_fallback():
    payload = [{"Fecha": "2023-01", "Valor": "1"}]
    result = normalize_table_payload(payload, "T1")
    assert len(result) == 1
    assert result[0].period == "2023-01"


def test_extract_period_uses_fk_periodo_fallback():
    payload = [{"FK_Periodo": "P2023", "Valor": "1"}]
    result = normalize_table_payload(payload, "T1")
    assert len(result) == 1
    assert result[0].period == "P2023"


# ---------------------------------------------------------------------------
# Geography extraction
# ---------------------------------------------------------------------------


def test_extract_geography_returns_empty_when_no_geo_hint():
    """Without a geo-hint in MetaData and no 'asturias' in name, geography is empty."""
    payload = [{"Nombre": "Produccion industrial", "Data": [{"Periodo": "2024", "Valor": "10"}]}]
    result = normalize_table_payload(payload, "T1")
    assert result[0].geography_name == ""
    assert result[0].geography_code == ""


def test_extract_geography_infers_asturias_from_series_name():
    """Series whose name contains 'asturias' (case-insensitive) gets geography_name='Asturias'."""
    payload = [{"Nombre": "Poblacion de Asturias", "Data": [{"Periodo": "2024", "Valor": "10"}]}]
    result = normalize_table_payload(payload, "T1")
    assert result[0].geography_name == "Asturias"


def test_extract_geography_from_metadata_geo_hint():
    """MetaData entry with a geo keyword maps its Nombre to geography_name."""
    payload = [
        {
            "Nombre": "Serie con territorio",
            "MetaData": [
                {"Variable": "Territorio", "Nombre": "Gijon", "Id": "33024"}
            ],
            "Data": [{"Periodo": "2024", "Valor": "5"}],
        }
    ]
    result = normalize_table_payload(payload, "T1")
    assert result[0].geography_name == "Gijon"
    assert result[0].geography_code == "33024"


# ---------------------------------------------------------------------------
# _ensure_list: MetaData as a single dict (not a list)
# ---------------------------------------------------------------------------


def test_ensure_list_wraps_scalar_metadata():
    """When MetaData is a dict instead of a list, it must still be processed correctly."""
    payload = [
        {
            "Nombre": "Serie",
            "MetaData": {"Variable": "Territorio", "Nombre": "Oviedo", "Id": "33044"},
            "Data": [{"Periodo": "2024", "Valor": "1"}],
        }
    ]
    result = normalize_table_payload(payload, "T1")
    assert len(result) == 1
    assert result[0].geography_name == "Oviedo"
