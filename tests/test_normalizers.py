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
