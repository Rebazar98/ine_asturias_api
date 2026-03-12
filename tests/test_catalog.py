import httpx

from tests.conftest import override_ine_service


ASTURIAS_TABLE_PAYLOAD = [
    {
        "Nombre": "Asturias. Total. Personas.",
        "MetaData": [
            {"Variable": "Comunidad autonoma", "Nombre": "Asturias", "Id": "33"},
            {"Variable": "Indicador", "Nombre": "Poblacion", "Id": "POB"},
        ],
        "Data": [
            {"Periodo": "2024", "Valor": "1011792", "Unidad": "personas"},
            {"Periodo": "2023", "Valor": "1012345", "Unidad": "personas"},
        ],
    }
]


NO_ASTURIAS_TABLE_PAYLOAD = [
    {
        "Nombre": "Zaragoza. Total. Personas.",
        "MetaData": [
            {"Variable": "Provincia", "Nombre": "Zaragoza", "Id": "50"},
            {"Variable": "Indicador", "Nombre": "Poblacion", "Id": "POB"},
        ],
        "Data": [{"Periodo": "2024", "Valor": "968503", "Unidad": "personas"}],
    }
]


def test_catalog_is_populated_and_updated_during_asturias_pipeline(
    client,
    dummy_ingestion_repo,
    dummy_series_repo,
    dummy_catalog_repo,
):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/VARIABLES_OPERACION/OP_AST":
            return httpx.Response(200, json=[{"Id": "115", "Nombre": "Comunidad autonoma"}])
        if request.url.path == "/VALORES_VARIABLEOPERACION/115/OP_AST":
            return httpx.Response(200, json=[{"Id": "33", "Nombre": "Asturias"}])
        if request.url.path == "/TABLAS_OPERACION/OP_AST":
            return httpx.Response(
                200,
                json=[
                    {"IdTabla": "501", "Nombre": "Tabla Asturias"},
                    {"IdTabla": "502", "Nombre": "Tabla Zaragoza"},
                ],
            )
        if request.url.path == "/DATOS_TABLA/501":
            return httpx.Response(200, json=ASTURIAS_TABLE_PAYLOAD)
        if request.url.path == "/DATOS_TABLA/502":
            return httpx.Response(200, json=NO_ASTURIAS_TABLE_PAYLOAD)
        raise AssertionError(f"Unexpected path: {request.url.path}")

    override_ine_service(handler)

    response = client.get("/ine/operation/OP_AST/asturias?background=false&max_tables=2")

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["tables_found"] == 2
    assert payload["summary"]["tables_succeeded"] == 1
    assert payload["summary"]["warnings"] == 1

    catalog_rows = {
        row["table_id"]: row
        for row in (awaitable_to_sync(dummy_catalog_repo.list_by_operation("OP_AST")))
    }
    assert set(catalog_rows) == {"501", "502"}
    assert catalog_rows["501"]["validation_status"] == "has_data"
    assert catalog_rows["501"]["has_asturias_data"] is True
    assert catalog_rows["501"]["normalized_rows"] == 2
    assert catalog_rows["502"]["validation_status"] == "no_data"
    assert catalog_rows["502"]["has_asturias_data"] is False
    assert catalog_rows["502"]["last_warning"] == "no_asturias_rows_after_validation"


def test_catalog_endpoints_return_operation_rows_and_summary(
    client,
    dummy_ingestion_repo,
    dummy_series_repo,
    dummy_catalog_repo,
):
    awaitable_to_sync(
        dummy_catalog_repo.upsert_discovered_tables(
            "22",
            [
                {"table_id": "2852", "table_name": "Tabla valida", "metadata": {"IdTabla": "2852"}},
                {
                    "table_id": "2855",
                    "table_name": "Tabla descartada",
                    "metadata": {"IdTabla": "2855"},
                },
            ],
            "TABLAS_OPERACION/22",
            {"asturias_label": "Asturias"},
        )
    )
    awaitable_to_sync(
        dummy_catalog_repo.update_table_status(
            operation_code="22",
            table_id="2852",
            table_name="Tabla valida",
            request_path="DATOS_TABLA/2852",
            validation_status="has_data",
            has_asturias_data=True,
            normalized_rows=75,
            raw_rows_retrieved=100,
            filtered_rows_retrieved=75,
            series_kept=1,
            series_discarded=0,
            metadata={"IdTabla": "2852"},
        )
    )
    awaitable_to_sync(
        dummy_catalog_repo.update_table_status(
            operation_code="22",
            table_id="2855",
            table_name="Tabla descartada",
            request_path="DATOS_TABLA/2855",
            validation_status="no_data",
            has_asturias_data=False,
            raw_rows_retrieved=10,
            filtered_rows_retrieved=0,
            series_kept=0,
            series_discarded=1,
            metadata={"IdTabla": "2855"},
            last_warning="no_asturias_rows_after_validation",
        )
    )

    rows_response = client.get("/ine/catalog/operation/22")
    summary_response = client.get("/ine/catalog/operation/22/summary")

    assert rows_response.status_code == 200
    assert summary_response.status_code == 200
    rows = rows_response.json()
    summary = summary_response.json()
    assert [row["table_id"] for row in rows] == ["2852", "2855"]
    assert rows[0]["validation_status"] == "has_data"
    assert rows[1]["validation_status"] == "no_data"
    assert summary == {
        "operation_code": "22",
        "total_tables": 2,
        "has_data": 1,
        "no_data": 1,
        "failed": 0,
        "unknown": 0,
    }


def test_skip_known_no_data_avoids_reprocessing_catalogued_tables(
    client,
    dummy_ingestion_repo,
    dummy_series_repo,
    dummy_catalog_repo,
):
    called_paths = []
    awaitable_to_sync(
        dummy_catalog_repo.update_table_status(
            operation_code="OP_AST",
            table_id="502",
            table_name="Tabla Zaragoza",
            request_path="DATOS_TABLA/502",
            validation_status="no_data",
            has_asturias_data=False,
            metadata={"IdTabla": "502"},
            last_warning="no_asturias_rows_after_validation",
        )
    )

    def handler(request: httpx.Request) -> httpx.Response:
        called_paths.append(request.url.path)
        if request.url.path == "/VARIABLES_OPERACION/OP_AST":
            return httpx.Response(200, json=[{"Id": "115", "Nombre": "Comunidad autonoma"}])
        if request.url.path == "/VALORES_VARIABLEOPERACION/115/OP_AST":
            return httpx.Response(200, json=[{"Id": "33", "Nombre": "Asturias"}])
        if request.url.path == "/TABLAS_OPERACION/OP_AST":
            return httpx.Response(
                200,
                json=[
                    {"IdTabla": "501", "Nombre": "Tabla Asturias"},
                    {"IdTabla": "502", "Nombre": "Tabla Zaragoza"},
                ],
            )
        if request.url.path == "/DATOS_TABLA/501":
            return httpx.Response(200, json=ASTURIAS_TABLE_PAYLOAD)
        if request.url.path == "/DATOS_TABLA/502":
            raise AssertionError("Table 502 should have been skipped by catalog")
        raise AssertionError(f"Unexpected path: {request.url.path}")

    override_ine_service(handler)

    response = client.get(
        "/ine/operation/OP_AST/asturias?background=false&skip_known_no_data=true&max_tables=2"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["tables_skipped_catalog"] == 1
    assert payload["tables_selected"] == ["501"]
    assert called_paths == [
        "/VARIABLES_OPERACION/OP_AST",
        "/VALORES_VARIABLEOPERACION/115/OP_AST",
        "/TABLAS_OPERACION/OP_AST",
        "/DATOS_TABLA/501",
    ]


def awaitable_to_sync(awaitable):
    import asyncio

    return asyncio.run(awaitable)
