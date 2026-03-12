import time

import httpx

from tests.conftest import override_ine_service


TABLE_1_PAYLOAD = [
    {
        "Nombre": "Serie Asturias tabla 501",
        "MetaData": [
            {"Variable": "Territorio", "Nombre": "Principado de Asturias", "Id": "8999"},
            {"Variable": "Indicador", "Nombre": "Poblacion", "Id": "POB"},
        ],
        "Data": [{"Periodo": "2024", "Valor": "1012345", "Unidad": "personas"}],
    }
]

TABLE_2_PAYLOAD = [
    {
        "Nombre": "Serie Asturias tabla 502",
        "MetaData": [
            {"Variable": "Territorio", "Nombre": "Principado de Asturias", "Id": "8999"},
            {"Variable": "Indicador", "Nombre": "Indice", "Id": "IDX"},
        ],
        "Data": [
            {"Periodo": "2024M01", "Valor": "101,5", "Unidad": "indice"},
            {"Periodo": "2024M02", "Valor": "102,1", "Unidad": "indice"},
        ],
    }
]

MIXED_TABLE_PAYLOAD = [
    {
        "Nombre": "Serie Asturias",
        "MetaData": [
            {"Variable": "Territorio", "Nombre": "Asturias, Principado de", "Id": "33"},
            {"Variable": "Indicador", "Nombre": "Indice general", "Id": "IPC"},
        ],
        "Data": [{"Periodo": "2024M01", "Valor": "101,5", "Unidad": "indice"}],
    },
    {
        "Nombre": "Serie Zaragoza",
        "MetaData": [
            {"Variable": "Territorio", "Nombre": "Zaragoza", "Id": "50"},
            {"Variable": "Indicador", "Nombre": "Indice general", "Id": "IPC"},
        ],
        "Data": [{"Periodo": "2024M01", "Valor": "99,4", "Unidad": "indice"}],
    },
]


def test_asturias_endpoint_can_run_in_background_and_report_status(
    client, dummy_ingestion_repo, dummy_series_repo
):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/VARIABLES_OPERACION/OP_AST":
            return httpx.Response(200, json=[{"Id": "115", "Nombre": "Comunidad autonoma"}])
        if request.url.path == "/VALORES_VARIABLEOPERACION/115/OP_AST":
            return httpx.Response(200, json=[{"Id": "33", "Nombre": "Principado de Asturias"}])
        if request.url.path == "/TABLAS_OPERACION/OP_AST":
            return httpx.Response(200, json=[{"IdTabla": "501", "Nombre": "Tabla principal"}])
        if request.url.path == "/DATOS_TABLA/501":
            return httpx.Response(200, json=TABLE_1_PAYLOAD)
        raise AssertionError(f"Unexpected path: {request.url.path}")

    override_ine_service(handler)

    response = client.get("/ine/operation/OP_AST/asturias?background=true&max_tables=1")

    assert response.status_code == 202
    accepted = response.json()
    assert accepted["job_type"] == "operation_asturias_ingestion"
    assert accepted["status"] in {"queued", "running"}

    job_payload = None
    for _ in range(50):
        status_response = client.get(accepted["status_path"])
        assert status_response.status_code == 200
        job_payload = status_response.json()
        if job_payload["status"] == "completed":
            break
        time.sleep(0.02)

    assert job_payload is not None
    assert job_payload["status"] == "completed"
    assert job_payload["progress"]["stage"] in {"table_completed", "tables_selected"}
    assert job_payload["result"]["summary"]["tables_succeeded"] == 1


def test_asturias_endpoint_resolves_automatically_through_tables(
    client, dummy_ingestion_repo, dummy_series_repo
):
    called_paths = []

    def handler(request: httpx.Request) -> httpx.Response:
        called_paths.append(request.url.path)
        if request.url.path == "/VARIABLES_OPERACION/OP_AST":
            return httpx.Response(
                200,
                json=[
                    {"Id": "200", "Nombre": "Indicador"},
                    {"Id": "115", "Nombre": "Comunidad autonoma"},
                ],
            )
        if request.url.path == "/VALORES_VARIABLEOPERACION/115/OP_AST":
            return httpx.Response(
                200,
                json=[
                    {"Id": "28", "Nombre": "Madrid"},
                    {"Id": "33", "Nombre": "Principado de Asturias"},
                ],
            )
        if request.url.path == "/TABLAS_OPERACION/OP_AST":
            return httpx.Response(
                200,
                json=[
                    {"IdTabla": "501", "Nombre": "Tabla principal"},
                    {"IdTabla": "502", "Nombre": "Tabla secundaria"},
                ],
            )
        if request.url.path == "/DATOS_TABLA/501":
            assert request.url.params["g1"] == "115:33"
            assert request.url.params["p"] == "1"
            return httpx.Response(200, json=TABLE_1_PAYLOAD)
        if request.url.path == "/DATOS_TABLA/502":
            assert request.url.params["g1"] == "115:33"
            assert request.url.params["p"] == "1"
            return httpx.Response(200, json=TABLE_2_PAYLOAD)
        raise AssertionError(f"Unexpected path: {request.url.path}")

    override_ine_service(handler)

    response = client.get("/ine/operation/OP_AST/asturias?periodicidad=1&nult=1&background=false")

    assert response.status_code == 200
    payload = response.json()
    assert payload["operation_code"] == "OP_AST"
    assert payload["resolution"]["geo_variable_id"] == "115"
    assert payload["resolution"]["asturias_value_id"] == "33"
    assert payload["tables_selected"] == ["501", "502"]
    assert len(payload["results"]) == 2
    assert payload["results"][0]["table_id"] == "501"
    assert payload["results"][1]["table_id"] == "502"
    assert payload["results"][0]["raw_rows_retrieved"] == 1
    assert payload["results"][1]["filtered_rows_retrieved"] == 2
    assert payload["summary"]["tables_succeeded"] == 2
    assert payload["summary"]["tables_failed"] == 0
    assert payload["summary"]["max_tables_effective"] == 3
    assert len(dummy_ingestion_repo.records) == 4
    assert dummy_ingestion_repo.records[0]["source_type"] == "operation_tables"
    assert dummy_ingestion_repo.records[1]["source_type"] == "operation_asturias_table"
    assert dummy_ingestion_repo.records[2]["source_type"] == "operation_asturias_table"
    assert dummy_ingestion_repo.records[3]["source_type"] == "operation_asturias"
    assert len(dummy_series_repo.items) == 3
    assert dummy_series_repo.items[0].operation_code == "OP_AST"
    assert dummy_series_repo.items[0].table_id == "501"
    assert dummy_series_repo.items[1].table_id == "502"
    assert called_paths == [
        "/VARIABLES_OPERACION/OP_AST",
        "/VALORES_VARIABLEOPERACION/115/OP_AST",
        "/TABLAS_OPERACION/OP_AST",
        "/DATOS_TABLA/501",
        "/DATOS_TABLA/502",
    ]


def test_asturias_endpoint_accepts_manual_override_and_returns_partial_results(
    client,
    dummy_ingestion_repo,
    dummy_series_repo,
):
    called_paths = []

    def handler(request: httpx.Request) -> httpx.Response:
        called_paths.append(request.url.path)
        if request.url.path == "/TABLAS_OPERACION/OP_AST":
            return httpx.Response(
                200,
                json=[
                    {"IdTabla": "501", "Nombre": "Tabla principal"},
                    {"IdTabla": "502", "Nombre": "Tabla secundaria"},
                ],
            )
        if request.url.path == "/DATOS_TABLA/501":
            assert request.url.params["g1"] == "999:33"
            return httpx.Response(200, json=TABLE_1_PAYLOAD)
        if request.url.path == "/DATOS_TABLA/502":
            return httpx.Response(503, json={"error": "upstream table error"})
        raise AssertionError(f"Unexpected path: {request.url.path}")

    override_ine_service(handler)

    response = client.get(
        "/ine/operation/OP_AST/asturias?geo_variable_id=999&asturias_value_id=33&tip=AM&background=false"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["resolution"]["geo_variable_id"] == "999"
    assert payload["resolution"]["asturias_value_id"] == "33"
    assert payload["summary"]["tables_succeeded"] == 1
    assert payload["summary"]["tables_failed"] == 1
    assert len(payload["errors"]) == 1
    assert payload["errors"][0]["table_id"] == "502"
    assert len(dummy_series_repo.items) == 1
    assert dummy_series_repo.items[0].table_id == "501"
    assert called_paths == [
        "/TABLAS_OPERACION/OP_AST",
        "/DATOS_TABLA/501",
        "/DATOS_TABLA/502",
    ]


def test_asturias_endpoint_respects_max_tables_limit(
    client, dummy_ingestion_repo, dummy_series_repo
):
    called_paths = []

    def handler(request: httpx.Request) -> httpx.Response:
        called_paths.append(request.url.path)
        if request.url.path == "/VARIABLES_OPERACION/OP_AST":
            return httpx.Response(200, json=[{"Id": "115", "Nombre": "Comunidad autonoma"}])
        if request.url.path == "/VALORES_VARIABLEOPERACION/115/OP_AST":
            return httpx.Response(200, json=[{"Id": "33", "Nombre": "Principado de Asturias"}])
        if request.url.path == "/TABLAS_OPERACION/OP_AST":
            return httpx.Response(
                200,
                json=[
                    {"IdTabla": "501", "Nombre": "Tabla 1"},
                    {"IdTabla": "502", "Nombre": "Tabla 2"},
                    {"IdTabla": "503", "Nombre": "Tabla 3"},
                ],
            )
        if request.url.path == "/DATOS_TABLA/501":
            return httpx.Response(200, json=TABLE_1_PAYLOAD)
        raise AssertionError(f"Unexpected path: {request.url.path}")

    override_ine_service(handler)

    response = client.get("/ine/operation/OP_AST/asturias?max_tables=1&background=false")

    assert response.status_code == 200
    payload = response.json()
    assert payload["tables_selected"] == ["501"]
    assert payload["summary"]["max_tables_effective"] == 1
    assert len(payload["results"]) == 1
    assert len(dummy_series_repo.items) == 1
    assert called_paths == [
        "/VARIABLES_OPERACION/OP_AST",
        "/VALORES_VARIABLEOPERACION/115/OP_AST",
        "/TABLAS_OPERACION/OP_AST",
        "/DATOS_TABLA/501",
    ]


def test_asturias_endpoint_filters_non_asturias_series_after_download(
    client, dummy_ingestion_repo, dummy_series_repo
):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/VARIABLES_OPERACION/OP_AST":
            return httpx.Response(200, json=[{"Id": "115", "Nombre": "Comunidad autonoma"}])
        if request.url.path == "/VALORES_VARIABLEOPERACION/115/OP_AST":
            return httpx.Response(200, json=[{"Id": "33", "Nombre": "Asturias, Principado de"}])
        if request.url.path == "/TABLAS_OPERACION/OP_AST":
            return httpx.Response(200, json=[{"IdTabla": "501", "Nombre": "Tabla 1"}])
        if request.url.path == "/DATOS_TABLA/501":
            return httpx.Response(200, json=MIXED_TABLE_PAYLOAD)
        raise AssertionError(f"Unexpected path: {request.url.path}")

    override_ine_service(handler)

    response = client.get("/ine/operation/OP_AST/asturias?max_tables=1&background=false")

    assert response.status_code == 200
    payload = response.json()
    assert payload["results"][0]["raw_rows_retrieved"] == 2
    assert payload["results"][0]["filtered_rows_retrieved"] == 1
    assert len(payload["results"][0]["data"]) == 1
    assert payload["results"][0]["data"][0]["Nombre"] == "Serie Asturias"
    assert len(dummy_series_repo.items) == 1
    assert dummy_series_repo.items[0].geography_name == "Asturias, Principado de"


def test_asturias_endpoint_returns_clear_error_if_all_tables_fail(
    client, dummy_ingestion_repo, dummy_series_repo
):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/VARIABLES_OPERACION/OP_AST":
            return httpx.Response(200, json=[{"Id": "115", "Nombre": "Comunidad autonoma"}])
        if request.url.path == "/VALORES_VARIABLEOPERACION/115/OP_AST":
            return httpx.Response(200, json=[{"Id": "33", "Nombre": "Principado de Asturias"}])
        if request.url.path == "/TABLAS_OPERACION/OP_AST":
            return httpx.Response(200, json=[{"IdTabla": "501", "Nombre": "Tabla principal"}])
        if request.url.path == "/DATOS_TABLA/501":
            return httpx.Response(503, json={"error": "upstream table error"})
        raise AssertionError(f"Unexpected path: {request.url.path}")

    override_ine_service(handler)

    response = client.get("/ine/operation/OP_AST/asturias?background=false")

    assert response.status_code == 502
    payload = response.json()
    assert (
        payload["detail"]["message"]
        == "No table data could be recovered for Asturias in this operation."
    )
    assert payload["detail"]["tables_found"] == ["501"]
    assert len(payload["detail"]["errors"]) == 1
    assert dummy_ingestion_repo.records[0]["source_type"] == "operation_tables"
    assert dummy_series_repo.items == []
