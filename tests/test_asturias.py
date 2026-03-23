import asyncio
import time

import httpx

from app.core.cache import InMemoryTTLCache
from app.dependencies import get_ine_client_service
from app.main import app
from app.services.ine_client import INEClientService
from app.settings import Settings
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

# Operation 33 (Movimiento Natural de la Población) realistic payloads.
# Table names arrive from INE with mojibake (UTF-8 bytes stored as Latin-1).
OP33_TABLE_WITH_ASTURIAS = [
    {
        "Nombre": "Asturias. Nacidos vivos por municipio",
        "MetaData": [
            {"Variable": "Comunidades y Ciudades AutÃ³nomas", "Nombre": "Principado de Asturias", "Id": "33"},
            {"Variable": "Indicador", "Nombre": "Nacidos vivos", "Id": "NV"},
        ],
        "Data": [{"Periodo": "2022", "Valor": "4521", "Unidad": "personas"}],
    },
    {
        "Nombre": "Madrid. Nacidos vivos por municipio",
        "MetaData": [
            {"Variable": "Comunidades y Ciudades AutÃ³nomas", "Nombre": "Madrid", "Id": "28"},
            {"Variable": "Indicador", "Nombre": "Nacidos vivos", "Id": "NV"},
        ],
        "Data": [{"Periodo": "2022", "Valor": "58432", "Unidad": "personas"}],
    },
]

OP33_TABLE_NATIONAL_ONLY = [
    {
        "Nombre": "Total Nacional. Nupcialidad",
        "MetaData": [
            {"Variable": "Indicador", "Nombre": "Total matrimonios", "Id": "TOT"},
        ],
        "Data": [{"Periodo": "2022", "Valor": "164356", "Unidad": "matrimonios"}],
    }
]


def test_asturias_endpoint_fixes_mojibake_in_op33_table_names(
    client, dummy_ingestion_repo, dummy_series_repo
):
    """Table names returned by INE with mojibake encoding are corrected in the response.

    Operation 33 (Movimiento Natural) TABLAS_OPERACION returns names like
    'InmigraciÃ³n' which must appear as 'Inmigración' in the API response.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/VARIABLES_OPERACION/33":
            return httpx.Response(
                200,
                json=[{"Id": "70", "Nombre": "Comunidades y Ciudades AutÃ³nomas"}],
            )
        if request.url.path == "/VALORES_VARIABLEOPERACION/70/33":
            return httpx.Response(
                200,
                json=[
                    {"Id": "33", "Nombre": "Principado de Asturias"},
                    {"Id": "28", "Nombre": "Comunidad de Madrid"},
                ],
            )
        if request.url.path == "/TABLAS_OPERACION/33":
            return httpx.Response(
                200,
                json=[
                    {"IdTabla": "2852", "Nombre": "Tasa Bruta de InmigraciÃ³n procedente del extranjero"},
                    {"IdTabla": "2901", "Nombre": "Nacidos vivos segÃºn edad de la madre"},
                ],
            )
        if request.url.path == "/DATOS_TABLA/2852":
            return httpx.Response(200, json=OP33_TABLE_WITH_ASTURIAS)
        if request.url.path == "/DATOS_TABLA/2901":
            return httpx.Response(200, json=OP33_TABLE_WITH_ASTURIAS)
        raise AssertionError(f"Unexpected path: {request.url.path}")

    override_ine_service(handler)

    response = client.get("/ine/operation/33/asturias?background=false")

    assert response.status_code == 200
    payload = response.json()
    table_names = {t["table_name"] for t in payload["tables_found"]}
    assert "Tasa Bruta de Inmigración procedente del extranjero" in table_names
    assert "Nacidos vivos según edad de la madre" in table_names
    assert payload["summary"]["tables_succeeded"] == 2
    assert payload["resolution"]["geo_variable_id"] == "70"


def test_asturias_endpoint_op33_national_tables_produce_warnings(
    client, dummy_ingestion_repo, dummy_series_repo
):
    """Tables without a geographic breakdown for Asturias produce no_asturias_rows_after_validation.

    Operation 33 has several national-level tables (no CCAA dimension) which
    the ingestion filters out and records as warnings, not errors.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/VARIABLES_OPERACION/33":
            return httpx.Response(200, json=[{"Id": "70", "Nombre": "Comunidades y Ciudades Autonomas"}])
        if request.url.path == "/VALORES_VARIABLEOPERACION/70/33":
            return httpx.Response(
                200,
                json=[{"Id": "33", "Nombre": "Principado de Asturias"}],
            )
        if request.url.path == "/TABLAS_OPERACION/33":
            return httpx.Response(
                200,
                json=[
                    {"IdTabla": "2852", "Nombre": "Nacidos vivos con desglose territorial"},
                    {"IdTabla": "2853", "Nombre": "Total nacional sin desglose territorial"},
                ],
            )
        if request.url.path == "/DATOS_TABLA/2852":
            return httpx.Response(200, json=OP33_TABLE_WITH_ASTURIAS)
        if request.url.path == "/DATOS_TABLA/2853":
            return httpx.Response(200, json=OP33_TABLE_NATIONAL_ONLY)
        raise AssertionError(f"Unexpected path: {request.url.path}")

    override_ine_service(handler)

    response = client.get("/ine/operation/33/asturias?background=false")

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["tables_succeeded"] == 1
    assert payload["summary"]["warnings"] == 1
    assert payload["summary"]["tables_failed"] == 0
    warning = payload["warnings"][0]
    assert warning["table_id"] == "2853"
    assert warning["warning"] == "no_asturias_rows_after_validation"
    assert len(dummy_series_repo.items) == 1


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
    """
    Validate partial results when one upstream table keeps returning 503.

    The INE client now retries retryable upstream failures, so the failing table
    is requested three times before the endpoint returns a partial-success payload.
    """
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
    assert called_paths.count("/TABLAS_OPERACION/OP_AST") == 1
    assert called_paths.count("/DATOS_TABLA/501") == 1
    assert called_paths.count("/DATOS_TABLA/502") == 3
    assert called_paths == [
        "/TABLAS_OPERACION/OP_AST",
        "/DATOS_TABLA/501",
        "/DATOS_TABLA/502",
        "/DATOS_TABLA/502",
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
    assert dummy_series_repo.items[0].geography_name == "Principado de Asturias"
    assert dummy_series_repo.items[0].geography_code == "33"


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


def test_asturias_endpoint_fetches_selected_tables_with_bounded_concurrency(
    client, dummy_ingestion_repo, dummy_series_repo
):
    table_start_times: dict[str, float] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/VARIABLES_OPERACION/OP_AST":
            return httpx.Response(200, json=[{"Id": "115", "Nombre": "Comunidad autonoma"}])
        if request.url.path == "/VALORES_VARIABLEOPERACION/115/OP_AST":
            return httpx.Response(200, json=[{"Id": "33", "Nombre": "Principado de Asturias"}])
        if request.url.path == "/TABLAS_OPERACION/OP_AST":
            return httpx.Response(
                200,
                json=[
                    {"IdTabla": "501", "Nombre": "Tabla principal"},
                    {"IdTabla": "502", "Nombre": "Tabla secundaria"},
                ],
            )
        if request.url.path == "/DATOS_TABLA/501":
            table_start_times["501"] = time.perf_counter()
            await asyncio.sleep(0.05)
            return httpx.Response(200, json=TABLE_1_PAYLOAD)
        if request.url.path == "/DATOS_TABLA/502":
            table_start_times["502"] = time.perf_counter()
            await asyncio.sleep(0.05)
            return httpx.Response(200, json=TABLE_2_PAYLOAD)
        raise AssertionError(f"Unexpected path: {request.url.path}")

    override_ine_service(handler)

    response = client.get("/ine/operation/OP_AST/asturias?background=false&max_tables=2")

    assert response.status_code == 200
    assert set(table_start_times) == {"501", "502"}
    assert abs(table_start_times["501"] - table_start_times["502"]) < 0.04
    assert [item.table_id for item in dummy_series_repo.items] == ["501", "502", "502"]


def test_asturias_inline_job_marks_failed_when_ine_upstream_returns_503(
    client, dummy_ingestion_repo, dummy_series_repo
):
    """When the INE API returns 503, the inline background job must end as 'failed'.

    Retries are disabled (max_attempts=1) so the job fails immediately without
    the default 1-second backoff that would outlast the polling window.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "service unavailable"})

    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(transport=transport)
    settings = Settings(
        ine_base_url="https://mocked.ine",
        enable_cache=False,
        http_retry_max_attempts=1,
        http_retry_backoff_seconds=0.01,
    )
    cache = InMemoryTTLCache(enabled=False, default_ttl_seconds=60)
    service = INEClientService(http_client=http_client, settings=settings, cache=cache)
    app.dependency_overrides[get_ine_client_service] = lambda: service

    response = client.get("/ine/operation/OP_AST/asturias?background=true&max_tables=1")
    assert response.status_code == 202

    accepted = response.json()
    job_record = None
    for _ in range(50):
        status_response = client.get(accepted["status_path"])
        assert status_response.status_code == 200
        job_record = status_response.json()
        if job_record["status"] in {"failed", "completed"}:
            break
        time.sleep(0.02)

    assert job_record is not None
    assert job_record["status"] == "failed"
    assert "error" in job_record


def test_asturias_inline_job_marks_failed_when_resolution_finds_no_asturias_territory(
    client, dummy_ingestion_repo, dummy_series_repo
):
    """When the INE operation has no territory variable, resolution fails and the job is 'failed'."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/VARIABLES_OPERACION/OP_AST":
            # Return variables with no geographic/territory dimension
            return httpx.Response(200, json=[{"Id": "999", "Nombre": "Indicador economico"}])
        raise AssertionError(f"Unexpected path: {request.url.path}")

    override_ine_service(handler)

    response = client.get("/ine/operation/OP_AST/asturias?background=true&max_tables=1")
    assert response.status_code == 202

    accepted = response.json()
    job_record = None
    for _ in range(50):
        status_response = client.get(accepted["status_path"])
        assert status_response.status_code == 200
        job_record = status_response.json()
        if job_record["status"] in {"failed", "completed"}:
            break
        time.sleep(0.02)

    assert job_record is not None
    assert job_record["status"] == "failed"
    assert "error" in job_record
