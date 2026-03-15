from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable

import httpx


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test del stack ine_asturias_api.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8001")
    parser.add_argument("--operation-code", default="22")
    parser.add_argument(
        "--municipality-code",
        default=os.getenv("SMOKE_MUNICIPALITY_CODE"),
    )
    parser.add_argument("--max-tables", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    parser.add_argument("--poll-interval", type=float, default=2.0)
    parser.add_argument("--page-size", type=int, default=5)
    parser.add_argument(
        "--api-key",
        default=os.getenv("SMOKE_API_KEY")
        or os.getenv("API_KEY")
        or _read_env_file_value("API_KEY"),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_url = args.base_url.rstrip("/")
    startup_grace_seconds = min(max(args.timeout_seconds / 4, 15.0), 60.0)
    request_timeout_seconds = min(max(args.timeout_seconds / 6, 10.0), 30.0)
    timeout = httpx.Timeout(request_timeout_seconds, connect=5.0)
    headers = {"X-API-Key": args.api_key} if args.api_key else None

    with httpx.Client(base_url=base_url, timeout=timeout, headers=headers) as client:
        _wait_for_json_condition(
            client=client,
            path="/health",
            timeout_seconds=startup_grace_seconds,
            poll_interval=min(args.poll_interval, 2.0),
            expected_status=200,
            description="/health",
            validator=lambda payload: payload.get("status") == "ok",
        )
        print("[smoke] /health OK")

        _wait_for_json_condition(
            client=client,
            path="/health/ready",
            timeout_seconds=startup_grace_seconds,
            poll_interval=min(args.poll_interval, 2.0),
            expected_status=200,
            description="/health/ready",
            validator=lambda payload: payload.get("status") == "ok",
        )
        print("[smoke] /health/ready OK")

        metrics_text = _get_text(client, "/metrics", expected_status=200)
        if "ine_asturias_worker_heartbeat_timestamp" not in metrics_text:
            raise RuntimeError("/metrics no incluye la senal de heartbeat del worker.")
        print("[smoke] /metrics OK")

        communities_payload = _get_json(
            client,
            "/territorios/comunidades-autonomas?page=1&page_size=1",
            expected_status=200,
        )
        if "items" not in communities_payload or "filters" not in communities_payload:
            raise RuntimeError(
                "/territorios/comunidades-autonomas no devolvio el contrato esperado."
            )
        print("[smoke] /territorios/comunidades-autonomas OK")

        _validate_territorial_catalog(client)
        print("[smoke] /territorios/catalogo OK")

        job_payload = _get_json(
            client,
            f"/ine/operation/{args.operation_code}/asturias?max_tables={args.max_tables}",
            expected_status=202,
        )
        job_id = str(job_payload["job_id"])
        print(f"[smoke] job encolado: {job_id}")

        job_status = _wait_for_terminal_job_state(
            client=client,
            job_path=f"/ine/jobs/{job_id}",
            timeout_seconds=args.timeout_seconds,
            poll_interval=args.poll_interval,
        )
        if job_status["status"] != "completed":
            raise RuntimeError(f"El job termino en estado no valido: {job_status}")
        print("[smoke] job completado")

        result = job_status.get("result") or {}
        tables_selected = result.get("tables_selected") or []
        table_id = str(tables_selected[0]) if tables_selected else None

        series_query = (
            f"/ine/series?operation_code={args.operation_code}&page=1&page_size={args.page_size}"
        )
        if table_id:
            series_query += f"&table_id={table_id}"
        series_payload = _get_json(client, series_query, expected_status=200)

        if int(series_payload.get("total") or 0) <= 0:
            raise RuntimeError(f"/ine/series no devolvio resultados tras el job: {series_payload}")
        if not series_payload.get("items"):
            raise RuntimeError(f"/ine/series devolvio total>0 pero sin items: {series_payload}")
        print("[smoke] /ine/series OK")

        if args.municipality_code:
            _validate_municipality_analytics(
                client=client,
                municipality_code=str(args.municipality_code),
                page_size=args.page_size,
                timeout_seconds=args.timeout_seconds,
                poll_interval=args.poll_interval,
            )
        else:
            print("[smoke] validacion analitica omitida (sin municipality_code)")

    print("[smoke] validacion completada")
    return 0


def _wait_for_terminal_job_state(
    client: httpx.Client,
    job_path: str,
    timeout_seconds: float,
    poll_interval: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_payload: dict[str, Any] | None = None

    while time.monotonic() < deadline:
        payload = _get_json(client, job_path, expected_status=200)
        last_payload = payload
        status = payload.get("status")
        if status in {"completed", "failed"}:
            return payload
        time.sleep(poll_interval)

    raise RuntimeError(
        f"Timeout esperando a que el job termine. Ultimo estado observado: {last_payload}"
    )


def _validate_territorial_catalog(client: httpx.Client) -> dict[str, Any]:
    catalog_payload = _get_json(client, "/territorios/catalogo", expected_status=200)
    if catalog_payload.get("source") != "internal.catalog.territorial":
        raise RuntimeError(
            f"/territorios/catalogo devolvio source no valido: {catalog_payload.get('source')}"
        )

    resources = catalog_payload.get("resources") or []
    resource_keys = {resource.get("resource_key") for resource in resources}
    expected_resources = {
        "territorial.autonomous_communities.list",
        "territorial.provinces.list",
        "territorial.geocode.query",
        "territorial.reverse_geocode.query",
        "territorial.municipality.detail",
        "territorial.municipality.summary",
        "territorial.municipality.report_job",
        "territorial.jobs.status",
    }
    if not expected_resources.issubset(resource_keys):
        raise RuntimeError(
            "/territorios/catalogo no expone todos los recursos esperados para descubrimiento."
        )

    territorial_levels = catalog_payload.get("territorial_levels") or []
    if not territorial_levels:
        raise RuntimeError("/territorios/catalogo no incluye cobertura territorial.")

    municipality_level = next(
        (level for level in territorial_levels if level.get("unit_level") == "municipality"),
        None,
    )
    if municipality_level is None:
        raise RuntimeError("/territorios/catalogo no incluye el nivel municipality.")

    return catalog_payload


def _validate_municipality_analytics(
    *,
    client: httpx.Client,
    municipality_code: str,
    page_size: int,
    timeout_seconds: float,
    poll_interval: float,
) -> None:
    query = f"?page=1&page_size={page_size}"
    summary_path = f"/territorios/municipio/{municipality_code}/resumen{query}"
    summary_payload = _get_json(client, summary_path, expected_status=200)
    if summary_payload.get("source") != "internal.analytics.municipality_summary":
        raise RuntimeError(
            f"{summary_path} no devolvio el contrato analitico esperado: {summary_payload}"
        )
    if summary_payload.get("filters", {}).get("municipality_code") != municipality_code:
        raise RuntimeError(
            f"{summary_path} devolvio municipality_code inesperado: {summary_payload}"
        )
    print(f"[smoke] {summary_path} OK")

    report_response = _request_json(
        client,
        "POST",
        f"/territorios/municipio/{municipality_code}/informe{query}",
        expected_status=202,
    )
    status_path = str(report_response.get("status_path") or "")
    if not status_path.startswith("/territorios/jobs/"):
        raise RuntimeError(f"El job territorial no devolvio status_path valido: {report_response}")
    print(f"[smoke] informe municipal encolado: {report_response['job_id']}")

    job_payload = _wait_for_terminal_job_state(
        client=client,
        job_path=status_path,
        timeout_seconds=timeout_seconds,
        poll_interval=poll_interval,
    )
    if job_payload["status"] != "completed":
        raise RuntimeError(f"El informe municipal termino en estado no valido: {job_payload}")

    result = job_payload.get("result") or {}
    if result.get("report_type") != "municipality_report":
        raise RuntimeError(f"El informe municipal no devolvio report_type esperado: {result}")
    if result.get("territorial_context", {}).get("municipality_code") != municipality_code:
        raise RuntimeError(f"El informe municipal devolvio municipality_code inesperado: {result}")
    if not result.get("sections"):
        raise RuntimeError(f"El informe municipal no incluye secciones: {result}")
    print("[smoke] informe municipal completado")


def _wait_for_json_condition(
    client: httpx.Client,
    path: str,
    timeout_seconds: float,
    poll_interval: float,
    expected_status: int,
    description: str,
    validator: Callable[[dict[str, Any]], bool],
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    last_payload: dict[str, Any] | None = None

    while time.monotonic() < deadline:
        try:
            payload = _get_json(client, path, expected_status=expected_status)
            last_payload = payload
            if validator(payload):
                return payload
            last_error = RuntimeError(f"{description} devolvio un payload no valido: {payload}")
        except Exception as exc:
            last_error = exc

        time.sleep(poll_interval)

    raise RuntimeError(
        f"Timeout esperando a que {description} estuviera disponible. "
        f"Ultimo payload observado: {last_payload}. Ultimo error: {last_error}"
    )


def _get_json(client: httpx.Client, path: str, expected_status: int) -> dict[str, Any]:
    response = _request_with_retry(client, "GET", path)
    if response.status_code != expected_status:
        raise RuntimeError(
            f"Respuesta inesperada para {path}: {response.status_code} {response.text}"
        )
    return response.json()


def _get_text(client: httpx.Client, path: str, expected_status: int) -> str:
    response = _request_with_retry(client, "GET", path)
    if response.status_code != expected_status:
        raise RuntimeError(
            f"Respuesta inesperada para {path}: {response.status_code} {response.text}"
        )
    return response.text


def _request_json(
    client: httpx.Client,
    method: str,
    path: str,
    expected_status: int,
) -> dict[str, Any]:
    response = _request_with_retry(client, method, path)
    if response.status_code != expected_status:
        raise RuntimeError(
            f"Respuesta inesperada para {path}: {response.status_code} {response.text}"
        )
    return response.json()


def _request_with_retry(
    client: httpx.Client,
    method: str,
    path: str,
    retries: int = 3,
    retry_interval: float = 1.0,
) -> httpx.Response:
    last_error: httpx.RequestError | None = None

    for attempt in range(retries):
        try:
            return client.request(method, path)
        except httpx.RequestError as exc:
            last_error = exc
            if attempt == retries - 1:
                break
            time.sleep(retry_interval)

    raise RuntimeError(f"Error de conexion para {path}: {last_error}") from last_error


def _read_env_file_value(name: str) -> str | None:
    for candidate in (Path(".env"), Path(".env.local"), Path(".env.example")):
        if not candidate.exists():
            continue
        for raw_line in candidate.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() == name:
                normalized = value.strip()
                return normalized or None
    return None


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[smoke] ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
