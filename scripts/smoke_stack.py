from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Any

import httpx


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test del stack ine_asturias_api.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8001")
    parser.add_argument("--operation-code", default="22")
    parser.add_argument("--max-tables", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    parser.add_argument("--poll-interval", type=float, default=2.0)
    parser.add_argument("--page-size", type=int, default=5)
    parser.add_argument("--api-key", default=os.getenv("SMOKE_API_KEY") or os.getenv("API_KEY"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_url = args.base_url.rstrip("/")
    request_timeout_seconds = min(max(args.timeout_seconds / 6, 10.0), 30.0)
    timeout = httpx.Timeout(request_timeout_seconds, connect=5.0)
    headers = {"X-API-Key": args.api_key} if args.api_key else None

    with httpx.Client(base_url=base_url, timeout=timeout, headers=headers) as client:
        health = _get_json(client, "/health", expected_status=200)
        if health.get("status") != "ok":
            raise RuntimeError(f"/health devolvio un estado invalido: {health}")
        print("[smoke] /health OK")

        readiness = _get_json(client, "/health/ready", expected_status=200)
        if readiness.get("status") != "ok":
            raise RuntimeError(f"/health/ready no esta listo: {readiness}")
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

        job_payload = _get_json(
            client,
            f"/ine/operation/{args.operation_code}/asturias?max_tables={args.max_tables}",
            expected_status=202,
        )
        job_id = str(job_payload["job_id"])
        print(f"[smoke] job encolado: {job_id}")

        job_status = _wait_for_terminal_job_state(
            client=client,
            job_id=job_id,
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

    print("[smoke] validacion completada")
    return 0


def _wait_for_terminal_job_state(
    client: httpx.Client,
    job_id: str,
    timeout_seconds: float,
    poll_interval: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_payload: dict[str, Any] | None = None

    while time.monotonic() < deadline:
        payload = _get_json(client, f"/ine/jobs/{job_id}", expected_status=200)
        last_payload = payload
        status = payload.get("status")
        if status in {"completed", "failed"}:
            return payload
        time.sleep(poll_interval)

    raise RuntimeError(
        f"Timeout esperando a que el job termine. Ultimo estado observado: {last_payload}"
    )


def _get_json(client: httpx.Client, path: str, expected_status: int) -> dict[str, Any]:
    response = client.get(path)
    if response.status_code != expected_status:
        raise RuntimeError(
            f"Respuesta inesperada para {path}: {response.status_code} {response.text}"
        )
    return response.json()


def _get_text(client: httpx.Client, path: str, expected_status: int) -> str:
    response = client.get(path)
    if response.status_code != expected_status:
        raise RuntimeError(
            f"Respuesta inesperada para {path}: {response.status_code} {response.text}"
        )
    return response.text


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[smoke] ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
