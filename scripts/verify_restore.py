from __future__ import annotations

import argparse
import asyncio
import os
import sys
from typing import Any

import asyncpg
import httpx


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verificacion minima de un restore PostgreSQL.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8001")
    parser.add_argument(
        "--postgres-dsn", default=os.getenv("VERIFY_POSTGRES_DSN") or os.getenv("POSTGRES_DSN")
    )
    parser.add_argument("--min-ingestion-rows", type=int, default=1)
    parser.add_argument("--min-normalized-rows", type=int, default=0)
    parser.add_argument("--page-size", type=int, default=1)
    parser.add_argument("--api-key", default=os.getenv("VERIFY_API_KEY") or os.getenv("API_KEY"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.postgres_dsn:
        raise RuntimeError(
            "Debes indicar --postgres-dsn o VERIFY_POSTGRES_DSN para verificar el restore."
        )

    asyncio.run(
        _verify_database(args.postgres_dsn, args.min_ingestion_rows, args.min_normalized_rows)
    )
    _verify_http(args.base_url.rstrip("/"), args.page_size, args.api_key)
    print("[restore-verify] verificacion completada")
    return 0


async def _verify_database(
    postgres_dsn: str, min_ingestion_rows: int, min_normalized_rows: int
) -> None:
    normalized_dsn = _normalize_asyncpg_dsn(postgres_dsn)
    connection = await asyncpg.connect(normalized_dsn)
    try:
        alembic_version = await connection.fetchval(
            "SELECT version_num FROM alembic_version LIMIT 1"
        )
        ingestion_count = int(await connection.fetchval("SELECT COUNT(*) FROM ingestion_raw"))
        normalized_count = int(
            await connection.fetchval("SELECT COUNT(*) FROM ine_series_normalized")
        )
    finally:
        await connection.close()

    if not alembic_version:
        raise RuntimeError("No se pudo leer alembic_version tras el restore.")
    if ingestion_count < min_ingestion_rows:
        raise RuntimeError(
            f"ingestion_raw tiene {ingestion_count} filas y se esperaban al menos {min_ingestion_rows}."
        )
    if normalized_count < min_normalized_rows:
        raise RuntimeError(
            f"ine_series_normalized tiene {normalized_count} filas y se esperaban al menos {min_normalized_rows}."
        )

    print(f"[restore-verify] alembic_version={alembic_version}")
    print(f"[restore-verify] ingestion_raw={ingestion_count}")
    print(f"[restore-verify] ine_series_normalized={normalized_count}")


def _verify_http(base_url: str, page_size: int, api_key: str | None) -> None:
    timeout = httpx.Timeout(10.0, connect=5.0)
    headers = {"X-API-Key": api_key} if api_key else None

    with httpx.Client(base_url=base_url, timeout=timeout, headers=headers) as client:
        health = _get_json(client, "/health")
        ready = _get_json(client, "/health/ready")
        series = _get_json(client, f"/ine/series?page=1&page_size={page_size}")

    if health.get("status") != "ok":
        raise RuntimeError(f"/health no es valido tras el restore: {health}")
    if ready.get("status") != "ok":
        raise RuntimeError(f"/health/ready no es valido tras el restore: {ready}")
    if "items" not in series or "total" not in series:
        raise RuntimeError(f"/ine/series no devolvio el contrato esperado: {series}")

    print("[restore-verify] /health OK")
    print("[restore-verify] /health/ready OK")
    print(f"[restore-verify] /ine/series total={series['total']}")


def _get_json(client: httpx.Client, path: str) -> dict[str, Any]:
    response = client.get(path)
    if response.status_code != 200:
        raise RuntimeError(
            f"Respuesta inesperada para {path}: {response.status_code} {response.text}"
        )
    return response.json()


def _normalize_asyncpg_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn.split("://", 1)[1]
    return dsn


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[restore-verify] ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
