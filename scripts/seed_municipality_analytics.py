# ruff: noqa: E402
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db import dispose_db, init_db, session_scope
from app.services.territorial_seed import (
    default_seed_municipality_name,
    ensure_municipality_analytics_seed,
)
from app.settings import get_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crea una semilla minima para validar analytics municipales en local."
    )
    parser.add_argument("--municipality-code", default="33044")
    parser.add_argument("--municipality-name", default="")
    return parser.parse_args()


async def async_main() -> int:
    args = parse_args()
    settings = get_settings()
    if not settings.postgres_dsn:
        raise RuntimeError("POSTGRES_DSN debe estar configurado para sembrar datos territoriales.")

    init_db(settings)
    try:
        async with session_scope() as session:
            if session is None:
                raise RuntimeError("No se pudo abrir una sesion de base de datos.")

            result = await ensure_municipality_analytics_seed(
                session,
                municipality_code=str(args.municipality_code),
                municipality_name=(
                    args.municipality_name.strip()
                    or default_seed_municipality_name(str(args.municipality_code))
                ),
            )
    finally:
        await dispose_db()

    print(
        "[territorial-seed] municipality_code={code} municipality_name={name} "
        "territorial_unit_id={unit_id} created_unit={created} normalized_rows_upserted={rows}".format(
            code=result.municipality_code,
            name=result.municipality_name,
            unit_id=result.territorial_unit_id,
            created=result.created_unit,
            rows=result.normalized_rows_upserted,
        )
    )
    return 0


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[territorial-seed] ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
