# ruff: noqa: E402
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import httpx

from app.db import dispose_db, init_db, session_scope
from app.repositories.ingestion import IngestionRepository
from app.repositories.territorial import TerritorialRepository
from app.services.ign_admin_boundaries import (
    IGNAdministrativeBoundariesLoaderService,
    IGN_ADMIN_SCOPE_ASTURIAS_CODE,
)
from app.services.ign_admin_client import (
    IGNAdministrativeSnapshotClient,
    load_ign_admin_feature_collection_from_path,
)
from app.settings import get_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Carga limites administrativos IGN/CNIG sobre el modelo territorial interno."
    )
    parser.add_argument("--input-path", default="")
    parser.add_argument("--snapshot-url", default="")
    parser.add_argument("--dataset-version", default="")
    parser.add_argument("--country-code", default="ES")
    parser.add_argument(
        "--autonomous-community-code",
        default=IGN_ADMIN_SCOPE_ASTURIAS_CODE,
        help="Codigo INE de la comunidad autonoma a cargar en v1 (default: Asturias=03).",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Imprime el resultado final en JSON indentado.",
    )
    return parser.parse_args()


async def async_main() -> int:
    args = parse_args()
    settings = get_settings()
    if not settings.postgres_dsn:
        raise RuntimeError("POSTGRES_DSN debe estar configurado para cargar limites IGN.")

    if args.input_path.strip():
        payload = load_ign_admin_feature_collection_from_path(args.input_path.strip())
        source_path = str(Path(args.input_path.strip()).resolve())
    else:
        snapshot_url = (args.snapshot_url or settings.ign_admin_snapshot_url or "").strip()
        if not snapshot_url:
            raise RuntimeError(
                "Debes indicar --input-path o configurar IGN_ADMIN_SNAPSHOT_URL/--snapshot-url."
            )
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(
                settings.http_timeout_seconds,
                connect=min(settings.http_timeout_seconds, 5.0),
            )
        ) as http_client:
            client = IGNAdministrativeSnapshotClient(http_client=http_client, settings=settings)
            payload = await client.fetch_snapshot(snapshot_url=snapshot_url)
        source_path = snapshot_url

    init_db(settings)
    try:
        async with session_scope() as session:
            if session is None:
                raise RuntimeError("No se pudo abrir una sesion de base de datos.")

            loader = IGNAdministrativeBoundariesLoaderService(
                ingestion_repo=IngestionRepository(session=session),
                territorial_repo=TerritorialRepository(session=session),
            )
            result = await loader.load_snapshot(
                payload=payload,
                source_path=source_path,
                dataset_version=(args.dataset_version or "").strip() or None,
                country_code=str(args.country_code).strip() or "ES",
                autonomous_community_code=str(args.autonomous_community_code).strip()
                or IGN_ADMIN_SCOPE_ASTURIAS_CODE,
            )
    finally:
        await dispose_db()

    if args.pretty:
        print(json.dumps(result, indent=2, ensure_ascii=True, default=str))
    else:
        print(
            "[ign-admin-load] source={source} dataset_version={dataset_version} "
            "features_selected={selected} features_upserted={upserted} "
            "features_rejected={rejected} raw_records_saved={raw_records_saved}".format(
                source=result["source"],
                dataset_version=result["dataset_version"],
                selected=result["features_selected"],
                upserted=result["features_upserted"],
                rejected=result["features_rejected"],
                raw_records_saved=result["raw_records_saved"],
            )
        )
    return 0


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[ign-admin-load] ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
