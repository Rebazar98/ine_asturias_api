# ruff: noqa: E402
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db import dispose_db, init_db, session_scope
from app.repositories.series import SeriesRepository
from app.services.geography_aliases import (
    build_configured_geography_alias_codes,
    build_configured_geography_alias_names,
)
from app.settings import get_settings


async def _run() -> int:
    settings = get_settings()
    init_db(settings)

    try:
        async with session_scope() as session:
            if session is None:
                raise RuntimeError("POSTGRES_DSN no configurado; no se puede reconciliar.")

            repository = SeriesRepository(session=session)
            result = await repository.reconcile_configured_geography_duplicates(
                canonical_geography_name=settings.default_geography_name,
                canonical_geography_code=settings.default_geography_code,
                alias_geography_names=sorted(
                    build_configured_geography_alias_names(settings.default_geography_name)
                ),
                alias_geography_codes=sorted(
                    build_configured_geography_alias_codes(settings.default_geography_code)
                ),
            )
    finally:
        await dispose_db()

    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
