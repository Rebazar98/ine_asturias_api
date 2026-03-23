from __future__ import annotations

import asyncio
from datetime import datetime, UTC
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models import TerritorialExportArtifact
from app.repositories.territorial_export_artifacts import TerritorialExportArtifactRepository
from tests.integration.postgres import require_integration_postgres


@pytest.mark.integration
def test_territorial_export_artifact_repository_roundtrip_with_postgres():
    postgres_dsn = require_integration_postgres()

    async def scenario() -> None:
        engine = create_async_engine(postgres_dsn, future=True)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        code_value = f"33{uuid4().hex[:6]}"

        try:
            async with session_factory() as session:
                repository = TerritorialExportArtifactRepository(session=session)
                stored = await repository.upsert_artifact(
                    territorial_unit_id=None,
                    unit_level="municipality",
                    code_value=code_value,
                    artifact_format="zip",
                    content_type="application/zip",
                    filename=f"territorial_export_municipality_{code_value}.zip",
                    payload_bytes=b"zip-bytes",
                    ttl_seconds=3600,
                    include_providers=["territorial", "ine"],
                    metadata={
                        "source": "internal.export.territorial_bundle",
                        "territorial_context": {"canonical_code": code_value},
                    },
                    now=datetime(2026, 3, 15, 13, 0, tzinfo=UTC),
                )
                assert stored is not None

                fresh = await repository.get_fresh_artifact(
                    unit_level="municipality",
                    code_value=code_value,
                    artifact_format="zip",
                    include_providers=["ine", "territorial"],
                    now=datetime(2026, 3, 15, 13, 1, tzinfo=UTC),
                )
                assert fresh is not None
                assert fresh["export_id"] == stored["export_id"]
                assert fresh["payload_bytes"] == b"zip-bytes"

                by_id = await repository.get_by_export_id(stored["export_id"])
                assert by_id is not None
                assert by_id["filename"] == f"territorial_export_municipality_{code_value}.zip"

        finally:
            async with session_factory() as session:
                await session.execute(
                    delete(TerritorialExportArtifact).where(
                        TerritorialExportArtifact.code_value == code_value
                    )
                )
                await session.commit()
            await engine.dispose()

    asyncio.run(scenario())
