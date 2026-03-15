import asyncio

import pytest
from pydantic import ValidationError

from app.repositories.territorial_export_artifacts import (
    TerritorialExportArtifactRepository,
    build_export_key,
    normalize_export_provider_keys,
)
from app.schemas import TerritorialExportRequest


def test_build_export_key_is_stable_for_semantically_equal_provider_sets():
    left = build_export_key(
        unit_level=" municipality ",
        code_value=" 33044 ",
        artifact_format="ZIP",
        include_providers=["analytics", "territorial", "ine", "territorial"],
    )
    right = build_export_key(
        unit_level="municipality",
        code_value="33044",
        artifact_format="zip",
        include_providers=["territorial", "ine", "analytics"],
    )

    assert left == right
    assert normalize_export_provider_keys(["analytics", "territorial", "ine", "territorial"]) == [
        "territorial",
        "ine",
        "analytics",
    ]


def test_normalize_export_provider_keys_keeps_catastro_opt_in():
    assert normalize_export_provider_keys(None) == ["territorial", "ine", "analytics"]
    assert normalize_export_provider_keys([]) == []
    assert normalize_export_provider_keys(["catastro"]) == ["catastro"]


def test_territorial_export_request_keeps_catastro_opt_in_by_default():
    request = TerritorialExportRequest(unit_level="municipality", code_value="33044")
    assert request.include_providers == ["territorial", "ine", "analytics"]

    catastro_request = TerritorialExportRequest(
        unit_level="municipality",
        code_value="33044",
        include_providers=["catastro"],
    )
    assert catastro_request.include_providers == ["catastro"]

    with pytest.raises(
        ValidationError,
        match="include_providers must include at least one supported provider",
    ):
        TerritorialExportRequest(
            unit_level="municipality",
            code_value="33044",
            include_providers=[],
        )


def test_get_fresh_artifact_returns_none_when_database_disabled():
    repository = TerritorialExportArtifactRepository(session=None)

    result = asyncio.run(
        repository.get_fresh_artifact(
            unit_level="municipality",
            code_value="33044",
            artifact_format="zip",
            include_providers=["territorial", "ine", "analytics"],
        )
    )

    assert result is None
