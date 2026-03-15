import asyncio

from app.repositories.catastro_cache import (
    CATASTRO_PROVIDER_FAMILY_URBANO,
    CatastroMunicipalityAggregateCacheRepository,
)


def test_catastro_cache_repository_returns_none_when_database_disabled():
    repository = CatastroMunicipalityAggregateCacheRepository(session=None)

    result = asyncio.run(
        repository.get_fresh_payload(
            provider_family=CATASTRO_PROVIDER_FAMILY_URBANO,
            municipality_code="33044",
            reference_year="2025",
        )
    )

    assert result is None
