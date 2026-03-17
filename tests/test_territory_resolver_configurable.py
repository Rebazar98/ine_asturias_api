"""Tests for D3 — TerritoryResolver (formerly AsturiasResolver) is configurable.

Validates:
- TerritoryResolver alias works identically to AsturiasResolver.
- geography_name / geography_code are stored on the instance.
- _detect_asturias_value uses geography_name, not hardcoded "asturias".
- Cataluña resolver (geography_name="Catalunya") does NOT match Asturias records.
- Asturias resolver (default) does NOT match Cataluña records.
- Cache key includes geography_code so two orgs don't share cached results.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.services.asturias_resolver import AsturiasResolver, TerritoryResolver


def _make_ine_values_payload(names: list[str]) -> list[dict]:
    return [{"Id": str(i + 1), "Nombre": name} for i, name in enumerate(names)]


def _make_resolver(geography_name: str = "Principado de Asturias", geography_code: str = "33"):
    ine_client = MagicMock()
    cache = MagicMock()
    cache.get = AsyncMock(return_value=None)
    cache.set = AsyncMock()
    return AsturiasResolver(
        ine_client=ine_client,
        cache=cache,
        geography_name=geography_name,
        geography_code=geography_code,
    )


class TestTerritoryResolverAlias:
    def test_territory_resolver_is_asturias_resolver(self):
        assert TerritoryResolver is AsturiasResolver

    def test_alias_instantiation_works(self):
        resolver = TerritoryResolver(
            ine_client=MagicMock(),
            cache=MagicMock(),
            geography_code="08",
            geography_name="Catalunya",
        )
        assert resolver.geography_code == "08"
        assert resolver.geography_name == "Catalunya"


class TestGeographyParameters:
    def test_default_geography_is_asturias(self):
        resolver = _make_resolver()
        assert resolver.geography_code == "33"
        assert resolver.geography_name == "Principado de Asturias"

    def test_custom_geography_stored(self):
        resolver = _make_resolver(geography_name="Catalunya", geography_code="08")
        assert resolver.geography_code == "08"
        assert resolver.geography_name == "Catalunya"

    def test_geography_key_term_extracted(self):
        resolver = _make_resolver(geography_name="Principado de Asturias")
        assert resolver._geography_key_term == "asturias"

    def test_catalonia_key_term_extracted(self):
        resolver = _make_resolver(geography_name="Catalunya", geography_code="08")
        assert resolver._geography_key_term == "catalunya"


class TestDetectTerritoryValue:
    def test_asturias_resolver_detects_asturias(self):
        resolver = _make_resolver(geography_name="Principado de Asturias", geography_code="33")
        payload = _make_ine_values_payload(
            ["Total Nacional", "Principado de Asturias", "Cataluña", "Madrid"]
        )
        result = resolver._detect_asturias_value(payload)
        assert result is not None
        assert result["name"] == "Principado de Asturias"
        assert result["id"] == "2"

    def test_asturias_resolver_does_not_match_catalonia(self):
        resolver = _make_resolver(geography_name="Principado de Asturias", geography_code="33")
        payload = _make_ine_values_payload(["Total Nacional", "Cataluña", "Madrid"])
        result = resolver._detect_asturias_value(payload)
        assert result is None

    def test_catalonia_resolver_detects_catalonia(self):
        resolver = _make_resolver(geography_name="Catalunya", geography_code="08")
        payload = _make_ine_values_payload(
            ["Total Nacional", "Principado de Asturias", "Catalunya", "Madrid"]
        )
        result = resolver._detect_asturias_value(payload)
        assert result is not None
        assert result["name"] == "Catalunya"

    def test_catalonia_resolver_does_not_match_asturias(self):
        resolver = _make_resolver(geography_name="Catalunya", geography_code="08")
        payload = _make_ine_values_payload(
            ["Total Nacional", "Principado de Asturias", "Madrid"]
        )
        result = resolver._detect_asturias_value(payload)
        assert result is None

    def test_exact_match_scores_higher(self):
        resolver = _make_resolver(geography_name="Principado de Asturias", geography_code="33")
        payload = _make_ine_values_payload(
            ["Provincia de Asturias", "Principado de Asturias"]
        )
        result = resolver._detect_asturias_value(payload)
        assert result is not None
        assert result["name"] == "Principado de Asturias"


class TestCacheKeyIncludesGeography:
    def test_cache_keys_differ_by_geography(self):
        asturias = _make_resolver(geography_code="33")
        catalonia = _make_resolver(geography_name="Catalunya", geography_code="08")

        key_asturias = asturias._cache_key("22", None, None)
        key_catalonia = catalonia._cache_key("22", None, None)

        assert key_asturias != key_catalonia
        assert "33" in key_asturias
        assert "08" in key_catalonia

    def test_cache_key_prefix_is_territory_resolution(self):
        resolver = _make_resolver()
        key = resolver._cache_key("22", None, None)
        assert key.startswith("territory_resolution:")
