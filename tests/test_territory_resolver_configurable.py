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

from app.services.asturias_resolver import AsturiasResolver, AsturiasResolutionError, TerritoryResolver


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


class TestResolve:
    """Tests for the async resolve() entry point."""

    @pytest.mark.anyio
    async def test_resolve_returns_cached_result_without_api_call(self):
        """Cache hit: resolve() returns stored result and makes no API calls."""
        resolver = _make_resolver()
        resolver.cache.get = AsyncMock(
            return_value={
                "geo_variable_id": "115",
                "asturias_value_id": "33",
                "variable_name": "Comunidad autónoma",
                "asturias_label": "Principado de Asturias",
                "name_based_fallback": False,
            }
        )
        resolver.ine_client.get_operation_variables = AsyncMock()

        result = await resolver.resolve("22")

        assert result.geo_variable_id == "115"
        assert result.asturias_value_id == "33"
        resolver.ine_client.get_operation_variables.assert_not_called()

    @pytest.mark.anyio
    async def test_resolve_with_both_ids_provided_skips_api_calls(self):
        """When caller provides both IDs, no API calls are made."""
        resolver = _make_resolver()
        resolver.ine_client.get_operation_variables = AsyncMock()

        result = await resolver.resolve("22", geo_variable_id="115", asturias_value_id="33")

        assert result.geo_variable_id == "115"
        assert result.asturias_value_id == "33"
        resolver.ine_client.get_operation_variables.assert_not_called()

    @pytest.mark.anyio
    async def test_resolve_full_flow_detects_geo_and_asturias(self):
        """Full resolution: auto-detects geo variable (Id=3 CCAA) and Asturias value from INE."""
        resolver = _make_resolver()
        resolver.ine_client.get_operation_variables = AsyncMock(
            return_value=[
                {"Id": "200", "Nombre": "Indicador"},
                {"Id": "3", "Nombre": "Comunidades y Ciudades Autónomas"},
            ]
        )
        resolver.ine_client.get_variable_values = AsyncMock(
            return_value=[
                {"Id": "28", "Nombre": "Madrid"},
                {"Id": "33", "Nombre": "Principado de Asturias"},
            ]
        )

        result = await resolver.resolve("22")

        assert result.geo_variable_id == "3"
        assert result.asturias_value_id == "33"
        assert result.asturias_label == "Principado de Asturias"
        assert result.name_based_fallback is False

    @pytest.mark.anyio
    async def test_resolve_prefers_validated_geo_candidate_over_known_id_false_positive(self):
        resolver = _make_resolver()
        resolver.ine_client.get_operation_variables = AsyncMock(
            return_value=[
                {"Id": "3", "Nombre": "Tipo de dato"},
                {"Id": "115", "Nombre": "Comunidad autonoma"},
            ]
        )

        async def get_variable_values(op_code: str, variable_id: str):
            if variable_id == "3":
                return []
            if variable_id == "115":
                return [{"Id": "33", "Nombre": "Principado de Asturias"}]
            return []

        resolver.ine_client.get_variable_values = AsyncMock(side_effect=get_variable_values)

        result = await resolver.resolve("22")

        assert result.geo_variable_id == "115"
        assert result.asturias_value_id == "33"
        assert result.variable_name == "Comunidad autonoma"
        assert result.name_based_fallback is False

    @pytest.mark.anyio
    async def test_resolve_raises_when_no_geo_variable_found(self):
        """Raises AsturiasResolutionError when variables payload has no geographic keyword."""
        resolver = _make_resolver()
        resolver.ine_client.get_operation_variables = AsyncMock(
            return_value=[{"Id": "1", "Nombre": "Indicador economico"}]
        )

        with pytest.raises(AsturiasResolutionError):
            await resolver.resolve("22")

    @pytest.mark.anyio
    async def test_resolve_name_based_fallback_when_values_empty(self):
        """When VALORES_VARIABLEOPERACION returns [], resolution uses name_based_fallback."""
        resolver = _make_resolver()
        resolver.ine_client.get_operation_variables = AsyncMock(
            return_value=[{"Id": "3", "Nombre": "Comunidades y Ciudades Autónomas"}]
        )
        resolver.ine_client.get_variable_values = AsyncMock(return_value=[])

        result = await resolver.resolve("22")

        assert result.geo_variable_id == "3"
        assert result.asturias_value_id is None
        assert result.variable_name is None
        assert result.name_based_fallback is True


class TestInternalMethodEdgeCases:
    """Edge cases in _detect_geo_variable, _detect_asturias_value, _iter_records, _pick_first."""

    def test_detect_geo_variable_skips_records_without_id(self):
        """Records with no Id/id/Codigo/codigo key are ignored."""
        resolver = _make_resolver()
        payload = [
            {"Nombre": "Sin identificador"},
            {"Id": "3", "Nombre": "Comunidades y Ciudades Autónomas"},
        ]
        result = resolver._detect_geo_variable(payload)
        assert result is not None
        assert result["id"] == "3"

    def test_detect_asturias_value_skips_records_without_id(self):
        """Records with no Id key are ignored in _detect_asturias_value."""
        resolver = _make_resolver()
        payload = [
            {"Nombre": "Principado de Asturias"},  # matches but has no Id
            {"Id": "33", "Nombre": "Principado de Asturias"},
        ]
        result = resolver._detect_asturias_value(payload)
        assert result is not None
        assert result["id"] == "33"

    def test_iter_records_returns_list_for_dict_payload(self):
        """A dict payload is wrapped in a list by _iter_records."""
        resolver = _make_resolver()
        record = {"Id": "3", "Nombre": "Comunidades y Ciudades Autónomas"}
        result = resolver._iter_records(record)
        assert result == [record]

    def test_pick_first_returns_empty_string_when_no_key_present(self):
        """_pick_first returns '' when none of the requested keys exist."""
        result = AsturiasResolver._pick_first({}, ("Id", "id", "Codigo", "codigo"))
        assert result == ""


class TestKnownGeoVariableIds:
    """Fast path: _detect_geo_variable matches by exact known INE variable ID."""

    def test_known_id_matches_even_without_keyword_in_name(self):
        """Id=3 is returned even if Nombre contains no geographic keywords."""
        resolver = _make_resolver()
        payload = [
            {"Id": "200", "Nombre": "Indicador economico"},
            {"Id": "3", "Nombre": "CCAA"},  # minimal name — no "comunidad" etc.
        ]
        result = resolver._detect_geo_variable(payload)
        assert result is not None
        assert result["id"] == "3"

    def test_known_id_wins_over_high_keyword_score(self):
        """Id=3 is returned ahead of a variable that scores very high by keywords."""
        resolver = _make_resolver()
        # "geo comunidad autonoma territorial" → score=10+10+9+7=36, but Id=3 is known
        payload = [
            {"Id": "99", "Nombre": "Variable geo comunidad autonoma territorial"},
            {"Id": "3", "Nombre": "CCAA"},
        ]
        result = resolver._detect_geo_variable(payload)
        assert result["id"] == "3"

    def test_known_municipios_id_matches(self):
        """Id=13 (Municipios) is recognized via fast path."""
        resolver = _make_resolver()
        payload = [{"Id": "13", "Nombre": "Municipios"}]
        result = resolver._detect_geo_variable(payload)
        assert result is not None
        assert result["id"] == "13"

    def test_unknown_id_with_keyword_still_resolves(self):
        """Non-known ID with geographic keyword still resolves via slow path."""
        resolver = _make_resolver()
        payload = [{"Id": "999", "Nombre": "Variable comunidad autonoma"}]
        result = resolver._detect_geo_variable(payload)
        assert result is not None
        assert result["id"] == "999"
