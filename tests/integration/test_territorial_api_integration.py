from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.dependencies import get_cartociudad_client_service
from app.main import app
from app.models import (
    GeocodeCache,
    IngestionRaw,
    ReverseGeocodeCache,
    TerritorialUnit,
    TerritorialUnitAlias,
    TerritorialUnitCode,
)
from app.repositories.geocoding import (
    GEOCODING_PROVIDER_CARTOCIUDAD,
    GeocodingCacheRepository,
    build_reverse_geocode_coordinate_key,
    normalize_geocode_query,
)
from app.repositories.territorial import (
    INE_AUTONOMOUS_COMMUNITY_CODE_TYPE,
    INE_MUNICIPALITY_CODE_TYPE,
    INE_PROVINCE_CODE_TYPE,
    INE_TERRITORIAL_SOURCE_SYSTEM,
    TERRITORIAL_ALIAS_TYPE_ALTERNATE_NAME,
    TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
    TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
    TERRITORIAL_UNIT_LEVEL_PROVINCE,
    normalize_territorial_name,
)
from app.settings import get_settings
from tests.integration.postgres import require_integration_postgres


class DummyCartoCiudadClientService:
    def __init__(self, payload):
        self.payload = payload
        self.calls: list[str] = []
        self.reverse_calls: list[tuple[float, float]] = []

    async def geocode(self, query: str):
        self.calls.append(query)
        return self.payload

    async def reverse_geocode(self, lat: float, lon: float):
        self.reverse_calls.append((lat, lon))
        return self.payload


@pytest.mark.integration
def test_geocode_endpoint_uses_persistent_cache_and_resolves_territorial_unit(monkeypatch):
    postgres_dsn = require_integration_postgres()

    async def scenario() -> None:
        engine = create_async_engine(postgres_dsn, future=True)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = uuid4().hex[:8]
        canonical_name = f"Oviedo Integration {suffix}"
        query = f"{canonical_name} geocode"
        municipality_code = f"33044{suffix[:3]}"
        unit_id: int | None = None
        normalized_query = normalize_geocode_query(query)

        try:
            async with session_factory() as session:
                unit = TerritorialUnit(
                    parent_id=None,
                    unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
                    canonical_name=canonical_name,
                    normalized_name=normalize_territorial_name(canonical_name),
                    display_name=canonical_name,
                    country_code="ES",
                    is_active=True,
                    attributes_json={},
                )
                session.add(unit)
                await session.flush()
                unit_id = unit.id
                session.add(
                    TerritorialUnitCode(
                        territorial_unit_id=unit.id,
                        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
                        code_type=INE_MUNICIPALITY_CODE_TYPE,
                        code_value=municipality_code,
                        is_primary=True,
                    )
                )
                await session.commit()

            async with session_factory() as session:
                geocoding_repo = GeocodingCacheRepository(session=session)
                await geocoding_repo.upsert_geocode_cache(
                    provider=GEOCODING_PROVIDER_CARTOCIUDAD,
                    query=query,
                    payload=[
                        {
                            "id": f"geo-{suffix}",
                            "type": "municipality",
                            "label": canonical_name,
                            "lat": 43.3614,
                            "lon": -5.8494,
                            "codigoMunicipio": municipality_code,
                            "municipio": canonical_name,
                        }
                    ],
                    ttl_seconds=3600,
                    metadata={"scope": "integration"},
                )

            dummy_client = DummyCartoCiudadClientService(payload=[])
            monkeypatch.setenv("APP_ENV", "test")
            monkeypatch.setenv("POSTGRES_DSN", postgres_dsn)
            monkeypatch.setenv("REDIS_URL", "")
            monkeypatch.setenv("API_KEY", "")
            get_settings.cache_clear()
            app.dependency_overrides[get_cartociudad_client_service] = lambda: dummy_client

            with TestClient(app) as client:
                response = client.get(f"/geocode?query={query}")

            assert response.status_code == 200
            payload = response.json()
            assert payload["cached"] is True
            assert payload["result"]["territorial_resolution"] == {
                "territorial_unit_id": unit_id,
                "matched_by": "code",
                "canonical_name": canonical_name,
                "canonical_code": municipality_code,
                "source_system": "ine",
                "unit_level": "municipality",
            }
            assert dummy_client.calls == []

        finally:
            app.dependency_overrides.clear()
            get_settings.cache_clear()
            async with session_factory() as session:
                await session.execute(
                    delete(GeocodeCache).where(
                        GeocodeCache.provider == GEOCODING_PROVIDER_CARTOCIUDAD,
                        GeocodeCache.normalized_query == normalized_query,
                    )
                )
                if unit_id is not None:
                    await session.execute(
                        delete(TerritorialUnitCode).where(
                            TerritorialUnitCode.territorial_unit_id == unit_id
                        )
                    )
                    await session.execute(
                        delete(TerritorialUnit).where(TerritorialUnit.id == unit_id)
                    )
                await session.commit()
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.integration
def test_reverse_geocode_endpoint_persists_cache_and_resolves_territorial_unit(monkeypatch):
    postgres_dsn = require_integration_postgres()

    async def scenario() -> None:
        engine = create_async_engine(postgres_dsn, future=True)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = uuid4().hex[:8]
        canonical_name = f"Asturias Integration {suffix}"
        province_code = f"33{suffix[:2]}"
        unit_id: int | None = None
        coordinate_key = build_reverse_geocode_coordinate_key(43.3614, -5.8494)
        raw_record_id: int | None = None

        try:
            async with session_factory() as session:
                unit = TerritorialUnit(
                    parent_id=None,
                    unit_level=TERRITORIAL_UNIT_LEVEL_PROVINCE,
                    canonical_name=canonical_name,
                    normalized_name=normalize_territorial_name(canonical_name),
                    display_name=canonical_name,
                    country_code="ES",
                    is_active=True,
                    attributes_json={},
                )
                session.add(unit)
                await session.flush()
                unit_id = unit.id
                session.add(
                    TerritorialUnitCode(
                        territorial_unit_id=unit.id,
                        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
                        code_type=INE_PROVINCE_CODE_TYPE,
                        code_value=province_code,
                        is_primary=True,
                    )
                )
                await session.commit()

            dummy_client = DummyCartoCiudadClientService(
                payload={
                    "id": f"reverse-{suffix}",
                    "type": "address",
                    "label": canonical_name,
                    "lat": 43.3614,
                    "lon": -5.8494,
                    "codigoProvincia": province_code,
                    "provincia": canonical_name,
                }
            )
            monkeypatch.setenv("APP_ENV", "test")
            monkeypatch.setenv("POSTGRES_DSN", postgres_dsn)
            monkeypatch.setenv("REDIS_URL", "")
            monkeypatch.setenv("API_KEY", "")
            get_settings.cache_clear()
            app.dependency_overrides[get_cartociudad_client_service] = lambda: dummy_client

            with TestClient(app) as client:
                response = client.get("/reverse_geocode?lat=43.3614&lon=-5.8494")

            assert response.status_code == 200
            payload = response.json()
            assert payload["cached"] is False
            assert payload["result"]["territorial_resolution"] == {
                "territorial_unit_id": unit_id,
                "matched_by": "code",
                "canonical_name": canonical_name,
                "canonical_code": province_code,
                "source_system": "ine",
                "unit_level": "province",
            }
            assert dummy_client.reverse_calls == [(43.3614, -5.8494)]

            async with session_factory() as session:
                geocoding_repo = GeocodingCacheRepository(session=session)
                cached = await geocoding_repo.get_reverse_geocode_cache(
                    provider=GEOCODING_PROVIDER_CARTOCIUDAD,
                    lat=43.3614,
                    lon=-5.8494,
                )
                assert cached is not None
                assert cached["payload"]["codigoProvincia"] == province_code
                raw_rows = (
                    (
                        await session.execute(
                            select(IngestionRaw).where(
                                IngestionRaw.source_type == "cartociudad_reverse_geocode"
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                assert len(raw_rows) == 1
                raw_record = raw_rows[0]
                raw_record_id = raw_record.id
                source_type = raw_record.source_type
                request_path = raw_record.request_path
                request_params = raw_record.request_params
                assert source_type == "cartociudad_reverse_geocode"
                assert request_path == "/reverseGeocode"
                assert request_params == {
                    "coordinate_hint": "43.3614,-5.8494",
                    "coordinate_precision": 4,
                    "request_kind": "coordinates",
                    "provider_contract_exposed": False,
                }

        finally:
            app.dependency_overrides.clear()
            get_settings.cache_clear()
            async with session_factory() as session:
                await session.execute(
                    delete(ReverseGeocodeCache).where(
                        ReverseGeocodeCache.provider == GEOCODING_PROVIDER_CARTOCIUDAD,
                        ReverseGeocodeCache.coordinate_key == coordinate_key,
                    )
                )
                if raw_record_id is not None:
                    await session.execute(
                        delete(IngestionRaw).where(IngestionRaw.id == raw_record_id)
                    )
                if unit_id is not None:
                    await session.execute(
                        delete(TerritorialUnitCode).where(
                            TerritorialUnitCode.territorial_unit_id == unit_id
                        )
                    )
                    await session.execute(
                        delete(TerritorialUnit).where(TerritorialUnit.id == unit_id)
                    )
                await session.commit()
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.integration
def test_municipio_endpoint_returns_detail_from_internal_model(monkeypatch):
    postgres_dsn = require_integration_postgres()

    async def scenario() -> None:
        engine = create_async_engine(postgres_dsn, future=True)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = uuid4().hex[:8]
        municipality_code = f"33044{suffix[:3]}"
        canonical_name = f"Oviedo API {suffix}"
        alias_name = f"Uvieu API {suffix}"
        unit_id: int | None = None
        community_id: int | None = None

        try:
            async with session_factory() as session:
                community = TerritorialUnit(
                    parent_id=None,
                    unit_level=TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
                    canonical_name=f"Asturias API {suffix}",
                    normalized_name=normalize_territorial_name(f"Asturias API {suffix}"),
                    display_name=f"Asturias API {suffix}",
                    country_code="ES",
                    is_active=True,
                    attributes_json={},
                )
                session.add(community)
                await session.flush()
                community_id = community.id
                session.add(
                    TerritorialUnitCode(
                        territorial_unit_id=community.id,
                        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
                        code_type=INE_AUTONOMOUS_COMMUNITY_CODE_TYPE,
                        code_value=f"03{suffix[:2]}",
                        is_primary=True,
                    )
                )

                unit = TerritorialUnit(
                    parent_id=community.id,
                    unit_level=TERRITORIAL_UNIT_LEVEL_MUNICIPALITY,
                    canonical_name=canonical_name,
                    normalized_name=normalize_territorial_name(canonical_name),
                    display_name=canonical_name,
                    country_code="ES",
                    is_active=True,
                    attributes_json={"population_scope": "municipal"},
                )
                session.add(unit)
                await session.flush()
                unit_id = unit.id
                session.add(
                    TerritorialUnitCode(
                        territorial_unit_id=unit.id,
                        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
                        code_type=INE_MUNICIPALITY_CODE_TYPE,
                        code_value=municipality_code,
                        is_primary=True,
                    )
                )
                session.add(
                    TerritorialUnitAlias(
                        territorial_unit_id=unit.id,
                        source_system="internal",
                        alias=alias_name,
                        normalized_alias=normalize_territorial_name(alias_name),
                        alias_type=TERRITORIAL_ALIAS_TYPE_ALTERNATE_NAME,
                    )
                )
                await session.commit()

            monkeypatch.setenv("APP_ENV", "test")
            monkeypatch.setenv("POSTGRES_DSN", postgres_dsn)
            monkeypatch.setenv("REDIS_URL", "")
            monkeypatch.setenv("API_KEY", "")
            get_settings.cache_clear()

            with TestClient(app) as client:
                response = client.get(f"/municipio/{municipality_code}")

            assert response.status_code == 200
            payload = response.json()
            assert payload["canonical_name"] == canonical_name
            assert payload["canonical_code"]["code_value"] == municipality_code
            assert payload["aliases"][0]["alias"] == alias_name
            assert payload["attributes"] == {"population_scope": "municipal"}

        finally:
            app.dependency_overrides.clear()
            get_settings.cache_clear()
            async with session_factory() as session:
                if unit_id is not None:
                    await session.execute(
                        delete(TerritorialUnitAlias).where(
                            TerritorialUnitAlias.territorial_unit_id == unit_id
                        )
                    )
                    await session.execute(
                        delete(TerritorialUnitCode).where(
                            TerritorialUnitCode.territorial_unit_id == unit_id
                        )
                    )
                    await session.execute(
                        delete(TerritorialUnit).where(TerritorialUnit.id == unit_id)
                    )
                if community_id is not None:
                    await session.execute(
                        delete(TerritorialUnitCode).where(
                            TerritorialUnitCode.territorial_unit_id == community_id
                        )
                    )
                    await session.execute(
                        delete(TerritorialUnit).where(TerritorialUnit.id == community_id)
                    )
                await session.commit()
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.integration
def test_territorial_listing_endpoints_return_real_db_results(monkeypatch):
    postgres_dsn = require_integration_postgres()

    async def scenario() -> None:
        engine = create_async_engine(postgres_dsn, future=True)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = uuid4().hex[:8]
        community_code = f"03{suffix[:2]}"
        province_code = f"33{suffix[:2]}"
        community_id: int | None = None
        province_id: int | None = None

        try:
            async with session_factory() as session:
                community = TerritorialUnit(
                    parent_id=None,
                    unit_level=TERRITORIAL_UNIT_LEVEL_AUTONOMOUS_COMMUNITY,
                    canonical_name=f"Asturias Listing {suffix}",
                    normalized_name=normalize_territorial_name(f"Asturias Listing {suffix}"),
                    display_name=f"Asturias Listing {suffix}",
                    country_code="ES",
                    is_active=True,
                    attributes_json={},
                )
                session.add(community)
                await session.flush()
                community_id = community.id
                session.add(
                    TerritorialUnitCode(
                        territorial_unit_id=community.id,
                        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
                        code_type=INE_AUTONOMOUS_COMMUNITY_CODE_TYPE,
                        code_value=community_code,
                        is_primary=True,
                    )
                )

                province = TerritorialUnit(
                    parent_id=community.id,
                    unit_level=TERRITORIAL_UNIT_LEVEL_PROVINCE,
                    canonical_name=f"Asturias Province {suffix}",
                    normalized_name=normalize_territorial_name(f"Asturias Province {suffix}"),
                    display_name=f"Asturias Province {suffix}",
                    country_code="ES",
                    is_active=True,
                    attributes_json={},
                )
                session.add(province)
                await session.flush()
                province_id = province.id
                session.add(
                    TerritorialUnitCode(
                        territorial_unit_id=province.id,
                        source_system=INE_TERRITORIAL_SOURCE_SYSTEM,
                        code_type=INE_PROVINCE_CODE_TYPE,
                        code_value=province_code,
                        is_primary=True,
                    )
                )
                await session.commit()

            monkeypatch.setenv("APP_ENV", "test")
            monkeypatch.setenv("POSTGRES_DSN", postgres_dsn)
            monkeypatch.setenv("REDIS_URL", "")
            monkeypatch.setenv("API_KEY", "")
            get_settings.cache_clear()

            with TestClient(app) as client:
                communities_response = client.get(
                    "/territorios/comunidades-autonomas?page=1&page_size=10"
                )
                provinces_response = client.get(
                    f"/territorios/provincias?autonomous_community_code={community_code}"
                )

            assert communities_response.status_code == 200
            communities_payload = communities_response.json()
            assert communities_payload["total"] >= 1
            assert any(
                item["canonical_code"]["code_value"] == community_code
                for item in communities_payload["items"]
            )

            assert provinces_response.status_code == 200
            provinces_payload = provinces_response.json()
            assert provinces_payload["filters"]["parent_id"] == community_id
            assert any(
                item["canonical_code"]["code_value"] == province_code
                for item in provinces_payload["items"]
            )

        finally:
            app.dependency_overrides.clear()
            get_settings.cache_clear()
            async with session_factory() as session:
                if province_id is not None:
                    await session.execute(
                        delete(TerritorialUnitCode).where(
                            TerritorialUnitCode.territorial_unit_id == province_id
                        )
                    )
                    await session.execute(
                        delete(TerritorialUnit).where(TerritorialUnit.id == province_id)
                    )
                if community_id is not None:
                    await session.execute(
                        delete(TerritorialUnitCode).where(
                            TerritorialUnitCode.territorial_unit_id == community_id
                        )
                    )
                    await session.execute(
                        delete(TerritorialUnit).where(TerritorialUnit.id == community_id)
                    )
                await session.commit()
            await engine.dispose()

    asyncio.run(scenario())
