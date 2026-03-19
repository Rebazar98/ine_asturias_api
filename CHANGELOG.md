# Changelog

Este proyecto sigue versionado semantico simple y un changelog acumulativo orientado a release candidates y versiones operativas.

El esquema actual es:

- `MAJOR.MINOR.PATCH` para versiones estables
- sufijo `-rcN` para release candidates previos a una version estable

## [Unreleased]

## [0.2.0-rc1] - 2026-03-19

### Added

- Fallback series-directo en `ingest_asturias_operation_via_series()`: cuando `TABLAS_OPERACION` devuelve lista vacia, la ingesta cambia automaticamente a `SERIES_OPERACION` + `DATOS_SERIE` con paginacion, descarga concurrente configurable (`MAX_CONCURRENT_SERIES_FETCHES`) y normalizacion por `normalize_serie_direct_payload_with_stats`.
- Nuevos metodos INE client: `get_operation_series(op_code, page)` y `get_serie_data(cod_serie, nult)`.
- Parametro `max_series` en `GET /ine/operation/{op_code}/asturias` para limitar el volumen de series procesadas.
- `name_based_fallback` en `AsturiasResolutionResult`: cuando `VALORES_VARIABLEOPERACION` devuelve lista vacia, la resolucion continua por nombre en lugar de abortar.
- Cobertura de tests de repositorios: `test_catalog_repository.py` (12 tests), `test_cartographic_qa_repository.py` (14 tests) y `test_series_repository.py` ampliado con 6 tests de `upsert_many`.
- Suite de tests para el fallback de series: 20 tests en `test_asturias_series_fallback.py`.

### Changed

- `ine_series_normalized` queda preparada para el cruce territorial futuro con una referencia interna opcional `territorial_unit_id`.
- La clave logica de deduplicacion actual no cambia y sigue basada en codigos/geografias externas del dominio INE.
- El backfill de `territorial_unit_id` queda diferido a una fase posterior de enriquecimiento territorial explicito.
- Se fijan convenciones espaciales base para PostGIS: SRID `4326`, limites `MULTIPOLYGON`, centroides `POINT` e indices `GIST` sobre `territorial_units`.
- Se anade cache geoespacial persistente con `geocode_cache`, `reverse_geocode_cache` y repositorio dedicado para lectura/upsert con expiracion.
- Se expone `GET /geocode` como primer endpoint geografico semantico con fallback a CartoCiudad y cache persistente.
- Se expone `GET /reverse_geocode` con la misma estrategia semantica y de cache persistente que `/geocode`.
- Se anade resolucion territorial interna en geocodificacion y reverse geocoding cuando existe match fiable contra `territorial_units`.
- Se abre la primera capa publica de lectura territorial con `GET /territorios/comunidades-autonomas`, `GET /territorios/provincias` y `GET /municipio/{codigo_ine}`.
- Se revalida staging y el proceso RC tras la integracion territorial/geografica, con Alembic en `0005_geocoding_cache`, smoke test correcto y restore verification correcto.
- El circuit breaker solo registra fallo en HTTP 5xx, no en 4xx.
- `AsturiasResolutionError` en el worker y en el router se captura y permite continuar la ingesta en lugar de abortar el job.

## [0.1.0-rc1] - 2026-03-12

Primera Release Candidate operativa aceptada del proyecto.

### Added

- Base backend FastAPI para datos INE con endpoints de salud, ingesta y consulta semantica.
- Persistencia raw en PostgreSQL y normalizacion de series en `ine_series_normalized`.
- Catalogo persistente de tablas INE con estados operativos.
- Jobs desacoplados con Redis y worker dedicado.
- Migraciones Alembic con bootstrap seguro para bases nuevas o heredadas.
- Nucleo territorial inicial con PostGIS preparado para evolucion futura.
- Smoke test, restore drill y security scan con Trivy.
- CI minima con lint, tests, migraciones y smoke test.
- Proceso formal de Release Candidate en `RELEASE_PROCESS.md`.
- Evidencia operativa registrada en `ACTA_RC1.md`.

### Security

- Runtime non-root para API y worker.
- Dependencias directas versionadas y `requirements.lock`.
- Imagenes base fijadas por tag y digest.
- Dependabot habilitado para pip, Docker y GitHub Actions.
