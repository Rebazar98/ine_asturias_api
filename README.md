# ine_asturias_api

`ine_asturias_api` es un backend FastAPI para ingesta, normalizacion y publicacion de datos del INE con foco inicial en Asturias. La base actual ya incorpora PostgreSQL, PostGIS, Alembic, Redis, jobs desacoplados, worker dedicado, catalogo persistente de tablas, observabilidad minima, CartoCiudad bajo demanda y una carga administrativa IGN/CNIG versionable para enriquecer el modelo territorial interno.

## Documentacion

Documentacion disponible en la raiz del proyecto:

- `README.md`: guia funcional y operativa del backend actual.
- `DOCUMENTACION_EVOLUCION_PROYECTO.txt`: registro historico y memoria tecnica del proyecto.
- `PLAN_TECNICO_PLATAFORMA_DATOS_TERRITORIALES.md`: roadmap de evolucion hacia plataforma territorial multi-fuente.
- `PLAN_FASE_CARTOCIUDAD_IGN_ON_DEMAND.md`: plan ejecutado para consolidar CartoCiudad como segunda fuente oficial bajo demanda.
- `PLAN_FASE_IGN_ADMINISTRATIVO_DIRECTO.md`: plan ejecutado para incorporar IGN/CNIG administrativo directo mediante snapshots versionables.
- `MONITORING_SLOS.md`: objetivos operativos, SLIs y reglas base de alertado para produccion.
- `FASE2_STAGING_OPERATIVO_BACKLOG.md`: contexto de trabajo y backlog ejecutable para la fase de staging operativo real.
- `FASE3_MODELO_TERRITORIAL_BACKLOG.md`: backlog ejecutable para la fase de modelo territorial operativo y base geoespacial interna.
- `FASE4_API_TERRITORIAL_BACKLOG.md`: backlog ejecutable para la fase de API territorial unificada y primera integracion geografica.
- `FASE5_AUTOMATIZACION_ANALITICA_BACKLOG.md`: backlog ejecutable para la fase de automatizacion, analitica operativa y consumo productivo.
- `AGENTS.md`: guia de arquitectura y reglas operativas para agentes y colaboradores tecnicos.
- `RELEASE_PROCESS.md`: proceso operativo para preparar y validar un release candidate.
- `DEPLOYMENT_STAGING.md`: guia operativa para desplegar y verificar un entorno de staging reproducible.
- `CHANGELOG.md`: historial de versiones y release candidates aceptados.
- `SECURITY_EXCEPTION_TEMPLATE.md`: plantilla minima para registrar excepciones temporales de seguridad.
- `ACTA_RC1.md`: acta del primer Release Candidate real y siguiente bloque recomendado de evolucion.

## Arquitectura actual

- `app/api`: routers FastAPI.
- `app/services`: cliente INE, CartoCiudad, adapter IGN administrativo, resolucion de Asturias, normalizacion y orquestacion de ingesta.
- `app/repositories`: persistencia raw, normalizada, catalogo, base territorial, cache geoespacial persistente y artefactos de exportacion reutilizables.
- `app/core`: logging JSON, cache TTL, jobs, metricas y utilidades de Redis.
- `app/models.py`: modelos SQLAlchemy, incluyendo el nucleo territorial preparado para PostGIS.
- `alembic/`: migraciones versionadas.
- `scripts/bootstrap_alembic.py`: bootstrap seguro de migraciones para bases nuevas o heredadas.
- `app/worker.py`: worker `arq` para jobs pesados y exportacion de metricas de aplicacion.

## Servicios Docker

`docker compose up --build` levanta:

- API FastAPI en `http://127.0.0.1:8001`
- PostgreSQL + PostGIS en `localhost:5433`
- Redis en `localhost:6379`
- servicio `migrate` para bootstrap seguro de Alembic y `upgrade head`
- worker dedicado para jobs asincronos
- `api`, `worker` y `migrate` se ejecutan como usuario non-root (`uid/gid 10001`) con `no-new-privileges` y sin capacidades Linux adicionales.

### Recuperacion local de PostgreSQL

Si `docker compose run --rm migrate` o `pytest` de integracion fallan con `password authentication failed for user "postgres"`, normalmente hay deriva entre las credenciales actuales de `.env` y el volumen persistente `postgres_data` creado con una password anterior.

Recuperacion recomendada en local:

```powershell
.\scripts\reset_local_postgres_volume.ps1 -Force
```

Si trabajas con un env file o project name distinto:

```powershell
.\scripts\reset_local_postgres_volume.ps1 -EnvFile .env.local -ProjectName playground -Force
```

Este script SOLO esta pensado para desarrollo local. Elimina la base persistente local y la recrea desde cero con las credenciales actuales.

## Variables de entorno

| Variable | Descripcion | Ejemplo |
|---|---|---|
| `APP_NAME` | Nombre logico de la app | `ine_asturias_api` |
| `APP_VERSION` | Version operativa actual de la aplicacion | `0.1.0-rc1` |
| `APP_ENV` | Entorno de ejecucion | `local` |
| `INE_BASE_URL` | Base URL del INE | `https://servicios.ine.es/wstempus/js/ES` |
| `CARTOCIUDAD_BASE_URL` | Base URL del provider geografico CartoCiudad | `https://www.cartociudad.es/geocoder/api/geocoder` |
| `IGN_ADMIN_SNAPSHOT_URL` | Snapshot versionable IGN/CNIG para carga administrativa directa | `https://.../recintos_municipales.zip` |
| `CATASTRO_BASE_URL` | Base URL del Portal del Catastro para agregados municipales urbanos | `https://www.catastro.hacienda.gob.es` |
| `CATASTRO_TIMEOUT_SECONDS` | Timeout individual para llamadas Catastro | `20` |
| `CATASTRO_CACHE_TTL_SECONDS` | TTL de cache persistente para agregados municipales Catastro | `604800` |
| `CATASTRO_URBANO_YEAR` | Ano de referencia fijo de Catastro Urbano; vacio = auto-discovery | `` |
| `HTTP_TIMEOUT_SECONDS` | Timeout HTTP hacia el proveedor | `15` |
| `PROVIDER_TOTAL_TIMEOUT_SECONDS` | Presupuesto total por llamada upstream incluyendo reintentos | `30` |
| `HTTP_RETRY_MAX_ATTEMPTS` | Numero maximo de intentos HTTP por llamada upstream | `3` |
| `HTTP_RETRY_BACKOFF_SECONDS` | Backoff exponencial inicial para reintentos upstream | `1` |
| `POSTGRES_DB` | Nombre de la base Postgres usada por Compose | `ine_asturias` |
| `POSTGRES_USER` | Usuario Postgres usado por Compose | `postgres` |
| `POSTGRES_PASSWORD` | Password Postgres usada por Compose | `local-dev-postgres-Y8mF3vQ2sL7nR5xC` |
| `POSTGRES_HOST_PORT` | Puerto del host para PostgreSQL | `5433` |
| `POSTGRES_DSN` | DSN async para PostgreSQL dentro de la red Docker | `postgresql+asyncpg://postgres:local-dev-postgres-Y8mF3vQ2sL7nR5xC@db:5432/ine_asturias` |
| `ENABLE_CACHE` | Activa cache local en memoria | `true` |
| `CACHE_TTL_SECONDS` | TTL de cache | `300` |
| `API_KEY` | Proteccion por cabecera `X-API-Key`; obligatoria fuera de `local/dev/test` | `local-dev-api-key-x4R7mN2cQ9wP6sT8vB3kJ5hL1zF` |
| `LOG_LEVEL` | Nivel de log | `INFO` |
| `REDIS_HOST_PORT` | Puerto del host para Redis | `6379` |
| `REDIS_URL` | Backend Redis para jobs y coordinacion | `redis://redis:6379/0` |
| `API_HOST_PORT` | Puerto del host para la API | `8001` |
| `JOB_QUEUE_NAME` | Nombre de cola `arq` | `ine_jobs` |
| `JOB_RESULT_TTL_SECONDS` | TTL de resultados de jobs en Redis | `86400` |
| `TERRITORIAL_EXPORT_TTL_SECONDS` | TTL de reutilizacion para bundles ZIP de exportacion territorial | `86400` |
| `RATE_LIMIT_ENABLED` | Activa rate limiting por IP y por `API_KEY` | `true` |
| `PROVIDER_CIRCUIT_BREAKER_FAILURES` | Fallos consecutivos para abrir el circuit breaker | `5` |
| `PROVIDER_CIRCUIT_BREAKER_RECOVERY_SECONDS` | Tiempo de enfriamiento antes de `HALF_OPEN` | `30` |
| `PROVIDER_CIRCUIT_BREAKER_HALF_OPEN_SAMPLE_SIZE` | Muestra de llamadas para cerrar el breaker tras recuperacion | `5` |
| `PROVIDER_CIRCUIT_BREAKER_SUCCESS_THRESHOLD` | Ratio minimo de exito para cerrar el breaker | `0.8` |
| `WORKER_HEARTBEAT_TTL_SECONDS` | TTL del heartbeat del worker | `60` |
| `WORKER_METRICS_PORT` | Puerto HTTP interno del worker para metricas | `9001` |
| `WORKER_METRICS_URL` | URL interna que usa el API para agregar metricas del worker | `http://worker:9001/metrics` |
| `ENABLE_SLACK_NOTIFICATIONS` | Activa notificaciones operativas de incidencias INE hacia Slack | `false` |
| `SLACK_WEBHOOK_URL` | Webhook de Slack para incidencias INE notificables | `https://hooks.slack.com/...` |
| `ENABLE_PAGERDUTY` | Activa notificaciones de incidencias INE hacia PagerDuty | `false` |
| `PAGERDUTY_KEY` | Routing key de PagerDuty Events API v2 | `xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx` |
| `INE_INCIDENT_NOTIFY_SEVERITIES` | Severidades que disparan notificacion operativa | `["high","medium"]` |
| `INE_INCIDENT_NOTIFY_ON_RESOLVED` | Notifica tambien resoluciones automaticas de incidencias | `true` |
| `INE_INCIDENT_PAGERDUTY_SEVERITIES` | Severidades que pueden escalarse a PagerDuty | `["high"]` |

Usa `.env.example` como plantilla local y `.env.staging.example` como base de una configuracion de staging ejecutable, siempre sin secretos reales en repositorio.

Reglas operativas de seguridad:

- en `staging` y `production`, la aplicacion falla si `API_KEY` o la password embebida en `POSTGRES_DSN` son debiles o placeholders;
- `staging` y `production` exigen `X-API-Key` en `/ine/*`, `/territorios/*` y `/metrics`; en `local/dev/test` la proteccion queda desactivada por defecto para no romper el desarrollo;
- para generar una clave nueva puedes usar `python -c "from app.core.security import generate_api_key; print(generate_api_key())"`;
- los secretos reales deben inyectarse desde ficheros `*.local` no versionados o desde el gestor de secretos del entorno.

## Dependencias y release

- `requirements.txt` mantiene solo dependencias directas y versionadas de forma exacta.
- `requirements.lock` congela el conjunto efectivo usado por Docker y CI.
- Docker y CI DEBEN instalar desde `requirements.lock`.
- El proyecto usa versionado semantico simple y registra hitos operativos en [CHANGELOG.md](C:/Users/user/OneDrive/Documents/Playground/CHANGELOG.md).
- Politica de actualizacion recomendada:
  1. editar `requirements.txt` de forma intencional,
  2. reconstruir la imagen base,
  3. regenerar `requirements.lock` con `docker compose run --rm api pip freeze`,
  4. rerun `ruff`, `pytest`, `docker compose run --rm migrate` y `scripts/smoke_stack.py`.

Version actual recomendada:

- `0.1.0-rc1`: primer Release Candidate operativo aceptado.

## Imagenes base y seguridad ligera

- La imagen de aplicacion queda fijada a `python:3.12.13-slim` con digest explicito en [Dockerfile](C:/Users/user/OneDrive/Documents/Playground/Dockerfile).
- `postgis/postgis` y `redis` quedan fijadas por tag y digest en [docker-compose.yml](C:/Users/user/OneDrive/Documents/Playground/docker-compose.yml).
- Esta politica reduce deriva del runtime sin introducir un sistema pesado de gestion de imagenes.
- Los contenedores de aplicacion corren como non-root y con `cap_drop: [ALL]`.

## Dependencias y vulnerabilidades

La capa ligera adoptada en esta fase es:

- [dependabot.yml](C:/Users/user/OneDrive/Documents/Playground/.github/dependabot.yml) para pip, docker y github-actions, con frecuencia semanal.
- [security-scan.yml](C:/Users/user/OneDrive/Documents/Playground/.github/workflows/security-scan.yml) para escaneo manual o semanal de la imagen API con Trivy y artefacto 	rivy-report.
- pip check en CI y en build para detectar incompatibilidades declarativas de dependencias.
- Los workflows oficiales usan versiones de `actions/*` alineadas con Node 24 (`checkout@v5`, `setup-python@v6`, `upload-artifact@v6`) para reducir riesgo de rotura futura en GitHub Actions.

Cobertura real:

- deriva y actualizaciones conocidas de dependencias Python directas,
- deriva de referencias Docker declaradas,
- CVEs `HIGH` y `CRITICAL` de la imagen API construida en CI,
- deriva de GitHub Actions.

Lo que NO cubre por si solo:

- CVEs fuera de la imagen API escaneada,
- vulnerabilidades de runtime fuera de manifests declarados,
- hardening avanzado del host Docker.

## Migraciones

La aplicacion ya no crea tablas automaticamente al arrancar. El esquema DEBE gestionarse con Alembic.

Comandos principales:

```bash
python scripts/bootstrap_alembic.py
alembic upgrade head
alembic history
```

Migraciones incluidas:

- `0001_initial_schema`: tablas actuales de ingesta raw, series normalizadas y catalogo INE.
- `0002_postgis_territorial`: activacion de PostGIS y nucleo territorial base.
- `0003_territorial_series_ref`: referencia territorial opcional en `ine_series_normalized`.
- `0004_postgis_conventions`: convenciones espaciales e indices `GIST` base.
- `0005_geocoding_cache`: tablas `geocode_cache` y `reverse_geocode_cache` para cache geoespacial persistente.

### Bootstrap seguro para bases existentes

El proyecto incluye [scripts/bootstrap_alembic.py](C:/Users/user/OneDrive/Documents/Playground/scripts/bootstrap_alembic.py) para resolver de forma segura la transicion desde bases heredadas sin `alembic_version`.

Reglas del bootstrap:

- si la base esta vacia, ejecuta `alembic upgrade head`.
- si la base contiene el esquema base heredado sin `alembic_version`, valida estrictamente tablas, columnas, tipos y nulabilidad frente a la migracion inicial; solo si coincide hace `alembic stamp 0001_initial_schema` y luego `upgrade head`.
- si la base ya contiene tambien el nucleo territorial sin versionado, valida ambos bloques de esquema y solo entonces hace `alembic stamp 0002_postgis_territorial`.
- si faltan tablas clave o la estructura no coincide con la migracion esperada, falla de forma explicita y NO hace `stamp`.

Nota operativa: los archivos `.ini` del proyecto DEBEN guardarse en UTF-8 sin BOM para evitar errores de `ConfigParser` y de Alembic.

## Endpoints principales

### Salud y operacion

```http
GET /health
GET /health/ready
GET /metrics
```

- `/health` es liveness simple.
- `/health/ready` comprueba PostgreSQL, Redis y heartbeat de worker cuando aplica.
- `/metrics` expone metricas Prometheus del API y agrega tambien las metricas de aplicacion del worker cuando `WORKER_METRICS_URL` esta configurada y accesible.
- `/metrics` exige `X-API-Key` fuera de `local/dev/test`; en local sigue siendo accesible sin cabecera para no bloquear desarrollo ni diagnostico.

### INE raw / ingesta

```http
GET /ine/table/{table_id}
GET /ine/operation/{op_code}/variables
GET /ine/operation/{op_code}/variable/{variable_id}/values
GET /ine/operation/{op_code}/asturias
GET /ine/jobs/{job_id}
```

Politicas de proteccion actuales:

- `/geocode` y `/reverse_geocode`: `100 req/min` por IP en modo publico, `1000 req/min` si la request viene autenticada con `API_KEY`.
- `/ine/series`: `50 req/min` por IP en modo publico, `1000 req/min` con `API_KEY`.
- `/ine/operation/*`: `10 req/min` por IP en modo publico, `1000 req/min` con `API_KEY`.
- en `staging` y `production`, `/ine/*`, `/territorios/*` y `/metrics` requieren cabecera `X-API-Key`; en `local/dev/test` la cabecera sigue siendo opcional.

### Runbook corto de priorizacion INE

La campana operativa actual para priorizar operaciones del INE debe ejecutarse con estas reglas:

- una operacion cada vez
- `skip_known_processed=true`
- `max_tables=3`
- uso de `X-API-Key` para evitar throttling local innecesario
- verificacion posterior de `ine_tables_catalog`, `ine_series_normalized` y `/health`

Ejemplo de llamada:

```http
GET /ine/operation/22/asturias?background=false&skip_known_processed=true&max_tables=3
```

Estado operativo consolidado a 24 de marzo de 2026:

- `SCHEDULED_INE_OPERATIONS=["71","22","33"]` se mantiene como shortlist programable actual.
- `22`: programable. Ultima pasada controlada con `tables_succeeded=3` y `normalized_rows=318`; `1855` filas acumuladas.
- `33`: programable. Ultima pasada controlada con `tables_succeeded=3` y `normalized_rows=150`; `2106` filas acumuladas.
- `71`: programable con vigilancia. Sigue aportando valor (`10958` filas acumuladas), pero varias tablas activan `large_table_detected` y una ya cruza `table_processing_aborted_by_threshold`.
- `23`: `background-only` y fuera del scheduler. Aporta mucho dato (`46170` filas acumuladas), pero mantiene tablas gigantes y fallos operativos; solo debe ejecutarse como exploracion manual en background.
- `353`: exploracion manual, fuera del scheduler. Tiene un primer `has_data`, pero coste alto e inestabilidad insuficientemente compensada.
- `10`, `21`, `30`, `72`, `293`: descartadas por ahora para sincronizacion programada.

Criterio operativo para entrar en scheduler:

- varias pasadas controladas sin degradar `api`
- resultados utiles repetibles para Asturias
- sin dependencia de `series_direct` masivo
- sin tablas que crucen recurrentemente los umbrales pesados sin compensar con cobertura util
- catalogo con evidencia suficiente de `has_data` y valor analitico

Observabilidad operativa disponible:

- `GET /sync/status` mantiene la vista general de worker y fuentes programadas.
- `GET /sync/ine/operations` expone el catalogo operativo INE con filtros por `execution_profile`, `last_run_status`, `schedule_enabled` y `operation_code`, incluyendo el ultimo estado observado por operacion.
- `GET /sync/ine/incidents` expone incidencias operativas abiertas o resueltas por operacion, con severidad, sugerencia de accion y perfil efectivo actual.
- las incidencias INE abiertas o resueltas pueden notificar a Slack y, para severidad `high`, a PagerDuty cuando la configuracion del entorno lo habilita.
- `POST /sync/ine/operations/{operation_code}/override` y `DELETE /sync/ine/operations/{operation_code}/override` permiten promover, degradar o revertir el perfil operativo efectivo sin tocar `settings.py`.
- `GET /sync/ine/operations/{operation_code}/history` expone el historial append-only de cambios de override para auditar promociones, degradaciones y limpiezas operativas.

### Dominio semantico

```http
GET /ine/series?operation_code=22&geography_code=33&page=1&page_size=50
GET /geocode?query=Oviedo
GET /reverse_geocode?lat=43.3614&lon=-5.8494
GET /territorios/resolve-point?lat=43.3614&lon=-5.8494
GET /territorios/comunidades-autonomas
GET /territorios/provincias?autonomous_community_code=03
GET /territorios/municipio/33044/resumen
GET /municipio/33044
POST /territorios/export
GET /territorios/exports/{job_id}
GET /territorios/exports/{job_id}/download
```

Filtros soportados en `/ine/series`:

- `operation_code`
- `table_id`
- `geography_code`
- `geography_name`
- `variable_id`
- `period_from`
- `period_to`
- `page`
- `page_size`

Contrato semantico actual:

- `/ine/series` consulta solo datos ya normalizados en `ine_series_normalized`
- `geography_code` usa actualmente el sistema de codigos territoriales del INE como codigo externo canonico para consulta
- `/ine/series` acepta ya `geography_code_system`, pero en esta fase solo soporta `ine`
- `geography_name` funciona como filtro exacto case-insensitive de apoyo, no como codigo canonico
- cuando `geography_name` puede resolverse contra el modelo territorial interno, el endpoint traduce ese nombre a `geography_code` de INE y devuelve `territorial_resolution`
- la respuesta incluye metadata de paginacion:
  - `total`
  - `page`
  - `page_size`
  - `pages`
  - `has_next`
  - `has_previous`
  - `filters`
  - `territorial_resolution`

### Geocodificacion semantica

```http
GET /geocode?query=Oviedo
GET /reverse_geocode?lat=43.3614&lon=-5.8494
```

Contrato actual de `/geocode`:

- usa CartoCiudad como provider geografico inicial;
- a fecha del 14 de marzo de 2026, la documentacion publica de CartoCiudad no exige API key oficial para `find` / `reverseGeocode`; por eso esta API endurece el consumo con su propio `API_KEY`, rate limiting, retries y circuit breaker;
- consulta primero `geocode_cache` como cache persistente;
- si no hay hit persistente, hace fallback al adapter del provider;
- persiste el payload crudo del provider en `geocode_cache`;
- registra tambien la llamada upstream en `ingestion_raw` con `source_type=cartociudad_geocode_find` y parametros saneados;
- devuelve contrato semantico propio:
  - `source`
  - `generated_at`
  - `query`
  - `cached`
  - `territorial_context`
  - `territorial_resolution`
  - `summary`
  - `result`
  - `metadata`

La respuesta NO replica el shape crudo del provider. En esta fase:

- cuando CartoCiudad devuelve coordenadas normalizables, `territorial_context` y `territorial_resolution` se enriquecen arriba con la resolucion interna por geometria en PostGIS;
- el `result` mantiene el mejor payload semantico del provider, mientras que el contexto territorial superior ya no depende solo del match por codigo o nombre del provider;
- `summary` distingue entre `provider_hit`, `territorial_match`, `cached` y `partial_resolution`;
- si no existe match espacial fiable, el contrato mantiene `territorial_resolution=null` y `territorial_context` vacio en la capa top-level;
- `cached=true` indica hit de cache persistente del endpoint.
- los logs operativos y errores upstream ya no exponen la query completa; usan contexto saneado (`query_fingerprint`, longitud y numero de terminos).
- el cliente aplica reintentos acotados (`3` intentos maximo, backoff `1s/2s/4s` dentro de un presupuesto total de `30s`) y circuit breaker por provider.

Contrato actual de `/reverse_geocode`:

- usa CartoCiudad como provider geografico inicial;
- consulta primero `reverse_geocode_cache` como cache persistente;
- si no hay hit persistente, hace fallback al adapter del provider;
- persiste el payload crudo del provider en `reverse_geocode_cache`;
- registra tambien la llamada upstream en `ingestion_raw` con `source_type=cartociudad_reverse_geocode` y parametros saneados;
- devuelve contrato semantico propio, enriquecido con contexto territorial interno:
  - `source`
  - `generated_at`
  - `query_coordinates`
  - `cached`
  - `territorial_context`
  - `territorial_resolution`
  - `summary`
  - `result`
  - `metadata`

En esta fase:

- `territorial_context` y `territorial_resolution` se resuelven con prioridad contra `territorial_units.geometry` via PostGIS;
- el `result` mantiene el mejor payload semantico del provider cuando existe, pero el contexto territorial superior ya no depende solo de codigos o nombres del provider;
- si CartoCiudad falla o devuelve una respuesta inutilizable, el endpoint PUEDE degradar a resolucion territorial interna por coordenada cuando exista cobertura fiable;
- `summary` distingue entre `provider_hit`, `territorial_match`, `cached` y `partial_resolution`;
- `cached=true` indica hit de cache persistente del endpoint;
- `lat` y `lon` se validan en rango geodesico antes de consultar provider o cache.
- los logs operativos y errores upstream usan una huella de coordenadas saneada en vez del par completo.
- el cliente aplica el mismo esquema de reintentos acotados y circuit breaker antes de devolver error controlado.

### Resolucion territorial semantica por punto

```http
GET /territorios/resolve-point?lat=43.3614&lon=-5.8494
```

Contrato actual de `/territorios/resolve-point`:

- resuelve un par `lat` / `lon` solo contra cobertura administrativa interna ya cargada desde IGN/CNIG;
- NO hace reverse geocoding contra provider externo ni expone `geometry`, `centroid` o GeoJSON publico;
- devuelve contrato semantico propio:
  - `source`
  - `generated_at`
  - `query_coordinates`
  - `territorial_context`
  - `territorial_resolution`
  - `summary`
  - `result`
  - `metadata`
- `result.best_match` devuelve la unidad territorial mas especifica que cubre el punto;
- `result.hierarchy` devuelve la jerarquia interna contenida (`country -> autonomous_community -> province -> municipality`) segun la cobertura realmente disponible;
- `result.coverage` expone `boundary_source`, `levels_considered`, `levels_loaded`, `levels_missing_geometry`, `levels_matched` y `coverage_status`;
- `territorial_resolution` resume la estrategia espacial usada (`spatial_cover`), la cobertura cargada y si la resolucion es parcial;
- `summary` deja lista para automatizacion una lectura compacta de `matched`, `boundary_coverage_loaded`, `coverage_status`, `levels_loaded_total`, `levels_matched_total` y `partial_resolution`;
- si el punto cae fuera de la cobertura cargada, el endpoint devuelve `200` con `result=null` y `metadata.reason=outside_loaded_coverage`;
- si no hay limites administrativos cargados, devuelve `200` con `result=null` y `metadata.reason=no_boundary_coverage_loaded`.

Reglas operativas:

- la resolucion usa limites administrativos internos y `ST_Covers`, sin fallback por centroides en esta fase;
- el endpoint comparte el mismo perfil de rate limiting que `/geocode` y `/reverse_geocode`;
- el smoke obligatorio solo valida que el recurso esta publicado en `/territorios/catalogo`; la comprobacion funcional real queda como validacion manual opcional cuando IGN administrativo ya esta cargado en el entorno.

Estrategia territorial actual:

- el cruce territorial futuro debe apoyarse en `territorial_unit_codes`
- para el dominio INE actual, el source system canonico previsto es `ine`
- mientras no se abran nuevas fuentes geograficas, `/ine/series` mantiene `geography_code_system=ine` como referencia explicita

Estrategia de codigo canonico por nivel en la fase actual:

- `country` -> `source_system=iso3166`, `code_type=alpha2`
- `autonomous_community` -> `source_system=ine`, `code_type=autonomous_community`
- `province` -> `source_system=ine`, `code_type=province`
- `municipality` -> `source_system=ine`, `code_type=municipality`

Regla operativa:

- el codigo canonico territorial vive en `territorial_unit_codes` y debe marcarse como `is_primary=true` para el nivel correspondiente;
- `geography_code` de `/ine/series` sigue siendo el codigo externo canonico del dominio INE mientras no se introduzca el cruce territorial semantico completo;
- los aliases no sustituyen al codigo canonico: solo apoyan matching y resolucion de nombres.
- `ine_series_normalized` incorpora ya `territorial_unit_id` como referencia interna opcional para fases futuras, pero la clave logica actual y los filtros semanticos siguen apoyandose en `geography_code` del INE;
- el backfill de `territorial_unit_id` no se hace automaticamente en esta fase: se dejara para un paso explicito de enriquecimiento territorial.

Reglas actuales de matching territorial:

- `canonical_name` es la fuente de verdad semantica y debe representarse tambien en `normalized_name` usando normalizacion interna;
- `display_name` es solo la etiqueta de presentacion y no debe usarse como criterio de matching por si sola;
- `territorial_unit_aliases` debe almacenar variantes linguisticas, nombres de proveedor, nombres cortos y nombres alternativos;
- la normalizacion minima de nombres se hace con `normalize_territorial_name(...)`, que elimina acentos, puntuacion irrelevante y espacios redundantes;
- el matching territorial no debe vivir en routers ni en services ad hoc: debe resolverse mediante `TerritorialRepository`.

Capacidades actuales del repositorio territorial:

- lookup por codigo canonico mediante `get_unit_by_canonical_code(...)`
- lookup por codigo externo mediante `get_unit_by_code(...)` y `get_unit_by_ine_code(...)`
- lookup por nombre canonico o nombre de entrada mediante `get_unit_by_name(...)`
- lookup por alias mediante `get_unit_by_alias(...)`
- listado estable de codigos asociados a una unidad mediante `list_codes(...)`
- listado paginado por nivel territorial mediante `list_units(...)`
- detalle territorial por codigo canonico mediante `get_unit_detail_by_canonical_code(...)`
- upsert idempotente de limites administrativos y centroides mediante `upsert_boundary_unit(...)`

Endpoints territoriales publicos actuales:

- `GET /territorios/catalogo` expone un catalogo minimo de recursos territoriales y analiticos publicados, con cobertura basica por nivel.
- `GET /territorios/comunidades-autonomas` devuelve comunidades autonomas desde el modelo interno con paginacion basica.
- `GET /territorios/provincias` devuelve provincias y admite filtro por `autonomous_community_code`.
- `GET /municipio/{codigo_ine}` devuelve detalle de municipio por codigo canonico INE, incluyendo codigos, aliases y atributos.
- `GET /territorios/catalogo` publica tambien `GET /geocode` y `GET /reverse_geocode` como recursos oficiales de descubrimiento.
- `GET /territorios/catalogo` anuncia tambien la cobertura administrativa cargada desde IGN/CNIG y el conteo de unidades con geometria/centroide por nivel.
- `POST /territorios/export` crea un job asincrono de exportacion multi-fuente por entidad territorial.
- `GET /territorios/exports/{job_id}` expone el estado y el resultado operativo del export.
- `GET /territorios/exports/{job_id}/download` devuelve el bundle ZIP de un export completado.

## Catalogo territorial minimo

La Fase 5 incorpora `GET /territorios/catalogo` como punto de descubrimiento ligero para automatizaciones, agentes y clientes API.

El endpoint devuelve:

- `resources`: recursos publicados realmente consumibles por contrato semantico interno.
- `territorial_levels`: niveles territoriales disponibles con cobertura basica (`units_total`, `active_units`, `geometry_units`, `centroid_units`) y rutas asociadas.
- `summary`: conteo agregado de recursos de lectura, analitica y jobs.
- `metadata`: contexto operativo minimo para consumo automatizado.

El catalogo NO expone tablas internas ni payloads raw. Su funcion es descubrir capacidades publicas, no inventariar la base de datos.

Con la consolidacion de CartoCiudad bajo demanda, el catalogo publicado ya incluye:

- `GET /geocode`
- `GET /reverse_geocode`
- `GET /territorios/resolve-point`
- `POST /territorios/export`
- `GET /territorios/exports/{job_id}`
- `GET /territorios/exports/{job_id}/download`
- `territorial.ign_administrative_boundaries.catalog`
- recursos territoriales y analiticos internos relacionados

La carga administrativa directa de IGN/CNIG NO abre aun endpoints publicos de geometria. En esta fase sirve para:

- enriquecer `territorial_units` con `geometry` y `centroid` en SRID `4326`
- dejar trazabilidad raw versionable en `ingestion_raw`
- publicar cobertura interna reutilizable desde `/territorios/catalogo`

## Exportacion territorial multi-fuente

La API incorpora ya una primera capacidad de descarga asincrona por entidad territorial. El objetivo es entregar un bundle semantico reutilizable por automatizaciones, clientes API y agentes sin exponer payloads raw de proveedor.

Endpoints actuales:

```http
POST /territorios/export
GET /territorios/exports/{job_id}
GET /territorios/exports/{job_id}/download
```

Contrato de `POST /territorios/export`:

- `unit_level`: `municipality` o `autonomous_community`
- `code_value`: codigo canonico interno de la entidad
- `format`: fijo en `zip`
- `include_providers`: lista opcional; por defecto `["territorial", "ine", "analytics"]`
- `include_providers=["catastro"]` o combinado con otros providers activa Catastro Urbano municipal como provider opt-in

Reglas operativas:

- siempre responde `202` y devuelve `job_id` y `status_path`;
- el export se ejecuta siempre como background job;
- el bundle se reutiliza mientras siga fresco en `territorial_export_artifacts`;
- la expiracion se controla con `TERRITORIAL_EXPORT_TTL_SECONDS`;
- si `analytics` no aplica al nivel pedido, el manifiesto lo marca como `applicable=false` y el export sigue siendo valido.
- si `catastro` se pide para `autonomous_community`, el manifiesto lo marca como `applicable=false` y el export sigue siendo valido.
- si `catastro` se pide para `municipality` y no existe cache valido ni respuesta correcta del upstream, el job termina en `failed`.

Estructura del ZIP v1:

- `manifest.json`
- `datasets/territorial_unit.json`
- `datasets/territorial_hierarchy.json`
- `datasets/ine_series.ndjson`
- `datasets/analytics_municipality_summary.json` cuando aplica
- `datasets/analytics_municipality_report.json` cuando aplica
- `datasets/catastro_municipality_aggregates.json` cuando `include_providers` incluye `catastro`

Garantias del contrato:

- `manifest.json` usa `source=internal.export.territorial_bundle`;
- el bundle NO expone `geometry`, `centroid`, GeoJSON publico ni payloads raw de proveedor;
- `ine_series` se exporta en `NDJSON` para soportar volumen y futuras fuentes heterogeneas;
- `catastro` se mantiene como provider opt-in y solo aplica a `municipality` en v1;
- el dataset Catastro v1 contiene agregados municipales urbanos oficiales y NO exporta inmuebles individuales, parcelas ni payload raw del proveedor;

Trazabilidad y cache de Catastro v1:

- cada fetch live de Catastro Urbano municipal se persiste en `ingestion_raw` con `source_type=catastro_urbano_municipality_aggregates`;
- la reutilizacion operativa del provider se apoya en `catastro_municipality_aggregate_cache`;
- el TTL del cache Catastro se controla con `CATASTRO_CACHE_TTL_SECONDS`;
- `CATASTRO_URBANO_YEAR` permite fijar un ano concreto; si queda vacio, el adapter hace auto-discovery del ultimo ano disponible.
- futuras fuentes como Catastro deben entrar como `provider` adicional y nuevos `datasets/*`, sin romper el manifiesto base ni el API publico.

## Contrato base de salidas analiticas

La Fase 5 fija una familia comun de respuestas para automatizacion, informes y agentes. Esta familia NO depende del shape raw de INE, CartoCiudad u otros providers.

Campos comunes:

- `source`: identificador semantico de la salida analitica, no del endpoint raw del proveedor.
- `generated_at`: instante de generacion del resultado.
- `territorial_context`: contexto territorial resuelto por el dominio interno.
- `filters`: filtros semanticos aplicados para construir la salida.
- `summary`: resumen compacto pensado para n8n, agentes y consumo programatico.
- `series`: observaciones o indicadores semanticos ya preparados para consumo.
- `metadata`: contexto adicional no ligado al payload raw de un proveedor.
- `pagination`: bloque opcional cuando la salida sea paginada.

Contrato recomendado:

```json
{
  "source": "internal.analytics.territorial_summary",
  "generated_at": "2026-03-14T13:30:00Z",
  "territorial_context": {
    "territorial_unit_id": 44,
    "unit_level": "municipality",
    "canonical_code": "33044",
    "canonical_name": "Oviedo",
    "display_name": "Oviedo",
    "source_system": "ine",
    "country_code": "ES",
    "autonomous_community_code": "33",
    "province_code": "33",
    "municipality_code": "33044"
  },
  "filters": {
    "indicator_family": "population",
    "period_from": "2020",
    "period_to": "2024"
  },
  "summary": {
    "series_count": 2,
    "last_period": "2024"
  },
  "series": [
    {
      "series_key": "population.total",
      "label": "Poblacion total",
      "value": 220543,
      "unit": "personas",
      "period": "2024",
      "metadata": {
        "operation_code": "22",
        "table_id": "2852"
      }
    }
  ],
  "metadata": {
    "consumer_profile": "automation"
  },
  "pagination": {
    "total": 2,
    "page": 1,
    "page_size": 50,
    "pages": 1,
    "has_next": false,
    "has_previous": false
  }
}
```

Reglas:

- `series` DEBE contener indicadores u observaciones semanticas, no nodos raw del proveedor.
- `territorial_context` DEBE apoyarse en el modelo territorial interno cuando exista resolucion fiable.
- `filters`, `summary` y `metadata` PUEDEN variar por endpoint, pero mantienen el mismo papel semantico en todas las salidas analiticas.
- `pagination` solo aparece cuando aporta valor real al consumidor.

Contrato de error recomendado para endpoints analiticos:

```json
{
  "detail": {
    "code": "territorial_summary_not_ready",
    "message": "The requested analytical snapshot is not ready yet.",
    "retryable": true,
    "metadata": {
      "job_id": "job-123",
      "status_path": "/territorios/jobs/job-123"
    }
  }
}
```

Este contrato esta pensado para que n8n, agentes y clientes API puedan tratar igual una salida analitica sin conocer detalles internos de INE, CartoCiudad o del modelo raw persistido.

Endpoints analiticos disponibles:

- `GET /territorios/municipio/{codigo_ine}/resumen`
- combina `territorial_unit` del modelo interno con indicadores latest de `ine_series_normalized`
- usa `codigo_ine` como referencia canonica del dominio INE para el municipio
- admite `operation_code`, `variable_id`, `period_from`, `period_to`, `page` y `page_size`
- devuelve una ficha reutilizable para automatizacion con:
  - `territorial_context`
  - `territorial_unit`
  - `summary`
  - `series`
  - `pagination`
- `POST /territorios/municipio/{codigo_ine}/informe`
- devuelve `202 Accepted` con `job_id`, `status_path` y `report_type=municipality_report`
- reutiliza la misma semantica analitica del resumen, pero la empaqueta como informe estructurado para consumo no interactivo
- `GET /territorios/jobs/{job_id}`
- permite hacer polling del job territorial hasta `completed` o `failed`
- cuando `ANALYTICAL_SNAPSHOT_TTL_SECONDS > 0` y PostgreSQL esta disponible, el informe se persiste y reutiliza desde `analytical_snapshots`
- si la persistencia no esta disponible, el contrato sigue funcionando con resultado en `job_store` (`RedisJobStore` o `InMemoryJobStore`)

## Jobs y ejecucion asincrona

El endpoint `/ine/operation/{op_code}/asturias` mantiene el contrato actual, pero el trabajo pesado ya sale del proceso web cuando Redis y worker estan disponibles:

- en topologia con Redis + worker, el job se encola en `arq`
- en `local/test` sin Redis disponible, la app hace fallback controlado a modo en memoria para no romper desarrollo ni tests

Contrato de estado:

- `queued`
- `running`
- `completed`
- `failed`

Consulta de estado:

```http
GET /ine/jobs/{job_id}
```

Para jobs analiticos territoriales:

```http
POST /territorios/municipio/{codigo_ine}/informe
GET /territorios/jobs/{job_id}
```

## Patron de consumo para n8n y agentes

El criterio operativo de esta fase es simple: `n8n`, agentes y clientes API DEBEN consumir la API propia y NO DEBEN depender del shape de INE, CartoCiudad ni de tablas raw internas.

Orden recomendado de consumo:

1. intentar primero un endpoint semantico sincronico cuando la necesidad sea puntual o interactiva;
2. usar un job analitico cuando el resultado esperado sea mas pesado, reusable o apto para automatizacion;
3. consumir `result`, `summary`, `series` y `metadata` del contrato interno, no campos opacos del proveedor;
4. conservar `job_id`, `status_path` y, cuando exista, `snapshot_key` como referencias operativas.

Endpoints recomendados hoy para automatizacion:

- `GET /territorios/catalogo`
- `GET /geocode`
- `GET /reverse_geocode`
- `GET /territorios/municipio/{codigo_ine}/resumen`
- `POST /territorios/municipio/{codigo_ine}/informe`
- `GET /territorios/jobs/{job_id}`
- `GET /geocode`
- `GET /reverse_geocode`

Datos que NO deben consumir automatizaciones ni agentes:

- payloads raw de proveedor persistidos en `ingestion_raw`
- endpoints o URLs directas de INE o CartoCiudad
- tablas internas como `ine_series_normalized`, `ine_tables_catalog` o `analytical_snapshots` como contrato externo
- claves o estructuras internas de provider dentro de `metadata` cuando no formen parte del contrato semantico documentado

Flujo recomendado de polling para jobs:

```text
1. POST /territorios/municipio/{codigo_ine}/informe
2. leer job_id y status_path
3. esperar unos segundos
4. GET status_path
5. repetir mientras status sea queued o running
6. usar result si status=completed
7. tratar error como terminal si status=failed
```

Politica recomendada de polling:

- espera inicial: `2-5` segundos
- reintento: cada `5-15` segundos segun criticidad
- stop conditions: `completed` o `failed`
- si `detail.retryable=true`, el cliente puede reintentar el flujo completo con backoff

Contrato practico para n8n:

- `summary` DEBE usarse para routing, decisiones y alertas ligeras
- `series` DEBE usarse para carga de datos, informes o escritura en sistemas externos
- `metadata` DEBE usarse como contexto operativo, no como fuente primaria de dato de negocio
- `pagination` DEBE respetarse si el endpoint la expone

Ejemplo de patron n8n:

```text
Webhook/Cron
  -> HTTP Request (POST /territorios/municipio/{codigo_ine}/informe)
  -> Wait
  -> HTTP Request (GET /territorios/jobs/{job_id})
  -> IF status == completed
  -> escribir result.summary / result.series en destino
  -> IF status == failed -> notificar / registrar incidencia
```

Regla semantica clave:

- para contexto territorial o informes, priorizar `/territorios/...`
- para resolucion geografica, priorizar `/geocode` y `/reverse_geocode`
- para automatizaciones nuevas, NO disenar flujos contra el proveedor externo aunque hoy parezca mas rapido

## Persistencia

### `ingestion_raw`

`ingestion_raw` ya no se usa solo para INE. En la fase multi-fuente bajo demanda tambien registra llamadas upstream de CartoCiudad con:

- `source_type=cartociudad_geocode_find`
- `source_type=cartociudad_reverse_geocode`
- `request_params` saneados para evitar exponer direcciones completas o coordenadas exactas en auditoria operacional reutilizable

Guarda payloads completos del proveedor con contexto de llamada para auditoria y depuracion.

La integracion administrativa directa con IGN/CNIG tambien persiste snapshots versionables y grupos por nivel con:

- `source_type=ign_admin_boundaries_snapshot`
- `source_type=ign_admin_boundaries_country`
- `source_type=ign_admin_boundaries_autonomous_community`
- `source_type=ign_admin_boundaries_province`
- `source_type=ign_admin_boundaries_municipality`

El payload raw almacenado conserva cobertura, version y fuente de carga, pero NO se expone como contrato publico.

## Carga administrativa IGN/CNIG

La nueva fuente oficial directa de limites administrativos se carga mediante script interno y snapshots versionables. El contrato publico sigue siendo el modelo territorial interno; no existe aun un endpoint publico de geometria cruda.

Modos de carga soportados:

- desde fichero local o ZIP descargado: `python scripts/load_ign_admin_boundaries.py --input-path data/ign_asturias_boundaries.zip --pretty`
- desde URL configurada: `python scripts/load_ign_admin_boundaries.py --snapshot-url https://example.invalid/ign-asturias.zip --pretty`
- usando `IGN_ADMIN_SNAPSHOT_URL` desde entorno: `python scripts/load_ign_admin_boundaries.py --pretty`

Comportamiento actual de la fase:

- scope por defecto: Asturias (`autonomous_community_code=03`) mas unidades padre necesarias
- normalizacion a `FeatureCollection` en `4326`
- validacion de limites `MULTIPOLYGON` y centroides `POINT`
- matching por codigo canonico y parent linkage antes del upsert final
- trazabilidad raw en `ingestion_raw` y publicacion agregada en `/territorios/catalogo`

### `ine_series_normalized`

Guarda observaciones normalizadas con deduplicacion logica via `ON CONFLICT DO UPDATE` por:

- `operation_code`
- `table_id`
- `variable_id`
- `geography_name`
- `geography_code`
- `period`

### `ine_tables_catalog`

Persistencia operativa del catalogo de tablas descubiertas por operacion con estados:

- `unknown`
- `has_data`
- `no_data`
- `failed`

### `analytical_snapshots`

Persistencia reutilizable para informes o snapshots analiticos generados por la API propia.

Reglas actuales:

- usa clave logica por `snapshot_type + scope_key + filters`
- evita duplicacion de resultados semanticos equivalentes
- guarda `payload`, `filters`, `metadata`, `generated_at` y `expires_at`
- el primer uso activo es `municipality_report`
- la expiracion se controla con `ANALYTICAL_SNAPSHOT_TTL_SECONDS`

### Nucleo territorial base

La base ya incluye el esqueleto para evolucion territorial y espacial:

- `territorial_units`
- `territorial_unit_codes`
- `territorial_unit_aliases`

Convenciones PostGIS actuales:

- SRID canonico: `4326`
- geometria territorial base: `MULTIPOLYGON`
- centroide territorial: `POINT`
- `geometry` y `centroid` se mantienen `nullable` hasta que exista una carga geografica explicita y validada
- los indices espaciales base del proyecto son `GIST` sobre `geometry` y `centroid`
- el proyecto no debe introducir WKT/WKB o geometria cruda en payloads raw ni en contratos publicos mientras no exista un contrato semantico geografico explicito

Contrato de carga geoespacial futura:

- el formato de intercambio inicial previsto es `GeoJSON FeatureCollection`
- el SRID canonico sigue siendo `4326`
- cualquier carga futura DEBE validarse primero en staging o en tablas temporales de ensayo
- no se abre todavia ninguna nueva fuente geografica: solo queda fijado el contrato que deberan respetar las futuras cargas

Contrato semantico futuro de geocodificacion:

- `GET /geocode` y `GET /reverse_geocode` DEBERAN devolver contratos internos y estables, nunca el payload crudo del proveedor;
- ambos contratos compartiran:
  - `source`
  - `cached`
  - `coordinates`
  - `entity_type`
  - `territorial_context`
  - `territorial_resolution`
  - `metadata`
- `GET /geocode` debera exponerse sobre `query`
- `GET /reverse_geocode` debera exponerse sobre `query_coordinates`
- el resultado geografico podra incluir `address`, `postal_code` y `label`, pero esos campos NO deben sustituir al cruce con `territorial_units` cuando exista una resolucion interna.

Adapter geografico futuro inmediato:

- CartoCiudad se integrara mediante `app/services/cartociudad_client.py`
- el adapter encapsula `find?q=...` y `reverseGeocode?lat=&lon=...`
- el adapter solo devuelve `dict` / `list`, maneja errores y construye claves de cache
- el adapter no persiste en base ni define el contrato publico final

## Observabilidad operativa

La exportacion Prometheus se resuelve de forma pragmatica:

- el API sigue exponiendo `/metrics` como punto unico de scrape inicial.
- el worker expone metricas HTTP propias en `WORKER_METRICS_PORT`.
- el API agrega las metricas de aplicacion del worker dentro de su propio `/metrics` usando `WORKER_METRICS_URL`.
- las metricas genericas de proceso y runtime (`python_*`, `process_*`) siguen siendo locales a cada proceso y NO se mezclan para evitar series duplicadas o invalidas.
- `/metrics` ya no debe exponerse sin control en entornos compartidos: en `staging` y `production` requiere `X-API-Key`, mientras que `local/dev/test` mantienen acceso abierto para desarrollo y troubleshooting.

Si necesitas diagnostico fino por proceso, consulta el worker directamente desde la red interna de Docker o publica temporalmente su puerto de metricas en un entorno controlado.
Los objetivos operativos y reglas base de alertado quedan definidos en [MONITORING_SLOS.md](C:/Users/user/OneDrive/Documents/Playground/MONITORING_SLOS.md) y [monitoring/prometheus-alerts.yml](C:/Users/user/OneDrive/Documents/Playground/monitoring/prometheus-alerts.yml).

Metricas operativas nuevas relevantes:

- `ine_asturias_provider_retries_total`
  - cuenta reintentos reales hacia INE y CartoCiudad por familia de endpoint y motivo.
- `ine_asturias_provider_circuit_breaker_transitions_total`
  - registra aperturas, recuperaciones y transiciones `HALF_OPEN`.
- `ine_asturias_auth_failures_total`
  - cuenta rechazos por clave ausente o invalida.
- `ine_asturias_rate_limit_rejections_total`
  - cuenta rechazos `429` por politica y modo de autenticacion.
- `ine_asturias_job_duration_seconds`
  - mide duracion de jobs inline y worker para SLIs de latencia de background.

Metricas analiticas nuevas en `/metrics`:

- `ine_asturias_analytical_flow_total`
  - cuenta resultados por `flow`, `outcome` y `storage_mode`
  - `flow` actual: `municipality_summary`, `municipality_report`
  - `outcome` actual: `completed`, `not_found`, `failed`
- `ine_asturias_analytical_flow_duration_seconds`
  - mide latencia total del flujo analitico con las mismas etiquetas semanticas
- `ine_asturias_analytical_flow_series_count`
  - registra el volumen de `series` devuelto por salida analitica
- `ine_asturias_analytical_flow_result_bytes`
  - aproxima el tamano serializado del resultado para detectar informes pesados
- `ine_asturias_analytical_snapshot_events_total`
  - cuenta `miss`, `persisted` y `hit` de snapshots analiticos

Lectura recomendada:

- si `municipality_report` crece en `duration_seconds` pero no en `series_count`, el cuello no esta en el volumen de salida sino en el proceso de construccion
- si suben `persisted` y `hit`, la cache analitica esta amortizando recalculo
- si predominan `miss` sin `hit`, la TTL o el patron de consumo probablemente no estan alineados con la demanda real

## Tratamiento del warning de collation en PostgreSQL

El contenedor puede mostrar una advertencia como esta:

```text
WARNING:  database "ine_asturias" has a collation version mismatch
DETAIL:  The database was created using collation version 2.41, but the operating system provides version 2.31.
HINT:  Rebuild all objects in this database that use the default collation and run ALTER DATABASE ine_asturias REFRESH COLLATION VERSION, or build PostgreSQL with the right library version.
```

Interpretacion:

- la base fue creada con una version de libreria de collation distinta de la que ofrece ahora la imagen del contenedor o el sistema subyacente.
- no bloquea el runtime ni las migraciones actuales, pero puede afectar ordenaciones e indices dependientes de collation.

### Procedimiento para entorno local desechable

Usa esta via cuando el entorno local no sea la copia canonica de datos y puedas recrearlo sin impacto operativo real.

1. si quieres conservar datos, genera primero un backup:

```bash
mkdir -p backups
docker compose exec -T db pg_dump -U postgres -d ine_asturias -Fc -f /tmp/ine_asturias.dump
docker compose cp db:/tmp/ine_asturias.dump backups/ine_asturias.dump
```

2. elimina el stack y el volumen de Postgres:

```bash
docker compose down -v
```

3. levanta de nuevo la base con la imagen actual y aplica migraciones:

```bash
docker compose up --build -d db redis
docker compose run --rm migrate
docker compose up --build -d api worker
```

4. valida que el warning ya no aparece y que el stack sigue sano:

```bash
docker compose logs --no-color db
curl http://127.0.0.1:8001/health
curl http://127.0.0.1:8001/health/ready
docker compose run --rm api python scripts/smoke_stack.py
```

### Procedimiento para staging o entornos persistentes

Usa esta via cuando la base conserva datos que NO deben recrearse a ciegas.

1. genera un backup completo y verificable antes de tocar collation.
2. identifica si existen objetos dependientes de la collation por defecto que deban reconstruirse.
3. planifica una ventana de mantenimiento; NO ejecutes este cambio durante trafico normal si el entorno es compartido.
4. reconstruye los indices u objetos afectados.
5. solo despues ejecuta:

```sql
ALTER DATABASE ine_asturias REFRESH COLLATION VERSION;
```

6. reinicia conexiones si procede y valida:

```bash
docker compose run --rm migrate
curl http://127.0.0.1:8001/health
curl http://127.0.0.1:8001/health/ready
docker compose run --rm api python scripts/smoke_stack.py
docker compose run --rm api python scripts/verify_restore.py --base-url http://api:8000 --min-ingestion-rows 1 --min-normalized-rows 1 --min-catalog-rows 1 --expected-alembic-version 0008_catastro_cache --functional-operation-code 22
```

### Regla operativa

- en local desechable, la solucion preferida es recrear volumen y base con la imagen actual.
- en staging o persistente, NO refresques la collation a ciegas; primero reconstruye los objetos afectados y trata el cambio como una operacion planificada.
- no cierres esta incidencia como resuelta hasta repetir `/health`, `/health/ready`, smoke test y verificacion minima de restore o integridad.

## Validacion manual recomendada

1. Levantar el stack:

```bash
docker compose up --build
```

2. Comprobar salud:

```bash
curl http://127.0.0.1:8001/health
curl http://127.0.0.1:8001/health/ready
curl http://127.0.0.1:8001/metrics -H "X-API-Key: <API_KEY>"
```

3. Lanzar una ingesta real pequena:

```bash
curl "http://127.0.0.1:8001/ine/operation/22/asturias?max_tables=1"
```

4. Consultar el job:

```bash
curl "http://127.0.0.1:8001/ine/jobs/{job_id}"
```

5. Verificar datos normalizados:

```bash
curl "http://127.0.0.1:8001/ine/series?operation_code=22&page=1&page_size=10"
```

## Evidencia operativa reciente

La ultima revalidacion operativa de staging y RC se ha ejecutado ya con la primera capa territorial/geografica integrada.

Resultado confirmado:

- Alembic en `0005_geocoding_cache`
- `/health` = `200`
- `/health/ready` = `200`
- `/metrics` = `200`
- smoke test correcto con job real
- `verify_restore.py` correcto con:
  - `ingestion_raw=6`
  - `ine_series_normalized=75`
  - `/ine/series total=75`

Esta evidencia confirma que la capa publica `geocode` / `reverse_geocode` / `territorios` no ha roto la operacion del stack ni el criterio de release candidate.

## Smoke test automatizado

El repositorio incluye [scripts/smoke_stack.py](C:/Users/user/OneDrive/Documents/Playground/scripts/smoke_stack.py) como validacion runtime minima del stack.

Valida de forma automatica:

- `GET /health`
- `GET /health/ready`
- `GET /metrics`
- `GET /territorios/comunidades-autonomas`
- encolado y completado de un job real corto en `/ine/operation/22/asturias?max_tables=1`
- disponibilidad de datos en `/ine/series` despues del job

El script reutiliza `API_KEY` del entorno si existe o acepta `--api-key`, por lo que sirve tanto en local abierto como en staging protegido.

Ejecucion recomendada en CI o en un entorno con Python local:

```bash
cp .env.example .env
docker compose up --build -d
python scripts/smoke_stack.py
docker compose down -v
```

Alternativa si quieres ejecutarlo desde la propia imagen del proyecto:

```bash
docker compose up --build -d
docker compose run --rm api python scripts/smoke_stack.py --base-url http://api:8000
docker compose down -v
```

## Checklist operativa breve

### Recuperar entorno local desechable

```bash
docker compose down -v
cp .env.example .env
docker compose up --build -d
```

### Relanzar migraciones de forma segura

```bash
docker compose run --rm migrate
```

### Verificar salud minima del stack

```bash
docker compose ps
curl http://127.0.0.1:8001/health
curl http://127.0.0.1:8001/health/ready
curl http://127.0.0.1:8001/metrics -H "X-API-Key: <API_KEY>"
```

### Confirmar API, Redis y worker operativos

- `GET /health/ready` debe devolver `postgres=ok`, `redis=ok` y `worker=ok`.
- `docker compose logs --no-color worker --tail 100` debe mostrar heartbeat y ejecucion de jobs.
- `docker compose logs --no-color api --tail 100` debe mostrar requests completadas y metricas accesibles.

### Tratar `collation version mismatch`

- en local desechable: haz backup si hace falta, elimina volumenes y recrea la base con la imagen actual.
- en entornos persistentes: NO ejecutes `REFRESH COLLATION VERSION` a ciegas; primero identifica y reconstruye los objetos afectados y planifica la operacion.

### Reconstruir volumenes si hace falta

```bash
docker compose down -v
docker compose up --build -d
docker compose run --rm migrate
```

## Calidad de codigo

El carril minimo de calidad queda basado en `ruff` y se ejecuta tanto en local como en CI. En esta ronda se ha ampliado de forma controlada al subconjunto de infraestructura y entrada: `app/api`, `app/core`, `scripts`, `main.py`, `app/settings.py` y `app/worker.py`.

Comandos principales:

```bash
ruff check .
ruff format --check app/api app/core scripts main.py app/settings.py app/worker.py
```

Si necesitas corregir formato de forma segura:

```bash
ruff format .
```

La configuracion queda en [pyproject.toml](C:/Users/user/OneDrive/Documents/Playground/pyproject.toml) con una seleccion ligera de reglas para evitar degradacion futura sin introducir una carga excesiva. El criterio seguido es ampliar el gate solo sobre capas operativas ya estables, evitando churn innecesario en el resto del repositorio.

## Backup y restore de PostgreSQL

### Politica minima recomendada

Se considera backup minimo valido un dump completo de `ine_asturias` en formato custom (`pg_dump -Fc`), generado con una version compatible de PostgreSQL/PostGIS y verificable mediante restore en entorno controlado.

- entorno local persistente: hacer backup antes de `docker compose down -v`, antes de pruebas destructivas y al menos una vez por semana si el dataset local se quiere conservar.
- staging: hacer backup antes de cada migracion o cambio operativo sensible y, si el entorno permanece levantado, al menos una vez al dia.
- verificacion de restore: debe ejecutarse siempre tras cambiar el procedimiento de backup/restore y antes de dar por valido un backup para mantenimiento o recuperacion.
### Backup completo desde Docker Compose

La forma mas portable en este entorno es generar el dump dentro del contenedor y despues copiarlo al host:

```bash
mkdir -p backups
docker compose exec -T db pg_dump -U postgres -d ine_asturias -Fc -f /tmp/ine_asturias.dump
docker compose cp db:/tmp/ine_asturias.dump backups/ine_asturias.dump
```

### Restore completo sobre una base recreada

```bash
docker compose cp backups/ine_asturias.dump db:/tmp/ine_asturias.dump
docker compose exec -T db dropdb -U postgres --if-exists ine_asturias
docker compose exec -T db createdb -U postgres -T template0 ine_asturias
docker compose exec -T db pg_restore -U postgres -d ine_asturias /tmp/ine_asturias.dump
docker compose run --rm migrate
```

### Recrear entorno local desde backup

```bash
docker compose down -v
docker compose up -d db redis
docker compose cp backups/ine_asturias.dump db:/tmp/ine_asturias.dump
docker compose exec -T db dropdb -U postgres --if-exists ine_asturias
docker compose exec -T db createdb -U postgres -T template0 ine_asturias
docker compose exec -T db pg_restore -U postgres -d ine_asturias /tmp/ine_asturias.dump
docker compose run --rm migrate
docker compose up --build -d api worker
```

### Verificacion minima de restore

Despues de restaurar la base en un entorno controlado:

```bash
docker compose run --rm migrate
docker compose run --rm api python scripts/verify_restore.py --base-url http://api:8000 --min-ingestion-rows 1 --min-catalog-rows 1 --expected-alembic-version 0008_catastro_cache --functional-operation-code 22
```

La verificacion reutiliza `API_KEY` del entorno si existe o acepta `--api-key` para entornos protegidos como staging.

Nota: en entornos basados en `postgis/postgis`, el restore limpio DEBE recrear la base con `template0` para evitar conflictos entre extensiones PostGIS preinstaladas y un `pg_restore --clean`.

La verificacion comprueba como minimo:

- `alembic_version`
- conteo minimo de `ingestion_raw`
- conteo minimo de `ine_series_normalized`
- `GET /health`
- `GET /health/ready`
- contrato basico de `/ine/series`

Si necesitas una validacion funcional mas completa tras el restore, ejecuta ademas [scripts/smoke_stack.py](C:/Users/user/OneDrive/Documents/Playground/scripts/smoke_stack.py).

### Rutina repetible de restore

El repositorio incluye [scripts/restore_drill.ps1](C:/Users/user/OneDrive/Documents/Playground/scripts/restore_drill.ps1) para repetir el drill completo de recuperacion de forma estable.

Comando recomendado para entorno local:

```bash
.\\scripts\\restore_drill.ps1 -BackupPath backups/ine_asturias.dump -TeardownOnSuccess
```

La rutina ejecuta:

- entorno limpio con `docker compose down -v`,
- restore desde backup,
- migraciones,
- arranque de `api` y `worker`,
- validacion de `/health`,
- validacion de `/health/ready`,
- smoke test completo.
Advertencias operativas:

- `pg_dump` y `pg_restore` DEBEN ser compatibles con la version mayor de PostgreSQL/PostGIS usada por el contenedor.
- si el backup viene de otro entorno o de otra imagen base, valida antes el estado de collation y las extensiones instaladas.
- despues de un restore completo, conviene ejecutar `/health`, `/health/ready` y el smoke test para confirmar que el sistema vuelve a estar operativo.

## Configuracion local y staging

La configuracion queda preparada para dos usos claros:

- `.env.example`: valores por defecto para desarrollo/local con Docker Compose.
- `.env.staging.example`: mismo contrato de variables, pero con `APP_ENV=staging` y placeholders seguros para futuros despliegues.
- [DEPLOYMENT_STAGING.md](C:/Users/user/OneDrive/Documents/Playground/DEPLOYMENT_STAGING.md): procedimiento operativo para levantar staging como entorno de ensayo real.
  Tambien incluye el rollback operativo minimo y el uso de restore desde backup como mecanismo oficial cuando no exista downgrade seguro de migraciones.
  A partir de esta fase tambien define el ensayo completo de staging: deploy, migrate, health, smoke, rollback y restore verification.

Nombres recomendados para ficheros reales no versionados:

- local: `.env.local` o `.env`
- staging: `.env.staging.local`
- release/ensayo puntual: `.env.rc.local`

### Arranque local vs staging

Local:

```bash
cp .env.example .env
docker compose --env-file .env.example -p ine_asturias_local up --build -d
```

Staging preparado:

```bash
docker compose --env-file .env.staging.example -p ine_asturias_staging up --build -d
docker compose --env-file .env.staging.example -p ine_asturias_staging run --rm api python scripts/smoke_stack.py --base-url http://api:8000
docker compose --env-file .env.staging.example -p ine_asturias_staging down -v
```

Valores por defecto recomendados:

- local: API `8001`, Postgres `5433`, Redis `6379`
- staging: API `8002`, Postgres `5434`, Redis `6380`

Parada del entorno staging:

```bash
docker compose --env-file .env.staging.example -p ine_asturias_staging down -v
```
Reglas operativas:

- NO reutilices `.env` local como configuracion de staging.
- NO guardes secretos reales en `.env.example` ni en `.env.staging.example`.
- Los ficheros `.env.local`, `.env.staging.local`, `.env.rc.local` y variantes `*.local` quedan fuera de Git por diseno.
- staging DEBE usar un fichero de entorno propio y no heredar secretos, puertos ni `API_KEY` desde local.
- staging DEBE usar el mismo contrato de variables que local para que la aplicacion siga siendo reproducible sin ramas de configuracion especiales.
## CI minima

La CI base queda en [.github/workflows/ci.yml](C:/Users/user/OneDrive/Documents/Playground/.github/workflows/ci.yml) y ejecuta:

- instalacion de dependencias Python desde `requirements.lock`
- `pytest`
- arranque controlado de `db` y `redis`
- validacion de migraciones con `docker compose run --rm migrate`
- arranque de `api` y `worker`
- smoke test con [scripts/smoke_stack.py](C:/Users/user/OneDrive/Documents/Playground/scripts/smoke_stack.py)
- recogida de logs en caso de fallo y `docker compose down -v` al final

Workflows manuales adicionales:

- [restore-drill.yml](C:/Users/user/OneDrive/Documents/Playground/.github/workflows/restore-drill.yml): ejecuta un restore drill completo bajo demanda en CI y publica `restore-drill-artifacts` con `seed-smoke.txt`, `smoke-test.txt`, `verify-restore.txt`, `compose-ps.txt` y logs de servicios.
- [security-scan.yml](C:/Users/user/OneDrive/Documents/Playground/.github/workflows/security-scan.yml): escanea la imagen API con Trivy de forma manual o semanal y publica el artefacto `trivy-report` con `trivy-report.txt`.

## Release candidate

El proceso operativo completo para preparar un RC queda formalizado en [RELEASE_PROCESS.md](C:/Users/user/OneDrive/Documents/Playground/RELEASE_PROCESS.md). La forma recomendada de ejecutarlo localmente es [scripts/release_candidate.ps1](C:/Users/user/OneDrive/Documents/Playground/scripts/release_candidate.ps1).

Ejemplo:

```powershell
.\scripts\release_candidate.ps1 -RunRestoreDrill -BackupPath backups/ine_asturias.dump
```

## Checklist minima de release

Antes de declarar una release operativa, comprobar como minimo:

- `ruff check .`
- `ruff format --check app/api app/core scripts main.py app/settings.py app/worker.py`
- `pytest`
- `docker compose run --rm migrate`
- smoke test verde con [scripts/smoke_stack.py](C:/Users/user/OneDrive/Documents/Playground/scripts/smoke_stack.py)
- restore drill ejecutado recientemente con [scripts/restore_drill.ps1](C:/Users/user/OneDrive/Documents/Playground/scripts/restore_drill.ps1)
- revision de `.env.example` y `.env.staging.example`
- confirmacion de `requirements.lock` actualizado si hubo cambios de dependencias
- confirmacion de tags/digests en [Dockerfile](C:/Users/user/OneDrive/Documents/Playground/Dockerfile) y [docker-compose.yml](C:/Users/user/OneDrive/Documents/Playground/docker-compose.yml)
- `/health`, `/health/ready` y `/metrics` respondiendo correctamente

### Continuidad post-RC

Despues de aceptar un RC, el equipo DEBE repetir `Restore Drill` y `Security Scan` cuando cambien workflows, dependencias, imagenes base o digests. El detalle operativo y la evidencia minima requerida quedan definidos en [RELEASE_PROCESS.md](C:/Users/user/OneDrive/Documents/Playground/RELEASE_PROCESS.md).
## Testing

Los tests actuales siguen evitando red real con `httpx.MockTransport` y dummies de repositorio. Tambien existe una prueba de integracion opcional para Redis real.

Ejecuta cuando tengas entorno Python disponible:

```bash
pytest
```

Cobertura relevante actual:

- salud, readiness y agregacion basica de metricas
- tabla INE y validacion de parametros
- errores upstream e JSON invalido
- resolucion automatica de Asturias
- catalogo de tablas
- endpoint semantico `/ine/series`
- endpoints geograficos `/geocode` y `/reverse_geocode`
- endpoints territoriales `/territorios/...` y `/municipio/{codigo_ine}`
- serializacion previa del `upsert`
- integracion opcional de Redis real para jobs
- integracion real con Postgres para cache geoespacial y matching territorial

## Limitaciones actuales

- La cache general sigue siendo local al proceso; Redis se usa ya para jobs, no aun para cache general.
- El worker en Redis + `arq` esta preparado para operacion, pero la politica completa de reintentos y observabilidad avanzada puede crecer en siguientes sprints.
- El modelo territorial base ya expone una primera capa publica de lectura territorial y ya incorpora CartoCiudad bajo demanda e IGN/CNIG administrativo por carga interna, pero aun no publica endpoints espaciales semanticos propios.
- La agregacion de `/metrics` mezcla metricas de aplicacion del API y del worker, pero las metricas genericas de proceso siguen siendo per-proceso por diseno.
- El contenedor Postgres actual puede mostrar `collation version mismatch`; ya esta tratado operativamente, pero conviene resolverlo antes de un entorno persistente serio.
- La carga IGN/CNIG actual esta pensada para snapshots versionables Asturias-first; la ampliacion nacional completa y los endpoints espaciales quedan para la siguiente fase.







