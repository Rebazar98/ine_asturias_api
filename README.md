# ine_asturias_api

`ine_asturias_api` es un backend FastAPI para ingesta, normalizacion y publicacion de datos del INE con foco inicial en Asturias. La base actual ya incorpora PostgreSQL, PostGIS, Alembic, Redis, jobs desacoplados, worker dedicado, catalogo persistente de tablas, observabilidad minima y una base territorial preparada para crecer hacia nuevas fuentes oficiales.

## Documentacion

Documentacion disponible en la raiz del proyecto:

- `README.md`: guia funcional y operativa del backend actual.
- `DOCUMENTACION_EVOLUCION_PROYECTO.txt`: registro historico y memoria tecnica del proyecto.
- `PLAN_TECNICO_PLATAFORMA_DATOS_TERRITORIALES.md`: roadmap de evolucion hacia plataforma territorial multi-fuente.
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
- `app/services`: cliente INE, resolucion de Asturias, normalizacion y orquestacion de ingesta.
- `app/repositories`: persistencia raw, normalizada, catalogo, base territorial y cache geoespacial persistente.
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

## Variables de entorno

| Variable | Descripcion | Ejemplo |
|---|---|---|
| `APP_NAME` | Nombre logico de la app | `ine_asturias_api` |
| `APP_VERSION` | Version operativa actual de la aplicacion | `0.1.0-rc1` |
| `APP_ENV` | Entorno de ejecucion | `local` |
| `INE_BASE_URL` | Base URL del INE | `https://servicios.ine.es/wstempus/js/ES` |
| `HTTP_TIMEOUT_SECONDS` | Timeout HTTP hacia el proveedor | `15` |
| `POSTGRES_DB` | Nombre de la base Postgres usada por Compose | `ine_asturias` |
| `POSTGRES_USER` | Usuario Postgres usado por Compose | `postgres` |
| `POSTGRES_PASSWORD` | Password Postgres usada por Compose | `postgres` |
| `POSTGRES_HOST_PORT` | Puerto del host para PostgreSQL | `5433` |
| `POSTGRES_DSN` | DSN async para PostgreSQL dentro de la red Docker | `postgresql+asyncpg://postgres:postgres@db:5432/ine_asturias` |
| `ENABLE_CACHE` | Activa cache local en memoria | `true` |
| `CACHE_TTL_SECONDS` | TTL de cache | `300` |
| `API_KEY` | Proteccion opcional por cabecera `X-API-Key` | vacio |
| `LOG_LEVEL` | Nivel de log | `INFO` |
| `REDIS_HOST_PORT` | Puerto del host para Redis | `6379` |
| `REDIS_URL` | Backend Redis para jobs y coordinacion | `redis://redis:6379/0` |
| `API_HOST_PORT` | Puerto del host para la API | `8001` |
| `JOB_QUEUE_NAME` | Nombre de cola `arq` | `ine_jobs` |
| `JOB_RESULT_TTL_SECONDS` | TTL de resultados de jobs en Redis | `86400` |
| `WORKER_HEARTBEAT_TTL_SECONDS` | TTL del heartbeat del worker | `60` |
| `WORKER_METRICS_PORT` | Puerto HTTP interno del worker para metricas | `9001` |
| `WORKER_METRICS_URL` | URL interna que usa el API para agregar metricas del worker | `http://worker:9001/metrics` |

Usa `.env.example` como plantilla local y `.env.staging.example` como base de una configuracion de staging ejecutable, siempre sin secretos reales en repositorio.

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

### INE raw / ingesta

```http
GET /ine/table/{table_id}
GET /ine/operation/{op_code}/variables
GET /ine/operation/{op_code}/variable/{variable_id}/values
GET /ine/operation/{op_code}/asturias
GET /ine/jobs/{job_id}
```

### Dominio semantico

```http
GET /ine/series?operation_code=22&geography_code=33&page=1&page_size=50
GET /geocode?query=Oviedo
GET /reverse_geocode?lat=43.3614&lon=-5.8494
GET /territorios/comunidades-autonomas
GET /territorios/provincias?autonomous_community_code=03
GET /municipio/33044
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
- consulta primero `geocode_cache` como cache persistente;
- si no hay hit persistente, hace fallback al adapter del provider;
- persiste el payload crudo normalizado en `geocode_cache`;
- devuelve contrato semantico propio:
  - `source`
  - `query`
  - `cached`
  - `result`
  - `metadata`

La respuesta NO replica el shape crudo del provider. En esta fase:

- `territorial_resolution` se rellena cuando el payload de CartoCiudad puede cruzarse de forma fiable con el modelo territorial interno;
- si no existe match fiable, `territorial_resolution` queda `null`;
- `cached=true` indica hit de cache persistente del endpoint.

Contrato actual de `/reverse_geocode`:

- usa CartoCiudad como provider geografico inicial;
- consulta primero `reverse_geocode_cache` como cache persistente;
- si no hay hit persistente, hace fallback al adapter del provider;
- persiste el payload crudo normalizado en `reverse_geocode_cache`;
- devuelve contrato semantico consistente con `/geocode`:
  - `source`
  - `query_coordinates`
  - `cached`
  - `result`
  - `metadata`

En esta fase:

- `territorial_resolution` se rellena cuando el payload puede resolverse contra `territorial_units` y `territorial_unit_codes`;
- si no existe match fiable, el contrato mantiene `territorial_resolution=null`;
- `cached=true` indica hit de cache persistente del endpoint;
- `lat` y `lon` se validan en rango geodesico antes de consultar provider o cache.

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

Endpoints territoriales publicos actuales:

- `GET /territorios/comunidades-autonomas` devuelve comunidades autonomas desde el modelo interno con paginacion basica.
- `GET /territorios/provincias` devuelve provincias y admite filtro por `autonomous_community_code`.
- `GET /municipio/{codigo_ine}` devuelve detalle de municipio por codigo canonico INE, incluyendo codigos, aliases y atributos.

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

## Persistencia

### `ingestion_raw`

Guarda payloads completos del proveedor con contexto de llamada para auditoria y depuracion.

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

Si necesitas diagnostico fino por proceso, consulta el worker directamente desde la red interna de Docker o publica temporalmente su puerto de metricas en un entorno controlado.

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
docker compose run --rm api python scripts/verify_restore.py --base-url http://api:8000 --postgres-dsn postgresql://postgres:postgres@db:5432/ine_asturias --min-ingestion-rows 1 --min-normalized-rows 1
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
curl http://127.0.0.1:8001/metrics
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
curl http://127.0.0.1:8001/metrics
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
docker compose run --rm api python scripts/verify_restore.py --base-url http://api:8000 --postgres-dsn postgresql://postgres:postgres@db:5432/ine_asturias --min-ingestion-rows 1
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
- El modelo territorial base ya expone una primera capa publica de lectura territorial y sigue preparado para la entrada futura de IGN y CartoCiudad.
- La agregacion de `/metrics` mezcla metricas de aplicacion del API y del worker, pero las metricas genericas de proceso siguen siendo per-proceso por diseno.
- El contenedor Postgres actual puede mostrar `collation version mismatch`; ya esta tratado operativamente, pero conviene resolverlo antes de un entorno persistente serio.
- No se han anadido todavia nuevas integraciones externas: este bloque endurece la base tecnica del sistema antes de ampliar fuentes.







