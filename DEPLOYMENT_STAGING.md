# Deployment Staging

Este documento define el procedimiento minimo para usar staging como entorno de ensayo real del proyecto `ine_asturias_api`.

No cubre despliegue cloud ni automatizacion avanzada. Su objetivo es que cualquier miembro del equipo pueda levantar, migrar, verificar y apagar un entorno de staging reproducible con Docker Compose.

## Alcance

Este procedimiento cubre:

- preparacion de entorno
- variables necesarias
- despliegue con Docker Compose
- migraciones
- verificacion de salud
- smoke test
- evidencia minima de ejecucion

El rollback detallado y el ensayo completo de staging se documentaran en los siguientes pasos de la fase, pero este documento ya deja la base operativa de despliegue.

## Prerequisitos

- Docker Desktop o Docker Engine operativo
- `docker compose` disponible
- copia local del repositorio actualizada
- acceso al fichero de entorno de staging fuera del repositorio
- puertos disponibles para staging:
  - API: `8002`
  - PostgreSQL: `5434`
  - Redis: `6380`

## Configuracion de entorno

La plantilla base de staging es:

- [.env.staging.example](C:/Users/user/OneDrive/Documents/Playground/.env.staging.example)

NO uses esa plantilla como fichero final de staging compartido. Debe copiarse fuera del repo o a un fichero local ignorado.

Nombre recomendado para entorno local de ensayo:

- `.env.staging.local`
- opcionalmente `.env.rc.local` para una validacion puntual de release candidate

Ejemplo:

```bash
cp .env.staging.example .env.staging.local
```

Variables que deben revisarse como minimo:

- `APP_ENV=staging`
- `APP_VERSION`
- `POSTGRES_PASSWORD`
- `POSTGRES_DSN`
- `API_KEY`
- `API_HOST_PORT`
- `POSTGRES_HOST_PORT`
- `REDIS_HOST_PORT`
- `REDIS_URL`
- `WORKER_METRICS_URL`
- `TERRITORIAL_EXPORT_TTL_SECONDS`
- `CATASTRO_CACHE_TTL_SECONDS`
- `CATASTRO_TIMEOUT_SECONDS`
- `CATASTRO_URBANO_YEAR` si quieres fijar un ano de referencia concreto

Reglas operativas:

- `.env.staging.example` es una plantilla, no un fichero operativo final.
- `.env.staging.local` y `.env.rc.local` DEBEN permanecer fuera de Git.
- NO reutilices `.env` o `.env.local` de desarrollo para staging.
- `API_KEY`, `POSTGRES_PASSWORD` y cualquier otro secreto real DEBEN inyectarse desde el fichero local no versionado o desde el entorno del host.

## Despliegue de staging

Comando recomendado:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging up --build -d
```

Esto debe levantar:

- `db`
- `redis`
- `migrate`
- `api`
- `worker`

## Migraciones

El servicio `migrate` se ejecuta automaticamente dentro del stack, pero en staging el equipo DEBE verificar explicitamente que el estado final es correcto.

Comprobacion recomendada:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm migrate
```

El resultado esperado es que Alembic quede en `head` sin aplicar cambios inesperados.

Nota operativa:

- en arranques completamente frios con volumen nuevo, el servicio `migrate` puede fallar en el primer `up` por una carrera corta al conectar con PostgreSQL;
- en esta topologia, el comando canonico para confirmar staging sigue siendo `docker compose ... run --rm migrate` despues del arranque inicial;
- si ese comando deja Alembic en `head`, staging puede seguir considerandose valido aunque el primer `migrate` del `up` haya fallado.

## Verificacion de salud

Comprobaciones minimas desde host:

```bash
curl http://127.0.0.1:8002/health
curl http://127.0.0.1:8002/health/ready
curl http://127.0.0.1:8002/metrics -H "X-API-Key: <API_KEY>"
```

Resultado esperado:

- `/health` devuelve `200`
- `/health/ready` devuelve `200` con `postgres=ok`, `redis=ok` y `worker=ok`
- `/metrics` responde correctamente

Comprobacion adicional de contenedores:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging ps
```

## Smoke test

El smoke test minimo del entorno de staging debe ejecutarse desde la propia imagen del proyecto para evitar diferencias entre host y contenedor.

Comando recomendado:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm api python scripts/smoke_stack.py --base-url http://api:8000 --api-key change-me
```

Si `API_KEY` esta disponible en el fichero de entorno, tambien puedes ejecutar:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm api python scripts/smoke_stack.py --base-url http://api:8000
```

Si staging tiene cargado el modelo territorial y quieres validar tambien la nueva capa analitica de Fase 5, usa un municipio canonico conocido:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm api python scripts/smoke_stack.py --base-url http://api:8000 --municipality-code 33044
```

Para entornos locales o RC sin `territorial_units` cargada, puedes sembrar antes un contexto minimo de validacion con:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm api python scripts/seed_municipality_analytics.py --municipality-code 33044
```

Ese seed es solo para validacion operativa del smoke analitico y no sustituye a una carga territorial real de staging.

Resultado esperado:

- `/health OK`
- `/health/ready OK`
- `/metrics OK`
- `/territorios/catalogo OK`
- job corto completado
- `/ine/series OK`
- si se aporta `--municipality-code`:
  - `/territorios/municipio/{codigo_ine}/resumen OK`
  - `informe municipal encolado`
  - `informe municipal completado`
- `validacion completada`

### Validacion manual opcional de CartoCiudad

Esta comprobacion NO forma parte del gate obligatorio de staging ni de CI. Sirve solo como evidencia manual de la segunda fuente oficial cuando se quiera comprobar el camino real contra proveedor.

Comandos sugeridos:

```bash
curl "http://127.0.0.1:8002/geocode?query=Oviedo" -H "X-API-Key: change-me"
curl "http://127.0.0.1:8002/reverse_geocode?lat=43.3614&lon=-5.8494" -H "X-API-Key: change-me"
```

Resultado esperado:

- `source=cartociudad`
- contrato semantico propio del API
- sin payload raw del proveedor en la respuesta
- `territorial_resolution` relleno si el cruce con el modelo territorial interno es fiable

Regla operativa:

- si esta validacion falla por disponibilidad del proveedor externo, NO debe tumbar por si sola la aceptacion del entorno;
- si falla el contrato semantico, la cache persistente o la trazabilidad raw, entonces si debe abrirse incidencia de producto.

### Validacion manual opcional de IGN administrativo

Esta comprobacion tampoco forma parte del gate obligatorio. Sirve como evidencia manual de la carga administrativa directa cuando staging tenga disponible un snapshot versionable IGN/CNIG por fichero o por URL.

Comandos sugeridos:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm api python scripts/load_ign_admin_boundaries.py --pretty
curl "http://127.0.0.1:8002/territorios/catalogo" -H "X-API-Key: change-me"
curl "http://127.0.0.1:8002/territorios/resolve-point?lat=43.3614&lon=-5.8494" -H "X-API-Key: change-me"
```

Si se quiere forzar un fichero local concreto montado en el contenedor:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm api python scripts/load_ign_admin_boundaries.py --input-path /app/data/ign_asturias_boundaries.zip --pretty
```

Resultado esperado:

- `source=ign_administrative_boundaries`
- `features_upserted > 0`
- `raw_records_saved > 0`
- `/territorios/catalogo` refleja `boundary_source=ign_administrative_boundaries` y conteos `geometry_units` / `centroid_units` en el nivel cargado
- `/territorios/resolve-point` devuelve contrato semantico interno con `best_match` y `hierarchy`, sin geometria publica

Regla operativa:

- si falta snapshot o la URL no esta disponible, la validacion queda como pendiente manual y NO tumba por si sola staging;
- si la carga completa pero no deja trazabilidad raw o no actualiza cobertura en catalogo, si debe abrirse incidencia.

### Validacion manual opcional de exportacion territorial

Esta comprobacion tampoco forma parte del gate obligatorio. Sirve para validar el bundle multi-fuente cuando staging ya dispone de datos territoriales e indicadores normalizados para la entidad elegida.

Ejemplo para municipio:

```bash
curl -X POST "http://127.0.0.1:8002/territorios/export" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: change-me" \
  -d '{"unit_level":"municipality","code_value":"33044","format":"zip","include_providers":["territorial","ine","analytics"]}'
curl "http://127.0.0.1:8002/territorios/exports/<job_id>" -H "X-API-Key: change-me"
curl -L "http://127.0.0.1:8002/territorios/exports/<job_id>/download" -H "X-API-Key: change-me" --output territorial_export_municipality_33044.zip
```

Resultado esperado:

- `POST /territorios/export` devuelve `202` con `job_type=territorial_export`
- `GET /territorios/exports/{job_id}` acaba en `completed`
- el ZIP contiene `manifest.json` y `datasets/ine_series.ndjson`
- si el nivel es `municipality`, tambien puede incluir `analytics_municipality_summary.json` y `analytics_municipality_report.json`
- si `include_providers` incluye `catastro`, el ZIP tambien debe incluir `datasets/catastro_municipality_aggregates.json`
- el manifiesto NO expone `geometry`, `centroid` ni payloads raw

Regla operativa:

- si staging no tiene datos suficientes para la entidad, el export puede completar con datasets vacios y eso NO implica fallo estructural;
- si el job no completa, el download devuelve un artefacto sin `manifest.json` o el bundle expone payloads raw, si debe abrirse incidencia.

## Evidencia minima

Cada despliegue de staging que se use como ensayo DEBE dejar, como minimo:

- comando de despliegue utilizado
- `docker compose ps`
- confirmacion de migraciones en `head`
- respuesta satisfactoria de `/health`
- respuesta satisfactoria de `/health/ready`
- smoke test en verde
- si se valida analitica productiva, municipio usado en `--municipality-code`

Evidencia recomendada adicional:

- logs de `api`
- logs de `worker`
- fecha y version (`APP_VERSION`) del ensayo
- si se valida IGN administrativo, `dataset_version` usado y resumen del load (`features_selected`, `features_upserted`, `raw_records_saved`)

## Rollback de staging

El rollback DEBE tratarse como una operacion controlada. La prioridad es volver a un estado sano y verificable, no forzar un downgrade peligroso.

### Rollback de contenedores

Usa este camino cuando el problema esta en la ejecucion de servicios o en una imagen recien desplegada, pero la base sigue siendo valida.

1. detener el stack actual:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging down
```

2. volver a levantar la revision anterior del codigo o de la imagen conocida como valida.

3. validar de nuevo:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging up --build -d
curl http://127.0.0.1:8002/health
curl http://127.0.0.1:8002/health/ready
```

### Rollback de migraciones

NO debe asumirse que toda migracion es reversible.

Regla operativa:

- solo hacer downgrade de migraciones si existe un camino de rollback probado y documentado;
- si no existe ese camino, el mecanismo oficial de recuperacion es restore desde backup.

Comprobacion previa antes de plantear downgrade:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm migrate
```

Si la migracion no es explicitamente reversible o no se ha validado en ensayo, NO hagas downgrade manual.

### Restore desde backup como mecanismo oficial

Cuando haya duda sobre la reversibilidad de migraciones o sobre el estado de la base, el camino oficial es:

1. disponer de backup valido;
2. destruir el stack de staging o al menos la base afectada;
3. restaurar desde backup;
4. reaplicar migraciones si corresponde;
5. validar salud y smoke test.

Secuencia base:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging down -v
docker compose --env-file .env.staging.local -p ine_asturias_staging up -d db redis
docker compose --env-file .env.staging.local -p ine_asturias_staging cp backups/ine_asturias.dump db:/tmp/ine_asturias.dump
docker compose --env-file .env.staging.local -p ine_asturias_staging exec -T db dropdb -U postgres --if-exists ine_asturias
docker compose --env-file .env.staging.local -p ine_asturias_staging exec -T db createdb -U postgres -T template0 ine_asturias
docker compose --env-file .env.staging.local -p ine_asturias_staging exec -T db pg_restore -U postgres -d ine_asturias /tmp/ine_asturias.dump
docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm migrate
docker compose --env-file .env.staging.local -p ine_asturias_staging up --build -d api worker
```

### Validacion posterior obligatoria

Todo rollback o restore en staging DEBE terminar con esta verificacion minima:

```bash
curl http://127.0.0.1:8002/health
curl http://127.0.0.1:8002/health/ready
docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm api python scripts/smoke_stack.py --base-url http://api:8000
docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm api python scripts/verify_restore.py --base-url http://api:8000 --min-ingestion-rows 1 --min-normalized-rows 1 --min-catalog-rows 1 --expected-alembic-version 0012_cartographic_qa_incidents --functional-operation-code 22
```

Si `API_KEY` esta activa en staging, anade `--api-key` o deja el valor en el entorno del contenedor.

## Parada del entorno

Para apagar staging sin eliminar volumenes:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging down
```

Para destruir el entorno completo y recrearlo despues:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging down -v
```

## Criterio minimo de despliegue satisfactorio

El despliegue de staging puede considerarse satisfactorio solo si:

- el stack levanta completo
- `migrate` termina correctamente
- `/health` y `/health/ready` responden bien
- el smoke test pasa
- la evidencia minima queda registrada

## Ensayo completo de staging

Este ensayo convierte staging en un entorno real de prueba operativa. Debe ejecutarse como una secuencia completa y dejar evidencia suficiente para evaluar deploy, rollback y recuperacion.

### Paso 0. Preparar backup

Antes de tocar staging, genera o confirma un backup valido:

```bash
mkdir -p backups
docker compose --env-file .env.staging.local -p ine_asturias_staging exec -T db pg_dump -U postgres -d ine_asturias -Fc -f /tmp/ine_asturias_staging.dump
docker compose --env-file .env.staging.local -p ine_asturias_staging cp db:/tmp/ine_asturias_staging.dump backups/ine_asturias_staging.dump
```

### Paso 1. Deploy

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging up --build -d
docker compose --env-file .env.staging.local -p ine_asturias_staging ps
```

Resultado esperado:

- `db`, `redis`, `api` y `worker` arriba
- `migrate` completado correctamente

### Paso 2. Migrate

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm migrate
```

Resultado esperado:

- Alembic en `head`
- ninguna migracion inesperada ni error de bootstrap

### Paso 3. Health

```bash
curl http://127.0.0.1:8002/health
curl http://127.0.0.1:8002/health/ready
curl http://127.0.0.1:8002/metrics -H "X-API-Key: <API_KEY>"
```

Resultado esperado:

- `/health` = `200`
- `/health/ready` = `200`
- `/metrics` accesible

### Paso 4. Smoke

Si `API_KEY` esta definida en `.env.staging.local`, exportala o pasala al comando:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm api python scripts/smoke_stack.py --base-url http://api:8000
```

Resultado esperado:

- `/health OK`
- `/health/ready OK`
- `/metrics OK`
- `job completado`
- `/ine/series OK`
- `validacion completada`

### Paso 5. Rollback controlado

Este paso no implica necesariamente destruir el entorno. El objetivo es comprobar que el procedimiento de vuelta atras esta claro y ejecutable.

Camino minimo:

1. detener el entorno:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging down
```

2. volver a levantar la revision conocida como valida:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging up --build -d
```

3. comprobar de nuevo:

```bash
curl http://127.0.0.1:8002/health
curl http://127.0.0.1:8002/health/ready
```

### Paso 6. Restore verification

Para verificar que el mecanismo de recuperacion sigue siendo usable en staging, usa el backup anterior y valida restore/integridad.

Opcion A. Verificacion minima sobre el entorno ya levantado:

```bash
docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm api python scripts/verify_restore.py --base-url http://api:8000 --min-ingestion-rows 1 --min-normalized-rows 1 --min-catalog-rows 1 --expected-alembic-version 0012_cartographic_qa_incidents --functional-operation-code 22
```

Opcion B. Drill completo de restore sobre staging de ensayo:

```powershell
.\scripts\restore_drill.ps1 -EnvFile .env.staging.local -ProjectName ine_asturias_staging -BaseUrl http://127.0.0.1:8002 -BackupPath backups/ine_asturias_staging.dump
```

Resultado esperado:

- restore sin errores
- `/health OK`
- `/health/ready OK`
- smoke test correcto
- `verify_restore.py` correcto

### Evidencia del ensayo completo

Cada ensayo completo de staging DEBE conservar al menos:

- fichero de entorno usado, sin secretos en claro dentro del repositorio
- `docker compose ps`
- confirmacion de migraciones
- salida de `/health`
- salida de `/health/ready`
- resultado del smoke test
- resultado de `verify_restore.py`
- referencia al backup usado

### Criterio de staging operativo real

Staging puede considerarse un entorno de ensayo real solo si este ensayo completo puede repetirse sin pasos ambiguos y sin depender de conocimiento oral del equipo.

## Evidencia operativa F3-9

La revalidacion de F3-9 se ha ejecutado sobre un entorno de staging local separado con:

- `--env-file .env.staging.local`
- `-p ine_asturias_staging`

Resultado validado:

- `docker compose ... up --build -d` levanta `db` y `redis` correctamente;
- `docker compose ... run --rm migrate` deja Alembic en `0004_postgis_conventions`;
- `/health` devuelve `200`;
- `/health/ready` devuelve `200` con `postgres=ok`, `redis=ok` y `worker=ok`;
- `/metrics` devuelve `200`;
- el smoke test completa con job real y `/ine/series OK`;
- `verify_restore.py` confirma:
  - `alembic_version=0004_postgis_conventions`
  - `ingestion_raw=3`
  - `ine_series_normalized=75`
  - `/health OK`
  - `/health/ready OK`
  - `/ine/series total=75`

## Evidencia operativa F5-9

La revalidacion de F5-9 mantiene el mismo patron de staging aislado, pero anade una evidencia nueva: la capa de automatizacion y analitica territorial debe comprobarse en runtime cuando exista un municipio canonico disponible en el modelo.

Evidencia minima adicional esperada en F5-9:

- `docker compose ... up --build -d` levanta `api`, `db`, `redis` y `worker` correctamente;
- `docker compose ... run --rm migrate` deja Alembic en `head`;
- `/health` devuelve `200`;
- `/health/ready` devuelve `200` con `postgres=ok`, `redis=ok` y `worker=ok`;
- `/metrics` devuelve `200`;
- el smoke test confirma `GET /territorios/catalogo`;
- si se aporta `--municipality-code`, el smoke test confirma:
  - `GET /territorios/municipio/{codigo_ine}/resumen`
  - `POST /territorios/municipio/{codigo_ine}/informe`
  - polling exitoso en `/territorios/jobs/{job_id}`;
- `verify_restore.py` sigue confirmando integridad tras restore.

Lectura operativa:

- la capa analitica nueva no debe romper deploy, migraciones, health, smoke ni restore verification;
- la validacion de staging ya no se limita a salud y persistencia: ahora debe cubrir tambien descubrimiento semantico y jobs analiticos cuando el modelo territorial este cargado;
- si staging no tiene un municipio canonico disponible para esa comprobacion, F5-9 NO debe considerarse completamente revalidada en ese entorno.

## Criterio de aceptacion de staging operativo real

Staging queda aceptado como entorno operativo real solo si se cumplen TODAS estas condiciones:

1. existe un fichero de entorno propio de staging fuera del repositorio, alineado con `.env.staging.example`;
2. el stack puede levantarse con un unico comando reproducible de `docker compose`;
3. `migrate` termina correctamente y deja Alembic en `head`;
4. `/health`, `/health/ready` y `/metrics` responden correctamente;
5. el smoke test pasa usando la topologia real de staging;
6. si staging dispone de modelo territorial cargado, el smoke test analitico pasa con `--municipality-code`;
7. existe un procedimiento de rollback documentado y ejecutable;
8. existe un procedimiento de restore verification documentado y reutilizable;
9. la evidencia minima del ensayo queda registrada;
10. el equipo no necesita pasos no documentados para repetir el flujo.

## Gate operativo recomendado

Antes de considerar staging como "listo para ensayo real", revisa esta checklist:

- `docker compose --env-file .env.staging.local -p ine_asturias_staging up --build -d`
- `docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm migrate`
- `curl http://127.0.0.1:8002/health`
- `curl http://127.0.0.1:8002/health/ready`
- `curl http://127.0.0.1:8002/metrics -H "X-API-Key: <API_KEY>"`
- `docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm api python scripts/smoke_stack.py --base-url http://api:8000`
- si el modelo territorial esta cargado: `docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm api python scripts/smoke_stack.py --base-url http://api:8000 --municipality-code 33044`
- rollback descrito y revisado
- restore verification descrito y revisado
- evidencia guardada

Si falta cualquiera de estos puntos, staging NO debe considerarse todavia aceptado como entorno operativo real.

## Validacion futura de cargas geoespaciales

Cuando el proyecto abra una carga geoespacial formal, staging DEBERA actuar como entorno obligatorio de ensayo previo. Esa validacion futura debera seguir este minimo:

1. cargar el artefacto geoespacial en staging o en tablas temporales de ensayo;
2. confirmar que el formato de intercambio es `GeoJSON FeatureCollection` o una transformacion equivalente ya llevada a ese formato canonico;
3. validar que todas las geometrias quedan en SRID `4326`;
4. validar que limites administrativos se normalizan a `MULTIPOLYGON` y centroides a `POINT`;
5. ejecutar comprobaciones de integridad geometrica y cobertura;
6. verificar que el cruce con `territorial_unit_codes` funciona antes de considerar la carga aceptable;
7. repetir `/health`, `/health/ready`, smoke test y restore verification despues de la carga.

Regla operativa:

- esta capacidad todavia NO esta abierta en el producto;
- este bloque solo fija como debera verificarse en staging cuando la siguiente fase habilite una carga geoespacial real.

## Evidencia operativa A6 — Carga IGN/CNIG INSPIRE real (2026-03-17)

Validacion completa ejecutada en entorno local con datos reales IGN/CNIG NATCODE INSPIRE y municipio canonico 33044 (Oviedo).

Ficheros de entrada:

- `ign_parents.geojson` — 3 features: pais (`NATCODE=34`), CCAA Principado de Asturias (`NATCODE=3403`), provincia Asturias (`NATCODE=340333`)
- `ign_municipios.geojson` — 8132 features totales, 78 seleccionados con `autonomous_community_code=03`

Secuencia ejecutada:

1. Stack arrancado: `db`, `redis`, `migrate`, `api`, `worker`
2. Migraciones en head: `0012_cartographic_qa_incidents`
3. Carga de padres (`ign_parents.geojson`): 3/3 upsertados
4. Carga de municipios (`ign_municipios.geojson`, `--autonomous-community-code 03`): 78/78 upsertados

Comprobaciones SQL:

```
unit_level             | count
-----------------------+-------
autonomous_community   |     1
country                |     1
municipality           |    78
province               |     1

invalid_geoms          = 0
wrong_srid             = 0
orphan_municipalities  = 0
orphan_provinces       = 0
```

Cadena jerarquica recursiva para 33044:

```
depth | unit_level           | canonical_name         | code
------+----------------------+------------------------+-------
    0 | municipality         | Oviedo                 | 33044
    1 | province             | Asturias               | 33
    2 | autonomous_community | Principado de Asturias | 03
    3 | country              | Espana                 | ES
```

Smoke test (`scripts/smoke_stack.py --municipality-code 33044`):

```
[smoke] /health OK
[smoke] /health/ready OK
[smoke] /metrics OK
[smoke] /territorios/comunidades-autonomas OK
[smoke] /territorios/catalogo OK
[smoke] job encolado y completado
[smoke] /ine/series OK
[smoke] /territorios/municipio/33044/resumen OK
[smoke] informe municipal encolado y completado
[smoke] validacion completada
```

Veredicto A6: PASS
