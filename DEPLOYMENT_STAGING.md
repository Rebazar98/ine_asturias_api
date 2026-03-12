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
curl http://127.0.0.1:8002/metrics
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

Resultado esperado:

- `/health OK`
- `/health/ready OK`
- `/metrics OK`
- job corto completado
- `/ine/series OK`
- `validacion completada`

## Evidencia minima

Cada despliegue de staging que se use como ensayo DEBE dejar, como minimo:

- comando de despliegue utilizado
- `docker compose ps`
- confirmacion de migraciones en `head`
- respuesta satisfactoria de `/health`
- respuesta satisfactoria de `/health/ready`
- smoke test en verde

Evidencia recomendada adicional:

- logs de `api`
- logs de `worker`
- fecha y version (`APP_VERSION`) del ensayo

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
docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm api python scripts/verify_restore.py --base-url http://api:8000 --postgres-dsn postgresql://postgres:change-me@db:5432/ine_asturias --min-ingestion-rows 1 --min-normalized-rows 1
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
curl http://127.0.0.1:8002/metrics
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
docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm api python scripts/verify_restore.py --base-url http://api:8000 --postgres-dsn postgresql://postgres:change-me@db:5432/ine_asturias --min-ingestion-rows 1 --min-normalized-rows 1
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

## Evidencia operativa F4-9

La revalidacion de F4-9 se ha ejecutado sobre el mismo patron de staging aislado, ya con la primera capa territorial/geografica publica integrada:

- `--env-file .env.staging.local`
- `-p ine_asturias_staging`

Resultado validado:

- `docker compose ... up --build -d` levanta `api`, `db`, `redis` y `worker` correctamente;
- `docker compose ... run --rm migrate` deja Alembic en `0005_geocoding_cache`;
- `/health` devuelve `200`;
- `/health/ready` devuelve `200` con `postgres=ok`, `redis=ok` y `worker=ok`;
- `/metrics` devuelve `200`;
- el smoke test completa con job real y `/ine/series OK`;
- `verify_restore.py` confirma:
  - `alembic_version=0005_geocoding_cache`
  - `ingestion_raw=6`
  - `ine_series_normalized=75`
  - `/health OK`
  - `/health/ready OK`
  - `/ine/series total=75`

Lectura operativa:

- la primera capa territorial/geografica no rompe deploy, migraciones, health, smoke ni restore verification;
- la validacion de staging sigue centrada en estabilidad del stack y persistencia;
- la cobertura especifica de `/geocode`, `/reverse_geocode` y `/municipio/{codigo_ine}` sigue soportada por la suite de tests y por los ensayos locales/integrados del proyecto.

## Criterio de aceptacion de staging operativo real

Staging queda aceptado como entorno operativo real solo si se cumplen TODAS estas condiciones:

1. existe un fichero de entorno propio de staging fuera del repositorio, alineado con `.env.staging.example`;
2. el stack puede levantarse con un unico comando reproducible de `docker compose`;
3. `migrate` termina correctamente y deja Alembic en `head`;
4. `/health`, `/health/ready` y `/metrics` responden correctamente;
5. el smoke test pasa usando la topologia real de staging;
6. existe un procedimiento de rollback documentado y ejecutable;
7. existe un procedimiento de restore verification documentado y reutilizable;
8. la evidencia minima del ensayo queda registrada;
9. el equipo no necesita pasos no documentados para repetir el flujo.

## Gate operativo recomendado

Antes de considerar staging como "listo para ensayo real", revisa esta checklist:

- `docker compose --env-file .env.staging.local -p ine_asturias_staging up --build -d`
- `docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm migrate`
- `curl http://127.0.0.1:8002/health`
- `curl http://127.0.0.1:8002/health/ready`
- `curl http://127.0.0.1:8002/metrics`
- `docker compose --env-file .env.staging.local -p ine_asturias_staging run --rm api python scripts/smoke_stack.py --base-url http://api:8000`
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
