# Release Candidate Process

Este documento formaliza el proceso minimo para declarar un release candidate (RC) operativo de `ine_asturias_api`.

## Alcance

El RC aplica sobre la base tecnica actual del proyecto:

- API FastAPI
- worker `arq`
- Redis
- PostgreSQL + PostGIS
- migraciones Alembic
- smoke test y restore drill

No cubre despliegue cloud ni nuevas integraciones funcionales.

## Requisitos previos

- Docker Desktop o Docker Engine operativo.
- `docker compose` disponible.
- `.env` derivado de `.env.example` o `--env-file` explicito.
- Backup reciente disponible en `backups/ine_asturias.dump` si se va a ejecutar el restore drill local.
- si el RC se valida sobre staging, staging DEBE cumplir previamente el criterio de aceptacion definido en [DEPLOYMENT_STAGING.md](C:/Users/user/OneDrive/Documents/Playground/DEPLOYMENT_STAGING.md).

## Comando operativo recomendado

La forma mas simple de ejecutar el RC en local es usar [scripts/release_candidate.ps1](C:/Users/user/OneDrive/Documents/Playground/scripts/release_candidate.ps1).

Sin restore drill en la misma ejecucion:

```powershell
.\scripts\release_candidate.ps1
```

Con restore drill completo dentro del mismo RC:

```powershell
.\scripts\release_candidate.ps1 -RunRestoreDrill -BackupPath backups/ine_asturias.dump
```

Para staging preparado:

```powershell
.\scripts\release_candidate.ps1 -EnvFile .env.staging.example -ProjectName ine_asturias_staging -BaseUrl http://127.0.0.1:8002
```

Para staging o RC con validacion analitica territorial completa:

```powershell
.\scripts\release_candidate.ps1 -EnvFile .env.staging.example -ProjectName ine_asturias_staging -BaseUrl http://127.0.0.1:8002 -MunicipalityCode 33044
```

Cuando se aporta `-MunicipalityCode`, el script siembra automaticamente un contexto minimo e idempotente de municipio e indicadores analiticos antes del smoke. Esto permite validar la ruta municipal en entornos locales o RC sin depender de que la base ya traiga `territorial_units` poblada.

Nota operativa para staging:

- tras un arranque completamente frio, la comprobacion canónica de migraciones sigue siendo `docker compose --env-file ... run --rm migrate`;
- si el `migrate` embebido en `up --build -d` falla por carrera de conexion al crear el volumen, el RC o ensayo de staging NO debe darse por fallido mientras la verificacion explicita posterior de `migrate` deje Alembic en `head`.

## Secuencia obligatoria del RC

1. **Sincronizacion de dependencias**
    - revisar si `requirements.txt` ha cambiado;
    - si ha cambiado, reconstruir imagenes y regenerar `requirements.lock` desde un contenedor limpio;
    - confirmar que Dependabot no deja PRs de seguridad criticas sin revisar.

2. **Versionado y changelog**
   - confirmar la version candidata actual en `APP_VERSION`;
   - actualizar [CHANGELOG.md](C:/Users/user/OneDrive/Documents/Playground/CHANGELOG.md) si el RC incorpora cambios relevantes desde la ultima entrada;
   - verificar que la version documentada en changelog y configuracion coincide con la release que se esta validando.

3. **Calidad de codigo**

```powershell
docker compose run --rm api python -m pip check
docker compose run --rm api ruff check .
docker compose run --rm api ruff format --check app/api app/core scripts main.py app/settings.py app/worker.py
```

4. **Test suite**

```powershell
docker compose run --rm api pytest
```

5. **Migraciones**

```powershell
docker compose run --rm migrate
```

6. **Smoke test**

```powershell
docker compose run --rm api python scripts/smoke_stack.py --base-url http://api:8000
```

Si la RC o staging tienen modelo territorial cargado y quieres validar tambien la nueva capa analitica:

```powershell
docker compose run --rm api python scripts/smoke_stack.py --base-url http://api:8000 --municipality-code 33044
```

Si se trata de una RC local sin modelo territorial cargado, puedes preparar ese contexto minimo manualmente con:

```powershell
docker compose run --rm api python scripts/seed_municipality_analytics.py --municipality-code 33044
```

Validacion manual opcional de la segunda fuente oficial, fuera del gate obligatorio:

```powershell
Invoke-RestMethod "http://127.0.0.1:8001/geocode?query=Oviedo" -Headers @{ "X-API-Key" = "change-me" }
Invoke-RestMethod "http://127.0.0.1:8001/reverse_geocode?lat=43.3614&lon=-5.8494" -Headers @{ "X-API-Key" = "change-me" }
```

Esta comprobacion sirve para RC o staging cuando se quiera validar el camino real de CartoCiudad. No sustituye a `pytest` ni entra como gate obligatorio porque depende de un proveedor externo.

Si el entorno ya tiene cobertura administrativa IGN cargada, puede añadirse esta comprobacion espacial semantica, tambien fuera del gate obligatorio:

```powershell
Invoke-RestMethod "http://127.0.0.1:8001/territorios/resolve-point?lat=43.3614&lon=-5.8494" -Headers @{ "X-API-Key" = "change-me" }
```

Resultado esperado:

- `source=internal.territorial.point_resolution`
- `result.best_match` relleno con la unidad mas especifica disponible
- `result.hierarchy` ordenada por niveles internos
- sin `geometry`, `centroid` ni GeoJSON en el contrato publico

Validacion manual opcional del bundle de exportacion territorial, tambien fuera del gate obligatorio:

```powershell
$job = Invoke-RestMethod "http://127.0.0.1:8001/territorios/export" `
  -Method Post `
  -Headers @{ "X-API-Key" = "change-me"; "Content-Type" = "application/json" } `
  -Body '{"unit_level":"municipality","code_value":"33044","format":"zip","include_providers":["territorial","ine","analytics"]}'
Invoke-RestMethod ("http://127.0.0.1:8001" + $job.status_path) -Headers @{ "X-API-Key" = "change-me" }
Invoke-WebRequest ("http://127.0.0.1:8001/territorios/exports/" + $job.job_id + "/download") -Headers @{ "X-API-Key" = "change-me" } -OutFile territorial_export_municipality_33044.zip
```

Resultado esperado:

- `job_type=territorial_export`
- estado final `completed`
- ZIP con `manifest.json` y `datasets/ine_series.ndjson`
- sin `geometry`, `centroid` ni payloads raw del proveedor en el bundle

7. **Restore drill reciente**
   - ejecutar [scripts/restore_drill.ps1](C:/Users/user/OneDrive/Documents/Playground/scripts/restore_drill.ps1) en local, o
   - ejecutar manualmente el workflow `Restore Drill` en GitHub Actions.

8. **Verificacion de endpoints criticos**

```powershell
Invoke-RestMethod http://127.0.0.1:8001/health
Invoke-RestMethod http://127.0.0.1:8001/health/ready
(Invoke-WebRequest http://127.0.0.1:8001/metrics -Headers @{ "X-API-Key" = "<API_KEY>" } -UseBasicParsing).StatusCode
```

Nota para el estado actual del proyecto:

- la validacion minima de RC sigue exigiendo `health`, `ready` y `metrics` como checks de disponibilidad del stack;
- la nueva capa territorial/geografica y analitica introducida en Fases 4 y 5 se da por cubierta en RC mediante:
  - `pytest`,
  - smoke test,
  - smoke test analitico con `--municipality-code` cuando staging tiene modelo territorial,
  - y la revalidacion operativa definida en F5-9.

9. **Verificacion de restore**

```powershell
docker compose run --rm api python scripts/verify_restore.py --base-url http://api:8000 --min-ingestion-rows 1 --min-normalized-rows 1 --min-catalog-rows 1 --expected-alembic-version 0007_territorial_exports --functional-operation-code 22
```

10. **Security scan reciente**
   - revisar el ultimo resultado de `Security Scan`, o
   - ejecutar manualmente el workflow `Security Scan` antes de cerrar el RC.

## Ejecucion manual de workflows y evidencia esperada

### Restore Drill

Ruta de ejecucion en GitHub:

1. `Actions`.
2. Seleccionar `Restore Drill`.
3. `Run workflow` sobre la rama candidata.
4. Abrir el run completado y revisar la seccion `Artifacts`.

Evidencia minima que DEBE recogerse:

- URL del workflow ejecutado.
- estado `Success` del job `restore-drill`.
- artefacto `restore-drill-artifacts` adjunto al run.
- dentro del log, paso `Run restore drill` en verde.
- dentro del artefacto:
  - `seed-smoke.txt`
  - `smoke-test.txt`
  - `verify-restore.txt`
  - `compose-ps.txt`
  - `api.log`
  - `worker.log`
  - `migrate.log`
  - `db.log`

### Security Scan

Ruta de ejecucion en GitHub:

1. `Actions`.
2. Seleccionar `Security Scan`.
3. `Run workflow` sobre la rama candidata.
4. Abrir el run completado y revisar la seccion `Artifacts`.

Evidencia minima que DEBE recogerse:

- URL del workflow ejecutado.
- estado final del job `app-image-scan`.
- artefacto 	rivy-report adjunto al run.
- fichero 	rivy-report.txt descargable desde el artefacto, incluso si el workflow termina en fallo por vulnerabilidades bloqueantes.
- si falla, confirmacion de si los hallazgos `HIGH` o `CRITICAL` bloquean la RC o quedan aceptados temporalmente con excepcion documentada.

Nota operativa: el workflow mantiene el escaneo mediante el contenedor oficial `aquasec/trivy` en vez de migrar a `aquasecurity/trivy-action`. La decision es deliberada para reducir dependencia de acciones JavaScript adicionales y limitar este workflow a `actions/checkout` y `actions/upload-artifact`, ya alineadas con Node 24.

## Archivado minimo de evidencia del RC

Para el primer RC real, el equipo DEBE conservar al menos:

- enlace al run de `Restore Drill`;
- enlace al run de `Security Scan`;
- artefacto `restore-drill-artifacts` descargado o retenido en GitHub Actions;
- artefacto `trivy-report` descargado o retenido en GitHub Actions;
- referencia a cualquier excepcion temporal registrada con [SECURITY_EXCEPTION_TEMPLATE.md](C:/Users/user/OneDrive/Documents/Playground/SECURITY_EXCEPTION_TEMPLATE.md).

## Politica minima de aceptacion de hallazgos

### Imagen de contenedor

- `CRITICAL`: bloquea la RC por defecto.
- `HIGH`: bloquea la RC por defecto.
- `MEDIUM`: puede aceptarse temporalmente si existe mitigacion razonable y excepcion documentada.
- `LOW` y `UNKNOWN`: no bloquean por si solos, pero deben revisarse cuando se toque la imagen o la dependencia afectada.

### Dependencias Python

- advisories o PRs de seguridad de Dependabot con severidad `CRITICAL` o `HIGH`: bloquean la RC por defecto.
- advisories `MEDIUM`: pueden aceptarse temporalmente con excepcion documentada.
- advisories `LOW`: no bloquean por si solos.

### Registro minimo de excepciones

Una excepcion temporal DEBE registrarse al menos en la PR o ticket de release con:

- componente afectado,
- identificador del hallazgo o paquete,
- severidad,
- motivo de aceptacion temporal,
- mitigacion existente,
- responsable,
- fecha limite de revision.

La fecha limite recomendada para excepciones temporales es de 30 dias maximo.

Plantilla recomendada: [SECURITY_EXCEPTION_TEMPLATE.md](C:/Users/user/OneDrive/Documents/Playground/SECURITY_EXCEPTION_TEMPLATE.md).

### Revalidacion de excepciones

Los hallazgos aceptados temporalmente DEBEN revalidarse:

- en el siguiente RC,
- tras cambios de imagen base,
- tras cambios de `requirements.txt` o `requirements.lock`,
- o cuando Dependabot proponga una correccion viable.

## Politica de actualizacion de dependencias

- Las actualizaciones se aceptan solo si pasan `pip check`, `ruff`, `pytest`, migraciones y smoke test.
- Si cambia `requirements.txt`, `requirements.lock` DEBE regenerarse en el mismo cambio.
- Los bumps de imagenes base o dependencias via Dependabot DEBEN tratarse como cambios de release candidate, no como merges ciegos.
- Tras aceptar updates de dependencias, DEBE repetirse la secuencia completa del RC.

## Workflows manuales asociados

- `CI`: valida lint, tests, migraciones y smoke test en cada push y pull request.
- `Restore Drill`: ejecuta un restore drill completo bajo demanda en CI.
- `Security Scan`: escaneo ligero manual o semanal de la imagen API con Trivy.

## Rutina de continuidad post-RC

La aceptacion de un RC no elimina la necesidad de volver a validar el estado operativo del repositorio. Esta rutina existe para confirmar que la base post-RC sigue estable tras cambios de workflow, dependencias, imagenes o runtime.

### Cuando DEBE repetirse

Se DEBE repetir `Restore Drill` y `Security Scan` cuando ocurra cualquiera de estos cambios:

- cambio en `.github/workflows/ci.yml`
- cambio en `.github/workflows/restore-drill.yml`
- cambio en `.github/workflows/security-scan.yml`
- cambio en `Dockerfile`
- cambio en `docker-compose.yml`
- cambio en `requirements.txt`
- cambio en `requirements.lock`
- cambio de imagen base o digest de Python, PostgreSQL/PostGIS o Redis
- actualizaciones de Dependabot que afecten dependencias Python, Docker o GitHub Actions

### Secuencia recomendada

1. ejecutar `CI` sobre la rama candidata o rama principal segun corresponda;
2. ejecutar manualmente `Restore Drill` en GitHub Actions;
3. revisar el artefacto `restore-drill-artifacts`;
4. ejecutar manualmente `Security Scan` en GitHub Actions;
5. revisar el artefacto `trivy-report`;
6. registrar la continuidad en la evidencia operativa del repositorio o de la release.

### Evidencia minima de continuidad

Cada repeticion de continuidad DEBE dejar:

- URL del run de `CI`
- URL del run de `Restore Drill`
- URL del run de `Security Scan`
- artefacto `restore-drill-artifacts` revisado
- artefacto `trivy-report` revisado
- decision explicita sobre si la continuidad queda:
  - aprobada sin reservas
  - aprobada con excepcion temporal documentada
  - bloqueada

### Criterio de continuidad aprobada

La continuidad post-RC se considera aprobada solo si:

- `CI` termina en verde;
- `Restore Drill` termina en `Success`;
- `Security Scan` no deja hallazgos `HIGH` o `CRITICAL` sin decision explicita;
- no aparecen regresiones operativas en `/health`, `/health/ready`, smoke test o integridad restaurada;
- cualquier excepcion temporal queda registrada con [SECURITY_EXCEPTION_TEMPLATE.md](C:/Users/user/OneDrive/Documents/Playground/SECURITY_EXCEPTION_TEMPLATE.md).

## Primer RC real aceptable

El primer RC real puede considerarse aceptable solo si existe evidencia de que:

- [scripts/release_candidate.ps1](C:/Users/user/OneDrive/Documents/Playground/scripts/release_candidate.ps1) ha terminado correctamente en local o staging controlado;
- el workflow `Restore Drill` ha terminado en `Success` y ha dejado `restore-drill-artifacts` revisable;
- el workflow `Security Scan` ha sido ejecutado y su artefacto `trivy-report` ha sido revisado;
- no existen hallazgos `HIGH` o `CRITICAL` sin decision explicita;
- cualquier excepcion temporal esta registrada con [SECURITY_EXCEPTION_TEMPLATE.md](C:/Users/user/OneDrive/Documents/Playground/SECURITY_EXCEPTION_TEMPLATE.md);
- `/health`, `/health/ready` y `/metrics` han sido confirmados durante la validacion.

## Criterio minimo de aprobacion del RC

Un RC se considera aceptable solo si:

- lint y format estan en verde;
- `pytest` esta en verde;
- `alembic upgrade head` es idempotente;
- el smoke test pasa;
- existe restore drill exitoso reciente;
- `/health`, `/health/ready` y `/metrics` responden correctamente;
- existe `Security Scan` reciente revisado;
- no hay hallazgos `HIGH` o `CRITICAL` abiertos sin decision explicita.

## Condicion de entrada para abrir nuevas fuentes

La aceptacion de un RC NO implica por si sola que el proyecto este listo para integrar una fuente nueva.

Antes de abrir CartoCiudad, IGN o cualquier nueva fuente externa, el repositorio DEBE cumplir ademas estas condiciones:

- staging cumple el criterio de "staging operativo real" definido en [DEPLOYMENT_STAGING.md](C:/Users/user/OneDrive/Documents/Playground/DEPLOYMENT_STAGING.md);
- la continuidad post-RC ha sido revalidada cuando haya cambios de workflows, imagenes o dependencias;
- `/ine/series` mantiene un contrato semantico estable y probado;
- la estrategia de cruce con `territorial_unit_codes` ya esta documentada y asumida como base canonica;
- no hay regresiones abiertas en ingesta, normalizacion, catalogo o jobs del dominio INE.

Si una de estas condiciones no se cumple, la siguiente fase debe permanecer cerrada y el equipo debe priorizar estabilizacion antes que expansion funcional.

Estado actual de esa condicion:

- CartoCiudad ya queda consolidada como segunda fuente oficial en modo bajo demanda;
- IGN/CNIG administrativo directo ya queda integrado como carga interna versionable para enriquecer `territorial_units`;
- la siguiente apertura recomendada ya no es otra fuente HTTP aislada, sino capa B2B (`API keys`, auditoria y cuotas) o endpoints espaciales semanticos apoyados en el modelo territorial enriquecido.

### Validacion manual opcional de IGN administrativo

Cuando un RC quiera dejar evidencia de la carga administrativa directa, puede ejecutarse esta comprobacion manual fuera del gate obligatorio:

```bash
docker compose run --rm api python scripts/load_ign_admin_boundaries.py --pretty
```

Si se prefiere fijar un snapshot concreto sin depender de la variable de entorno:

```bash
docker compose run --rm api python scripts/load_ign_admin_boundaries.py --input-path /app/data/ign_asturias_boundaries.zip --pretty
```

Evidencia esperada:

- `source=ign_administrative_boundaries`
- `features_upserted > 0`
- `raw_records_saved > 0`
- `/territorios/catalogo` reflejando `boundary_source=ign_administrative_boundaries` y conteo de `geometry_units`

## Evidencia operativa reciente

La actualizacion de F5-9 deja formalizado el criterio de continuidad para la nueva capa analitica:

- el RC sigue exigiendo `/health`, `/health/ready`, `/metrics`, smoke y restore verification;
- cuando staging o RC tienen un municipio canonico disponible, el smoke debe ejecutarse tambien con `--municipality-code` para validar:
  - `GET /territorios/catalogo`
  - `GET /territorios/municipio/{codigo_ine}/resumen`
  - `POST /territorios/municipio/{codigo_ine}/informe`
  - polling correcto en `/territorios/jobs/{job_id}`;
- si esa validacion analitica no puede ejecutarse por falta de modelo territorial cargado, la RC puede seguir validando disponibilidad general, pero F5-9 no debe darse por revalidada plenamente en ese entorno.

