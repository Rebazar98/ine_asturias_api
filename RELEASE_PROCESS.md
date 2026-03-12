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

## Secuencia obligatoria del RC

1. **Sincronizacion de dependencias**
   - revisar si `requirements.txt` ha cambiado;
   - si ha cambiado, reconstruir imagenes y regenerar `requirements.lock` desde un contenedor limpio;
   - confirmar que Dependabot no deja PRs de seguridad criticas sin revisar.

2. **Calidad de codigo**

```powershell
docker compose run --rm api python -m pip check
docker compose run --rm api ruff check .
docker compose run --rm api ruff format --check app/api app/core scripts main.py app/settings.py app/worker.py
```

3. **Test suite**

```powershell
docker compose run --rm api pytest
```

4. **Migraciones**

```powershell
docker compose run --rm migrate
```

5. **Smoke test**

```powershell
docker compose run --rm api python scripts/smoke_stack.py --base-url http://api:8000
```

6. **Restore drill reciente**
   - ejecutar [scripts/restore_drill.ps1](C:/Users/user/OneDrive/Documents/Playground/scripts/restore_drill.ps1) en local, o
   - ejecutar manualmente el workflow `Restore Drill` en GitHub Actions.

7. **Verificacion de endpoints criticos**

```powershell
Invoke-RestMethod http://127.0.0.1:8001/health
Invoke-RestMethod http://127.0.0.1:8001/health/ready
(Invoke-WebRequest http://127.0.0.1:8001/metrics -UseBasicParsing).StatusCode
```

8. **Verificacion de restore**

```powershell
docker compose run --rm api python scripts/verify_restore.py --base-url http://api:8000 --postgres-dsn postgresql://postgres:postgres@db:5432/ine_asturias --min-ingestion-rows 1 --min-normalized-rows 1
```

9. **Security scan reciente**
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

