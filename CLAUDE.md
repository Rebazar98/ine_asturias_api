# CLAUDE.md

## Referencia arquitectónica

Las reglas de arquitectura, capas, convenciones y prohibiciones para agentes están en **AGENTS.md**. Léelo antes de hacer cambios estructurales.

---

## Qué es este proyecto

Backend FastAPI de integración de datos territoriales españoles. Integra INE, CartoCiudad, IGN/CNIG y Catastro en una plataforma unificada con contratos semánticos propios, trazabilidad e idempotencia.

**Versión actual:** `0.2.0-rc1`
**Stack:** Python 3.12, FastAPI, PostgreSQL 16 + PostGIS, Redis + arq, Alembic, Docker

---

## Comandos habituales

```bash
# Stack completo (API en :8001, PostgreSQL en :5433, Redis en :6379)
docker compose up --build

# Solo migraciones
docker compose run --rm migrate

# Bootstrap seguro (base heredada o nueva)
python scripts/bootstrap_alembic.py
alembic upgrade head

# Tests unitarios (sin red real, requiere stack levantado)
docker compose run --rm api pytest -m "not integration"

# Tests de integración (requiere PostgreSQL y Redis activos)
docker compose run --rm api pytest -m integration

# Smoke test del stack completo
python scripts/smoke_stack.py

# Linting (disponible localmente)
ruff check .
ruff format .
```

---

## Estructura del código

```
app/
├── api/          # Routers FastAPI (HTTP puro, sin lógica de negocio)
├── services/     # Lógica de dominio y adapters de proveedores externos
├── repositories/ # Persistencia SQLAlchemy (toda escritura/lectura a PG)
├── core/         # Infraestructura transversal (logging, cache, jobs, métricas)
├── models.py     # ORM SQLAlchemy
├── schemas.py    # Contratos Pydantic entrada/salida
├── settings.py   # Configuración por entorno
└── worker.py     # Worker arq para jobs asíncronos
alembic/          # Migraciones versionadas
scripts/          # Utilidades operativas
```

---

## Reglas críticas (resumen)

- Los routers **no** contienen lógica de negocio ni acceden a SQLAlchemy directamente.
- Los adapters de proveedor (INE, CartoCiudad, IGN, Catastro) viven en `app/services` y **no** escriben en base de datos.
- Todo cambio de esquema **requiere** migración Alembic versionada.
- La API pública expone contratos semánticos propios, **nunca** el shape crudo del proveedor.
- Las operaciones costosas usan jobs asíncronos con arq/Redis (`202 + job_id + polling`).
- Toda persistencia normalizada es idempotente mediante upsert por clave lógica.

---

## Variables de entorno necesarias

```env
DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5433/ine_asturias
REDIS_URL=redis://localhost:6379
API_KEY=<clave-de-acceso>
ENVIRONMENT=development   # development | staging | production
```

---

## Estado de fases

| Fase | Estado |
|------|--------|
| 1 - INE consolidación | Completa |
| 2 - Staging y endurecimiento | Completa (RC1) |
| 3 - Modelo territorial + PostGIS | Completa |
| 4 - CartoCiudad + IGN administrativo | Completa |
| 5 - Catastro, analítica y exportación | Completa |
| 6 - Fallback series-directo + cobertura de repositorios | Completa (RC2) |
| 7 - Endpoints territoriales, sync status, QA cartográfica | Completa |
| 8+ - Nuevas fuentes, analítica avanzada, publicación | Pendiente |

---

## Permisos

Los permisos pre-aprobados están en `.claude/settings.json`. Requieren aprobación manual: `git push`, `git reset --hard`, `git rebase`, `rm -rf`, `sudo` y operaciones destructivas de Docker. Todo lo demás (ediciones, git, pytest, ruff, alembic, docker compose, scripts) es automático.

---

## Mantenimiento de documentación

Tras cualquier modificación significativa (nueva funcionalidad, cambio arquitectónico, nueva fase, cambio de convenciones), Claude **debe**:

1. Actualizar `DOCUMENTACION_EVOLUCION_PROYECTO.txt` añadiendo una entrada con fecha, descripción del cambio y decisiones relevantes tomadas.
2. Actualizar este `CLAUDE.md` si el cambio afecta a comandos, estructura, estado de fases o reglas críticas.
3. Actualizar `AGENTS.md` si el cambio afecta a convenciones arquitectónicas, nuevas capas o nuevos providers.
4. Actualizar `CHANGELOG.md` si el cambio constituye una entrega o versión relevante.

Los documentos en `docs/archive/` son históricos y **no** deben modificarse.

---

## Skills disponibles

Hay skills especializadas en `C:\Users\user\OneDrive\Desktop\codex_skills_dev\`. Cada skill tiene un `SKILL.md` con principios, reglas y checklist, y una carpeta `examples/` con código de referencia.

| Skill | Cuándo usarla |
|-------|---------------|
| `fastapi_endpoint` | Al crear o modificar endpoints en `app/api/` |
| `api_client_generation` | Al crear o refactorizar un adapter de proveedor externo en `app/services/` (INE, CartoCiudad, IGN, Catastro, o cualquier fuente nueva) |
| `data_ingestion_pipeline` | Al trabajar en flujos de ingesta (extract → raw → validate → normalize → upsert) |
| `postgres_query` | Al escribir o revisar queries SQL en `app/repositories/` |
| `postgis_query` | Al escribir queries espaciales con PostGIS (intersecciones, SRID, geometrías, centroides) |
| `alembic_migration` | Al crear o revisar migraciones Alembic (naming, downgrade, GIST, async engine) |
| `arq_worker_job` | Al añadir o modificar jobs en `app/worker.py` (mark_running, progress, complete/fail) |
| `pytest_fastapi` | Al escribir tests de endpoints, servicios o repositorios (dummy repos, dependency_overrides, anyio) |

La carpeta `agent_feedback/` contiene plantillas para registrar errores recurrentes y mejoras observadas en el uso de las skills.

## Slash commands disponibles

Comandos personalizados en `.claude/commands/` invocables con `/nombre`:

| Comando | Qué hace |
|---------|----------|
| `/check` | Ejecuta ruff check, ruff format --check y pytest. Reporta todo al final |
| `/smoke` | Lanza `scripts/smoke_stack.py` y diagnostica fallos |
| `/migration <descripción>` | Crea una migración Alembic siguiendo las convenciones del proyecto |

---

## Documentación relevante

| Archivo | Contenido |
|---------|-----------|
| `AGENTS.md` | Reglas completas de arquitectura para agentes |
| `DOCUMENTACION_EVOLUCION_PROYECTO.txt` | Registro histórico y técnico del proyecto **(actualizar tras cambios)** |
| `PLAN_TECNICO_PLATAFORMA_DATOS_TERRITORIALES.md` | Visión y arquitectura objetivo |
| `CHANGELOG.md` | Historial de versiones |
| `DEPLOYMENT_STAGING.md` | Guía de staging |
| `MONITORING_SLOS.md` | SLOs y alertas Prometheus |
| `RELEASE_PROCESS.md` | Proceso de release y candidatos |
| `docs/archive/` | Backlogs de fases completadas, planes ejecutados y actas históricas |
