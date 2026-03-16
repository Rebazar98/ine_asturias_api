# Acta RC-001

## Estado

Fecha de cierre: 12 de marzo de 2026

Repositorio: `ine_asturias_api`

Decision: **RC-001 aceptable como primer Release Candidate operativo**

## Evidencia validada

- Stack validado end-to-end con Docker Compose.
- `pytest` en verde.
- Migraciones Alembic idempotentes.
- `scripts/release_candidate.ps1` ejecutado correctamente.
- Workflow `Restore Drill` ejecutado con exito en GitHub Actions.
- Artefacto `restore-drill-artifacts` revisado:
  - `seed-smoke.txt`
  - `smoke-test.txt`
  - `verify-restore.txt`
  - `compose-ps.txt`
  - `api.log`
  - `worker.log`
  - `migrate.log`
  - `db.log`
- Workflow `Security Scan` ejecutado y artefacto `trivy-report` revisado.
- Resultado de seguridad del primer RC:
  - `HIGH: 0`
  - `CRITICAL: 0`

## Conclusion operativa

El repositorio dispone ya de una base tecnica y operativa suficientemente estable para declarar un primer RC interno. La validacion local y la validacion remota en GitHub Actions son coherentes entre si y no se han detectado bloqueos tecnicos ni hallazgos de seguridad de severidad alta o critica que impidan el avance.

Este RC NO implica aun produccion real. Implica que el proyecto ya tiene:

- proceso de build reproducible,
- migraciones controladas,
- validacion automatizada minima,
- rutina de recuperacion probada,
- escaneo basico de seguridad,
- y criterio documentado de aceptacion de release candidate.

## Siguiente fase recomendada

La evolucion inmediata del proyecto deberia seguir este orden:

### 1. Cierre de release readiness

Objetivo: dejar el proyecto listo para una primera preproduccion controlada.

- Actualizar los workflows de GitHub Actions afectados por el warning de Node 20 cuando exista una version estable compatible.
- Resolver de forma planificada el `collation version mismatch` de PostgreSQL.
- Ejecutar un segundo `Restore Drill` y un segundo `Security Scan` como verificacion de continuidad, no solo como prueba inicial.
- Definir un criterio simple de versionado de release (`v0.x.y`) y una rutina de changelog/release notes.

### 2. Preparacion de entorno de staging operativo

Objetivo: pasar de staging "arrancable" a staging "usable como ensayo de release".

- Fijar una checklist de despliegue y rollback.
- Dejar una configuracion de staging con secretos reales fuera del repositorio.
- Hacer un ensayo completo de despliegue + migracion + smoke test + rollback controlado.

### 3. Consolidacion funcional del dominio INE

Objetivo: endurecer el valor de la API antes de abrir nuevas fuentes.

- Mejorar endpoints semanticos sobre datos ya normalizados.
- Refinar filtros territoriales y paginacion.
- Consolidar el modelo territorial comun sobre la base PostGIS ya preparada.
- Aumentar cobertura de tests de integracion reales sobre repositorios y jobs.

### 4. Apertura de la siguiente integracion

Objetivo: iniciar la siguiente fase de plataforma territorial solo cuando la base operativa ya sea estable.

Orden recomendado:

1. modelo territorial comun utilizable;
2. CartoCiudad / IGN como primera fuente geoespacial nueva;
3. consultas espaciales y enriquecimiento territorial;
4. automatizacion mas avanzada con n8n y flujos aguas abajo.

## Criterio de continuidad

La siguiente ronda del proyecto no deberia empezar por anadir nuevas features grandes. Deberia empezar por convertir este RC-001 en una base repetible de preproduccion:

- repetir evidencias,
- cerrar los ultimos riesgos operativos,
- y solo despues ampliar el dominio funcional.

