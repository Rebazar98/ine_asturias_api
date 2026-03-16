# Fase 2: Staging Operativo Real

Este documento fija el contexto operativo de la siguiente etapa del proyecto tras la aceptacion de RC-001. Su objetivo es convertir la base tecnica ya validada en un entorno de staging realmente util para ensayo de release, sin abrir todavia nuevas integraciones externas ni nuevas lineas funcionales.

## Estado de partida

La base actual ya dispone de:

- Docker Compose estable
- Alembic idempotente
- Redis + worker operativos
- smoke test funcional
- restore drill y security scan operativos
- CI minima estable
- proceso de RC documentado
- evidencia local y remota validada en `ACTA_RC1.md`

## Objetivo de Fase 2

La fase correcta no es ampliar producto. La fase correcta es:

1. cerrar completamente el bloque final de release readiness;
2. convertir staging en entorno de ensayo real;
3. consolidar el dominio INE antes de nuevas fuentes;
4. dejar preparado, solo a nivel de diseno, el encaje de futuras integraciones.

## Bloques de trabajo

### Bloque A - Cierre final de release readiness

- A1. Actualizar workflows por compatibilidad futura de GitHub Actions.
- A2. Documentar la resolucion operativa del `collation version mismatch`.
- A3. Definir rutina de continuidad post-RC.
- A4. Introducir versionado semantico y `CHANGELOG.md`.

### Bloque B - Staging operativo real

- B1. Crear `DEPLOYMENT_STAGING.md`.
- B2. Documentar rollback de staging.
- B3. Endurecer la separacion real de secretos.
- B4. Definir ensayo completo de staging.
- B5. Definir criterio de "staging operativo real".

### Bloque C - Consolidacion del dominio INE

- C1. Estabilizar el contrato semantico de `/ine/series`.
- C2. Separar mejor endpoints semanticos de endpoints de ingestion.
- C3. Normalizar filtros territoriales y temporales.
- C4. Confirmar la estrategia de cruce con el modelo territorial.
- C5. Reforzar tests de integracion del dominio INE.

### Bloque D - Preparacion de futuras integraciones

- D1. Definir patron de adapter para CartoCiudad / IGN.
- D2. Definir condicion de entrada para abrir nuevas fuentes.

## Orden recomendado

1. Bloque A
2. Bloque B
3. Bloque C
4. Bloque D

## Sprinting recomendado

### Sprint 1

- A1
- A2
- A3
- A4
- B1
- B2
- B3
- B4
- B5

### Sprint 2

- C1
- C2
- C3
- C4
- C5
- D1
- D2

## Estado de implementacion

- A1: completado
- A2: completado
- A3: completado
- A4: completado
- B1: completado
- B2: completado
- B3: completado
- B4: completado
- B5: completado
- C1: completado
- C2: completado
- C3: completado
- C4: completado
- C5: completado
- D1: completado
- D2: completado
