# Fase 3: Modelo Territorial Operativo y Base Geoespacial Interna

Este documento convierte la siguiente etapa del proyecto en un backlog ejecutable tras el cierre completo de la Fase 2.

La Fase 3 NO abre todavia nuevas integraciones externas. Su objetivo es consolidar el nucleo territorial ya introducido en la base, alinear el dominio INE con ese modelo y dejar preparada la base geoespacial interna antes de abrir CartoCiudad o IGN.

## Objetivo general

La Fase 3 debe dejar resueltos estos frentes:

1. hacer operativo el modelo territorial actual;
2. fijar una estrategia clara de codigo territorial canonico;
3. alinear `/ine/series` con el nucleo territorial sin romper compatibilidad;
4. preparar PostGIS para uso real sin abrir aun nuevas fuentes;
5. reforzar la validacion de staging sobre esta nueva base.

## Principios de ejecucion

- No introducir nuevas fuentes de datos.
- No abrir nuevos endpoints funcionales de producto si no son estrictamente necesarios para el dominio territorial interno.
- No cambiar la arquitectura principal.
- Priorizar trazabilidad, compatibilidad y criterio de produccion.
- Todo cambio de esquema debe ir acompanado de migracion Alembic.

## Backlog ejecutable

### F3-1. Fijar codigo territorial canonico por nivel

**Objetivo**

Definir que codigo sera el identificador canonico para pais, comunidad autonoma, provincia y municipio dentro del modelo territorial.

**Archivos a tocar**

- `app/models.py`
- `app/repositories/territorial.py`
- `AGENTS.md`
- `README.md`

**Dependencias**

- Ninguna

**Tareas**

- revisar `territorial_units`, `territorial_unit_codes` y `territorial_unit_aliases`;
- decidir el papel de `source_system`, `code_type` e `is_primary`;
- documentar que codigo sera el canonico por nivel;
- dejar claro como convive el codigo canonico con los codigos externos del INE.

**Criterio de validacion**

- existe una decision explicita y documentada sobre el codigo canonico por nivel;
- no queda ambiguedad entre codigo interno, codigo INE y alias;
- la documentacion y la persistencia apuntan a la misma estrategia.

**Prioridad**

- Alta

### F3-2. Endurecer el repositorio territorial para lookup y cruce

**Objetivo**

Convertir el repositorio territorial en una pieza util para consultas y cruces semanticos reales.

**Archivos a tocar**

- `app/repositories/territorial.py`
- `app/schemas.py`
- `tests/integration`

**Dependencias**

- F3-1

**Tareas**

- anadir lookups por codigo canonico;
- mantener lookups por codigo externo INE;
- anadir lookups por alias normalizado;
- devolver estructuras estables y tipadas para consumo por servicios o endpoints.

**Criterio de validacion**

- el repositorio resuelve unidades por codigo y alias de forma consistente;
- existe cobertura automatica de los casos principales;
- el repositorio deja de ser solo preparatorio y pasa a ser utilizable por dominio.

**Prioridad**

- Alta

### F3-3. Definir reglas de matching territorial y aliases

**Objetivo**

Evitar heuristicas dispersas de nombres y dejar reglas claras para matching territorial.

**Archivos a tocar**

- `app/models.py`
- `app/repositories/territorial.py`
- `AGENTS.md`
- `DOCUMENTACION_EVOLUCION_PROYECTO.txt`

**Dependencias**

- F3-1

**Tareas**

- definir cuando un nombre es canonico;
- definir cuando un nombre debe ir a aliases;
- definir normalizacion minima de nombres;
- documentar que el matching territorial no debe vivir en routers.

**Criterio de validacion**

- existe una regla clara de alias, canonical name y display name;
- la estrategia queda documentada y es coherente con el repositorio territorial;
- no depende de matching ad hoc en modulos externos.

**Prioridad**

- Alta

### F3-4. Alinear `/ine/series` con el modelo territorial

**Objetivo**

Permitir que el endpoint semantico del INE se cruce limpiamente con el modelo territorial sin romper compatibilidad actual.

**Archivos a tocar**

- `app/api/routes_ine.py`
- `app/schemas.py`
- `app/repositories/series.py`
- `app/repositories/territorial.py`
- `tests/test_series_endpoint.py`

**Dependencias**

- F3-2
- F3-3

**Tareas**

- revisar el papel de `geography_code_system=ine`;
- preparar una via clara de cruce con `territorial_unit_codes`;
- mantener compatibilidad de `/ine/series`;
- documentar el contrato actualizado.

**Criterio de validacion**

- `/ine/series` sigue funcionando con el contrato actual;
- el cruce territorial queda definido de forma clara;
- la documentacion y los tests reflejan el modelo final.

**Prioridad**

- Alta

### F3-5. Revisar la persistencia normalizada para soporte territorial futuro

**Objetivo**

Decidir si `ine_series_normalized` debe seguir basandose solo en `geography_code` externo o si necesita referencia territorial interna.

**Archivos a tocar**

- `app/models.py`
- `alembic/`
- `app/repositories/series.py`
- `CHANGELOG.md`

**Dependencias**

- F3-1
- F3-4

**Tareas**

- evaluar el impacto de una referencia interna a unidad territorial;
- decidir si hace falta cambio de esquema;
- si hace falta, disenar migracion y estrategia de backfill;
- documentar la decision.

**Criterio de validacion**

- existe una estrategia clara y documentada de persistencia territorial futura;
- cualquier cambio de esquema viene acompanado de migracion;
- no se introduce deuda de deduplicacion.

**Prioridad**

- Media-Alta

### F3-6. Hacer operativo PostGIS con convenciones claras

**Objetivo**

Pasar de "PostGIS habilitado" a "PostGIS preparado para uso real" con reglas tecnicas claras.

**Archivos a tocar**

- `app/models.py`
- `alembic/`
- `README.md`
- `PLAN_TECNICO_PLATAFORMA_DATOS_TERRITORIALES.md`

**Dependencias**

- F3-1

**Tareas**

- confirmar SRID canonico;
- confirmar tipos geometricos esperados;
- definir politica de nulabilidad de `geometry` y `centroid`;
- documentar convenciones de uso.

**Criterio de validacion**

- el proyecto deja claras sus convenciones espaciales base;
- cualquier nueva carga futura sabra que formato y SRID debe respetar;
- no se abren consultas espaciales nuevas todavia.

**Prioridad**

- Media

### F3-7. Definir contrato de carga geoespacial futura

**Objetivo**

Dejar preparado como deberan cargarse geometria y centroides cuando toque abrir la siguiente fase geografica.

**Archivos a tocar**

- `PLAN_TECNICO_PLATAFORMA_DATOS_TERRITORIALES.md`
- `AGENTS.md`
- `DEPLOYMENT_STAGING.md`

**Dependencias**

- F3-6

**Tareas**

- documentar formato esperado de carga;
- documentar validaciones minimas de geometria;
- documentar que no se abre aun una fuente geografica nueva;
- documentar como debera verificarse en staging.

**Criterio de validacion**

- existe una guia clara para futuras cargas espaciales;
- la fase siguiente no tendra que improvisar estas reglas.

**Prioridad**

- Media

### F3-8. Reforzar tests de integracion territoriales

**Objetivo**

Aumentar cobertura automatica sobre lookup territorial, cruce semantico y regresiones del dominio INE.

**Archivos a tocar**

- `tests/integration`
- `tests/test_series_endpoint.py`
- `tests/test_normalizers.py`
- `tests/conftest.py`

**Dependencias**

- F3-2
- F3-4

**Tareas**

- anadir tests de lookup por codigo;
- anadir tests de lookup por alias;
- anadir tests del cruce con `/ine/series`;
- reforzar regresiones del dominio territorial.

**Criterio de validacion**

- hay cobertura adicional sobre el nucleo territorial;
- los tests detectan regresiones del cruce con INE;
- la suite sigue siendo ejecutable en Docker y CI.

**Prioridad**

- Alta

### F3-9. Ensayo de staging con modelo territorial

**Objetivo**

Revalidar staging una vez endurecido el modelo territorial y la base geoespacial interna.

**Archivos a tocar**

- `DEPLOYMENT_STAGING.md`
- `RELEASE_PROCESS.md`
- `DOCUMENTACION_EVOLUCION_PROYECTO.txt`

**Dependencias**

- F3-4
- F3-6
- F3-8

**Tareas**

- repetir deploy, migrate, health y smoke;
- validar que el modelo territorial no rompe restore verification;
- registrar evidencia operativa;
- actualizar criterio de staging si hace falta.

**Criterio de validacion**

- staging sigue siendo operativo tras la consolidacion territorial;
- no aparecen regresiones en runtime ni en restore.

**Prioridad**

- Media-Alta

## Orden recomendado

1. F3-1
2. F3-2
3. F3-3
4. F3-4
5. F3-8
6. F3-5
7. F3-6
8. F3-7
9. F3-9

## Propuesta de sprint

### Sprint 1

- F3-1
- F3-2
- F3-3
- F3-4
- F3-8

Objetivo:

Dejar operativo el modelo territorial y su cruce semantico con el dominio INE.

### Sprint 2

- F3-5
- F3-6
- F3-7
- F3-9

Objetivo:

Cerrar la base geoespacial interna y revalidar staging antes de abrir futuras integraciones.

## Estado de implementacion

- F3-1: completado
- F3-2: completado
- F3-3: completado
- F3-4: completado
- F3-5: completado
- F3-6: completado
- F3-7: completado
- F3-8: completado
- F3-9: completado
