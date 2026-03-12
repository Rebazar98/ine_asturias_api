# Fase 4: API Territorial Unificada y Primera Integracion Geografica

Este documento convierte la siguiente etapa del proyecto en un backlog ejecutable tras el cierre completo de la Fase 3.

La Fase 4 YA puede abrir la primera integracion geografica, pero debe hacerlo de forma controlada y sin romper la base estabilizada en las fases anteriores. El objetivo es usar el modelo territorial, PostGIS y el dominio INE ya consolidados para introducir una capa semantica territorial util y un primer adapter geografico limpio.

## Objetivo general

La Fase 4 debe dejar resueltos estos frentes:

1. abrir una API territorial semantica sobre el modelo interno;
2. introducir el primer adapter geografico siguiendo el patron definido en la fase anterior;
3. materializar cache persistente para geocodificacion y reverse geocoding;
4. alinear resultados geograficos con `territorial_units` y `territorial_unit_codes`;
5. revalidar staging y el proceso de RC con esta nueva capacidad.

## Principios de ejecucion

- Abrir solo una nueva familia funcional a la vez.
- Mantener contratos semanticos internos y NO exponer el shape crudo del proveedor.
- Reutilizar services, repositories y criterios de staging ya consolidados.
- Toda nueva persistencia debe llegar con migracion Alembic.
- Toda nueva capacidad publica debe venir con tests y evidencia operativa.

## Backlog ejecutable

### F4-1. Definir contrato semantico de geocodificacion

**Objetivo**

Fijar el contrato interno y publico de geocodificacion y reverse geocoding antes de escribir el adapter.

**Archivos a tocar**

- `app/schemas.py`
- `README.md`
- `AGENTS.md`

**Dependencias**

- Ninguna

**Tareas**

- definir respuesta semantica de `/geocode`;
- definir respuesta semantica de `/reverse_geocode`;
- decidir campos minimos obligatorios y metadata adicional;
- documentar que el contrato publico no replica el payload crudo del proveedor.

**Criterio de validacion**

- existe un contrato Pydantic claro para geocode y reverse geocode;
- la documentacion y los esquemas usan la misma semantica;
- no hay dependencias del provider en el contrato publico.

**Prioridad**

- Alta

### F4-2. Introducir adapter CartoCiudad dedicado

**Objetivo**

Crear el primer adapter geografico real siguiendo el patron fijado en Fase 3.

**Archivos a tocar**

- `app/services/cartociudad_client.py`
- `app/settings.py`
- `app/dependencies.py`
- `tests/`

**Dependencias**

- F4-1

**Tareas**

- encapsular `base_url`, timeouts y paths del proveedor;
- manejar errores HTTP, timeouts y JSON invalido;
- devolver solo `dict` / `list` simples;
- construir claves de cache para consultas geograficas repetidas;
- introducir configuracion de entorno necesaria.

**Criterio de validacion**

- existe un adapter independiente del router;
- el adapter no persiste en base ni normaliza contratos publicos;
- hay tests unitarios del cliente y de errores.

**Prioridad**

- Alta

### F4-3. Persistencia de cache geoespacial

**Objetivo**

Anadir persistencia reutilizable para geocodificacion y reverse geocoding.

**Archivos a tocar**

- `app/models.py`
- `app/repositories/geocoding.py`
- `alembic/`
- `tests/`

**Dependencias**

- F4-1

**Tareas**

- crear tablas `geocode_cache` y `reverse_geocode_cache`;
- definir claves logicas y tiempos de cache;
- anadir repository dedicado para lectura/escritura;
- acompanar con migracion Alembic.

**Criterio de validacion**

- existe cache persistente tipada;
- la migracion aplica sin romper el esquema actual;
- los tests cubren upsert y lectura.

**Prioridad**

- Alta

### F4-4. Implementar `/geocode` como endpoint semantico

**Objetivo**

Exponer la primera consulta geografica publica sobre el adapter y la cache persistente.

**Archivos a tocar**

- `app/api/routes_territorial.py`
- `app/schemas.py`
- `app/services/cartociudad_client.py`
- `app/repositories/geocoding.py`
- `app/main.py`
- `tests/`

**Dependencias**

- F4-1
- F4-2
- F4-3

**Tareas**

- crear endpoint `GET /geocode`;
- integrar cache persistente y fallback al provider;
- devolver contrato semantico estable;
- documentar errores y validacion de parametros.

**Criterio de validacion**

- `/geocode` funciona con respuesta semantica;
- se reutiliza cache cuando corresponde;
- hay tests de caso feliz, validacion y fallo upstream.

**Prioridad**

- Alta

### F4-5. Implementar `/reverse_geocode` como endpoint semantico

**Objetivo**

Completar la pareja minima de geocodificacion con resolucion inversa.

**Archivos a tocar**

- `app/api/routes_territorial.py`
- `app/schemas.py`
- `app/services/cartociudad_client.py`
- `app/repositories/geocoding.py`
- `tests/`

**Dependencias**

- F4-4

**Tareas**

- crear endpoint `GET /reverse_geocode`;
- validar `lat` y `lon`;
- reutilizar cache persistente;
- exponer contrato consistente con `/geocode`.

**Criterio de validacion**

- `/reverse_geocode` funciona y usa la misma semantica de contrato;
- hay tests de validacion, cache y error upstream.

**Prioridad**

- Alta

### F4-6. Alinear geocodificacion con el modelo territorial

**Objetivo**

Conectar resultados del provider geografico con `territorial_units` y codigos canonicos.

**Archivos a tocar**

- `app/services/cartociudad_normalizers.py`
- `app/repositories/territorial.py`
- `app/schemas.py`
- `tests/`

**Dependencias**

- F4-2
- F4-4
- F4-5

**Tareas**

- definir normalizacion de resultados geograficos;
- mapear resultados a unidades territoriales cuando exista coincidencia;
- incluir `territorial_resolution` o equivalente en la respuesta;
- documentar limites de matching y fallback.

**Criterio de validacion**

- las respuestas geograficas pueden cruzarse con el modelo territorial interno;
- el matching reutiliza `TerritorialRepository`;
- hay tests de coincidencia y fallback.

**Prioridad**

- Media-Alta

### F4-7. Abrir endpoints territoriales base

**Objetivo**

Exponer la primera capa de lectura territorial propia del proyecto.

**Archivos a tocar**

- `app/api/routes_territorial.py`
- `app/repositories/territorial.py`
- `app/schemas.py`
- `app/main.py`
- `tests/`

**Dependencias**

- F3-2
- F3-3

**Tareas**

- crear `GET /territorios/comunidades-autonomas`;
- crear `GET /territorios/provincias`;
- crear `GET /municipio/{codigo_ine}` o equivalente canonico;
- definir paginacion y filtros minimos donde aplique.

**Criterio de validacion**

- existe una capa territorial publica minima basada en el modelo interno;
- los contratos son estables y coherentes;
- los tests cubren lecturas base.

**Prioridad**

- Media

### F4-8. Reforzar tests integrados de la capa territorial y geografica

**Objetivo**

Cubrir la nueva capa semantica territorial y geocodificacion antes de ampliar el alcance funcional.

**Archivos a tocar**

- `tests/`
- `tests/integration/`
- `tests/conftest.py`

**Dependencias**

- F4-4
- F4-5
- F4-6
- F4-7

**Tareas**

- anadir tests de endpoints geograficos;
- anadir tests de cache persistente;
- anadir tests de matching territorial;
- anadir casos de regresion sobre contratos publicos.

**Criterio de validacion**

- la suite cubre geocode, reverse geocode, cache y matching territorial;
- no se depende de red real del provider.

**Prioridad**

- Alta

### F4-9. Revalidar staging y RC con la nueva capa territorial

**Objetivo**

Confirmar que la primera integracion geografica no rompe deploy, migraciones, smoke ni restore.

**Archivos a tocar**

- `DEPLOYMENT_STAGING.md`
- `RELEASE_PROCESS.md`
- `DOCUMENTACION_EVOLUCION_PROYECTO.txt`

**Dependencias**

- F4-4
- F4-5
- F4-8

**Tareas**

- repetir deploy y migrate en staging;
- ampliar smoke test o checklist operativa si hace falta;
- ejecutar restore verification con la nueva base;
- registrar evidencia operativa.

**Criterio de validacion**

- staging sigue siendo operativo;
- RC sigue siendo repetible con la nueva capa geografica;
- no aparecen regresiones en runtime ni restore.

**Prioridad**

- Media-Alta

## Orden recomendado

1. F4-1
2. F4-2
3. F4-3
4. F4-4
5. F4-5
6. F4-6
7. F4-7
8. F4-8
9. F4-9

## Propuesta de sprint

### Sprint 1

- F4-1
- F4-2
- F4-3
- F4-4
- F4-5

Objetivo:

Abrir la primera capacidad geografica controlada del proyecto con contratos semanticos y cache persistente.

### Sprint 2

- F4-6
- F4-7
- F4-8
- F4-9

Objetivo:

Consolidar el cruce con el modelo territorial, abrir la capa territorial publica minima y revalidar staging/RC.

## Estado de implementacion

- F4-1: completado
- F4-2: completado
- F4-3: completado
- F4-4: completado
- F4-5: completado
- F4-6: completado
- F4-7: completado
- F4-8: completado
- F4-9: completado
