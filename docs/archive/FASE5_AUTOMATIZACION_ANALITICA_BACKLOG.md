# Fase 5: Automatizacion, Analitica Operativa y Consumo Productivo

Este documento abre la siguiente etapa del proyecto tras el cierre completo de la Fase 4.

La Fase 5 NO introduce nuevas fuentes externas. Parte de una base ya estabilizada:

- dominio INE consolidado;
- modelo territorial operativo;
- primera capa geografica publica con CartoCiudad;
- staging y proceso RC revalidados con la nueva capacidad territorial.

El objetivo de esta fase es convertir esa base en una plataforma realmente util para consumo automatizado, informes y agentes, sin degradar la arquitectura ni reabrir deuda de integracion.

## Objetivo general

La Fase 5 debe dejar resueltos estos frentes:

1. definir contratos de consumo analitico estables y reutilizables;
2. abrir una primera capa de endpoints orientados a informe y automatizacion;
3. introducir jobs y resultados reutilizables para cargas analiticas no interactivas;
4. dejar patrones claros de consumo desde n8n y agentes;
5. reforzar observabilidad y revalidar staging/RC con este nuevo uso operativo.

## Principios de ejecucion

- NO abrir nuevas fuentes externas en esta fase.
- reutilizar el dominio semantico actual en lugar de exponer payloads raw;
- priorizar contratos pensados para automatizacion, polling y consumo programatico;
- usar jobs en background cuando el trabajo deje de ser claramente interactivo;
- persistir solo lo que aporte reutilizacion operativa o trazabilidad;
- toda nueva persistencia debe llegar con migracion Alembic;
- toda nueva capacidad publica debe venir con tests y validacion operativa.

## Backlog ejecutable

### F5-1. Definir contrato semantico de salidas analiticas

**Objetivo**

Fijar la semantica base de las respuestas orientadas a automatizacion e informes antes de abrir endpoints nuevos.

**Archivos a tocar**

- `app/schemas.py`
- `README.md`
- `AGENTS.md`

**Dependencias**

- Ninguna

**Tareas**

- definir respuesta base para salidas analiticas;
- decidir campos minimos comunes:
  - `source`
  - `generated_at`
  - `territorial_context`
  - `filters`
  - `summary`
  - `series`
  - `metadata`
- decidir contrato de error y de paginacion si aplica;
- documentar que estos contratos no dependen del shape de INE ni de CartoCiudad.

**Criterio de validacion**

- existen esquemas Pydantic claros para respuestas analiticas;
- la documentacion y los esquemas usan la misma semantica;
- el contrato es util para API, n8n y agentes.

**Prioridad**

- Alta

### F5-2. Introducir endpoint de ficha territorial semantica

**Objetivo**

Abrir una primera salida agregada y util para consumo productivo, combinando contexto territorial y datos semanticos ya disponibles.

**Archivos a tocar**

- `app/api/routes_territorial.py`
- `app/schemas.py`
- `app/repositories/territorial.py`
- `app/repositories/series.py`
- `tests/`

**Dependencias**

- F5-1

**Tareas**

- definir un endpoint tipo `GET /territorios/municipio/{codigo_ine}/resumen` o equivalente;
- combinar detalle territorial con series o indicadores basicos ya persistidos;
- devolver contrato semantico estable;
- mantener compatibilidad con el dominio actual.

**Criterio de validacion**

- existe una ficha territorial reutilizable para automatizacion;
- no se accede a raw ingestion desde el router;
- hay tests de contrato y de caso feliz.

**Prioridad**

- Alta

### F5-3. Introducir job de generacion de informe territorial

**Objetivo**

Crear un primer flujo analitico no interactivo para generacion de informe reutilizable.

**Archivos a tocar**

- `app/api/routes_territorial.py`
- `app/core/jobs.py`
- `app/worker.py`
- `app/services/`
- `app/schemas.py`
- `tests/`

**Dependencias**

- F5-1
- F5-2

**Tareas**

- definir un job de informe territorial con resultado estructurado;
- exponer un endpoint que devuelva `202` y `job_id`;
- mantener polling por `jobs/{job_id}`;
- decidir si el resultado vive solo en Redis o necesita persistencia adicional.

**Criterio de validacion**

- existe un job analitico reproducible;
- el flujo sigue el contrato actual de jobs;
- hay tests de estado y resultado.

**Prioridad**

- Alta

### F5-4. Persistencia reutilizable de informes o snapshots analiticos

**Objetivo**

Evitar recalculo innecesario de informes o snapshots analiticos cuando su uso operativo lo justifique.

**Archivos a tocar**

- `app/models.py`
- `app/repositories/`
- `alembic/`
- `tests/`

**Dependencias**

- F5-3

**Tareas**

- decidir si se necesita una tabla de snapshots o informes generados;
- definir clave logica, expiracion y metadata;
- crear repository dedicado;
- acompanar con migracion Alembic si se confirma la persistencia.

**Criterio de validacion**

- existe estrategia clara de reutilizacion analitica;
- si hay persistencia nueva, llega con migracion y tests;
- no se introduce duplicacion de resultados sin clave logica.

**Prioridad**

- Media-Alta

### F5-5. Definir patron de consumo desde n8n y agentes

**Objetivo**

Dejar un patron claro para que automatizaciones y agentes consuman la API propia y no los providers.

**Archivos a tocar**

- `README.md`
- `PLAN_TECNICO_PLATAFORMA_DATOS_TERRITORIALES.md`
- `AGENTS.md`

**Dependencias**

- F5-1
- F5-3

**Tareas**

- documentar flujo de polling de jobs;
- documentar contratos recomendados para n8n;
- documentar uso de endpoints semanticos frente a endpoints raw;
- definir que datos deben consumir agentes y cuales no.

**Criterio de validacion**

- existe una guia clara de consumo para n8n y agentes;
- se evita el acceso directo a providers externos;
- la documentacion refleja el estado real del backend.

**Prioridad**

- Media

### F5-6. Reforzar observabilidad de flujos analiticos

**Objetivo**

Hacer visibles tiempos, volumenes y resultados de los nuevos flujos de informe y consumo productivo.

**Archivos a tocar**

- `app/core/metrics.py`
- `app/core/logging.py`
- `app/services/`
- `app/api/`
- `tests/`

**Dependencias**

- F5-3
- F5-4

**Tareas**

- definir metricas minimas para generacion de informes;
- registrar tiempos, volumen de series y tamanos de resultado;
- documentar como interpretar esas metricas en `/metrics`;
- anadir tests o validaciones basicas cuando aplique.

**Criterio de validacion**

- los nuevos flujos analiticos quedan trazados;
- `/metrics` y logs recogen informacion util;
- no se introducen metricas redundantes o inconsistentes.

**Prioridad**

- Media-Alta

### F5-7. Abrir un endpoint de catalogo analitico minimo

**Objetivo**

Exponer una vista resumida de que recursos semanticos y territoriales estan disponibles para consumo.

**Archivos a tocar**

- `app/api/routes_territorial.py`
- `app/schemas.py`
- `app/repositories/`
- `tests/`

**Dependencias**

- F5-2
- F5-4

**Tareas**

- definir un endpoint tipo `/catalogo/territorial` o equivalente;
- exponer recursos disponibles, niveles territoriales y cobertura basica;
- dejarlo util para automatizaciones que necesiten descubrir capacidades.

**Criterio de validacion**

- existe una capa minima de descubrimiento de recursos;
- el endpoint usa contratos semanticos propios;
- hay tests de contrato y paginacion si aplica.

**Prioridad**

- Media

### F5-8. Reforzar tests integrados de consumo analitico

**Objetivo**

Cubrir la nueva capa de informe, jobs y consumo semantico antes de seguir ampliando superficie publica.

**Archivos a tocar**

- `tests/`
- `tests/integration/`
- `tests/conftest.py`

**Dependencias**

- F5-2
- F5-3
- F5-4
- F5-7

**Tareas**

- anadir tests de endpoints analiticos;
- anadir tests de jobs de informe;
- anadir tests de snapshots o cache analitica si existe;
- cubrir regresiones sobre contratos para automatizacion.

**Criterio de validacion**

- la suite cubre los nuevos flujos de consumo analitico;
- no se depende de red real de providers;
- se detectan regresiones en contratos publicos.

**Prioridad**

- Alta

### F5-9. Revalidar staging y RC con la capa de automatizacion y analitica

**Objetivo**

Confirmar que la capa de consumo productivo no rompe deploy, migraciones, smoke ni restore.

**Archivos a tocar**

- `DEPLOYMENT_STAGING.md`
- `RELEASE_PROCESS.md`
- `DOCUMENTACION_EVOLUCION_PROYECTO.txt`

**Dependencias**

- F5-3
- F5-4
- F5-8

**Tareas**

- repetir deploy y migrate en staging;
- ampliar smoke o checklist operativa si hace falta;
- validar jobs analiticos en staging;
- ejecutar restore verification;
- registrar evidencia operativa.

**Criterio de validacion**

- staging sigue siendo operativo;
- RC sigue siendo repetible con la capa analitica nueva;
- no aparecen regresiones de runtime, jobs o restore.

**Prioridad**

- Media-Alta

## Orden recomendado

1. F5-1
2. F5-2
3. F5-3
4. F5-4
5. F5-5
6. F5-6
7. F5-7
8. F5-8
9. F5-9

## Propuesta de sprint

### Sprint 1

- F5-1
- F5-2
- F5-3
- F5-4

Objetivo:

Abrir la primera capa de consumo analitico e informes reutilizables sobre la API territorial ya estabilizada.

### Sprint 2

- F5-5
- F5-6
- F5-7
- F5-8
- F5-9

Objetivo:

Consolidar el uso por automatizaciones y agentes, reforzar observabilidad y revalidar staging/RC con la nueva capa de analitica operativa.

## Estado de implementacion

- F5-1: completada
- F5-2: completada
- F5-3: completada
- F5-4: completada
- F5-5: completada
- F5-6: completada
- F5-7: completada
- F5-8: completada
- F5-9: completada
