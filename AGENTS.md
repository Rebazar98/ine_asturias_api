# AGENTS.md

## 1. Vision general del proyecto

Este repositorio implementa un backend de datos territoriales en Python sobre FastAPI. Su funcion actual es integrar datos del INE, persistir respuestas raw, normalizar observaciones y exponer una API propia orientada a automatizacion, analitica y consumo programatico. El sistema ya opera con PostgreSQL, jobs en background, cache en memoria, catalogo persistente de tablas y Docker. La arquitectura DEBE evolucionar hacia una plataforma territorial mas amplia que incorpore IGN, CartoCiudad, PostGIS, consultas espaciales, automatizacion con n8n y pipelines de datos mas complejos.

La filosofia arquitectonica del proyecto DEBE mantenerse en cuatro principios:

- La API publica DEBE exponer contratos semanticos propios y NO DEBE exponer directamente el shape de los proveedores externos.
- El pipeline DEBE separar claramente extraccion, persistencia raw, validacion, filtrado, normalizacion y persistencia analitica.
- El sistema DEBE ser trazable e idempotente: cada llamada relevante a proveedor DEBE poder auditarse y cada persistencia normalizada DEBE evitar duplicados logicos.
- Las cargas pesadas DEBEN ejecutarse fuera del camino critico de una request HTTP sincronica cuando el volumen o la latencia lo requieran.

El proyecto NO es un proxy generico. Es una plataforma de integracion de datos con responsabilidades de catalogacion, normalizacion y publicacion. Toda nueva funcionalidad DEBE reforzar esa direccion y NO DEBE degradar el backend a una simple pasarela HTTP.

## 2. Arquitectura del sistema

La arquitectura real del repositorio esta organizada por capas y esa separacion DEBE respetarse estrictamente.

### 2.1 Capas actuales

- `app/api`: capa HTTP. Contiene routers FastAPI, validacion de entrada, dependencias, codigos de estado y serializacion de salida.
- `app/services`: capa de negocio y adaptacion de proveedores. Incluye el cliente del INE, el adapter de CartoCiudad, la resolucion automatica de Asturias y la normalizacion de payloads.
- `app/repositories`: capa de persistencia. Encapsula escrituras y lecturas hacia PostgreSQL para raw ingestion, series normalizadas, catalogo de tablas, cache geoespacial y modelo territorial.
- `app/core`: infraestructura transversal. Incluye logging estructurado, cache TTL en memoria y registro de jobs en memoria.
- `app/models.py`: modelo ORM de persistencia.
- `app/schemas.py`: contratos Pydantic de entrada y salida.
- `app/db.py`: inicializacion de engine, factorias de sesion y ciclo de vida de la base.
- `app/settings.py`: configuracion por entorno.
- `app/main.py`: composicion de la aplicacion, middleware, handlers y lifespan.
- `main.py`: shim de compatibilidad para exponer `app.main:app`.

### 2.2 Definicion de responsabilidades

#### Routers

Los routers DEBEN:

- recibir parametros HTTP
- validar entrada
- resolver dependencias
- invocar services o funciones de orquestacion
- devolver `response_model` o `JSONResponse`
- mapear errores de dominio a errores HTTP

Los routers NO DEBEN:

- contener reglas de negocio complejas
- acceder directamente a SQLAlchemy
- hacer parsing manual repetitivo del payload de proveedores
- implementar logica de deduplicacion o persistencia
- conocer detalles internos del proveedor mas alla del caso de uso que exponen

#### Services

Los services DEBEN encapsular logica de dominio y adaptacion externa. En el estado actual existen varias piezas principales:

- `INEClientService`: provider adapter actual del INE
- `CartoCiudadClientService`: provider adapter geografico actual
- `IGNAdministrativeSnapshotClient`: adapter actual para snapshots administrativos versionables IGN/CNIG
- `CatastroClientService`: adapter actual para agregados municipales urbanos de Catastro
- `IGNAdministrativeBoundariesLoaderService`: orquestacion interna de `fetch -> raw -> validacion -> matching -> upsert` sobre el modelo territorial
- `TerritorialExportService`: orquestacion interna de bundles semanticos multi-fuente por entidad territorial
- `AsturiasResolver`: logica de resolucion geografica para operaciones del INE
- `normalizers.py`: capa de normalizacion y flatten de payloads INE
- `cartociudad_normalizers.py`: traduccion inicial de payloads geograficos al contrato semantico interno

Los services DEBEN:

- trabajar con estructuras Python tipadas o validadas
- aislar heuristicas de deteccion, filtrado y resolucion
- producir salidas estables para repositorios y routers
- emitir logs con contexto de dominio

#### Providers

El proyecto DEBE tratar a cada proveedor externo como una capa explicita de adaptacion. En el codigo actual esto ya existe para INE, CartoCiudad, IGN administrativo y Catastro en `app/services`. Toda nueva fuente futura DEBE seguir el mismo patron.

Un provider adapter DEBE:

- encapsular la URL base y la semantica del proveedor
- gestionar timeouts, errores HTTP y JSON invalido
- construir claves de cache
- devolver solo `dict` o `list`
- traducir errores externos a excepciones propias del dominio

Un provider adapter NO DEBE:

- escribir directamente en base de datos
- decidir politicas de catalogo
- aplicar normalizacion analitica compleja

#### Normalization layer

La capa de normalizacion DEBE convertir payloads heterogeneos a una estructura canonica interna. En este proyecto esa capa vive hoy en `app/services/normalizers.py` y trabaja sobre `NormalizedSeriesItem`.

La capa de normalizacion DEBE:

- aceptar payload root `dict` y `list`
- detectar series y observaciones de forma defensiva
- construir una fila por observacion atomica
- conservar `metadata` y `raw_payload`
- tolerar cambios de shape del INE sin romper el pipeline completo

#### Repositories y persistence layer

Los repositories DEBEN encapsular toda escritura y lectura persistente. Hoy existen:

- `IngestionRepository`
- `SeriesRepository`
- `TableCatalogRepository`
- `GeocodingCacheRepository`
- `AnalyticalSnapshotRepository`
- `TerritorialExportArtifactRepository`

La persistence layer DEBE encargarse de:

- insertar raw payloads
- hacer `upsert` de observaciones normalizadas
- mantener el catalogo de tablas y su estado operativo
- mantener cache persistente de geocodificacion y reverse geocoding cuando aplique
- mantener snapshots analiticos reutilizables cuando exista una justificacion operativa explicita
- mantener artefactos de exportacion territorial reutilizables cuando exista un contrato publico de bundle semantico
- aislar al resto del sistema de detalles SQLAlchemy y PostgreSQL

#### Catalog system

El catalogo de tablas es parte central de la arquitectura, no un accesorio. `ine_tables_catalog` DEBE actuar como memoria operativa del sistema para saber que tablas existen, cuales ya fueron evaluadas y cuales tienen valor analitico para Asturias.

## 3. Filosofia del modelo de datos

El sistema maneja dos familias de datos con objetivos diferentes.

### 3.1 Datos raw

Los datos raw DEBEN representar la respuesta del proveedor con la menor transformacion posible. Actualmente se persisten en `ingestion_raw` con:

- `source_type`
- `source_key`
- `request_path`
- `request_params`
- `payload`
- `fetched_at`

Los datos raw DEBEN usarse para:

- auditoria
- depuracion
- reproducibilidad del pipeline
- analisis de cambios de shape de proveedor

Los datos raw NO DEBEN usarse directamente como contrato de la API publica ni como modelo analitico.

### 3.2 Datos normalizados

Los datos normalizados DEBEN representar observaciones atomicas. La estructura canonica actual esta reflejada por `NormalizedSeriesItem` y `ine_series_normalized`.

Formato canonico de observacion normalizada:

```json
{
  "operation_code": "22",
  "table_id": "2852",
  "variable_id": "DPOP15001",
  "geography_name": "Principado de Asturias",
  "geography_code": "33",
  "period": "2021",
  "value": 1011792.0,
  "unit": "personas",
  "metadata": {},
  "raw_payload": {}
}
```

Reglas obligatorias:

- una fila normalizada DEBE corresponder a una observacion unica de una serie
- `period` DEBE persistirse como `string`
- `value` DEBE persistirse como `float` o `null`
- `metadata` DEBE almacenar contexto util de serie y observacion
- `raw_payload` DEBE conservar la observacion y el contexto de origen necesarios para reconstruir el dato

### 3.3 Estrategia de upsert e idempotencia

La clave logica actual de `ine_series_normalized` esta compuesta por:

- `operation_code`
- `table_id`
- `variable_id`
- `geography_name`
- `geography_code`
- `period`

Toda persistencia normalizada DEBE ser idempotente respecto a esa clave. El `upsert` DEBE actualizar como minimo:

- `value`
- `unit`
- `territorial_unit_id`
- `metadata`
- `raw_payload`

Ningun agente DEBE introducir una nueva columna o cambiar la clave logica sin definir explicitamente el impacto en deduplicacion, consultas e historico.

La referencia territorial interna en `ine_series_normalized` DEBE seguir estas reglas:

- `territorial_unit_id` PUEDE existir como referencia interna opcional;
- `territorial_unit_id` NO DEBE sustituir a `geography_code` ni entrar en la clave logica actual sin una migracion de contrato coordinada;
- el backfill de `territorial_unit_id` DEBE hacerse solo mediante un proceso explicito de enriquecimiento territorial y NO de forma oportunista dentro de queries HTTP.

## 4. Pipeline de ingestion

El flujo canonico del proyecto es:

`API externa -> llamada HTTP -> persistencia raw -> validacion -> filtrado -> normalizacion -> upsert -> respuesta API o job result`

### 4.1 Flujo actual para INE

En el flujo Asturias actual, el backend ejecuta estas fases:

1. resolver variable geografica y valor Asturias
2. descubrir tablas con `TABLAS_OPERACION/{op_code}`
3. registrar tablas descubiertas en catalogo
4. descargar `DATOS_TABLA/{table_id}` por tabla
5. persistir payload raw por tabla
6. filtrar filas y series validas para Asturias
7. normalizar observaciones
8. hacer `upsert` por batches
9. actualizar catalogo con estado y metricas
10. devolver resultado agregado o completar job en background

### 4.2 Manejo de datasets grandes

Los agentes DEBEN asumir que el INE puede devolver tablas muy grandes. Por tanto:

- NO DEBEN cargar volumen arbitrario sin limites explicitos
- DEBEN usar parametros de control como `max_tables`
- DEBEN mantener filtrado temprano antes de normalizar cuando sea posible
- DEBEN evitar construir inserts gigantes de una sola vez
- DEBEN usar procesamiento por lotes para persistencia
- DEBEN registrar advertencias al cruzar umbrales grandes

Valores actuales relevantes:

- `DEV_MAX_TABLES_DEFAULT = 3`
- `LARGE_TABLE_WARNING_THRESHOLD = 50000`
- `DEFAULT_BATCH_SIZE = 500`

Si un agente cambia estos valores, DEBE actualizar tests, logs esperados y documentacion operativa.

### 4.3 Control de memoria, timeouts y reintentos

El pipeline DEBE minimizar trabajo sincronico en memoria. Para cargas grandes:

- la normalizacion pesada DEBE ejecutarse fuera del event loop cuando corresponda
- el filtrado DEBE hacerse antes del `upsert`
- la escritura DEBE dividirse en lotes pequenos y commit por batch

Las llamadas HTTP a proveedores DEBEN:

- usar timeout configurable por entorno
- capturar `RequestError` y `HTTPStatusError`
- traducir fallos a errores de dominio
- NO implementar reintentos infinitos

Si se introducen reintentos, estos DEBEN ser acotados, observables y con backoff. Un retry NO DEBE ocultar errores estructurales del proveedor.

## 5. Sistema de catalogo de tablas

El catalogo de tablas DEBE operar como fuente de verdad sobre cobertura de una operacion del INE. La tabla `ine_tables_catalog` persiste:

- `operation_code`
- `table_id`
- `table_name`
- `request_path`
- `resolution_context`
- `has_asturias_data`
- `validation_status`
- `normalized_rows`
- `raw_rows_retrieved`
- `filtered_rows_retrieved`
- `series_kept`
- `series_discarded`
- `last_checked_at`
- `first_seen_at`
- `updated_at`
- `metadata`
- `notes`
- `last_warning`

### 5.1 Estados del catalogo

Los estados validos actuales son:

- `unknown`
- `has_data`
- `no_data`
- `failed`

Reglas:

- una tabla descubierta y no evaluada DEBE quedar en `unknown`
- una tabla con series validas para Asturias DEBE quedar en `has_data`
- una tabla evaluada sin filas validas DEBE quedar en `no_data`
- una tabla que falla por error tecnico DEBE quedar en `failed`

### 5.2 Reglas de skip y reprocesamiento

- una tabla marcada como `no_data` PUEDE omitirse en ejecuciones futuras cuando se use `skip_known_no_data=true`
- el comportamiento por defecto NO DEBE omitir catalogos conocidos salvo que el caller lo pida o exista una politica operativa explicita
- una tabla `failed` NO DEBE marcarse como `no_data`
- una tabla `failed` DEBE poder reprocesarse en una nueva ejecucion
- una tabla `has_data` PUEDE reprocesarse para refrescar metricas o datos

El catalogo NO DEBE usarse solo como registro historico. DEBE influir en las decisiones de ejecucion futuras.

## 6. Politica de trabajos en segundo plano

### 6.1 Cuando usar endpoints sincronicos

Un endpoint sincronico SOLO DEBE utilizarse cuando el trabajo esperado sea pequeno, acotado y razonablemente rapido. Ejemplos:

- `/health`
- lecturas directas de metadatos ligeros
- consultas de tabla unitaria con volumen pequeno

### 6.2 Cuando usar background jobs

Los jobs en background DEBEN utilizarse cuando:

- el flujo recorre varias tablas
- el volumen potencial es grande
- hay persistencia y normalizacion costosas
- la latencia superaria claramente una request interactiva

El endpoint `/ine/operation/{op_code}/asturias` ya sigue este patron. En entornos `dev`, `development`, `local` y `test`, el modo background es el default si el caller no lo fuerza en sentido contrario.

### 6.3 Cuando usar workers dedicados

Si un proceso:

- excede de forma recurrente el tiempo de una request web
- compite con la API por CPU o memoria
- requiere colas, reintentos, priorizacion o persistencia de jobs

entonces DEBE moverse a workers dedicados. El registro actual `InMemoryJobRegistry` es valido para el estado actual del repositorio, pero NO DEBE considerarse solucion final de produccion para cargas prolongadas o despliegues con varias replicas.

### 6.4 Seguimiento de estado

Todo job DEBE exponer como minimo:

- `job_id`
- `job_type`
- `status`
- `created_at`
- `started_at`
- `finished_at`
- `params`
- `progress`
- `result`
- `error`

Los estados validos actuales son:

- `queued`
- `running`
- `completed`
- `failed`

Los agentes DEBEN mantener estos estados o introducir una migracion coordinada de contratos si necesitan ampliarlos.

## 7. Reglas de base de datos

### 7.1 Migraciones

La situacion actual usa `Base.metadata.create_all()` en el arranque como mecanismo heredado. A partir de ahora, cualquier cambio de esquema DEBE ir acompanado de una migracion formal.

Reglas obligatorias:

- NO DEBE modificarse el esquema en produccion sin migracion versionada
- NO DEBE anadirse una columna, indice, restriccion o tabla nueva sin plan de migracion
- NO DEBE cambiarse una clave unica sin definir estrategia de backfill y deduplicacion
- Alembic DEBE introducirse como mecanismo oficial antes de ampliar el modelo de datos en serio

### 7.2 Politica de indices

Toda tabla persistente DEBE tener indices alineados con sus consultas reales.

En este proyecto:

- `ingestion_raw` DEBE indexar tipos de fuente, clave de fuente y fecha
- `ine_series_normalized` DEBE indexar claves logicas y dimensiones de consulta
- `ine_tables_catalog` DEBE indexar operacion, estado, visibilidad operativa y fecha de chequeo

Un agente NO DEBE crear indices por intuicion. DEBE justificar el patron de consulta o la restriccion de unicidad que resuelve.

### 7.3 Separacion de tablas

El modelo DEBE separar con claridad:

- tablas raw: captura de origen y auditoria
- tablas normalizadas: observaciones canonicas para consulta
- tablas de catalogo: control operativo y cobertura
- tablas de snapshots analiticos: salidas semanticas reutilizables con clave logica y expiracion
- tablas de artefactos de exportacion: bundles publicos reutilizables con TTL, hash y metadata de manifiesto
- futuras tablas dimensionales o espaciales: territorios, geometria, codigos y relaciones

Las tablas raw NO DEBEN mezclarse con las analiticas ni reutilizarse como fuente directa de endpoints semanticos.

Los snapshots analiticos NO DEBEN sustituir al modelo normalizado ni convertirse en nueva fuente de verdad del dominio. Su funcion DEBE limitarse a reutilizacion operativa, reduccion de recalculo y soporte a automatizacion.

Los artefactos de exportacion territorial NO DEBEN convertirse en fuente primaria del dominio. Su funcion DEBE limitarse a distribucion controlada, reutilizacion temporal y entrega de bundles semanticos multi-fuente.

### 7.4 Estrategia de codigo territorial canonico

Mientras el proyecto siga consolidando el dominio INE y preparando el terreno para geografia, la estrategia canonica DEBE ser:

- `country` -> `source_system=iso3166`, `code_type=alpha2`
- `autonomous_community` -> `source_system=ine`, `code_type=autonomous_community`
- `province` -> `source_system=ine`, `code_type=province`
- `municipality` -> `source_system=ine`, `code_type=municipality`

Reglas:

- el codigo canonico territorial DEBE vivir en `territorial_unit_codes`;
- el codigo canonico DEBE marcarse con `is_primary=true` en el nivel correspondiente;
- `geography_code` del dominio INE puede seguir exponiendo el codigo externo del INE mientras el cruce con el modelo territorial se termina de consolidar;
- los aliases NO DEBEN sustituir al codigo canonico; solo DEBEN apoyar matching y resolucion de nombres.

### 7.5 Reglas de matching territorial y aliases

El matching territorial DEBE seguir estas reglas:

- `canonical_name` DEBE ser la fuente de verdad semantica de una unidad territorial;
- `normalized_name` DEBE contener la version normalizada de `canonical_name` y DEBE usarse para matching directo por nombre;
- `display_name` DEBE reservarse para presentacion y NO DEBE usarse como identificador semantico por si solo;
- `territorial_unit_aliases` DEBE almacenar variantes linguisticas, nombres de proveedor, nombres cortos y nombres alternativos;
- la normalizacion minima de nombres DEBE eliminar acentos, puntuacion irrelevante y espacios redundantes antes de comparar;
- el matching por nombre DEBE resolverse dentro de `TerritorialRepository`, no en routers ni en services dispersos.

Un agente NO DEBE introducir heuristicas ad hoc de matching territorial en endpoints o normalizadores si el repositorio territorial puede encapsular esa logica.

### 7.6 Convenciones PostGIS del modelo territorial

Mientras no exista una carga geoespacial formal, el proyecto DEBE mantener estas convenciones base:

- el SRID canonico DEBE ser `4326`;
- `territorial_units.geometry` DEBE almacenar limites administrativos en `MULTIPOLYGON`;
- `territorial_units.centroid` DEBE almacenar centroides en `POINT`;
- ambas columnas PUEDEN permanecer `NULL` hasta que exista una carga validada;
- los indices espaciales base DEBEN ser `GIST` sobre `geometry` y `centroid`.

Un agente NO DEBE introducir geometria cruda en payloads raw, respuestas publicas o tablas no espaciales sin contrato explicito y sin respetar estas convenciones.

## 8. Observabilidad

### 8.1 Logging estructurado

El proyecto ya usa logging JSON. Toda nueva pieza DEBE integrarse en esa estrategia.

Se DEBEN registrar como minimo:

- arranque y cierre de app
- request timing por endpoint
- llamadas a proveedor con `path`, `params`, `status_code` y `duration_ms`
- cache hits
- fallos upstream
- descubrimiento de tablas
- filtrado y conteo de filas
- preparacion de normalizacion
- `upserts` por batch
- cambios de estado del catalogo
- estados y progreso de jobs

Los logs de depuracion DEBEN aportar contexto suficiente para reconstruir una ejecucion fallida sin inspeccion manual de base de datos en primera instancia.

### 8.2 Metricas minimas de pipeline

Aunque hoy no exista un backend formal de metrics, el sistema DEBE emitir al menos en logs estas metricas de dominio:

- `tables_found`
- `tables_selected`
- `tables_succeeded`
- `tables_failed`
- `warnings`
- `raw_rows_retrieved`
- `filtered_rows_retrieved`
- `normalized_rows`
- `batch_size`
- `rows_inserted`
- `tables_skipped_catalog`

Si en una fase posterior se incorpora Prometheus u otra solucion, esas magnitudes DEBEN convertirse en metricas formales.

## 9. Reglas de seguridad

La seguridad minima del backend DEBE ser explicita.

Reglas obligatorias:

- los secretos DEBEN entrar por variables de entorno o gestor seguro
- NO DEBEN commitearse secretos en el repositorio
- la `API_KEY` DEBE proteger endpoints del dominio INE cuando el despliegue no sea local o controlado
- los logs NO DEBEN incluir secretos, credenciales ni headers sensibles
- los errores HTTP NO DEBEN exponer stack traces internos al cliente
- los parametros externos DEBEN validarse con Pydantic o tipos FastAPI

Para entornos productivos, el sistema DEBE desplegarse detras de un proxy seguro con TLS terminado en infraestructura. Cuando se introduzca CartoCiudad o geocodificacion de direcciones, los agentes DEBEN tratar esos datos como potencialmente sensibles desde el punto de vista operacional y NO DEBEN volcarlos masivamente a logs.

## 10. Requisitos de testing

Todo cambio relevante DEBE venir acompanado de tests.

### 10.1 Services

Los agentes DEBEN anadir tests unitarios para services cuando modifiquen:

- resolucion de Asturias
- parsing o cliente INE
- normalizacion
- logica de filtrado territorial
- futuros providers IGN o CartoCiudad

### 10.2 Endpoints

Los agentes DEBEN cubrir:

- caso feliz
- validacion de parametros
- error upstream
- error de negocio traducido a HTTP
- contratos de respuesta
- jobs y estado cuando aplique

### 10.3 Ingestion y persistencia

Los agentes DEBEN anadir pruebas para:

- shape real del payload
- persistencia raw
- `upsert_many` y serializacion previa
- catalogacion de tablas
- reglas de skip y reprocesamiento

Los tests NO DEBEN depender de la red real del INE. El patron actual con `httpx.MockTransport`, overrides y dummies DEBE mantenerse.

## 11. Reglas de desarrollo para agentes

### 11.1 Prohibiciones

Los agentes NO DEBEN:

- poner logica de negocio en routers
- acceder a la base de datos desde routers
- exponer a clientes el esquema crudo del proveedor externo
- introducir cambios de esquema sin migracion
- anadir endpoints que devuelvan directamente payloads externos salvo endpoints raw explicitamente definidos
- bloquear el event loop con CPU o IO pesado evitable
- meter heuristicas de proveedor duplicadas en varios modulos

### 11.2 Obligaciones

Los agentes DEBEN:

- usar services y repositories existentes cuando encajen
- crear nuevas capas solo si existe una responsabilidad nueva real
- mantener compatibilidad hacia atras salvo instruccion explicita
- anadir tests por cada cambio funcional
- dejar logs utiles para operacion
- documentar cambios de contrato o de persistencia
- pensar primero en idempotencia, observabilidad y recuperacion ante fallos

### 11.3 Contratos de API

Todo endpoint nuevo DEBE definir:

- `response_model` cuando el contrato sea estable
- politicas de error
- modo sincronico o background
- impacto en persistencia
- tests de regresion

Si un endpoint inicia una operacion costosa, DEBE devolver `202` y estado de job en lugar de colgar una request hasta agotarla.

### 11.4 Contratos analiticos

Toda salida analitica nueva DEBE apoyarse en una semantica comun para automatizacion, informes y agentes.

Campos minimos comunes:

- `source`
- `generated_at`
- `territorial_context`
- `filters`
- `summary`
- `series`
- `metadata`

Reglas:

- `pagination` PUEDE anadirse cuando la salida sea paginada y DEBE usar un bloque semantico explicito;
- los errores analiticos DEBEN exponer `detail.code`, `detail.message`, `detail.retryable` y `detail.metadata` cuando exista un contrato estable de error;
- `series` DEBE representar observaciones o indicadores semanticos listos para consumo programatico;
- estos contratos NO DEBEN copiar el shape raw de INE, CartoCiudad ni de ningun proveedor futuro;
- `territorial_context` DEBE apoyarse en el modelo territorial interno cuando exista resolucion fiable;
- `filters`, `summary` y `metadata` PUEDEN variar por caso de uso, pero DEBEN mantener una funcion semantica estable entre endpoints.

## 12. Evolucion futura de la arquitectura

La hoja de ruta tecnica del proyecto DEBE alinearse con estas lineas:

### 12.1 IGN, CartoCiudad y Catastro

- YA existen un provider adapter dedicado para CartoCiudad y un adapter/versioned loader para IGN administrativo
- YA existe un provider adapter dedicado para agregados municipales urbanos de Catastro
- DEBEN existir contratos normalizados para geocodificacion y reverse geocoding
- DEBE persistirse cache para consultas repetidas de alto valor
- NO DEBE exponerse el payload crudo del proveedor como contrato principal
- el adapter DEBE vivir en `app/services` y limitarse a adaptacion HTTP, errores, timeouts y cache keys
- la persistencia de cache o catalogos futuros DEBE resolverse en repositories, no dentro del adapter
- la carga administrativa directa desde IGN/CNIG DEBE priorizar snapshots versionables o ficheros descargables frente a WFS live cuando se busque reproducibilidad operativa
- la carga IGN administrativa DEBE persistir raw en `ingestion_raw`, validar `MULTIPOLYGON`/`POINT` en `4326` y hacer upsert territorial solo tras matching canonico fiable
- cualquier contrato publico futuro DEBE apoyarse en el modelo territorial interno y no en ids opacos del proveedor
- los contratos publicos futuros de `/geocode` y `/reverse_geocode` DEBEN compartir una semantica comun basada en `source`, `cached`, `coordinates`, `entity_type`, `territorial_context`, `territorial_resolution` y `metadata`
- `query` y `query_coordinates` DEBEN diferenciar el caso directo y el inverso, pero el shape del `result` DEBE mantenerse consistente
- el primer adapter geografico real DEBE encapsular los endpoints oficiales `find` y `reverseGeocode` de CartoCiudad sin mezclar persistencia ni logica de matching territorial dentro del cliente HTTP

### 12.2 PostGIS y consultas espaciales

- la base DEBE evolucionar a PostgreSQL + PostGIS
- DEBEN introducirse tablas territoriales y geometria versionable
- las consultas espaciales DEBEN apoyarse en indices GIST o equivalentes
- la geometria NO DEBE mezclarse dentro de payloads raw de forma oportunista; DEBE tener modelo persistente claro
- cualquier carga geoespacial futura DEBE entrar mediante un contrato interno explicito y NO como importacion manual ad hoc
- el formato de intercambio inicial DEBE ser `GeoJSON FeatureCollection` en SRID `4326`
- los limites administrativos DEBEN normalizarse a `MULTIPOLYGON` y los centroides a `POINT`
- toda carga futura DEBE validar `ST_IsValid`, tipo geometrico, cobertura minima y cruce con `territorial_unit_codes` antes de persistirse en tablas finales
- si un proveedor futuro entrega otro SRID o formato, el adapter o pipeline de carga DEBE transformarlo antes de persistirlo

### 12.3 Automatizacion con n8n

- n8n DEBE consumir la API propia, no proveedores externos
- los jobs largos DEBEN exponerse con estado y resultado trazable
- los endpoints para automatizacion DEBEN priorizar contratos estables y semanticos
- el patron preferente DEBE ser: endpoint semantico sincronico si el dato ya existe; `POST` de job + polling de `status_path` si el calculo es costoso
- `summary` DEBE usarse para decisiones ligeras, `series` para mover dato de negocio y `metadata` solo como contexto operativo
- los flujos NO DEBEN leer tablas internas ni asumir acceso directo a `analytical_snapshots`
- `job_id`, `status_path` y `snapshot_key` SI PUEDEN tratarse como identificadores operativos estables cuando formen parte del contrato documentado

### 12.4 RAG y agentes

Si el proyecto incorpora RAG, la fuente base DEBE ser contenido normalizado, catalogado y documentado. Un pipeline RAG NO DEBE indexar indiscriminadamente payloads raw de proveedor sin curacion, porque eso degrada trazabilidad, calidad y gobernanza del dato.

Mientras el consumo por agentes siga siendo API-first, se DEBEN aplicar estas reglas:

- los agentes DEBEN consumir endpoints semanticos (`/territorios/...`, `/geocode`, `/reverse_geocode`, `/ine/series` cuando el contrato este estabilizado) y NO payloads raw de proveedor;
- los agentes NO DEBEN usar `ingestion_raw`, `ine_tables_catalog`, `analytical_snapshots` ni otras tablas internas como fuente primaria de respuesta;
- si un flujo requiere polling, el agente DEBE tratar `queued` y `running` como estados transitorios y `completed` / `failed` como terminales;
- `detail.retryable` y `detail.metadata` DEBEN guiar reintentos o escalado, no parsing ad hoc de mensajes de error;
- ningun agente DEBE reconstruir contratos publicos a partir de metadatos internos cuando ya exista un endpoint semantico para el caso de uso.

## Reglas finales de operacion

- `AGENTS.md` DEBE mantenerse como fuente unica de instrucciones para agentes.`r`n- Cualquier cambio arquitectonico relevante DEBE reflejarse en este documento.
- Si el estado real del repositorio contradice esta guia, el agente DEBE corregir primero la documentacion o justificar explicitamente la desviacion en su entrega.
- El criterio de calidad del proyecto NO es solo que "funcione". El criterio de calidad DEBE incluir robustez, trazabilidad, seguridad, compatibilidad y capacidad de evolucion a produccion.

