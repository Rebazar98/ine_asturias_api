# Plan Técnico de Implementación de la Plataforma de Datos Territoriales de España

## 1. Introducción y visión del sistema

El proyecto parte de una base ya operativa: un backend `ine_asturias_api` construido con FastAPI, persistencia en PostgreSQL, normalización de series del INE, ejecución de trabajos en background y catálogo persistente de tablas descubiertas por operación. Esa primera iteración ha demostrado que el patrón arquitectónico es válido para trabajar con fuentes oficiales heterogéneas: descubrir recursos, descargar datos, persistir payloads raw, normalizar observaciones útiles y exponer una API propia preparada para automatización y consumo por terceros.

El siguiente paso es convertir ese backend especializado en una plataforma de datos territoriales de España, manteniendo la filosofía pragmática del MVP actual pero ampliando su alcance funcional. La plataforma debe integrar dos familias de información complementarias. Por un lado, el INE aporta series estadísticas, datos municipales e indicadores territoriales. Por otro, IGN / CartoCiudad aporta geocodificación, direcciones, coordenadas, topónimos y unidades administrativas, que son claves para enriquecer análisis espaciales y facilitar búsquedas territoriales.

La visión del sistema es disponer de una capa de datos unificada, estable y trazable sobre fuentes oficiales, de modo que `n8n`, agentes de IA, aplicaciones internas y futuros cuadros de mando no dependan directamente de APIs externas con formatos variables. La plataforma debe ofrecer respuestas normalizadas, trazabilidad de origen, persistencia reutilizable y una evolución controlada hacia capacidades espaciales más avanzadas con PostGIS.

## 2. Arquitectura técnica objetivo

La arquitectura objetivo mantiene una separación clara por capas para desacoplar la volatilidad de los proveedores externos del contrato estable ofrecido por la API propia.

```text
Fuentes oficiales
  ├─ INE API
  └─ IGN / CartoCiudad API
        ↓
Capa de ingesta / ETL (Python)
  ├─ extracción
  ├─ normalización
  ├─ enriquecimiento territorial
  └─ planificación / cron
        ↓
Persistencia
  ├─ PostgreSQL
  ├─ PostGIS
  ├─ tablas raw
  ├─ tablas normalizadas
  └─ catálogos de cobertura
        ↓
Capa de servicio
  └─ FastAPI
        ↓
Consumo y automatización
  ├─ n8n
  ├─ agentes de IA
  ├─ aplicaciones cliente
  └─ generación de informes
```

La decisión de mantener una arquitectura por capas responde a tres necesidades. La primera es aislar cambios en las fuentes externas. La segunda es poder reutilizar la persistencia y la normalización en procesos batch, sin depender de peticiones síncronas. La tercera es exponer una semántica de negocio estable, independientemente de cómo evolucionen INE o CartoCiudad.

FastAPI seguirá siendo la fachada principal de servicio, por su velocidad de desarrollo, soporte asíncrono y buena integración con OpenAPI. PostgreSQL continuará siendo el sistema de registro. PostGIS no sustituirá el diseño actual, sino que lo ampliará para incorporar geometrías, relaciones espaciales, centroides y búsquedas geográficas de forma nativa.

## 3. Diseño del pipeline de ingestión

El pipeline debe estructurarse en fases bien diferenciadas:

1. **Descubrimiento de recursos**: identificación de tablas, variables, valores geográficos o catálogos administrativos disponibles en la fuente.
2. **Extracción**: descarga de payloads desde APIs oficiales con cliente HTTP robusto, timeouts y control de errores.
3. **Persistencia raw**: almacenamiento del payload original en PostgreSQL para trazabilidad, auditoría y depuración.
4. **Validación**: comprobación del shape recibido, consistencia mínima, cobertura geográfica y presencia de observaciones útiles.
5. **Normalización**: transformación a modelos internos homogéneos preparados para consulta analítica.
6. **Enriquecimiento territorial**: unión con catálogos administrativos, códigos INE, coordenadas o geometrías cuando aplique.
7. **Publicación vía API**: exposición de resultados a través de endpoints estables y listos para automatización.

Para el **INE**, debe mantenerse el patrón que ya ha demostrado funcionar: descubrimiento de tablas por operación, resolución geográfica, descarga de `DATOS_TABLA`, filtrado territorial, normalización por observación, persistencia de series y actualización del catálogo de tablas. La experiencia con operaciones grandes demuestra que las cargas deben ejecutarse como trabajos en background y en lotes controlados, no en una única request HTTP bloqueante.

Para **CartoCiudad**, conviene distinguir dos modos de ingesta. El primero es la consulta bajo demanda desde la API propia, por ejemplo para `geocode` o `reverse_geocode`, con posibilidad de cachear resultados frecuentes. El segundo es la materialización selectiva de catálogos administrativos o topónimos si se detecta valor en persistirlos para acelerar consultas, enriquecer otras series o reducir dependencia de la red.

Además del consumo bajo demanda, el sistema debe incorporar tareas programadas: refresco del catálogo de tablas del INE, actualización de entidades territoriales, revisión de tablas ya catalogadas y comprobaciones periódicas de cobertura y calidad. El diseño actual con jobs en background es una base adecuada para evolucionar más adelante hacia workers dedicados.

## 4. Diseño de base de datos PostgreSQL + PostGIS

La base actual `ine_asturias` puede evolucionar de forma incremental sin romper compatibilidad inicial. El objetivo no es descartar el esquema existente, sino organizarlo en familias de tablas que permitan convivir a los datos estadísticos y geoespaciales.

| Familia | Tabla propuesta | Propósito principal |
|---|---|---|
| Ingesta raw | `ingestion_raw` | Trazabilidad completa de llamadas y payloads originales |
| Estadística normalizada | `ine_series_normalized` | Observaciones normalizadas por operación, tabla, periodo y geografía |
| Catálogo de recursos | `ine_tables_catalog` | Cobertura y utilidad de tablas descubiertas por operación |
| Dimensiones territoriales | `territorial_units`, `municipalities`, `provinces`, `autonomous_communities` | Modelo maestro de códigos y niveles territoriales |
| Cache geoespacial | `geocode_cache`, `reverse_geocode_cache` | Reutilización de resultados CartoCiudad |
| Soporte espacial | columnas `geometry` | Geometrías, centroides, envolventes y relaciones espaciales |

Las tablas existentes `ingestion_raw`, `ine_series_normalized` e `ine_tables_catalog` deben conservarse y reforzarse. Sobre ellas se añadirá una capa de dimensiones territoriales. `territorial_units` puede funcionar como tabla base común con código, nivel, nombre, provincia, comunidad autónoma, vigencia y referencias externas; mientras que las tablas específicas por nivel pueden facilitar consultas frecuentes y mantener claves limpias.

PostGIS permitirá añadir columnas `geometry` para límites administrativos, centroides y geometrías simplificadas. Se recomiendan índices B-tree para códigos y claves lógicas, e índices GIST o SP-GIST para búsquedas espaciales. Los metadatos variables del proveedor deben seguir almacenándose en JSONB para preservar flexibilidad. La estrategia correcta de evolución de esquema debe pasar a gestionarse con Alembic, aunque la base actual haya empezado con `create_all`.

## 5. Diseño de la API propia

La API propia debe consolidarse como una capa estable sobre proveedores heterogéneos. El principio de diseño es claro: la API no debe reflejar el shape crudo del INE o de CartoCiudad, sino ofrecer recursos semánticos listos para uso en automatizaciones y aplicaciones.

Las familias de endpoints recomendadas son:

- **Salud y observabilidad**: `/health`, métricas básicas, estado de jobs.
- **Estadística territorial**: series por municipio, provincia, comunidad autónoma o indicador.
- **Geocodificación**: búsqueda de direcciones, topónimos y reverse geocoding.
- **Catálogos administrativos**: municipios, provincias, comunidades y códigos asociados.
- **Cobertura y catálogo de tablas**: estado de operaciones INE y tablas útiles.
- **Jobs**: seguimiento de ejecuciones pesadas y reprocesos.

Ejemplos de endpoints objetivo:

- `GET /municipio/{codigo_ine}`
- `GET /municipios`
- `GET /estadisticas/poblacion/{codigo_ine}`
- `GET /estadisticas/{indicador}/{codigo_territorial}`
- `GET /geocode?direccion=`
- `GET /reverse_geocode?lat=&lon=`
- `GET /territorios/provincias`
- `GET /territorios/comunidades-autonomas`
- `GET /jobs/{job_id}`

Los contratos de respuesta deben converger en una estructura homogénea con campos como `source`, `territory`, `period`, `value`, `unit`, `geometry` y `metadata`. Eso facilita el uso por n8n, por agentes de IA y por consumidores humanos. También permite que un mismo cliente trate igual una serie del INE y un resultado geocodificado enriquecido con información administrativa.

## 6. Integración con n8n y automatización

La integración con `n8n` debe apoyarse siempre en la API propia y no en llamadas directas a proveedores externos. Esta decisión reduce acoplamiento, centraliza autenticación, control de errores, cache y trazabilidad. Además, permite que los flujos de automatización trabajen contra un modelo más estable.

Casos de uso inmediatos:

- enriquecimiento automático de direcciones con municipio, provincia y coordenadas;
- generación periódica de informes estadísticos por municipio o provincia;
- alertas por cambios relevantes en indicadores territoriales;
- preparación de contexto territorial para agentes de IA;
- validación de direcciones y topónimos antes de procesos de integración.

El patrón recomendado es:

```text
n8n → POST/GET API propia → job de ingesta/enriquecimiento → PostgreSQL/PostGIS → resultado normalizado → informe / agente / dashboard
```

Para procesos ligeros, n8n podrá invocar endpoints síncronos. Para cargas grandes, como refresco de tablas INE o generación de catálogos territoriales, la API debe lanzar jobs asíncronos y exponer su estado. El consumo puede resolverse con polling controlado o, en fases posteriores, con webhooks cuando la orquestación lo justifique.

## 7. Estrategia de cacheado, resiliencia y calidad

La plataforma necesita cache por capas. La cache en memoria ya existente es útil para metadatos ligeros, variables y resoluciones repetitivas. Para CartoCiudad conviene añadir cache persistente en PostgreSQL para geocodificaciones frecuentes y reverse geocoding, evitando costes de latencia y reduciendo dependencia del proveedor. El catálogo persistente de tablas del INE ya es un buen ejemplo de cache semántica y cobertura acumulada.

En resiliencia, se recomienda consolidar:

- timeouts por proveedor;
- retries acotados con backoff exponencial;
- logging estructurado de errores upstream;
- degradación controlada cuando una tabla o recurso falle;
- procesamiento parcial por lotes y tablas;
- límites de trabajo por request y derivación a background jobs.

La calidad del dato debe sostenerse en cuatro pilares: validación de payloads, persistencia raw, normalización defensiva y trazabilidad por job. A nivel funcional, la plataforma debe seguir distinguiendo estados como `unknown`, `has_data`, `no_data` y `failed`, ya que esa clasificación ofrece una visión muy útil sobre cobertura real de la fuente y evita reprocesar recursos inútiles.

## 8. Estrategia de escalabilidad

La escalabilidad debe abordarse de forma progresiva, evitando una sobrearquitectura prematura. La API puede seguir siendo stateless y desplegable en una o varias instancias. Las ingestas pesadas deben separarse del serving síncrono mediante jobs en background, y PostgreSQL/PostGIS seguirá siendo el backend central de persistencia.

En el estado actual, una instancia puede servir como MVP siempre que los trabajos pesados se controlen con límites como `max_tables`, batching y normalización por chunks. La siguiente etapa razonable es introducir workers dedicados para ETL y procesos programados. Más adelante, si el volumen crece, puede incorporarse Redis como cache distribuida y soporte para colas ligeras, manteniendo FastAPI como puerta de entrada.

También deben contemplarse optimizaciones concretas:

- batching de inserts y upserts;
- normalización incremental de payloads grandes;
- materialización de agregados frecuentes;
- paginación en endpoints de catálogos;
- cacheo de resultados territoriales muy consultados;
- separación entre datos de referencia y datos transaccionales de ingesta.

## 9. Plan de desarrollo por fases

**Fase 1. Consolidación INE**  
Objetivo: estabilizar la base actual.  
Entregables: endurecimiento de catálogos, métricas de cobertura, normalización robusta, tests y observabilidad básica.  
Criterio de éxito: operaciones relevantes del INE procesadas con persistencia raw y normalizada consistente.

**Fase 2. Incorporación de CartoCiudad**  
Objetivo: añadir capacidades de geocodificación y localización territorial.  
Entregables: cliente HTTP para IGN / CartoCiudad, endpoints `geocode` y `reverse_geocode`, cache persistente y modelos de respuesta homogéneos.  
Criterio de éxito: geocodificación estable y reutilizable desde la API y desde n8n.

**Fase 3. PostGIS y modelo territorial**  
Objetivo: convertir la base en un repositorio territorial con soporte espacial.  
Entregables: tablas administrativas, extensión PostGIS, geometrías, centroides, índices espaciales y unión con códigos INE.  
Criterio de éxito: posibilidad de responder consultas espaciales y enriquecer resultados con contexto geográfico.

**Fase 4. API territorial unificada**  
Objetivo: exponer una capa de servicio coherente para estadísticas y geografía.  
Entregables: endpoints por municipio, provincia y comunidad, catálogos administrativos y contratos homogéneos.  
Criterio de éxito: clientes externos capaces de consumir la API sin conocer detalles internos del INE o de CartoCiudad.

**Fase 5. Automatización y analítica**  
Objetivo: consolidar el uso operativo de la plataforma.  
Entregables: flujos n8n, generación de informes, soporte para agentes de IA y monitorización operativa.  
Criterio de éxito: automatizaciones productivas apoyadas exclusivamente en la API propia y en el almacén interno.

## 10. Ejemplos de endpoints y respuestas

### `GET /municipio/{codigo_ine}`
Propósito: recuperar la ficha territorial básica de un municipio.  
Parámetros: `codigo_ine`.  
Respuesta mínima:

```json
{
  "source": "territorial_units",
  "territory": {
    "code": "33044",
    "name": "Oviedo",
    "level": "municipality",
    "province": "Asturias",
    "autonomous_community": "Principado de Asturias"
  },
  "geometry": null,
  "metadata": {}
}
```

### `GET /municipios`
Propósito: listar municipios, con filtros y paginación en fases posteriores.  
Respuesta mínima:

```json
{
  "items": [
    {"code": "33044", "name": "Oviedo"},
    {"code": "33024", "name": "Gijon"}
  ],
  "total": 2
}
```

### `GET /estadisticas/poblacion/{codigo_ine}`
Propósito: devolver serie de población normalizada para un territorio.  
Parámetros: `codigo_ine`.  
Respuesta mínima:

```json
{
  "source": "INE",
  "territory": {"code": "33044", "name": "Oviedo"},
  "indicator": "poblacion",
  "series": [
    {"period": "2021", "value": 220543, "unit": "personas"},
    {"period": "2022", "value": 219910, "unit": "personas"}
  ],
  "metadata": {"table_id": "2852"}
}
```

### `GET /geocode?direccion=`
Propósito: geocodificar una dirección o topónimo con apoyo de CartoCiudad.  
Parámetros: `direccion`.  
Respuesta mínima:

```json
{
  "source": "CartoCiudad",
  "query": "Oviedo",
  "result": {
    "label": "Oviedo",
    "lat": 43.3614,
    "lon": -5.8494,
    "municipality": "Oviedo",
    "province": "Asturias",
    "entity_type": "municipio"
  }
}
```

### `GET /reverse_geocode?lat=&lon=`
Propósito: resolver la entidad territorial más probable a partir de coordenadas.  
Parámetros: `lat`, `lon`.  
Respuesta mínima:

```json
{
  "source": "CartoCiudad",
  "coordinates": {"lat": 43.3614, "lon": -5.8494},
  "result": {
    "address": "Oviedo, Asturias",
    "municipality": "Oviedo",
    "province": "Asturias",
    "autonomous_community": "Principado de Asturias"
  }
}
```

## Conclusión

La plataforma propuesta no parte de cero: se apoya en un backend ya funcional que ha resuelto problemas reales de ingesta, persistencia, normalización, jobs en background y trazabilidad del INE. La estrategia recomendada es evolucionar ese núcleo hacia una plataforma territorial más amplia, donde INE aporte profundidad estadística y IGN / CartoCiudad aporte capacidad geoespacial y contexto administrativo. PostgreSQL seguirá siendo el sistema de registro y PostGIS será la extensión natural para consultas espaciales y enriquecimiento territorial. Con esta base, FastAPI podrá consolidarse como capa de servicio estable para n8n, agentes y aplicaciones, apoyando una evolución incremental, mantenible y preparada para crecimiento.
