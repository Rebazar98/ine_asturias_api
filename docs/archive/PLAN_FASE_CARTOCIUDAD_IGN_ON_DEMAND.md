# Plan Fase CartoCiudad/IGN On Demand

## Objetivo

Convertir CartoCiudad en la segunda fuente oficial del producto en modo consulta bajo demanda, manteniendo contratos semanticos propios, cache persistente, trazabilidad raw reutilizando `ingestion_raw` y descubrimiento desde la API propia.

## Alcance de la fase

- `GET /geocode`
- `GET /reverse_geocode`
- `GET /territorios/catalogo`
- documentacion y validacion operativa asociadas

Fuera de alcance:

- carga masiva de capas IGN
- catalogos geograficos materializados
- geometria administrativa persistida
- API keys, cuotas o multi-tenant

## Hitos / Sprints

### Sprint 1. Endurecimiento del flujo geográfico

Estado: completado

- orquestacion `provider -> cache persistente -> raw ingestion -> normalization -> response`
- persistencia raw de geocode y reverse geocode en `ingestion_raw`
- sanitizacion de logs y contexto de request para evitar queries o coordenadas completas
- mantenimiento de `geocode_cache` y `reverse_geocode_cache` como cache persistente oficial

### Sprint 2. Producto y descubrimiento

Estado: completado

- `GET /territorios/catalogo` publica `geocode` y `reverse_geocode` como recursos oficiales
- se mantiene contrato semantico estable con `source`, `cached`, `territorial_context`, `territorial_resolution` y `metadata`
- la documentacion deja explicito que clientes, agentes y n8n consumen la API propia y no CartoCiudad directamente

### Sprint 3. Validacion operativa y cierre de fase

Estado: completado

- cobertura de tests de cache, endpoint, auditoria raw y catalogo
- smoke actualizado para validar el catalogo publicado sin depender de la red real del proveedor
- validacion manual de staging documentada como opcion operativa, nunca como gate obligatorio
- checklist formal de fase cerrada

## Reglas de implementacion

- la API publica no expone payload raw del proveedor
- `ingestion_raw` guarda payload raw con parametros saneados
- los tests automaticos siguen usando mocks y `httpx.MockTransport`
- staging puede validar `geocode` y `reverse_geocode` manualmente, pero esa llamada real no entra en CI

## Criterio de aceptacion

La fase queda aceptada si:

1. `ruff` y `pytest` pasan.
2. `/territorios/catalogo` expone `geocode` y `reverse_geocode`.
3. las llamadas a CartoCiudad quedan cacheadas y auditables en `ingestion_raw`.
4. los logs ya no vuelcan queries o coordenadas completas.
5. la documentacion y el runbook de staging permiten validar la segunda fuente sin mirar el codigo.

## Siguiente paso natural

- `API keys + auditoria + cuotas` para comercializacion B2B, o
- materializacion selectiva de catalogos geograficos si el mercado pide mas profundidad territorial
