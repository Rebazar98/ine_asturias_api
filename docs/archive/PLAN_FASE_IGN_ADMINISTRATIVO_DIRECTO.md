# PLAN_FASE_IGN_ADMINISTRATIVO_DIRECTO

## Objetivo

Incorporar IGN/CNIG administrativo directo como fuente oficial interna para enriquecer el modelo territorial, manteniendo contratos publicos estables y sin introducir dependencia de red real en CI.

## Alcance ejecutado

- adapter dedicado para snapshots administrativos IGN/CNIG en `app/services/ign_admin_client.py`
- loader/orquestador interno en `app/services/ign_admin_boundaries.py`
- script operativo `scripts/load_ign_admin_boundaries.py`
- trazabilidad raw en `ingestion_raw` por snapshot completo y por nivel cargado
- upsert idempotente de `geometry` y `centroid` sobre `territorial_units`
- publicacion minima en `GET /territorios/catalogo` con `boundary_source`, `geometry_units` y `centroid_units`
- metricas y logs estructurados para el proceso de carga

## Reglas de la fase

- scope inicial: Asturias (`autonomous_community_code=03`) mas unidades padre necesarias
- formato de intercambio: `GeoJSON FeatureCollection` en SRID `4326`
- limites persistidos como `MULTIPOLYGON`
- centroides persistidos como `POINT`
- sin endpoints publicos nuevos de geometria en esta fase
- sin dependencia de WFS live para CI o tests automatizados

## Flujo canonico

`snapshot IGN/CNIG -> parseo -> persistencia raw -> validacion geometrica -> matching canonico -> upsert territorial -> catalogo`

## Evidencia esperada

- `features_selected`, `features_upserted`, `features_rejected`
- `raw_records_saved`
- incidencias por feature rechazada o parent no resuelto
- `/territorios/catalogo` reflejando cobertura administrativa cargada

## Validacion

- tests unitarios de parseo, normalizacion, scope y rechazos
- prueba de integracion opcional de roundtrip con PostGIS real
- smoke sin dependencia de red IGN

## Siguiente fase natural

- endpoints espaciales semanticos apoyados en `territorial_units`
- o ampliacion nacional del mismo pipeline de carga
