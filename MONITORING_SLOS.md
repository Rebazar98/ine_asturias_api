# Monitoring SLOs

Este documento fija una base minima de SLOs/SLIs para el backend antes de una exposicion productiva mas seria.

## SLOs iniciales

- disponibilidad del API: `99.5%`
- latencia `p99` de `GET /geocode` y `GET /ine/series`: `< 500 ms`
- latencia `p99` de jobs de background e inline equivalentes: `< 5 s`
- tasa de error `5xx`: `< 0.5%`

## SLIs y metricas

- disponibilidad:
  - fuente recomendada: `up{job="ine_asturias_api"}`
  - comprobacion funcional complementaria: `GET /health` y `GET /health/ready`
- latencia HTTP:
  - `ine_asturias_http_request_duration_seconds_bucket`
  - rutas criticas actuales: `/geocode`, `/reverse_geocode`, `/ine/series`
- tasa de error:
  - `ine_asturias_http_requests_total{status_code=~"5.."}`
- latencia de jobs:
  - `ine_asturias_job_duration_seconds_bucket`
  - `job_type` actual: `operation_asturias_ingestion`, `territorial_municipality_report`
- resiliencia upstream:
  - `ine_asturias_provider_retries_total`
  - `ine_asturias_provider_circuit_breaker_transitions_total`
- abuso y seguridad:
  - `ine_asturias_auth_failures_total`
  - `ine_asturias_rate_limit_rejections_total`

## Reglas operativas

- `/metrics` debe quedar protegido con `X-API-Key` en cualquier entorno compartido.
- los SLOs deben medirse con ventanas moviles de 5 a 30 minutos para alertas rapidas y con ventanas de 28 dias para reporting formal.
- los rechazos `401` y `429` no deben computar como disponibilidad perdida del servicio, pero si deben auditarse como senales de abuso o mala configuracion.

## Alertas base

Las reglas iniciales estan en [monitoring/prometheus-alerts.yml](C:/Users/user/OneDrive/Documents/Playground/monitoring/prometheus-alerts.yml).

Estas reglas son una base y asumen jobs Prometheus llamados `ine_asturias_api` y `ine_asturias_worker`. Si el despliegue usa otros nombres, ajusta las expresiones antes de activarlas.
