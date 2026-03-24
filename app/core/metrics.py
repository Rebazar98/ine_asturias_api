from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable, Sequence
from time import time
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest


APP_METRICS_PREFIX = "ine_asturias_"

HTTP_REQUESTS_TOTAL = Counter(
    "ine_asturias_http_requests_total",
    "Total HTTP requests served.",
    ["method", "path", "status_code"],
)
HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "ine_asturias_http_request_duration_seconds",
    "HTTP request latency.",
    ["method", "path"],
)
PROVIDER_REQUESTS_TOTAL = Counter(
    "ine_asturias_provider_requests_total",
    "Provider request outcomes.",
    ["provider", "endpoint_family", "status"],
)
PROVIDER_REQUEST_DURATION_SECONDS = Histogram(
    "ine_asturias_provider_request_duration_seconds",
    "Provider request latency.",
    ["provider", "endpoint_family"],
)
PROVIDER_RETRIES_TOTAL = Counter(
    "ine_asturias_provider_retries_total",
    "Provider retries performed after retryable failures.",
    ["provider", "endpoint_family", "reason"],
)
PROVIDER_CIRCUIT_BREAKER_TRANSITIONS_TOTAL = Counter(
    "ine_asturias_provider_circuit_breaker_transitions_total",
    "Circuit breaker state transitions for providers.",
    ["provider", "previous_state", "new_state"],
)
PROVIDER_CACHE_HITS_TOTAL = Counter(
    "ine_asturias_provider_cache_hits_total",
    "Provider cache hits.",
    ["provider", "scope"],
)
AUTH_FAILURES_TOTAL = Counter(
    "ine_asturias_auth_failures_total",
    "Authentication failures by reason.",
    ["reason"],
)
RATE_LIMIT_REJECTIONS_TOTAL = Counter(
    "ine_asturias_rate_limit_rejections_total",
    "Requests rejected by rate limiting.",
    ["policy", "auth_mode"],
)
ANALYTICAL_FLOW_TOTAL = Counter(
    "ine_asturias_analytical_flow_total",
    "Analytical flow outcomes.",
    ["flow", "outcome", "storage_mode"],
)
ANALYTICAL_FLOW_DURATION_SECONDS = Histogram(
    "ine_asturias_analytical_flow_duration_seconds",
    "Analytical flow duration.",
    ["flow", "outcome", "storage_mode"],
)
ANALYTICAL_FLOW_SERIES_COUNT = Histogram(
    "ine_asturias_analytical_flow_series_count",
    "Series returned by analytical flows.",
    ["flow", "storage_mode"],
    buckets=(0, 1, 5, 10, 25, 50, 100, 250, 500, 1000),
)
ANALYTICAL_FLOW_RESULT_BYTES = Histogram(
    "ine_asturias_analytical_flow_result_bytes",
    "Serialized analytical flow result size in bytes.",
    ["flow", "storage_mode"],
    buckets=(0, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144),
)
ANALYTICAL_SNAPSHOT_EVENTS_TOTAL = Counter(
    "ine_asturias_analytical_snapshot_events_total",
    "Analytical snapshot lifecycle events.",
    ["snapshot_type", "event"],
)
RAW_INGESTION_TOTAL = Counter(
    "ine_asturias_raw_ingestion_total",
    "Raw ingestion records persisted.",
    ["source_type"],
)
NORMALIZATION_ROWS_GENERATED_TOTAL = Counter(
    "ine_asturias_normalization_rows_generated_total",
    "Normalized rows generated.",
    ["flow"],
)
NORMALIZATION_ROWS_DROPPED_TOTAL = Counter(
    "ine_asturias_normalization_rows_dropped_total",
    "Normalized rows dropped.",
    ["flow", "reason"],
)
PERSISTENCE_BATCHES_TOTAL = Counter(
    "ine_asturias_persistence_batches_total",
    "Persistence batches executed.",
    ["repository"],
)
PERSISTENCE_ROWS_TOTAL = Counter(
    "ine_asturias_persistence_rows_total",
    "Persistence rows written.",
    ["repository"],
)
CATALOG_EVENTS_TOTAL = Counter(
    "ine_asturias_catalog_events_total",
    "Catalog lifecycle events.",
    ["event"],
)
JOB_EVENTS_TOTAL = Counter(
    "ine_asturias_job_events_total",
    "Job lifecycle events.",
    ["job_type", "status"],
)
JOB_DURATION_SECONDS = Histogram(
    "ine_asturias_job_duration_seconds",
    "Background and inline job duration.",
    ["job_type", "outcome"],
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)
INE_OPERATION_EXECUTIONS_TOTAL = Counter(
    "ine_asturias_ine_operation_executions_total",
    "INE Asturias operation execution outcomes.",
    ["operation_code", "trigger_mode", "outcome"],
)
INE_OPERATION_EXECUTION_DURATION_SECONDS = Histogram(
    "ine_asturias_ine_operation_execution_duration_seconds",
    "INE Asturias operation execution duration.",
    ["operation_code", "trigger_mode", "outcome"],
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)
WORKER_HEARTBEAT_TIMESTAMP = Gauge(
    "ine_asturias_worker_heartbeat_timestamp",
    "Latest worker heartbeat timestamp.",
    ["queue_name"],
)
TERRITORIAL_BOUNDARY_FEATURES_TOTAL = Counter(
    "ine_asturias_territorial_boundary_features_total",
    "Territorial administrative boundary feature events.",
    ["source", "stage", "unit_level"],
)
TERRITORIAL_BOUNDARY_LOAD_DURATION_SECONDS = Histogram(
    "ine_asturias_territorial_boundary_load_duration_seconds",
    "Administrative boundary load duration.",
    ["source", "outcome"],
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)
TERRITORIAL_POINT_RESOLUTION_TOTAL = Counter(
    "ine_asturias_territorial_point_resolution_total",
    "Territorial point resolution outcomes.",
    ["outcome"],
)
TERRITORIAL_POINT_RESOLUTION_DURATION_SECONDS = Histogram(
    "ine_asturias_territorial_point_resolution_duration_seconds",
    "Territorial point resolution duration.",
    ["outcome"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)


def metrics_payload() -> bytes:
    return generate_latest()


def metrics_content_type() -> str:
    return CONTENT_TYPE_LATEST


def merge_metrics_payloads(
    api_payload: bytes, extra_payloads: Sequence[bytes] | None = None
) -> bytes:
    output_lines: list[str] = []
    app_metadata: OrderedDict[tuple[str, str], str] = OrderedDict()
    app_samples: OrderedDict[str, float] = OrderedDict()

    for line in _iter_prometheus_lines(api_payload):
        metric_name = _extract_metric_name(line)
        if metric_name and metric_name.startswith(APP_METRICS_PREFIX):
            _collect_app_metric_line(line, app_metadata, app_samples)
            continue
        output_lines.append(line)

    for payload in extra_payloads or ():
        for line in _iter_prometheus_lines(payload):
            metric_name = _extract_metric_name(line)
            if not metric_name or not metric_name.startswith(APP_METRICS_PREFIX):
                continue
            _collect_app_metric_line(line, app_metadata, app_samples)

    output_lines.extend(app_metadata.values())
    output_lines.extend(
        _format_sample_line(sample_key, value) for sample_key, value in app_samples.items()
    )
    return ("\n".join(output_lines) + "\n").encode("utf-8")


def record_http_request(method: str, path: str, status_code: int, duration_seconds: float) -> None:
    HTTP_REQUESTS_TOTAL.labels(method=method, path=path, status_code=str(status_code)).inc()
    HTTP_REQUEST_DURATION_SECONDS.labels(method=method, path=path).observe(duration_seconds)


def record_provider_request(
    provider: str, endpoint_family: str, status: str, duration_seconds: float
) -> None:
    PROVIDER_REQUESTS_TOTAL.labels(
        provider=provider, endpoint_family=endpoint_family, status=status
    ).inc()
    PROVIDER_REQUEST_DURATION_SECONDS.labels(
        provider=provider, endpoint_family=endpoint_family
    ).observe(duration_seconds)


def record_provider_retry(provider: str, endpoint_family: str, reason: str) -> None:
    PROVIDER_RETRIES_TOTAL.labels(
        provider=provider,
        endpoint_family=endpoint_family,
        reason=reason,
    ).inc()


def record_provider_circuit_breaker_transition(
    provider: str, previous_state: str, new_state: str
) -> None:
    PROVIDER_CIRCUIT_BREAKER_TRANSITIONS_TOTAL.labels(
        provider=provider,
        previous_state=previous_state,
        new_state=new_state,
    ).inc()


def record_provider_cache_hit(provider: str, scope: str) -> None:
    PROVIDER_CACHE_HITS_TOTAL.labels(provider=provider, scope=scope).inc()


def record_auth_failure(reason: str) -> None:
    AUTH_FAILURES_TOTAL.labels(reason=reason).inc()


def record_rate_limit_rejection(policy: str, auth_mode: str) -> None:
    RATE_LIMIT_REJECTIONS_TOTAL.labels(policy=policy, auth_mode=auth_mode).inc()


def record_analytical_flow(
    *,
    flow: str,
    outcome: str,
    storage_mode: str,
    duration_seconds: float,
    series_count: int | None = None,
    result_bytes: int | None = None,
) -> None:
    ANALYTICAL_FLOW_TOTAL.labels(
        flow=flow,
        outcome=outcome,
        storage_mode=storage_mode,
    ).inc()
    if duration_seconds >= 0:
        ANALYTICAL_FLOW_DURATION_SECONDS.labels(
            flow=flow,
            outcome=outcome,
            storage_mode=storage_mode,
        ).observe(duration_seconds)
    if series_count is not None and series_count >= 0:
        ANALYTICAL_FLOW_SERIES_COUNT.labels(
            flow=flow,
            storage_mode=storage_mode,
        ).observe(series_count)
    if result_bytes is not None and result_bytes >= 0:
        ANALYTICAL_FLOW_RESULT_BYTES.labels(
            flow=flow,
            storage_mode=storage_mode,
        ).observe(result_bytes)


def record_analytical_snapshot_event(snapshot_type: str, event: str) -> None:
    ANALYTICAL_SNAPSHOT_EVENTS_TOTAL.labels(
        snapshot_type=snapshot_type,
        event=event,
    ).inc()


def record_raw_ingestion(source_type: str) -> None:
    RAW_INGESTION_TOTAL.labels(source_type=source_type).inc()


def record_normalization(
    flow: str, generated_rows: int, discarded_counts: dict[str, int] | None = None
) -> None:
    if generated_rows > 0:
        NORMALIZATION_ROWS_GENERATED_TOTAL.labels(flow=flow).inc(generated_rows)
    for reason, count in (discarded_counts or {}).items():
        if count > 0:
            NORMALIZATION_ROWS_DROPPED_TOTAL.labels(flow=flow, reason=reason).inc(count)


def record_persistence_batch(repository: str, batch_size: int, rows_inserted: int) -> None:
    if batch_size > 0:
        PERSISTENCE_BATCHES_TOTAL.labels(repository=repository).inc()
    if rows_inserted > 0:
        PERSISTENCE_ROWS_TOTAL.labels(repository=repository).inc(rows_inserted)


def record_catalog_event(event: str) -> None:
    CATALOG_EVENTS_TOTAL.labels(event=event).inc()


def record_job_event(job_type: str, status: str) -> None:
    JOB_EVENTS_TOTAL.labels(job_type=job_type, status=status).inc()


def record_job_duration(job_type: str, outcome: str, duration_seconds: float) -> None:
    if duration_seconds >= 0:
        JOB_DURATION_SECONDS.labels(job_type=job_type, outcome=outcome).observe(duration_seconds)


def record_ine_operation_execution(
    operation_code: str,
    trigger_mode: str,
    outcome: str,
    duration_seconds: float,
) -> None:
    INE_OPERATION_EXECUTIONS_TOTAL.labels(
        operation_code=operation_code,
        trigger_mode=trigger_mode,
        outcome=outcome,
    ).inc()
    if duration_seconds >= 0:
        INE_OPERATION_EXECUTION_DURATION_SECONDS.labels(
            operation_code=operation_code,
            trigger_mode=trigger_mode,
            outcome=outcome,
        ).observe(duration_seconds)


def record_worker_heartbeat(queue_name: str) -> None:
    WORKER_HEARTBEAT_TIMESTAMP.labels(queue_name=queue_name).set(time())


def record_territorial_boundary_feature(
    source: str,
    stage: str,
    unit_level: str,
    count: int = 1,
) -> None:
    if count > 0:
        TERRITORIAL_BOUNDARY_FEATURES_TOTAL.labels(
            source=source,
            stage=stage,
            unit_level=unit_level,
        ).inc(count)


def record_territorial_boundary_load(
    source: str,
    outcome: str,
    duration_seconds: float,
) -> None:
    if duration_seconds >= 0:
        TERRITORIAL_BOUNDARY_LOAD_DURATION_SECONDS.labels(
            source=source,
            outcome=outcome,
        ).observe(duration_seconds)


def record_territorial_point_resolution(outcome: str, duration_seconds: float) -> None:
    TERRITORIAL_POINT_RESOLUTION_TOTAL.labels(outcome=outcome).inc()
    if duration_seconds >= 0:
        TERRITORIAL_POINT_RESOLUTION_DURATION_SECONDS.labels(outcome=outcome).observe(
            duration_seconds
        )


def _iter_prometheus_lines(payload: bytes) -> Iterable[str]:
    for line in payload.decode("utf-8", errors="replace").splitlines():
        if line:
            yield line


def _extract_metric_name(line: str) -> str | None:
    if line.startswith("# HELP ") or line.startswith("# TYPE "):
        parts = line.split(maxsplit=3)
        if len(parts) >= 3:
            return parts[2]
        return None
    if line.startswith("#"):
        return None
    sample_key = line.split(maxsplit=1)[0]
    return sample_key.split("{", 1)[0]


def _collect_app_metric_line(
    line: str,
    app_metadata: OrderedDict[tuple[str, str], str],
    app_samples: OrderedDict[str, float],
) -> None:
    if line.startswith("# HELP ") or line.startswith("# TYPE "):
        parts = line.split(maxsplit=3)
        if len(parts) >= 3:
            app_metadata.setdefault((parts[1], parts[2]), line)
        return

    if line.startswith("#"):
        return

    parsed = _parse_sample_line(line)
    if parsed is None:
        return

    sample_key, metric_name, value = parsed
    if metric_name.endswith("_created"):
        return

    app_samples[sample_key] = app_samples.get(sample_key, 0.0) + value


def _parse_sample_line(line: str) -> tuple[str, str, float] | None:
    parts = line.split()
    if len(parts) < 2:
        return None

    sample_key = parts[0]
    metric_name = sample_key.split("{", 1)[0]
    try:
        value = float(parts[1])
    except ValueError:
        return None

    return sample_key, metric_name, value


def _format_sample_line(sample_key: str, value: float) -> str:
    return f"{sample_key} {format(value, 'g')}"
