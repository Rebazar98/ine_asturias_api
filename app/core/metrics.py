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
PROVIDER_CACHE_HITS_TOTAL = Counter(
    "ine_asturias_provider_cache_hits_total",
    "Provider cache hits.",
    ["provider", "scope"],
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
WORKER_HEARTBEAT_TIMESTAMP = Gauge(
    "ine_asturias_worker_heartbeat_timestamp",
    "Latest worker heartbeat timestamp.",
    ["queue_name"],
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


def record_provider_cache_hit(provider: str, scope: str) -> None:
    PROVIDER_CACHE_HITS_TOTAL.labels(provider=provider, scope=scope).inc()


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


def record_worker_heartbeat(queue_name: str) -> None:
    WORKER_HEARTBEAT_TIMESTAMP.labels(queue_name=queue_name).set(time())


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
