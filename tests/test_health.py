import re
import time

import httpx

from app.repositories.territorial import TERRITORIAL_UNIT_LEVEL_MUNICIPALITY
from app.schemas import NormalizedSeriesItem


def test_health_endpoint_returns_ok(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readiness_endpoint_reports_disabled_dependencies_in_local_mode(client):
    response = client.get("/health/ready")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["components"]["postgres"]["status"] == "disabled"
    assert payload["components"]["redis"]["status"] == "disabled"
    assert payload["components"]["worker"]["status"] == "disabled"


def test_metrics_endpoint_merges_worker_application_metrics(client, monkeypatch):
    worker_metrics_url = "http://worker:9001/metrics"
    client.app.state.settings.worker_metrics_url = worker_metrics_url

    async def fake_get(url, timeout=None):
        assert url == worker_metrics_url
        return httpx.Response(
            200,
            text=(
                "# HELP ine_asturias_worker_only_total Worker-only metric.\n"
                "# TYPE ine_asturias_worker_only_total counter\n"
                'ine_asturias_worker_only_total{source="worker"} 3\n'
                "# HELP python_gc_objects_collected_total Objects collected during gc\n"
                "# TYPE python_gc_objects_collected_total counter\n"
                "python_gc_objects_collected_total 999\n"
            ),
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(client.app.state.http_client, "get", fake_get)

    response = client.get("/metrics")

    assert response.status_code == 200
    assert 'ine_asturias_worker_only_total{source="worker"} 3' in response.text
    assert "python_gc_objects_collected_total 999" not in response.text


def test_metrics_endpoint_exposes_analytical_flow_metrics(
    client, dummy_territorial_repo, dummy_series_repo, dummy_analytical_snapshot_repo
):
    client.app.state.settings.worker_metrics_url = None
    dummy_territorial_repo.detail_by_canonical_code[
        (TERRITORIAL_UNIT_LEVEL_MUNICIPALITY, "33044")
    ] = {
        "id": 33044,
        "parent_id": 33,
        "unit_level": "municipality",
        "canonical_name": "Oviedo",
        "display_name": "Oviedo",
        "country_code": "ES",
        "is_active": True,
        "canonical_code_strategy": {"source_system": "ine", "code_type": "municipality"},
        "canonical_code": {
            "source_system": "ine",
            "code_type": "municipality",
            "code_value": "33044",
            "is_primary": True,
        },
        "codes": [
            {
                "source_system": "ine",
                "code_type": "municipality",
                "code_value": "33044",
                "is_primary": True,
            }
        ],
        "aliases": [],
        "attributes": {"population_scope": "municipal"},
    }
    dummy_series_repo.items.extend(
        [
            NormalizedSeriesItem(
                operation_code="22",
                table_id="2852",
                variable_id="POP_TOTAL",
                geography_name="Oviedo",
                geography_code="33044",
                period="2024",
                value=220543,
                unit="personas",
                metadata={"series_name": "Poblacion total"},
            ),
            NormalizedSeriesItem(
                operation_code="22",
                table_id="3901",
                variable_id="AGEING_INDEX",
                geography_name="Oviedo",
                geography_code="33044",
                period="2024M01",
                value=142.5,
                unit="indice",
                metadata={"series_name": "Indice de envejecimiento"},
            ),
        ]
    )

    summary_response = client.get("/territorios/municipio/33044/resumen?page=1&page_size=10")
    assert summary_response.status_code == 200

    first_job_response = client.post("/territorios/municipio/33044/informe?page=1&page_size=10")
    assert first_job_response.status_code == 202
    for _ in range(50):
        status_response = client.get(first_job_response.json()["status_path"])
        assert status_response.status_code == 200
        if status_response.json()["status"] == "completed":
            break
        time.sleep(0.02)

    second_job_response = client.post("/territorios/municipio/33044/informe?page=1&page_size=10")
    assert second_job_response.status_code == 202
    for _ in range(50):
        status_response = client.get(second_job_response.json()["status_path"])
        assert status_response.status_code == 200
        if status_response.json()["status"] == "completed":
            break
        time.sleep(0.02)

    response = client.get("/metrics")

    assert response.status_code == 200
    assert (
        'ine_asturias_analytical_flow_total{flow="municipality_summary",outcome="completed",storage_mode="direct"}'
        in response.text
    )
    assert (
        'ine_asturias_analytical_flow_total{flow="municipality_report",outcome="completed",storage_mode="persistent_snapshot"}'
        in response.text
    )
    assert (
        'ine_asturias_analytical_snapshot_events_total{snapshot_type="municipality_report",event="persisted"}'
        in response.text
        or 'ine_asturias_analytical_snapshot_events_total{event="persisted",snapshot_type="municipality_report"}'
        in response.text
    )
    assert (
        'ine_asturias_analytical_snapshot_events_total{snapshot_type="municipality_report",event="hit"}'
        in response.text
        or 'ine_asturias_analytical_snapshot_events_total{event="hit",snapshot_type="municipality_report"}'
        in response.text
    )
    assert (
        re.search(
            r'ine_asturias_analytical_flow_duration_seconds_bucket\{[^}]*flow="municipality_report"[^}]*outcome="completed"[^}]*storage_mode="persistent_snapshot"[^}]*\}',
            response.text,
        )
        is not None
    )
    assert (
        re.search(
            r'ine_asturias_analytical_flow_series_count_bucket\{[^}]*flow="municipality_report"[^}]*storage_mode="persistent_snapshot"[^}]*\}',
            response.text,
        )
        is not None
    )
    assert (
        re.search(
            r'ine_asturias_analytical_flow_result_bytes_bucket\{[^}]*flow="municipality_report"[^}]*storage_mode="persistent_snapshot"[^}]*\}',
            response.text,
        )
        is not None
    )
