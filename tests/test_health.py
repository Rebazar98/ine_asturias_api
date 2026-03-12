import httpx


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
                "ine_asturias_worker_only_total{source=\"worker\"} 3\n"
                "# HELP python_gc_objects_collected_total Objects collected during gc\n"
                "# TYPE python_gc_objects_collected_total counter\n"
                "python_gc_objects_collected_total 999\n"
            ),
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(client.app.state.http_client, "get", fake_get)

    response = client.get("/metrics")

    assert response.status_code == 200
    assert "ine_asturias_worker_only_total{source=\"worker\"} 3" in response.text
    assert "python_gc_objects_collected_total 999" not in response.text
