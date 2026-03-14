from __future__ import annotations

import httpx

from scripts import smoke_stack


def test_wait_for_json_condition_retries_until_service_is_ready() -> None:
    request = httpx.Request("GET", "http://testserver/health")
    attempts = iter(
        [
            httpx.ReadError("connection reset", request=request),
            httpx.Response(200, json={"status": "starting"}),
            httpx.Response(200, json={"status": "ok"}),
        ]
    )

    def handler(_: httpx.Request) -> httpx.Response:
        result = next(attempts)
        if isinstance(result, Exception):
            raise result
        return result

    with httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
    ) as client:
        payload = smoke_stack._wait_for_json_condition(
            client=client,
            path="/health",
            timeout_seconds=3.0,
            poll_interval=0.0,
            expected_status=200,
            description="/health",
            validator=lambda body: body.get("status") == "ok",
        )

    assert payload == {"status": "ok"}


def test_get_json_retries_transient_request_errors() -> None:
    request = httpx.Request("GET", "http://testserver/health")
    attempts = iter(
        [
            httpx.ReadError("connection reset", request=request),
            httpx.Response(200, json={"status": "ok"}),
        ]
    )

    def handler(_: httpx.Request) -> httpx.Response:
        result = next(attempts)
        if isinstance(result, Exception):
            raise result
        return result

    with httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
    ) as client:
        payload = smoke_stack._get_json(client, "/health", expected_status=200)

    assert payload == {"status": "ok"}


def test_wait_for_terminal_job_state_supports_status_paths() -> None:
    statuses = iter(
        [
            httpx.Response(200, json={"status": "queued"}),
            httpx.Response(200, json={"status": "completed", "result": {"ok": True}}),
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/territorios/jobs/job-123"
        return next(statuses)

    with httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
    ) as client:
        payload = smoke_stack._wait_for_terminal_job_state(
            client=client,
            job_path="/territorios/jobs/job-123",
            timeout_seconds=3.0,
            poll_interval=0.0,
        )

    assert payload["status"] == "completed"
    assert payload["result"] == {"ok": True}


def test_validate_territorial_catalog_accepts_expected_resources() -> None:
    catalog_payload = {
        "source": "internal.catalog.territorial",
        "territorial_levels": [{"unit_level": "municipality", "units_total": 1, "active_units": 1}],
        "resources": [
            {"resource_key": "territorial.autonomous_communities.list"},
            {"resource_key": "territorial.provinces.list"},
            {"resource_key": "territorial.municipality.detail"},
            {"resource_key": "territorial.municipality.summary"},
            {"resource_key": "territorial.municipality.report_job"},
            {"resource_key": "territorial.jobs.status"},
        ],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/territorios/catalogo"
        return httpx.Response(200, json=catalog_payload)

    with httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
    ) as client:
        payload = smoke_stack._validate_territorial_catalog(client)

    assert payload == catalog_payload


def test_validate_municipality_analytics_checks_summary_and_report_job() -> None:
    job_statuses = iter(
        [
            httpx.Response(200, json={"status": "running"}),
            httpx.Response(
                200,
                json={
                    "status": "completed",
                    "result": {
                        "report_type": "municipality_report",
                        "territorial_context": {"municipality_code": "33044"},
                        "sections": [{"section_key": "latest_indicators"}],
                    },
                },
            ),
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/territorios/municipio/33044/resumen":
            assert request.url.query == b"page=1&page_size=5"
            return httpx.Response(
                200,
                json={
                    "source": "internal.analytics.municipality_summary",
                    "filters": {"municipality_code": "33044"},
                },
            )
        if request.method == "POST" and request.url.path == "/territorios/municipio/33044/informe":
            assert request.url.query == b"page=1&page_size=5"
            return httpx.Response(
                202,
                json={
                    "job_id": "job-123",
                    "status_path": "/territorios/jobs/job-123",
                },
            )
        if request.method == "GET" and request.url.path == "/territorios/jobs/job-123":
            return next(job_statuses)
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    with httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
    ) as client:
        smoke_stack._validate_municipality_analytics(
            client=client,
            municipality_code="33044",
            page_size=5,
            timeout_seconds=3.0,
            poll_interval=0.0,
        )
