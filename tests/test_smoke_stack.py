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
