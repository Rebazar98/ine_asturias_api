import time

from tests.conftest import seed_municipality_analytics_context


def _wait_for_terminal_job(client, status_path: str) -> dict:
    job_payload = None
    for _ in range(50):
        status_response = client.get(status_path)
        assert status_response.status_code == 200
        job_payload = status_response.json()
        if job_payload["status"] in {"completed", "failed"}:
            break
        time.sleep(0.02)

    assert job_payload is not None
    assert job_payload["status"] in {"completed", "failed"}
    return job_payload


def test_catalog_summary_and_report_job_keep_semantic_contracts(
    client, dummy_territorial_repo, dummy_series_repo, dummy_analytical_snapshot_repo
):
    seeded = seed_municipality_analytics_context(dummy_territorial_repo, dummy_series_repo)

    catalog_response = client.get("/territorios/catalogo")
    assert catalog_response.status_code == 200
    catalog_payload = catalog_response.json()
    resources = {resource["resource_key"]: resource for resource in catalog_payload["resources"]}

    summary_resource = resources["territorial.municipality.summary"]
    report_resource = resources["territorial.municipality.report_job"]
    job_resource = resources["territorial.jobs.status"]

    summary_response = client.get(
        f"/territorios/municipio/{seeded['municipality_code']}/resumen"
        "?operation_code=22&page=1&page_size=10"
    )
    assert summary_response.status_code == 200
    summary_payload = summary_response.json()

    accepted_response = client.post(
        f"/territorios/municipio/{seeded['municipality_code']}/informe"
        "?operation_code=22&page=1&page_size=10"
    )
    assert accepted_response.status_code == 202
    accepted_payload = accepted_response.json()

    assert accepted_payload["status_path"] == (
        job_resource["path"].replace("{job_id}", accepted_payload["job_id"])
    )
    assert accepted_payload["status"] == "queued"
    assert accepted_payload["params"] == {
        "municipality_code": seeded["municipality_code"],
        "operation_code": "22",
        "variable_id": None,
        "period_from": None,
        "period_to": None,
        "page": 1,
        "page_size": 10,
    }

    job_payload = _wait_for_terminal_job(client, accepted_payload["status_path"])
    assert job_payload["status"] == "completed"

    report_payload = job_payload["result"]
    assert summary_resource["path"].endswith("/resumen")
    assert report_resource["path"].endswith("/informe")
    assert report_payload["report_type"] == "municipality_report"
    assert report_payload["filters"] == summary_payload["filters"]
    assert report_payload["summary"] == summary_payload["summary"]
    assert report_payload["territorial_context"] == summary_payload["territorial_context"]
    assert report_payload["territorial_unit"] == summary_payload["territorial_unit"]
    assert {item["series_key"] for item in report_payload["series"]} == {
        item["series_key"] for item in summary_payload["series"]
    }
    latest_indicators = next(
        section
        for section in report_payload["sections"]
        if section["section_key"] == "latest_indicators"
    )
    assert {item["series_key"] for item in latest_indicators["series"]} == {
        item["series_key"] for item in summary_payload["series"]
    }
    assert report_payload["metadata"]["storage_mode"] == "persistent_snapshot"
    assert report_payload["metadata"]["snapshot_reused"] is False
    assert len(dummy_analytical_snapshot_repo.rows) == 1


def test_report_job_snapshot_scope_changes_with_filters(
    client, dummy_territorial_repo, dummy_series_repo, dummy_analytical_snapshot_repo
):
    seeded = seed_municipality_analytics_context(dummy_territorial_repo, dummy_series_repo)

    first_response = client.post(
        f"/territorios/municipio/{seeded['municipality_code']}/informe"
        "?operation_code=22&variable_id=POP_TOTAL&page=1&page_size=10"
    )
    assert first_response.status_code == 202
    first_job = _wait_for_terminal_job(client, first_response.json()["status_path"])
    assert first_job["status"] == "completed"

    second_response = client.post(
        f"/territorios/municipio/{seeded['municipality_code']}/informe"
        "?operation_code=22&variable_id=POP_TOTAL&page=1&page_size=10"
    )
    assert second_response.status_code == 202
    second_job = _wait_for_terminal_job(client, second_response.json()["status_path"])
    assert second_job["status"] == "completed"

    third_response = client.post(
        f"/territorios/municipio/{seeded['municipality_code']}/informe"
        "?operation_code=22&variable_id=AGEING_INDEX&page=1&page_size=10"
    )
    assert third_response.status_code == 202
    third_job = _wait_for_terminal_job(client, third_response.json()["status_path"])
    assert third_job["status"] == "completed"

    first_snapshot_key = first_job["result"]["metadata"]["snapshot_key"]
    second_snapshot_key = second_job["result"]["metadata"]["snapshot_key"]
    third_snapshot_key = third_job["result"]["metadata"]["snapshot_key"]

    assert first_snapshot_key == second_snapshot_key
    assert first_job["result"]["metadata"]["snapshot_reused"] is False
    assert second_job["result"]["metadata"]["snapshot_reused"] is True
    assert third_snapshot_key != first_snapshot_key
    assert third_job["result"]["metadata"]["snapshot_reused"] is False
    assert len(dummy_analytical_snapshot_repo.rows) == 2
    assert dummy_analytical_snapshot_repo.upsert_calls == 2
    assert dummy_series_repo.latest_indicator_calls == 2

    filters_by_key = {
        key: row["filters"]["variable_id"]
        for key, row in dummy_analytical_snapshot_repo.rows.items()
    }
    assert filters_by_key[first_snapshot_key] == "POP_TOTAL"
    assert filters_by_key[third_snapshot_key] == "AGEING_INDEX"
