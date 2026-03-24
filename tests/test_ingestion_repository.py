from __future__ import annotations

from app.repositories.ingestion import IngestionRepository


def test_truncate_payload_if_needed_preserves_small_payload() -> None:
    repo = IngestionRepository(session=None)
    payload = {"summary": {"normalized_rows": 3}, "errors": []}

    result = repo._truncate_payload_if_needed(
        source_type="operation_series_direct",
        source_key="22",
        request_path="SERIES_OPERACION/22 -> DATOS_SERIE/*",
        request_params={},
        payload=payload,
        max_payload_bytes=4096,
    )

    assert result == payload


def test_truncate_payload_if_needed_summarizes_large_payload() -> None:
    repo = IngestionRepository(session=None)
    payload = {
        "summary": {"normalized_rows": 0},
        "errors": [{"error": "x" * 2000}, {"error": "y" * 2000}],
        "warnings": [{"warning": "large_table_detected"}],
    }

    result = repo._truncate_payload_if_needed(
        source_type="operation_series_direct",
        source_key="22",
        request_path="SERIES_OPERACION/22 -> DATOS_SERIE/*",
        request_params={"nult": 5},
        payload=payload,
        max_payload_bytes=256,
    )

    assert result["payload_truncated"] is True
    assert result["source_type"] == "operation_series_direct"
    assert result["request_params"] == {"nult": 5}
    assert result["payload_shape"]["summary"] == {"normalized_rows": 0}
    assert len(result["payload_shape"]["errors_sample"]) == 2
