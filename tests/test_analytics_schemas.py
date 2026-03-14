from app.schemas import (
    AnalyticalErrorResponse,
    AnalyticalResponse,
)


def test_analytical_response_schema_supports_semantic_contract() -> None:
    response = AnalyticalResponse.model_validate(
        {
            "source": "internal.analytics.territorial_summary",
            "generated_at": "2026-03-14T13:30:00Z",
            "territorial_context": {
                "territorial_unit_id": 44,
                "unit_level": "municipality",
                "canonical_code": "33044",
                "canonical_name": "Oviedo",
                "display_name": "Oviedo",
                "source_system": "ine",
                "country_code": "ES",
                "autonomous_community_code": "33",
                "province_code": "33",
                "municipality_code": "33044",
            },
            "filters": {
                "indicator_family": "population",
                "period_from": "2020",
                "period_to": "2024",
            },
            "summary": {
                "series_count": 2,
                "last_period": "2024",
            },
            "series": [
                {
                    "series_key": "population.total",
                    "label": "Poblacion total",
                    "value": 220543,
                    "unit": "personas",
                    "period": "2024",
                    "metadata": {
                        "operation_code": "22",
                        "table_id": "2852",
                    },
                },
                {
                    "series_key": "population.delta",
                    "label": "Variacion interanual",
                    "value": 1250,
                    "unit": "personas",
                    "period": "2024",
                },
            ],
            "metadata": {
                "consumer_profile": "automation",
            },
            "pagination": {
                "total": 2,
                "page": 1,
                "page_size": 50,
                "pages": 1,
                "has_next": False,
                "has_previous": False,
            },
        }
    )

    assert response.source == "internal.analytics.territorial_summary"
    assert response.territorial_context.canonical_code == "33044"
    assert response.summary["series_count"] == 2
    assert response.series[0].series_key == "population.total"
    assert response.pagination is not None
    assert response.pagination.total == 2


def test_analytical_error_response_schema_supports_semantic_contract() -> None:
    response = AnalyticalErrorResponse.model_validate(
        {
            "detail": {
                "code": "territorial_summary_not_ready",
                "message": "The requested analytical snapshot is not ready yet.",
                "retryable": True,
                "metadata": {
                    "job_id": "job-123",
                    "status_path": "/territorios/jobs/job-123",
                },
            }
        }
    )

    assert response.detail.code == "territorial_summary_not_ready"
    assert response.detail.retryable is True
    assert response.detail.metadata["job_id"] == "job-123"
