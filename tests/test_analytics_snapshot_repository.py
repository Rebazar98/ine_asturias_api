import asyncio

from app.repositories.analytics_snapshots import (
    AnalyticalSnapshotRepository,
    build_snapshot_key,
    normalize_snapshot_scope_key,
)


def test_build_snapshot_key_is_stable_for_semantically_equal_filters():
    left = build_snapshot_key(
        snapshot_type="municipality_report",
        scope_key="  Municipality:33044  ",
        filters={"page_size": 10, "page": 1, "operation_code": None},
    )
    right = build_snapshot_key(
        snapshot_type="municipality_report",
        scope_key="municipality:33044",
        filters={"operation_code": None, "page": 1, "page_size": 10},
    )

    assert left == right
    assert normalize_snapshot_scope_key("  Municipality:33044  ") == "municipality:33044"


def test_get_fresh_snapshot_returns_none_when_database_disabled():
    repository = AnalyticalSnapshotRepository(session=None)

    result = asyncio.run(
        repository.get_fresh_snapshot(
            snapshot_type="municipality_report",
            scope_key="municipality:33044",
            filters={"page": 1, "page_size": 10},
        )
    )

    assert result is None
