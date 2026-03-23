# ruff: noqa: E402
from __future__ import annotations

import asyncio
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import asyncpg

from app.core.security import ensure_secret_strength, extract_password_from_dsn
from app.settings import get_settings


BASELINE_TABLES = {
    "ingestion_raw",
    "ine_series_normalized",
    "ine_tables_catalog",
}
TERRITORIAL_TABLES = {
    "territorial_units",
    "territorial_unit_codes",
    "territorial_unit_aliases",
}
BASELINE_REVISION = "0001_initial_schema"


def detect_head_revision() -> str:
    pattern = re.compile(r'^revision(?:\s*:\s*[^=]+)?\s*=\s*"([^"]+)"', re.MULTILINE)
    head_revision: str | None = None

    for path in sorted((PROJECT_ROOT / "alembic" / "versions").glob("*.py")):
        match = pattern.search(path.read_text(encoding="utf-8"))
        if match:
            head_revision = match.group(1)

    if not head_revision:
        raise RuntimeError("Could not determine Alembic head revision from alembic/versions.")

    return head_revision


HEAD_REVISION = detect_head_revision()


@dataclass(frozen=True, slots=True)
class ColumnSpec:
    data_type: str
    udt_name: str
    nullable: bool


EXPECTED_BASELINE_SCHEMA = {
    "ingestion_raw": {
        "id": ColumnSpec("integer", "int4", False),
        "source_type": ColumnSpec("character varying", "varchar", False),
        "source_key": ColumnSpec("character varying", "varchar", False),
        "request_path": ColumnSpec("text", "text", False),
        "request_params": ColumnSpec("jsonb", "jsonb", False),
        "payload": ColumnSpec("jsonb", "jsonb", False),
        "fetched_at": ColumnSpec("timestamp with time zone", "timestamptz", False),
    },
    "ine_series_normalized": {
        "id": ColumnSpec("integer", "int4", False),
        "operation_code": ColumnSpec("character varying", "varchar", False),
        "table_id": ColumnSpec("character varying", "varchar", False),
        "variable_id": ColumnSpec("character varying", "varchar", False),
        "geography_name": ColumnSpec("character varying", "varchar", False),
        "geography_code": ColumnSpec("character varying", "varchar", False),
        "period": ColumnSpec("character varying", "varchar", False),
        "value": ColumnSpec("double precision", "float8", True),
        "unit": ColumnSpec("character varying", "varchar", False),
        "metadata": ColumnSpec("jsonb", "jsonb", False),
        "raw_payload": ColumnSpec("jsonb", "jsonb", False),
        "inserted_at": ColumnSpec("timestamp with time zone", "timestamptz", False),
    },
    "ine_tables_catalog": {
        "id": ColumnSpec("integer", "int4", False),
        "operation_code": ColumnSpec("character varying", "varchar", False),
        "table_id": ColumnSpec("character varying", "varchar", False),
        "table_name": ColumnSpec("character varying", "varchar", False),
        "request_path": ColumnSpec("text", "text", False),
        "resolution_context": ColumnSpec("jsonb", "jsonb", False),
        "has_asturias_data": ColumnSpec("boolean", "bool", True),
        "validation_status": ColumnSpec("character varying", "varchar", False),
        "normalized_rows": ColumnSpec("integer", "int4", False),
        "raw_rows_retrieved": ColumnSpec("integer", "int4", False),
        "filtered_rows_retrieved": ColumnSpec("integer", "int4", False),
        "series_kept": ColumnSpec("integer", "int4", False),
        "series_discarded": ColumnSpec("integer", "int4", False),
        "last_checked_at": ColumnSpec("timestamp with time zone", "timestamptz", True),
        "first_seen_at": ColumnSpec("timestamp with time zone", "timestamptz", False),
        "updated_at": ColumnSpec("timestamp with time zone", "timestamptz", False),
        "metadata": ColumnSpec("jsonb", "jsonb", False),
        "notes": ColumnSpec("text", "text", False),
        "last_warning": ColumnSpec("text", "text", False),
    },
}
EXPECTED_TERRITORIAL_SCHEMA = {
    "territorial_units": {
        "id": ColumnSpec("integer", "int4", False),
        "parent_id": ColumnSpec("integer", "int4", True),
        "unit_level": ColumnSpec("character varying", "varchar", False),
        "canonical_name": ColumnSpec("character varying", "varchar", False),
        "normalized_name": ColumnSpec("character varying", "varchar", False),
        "display_name": ColumnSpec("character varying", "varchar", False),
        "country_code": ColumnSpec("character varying", "varchar", False),
        "is_active": ColumnSpec("boolean", "bool", False),
        "geometry": ColumnSpec("USER-DEFINED", "geometry", True),
        "centroid": ColumnSpec("USER-DEFINED", "geometry", True),
        "attributes": ColumnSpec("jsonb", "jsonb", False),
        "created_at": ColumnSpec("timestamp with time zone", "timestamptz", False),
        "updated_at": ColumnSpec("timestamp with time zone", "timestamptz", False),
    },
    "territorial_unit_codes": {
        "id": ColumnSpec("integer", "int4", False),
        "territorial_unit_id": ColumnSpec("integer", "int4", False),
        "source_system": ColumnSpec("character varying", "varchar", False),
        "code_type": ColumnSpec("character varying", "varchar", False),
        "code_value": ColumnSpec("character varying", "varchar", False),
        "is_primary": ColumnSpec("boolean", "bool", False),
        "created_at": ColumnSpec("timestamp with time zone", "timestamptz", False),
    },
    "territorial_unit_aliases": {
        "id": ColumnSpec("integer", "int4", False),
        "territorial_unit_id": ColumnSpec("integer", "int4", False),
        "source_system": ColumnSpec("character varying", "varchar", False),
        "alias": ColumnSpec("character varying", "varchar", False),
        "normalized_alias": ColumnSpec("character varying", "varchar", False),
        "alias_type": ColumnSpec("character varying", "varchar", False),
        "created_at": ColumnSpec("timestamp with time zone", "timestamptz", False),
    },
}


@dataclass(slots=True)
class DatabaseState:
    tables: set[str]
    columns_by_table: dict[str, dict[str, ColumnSpec]]
    alembic_version_table_present: bool
    alembic_version: str | None


async def inspect_database(asyncpg_dsn: str) -> DatabaseState:
    connection = await asyncpg.connect(asyncpg_dsn)
    try:
        table_rows = await connection.fetch(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            """
        )
        tables = {row["table_name"] for row in table_rows}

        column_rows = await connection.fetch(
            """
            SELECT table_name, column_name, data_type, udt_name, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
            """
        )
        columns_by_table: dict[str, dict[str, ColumnSpec]] = {}
        for row in column_rows:
            columns_by_table.setdefault(row["table_name"], {})[row["column_name"]] = ColumnSpec(
                data_type=row["data_type"],
                udt_name=row["udt_name"],
                nullable=row["is_nullable"] == "YES",
            )

        alembic_version_table_present = "alembic_version" in tables
        alembic_version = None
        if alembic_version_table_present:
            alembic_version = await connection.fetchval(
                "SELECT version_num FROM alembic_version LIMIT 1"
            )

        return DatabaseState(
            tables=tables,
            columns_by_table=columns_by_table,
            alembic_version_table_present=alembic_version_table_present,
            alembic_version=alembic_version,
        )
    finally:
        await connection.close()


def normalize_asyncpg_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn.split("://", 1)[1]
    return dsn


def run_alembic(*args: str) -> None:
    command = ["alembic", *args]
    print(f"[bootstrap_alembic] running: {' '.join(command)}")
    subprocess.run(command, check=True)


def validate_schema_group(
    group_name: str,
    expected_schema: dict[str, dict[str, ColumnSpec]],
    state: DatabaseState,
) -> list[str]:
    errors: list[str] = []

    for table_name, expected_columns in expected_schema.items():
        actual_columns = state.columns_by_table.get(table_name)
        if actual_columns is None:
            errors.append(f"{group_name}: missing table '{table_name}'")
            continue

        missing_columns = sorted(set(expected_columns) - set(actual_columns))
        unexpected_columns = sorted(set(actual_columns) - set(expected_columns))
        if missing_columns:
            errors.append(
                f"{group_name}: table '{table_name}' is missing columns: {', '.join(missing_columns)}"
            )
        if unexpected_columns:
            errors.append(
                f"{group_name}: table '{table_name}' has unexpected columns: {', '.join(unexpected_columns)}"
            )

        for column_name, expected_spec in expected_columns.items():
            actual_spec = actual_columns.get(column_name)
            if actual_spec is None:
                continue
            if actual_spec != expected_spec:
                errors.append(
                    f"{group_name}: table '{table_name}', column '{column_name}' mismatch: "
                    f"expected (data_type={expected_spec.data_type}, "
                    f"udt_name={expected_spec.udt_name}, nullable={expected_spec.nullable}) "
                    f"but found (data_type={actual_spec.data_type}, "
                    f"udt_name={actual_spec.udt_name}, nullable={actual_spec.nullable})"
                )

    return errors


def determine_stamp_revision(state: DatabaseState) -> str | None:
    if state.alembic_version_table_present:
        if state.alembic_version:
            print(f"[bootstrap_alembic] alembic_version already present: {state.alembic_version}")
            return None
        raise RuntimeError(
            "The database already contains the alembic_version table but it has no revision row. "
            "Manual migration alignment is required."
        )

    domain_tables = BASELINE_TABLES | TERRITORIAL_TABLES
    present_domain_tables = state.tables & domain_tables
    if not present_domain_tables:
        print("[bootstrap_alembic] empty schema detected, no stamp required")
        return None

    baseline_present = BASELINE_TABLES <= state.tables
    territorial_present = TERRITORIAL_TABLES <= state.tables
    territorial_partial = bool(state.tables & TERRITORIAL_TABLES) and not territorial_present
    baseline_partial = bool(state.tables & BASELINE_TABLES) and not baseline_present

    if baseline_partial:
        raise RuntimeError(
            "Existing database has a partial baseline schema. "
            "Manual migration alignment is required before stamping Alembic."
        )

    if territorial_partial:
        raise RuntimeError(
            "Existing database has a partial territorial schema. "
            "Manual migration alignment is required before stamping Alembic."
        )

    if baseline_present:
        baseline_errors = validate_schema_group("baseline", EXPECTED_BASELINE_SCHEMA, state)
        if baseline_errors:
            raise RuntimeError(
                "Existing database baseline schema does not match the expected initial migration:\n- "
                + "\n- ".join(baseline_errors)
            )

    if territorial_present:
        territorial_errors = validate_schema_group(
            "territorial", EXPECTED_TERRITORIAL_SCHEMA, state
        )
        if territorial_errors:
            raise RuntimeError(
                "Existing database territorial schema does not match the expected migration:\n- "
                + "\n- ".join(territorial_errors)
            )
        print(
            "[bootstrap_alembic] validated existing baseline and territorial schema, stamping head revision"
        )
        return HEAD_REVISION

    if baseline_present:
        print("[bootstrap_alembic] validated existing baseline schema, stamping baseline revision")
        return BASELINE_REVISION

    raise RuntimeError(
        "Existing database contains domain tables in an unsupported state for automatic stamping."
    )


async def async_main() -> int:
    settings = get_settings()
    if not settings.postgres_dsn:
        raise RuntimeError("POSTGRES_DSN must be configured to bootstrap Alembic.")
    if not settings.is_local_env:
        ensure_secret_strength(
            extract_password_from_dsn(settings.postgres_dsn),
            secret_name="POSTGRES_DSN password",
            min_length=16,
        )

    state = await inspect_database(normalize_asyncpg_dsn(settings.postgres_dsn))
    revision = determine_stamp_revision(state)

    if revision is not None:
        run_alembic("stamp", revision)

    run_alembic("upgrade", "head")
    return 0


def main() -> int:
    try:
        return asyncio.run(async_main())
    except subprocess.CalledProcessError as exc:
        return exc.returncode
    except Exception as exc:
        print(f"[bootstrap_alembic] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
