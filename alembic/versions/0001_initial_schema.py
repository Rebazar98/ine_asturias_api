"""Initial application schema.

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-03-12 00:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ingestion_raw",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_type", sa.String(length=64), nullable=False),
        sa.Column("source_key", sa.String(length=255), nullable=False),
        sa.Column("request_path", sa.Text(), nullable=False),
        sa.Column("request_params", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_ingestion_raw_fetched_at", "ingestion_raw", ["fetched_at"], unique=False)
    op.create_index("ix_ingestion_raw_source_key", "ingestion_raw", ["source_key"], unique=False)
    op.create_index("ix_ingestion_raw_source_type", "ingestion_raw", ["source_type"], unique=False)
    op.create_index("ix_ingestion_raw_lookup", "ingestion_raw", ["source_type", "source_key", "fetched_at"], unique=False)

    op.create_table(
        "ine_series_normalized",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("operation_code", sa.String(length=64), server_default="", nullable=False),
        sa.Column("table_id", sa.String(length=64), server_default="", nullable=False),
        sa.Column("variable_id", sa.String(length=128), server_default="", nullable=False),
        sa.Column("geography_name", sa.String(length=255), server_default="", nullable=False),
        sa.Column("geography_code", sa.String(length=128), server_default="", nullable=False),
        sa.Column("period", sa.String(length=128), server_default="", nullable=False),
        sa.Column("value", sa.Float(), nullable=True),
        sa.Column("unit", sa.String(length=128), server_default="", nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("inserted_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "operation_code",
            "table_id",
            "variable_id",
            "geography_name",
            "geography_code",
            "period",
            name="uq_ine_series_normalized_logical",
        ),
    )
    op.create_index("ix_ine_series_normalized_inserted_at", "ine_series_normalized", ["inserted_at"], unique=False)
    op.create_index("ix_ine_series_normalized_lookup", "ine_series_normalized", ["operation_code", "table_id", "geography_code", "period"], unique=False)
    op.create_index("ix_ine_series_normalized_period", "ine_series_normalized", ["period"], unique=False)

    op.create_table(
        "ine_tables_catalog",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("operation_code", sa.String(length=64), nullable=False),
        sa.Column("table_id", sa.String(length=64), nullable=False),
        sa.Column("table_name", sa.String(length=255), server_default="", nullable=False),
        sa.Column("request_path", sa.Text(), server_default="", nullable=False),
        sa.Column("resolution_context", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("has_asturias_data", sa.Boolean(), nullable=True),
        sa.Column("validation_status", sa.String(length=32), server_default="unknown", nullable=False),
        sa.Column("normalized_rows", sa.Integer(), server_default="0", nullable=False),
        sa.Column("raw_rows_retrieved", sa.Integer(), server_default="0", nullable=False),
        sa.Column("filtered_rows_retrieved", sa.Integer(), server_default="0", nullable=False),
        sa.Column("series_kept", sa.Integer(), server_default="0", nullable=False),
        sa.Column("series_discarded", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("notes", sa.Text(), server_default="", nullable=False),
        sa.Column("last_warning", sa.Text(), server_default="", nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("operation_code", "table_id", name="uq_ine_tables_catalog_operation_table"),
    )
    op.create_index("ix_ine_tables_catalog_operation_code", "ine_tables_catalog", ["operation_code"], unique=False)
    op.create_index("ix_ine_tables_catalog_table_id", "ine_tables_catalog", ["table_id"], unique=False)
    op.create_index("ix_ine_tables_catalog_operation_checked", "ine_tables_catalog", ["operation_code", "last_checked_at"], unique=False)
    op.create_index("ix_ine_tables_catalog_operation_status", "ine_tables_catalog", ["operation_code", "validation_status", "has_asturias_data"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_ine_tables_catalog_operation_status", table_name="ine_tables_catalog")
    op.drop_index("ix_ine_tables_catalog_operation_checked", table_name="ine_tables_catalog")
    op.drop_index("ix_ine_tables_catalog_table_id", table_name="ine_tables_catalog")
    op.drop_index("ix_ine_tables_catalog_operation_code", table_name="ine_tables_catalog")
    op.drop_table("ine_tables_catalog")

    op.drop_index("ix_ine_series_normalized_period", table_name="ine_series_normalized")
    op.drop_index("ix_ine_series_normalized_lookup", table_name="ine_series_normalized")
    op.drop_index("ix_ine_series_normalized_inserted_at", table_name="ine_series_normalized")
    op.drop_table("ine_series_normalized")

    op.drop_index("ix_ingestion_raw_lookup", table_name="ingestion_raw")
    op.drop_index("ix_ingestion_raw_source_type", table_name="ingestion_raw")
    op.drop_index("ix_ingestion_raw_source_key", table_name="ingestion_raw")
    op.drop_index("ix_ingestion_raw_fetched_at", table_name="ingestion_raw")
    op.drop_table("ingestion_raw")
