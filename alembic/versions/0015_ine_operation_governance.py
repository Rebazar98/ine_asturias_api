"""create ine_operation_governance table

Revision ID: 0015_ine_operation_governance
Revises: 0014_sync_schedule
Create Date: 2026-03-24 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "0015_ine_operation_governance"
down_revision: str | None = "0014_sync_schedule"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "ine_operation_governance",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("operation_code", sa.String(length=64), nullable=False),
        sa.Column("execution_profile", sa.String(length=32), nullable=False),
        sa.Column(
            "schedule_enabled",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
        sa.Column("decision_reason", sa.Text(), nullable=False, server_default=""),
        sa.Column(
            "decision_source",
            sa.String(length=64),
            nullable=False,
            server_default="runtime_settings",
        ),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("last_job_id", sa.String(length=64), nullable=True),
        sa.Column("last_run_status", sa.String(length=32), nullable=True),
        sa.Column("last_trigger_mode", sa.String(length=32), nullable=True),
        sa.Column(
            "last_background_forced",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
        sa.Column("last_background_reason", sa.String(length=128), nullable=True),
        sa.Column("last_run_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_duration_ms", sa.Integer(), nullable=True),
        sa.Column("last_tables_found", sa.Integer(), nullable=True),
        sa.Column("last_tables_selected", sa.Integer(), nullable=True),
        sa.Column("last_tables_succeeded", sa.Integer(), nullable=True),
        sa.Column("last_tables_failed", sa.Integer(), nullable=True),
        sa.Column("last_tables_skipped_catalog", sa.Integer(), nullable=True),
        sa.Column("last_normalized_rows", sa.Integer(), nullable=True),
        sa.Column("last_warning_count", sa.Integer(), nullable=True),
        sa.Column("last_error_message", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "operation_code",
            name="uq_ine_operation_governance_operation_code",
        ),
    )
    op.create_index(
        "ix_ine_operation_governance_operation_code",
        "ine_operation_governance",
        ["operation_code"],
        unique=False,
    )
    op.create_index(
        "ix_ine_operation_governance_last_job_id",
        "ine_operation_governance",
        ["last_job_id"],
        unique=False,
    )
    op.create_index(
        "ix_ine_operation_governance_last_run_status",
        "ine_operation_governance",
        ["last_run_status"],
        unique=False,
    )
    op.create_index(
        "ix_ine_operation_governance_execution_profile",
        "ine_operation_governance",
        ["execution_profile"],
        unique=False,
    )
    op.create_index(
        "ix_ine_operation_governance_profile_status",
        "ine_operation_governance",
        ["execution_profile", "schedule_enabled", "last_run_status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_ine_operation_governance_profile_status",
        table_name="ine_operation_governance",
    )
    op.drop_index(
        "ix_ine_operation_governance_execution_profile",
        table_name="ine_operation_governance",
    )
    op.drop_index(
        "ix_ine_operation_governance_last_run_status",
        table_name="ine_operation_governance",
    )
    op.drop_index(
        "ix_ine_operation_governance_last_job_id",
        table_name="ine_operation_governance",
    )
    op.drop_index(
        "ix_ine_operation_governance_operation_code",
        table_name="ine_operation_governance",
    )
    op.drop_table("ine_operation_governance")
