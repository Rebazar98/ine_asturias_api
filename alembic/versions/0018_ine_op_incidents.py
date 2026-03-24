"""add ine operation incidents and governance streaks

Revision ID: 0018_ine_op_incidents
Revises: 0017_ine_op_gov_hist
Create Date: 2026-03-24 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "0018_ine_op_incidents"
down_revision: str | None = "0017_ine_op_gov_hist"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "ine_operation_governance",
        sa.Column("failure_streak", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "ine_operation_governance",
        sa.Column("no_data_streak", sa.Integer(), nullable=False, server_default="0"),
    )

    op.create_table(
        "ine_operation_incidents",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("operation_code", sa.String(length=64), nullable=False),
        sa.Column("incident_type", sa.String(length=64), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column(
            "first_seen_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("last_resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("occurrence_count", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("last_job_id", sa.String(length=64), nullable=True),
        sa.Column("last_run_status", sa.String(length=32), nullable=True),
        sa.Column("suggested_action", sa.String(length=64), nullable=True),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
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
    )
    op.create_index(
        "ix_ine_operation_incidents_operation_code",
        "ine_operation_incidents",
        ["operation_code"],
        unique=False,
    )
    op.create_index(
        "ix_ine_operation_incidents_incident_type",
        "ine_operation_incidents",
        ["incident_type"],
        unique=False,
    )
    op.create_index(
        "ix_ine_operation_incidents_severity",
        "ine_operation_incidents",
        ["severity"],
        unique=False,
    )
    op.create_index(
        "ix_ine_operation_incidents_status",
        "ine_operation_incidents",
        ["status"],
        unique=False,
    )
    op.create_index(
        "ix_ine_operation_incidents_last_job_id",
        "ine_operation_incidents",
        ["last_job_id"],
        unique=False,
    )
    op.create_index(
        "ix_ine_operation_incidents_last_run_status",
        "ine_operation_incidents",
        ["last_run_status"],
        unique=False,
    )
    op.create_index(
        "ix_ine_operation_incidents_status_severity",
        "ine_operation_incidents",
        ["status", "severity"],
        unique=False,
    )
    op.create_index(
        "ix_ine_op_incident_operation_status",
        "ine_operation_incidents",
        ["operation_code", "status"],
        unique=False,
    )
    op.create_index(
        "uq_ine_op_incident_open",
        "ine_operation_incidents",
        ["operation_code", "incident_type"],
        unique=True,
        postgresql_where=sa.text("status = 'open'"),
    )


def downgrade() -> None:
    op.drop_index("uq_ine_op_incident_open", table_name="ine_operation_incidents")
    op.drop_index(
        "ix_ine_op_incident_operation_status",
        table_name="ine_operation_incidents",
    )
    op.drop_index(
        "ix_ine_operation_incidents_status_severity",
        table_name="ine_operation_incidents",
    )
    op.drop_index(
        "ix_ine_operation_incidents_last_run_status",
        table_name="ine_operation_incidents",
    )
    op.drop_index(
        "ix_ine_operation_incidents_last_job_id",
        table_name="ine_operation_incidents",
    )
    op.drop_index("ix_ine_operation_incidents_status", table_name="ine_operation_incidents")
    op.drop_index("ix_ine_operation_incidents_severity", table_name="ine_operation_incidents")
    op.drop_index(
        "ix_ine_operation_incidents_incident_type",
        table_name="ine_operation_incidents",
    )
    op.drop_index(
        "ix_ine_operation_incidents_operation_code",
        table_name="ine_operation_incidents",
    )
    op.drop_table("ine_operation_incidents")
    op.drop_column("ine_operation_governance", "no_data_streak")
    op.drop_column("ine_operation_governance", "failure_streak")
