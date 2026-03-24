"""create ine_operation_governance_history table

Revision ID: 0017_ine_op_gov_hist
Revises: 0016_ine_op_governance_ovr
Create Date: 2026-03-24 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "0017_ine_op_gov_hist"
down_revision: str | None = "0016_ine_op_governance_ovr"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "ine_operation_governance_history",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("operation_code", sa.String(length=64), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column(
            "effective_execution_profile_before",
            sa.String(length=32),
            nullable=True,
        ),
        sa.Column(
            "effective_execution_profile_after",
            sa.String(length=32),
            nullable=True,
        ),
        sa.Column("schedule_enabled_before", sa.Boolean(), nullable=True),
        sa.Column("schedule_enabled_after", sa.Boolean(), nullable=True),
        sa.Column("background_required_before", sa.Boolean(), nullable=True),
        sa.Column("background_required_after", sa.Boolean(), nullable=True),
        sa.Column("override_active_before", sa.Boolean(), nullable=True),
        sa.Column("override_active_after", sa.Boolean(), nullable=True),
        sa.Column("decision_reason", sa.Text(), nullable=True),
        sa.Column("decision_source", sa.String(length=64), nullable=True),
        sa.Column("override_decision_reason", sa.Text(), nullable=True),
        sa.Column("override_decision_source", sa.String(length=64), nullable=True),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ine_operation_governance_history_operation_code",
        "ine_operation_governance_history",
        ["operation_code"],
        unique=False,
    )
    op.create_index(
        "ix_ine_operation_governance_history_event_type",
        "ine_operation_governance_history",
        ["event_type"],
        unique=False,
    )
    op.create_index(
        "ix_ine_operation_governance_history_occurred_at",
        "ine_operation_governance_history",
        ["occurred_at"],
        unique=False,
    )
    op.create_index(
        "ix_ine_op_gov_history_operation_occurred",
        "ine_operation_governance_history",
        ["operation_code", "occurred_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_ine_op_gov_history_operation_occurred",
        table_name="ine_operation_governance_history",
    )
    op.drop_index(
        "ix_ine_operation_governance_history_occurred_at",
        table_name="ine_operation_governance_history",
    )
    op.drop_index(
        "ix_ine_operation_governance_history_event_type",
        table_name="ine_operation_governance_history",
    )
    op.drop_index(
        "ix_ine_operation_governance_history_operation_code",
        table_name="ine_operation_governance_history",
    )
    op.drop_table("ine_operation_governance_history")
