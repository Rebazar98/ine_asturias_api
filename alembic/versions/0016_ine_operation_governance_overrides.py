"""add override fields to ine_operation_governance

Revision ID: 0016_ine_operation_governance_overrides
Revises: 0015_ine_operation_governance
Create Date: 2026-03-24 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op


revision: str = "0016_ine_operation_governance_overrides"
down_revision: str | None = "0015_ine_operation_governance"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "ine_operation_governance",
        sa.Column(
            "override_active",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
    )
    op.add_column(
        "ine_operation_governance",
        sa.Column("override_execution_profile", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "ine_operation_governance",
        sa.Column("override_schedule_enabled", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "ine_operation_governance",
        sa.Column("override_decision_reason", sa.Text(), nullable=True),
    )
    op.add_column(
        "ine_operation_governance",
        sa.Column("override_decision_source", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "ine_operation_governance",
        sa.Column("override_applied_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("ine_operation_governance", "override_applied_at")
    op.drop_column("ine_operation_governance", "override_decision_source")
    op.drop_column("ine_operation_governance", "override_decision_reason")
    op.drop_column("ine_operation_governance", "override_schedule_enabled")
    op.drop_column("ine_operation_governance", "override_execution_profile")
    op.drop_column("ine_operation_governance", "override_active")
