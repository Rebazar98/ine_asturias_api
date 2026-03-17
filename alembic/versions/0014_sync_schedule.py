"""create sync_schedule table with geonalon seed rows

Revision ID: 0014_sync_schedule
Revises: 0013_org_id_preparation
Create Date: 2026-03-17 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op


revision: str = "0014_sync_schedule"
down_revision: str | None = "0013_org_id_preparation"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

_SEED_ROWS = [
    {"org_id": "geonalon", "source": "ine", "cron_expression": "0 3 * * *"},
    {"org_id": "geonalon", "source": "ign", "cron_expression": "0 4 * * 1"},
    {"org_id": "geonalon", "source": "catastro", "cron_expression": "0 5 * * 1"},
    {"org_id": "geonalon", "source": "sadei", "cron_expression": "0 5 * * *"},
    {"org_id": "geonalon", "source": "ideas", "cron_expression": "30 4 * * 1"},
]


def upgrade() -> None:
    sync_schedule = op.create_table(
        "sync_schedule",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("org_id", sa.String(length=64), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("cron_expression", sa.String(length=128), nullable=False),
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default="true",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", "source", name="uq_sync_schedule_org_source"),
    )
    op.create_index("ix_sync_schedule_org_id", "sync_schedule", ["org_id"], unique=False)
    op.create_index(
        "ix_sync_schedule_org_source",
        "sync_schedule",
        ["org_id", "source"],
        unique=False,
    )

    op.bulk_insert(sync_schedule, _SEED_ROWS)


def downgrade() -> None:
    op.drop_index("ix_sync_schedule_org_source", table_name="sync_schedule")
    op.drop_index("ix_sync_schedule_org_id", table_name="sync_schedule")
    op.drop_table("sync_schedule")
