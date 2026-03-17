"""add org_id column to ingestion_raw, ine_series_normalized, territorial_units

Revision ID: 0013_org_id_preparation
Revises: 0012_cartographic_qa_incidents
Create Date: 2026-03-17 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op


revision: str = "0013_org_id_preparation"
down_revision: str | None = "0012_cartographic_qa_incidents"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

_TABLES = ("ingestion_raw", "ine_series_normalized", "territorial_units")


def upgrade() -> None:
    for table in _TABLES:
        op.add_column(
            table,
            sa.Column(
                "org_id",
                sa.String(length=64),
                nullable=True,
                server_default="geonalon",
            ),
        )
        op.create_index(f"ix_{table}_org_id", table, ["org_id"], unique=False)


def downgrade() -> None:
    for table in reversed(_TABLES):
        op.drop_index(f"ix_{table}_org_id", table_name=table)
        op.drop_column(table, "org_id")
