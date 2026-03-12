"""add territorial unit reference to ine series

Revision ID: 0003_territorial_series_ref
Revises: 0002_postgis_territorial
Create Date: 2026-03-12 16:15:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op


revision: str = "0003_territorial_series_ref"
down_revision: str | None = "0002_postgis_territorial"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "ine_series_normalized",
        sa.Column("territorial_unit_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_ine_series_normalized_territorial_unit",
        "ine_series_normalized",
        "territorial_units",
        ["territorial_unit_id"],
        ["id"],
    )
    op.create_index(
        "ix_ine_series_normalized_territorial_unit_id",
        "ine_series_normalized",
        ["territorial_unit_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_ine_series_normalized_territorial_unit_id", table_name="ine_series_normalized")
    op.drop_constraint(
        "fk_ine_series_normalized_territorial_unit",
        "ine_series_normalized",
        type_="foreignkey",
    )
    op.drop_column("ine_series_normalized", "territorial_unit_id")
