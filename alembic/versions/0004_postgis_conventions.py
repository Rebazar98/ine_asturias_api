"""add postgis spatial indexes and conventions

Revision ID: 0004_postgis_conventions
Revises: 0003_territorial_series_ref
Create Date: 2026-03-12 16:35:00
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0004_postgis_conventions"
down_revision: str | None = "0003_territorial_series_ref"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_territorial_units_geometry_gist",
        "territorial_units",
        ["geometry"],
        unique=False,
        postgresql_using="gist",
    )
    op.create_index(
        "ix_territorial_units_centroid_gist",
        "territorial_units",
        ["centroid"],
        unique=False,
        postgresql_using="gist",
    )


def downgrade() -> None:
    op.drop_index("ix_territorial_units_centroid_gist", table_name="territorial_units")
    op.drop_index("ix_territorial_units_geometry_gist", table_name="territorial_units")
