"""add source_provider column to ine_series_normalized and ine_tables_catalog

Revision ID: 0010_source_provider_column
Revises: 0009_catastro_territorial_cache
Create Date: 2026-03-17 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op


revision: str = "0010_source_provider_column"
down_revision: str | None = "0009_catastro_territorial_cache"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    for table in ("ine_series_normalized", "ine_tables_catalog"):
        op.add_column(
            table,
            sa.Column(
                "source_provider",
                sa.String(length=32),
                nullable=False,
                server_default="ine",
            ),
        )
        op.create_index(
            f"ix_{table}_source_provider",
            table,
            ["source_provider"],
            unique=False,
        )


def downgrade() -> None:
    for table in ("ine_series_normalized", "ine_tables_catalog"):
        op.drop_index(f"ix_{table}_source_provider", table_name=table)
        op.drop_column(table, "source_provider")
