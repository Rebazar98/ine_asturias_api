"""add catastro municipality aggregate cache

Revision ID: 0008_catastro_cache
Revises: 0007_territorial_exports
Create Date: 2026-03-15 15:45:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "0008_catastro_cache"
down_revision: str | None = "0007_territorial_exports"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "catastro_municipality_aggregate_cache",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("provider_family", sa.String(length=64), nullable=False),
        sa.Column("municipality_code", sa.String(length=128), nullable=False),
        sa.Column("reference_year", sa.String(length=8), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "cached_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "provider_family",
            "municipality_code",
            "reference_year",
            name="uq_catastro_municipality_aggregate_cache_scope",
        ),
    )
    op.create_index(
        "ix_catastro_municipality_aggregate_cache_provider_family",
        "catastro_municipality_aggregate_cache",
        ["provider_family"],
        unique=False,
    )
    op.create_index(
        "ix_catastro_municipality_aggregate_cache_municipality_code",
        "catastro_municipality_aggregate_cache",
        ["municipality_code"],
        unique=False,
    )
    op.create_index(
        "ix_catastro_municipality_aggregate_cache_reference_year",
        "catastro_municipality_aggregate_cache",
        ["reference_year"],
        unique=False,
    )
    op.create_index(
        "ix_catastro_municipality_aggregate_cache_expires_at",
        "catastro_municipality_aggregate_cache",
        ["expires_at"],
        unique=False,
    )
    op.create_index(
        "ix_catastro_municipality_aggregate_cache_scope_expires",
        "catastro_municipality_aggregate_cache",
        ["provider_family", "municipality_code", "reference_year", "expires_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_catastro_municipality_aggregate_cache_scope_expires",
        table_name="catastro_municipality_aggregate_cache",
    )
    op.drop_index(
        "ix_catastro_municipality_aggregate_cache_expires_at",
        table_name="catastro_municipality_aggregate_cache",
    )
    op.drop_index(
        "ix_catastro_municipality_aggregate_cache_reference_year",
        table_name="catastro_municipality_aggregate_cache",
    )
    op.drop_index(
        "ix_catastro_municipality_aggregate_cache_municipality_code",
        table_name="catastro_municipality_aggregate_cache",
    )
    op.drop_index(
        "ix_catastro_municipality_aggregate_cache_provider_family",
        table_name="catastro_municipality_aggregate_cache",
    )
    op.drop_table("catastro_municipality_aggregate_cache")
