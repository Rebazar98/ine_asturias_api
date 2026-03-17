"""add catastro territorial aggregate cache

Revision ID: 0009_catastro_territorial_cache
Revises: 0008_catastro_cache
Create Date: 2026-03-16 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "0009_catastro_territorial_cache"
down_revision: str | None = "0008_catastro_cache"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "catastro_territorial_aggregate_cache",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("provider_family", sa.String(length=64), nullable=False),
        sa.Column("unit_level", sa.String(length=32), nullable=False),
        sa.Column("code_value", sa.String(length=128), nullable=False),
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
            "unit_level",
            "code_value",
            "reference_year",
            name="uq_catastro_territorial_aggregate_cache_scope",
        ),
    )
    op.create_index(
        "ix_catastro_territorial_aggregate_cache_provider_family",
        "catastro_territorial_aggregate_cache",
        ["provider_family"],
        unique=False,
    )
    op.create_index(
        "ix_catastro_territorial_aggregate_cache_unit_level",
        "catastro_territorial_aggregate_cache",
        ["unit_level"],
        unique=False,
    )
    op.create_index(
        "ix_catastro_territorial_aggregate_cache_code_value",
        "catastro_territorial_aggregate_cache",
        ["code_value"],
        unique=False,
    )
    op.create_index(
        "ix_catastro_territorial_aggregate_cache_reference_year",
        "catastro_territorial_aggregate_cache",
        ["reference_year"],
        unique=False,
    )
    op.create_index(
        "ix_catastro_territorial_aggregate_cache_expires_at",
        "catastro_territorial_aggregate_cache",
        ["expires_at"],
        unique=False,
    )
    op.create_index(
        "ix_catastro_territorial_aggregate_cache_scope_expires",
        "catastro_territorial_aggregate_cache",
        ["provider_family", "unit_level", "code_value", "reference_year", "expires_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_catastro_territorial_aggregate_cache_scope_expires",
        table_name="catastro_territorial_aggregate_cache",
    )
    op.drop_index(
        "ix_catastro_territorial_aggregate_cache_expires_at",
        table_name="catastro_territorial_aggregate_cache",
    )
    op.drop_index(
        "ix_catastro_territorial_aggregate_cache_reference_year",
        table_name="catastro_territorial_aggregate_cache",
    )
    op.drop_index(
        "ix_catastro_territorial_aggregate_cache_code_value",
        table_name="catastro_territorial_aggregate_cache",
    )
    op.drop_index(
        "ix_catastro_territorial_aggregate_cache_unit_level",
        table_name="catastro_territorial_aggregate_cache",
    )
    op.drop_index(
        "ix_catastro_territorial_aggregate_cache_provider_family",
        table_name="catastro_territorial_aggregate_cache",
    )
    op.drop_table("catastro_territorial_aggregate_cache")
