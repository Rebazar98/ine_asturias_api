"""add persistent geocoding cache tables

Revision ID: 0005_geocoding_cache
Revises: 0004_postgis_conventions
Create Date: 2026-03-12 19:05:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "0005_geocoding_cache"
down_revision: str | None = "0004_postgis_conventions"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "geocode_cache",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("query_text", sa.String(length=512), nullable=False),
        sa.Column("normalized_query", sa.String(length=512), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("cached_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("provider", "normalized_query", name="uq_geocode_cache_provider_query"),
    )
    op.create_index("ix_geocode_cache_provider", "geocode_cache", ["provider"], unique=False)
    op.create_index(
        "ix_geocode_cache_normalized_query",
        "geocode_cache",
        ["normalized_query"],
        unique=False,
    )
    op.create_index("ix_geocode_cache_expires_at", "geocode_cache", ["expires_at"], unique=False)
    op.create_index(
        "ix_geocode_cache_provider_expires",
        "geocode_cache",
        ["provider", "expires_at"],
        unique=False,
    )

    op.create_table(
        "reverse_geocode_cache",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("latitude", sa.Float(), nullable=False),
        sa.Column("longitude", sa.Float(), nullable=False),
        sa.Column("coordinate_key", sa.String(length=128), nullable=False),
        sa.Column("precision_digits", sa.Integer(), server_default="6", nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("cached_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "provider",
            "coordinate_key",
            name="uq_reverse_geocode_cache_provider_key",
        ),
    )
    op.create_index(
        "ix_reverse_geocode_cache_provider",
        "reverse_geocode_cache",
        ["provider"],
        unique=False,
    )
    op.create_index(
        "ix_reverse_geocode_cache_coordinate_key",
        "reverse_geocode_cache",
        ["coordinate_key"],
        unique=False,
    )
    op.create_index(
        "ix_reverse_geocode_cache_expires_at",
        "reverse_geocode_cache",
        ["expires_at"],
        unique=False,
    )
    op.create_index(
        "ix_reverse_geocode_cache_provider_expires",
        "reverse_geocode_cache",
        ["provider", "expires_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_reverse_geocode_cache_provider_expires", table_name="reverse_geocode_cache")
    op.drop_index("ix_reverse_geocode_cache_expires_at", table_name="reverse_geocode_cache")
    op.drop_index("ix_reverse_geocode_cache_coordinate_key", table_name="reverse_geocode_cache")
    op.drop_index("ix_reverse_geocode_cache_provider", table_name="reverse_geocode_cache")
    op.drop_table("reverse_geocode_cache")

    op.drop_index("ix_geocode_cache_provider_expires", table_name="geocode_cache")
    op.drop_index("ix_geocode_cache_expires_at", table_name="geocode_cache")
    op.drop_index("ix_geocode_cache_normalized_query", table_name="geocode_cache")
    op.drop_index("ix_geocode_cache_provider", table_name="geocode_cache")
    op.drop_table("geocode_cache")
