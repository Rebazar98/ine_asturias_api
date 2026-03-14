"""add analytical snapshots table

Revision ID: 0006_analytical_snapshots
Revises: 0005_geocoding_cache
Create Date: 2026-03-14 15:30:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "0006_analytical_snapshots"
down_revision: str | None = "0005_geocoding_cache"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "analytical_snapshots",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("snapshot_key", sa.String(length=128), nullable=False),
        sa.Column("snapshot_type", sa.String(length=64), nullable=False),
        sa.Column("scope_key", sa.String(length=255), nullable=False),
        sa.Column("source", sa.String(length=128), server_default="", nullable=False),
        sa.Column("territorial_unit_id", sa.Integer(), nullable=True),
        sa.Column("filters", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "generated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["territorial_unit_id"],
            ["territorial_units.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("snapshot_key", name="uq_analytical_snapshots_key"),
    )
    op.create_index(
        "ix_analytical_snapshots_snapshot_key",
        "analytical_snapshots",
        ["snapshot_key"],
        unique=False,
    )
    op.create_index(
        "ix_analytical_snapshots_snapshot_type",
        "analytical_snapshots",
        ["snapshot_type"],
        unique=False,
    )
    op.create_index(
        "ix_analytical_snapshots_scope_key",
        "analytical_snapshots",
        ["scope_key"],
        unique=False,
    )
    op.create_index(
        "ix_analytical_snapshots_territorial_unit_id",
        "analytical_snapshots",
        ["territorial_unit_id"],
        unique=False,
    )
    op.create_index(
        "ix_analytical_snapshots_generated_at",
        "analytical_snapshots",
        ["generated_at"],
        unique=False,
    )
    op.create_index(
        "ix_analytical_snapshots_expires_at",
        "analytical_snapshots",
        ["expires_at"],
        unique=False,
    )
    op.create_index(
        "ix_analytical_snapshots_type_scope_expires",
        "analytical_snapshots",
        ["snapshot_type", "scope_key", "expires_at"],
        unique=False,
    )
    op.create_index(
        "ix_analytical_snapshots_unit_type",
        "analytical_snapshots",
        ["territorial_unit_id", "snapshot_type"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_analytical_snapshots_unit_type", table_name="analytical_snapshots")
    op.drop_index(
        "ix_analytical_snapshots_type_scope_expires",
        table_name="analytical_snapshots",
    )
    op.drop_index("ix_analytical_snapshots_expires_at", table_name="analytical_snapshots")
    op.drop_index("ix_analytical_snapshots_generated_at", table_name="analytical_snapshots")
    op.drop_index(
        "ix_analytical_snapshots_territorial_unit_id",
        table_name="analytical_snapshots",
    )
    op.drop_index("ix_analytical_snapshots_scope_key", table_name="analytical_snapshots")
    op.drop_index(
        "ix_analytical_snapshots_snapshot_type",
        table_name="analytical_snapshots",
    )
    op.drop_index(
        "ix_analytical_snapshots_snapshot_key",
        table_name="analytical_snapshots",
    )
    op.drop_table("analytical_snapshots")
