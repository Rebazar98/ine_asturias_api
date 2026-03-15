"""add territorial export artifacts table

Revision ID: 0007_territorial_export_artifacts
Revises: 0006_analytical_snapshots
Create Date: 2026-03-15 14:15:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "0007_territorial_export_artifacts"
down_revision: str | None = "0006_analytical_snapshots"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "territorial_export_artifacts",
        sa.Column("export_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("export_key", sa.String(length=128), nullable=False),
        sa.Column("territorial_unit_id", sa.Integer(), nullable=True),
        sa.Column("unit_level", sa.String(length=32), nullable=False),
        sa.Column("code_value", sa.String(length=128), nullable=False),
        sa.Column("artifact_format", sa.String(length=32), server_default="zip", nullable=False),
        sa.Column("content_type", sa.String(length=128), server_default="", nullable=False),
        sa.Column("filename", sa.String(length=255), server_default="", nullable=False),
        sa.Column("payload_bytes", sa.LargeBinary(), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("byte_size", sa.Integer(), server_default="0", nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
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
        sa.PrimaryKeyConstraint("export_id"),
        sa.UniqueConstraint("export_key", name="uq_territorial_export_artifacts_key"),
    )
    op.create_index(
        "ix_territorial_export_artifacts_export_key",
        "territorial_export_artifacts",
        ["export_key"],
        unique=False,
    )
    op.create_index(
        "ix_territorial_export_artifacts_territorial_unit_id",
        "territorial_export_artifacts",
        ["territorial_unit_id"],
        unique=False,
    )
    op.create_index(
        "ix_territorial_export_artifacts_unit_level",
        "territorial_export_artifacts",
        ["unit_level"],
        unique=False,
    )
    op.create_index(
        "ix_territorial_export_artifacts_code_value",
        "territorial_export_artifacts",
        ["code_value"],
        unique=False,
    )
    op.create_index(
        "ix_territorial_export_artifacts_payload_sha256",
        "territorial_export_artifacts",
        ["payload_sha256"],
        unique=False,
    )
    op.create_index(
        "ix_territorial_export_artifacts_expires_at",
        "territorial_export_artifacts",
        ["expires_at"],
        unique=False,
    )
    op.create_index(
        "ix_territorial_export_artifacts_unit_expires",
        "territorial_export_artifacts",
        ["unit_level", "code_value", "expires_at"],
        unique=False,
    )
    op.create_index(
        "ix_territorial_export_artifacts_unit_format",
        "territorial_export_artifacts",
        ["territorial_unit_id", "artifact_format"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_territorial_export_artifacts_unit_format",
        table_name="territorial_export_artifacts",
    )
    op.drop_index(
        "ix_territorial_export_artifacts_unit_expires",
        table_name="territorial_export_artifacts",
    )
    op.drop_index(
        "ix_territorial_export_artifacts_expires_at",
        table_name="territorial_export_artifacts",
    )
    op.drop_index(
        "ix_territorial_export_artifacts_payload_sha256",
        table_name="territorial_export_artifacts",
    )
    op.drop_index(
        "ix_territorial_export_artifacts_code_value",
        table_name="territorial_export_artifacts",
    )
    op.drop_index(
        "ix_territorial_export_artifacts_unit_level",
        table_name="territorial_export_artifacts",
    )
    op.drop_index(
        "ix_territorial_export_artifacts_territorial_unit_id",
        table_name="territorial_export_artifacts",
    )
    op.drop_index(
        "ix_territorial_export_artifacts_export_key",
        table_name="territorial_export_artifacts",
    )
    op.drop_table("territorial_export_artifacts")
