"""create cartographic_qa_incidents table

Revision ID: 0012_cartographic_qa_incidents
Revises: 0011_ideas_features
Create Date: 2026-03-17 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "0012_cartographic_qa_incidents"
down_revision: str | None = "0011_ideas_features"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "cartographic_qa_incidents",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("layer", sa.String(length=128), nullable=False),
        sa.Column("entity_id", sa.String(length=255), nullable=False),
        sa.Column("error_type", sa.String(length=64), nullable=False),
        sa.Column(
            "severity",
            sa.String(length=16),
            nullable=False,
            server_default="warning",
        ),
        sa.Column("description", sa.Text(), nullable=False, server_default=""),
        sa.Column("source_provider", sa.String(length=32), nullable=False, server_default=""),
        sa.Column(
            "detected_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "resolved",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_cartographic_qa_incidents_layer",
        "cartographic_qa_incidents",
        ["layer"],
        unique=False,
    )
    op.create_index(
        "ix_cartographic_qa_incidents_entity_id",
        "cartographic_qa_incidents",
        ["entity_id"],
        unique=False,
    )
    op.create_index(
        "ix_cartographic_qa_incidents_error_type",
        "cartographic_qa_incidents",
        ["error_type"],
        unique=False,
    )
    op.create_index(
        "ix_cartographic_qa_incidents_detected_at",
        "cartographic_qa_incidents",
        ["detected_at"],
        unique=False,
    )
    op.create_index(
        "ix_cartographic_qa_incidents_resolved",
        "cartographic_qa_incidents",
        ["resolved"],
        unique=False,
    )
    op.create_index(
        "ix_qa_incidents_layer_resolved",
        "cartographic_qa_incidents",
        ["layer", "resolved", "detected_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_qa_incidents_layer_resolved", table_name="cartographic_qa_incidents")
    op.drop_index("ix_cartographic_qa_incidents_resolved", table_name="cartographic_qa_incidents")
    op.drop_index(
        "ix_cartographic_qa_incidents_detected_at", table_name="cartographic_qa_incidents"
    )
    op.drop_index("ix_cartographic_qa_incidents_error_type", table_name="cartographic_qa_incidents")
    op.drop_index("ix_cartographic_qa_incidents_entity_id", table_name="cartographic_qa_incidents")
    op.drop_index("ix_cartographic_qa_incidents_layer", table_name="cartographic_qa_incidents")
    op.drop_table("cartographic_qa_incidents")
