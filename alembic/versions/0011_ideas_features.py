"""create ideas_features_normalized table

Revision ID: 0011_ideas_features
Revises: 0010_source_provider_column
Create Date: 2026-03-17 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from geoalchemy2 import Geometry
from sqlalchemy.dialects import postgresql


revision: str = "0011_ideas_features"
down_revision: str | None = "0010_source_provider_column"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "ideas_features_normalized",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("layer_name", sa.String(length=128), nullable=False),
        sa.Column("feature_id", sa.String(length=255), nullable=False),
        sa.Column(
            "source_provider",
            sa.String(length=32),
            nullable=False,
            server_default="ideas",
        ),
        sa.Column(
            "geometry",
            Geometry(geometry_type="GEOMETRY", srid=4326, spatial_index=False),
            nullable=True,
        ),
        sa.Column("attributes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("org_id", sa.String(length=64), nullable=True),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "layer_name",
            "feature_id",
            name="uq_ideas_features_normalized_layer_feature",
        ),
    )
    op.create_index(
        "ix_ideas_features_normalized_layer_name",
        "ideas_features_normalized",
        ["layer_name"],
        unique=False,
    )
    op.create_index(
        "ix_ideas_features_normalized_feature_id",
        "ideas_features_normalized",
        ["feature_id"],
        unique=False,
    )
    op.create_index(
        "ix_ideas_features_normalized_source_provider",
        "ideas_features_normalized",
        ["source_provider"],
        unique=False,
    )
    op.create_index(
        "ix_ideas_features_normalized_org_id",
        "ideas_features_normalized",
        ["org_id"],
        unique=False,
    )
    op.create_index(
        "ix_ideas_features_normalized_ingested_at",
        "ideas_features_normalized",
        ["ingested_at"],
        unique=False,
    )
    op.create_index(
        "ix_ideas_features_normalized_expires_at",
        "ideas_features_normalized",
        ["expires_at"],
        unique=False,
    )
    op.create_index(
        "ix_ideas_features_normalized_layer_expires",
        "ideas_features_normalized",
        ["layer_name", "expires_at"],
        unique=False,
    )
    op.create_index(
        "idx_ideas_features_normalized_geometry_gist",
        "ideas_features_normalized",
        ["geometry"],
        unique=False,
        postgresql_using="gist",
    )


def downgrade() -> None:
    op.drop_index(
        "idx_ideas_features_normalized_geometry_gist",
        table_name="ideas_features_normalized",
        postgresql_using="gist",
    )
    op.drop_index(
        "ix_ideas_features_normalized_layer_expires",
        table_name="ideas_features_normalized",
    )
    op.drop_index(
        "ix_ideas_features_normalized_expires_at",
        table_name="ideas_features_normalized",
    )
    op.drop_index(
        "ix_ideas_features_normalized_ingested_at",
        table_name="ideas_features_normalized",
    )
    op.drop_index(
        "ix_ideas_features_normalized_org_id",
        table_name="ideas_features_normalized",
    )
    op.drop_index(
        "ix_ideas_features_normalized_source_provider",
        table_name="ideas_features_normalized",
    )
    op.drop_index(
        "ix_ideas_features_normalized_feature_id",
        table_name="ideas_features_normalized",
    )
    op.drop_index(
        "ix_ideas_features_normalized_layer_name",
        table_name="ideas_features_normalized",
    )
    op.drop_table("ideas_features_normalized")
