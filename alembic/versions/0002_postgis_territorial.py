"""Enable PostGIS and territorial base schema.

Revision ID: 0002_postgis_territorial
Revises: 0001_initial_schema
Create Date: 2026-03-12 00:10:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from geoalchemy2 import Geometry
from sqlalchemy.dialects import postgresql


revision = "0002_postgis_territorial"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")

    op.create_table(
        "territorial_units",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("parent_id", sa.Integer(), nullable=True),
        sa.Column("unit_level", sa.String(length=32), nullable=False),
        sa.Column("canonical_name", sa.String(length=255), nullable=False),
        sa.Column("normalized_name", sa.String(length=255), nullable=False),
        sa.Column("display_name", sa.String(length=255), server_default="", nullable=False),
        sa.Column("country_code", sa.String(length=2), server_default="ES", nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("geometry", Geometry(geometry_type="MULTIPOLYGON", srid=4326, spatial_index=False), nullable=True),
        sa.Column("centroid", Geometry(geometry_type="POINT", srid=4326, spatial_index=False), nullable=True),
        sa.Column("attributes", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["parent_id"], ["territorial_units.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("unit_level", "normalized_name", "parent_id", name="uq_territorial_units_level_parent_name"),
    )
    op.create_index("ix_territorial_units_parent_id", "territorial_units", ["parent_id"], unique=False)
    op.create_index("ix_territorial_units_unit_level", "territorial_units", ["unit_level"], unique=False)
    op.create_index("ix_territorial_units_normalized_name", "territorial_units", ["normalized_name"], unique=False)
    op.create_index("ix_territorial_units_level_parent", "territorial_units", ["unit_level", "parent_id"], unique=False)

    op.create_table(
        "territorial_unit_codes",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("territorial_unit_id", sa.Integer(), nullable=False),
        sa.Column("source_system", sa.String(length=32), nullable=False),
        sa.Column("code_type", sa.String(length=32), server_default="default", nullable=False),
        sa.Column("code_value", sa.String(length=128), nullable=False),
        sa.Column("is_primary", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["territorial_unit_id"], ["territorial_units.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_system", "code_type", "code_value", name="uq_territorial_unit_codes_source_value"),
    )
    op.create_index("ix_territorial_unit_codes_territorial_unit_id", "territorial_unit_codes", ["territorial_unit_id"], unique=False)
    op.create_index("ix_territorial_unit_codes_source_system", "territorial_unit_codes", ["source_system"], unique=False)
    op.create_index("ix_territorial_unit_codes_code_value", "territorial_unit_codes", ["code_value"], unique=False)
    op.create_index("ix_territorial_unit_codes_unit_source", "territorial_unit_codes", ["territorial_unit_id", "source_system"], unique=False)

    op.create_table(
        "territorial_unit_aliases",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("territorial_unit_id", sa.Integer(), nullable=False),
        sa.Column("source_system", sa.String(length=32), server_default="", nullable=False),
        sa.Column("alias", sa.String(length=255), nullable=False),
        sa.Column("normalized_alias", sa.String(length=255), nullable=False),
        sa.Column("alias_type", sa.String(length=32), server_default="name", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["territorial_unit_id"], ["territorial_units.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "territorial_unit_id",
            "source_system",
            "normalized_alias",
            "alias_type",
            name="uq_territorial_unit_aliases_unique",
        ),
    )
    op.create_index("ix_territorial_unit_aliases_territorial_unit_id", "territorial_unit_aliases", ["territorial_unit_id"], unique=False)
    op.create_index("ix_territorial_unit_aliases_normalized_alias", "territorial_unit_aliases", ["normalized_alias"], unique=False)
    op.create_index("ix_territorial_unit_aliases_lookup", "territorial_unit_aliases", ["normalized_alias", "source_system"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_territorial_unit_aliases_lookup", table_name="territorial_unit_aliases")
    op.drop_index("ix_territorial_unit_aliases_normalized_alias", table_name="territorial_unit_aliases")
    op.drop_index("ix_territorial_unit_aliases_territorial_unit_id", table_name="territorial_unit_aliases")
    op.drop_table("territorial_unit_aliases")

    op.drop_index("ix_territorial_unit_codes_unit_source", table_name="territorial_unit_codes")
    op.drop_index("ix_territorial_unit_codes_code_value", table_name="territorial_unit_codes")
    op.drop_index("ix_territorial_unit_codes_source_system", table_name="territorial_unit_codes")
    op.drop_index("ix_territorial_unit_codes_territorial_unit_id", table_name="territorial_unit_codes")
    op.drop_table("territorial_unit_codes")

    op.drop_index("ix_territorial_units_level_parent", table_name="territorial_units")
    op.drop_index("ix_territorial_units_normalized_name", table_name="territorial_units")
    op.drop_index("ix_territorial_units_unit_level", table_name="territorial_units")
    op.drop_index("ix_territorial_units_parent_id", table_name="territorial_units")
    op.drop_table("territorial_units")
