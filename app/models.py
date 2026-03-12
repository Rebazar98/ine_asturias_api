from __future__ import annotations

from datetime import datetime
from typing import Any

from geoalchemy2 import Geometry
from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class IngestionRaw(Base):
    __tablename__ = "ingestion_raw"
    __table_args__ = (
        Index("ix_ingestion_raw_lookup", "source_type", "source_key", "fetched_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source_type: Mapped[str] = mapped_column(String(64), index=True)
    source_key: Mapped[str] = mapped_column(String(255), index=True)
    request_path: Mapped[str] = mapped_column(Text)
    request_params: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    payload: Mapped[dict[str, Any] | list[Any]] = mapped_column(JSONB)
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        index=True,
    )


class INESeriesNormalized(Base):
    __tablename__ = "ine_series_normalized"
    __table_args__ = (
        UniqueConstraint(
            "operation_code",
            "table_id",
            "variable_id",
            "geography_name",
            "geography_code",
            "period",
            name="uq_ine_series_normalized_logical",
        ),
        Index(
            "ix_ine_series_normalized_lookup",
            "operation_code",
            "table_id",
            "geography_code",
            "period",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    operation_code: Mapped[str] = mapped_column(String(64), default="", server_default="")
    table_id: Mapped[str] = mapped_column(String(64), default="", server_default="")
    variable_id: Mapped[str] = mapped_column(String(128), default="", server_default="")
    geography_name: Mapped[str] = mapped_column(String(255), default="", server_default="")
    geography_code: Mapped[str] = mapped_column(String(128), default="", server_default="")
    period: Mapped[str] = mapped_column(String(128), default="", server_default="", index=True)
    value: Mapped[float | None] = mapped_column(Float, nullable=True)
    unit: Mapped[str] = mapped_column(String(128), default="", server_default="")
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, default=dict)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    inserted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        index=True,
    )


class INETableCatalog(Base):
    __tablename__ = "ine_tables_catalog"
    __table_args__ = (
        UniqueConstraint(
            "operation_code",
            "table_id",
            name="uq_ine_tables_catalog_operation_table",
        ),
        Index(
            "ix_ine_tables_catalog_operation_status",
            "operation_code",
            "validation_status",
            "has_asturias_data",
        ),
        Index(
            "ix_ine_tables_catalog_operation_checked",
            "operation_code",
            "last_checked_at",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    operation_code: Mapped[str] = mapped_column(String(64), index=True)
    table_id: Mapped[str] = mapped_column(String(64), index=True)
    table_name: Mapped[str] = mapped_column(String(255), default="", server_default="")
    request_path: Mapped[str] = mapped_column(Text, default="", server_default="")
    resolution_context: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    has_asturias_data: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    validation_status: Mapped[str] = mapped_column(String(32), default="unknown", server_default="unknown")
    normalized_rows: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    raw_rows_retrieved: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    filtered_rows_retrieved: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    series_kept: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    series_discarded: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, default=dict)
    notes: Mapped[str] = mapped_column(Text, default="", server_default="")
    last_warning: Mapped[str] = mapped_column(Text, default="", server_default="")


class TerritorialUnit(Base):
    __tablename__ = "territorial_units"
    __table_args__ = (
        UniqueConstraint(
            "unit_level",
            "normalized_name",
            "parent_id",
            name="uq_territorial_units_level_parent_name",
        ),
        Index("ix_territorial_units_level_parent", "unit_level", "parent_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    parent_id: Mapped[int | None] = mapped_column(ForeignKey("territorial_units.id"), nullable=True, index=True)
    unit_level: Mapped[str] = mapped_column(String(32), index=True)
    canonical_name: Mapped[str] = mapped_column(String(255))
    normalized_name: Mapped[str] = mapped_column(String(255), index=True)
    display_name: Mapped[str] = mapped_column(String(255), default="", server_default="")
    country_code: Mapped[str] = mapped_column(String(2), default="ES", server_default="ES")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    geometry: Mapped[Any | None] = mapped_column(
        Geometry(geometry_type="MULTIPOLYGON", srid=4326, spatial_index=False),
        nullable=True,
    )
    centroid: Mapped[Any | None] = mapped_column(
        Geometry(geometry_type="POINT", srid=4326, spatial_index=False),
        nullable=True,
    )
    attributes_json: Mapped[dict[str, Any]] = mapped_column("attributes", JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class TerritorialUnitCode(Base):
    __tablename__ = "territorial_unit_codes"
    __table_args__ = (
        UniqueConstraint(
            "source_system",
            "code_type",
            "code_value",
            name="uq_territorial_unit_codes_source_value",
        ),
        Index("ix_territorial_unit_codes_unit_source", "territorial_unit_id", "source_system"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    territorial_unit_id: Mapped[int] = mapped_column(ForeignKey("territorial_units.id"), index=True)
    source_system: Mapped[str] = mapped_column(String(32), index=True)
    code_type: Mapped[str] = mapped_column(String(32), default="default", server_default="default")
    code_value: Mapped[str] = mapped_column(String(128), index=True)
    is_primary: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class TerritorialUnitAlias(Base):
    __tablename__ = "territorial_unit_aliases"
    __table_args__ = (
        UniqueConstraint(
            "territorial_unit_id",
            "source_system",
            "normalized_alias",
            "alias_type",
            name="uq_territorial_unit_aliases_unique",
        ),
        Index("ix_territorial_unit_aliases_lookup", "normalized_alias", "source_system"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    territorial_unit_id: Mapped[int] = mapped_column(ForeignKey("territorial_units.id"), index=True)
    source_system: Mapped[str] = mapped_column(String(32), default="", server_default="")
    alias: Mapped[str] = mapped_column(String(255))
    normalized_alias: Mapped[str] = mapped_column(String(255), index=True)
    alias_type: Mapped[str] = mapped_column(String(32), default="name", server_default="name")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
