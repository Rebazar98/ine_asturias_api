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
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


POSTGIS_DEFAULT_SRID = 4326
TERRITORIAL_BOUNDARY_GEOMETRY_TYPE = "MULTIPOLYGON"
TERRITORIAL_CENTROID_GEOMETRY_TYPE = "POINT"


class Base(DeclarativeBase):
    pass


class IngestionRaw(Base):
    __tablename__ = "ingestion_raw"
    __table_args__ = (Index("ix_ingestion_raw_lookup", "source_type", "source_key", "fetched_at"),)

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
    org_id: Mapped[str | None] = mapped_column(
        String(64), nullable=True, server_default="geonalon", index=True
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
    territorial_unit_id: Mapped[int | None] = mapped_column(
        ForeignKey("territorial_units.id"),
        nullable=True,
        index=True,
    )
    geography_name: Mapped[str] = mapped_column(String(255), default="", server_default="")
    geography_code: Mapped[str] = mapped_column(String(128), default="", server_default="")
    period: Mapped[str] = mapped_column(String(128), default="", server_default="", index=True)
    value: Mapped[float | None] = mapped_column(Float, nullable=True)
    unit: Mapped[str] = mapped_column(String(128), default="", server_default="")
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, default=dict)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    source_provider: Mapped[str] = mapped_column(
        String(32), default="ine", server_default="ine", index=True
    )
    inserted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        index=True,
    )
    org_id: Mapped[str | None] = mapped_column(
        String(64), nullable=True, server_default="geonalon", index=True
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
    validation_status: Mapped[str] = mapped_column(
        String(32), default="unknown", server_default="unknown"
    )
    normalized_rows: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    raw_rows_retrieved: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    filtered_rows_retrieved: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    series_kept: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    series_discarded: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, default=dict)
    notes: Mapped[str] = mapped_column(Text, default="", server_default="")
    last_warning: Mapped[str] = mapped_column(Text, default="", server_default="")
    source_provider: Mapped[str] = mapped_column(
        String(32), default="ine", server_default="ine", index=True
    )


class GeocodeCache(Base):
    __tablename__ = "geocode_cache"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "normalized_query",
            name="uq_geocode_cache_provider_query",
        ),
        Index("ix_geocode_cache_provider_expires", "provider", "expires_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    provider: Mapped[str] = mapped_column(String(32), index=True)
    query_text: Mapped[str] = mapped_column(String(512))
    normalized_query: Mapped[str] = mapped_column(String(512), index=True)
    payload: Mapped[dict[str, Any] | list[Any]] = mapped_column(JSONB)
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, default=dict)
    cached_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class ReverseGeocodeCache(Base):
    __tablename__ = "reverse_geocode_cache"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "coordinate_key",
            name="uq_reverse_geocode_cache_provider_key",
        ),
        Index("ix_reverse_geocode_cache_provider_expires", "provider", "expires_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    provider: Mapped[str] = mapped_column(String(32), index=True)
    latitude: Mapped[float] = mapped_column(Float)
    longitude: Mapped[float] = mapped_column(Float)
    coordinate_key: Mapped[str] = mapped_column(String(128), index=True)
    precision_digits: Mapped[int] = mapped_column(Integer, default=6, server_default="6")
    payload: Mapped[dict[str, Any] | list[Any]] = mapped_column(JSONB)
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, default=dict)
    cached_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class AnalyticalSnapshot(Base):
    __tablename__ = "analytical_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "snapshot_key",
            name="uq_analytical_snapshots_key",
        ),
        Index(
            "ix_analytical_snapshots_type_scope_expires",
            "snapshot_type",
            "scope_key",
            "expires_at",
        ),
        Index(
            "ix_analytical_snapshots_unit_type",
            "territorial_unit_id",
            "snapshot_type",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    snapshot_key: Mapped[str] = mapped_column(String(128), index=True)
    snapshot_type: Mapped[str] = mapped_column(String(64), index=True)
    scope_key: Mapped[str] = mapped_column(String(255), index=True)
    source: Mapped[str] = mapped_column(String(128), default="", server_default="")
    territorial_unit_id: Mapped[int | None] = mapped_column(
        ForeignKey("territorial_units.id"),
        nullable=True,
        index=True,
    )
    filters_json: Mapped[dict[str, Any]] = mapped_column("filters", JSONB, default=dict)
    payload: Mapped[dict[str, Any] | list[Any]] = mapped_column(JSONB)
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, default=dict)
    generated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class TerritorialExportArtifact(Base):
    __tablename__ = "territorial_export_artifacts"
    __table_args__ = (
        UniqueConstraint(
            "export_key",
            name="uq_territorial_export_artifacts_key",
        ),
        Index(
            "ix_territorial_export_artifacts_unit_expires",
            "unit_level",
            "code_value",
            "expires_at",
        ),
        Index(
            "ix_territorial_export_artifacts_unit_format",
            "territorial_unit_id",
            "artifact_format",
        ),
    )

    export_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    export_key: Mapped[str] = mapped_column(String(128), index=True)
    territorial_unit_id: Mapped[int | None] = mapped_column(
        ForeignKey("territorial_units.id"),
        nullable=True,
        index=True,
    )
    unit_level: Mapped[str] = mapped_column(String(32), index=True)
    code_value: Mapped[str] = mapped_column(String(128), index=True)
    artifact_format: Mapped[str] = mapped_column(String(32), default="zip", server_default="zip")
    content_type: Mapped[str] = mapped_column(String(128), default="", server_default="")
    filename: Mapped[str] = mapped_column(String(255), default="", server_default="")
    payload_bytes: Mapped[bytes] = mapped_column(LargeBinary)
    payload_sha256: Mapped[str] = mapped_column(String(64), index=True)
    byte_size: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class CatastroMunicipalityAggregateCache(Base):
    __tablename__ = "catastro_municipality_aggregate_cache"
    __table_args__ = (
        UniqueConstraint(
            "provider_family",
            "municipality_code",
            "reference_year",
            name="uq_catastro_municipality_aggregate_cache_scope",
        ),
        Index(
            "ix_catastro_municipality_aggregate_cache_scope_expires",
            "provider_family",
            "municipality_code",
            "reference_year",
            "expires_at",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    provider_family: Mapped[str] = mapped_column(String(64), index=True)
    municipality_code: Mapped[str] = mapped_column(String(128), index=True)
    reference_year: Mapped[str] = mapped_column(String(8), index=True)
    payload: Mapped[dict[str, Any] | list[Any]] = mapped_column(JSONB)
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, default=dict)
    cached_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class CatastroTerritorialAggregateCache(Base):
    __tablename__ = "catastro_territorial_aggregate_cache"
    __table_args__ = (
        UniqueConstraint(
            "provider_family",
            "unit_level",
            "code_value",
            "reference_year",
            name="uq_catastro_territorial_aggregate_cache_scope",
        ),
        Index(
            "ix_catastro_territorial_aggregate_cache_scope_expires",
            "provider_family",
            "unit_level",
            "code_value",
            "reference_year",
            "expires_at",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    provider_family: Mapped[str] = mapped_column(String(64), index=True)
    unit_level: Mapped[str] = mapped_column(String(32), index=True)
    code_value: Mapped[str] = mapped_column(String(128), index=True)
    reference_year: Mapped[str] = mapped_column(String(8), index=True)
    payload: Mapped[dict[str, Any] | list[Any]] = mapped_column(JSONB)
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, default=dict)
    cached_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


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
        Index("ix_territorial_units_geometry_gist", "geometry", postgresql_using="gist"),
        Index("ix_territorial_units_centroid_gist", "centroid", postgresql_using="gist"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    parent_id: Mapped[int | None] = mapped_column(
        ForeignKey("territorial_units.id"), nullable=True, index=True
    )
    unit_level: Mapped[str] = mapped_column(String(32), index=True)
    canonical_name: Mapped[str] = mapped_column(String(255))
    normalized_name: Mapped[str] = mapped_column(String(255), index=True)
    display_name: Mapped[str] = mapped_column(String(255), default="", server_default="")
    country_code: Mapped[str] = mapped_column(String(2), default="ES", server_default="ES")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    geometry: Mapped[Any | None] = mapped_column(
        Geometry(
            geometry_type=TERRITORIAL_BOUNDARY_GEOMETRY_TYPE,
            srid=POSTGIS_DEFAULT_SRID,
            spatial_index=False,
        ),
        nullable=True,
    )
    centroid: Mapped[Any | None] = mapped_column(
        Geometry(
            geometry_type=TERRITORIAL_CENTROID_GEOMETRY_TYPE,
            srid=POSTGIS_DEFAULT_SRID,
            spatial_index=False,
        ),
        nullable=True,
    )
    attributes_json: Mapped[dict[str, Any]] = mapped_column("attributes", JSONB, default=dict)
    org_id: Mapped[str | None] = mapped_column(
        String(64), nullable=True, server_default="geonalon", index=True
    )
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


class CartographicQAIncident(Base):
    __tablename__ = "cartographic_qa_incidents"
    __table_args__ = (Index("ix_qa_incidents_layer_resolved", "layer", "resolved", "detected_at"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    layer: Mapped[str] = mapped_column(String(128), index=True)
    entity_id: Mapped[str] = mapped_column(String(255), index=True)
    error_type: Mapped[str] = mapped_column(String(64), index=True)
    severity: Mapped[str] = mapped_column(String(16), default="warning", server_default="warning")
    description: Mapped[str] = mapped_column(Text, default="", server_default="")
    source_provider: Mapped[str] = mapped_column(String(32), default="", server_default="")
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    resolved: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="false", index=True
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, default=dict)


class IDEASFeature(Base):
    __tablename__ = "ideas_features_normalized"
    __table_args__ = (
        UniqueConstraint(
            "layer_name",
            "feature_id",
            name="uq_ideas_features_normalized_layer_feature",
        ),
        Index("ix_ideas_features_normalized_layer_expires", "layer_name", "expires_at"),
        Index("ix_ideas_features_normalized_geometry_gist", "geometry", postgresql_using="gist"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    layer_name: Mapped[str] = mapped_column(String(128), index=True)
    feature_id: Mapped[str] = mapped_column(String(255), index=True)
    source_provider: Mapped[str] = mapped_column(
        String(32), default="ideas", server_default="ideas", index=True
    )
    geometry: Mapped[Any | None] = mapped_column(
        Geometry(
            geometry_type="GEOMETRY",
            srid=POSTGIS_DEFAULT_SRID,
            spatial_index=False,
        ),
        nullable=True,
    )
    attributes_json: Mapped[dict[str, Any]] = mapped_column("attributes", JSONB, default=dict)
    org_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class SyncSchedule(Base):
    __tablename__ = "sync_schedule"
    __table_args__ = (
        UniqueConstraint("org_id", "source", name="uq_sync_schedule_org_source"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    org_id: Mapped[str] = mapped_column(String(64), index=True)
    source: Mapped[str] = mapped_column(String(64))
    cron_expression: Mapped[str] = mapped_column(String(128))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )
