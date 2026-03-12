from geoalchemy2 import Geometry

from app.models import (
    POSTGIS_DEFAULT_SRID,
    TERRITORIAL_BOUNDARY_GEOMETRY_TYPE,
    TERRITORIAL_CENTROID_GEOMETRY_TYPE,
    TerritorialUnit,
)


def test_postgis_conventions_are_explicit_in_model():
    geometry_column = TerritorialUnit.__table__.c.geometry
    centroid_column = TerritorialUnit.__table__.c.centroid

    assert POSTGIS_DEFAULT_SRID == 4326
    assert TERRITORIAL_BOUNDARY_GEOMETRY_TYPE == "MULTIPOLYGON"
    assert TERRITORIAL_CENTROID_GEOMETRY_TYPE == "POINT"

    assert isinstance(geometry_column.type, Geometry)
    assert geometry_column.type.geometry_type == TERRITORIAL_BOUNDARY_GEOMETRY_TYPE
    assert geometry_column.type.srid == POSTGIS_DEFAULT_SRID
    assert geometry_column.nullable is True

    assert isinstance(centroid_column.type, Geometry)
    assert centroid_column.type.geometry_type == TERRITORIAL_CENTROID_GEOMETRY_TYPE
    assert centroid_column.type.srid == POSTGIS_DEFAULT_SRID
    assert centroid_column.nullable is True


def test_postgis_indexes_are_declared_for_geometry_and_centroid():
    index_names = {index.name for index in TerritorialUnit.__table__.indexes}

    assert "ix_territorial_units_geometry_gist" in index_names
    assert "ix_territorial_units_centroid_gist" in index_names
