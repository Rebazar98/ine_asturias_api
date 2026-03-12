from app.repositories.geocoding import (
    DEFAULT_REVERSE_GEOCODE_PRECISION,
    build_reverse_geocode_coordinate_key,
    normalize_geocode_query,
    normalize_reverse_geocode_coordinates,
)


def test_normalize_geocode_query_collapses_whitespace_and_casefolds() -> None:
    assert normalize_geocode_query("  Calle   Uria  10  ") == "calle uria 10"


def test_normalize_reverse_geocode_coordinates_rounds_to_default_precision() -> None:
    assert normalize_reverse_geocode_coordinates(43.3614004, -5.8493996) == (
        43.3614,
        -5.8494,
    )


def test_build_reverse_geocode_coordinate_key_is_stable_for_equivalent_coordinates() -> None:
    first = build_reverse_geocode_coordinate_key(43.3614, -5.8494)
    second = build_reverse_geocode_coordinate_key(43.3614004, -5.8493996)

    assert (
        first
        == second
        == f"{43.3614:.{DEFAULT_REVERSE_GEOCODE_PRECISION}f},{-5.8494:.{DEFAULT_REVERSE_GEOCODE_PRECISION}f}"
    )
