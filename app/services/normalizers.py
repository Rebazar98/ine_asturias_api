from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any

from app.schemas import NormalizedSeriesItem
from app.services.geography_aliases import canonicalize_configured_geography


_PERIOD_KEYS = ("Periodo", "NombrePeriodo", "period", "Period", "Date")
_VALUE_KEYS = ("Valor", "value", "Value", "Dato", "data")
_UNIT_KEYS = ("Unidad", "unit", "Unit", "FK_Unidad")
_NAME_KEYS = ("Nombre", "name", "Name", "Descripcion", "description", "COD", "Cod", "Codigo")
_ID_KEYS = ("Id", "id", "Codigo", "codigo", "Code", "code")
_VARIABLE_KEYS = ("IdVariable", "Variable", "variable", "COD", "Cod", "CodigoSerie", "codigoSerie")
_GEO_HINTS = ("geo", "geogr", "territ", "provincia", "municip", "autonom", "ccaa", "comunidad")
_SERIES_COLLECTION_KEYS = ("Resultados", "results", "Series", "series", "Data", "data")
_SERIES_HINT_KEYS = (
    "COD",
    "Cod",
    "Codigo",
    "Nombre",
    "name",
    "MetaData",
    "metadata",
    "FK_Unidad",
    "FK_Escala",
)
_OBSERVATION_HINT_KEYS = (
    "Valor",
    "value",
    "Value",
    "Dato",
    "data",
    "Fecha",
    "Date",
    "Anyo",
    "Ano",
    "FK_Periodo",
)


@dataclass(slots=True)
class NormalizationOutcome:
    items: list[NormalizedSeriesItem] = field(default_factory=list)
    discarded_counts: dict[str, int] = field(default_factory=dict)
    payload_type: str = "unknown"
    series_detected: int = 0
    observations_total: int = 0


def canonicalize_configured_geography_items(
    items: list[NormalizedSeriesItem],
    *,
    geography_name: str,
    geography_code: str,
    canonical_name: str,
    canonical_code: str,
) -> dict[str, Any]:
    observed_names: set[str] = set()
    observed_codes: set[str] = set()
    canonicalized_rows = 0

    for item in items:
        observed_names.add(item.geography_name)
        observed_codes.add(item.geography_code)
        canonical_name_value, canonical_code_value, changed = canonicalize_configured_geography(
            candidate_name=item.geography_name,
            candidate_code=item.geography_code,
            geography_name=geography_name,
            geography_code=geography_code,
            canonical_name=canonical_name,
            canonical_code=canonical_code,
        )
        if not changed:
            continue

        item.geography_name = canonical_name_value
        item.geography_code = canonical_code_value
        canonicalized_rows += 1

    return {
        "canonicalized_rows": canonicalized_rows,
        "observed_names": sorted(value for value in observed_names if value),
        "observed_codes": sorted(value for value in observed_codes if value),
        "canonical_name": canonical_name,
        "canonical_code": canonical_code,
    }


def normalize_table_payload(
    payload: dict[str, Any] | list[Any], table_id: str
) -> list[NormalizedSeriesItem]:
    return normalize_table_payload_with_stats(payload, table_id).items


def normalize_table_payload_with_stats(
    payload: dict[str, Any] | list[Any],
    table_id: str,
) -> NormalizationOutcome:
    return _normalize_payload(payload, table_id=table_id)


def normalize_asturias_payload(
    payload: dict[str, Any] | list[Any],
    op_code: str,
    geography_name: str,
    geography_code: str,
    table_id: str = "",
) -> list[NormalizedSeriesItem]:
    return normalize_asturias_payload_with_stats(
        payload,
        op_code,
        geography_name,
        geography_code,
        table_id,
    ).items


def normalize_asturias_payload_with_stats(
    payload: dict[str, Any] | list[Any],
    op_code: str,
    geography_name: str,
    geography_code: str,
    table_id: str = "",
) -> NormalizationOutcome:
    return _normalize_payload(
        payload,
        operation_code=op_code,
        table_id=table_id,
        geography_name_override=geography_name,
        geography_code_override=geography_code,
    )


def inspect_payload_shape(payload: dict[str, Any] | list[Any]) -> dict[str, Any]:
    series_items = _extract_series_items(payload)
    observations_total = 0

    for series in series_items:
        data_points = _extract_data_points(series)
        observations_total += len(data_points) if data_points else 1

    return {
        "payload_type": type(payload).__name__,
        "series_detected": len(series_items),
        "observations_total": observations_total,
    }


def parse_numeric_value(raw_value: Any) -> float | None:
    if raw_value is None:
        return None
    if isinstance(raw_value, bool):
        return float(raw_value)
    if isinstance(raw_value, (int, float)):
        return float(raw_value)

    value = str(raw_value).strip()
    if not value or value in {"-", "..", "...", "null", "None"}:
        return None

    value = value.replace("%", "")
    value = value.replace(" ", "")

    if "," in value and "." in value:
        if value.rfind(",") > value.rfind("."):
            value = value.replace(".", "").replace(",", ".")
        else:
            value = value.replace(",", "")
    elif "," in value:
        value = value.replace(",", ".")

    value = re.sub(r"[^0-9.-]", "", value)
    if value in {"", ".", "-", "-."}:
        return None

    try:
        return float(value)
    except ValueError:
        return None


def _normalize_payload(
    payload: dict[str, Any] | list[Any],
    operation_code: str = "",
    table_id: str = "",
    geography_name_override: str | None = None,
    geography_code_override: str | None = None,
) -> NormalizationOutcome:
    outcome = NormalizationOutcome()
    shape = inspect_payload_shape(payload)
    outcome.payload_type = shape["payload_type"]
    outcome.series_detected = shape["series_detected"]
    outcome.observations_total = shape["observations_total"]

    series_items = _extract_series_items(payload)

    for series in series_items:
        meta_data = _ensure_list(series.get("MetaData") or series.get("metadata"))
        variable_id = _extract_variable_id(series, meta_data)
        geography_name, geography_code = _extract_geography(meta_data, series)
        geography_name = geography_name_override or geography_name
        geography_code = geography_code_override or geography_code
        unit = _pick_string(series, _UNIT_KEYS)
        series_name = _extract_series_name(series)
        data_points = _extract_data_points(series)

        if data_points:
            for point in data_points:
                row, discard_reason = _build_row(
                    point=point,
                    series=series,
                    meta_data=meta_data,
                    operation_code=operation_code,
                    table_id=table_id,
                    variable_id=variable_id,
                    geography_name=geography_name,
                    geography_code=geography_code,
                    unit=unit or _pick_string(point, _UNIT_KEYS),
                    series_name=series_name,
                )
                if row is not None:
                    outcome.items.append(row)
                elif discard_reason is not None:
                    outcome.discarded_counts[discard_reason] = (
                        outcome.discarded_counts.get(discard_reason, 0) + 1
                    )
            continue

        row, discard_reason = _build_row(
            point=series,
            series=series,
            meta_data=meta_data,
            operation_code=operation_code,
            table_id=table_id,
            variable_id=variable_id,
            geography_name=geography_name,
            geography_code=geography_code,
            unit=unit,
            series_name=series_name,
        )
        if row is not None:
            outcome.items.append(row)
        elif discard_reason is not None:
            outcome.discarded_counts[discard_reason] = (
                outcome.discarded_counts.get(discard_reason, 0) + 1
            )

    return outcome


def _extract_series_items(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if not isinstance(payload, dict):
        return []

    if _looks_like_series_record(payload) or _looks_like_observation(payload):
        return [payload]

    for key in _SERIES_COLLECTION_KEYS:
        value = payload.get(key)
        if not isinstance(value, list):
            continue

        candidate_series = [item for item in value if isinstance(item, dict)]
        if not candidate_series:
            continue
        return candidate_series

    return [payload]


def _extract_data_points(series: dict[str, Any]) -> list[dict[str, Any]]:
    value = series.get("Data") or series.get("data")
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _build_row(
    point: dict[str, Any],
    series: dict[str, Any],
    meta_data: list[Any],
    operation_code: str,
    table_id: str,
    variable_id: str,
    geography_name: str,
    geography_code: str,
    unit: str,
    series_name: str,
) -> tuple[NormalizedSeriesItem | None, str | None]:
    period = _extract_period(point)
    value = _extract_value(point)

    if not period and value is None:
        return None, "missing_period_and_value"

    metadata = {
        "series_name": series_name,
        "series_code": _pick_string(series, ("COD", "Cod", "Codigo")),
        "meta_data": meta_data,
        "series_context": {
            key: value
            for key, value in series.items()
            if key not in {"Data", "data", "MetaData", "metadata"}
        },
        "point_context": {
            key: value
            for key, value in point.items()
            if key
            not in set(_VALUE_KEYS) | set(_PERIOD_KEYS) | {"Fecha", "Anyo", "Ano", "FK_Periodo"}
        },
    }

    raw_payload = {
        "series_name": series_name,
        "series_code": _pick_string(series, ("COD", "Cod", "Codigo")),
        "meta_data": meta_data,
        "series": {key: value for key, value in series.items() if key not in {"Data", "data"}},
        "point": point,
    }

    return (
        NormalizedSeriesItem(
            operation_code=operation_code,
            table_id=table_id,
            variable_id=variable_id,
            geography_name=geography_name,
            geography_code=geography_code,
            period=period or "unknown",
            value=value,
            unit=unit,
            metadata=metadata,
            raw_payload=raw_payload,
        ),
        None,
    )


def _extract_period(point: dict[str, Any]) -> str | None:
    for key in _PERIOD_KEYS:
        if key in point and point[key] not in (None, ""):
            return str(point[key])

    year_value = point.get("Anyo", point.get("Ano"))
    if year_value not in (None, ""):
        return str(year_value)

    date_value = point.get("Fecha")
    if date_value not in (None, ""):
        return str(date_value)

    fk_period_value = point.get("FK_Periodo")
    if fk_period_value not in (None, ""):
        return str(fk_period_value)

    return None


def _extract_value(point: dict[str, Any]) -> float | None:
    for key in _VALUE_KEYS:
        if key in point:
            parsed = parse_numeric_value(point[key])
            if parsed is not None or point[key] is None:
                return parsed
    return None


def _extract_variable_id(series: dict[str, Any], meta_data: list[Any]) -> str:
    direct_value = _pick_string(series, _VARIABLE_KEYS)
    if direct_value:
        return direct_value

    for item in meta_data:
        if not isinstance(item, dict):
            continue
        candidate_label = _pick_string(
            item, ("Variable", "variable", "Descripcion", "description", "Nombre", "name")
        )
        if any(hint in _normalized_text(candidate_label) for hint in _GEO_HINTS):
            continue
        candidate_id = _pick_string(item, _ID_KEYS)
        if candidate_id:
            return candidate_id

    return ""


def _extract_geography(meta_data: list[Any], series: dict[str, Any]) -> tuple[str, str]:
    for item in meta_data:
        if not isinstance(item, dict):
            continue
        candidate_label = _pick_string(
            item, ("Variable", "variable", "Descripcion", "description", "Nombre", "name")
        )
        if any(hint in _normalized_text(candidate_label) for hint in _GEO_HINTS):
            return _pick_string(item, ("Nombre", "Valor", "value")), _pick_string(item, _ID_KEYS)

    series_name = _normalized_text(_extract_series_name(series))
    if "asturias" in series_name:
        return "Asturias", ""

    return "", ""


def _extract_series_name(series: dict[str, Any]) -> str:
    return _pick_string(series, _NAME_KEYS)


def _looks_like_series_record(payload: dict[str, Any]) -> bool:
    return any(key in payload for key in _SERIES_HINT_KEYS)


def _looks_like_observation(payload: dict[str, Any]) -> bool:
    return any(key in payload for key in _OBSERVATION_HINT_KEYS)


def _pick_string(payload: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def _ensure_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def _normalized_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    return ascii_value.lower().strip()


# ---------------------------------------------------------------------------
# Series directas (DATOS_SERIE) — normalizer
# ---------------------------------------------------------------------------

_FK_PERIODO_TO_SUFFIX: dict[int, str] = {
    1: "M01", 2: "M02", 3: "M03", 4: "M04", 5: "M05", 6: "M06",
    7: "M07", 8: "M08", 9: "M09", 10: "M10", 11: "M11", 12: "M12",
    13: "T1", 14: "T2", 15: "T3", 16: "T4",
    28: "",  # anual — sólo Anyo
}


def _build_period_from_anyo_fk(anyo: Any, fk_periodo: Any) -> str | None:
    """Construye el string de periodo a partir de Anyo + FK_Periodo del INE.

    Ejemplos: (2011, 8) → "2011M08", (2022, 28) → "2022", (2023, 3) → "2023T1".
    Valores FK_Periodo desconocidos producen un string trazable como "2020FK99".
    """
    if anyo is None:
        return None
    year_str = str(anyo)
    if fk_periodo is None:
        return year_str
    try:
        suffix = _FK_PERIODO_TO_SUFFIX.get(int(fk_periodo))
    except (TypeError, ValueError):
        return f"{year_str}FK{fk_periodo}"
    if suffix is None:
        return f"{year_str}FK{fk_periodo}"
    return f"{year_str}{suffix}" if suffix else year_str


def normalize_serie_direct_payload_with_stats(
    series_list: list[dict[str, Any]],
    op_code: str,
    geography_name: str = "Principado de Asturias",
    geography_code: str = "33",
) -> NormalizationOutcome:
    """Normaliza una lista de respuestas DATOS_SERIE en NormalizedSeriesItems.

    Cada elemento de `series_list` es la respuesta directa de
    ``GET DATOS_SERIE/{cod}`` con estructura::

        {"COD": str, "Nombre": str, "FK_Unidad": int,
         "Data": [{"Anyo": int, "FK_Periodo": int, "Valor": float, "Secreto": bool}]}

    El ``geography_code`` y ``geography_name`` se fijan a la identidad
    territorial canonica configurada para este flujo, porque estas series ya
    han sido filtradas por nombre antes de llamar a este normalizer.
    """
    outcome = NormalizationOutcome()
    outcome.payload_type = "list"
    outcome.series_detected = len(series_list)

    for series in series_list:
        if not isinstance(series, dict):
            continue
        cod = series.get("COD") or series.get("Cod") or ""
        nombre = series.get("Nombre") or ""
        fk_unidad = series.get("FK_Unidad")
        unit = str(fk_unidad) if fk_unidad is not None else ""
        data_points = series.get("Data") or []
        outcome.observations_total += len(data_points)

        for point in data_points:
            if not isinstance(point, dict):
                continue

            secreto = point.get("Secreto", False)
            raw_valor = point.get("Valor")
            value: float | None = None
            if not secreto:
                value = parse_numeric_value(raw_valor)

            anyo = point.get("Anyo")
            fk_periodo = point.get("FK_Periodo")
            period = _build_period_from_anyo_fk(anyo, fk_periodo)

            if period is None and value is None:
                outcome.discarded_counts["missing_period_and_value"] = (
                    outcome.discarded_counts.get("missing_period_and_value", 0) + 1
                )
                continue

            outcome.items.append(
                NormalizedSeriesItem(
                    operation_code=op_code,
                    table_id=cod,
                    variable_id=cod,
                    geography_name=geography_name,
                    geography_code=geography_code,
                    period=period or "unknown",
                    value=value,
                    unit=unit,
                    metadata={
                        "series_name": nombre,
                        "series_code": cod,
                        "meta_data": [],
                        "series_context": {k: v for k, v in series.items() if k != "Data"},
                        "point_context": {
                            k: v for k, v in point.items()
                            if k not in {"Valor", "Anyo", "FK_Periodo", "Fecha"}
                        },
                    },
                    raw_payload={
                        "series_name": nombre,
                        "series_code": cod,
                        "meta_data": [],
                        "series": {k: v for k, v in series.items() if k != "Data"},
                        "point": point,
                    },
                )
            )

    return outcome
