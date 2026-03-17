from __future__ import annotations

import re
from typing import Any

from app.schemas import NormalizedSeriesItem


_GEOGRAPHY_CODE_KEYS = (
    "codigo_municipio",
    "cod_municipio",
    "codigo_ine",
    "cod_ine",
    "municipio_cod",
    "ine_code",
    "codigo",
)
_GEOGRAPHY_NAME_KEYS = ("municipio", "nombre_municipio", "municipio_nombre", "name", "nombre")
_PERIOD_KEYS = ("anyo", "año", "año_referencia", "year", "periodo", "period", "fecha")
_VALUE_KEYS = ("valor", "value", "dato", "data", "importe", "total", "habitantes", "pib")

_ASTURIAS_MUNICIPALITY_RE = re.compile(r"^(33\d{3}|0*33\d{3})$")


def normalize_sadei_row(row: dict[str, Any], dataset_id: str) -> NormalizedSeriesItem | None:
    """
    Normalise a single raw SADEI Excel row to a NormalizedSeriesItem.

    Returns None if mandatory fields (period, geography_code) cannot be resolved.
    """
    lowered = {k.lower().strip(): v for k, v in row.items() if k != "_dataset_id"}

    geography_code = _extract_str(lowered, _GEOGRAPHY_CODE_KEYS)
    geography_name = _extract_str(lowered, _GEOGRAPHY_NAME_KEYS)
    period = _extract_period(lowered)
    value = _extract_float(lowered, _VALUE_KEYS)

    if not period:
        return None
    if not geography_code:
        return None

    # Normalise to 5-digit INE code
    geography_code = geography_code.strip().zfill(5)

    return NormalizedSeriesItem(
        operation_code=f"sadei_{dataset_id}",
        table_id=dataset_id,
        variable_id=dataset_id,
        geography_name=geography_name or "",
        geography_code=geography_code,
        period=period,
        value=value,
        unit=_infer_unit(dataset_id),
        metadata_json={"source": "sadei", "dataset_id": dataset_id},
        raw_payload={k: str(v) if v is not None else None for k, v in lowered.items()},
        source_provider="sadei",
    )


def normalize_sadei_dataset(
    rows: list[dict[str, Any]], dataset_id: str
) -> list[NormalizedSeriesItem]:
    items: list[NormalizedSeriesItem] = []
    for row in rows:
        item = normalize_sadei_row(row, dataset_id)
        if item is not None:
            items.append(item)
    return items


def _extract_str(data: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        val = data.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    return ""


def _extract_period(data: dict[str, Any]) -> str:
    for key in _PERIOD_KEYS:
        val = data.get(key)
        if val is None:
            continue
        s = str(val).strip()
        # Accept 4-digit years only
        if re.fullmatch(r"\d{4}", s):
            return s
        # Try extracting year from datetime or "YYYY-MM-DD"
        m = re.search(r"\b(\d{4})\b", s)
        if m:
            year = int(m.group(1))
            if 1900 <= year <= 2100:
                return str(year)
    return ""


def _extract_float(data: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        val = data.get(key)
        if val is None:
            continue
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            return float(val)
        s = str(val).strip().replace(",", ".").replace(" ", "")
        try:
            return float(s)
        except ValueError:
            continue
    return None


def _infer_unit(dataset_id: str) -> str:
    if "pib" in dataset_id:
        return "miles_euros"
    if "padron" in dataset_id or "habitantes" in dataset_id:
        return "personas"
    return ""
