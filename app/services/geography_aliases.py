from __future__ import annotations

import re
import unicodedata


_ASTURIAS_CANONICAL_CODE = "33"
_ASTURIAS_LEGACY_CODES = frozenset({"33", "8999"})
_ASTURIAS_ALIAS_NAMES = frozenset(
    {
        "asturias",
        "principado de asturias",
        "asturias principado de",
    }
)


def normalize_geography_text(value: str | None) -> str:
    normalized_value = unicodedata.normalize("NFKD", value or "")
    normalized_value = "".join(
        character for character in normalized_value if not unicodedata.combining(character)
    )
    normalized_value = normalized_value.casefold().replace("_", " ")
    normalized_value = re.sub(r"[^\w\s]", " ", normalized_value)
    normalized_value = re.sub(r"\s+", " ", normalized_value)
    return normalized_value.strip()


def build_configured_geography_alias_codes(geography_code: str | None) -> set[str]:
    normalized_code = str(geography_code or "").strip()
    aliases = {normalized_code} if normalized_code else set()
    if normalized_code in _ASTURIAS_LEGACY_CODES:
        aliases |= set(_ASTURIAS_LEGACY_CODES)
    return aliases


def build_configured_geography_alias_names(geography_name: str | None) -> set[str]:
    normalized_name = normalize_geography_text(geography_name)
    aliases = {normalized_name} if normalized_name else set()

    if normalized_name:
        aliases.add(normalized_name.split()[-1])

    if "asturias" in aliases or "asturias" in normalized_name:
        aliases |= set(_ASTURIAS_ALIAS_NAMES)

    return aliases


def matches_configured_geography(
    *,
    candidate_name: str | None,
    candidate_code: str | None,
    geography_name: str,
    geography_code: str,
) -> bool:
    alias_codes = build_configured_geography_alias_codes(geography_code)
    alias_names = build_configured_geography_alias_names(geography_name)

    normalized_code = str(candidate_code or "").strip()
    normalized_name = normalize_geography_text(candidate_name)

    return (
        bool(normalized_code and normalized_code in alias_codes)
        or bool(normalized_name and normalized_name in alias_names)
    )


def canonicalize_configured_geography(
    *,
    candidate_name: str | None,
    candidate_code: str | None,
    geography_name: str,
    geography_code: str,
    canonical_name: str,
    canonical_code: str,
) -> tuple[str, str, bool]:
    current_name = str(candidate_name or "").strip()
    current_code = str(candidate_code or "").strip()

    if not matches_configured_geography(
        candidate_name=current_name,
        candidate_code=current_code,
        geography_name=geography_name,
        geography_code=geography_code,
    ):
        return current_name, current_code, False

    changed = current_name != canonical_name or current_code != canonical_code
    return canonical_name, canonical_code, changed


def is_asturias_code_alias(value: str | None) -> bool:
    return str(value or "").strip() in _ASTURIAS_LEGACY_CODES


def asturias_canonical_code() -> str:
    return _ASTURIAS_CANONICAL_CODE
