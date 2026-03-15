from __future__ import annotations

import hashlib
import os
import secrets
from collections.abc import Mapping
from typing import Any
from urllib.parse import unquote, urlsplit


DEFAULT_API_KEY_BYTES = 32
DEFAULT_SECRET_MIN_LENGTH = 24
WEAK_SECRET_VALUES = {
    "",
    "postgres",
    "password",
    "secret",
    "change-me",
    "changeme",
    "replace-me",
    "example",
    "local-dev-api-key-change-me",
    "local-dev-postgres-password-change-me",
}


def generate_api_key(byte_length: int = DEFAULT_API_KEY_BYTES) -> str:
    """Generate a high-entropy API key suitable for environment configuration."""
    return secrets.token_urlsafe(byte_length)


def get_api_key_from_env() -> str | None:
    """Read the configured API key from the current process environment."""
    value = os.getenv("API_KEY")
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def compare_api_keys(provided_key: str | None, configured_key: str | None) -> bool:
    """Compare API keys in constant time."""
    if not provided_key or not configured_key:
        return False
    return secrets.compare_digest(provided_key, configured_key)


def hash_sensitive_data(data: str) -> str:
    """Hash potentially sensitive text for logs or audit identifiers."""
    normalized = (data or "").strip()
    if not normalized:
        return "empty"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def sanitize_for_logging(text: str, max_length: int = 50) -> str:
    """Produce a bounded representation for logs without exposing full values."""
    if not text:
        return "[empty]"
    if len(text) > max_length:
        return f"{text[:max_length]}... [+{len(text) - max_length} chars]"
    return text


def sanitize_query_params_for_logging(query_params: Mapping[str, Any]) -> dict[str, Any]:
    """Summarize request query params without logging raw values."""
    if not query_params:
        return {"query_params_count": 0, "query_param_keys": []}

    normalized_items = sorted((str(key), str(value)) for key, value in query_params.items())
    canonical = "&".join(f"{key}={value}" for key, value in normalized_items)
    return {
        "query_params_count": len(normalized_items),
        "query_param_keys": [key for key, _ in normalized_items],
        "query_fingerprint": hash_sensitive_data(canonical),
    }


def extract_password_from_dsn(dsn: str | None) -> str | None:
    if not dsn:
        return None

    parsed = urlsplit(dsn)
    if parsed.password is None:
        return None
    return unquote(parsed.password)


def is_weak_secret(secret: str | None, *, min_length: int = DEFAULT_SECRET_MIN_LENGTH) -> bool:
    if secret is None:
        return True

    normalized = secret.strip()
    if len(normalized) < min_length:
        return True

    lowered = normalized.casefold()
    if lowered in WEAK_SECRET_VALUES:
        return True

    if lowered.startswith("change-") or lowered.startswith("replace-"):
        return True

    return False


def ensure_secret_strength(
    secret: str | None,
    *,
    secret_name: str,
    min_length: int = DEFAULT_SECRET_MIN_LENGTH,
) -> None:
    if is_weak_secret(secret, min_length=min_length):
        raise ValueError(
            f"{secret_name} must be configured with a non-default secret of at least "
            f"{min_length} characters."
        )
