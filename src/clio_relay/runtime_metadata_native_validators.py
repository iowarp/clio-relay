"""Strict, fail-closed field validators for exact native JARVIS documents.

Extracted from ``runtime_metadata.py`` (clio-relay split/runtime-metadata-w2):
these bounded string/identity/timestamp/JSON validators back every field on
the exact native JARVIS wire-document family
(``runtime_metadata_native_documents.py``). Unlike the loose coercion helpers
in ``runtime_metadata_coercion.py``, every function here raises ``ValueError``
on a value that does not fit -- native producer documents are trusted-schema
inputs that must fail closed, not best-effort extractions from an untrusted
heterogeneous payload.
"""

from __future__ import annotations

import json
from datetime import datetime

from clio_relay.runtime_metadata_types import _WINDOWS_RESERVED_COMPONENTS


def _validate_native_identity(value: str, field_name: str) -> None:
    """Validate one portable JARVIS execution identity."""
    _validate_native_text(value, field_name, maximum=128)
    reserved_stem = value.split(".", 1)[0].upper()
    if (
        not value[0].isalnum()
        or value.endswith(".")
        or reserved_stem in _WINDOWS_RESERVED_COMPONENTS
        or any(
            not (character.isascii() and (character.isalnum() or character in "._-"))
            for character in value
        )
    ):
        raise ValueError(f"native JARVIS {field_name} must use portable ASCII identity characters")


def _validate_native_text(
    value: str,
    field_name: str,
    *,
    maximum: int = 4096,
    allow_newlines: bool = False,
) -> None:
    """Validate one bounded nonempty native producer string."""
    if not value or len(value.encode("utf-8")) > maximum:
        raise ValueError(f"native JARVIS {field_name} must be a bounded nonempty string")
    allowed_controls: set[str] = {"\n", "\r", "\t"} if allow_newlines else set()
    if any(
        (ord(character) < 32 and character not in allowed_controls) or ord(character) == 127
        for character in value
    ):
        raise ValueError(f"native JARVIS {field_name} contains control characters")


def _validate_native_timestamp(value: str, field_name: str) -> None:
    """Require a timezone-aware ISO-8601 producer timestamp."""
    _validate_native_text(value, field_name, maximum=64)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"native JARVIS {field_name} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"native JARVIS {field_name} must include a timezone")


def _validate_native_json(value: object, label: str, *, maximum: int) -> None:
    """Require finite, bounded JSON from a native producer."""
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ValueError(f"native JARVIS {label} must contain finite JSON") from exc
    if len(encoded) > maximum:
        raise ValueError(f"native JARVIS {label} exceeds its byte limit")
