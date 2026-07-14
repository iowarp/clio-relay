"""Portable durable identifiers and collision-free filesystem keys."""

from __future__ import annotations

import re
from hashlib import sha256
from typing import Annotated

from pydantic import AfterValidator, StringConstraints

DURABLE_RECORD_ID_PATTERN = r"^[a-z0-9][a-z0-9_-]{0,127}$"
DURABLE_RECORD_ID_MAX_BYTES = 128
FILESYSTEM_KEY_VERSION = "k2"
FILESYSTEM_KEY_PREFIX = f"{FILESYSTEM_KEY_VERSION}-"

_DURABLE_RECORD_ID = re.compile(DURABLE_RECORD_ID_PATTERN)
_FILESYSTEM_KEY_DOMAIN = re.compile(r"[a-z][a-z0-9_.-]{0,63}\Z")
_WINDOWS_RESERVED_DEVICE_BASENAMES = frozenset(
    {
        "aux",
        "con",
        "nul",
        "prn",
        *(f"com{number}" for number in range(1, 10)),
        *(f"lpt{number}" for number in range(1, 10)),
    }
)


def validate_durable_record_id(value: object) -> str:
    """Return a portable durable record ID or raise ``ValueError``.

    Durable identifiers are deliberately narrower than operator-facing labels.
    The contract is portable across case-insensitive filesystems and rejects
    Windows device basenames even when validation runs on another platform.
    """
    if not isinstance(value, str):
        raise TypeError("durable record ID must be a string")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError("durable record ID must be valid UTF-8") from error
    if len(encoded) > DURABLE_RECORD_ID_MAX_BYTES:
        raise ValueError(
            f"durable record ID must be at most {DURABLE_RECORD_ID_MAX_BYTES} UTF-8 bytes"
        )
    if _DURABLE_RECORD_ID.fullmatch(value) is None:
        raise ValueError(
            "durable record ID must match "
            f"{DURABLE_RECORD_ID_PATTERN}: lowercase portable ASCII only"
        )
    if value in _WINDOWS_RESERVED_DEVICE_BASENAMES:
        raise ValueError(f"durable record ID is a reserved Windows device basename: {value}")
    return value


DurableRecordId = Annotated[
    str,
    StringConstraints(
        strict=True,
        min_length=1,
        max_length=DURABLE_RECORD_ID_MAX_BYTES,
        pattern=DURABLE_RECORD_ID_PATTERN,
    ),
    AfterValidator(validate_durable_record_id),
]


def durable_record_id_json_schema() -> dict[str, object]:
    """Return an independent JSON Schema fragment for a durable record ID."""
    return {
        "type": "string",
        "minLength": 1,
        "maxLength": DURABLE_RECORD_ID_MAX_BYTES,
        "pattern": DURABLE_RECORD_ID_PATTERN,
    }


def filesystem_key(value: object, *, domain: str) -> str:
    """Map a logical operator label to a portable, collision-free path component.

    Already-portable lowercase components are preserved for readability. Values
    that need encoding, including all values in the reserved ``k2-`` namespace,
    use a full domain-separated SHA-256 digest. Logical values are never changed.
    """
    _validate_filesystem_key_domain(domain)
    if not isinstance(value, str):
        raise TypeError("filesystem key value must be a string")
    if value == "":
        raise ValueError("filesystem key value must not be empty")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError("filesystem key value must be valid UTF-8") from error
    if _portable_passthrough_key(value):
        return value
    material = b"clio-relay.fs-key.v2\0" + domain.encode("ascii") + b"\0" + encoded
    return f"{FILESYSTEM_KEY_PREFIX}{sha256(material).hexdigest()}"


def _portable_passthrough_key(value: str) -> bool:
    if value.startswith(FILESYSTEM_KEY_PREFIX):
        return False
    try:
        validate_durable_record_id(value)
    except (TypeError, ValueError):
        return False
    return True


def _validate_filesystem_key_domain(domain: str) -> None:
    if _FILESYSTEM_KEY_DOMAIN.fullmatch(domain) is None:
        raise ValueError(
            "filesystem key domain must start with a lowercase letter and contain only "
            "lowercase ASCII letters, digits, dots, underscores, and hyphens"
        )
