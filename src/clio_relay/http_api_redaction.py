"""Capability-redaction helpers shared by every ``http_api`` HTTP response.

split/http-api-w3 (iowarp/clio-relay#231): moved verbatim out of
``http_api.py``. None of these four functions referenced any of
``create_app()``'s local closures -- they take their record/payload as a
plain argument -- so this is an unmodified, atomic move.
"""

from __future__ import annotations

from typing import TypeVar, cast

from pydantic import BaseModel

from clio_relay.validation_report import redact_sensitive_values

ModelRecord = TypeVar("ModelRecord", bound=BaseModel)


def _public_record(record: ModelRecord) -> ModelRecord:  # noqa: UP047
    """Return a response copy with nested capability values redacted."""
    original = record.model_dump(mode="json")
    payload = _restore_environment_references(original, redact_sensitive_values(original))
    return type(record).model_validate(payload)


def _public_payload(payload: dict[str, object]) -> dict[str, object]:
    """Redact nested capability values from a free-form HTTP payload."""
    redacted = _restore_environment_references(payload, redact_sensitive_values(payload))
    return cast(dict[str, object], redacted)


def _public_model_page(  # noqa: UP047
    record_key: str,
    records: list[ModelRecord],
    *,
    cursor: int,
    limit: int,
    next_cursor: int | None,
    total: int,
) -> dict[str, object]:
    """Return a redacted, stable one-based model collection page."""
    return {
        record_key: [record.model_dump(mode="json") for record in records],
        "cursor": cursor,
        "limit": limit,
        "next_cursor": next_cursor,
        "total": total,
    }


def _restore_environment_references(original: object, redacted: object) -> object:
    """Keep non-secret env_from variable names valid after capability redaction."""
    if isinstance(original, dict) and isinstance(redacted, dict):
        original_mapping = cast(dict[object, object], original)
        redacted_mapping = cast(dict[object, object], redacted)
        restored: dict[object, object] = {}
        for key, value in redacted_mapping.items():
            original_value = original_mapping.get(key)
            restored[key] = (
                original_value
                if key == "env_from" and isinstance(original_value, dict)
                else _restore_environment_references(original_value, value)
            )
        return restored
    if isinstance(original, list) and isinstance(redacted, list):
        original_values = cast(list[object], original)
        redacted_values = cast(list[object], redacted)
        return [
            _restore_environment_references(original_value, redacted_value)
            for original_value, redacted_value in zip(
                original_values,
                redacted_values,
                strict=False,
            )
        ]
    return redacted
