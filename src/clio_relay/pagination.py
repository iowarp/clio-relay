"""Shared response-page bounds for relay event and monitor surfaces."""

from __future__ import annotations

DEFAULT_RESPONSE_PAGE_RECORDS = 100
MAX_RESPONSE_PAGE_RECORDS = 500
MAX_GC_BATCH_RECORDS = 100


def validate_response_page_limit(value: object, *, field_name: str = "limit") -> int:
    """Return a strict page limit or reject values outside the shared bound."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    if value < 1 or value > MAX_RESPONSE_PAGE_RECORDS:
        raise ValueError(f"{field_name} must be between 1 and {MAX_RESPONSE_PAGE_RECORDS}")
    return value


def validate_record_cursor(value: object, *, field_name: str = "cursor") -> int:
    """Return a one-based durable record cursor."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    if value < 1:
        raise ValueError(f"{field_name} must be greater than or equal to 1")
    return value


def validate_gc_batch_size(value: object) -> int:
    """Return a bounded destructive GC batch size."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("batch_size must be an integer")
    if value < 1 or value > MAX_GC_BATCH_RECORDS:
        raise ValueError(f"batch_size must be between 1 and {MAX_GC_BATCH_RECORDS}")
    return value
