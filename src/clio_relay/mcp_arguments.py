"""Pure MCP tool-argument coercion and validation helpers.

Split out of mcp_server.py (iowarp/clio-relay#231) -- ~25 small private
functions reading/validating one field out of an MCP tool call's
``arguments`` dict (or a comparably shaped payload), scattered across two
widely separated regions of the original file. None of them call an
imported name any test monkeypatches (confirmed by grep before the move --
contrast ``_remote_json``/``_remote_json_value``, which call the
monkeypatched ``run_remote_clio`` and stayed in mcp_server.py for exactly
that reason), so this is a clean leaf module: pure functions of their
arguments, no dependency on live server/session state, no back-reference to
mcp_server.py.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime
from typing import Any, cast

from pydantic import ValidationError

from clio_relay.identifiers import validate_durable_record_id
from clio_relay.mcp_tool_catalog_job_lifecycle import MAX_AGENT_LOG_READ_BYTES
from clio_relay.models import ArtifactUse, artifact_use_payload, validate_artifact_use_collection
from clio_relay.pagination import (
    DEFAULT_RESPONSE_PAGE_RECORDS,
    validate_record_cursor,
    validate_response_page_limit,
)
from clio_relay.spool import MAX_LOG_READ_BYTES

JSON = dict[str, Any]


def _positive_integer_argument(
    arguments: JSON,
    field_name: str,
    *,
    default: int | None = None,
    required: bool = False,
) -> int:
    """Read one positive integer without treating booleans as integers."""
    if field_name not in arguments:
        if required or default is None:
            raise ValueError(f"{field_name} is required")
        return default
    value = arguments[field_name]
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _boolean_argument(arguments: JSON, field_name: str, *, default: bool) -> bool:
    """Read one strict boolean argument."""
    value = arguments.get(field_name, default)
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean")
    return value


def _object(value: Any) -> JSON:
    if not isinstance(value, dict):
        raise ValueError("expected object")
    return cast(JSON, value)


def _required_str(value: JSON, key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"{key} is required")
    return item


def _optional_str(value: JSON, key: str) -> str | None:
    item = value.get(key)
    if item is None:
        return None
    if not isinstance(item, str) or not item:
        raise ValueError(f"{key} must be a non-empty string")
    return item


def _required_durable_record_id(value: JSON, key: str) -> str:
    """Read and validate a required durable record ID before queue access."""
    return validate_durable_record_id(_required_str(value, key))


def _optional_durable_record_id(value: JSON, key: str) -> str | None:
    """Read and validate an optional durable record ID before queue access."""
    item = _optional_str(value, key)
    return None if item is None else validate_durable_record_id(item)


def _string_list(value: Any, name: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a string array")
    items = cast(list[object], value)
    if not all(isinstance(item, str) for item in items):
        raise ValueError(f"{name} must be a string array")
    return cast(list[str], items)


def _string_mapping(value: Any, name: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a string object")
    mapping = cast(dict[object, object], value)
    if not all(isinstance(key, str) and isinstance(item, str) for key, item in mapping.items()):
        raise ValueError(f"{name} must be a string object")
    return cast(dict[str, str], mapping)


def _artifact_use_refs(arguments: JSON) -> list[ArtifactUse]:
    """Parse and canonicalize content-pinned artifact dependencies."""
    raw = arguments.get("used_artifact_refs", [])
    if not isinstance(raw, list):
        raise ValueError("used_artifact_refs must be an array")
    values = cast(list[object], raw)
    if len(values) > 1_000:
        raise ValueError("used_artifact_refs must contain at most 1000 records")
    try:
        refs = [ArtifactUse.model_validate(value) for value in values]
    except ValidationError as exc:
        raise ValueError(f"used_artifact_refs is invalid: {exc}") from exc
    artifact_ids = [ref.artifact_id for ref in refs]
    if len(artifact_ids) != len(set(artifact_ids)):
        raise ValueError("used_artifact_refs must contain unique artifact_id values")
    canonical = sorted(refs, key=lambda ref: ref.artifact_id)
    validate_artifact_use_collection(canonical)
    return canonical


def _artifact_use_cli_value(ref: ArtifactUse) -> str:
    """Render a legacy shorthand or canonical JSON dependency for remote CLI transport."""
    if ref.provenance is None:
        return f"{ref.artifact_id}={ref.sha256}"
    return json.dumps(
        artifact_use_payload(ref),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def _log_limit(arguments: JSON) -> int:
    return _bounded_integer_limit(
        arguments,
        field_name="log_limit",
        default=MAX_AGENT_LOG_READ_BYTES,
        maximum=MAX_AGENT_LOG_READ_BYTES,
    )


def _job_log_limit(arguments: JSON) -> int:
    return _bounded_integer_limit(
        arguments,
        field_name="limit",
        default=65_536,
        maximum=MAX_LOG_READ_BYTES,
    )


def _bounded_integer_limit(
    arguments: JSON,
    *,
    field_name: str,
    default: int,
    maximum: int,
) -> int:
    value = arguments.get(field_name, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    if value < 1 or value > maximum:
        raise ValueError(f"{field_name} must be between 1 and {maximum}")
    return value


def _positive_float_argument(
    arguments: JSON,
    field_name: str,
    *,
    default: float,
    maximum: float,
) -> float:
    """Read a positive bounded numeric MCP argument without accepting booleans."""
    raw = arguments.get(field_name, default)
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(f"{field_name} must be a number")
    value = float(raw)
    if value <= 0 or value > maximum:
        raise ValueError(f"{field_name} must be greater than 0 and at most {maximum:g}")
    return value


def _observation_timeout_seconds(
    arguments: JSON,
    field_name: str,
    *,
    default: float = 600.0,
) -> float:
    """Read one finite positive observation bound without creating an execution deadline."""
    raw = arguments.get(field_name, default)
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(f"{field_name} must be a number")
    value = float(raw)
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{field_name} must be a finite number greater than 0")
    return value


def _attach_wait_observation(
    result: JSON,
    *,
    observation_unknown: bool,
    timeout_seconds: float,
) -> None:
    """Attach a machine-readable outcome for one bounded wait without mutating its job."""
    result["observation"] = {
        "outcome": "observation_unknown" if observation_unknown else "terminal",
        "timeout_seconds": timeout_seconds,
        "scheduler_action": "none",
        "relay_action": "none",
    }


def _jarvis_submission_wait_timeout_seconds(arguments: JSON) -> float:
    """Resolve canonical and legacy JARVIS submission observation bounds."""
    resolved: dict[str, float] = {}
    for field_name in ("wait_timeout_seconds", "timeout_seconds"):
        if field_name not in arguments:
            continue
        raw = arguments[field_name]
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise ValueError(f"{field_name} must be a number")
        value = float(raw)
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"{field_name} must be a finite number greater than 0")
        resolved[field_name] = value
    canonical = resolved.get("wait_timeout_seconds")
    legacy = resolved.get("timeout_seconds")
    if canonical is not None and legacy is not None and canonical != legacy:
        raise ValueError(
            "wait_timeout_seconds and legacy timeout_seconds must be equal when both are "
            "provided; both fields bound observation only"
        )
    if canonical is not None:
        return canonical
    if legacy is not None:
        return legacy
    return 600.0


def _response_page_limit(arguments: JSON) -> int:
    return validate_response_page_limit(arguments.get("limit", DEFAULT_RESPONSE_PAGE_RECORDS))


def _response_page_cursor(arguments: JSON) -> int:
    return validate_record_cursor(arguments.get("cursor", 1))


def _record_page(
    record_key: str,
    records: list[JSON],
    *,
    cursor: int,
    limit: int,
    next_cursor: int | None,
    total: int,
) -> JSON:
    """Build the shared one-based collection response used by MCP tools."""
    return {
        record_key: records,
        "cursor": cursor,
        "limit": limit,
        "next_cursor": next_cursor,
        "total": total,
    }


def _optional_int(value: JSON, key: str) -> int | None:
    item = value.get(key)
    if item is None:
        return None
    return int(item)


def _optional_float(value: JSON, key: str) -> float | None:
    item = value.get(key)
    if item is None:
        return None
    if isinstance(item, bool):
        raise ValueError(f"{key} must be a number")
    return float(item)


def _optional_datetime_argument(value: JSON, key: str) -> datetime | None:
    item = value.get(key)
    if item is None:
        return None
    if not isinstance(item, str):
        raise ValueError(f"{key} must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(item)
    except ValueError as exc:
        raise ValueError(f"{key} must be an ISO-8601 string") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{key} must include a timezone")
    return parsed


def _stable_digest(value: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
