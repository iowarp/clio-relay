"""Validation for the bounded JARVIS artifact query/page/event wire documents.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3). Pure
leaf validation -- no facade reach-back needed.
"""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any, cast
from urllib.parse import urlsplit

from clio_relay.constants import (
    _JARVIS_ARTIFACT_CHECKSUM,
    _JARVIS_ARTIFACT_CURSOR,
    _JARVIS_ARTIFACT_DEFAULT_PAGE_SIZE,
    _JARVIS_ARTIFACT_ID,
    _JARVIS_ARTIFACT_LOCATION_KINDS,
    _JARVIS_ARTIFACT_MAX_CURSOR_LENGTH,
    _JARVIS_ARTIFACT_MAX_EVENT_BYTES,
    _JARVIS_ARTIFACT_MAX_METADATA_BYTES,
    _JARVIS_ARTIFACT_MAX_PAGE_SIZE,
    _JARVIS_ARTIFACT_MEDIA_TYPE,
    _JARVIS_ARTIFACT_OPTIONAL_FIELDS,
    _JARVIS_ARTIFACT_OWNERSHIP,
    _JARVIS_ARTIFACT_REQUIRED_FIELDS,
    _JARVIS_ARTIFACT_ROLES,
    _JARVIS_ARTIFACT_STATES,
    _JARVIS_ARTIFACT_STRUCTURES,
    _JARVIS_ARTIFACT_UNSAFE_URI_SCHEMES,
    _JARVIS_ARTIFACT_URI_SCHEME,
    MCP_JARVIS_ARTIFACT_SCHEMA,
    MCP_JARVIS_EXECUTION_ARTIFACTS_SCHEMA,
)
from clio_relay.protocol_messages import (
    _bounded_finite_json,
    _finite_progress_number,
    _McpProtocolFailure,
)


def _validated_jarvis_artifact_query(value: object) -> dict[str, Any] | None:
    """Validate the bounded artifact selector before trusting its response page."""
    if value is None:
        return None
    if not isinstance(value, dict):
        raise _McpProtocolFailure("MCP JARVIS artifacts query must be an object or null")
    typed = dict(cast(dict[str, Any], value))
    allowed = {"package_id", "role", "state", "artifact_id", "page_size", "cursor"}
    allowed |= {"content_max_bytes"}
    if not set(typed).issubset(allowed):
        raise _McpProtocolFailure("MCP JARVIS artifact query contained unknown filters")
    for field_name, maximum in (("package_id", 256), ("artifact_id", 90)):
        if (field_value := typed.get(field_name)) is not None:
            _jarvis_artifact_text(field_value, field_name, maximum=maximum)
    artifact_id = typed.get("artifact_id")
    if artifact_id is not None and _JARVIS_ARTIFACT_ID.fullmatch(cast(str, artifact_id)) is None:
        raise _McpProtocolFailure("MCP JARVIS artifact_id filter was invalid")
    role = typed.get("role")
    if role is not None and role not in _JARVIS_ARTIFACT_ROLES:
        raise _McpProtocolFailure("MCP JARVIS artifact role filter was invalid")
    state = typed.get("state")
    if state is not None and state not in _JARVIS_ARTIFACT_STATES:
        raise _McpProtocolFailure("MCP JARVIS artifact state filter was invalid")
    page_size = typed.get("page_size", _JARVIS_ARTIFACT_DEFAULT_PAGE_SIZE)
    if (
        isinstance(page_size, bool)
        or not isinstance(page_size, int)
        or not 1 <= page_size <= _JARVIS_ARTIFACT_MAX_PAGE_SIZE
    ):
        raise _McpProtocolFailure("MCP JARVIS artifact page_size was invalid")
    cursor = typed.get("cursor")
    if cursor is not None and (
        not isinstance(cursor, str)
        or not cursor
        or len(cursor) > _JARVIS_ARTIFACT_MAX_CURSOR_LENGTH
        or _JARVIS_ARTIFACT_CURSOR.fullmatch(cursor) is None
    ):
        raise _McpProtocolFailure("MCP JARVIS artifact cursor was invalid")
    return {
        "package_id": typed.get("package_id"),
        "role": role,
        "state": state,
        "artifact_id": artifact_id,
        "page_size": page_size,
        "cursor": cursor,
    }


def _validated_jarvis_artifact_page(
    value: object,
    *,
    query: dict[str, Any],
    pipeline_id: str,
    execution_id: str,
    execution_state: str,
    terminal: bool,
) -> dict[str, Any]:
    """Validate identity, lifecycle, counts, filters, and cursor bounds for one page."""
    if not isinstance(value, dict):
        raise _McpProtocolFailure("MCP JARVIS artifact_page must be an object")
    typed = dict(cast(dict[str, Any], value))
    expected = {
        "producer_schema_version",
        "pipeline_id",
        "execution_id",
        "execution_state",
        "terminal",
        "artifacts",
        "matching_artifact_count",
        "returned_artifact_count",
        "next_cursor",
    }
    if set(typed) != expected or typed.get("producer_schema_version") != (
        MCP_JARVIS_EXECUTION_ARTIFACTS_SCHEMA
    ):
        raise _McpProtocolFailure("MCP JARVIS artifact page schema was invalid")
    if typed.get("pipeline_id") != pipeline_id or typed.get("execution_id") != execution_id:
        raise _McpProtocolFailure("MCP JARVIS artifact page identity did not match")
    if typed.get("execution_state") != execution_state or typed.get("terminal") is not terminal:
        raise _McpProtocolFailure("MCP JARVIS artifact page lifecycle did not match")
    raw_artifacts = typed.get("artifacts")
    if not isinstance(raw_artifacts, list):
        raise _McpProtocolFailure("MCP JARVIS artifact page entries must be an array")
    artifact_items = cast(list[object], raw_artifacts)
    page_size = cast(int, query["page_size"])
    if len(artifact_items) > page_size:
        raise _McpProtocolFailure("MCP JARVIS artifact page exceeded the requested page_size")
    seen_ids: set[str] = set()
    artifacts = [
        _validated_jarvis_artifact_event(
            item,
            execution_id=execution_id,
            query=query,
            seen_ids=seen_ids,
        )
        for item in artifact_items
    ]
    returned = typed.get("returned_artifact_count")
    matching = typed.get("matching_artifact_count")
    if (
        isinstance(returned, bool)
        or not isinstance(returned, int)
        or returned != len(artifacts)
        or isinstance(matching, bool)
        or not isinstance(matching, int)
        or matching < returned
    ):
        raise _McpProtocolFailure("MCP JARVIS artifact page counts did not match")
    if query.get("artifact_id") is not None and (matching > 1 or returned > 1):
        raise _McpProtocolFailure("MCP JARVIS exact artifact filter returned multiple matches")
    next_cursor = typed.get("next_cursor")
    if next_cursor is not None and (
        not artifacts
        or not isinstance(next_cursor, str)
        or not next_cursor
        or len(next_cursor) > _JARVIS_ARTIFACT_MAX_CURSOR_LENGTH
        or _JARVIS_ARTIFACT_CURSOR.fullmatch(next_cursor) is None
    ):
        raise _McpProtocolFailure("MCP JARVIS artifact next_cursor was invalid")
    if query.get("artifact_id") is not None and next_cursor is not None:
        raise _McpProtocolFailure("MCP JARVIS exact artifact filter unexpectedly paginated")
    typed["artifacts"] = artifacts
    return typed


def _validated_jarvis_artifact_event(
    value: object,
    *,
    execution_id: str,
    query: dict[str, Any],
    seen_ids: set[str],
) -> dict[str, Any]:
    """Validate one generated artifact and require it to satisfy the request filters."""
    if not isinstance(value, dict):
        raise _McpProtocolFailure("MCP JARVIS artifact entry must be an object")
    typed = dict(cast(dict[str, Any], value))
    if (
        not _JARVIS_ARTIFACT_REQUIRED_FIELDS.issubset(typed)
        or not set(typed).issubset(
            _JARVIS_ARTIFACT_REQUIRED_FIELDS | _JARVIS_ARTIFACT_OPTIONAL_FIELDS
        )
        or typed.get("schema_version") != MCP_JARVIS_ARTIFACT_SCHEMA
        or typed.get("execution_id") != execution_id
    ):
        raise _McpProtocolFailure("MCP JARVIS artifact entry schema or identity was invalid")
    for field_name in ("package_name", "package_id", "logical_name", "kind"):
        _jarvis_artifact_text(typed.get(field_name), field_name, maximum=256)
    artifact_id = typed.get("artifact_id")
    if (
        not isinstance(artifact_id, str)
        or _JARVIS_ARTIFACT_ID.fullmatch(artifact_id) is None
        or artifact_id in seen_ids
    ):
        raise _McpProtocolFailure("MCP JARVIS artifact identity was invalid")
    seen_ids.add(artifact_id)
    allowed_fields = {
        "role": _JARVIS_ARTIFACT_ROLES,
        "state": _JARVIS_ARTIFACT_STATES,
        "structure": _JARVIS_ARTIFACT_STRUCTURES,
        "ownership": _JARVIS_ARTIFACT_OWNERSHIP,
    }
    for field_name, allowed in allowed_fields.items():
        if typed.get(field_name) not in allowed:
            raise _McpProtocolFailure(f"MCP JARVIS artifact {field_name} was invalid")
    for field_name in ("revision", "sequence"):
        item = typed.get(field_name)
        if isinstance(item, bool) or not isinstance(item, int) or item < 1:
            raise _McpProtocolFailure(f"MCP JARVIS artifact {field_name} was invalid")
    observed = _finite_progress_number(typed.get("observed_at_epoch"))
    if observed is None or observed < 0:
        raise _McpProtocolFailure("MCP JARVIS artifact observation time was invalid")
    metadata_value = typed.get("metadata")
    if not isinstance(metadata_value, dict):
        raise _McpProtocolFailure("MCP JARVIS artifact metadata was invalid")
    _bounded_finite_json(
        cast(dict[str, Any], metadata_value),
        "JARVIS artifact metadata",
        _JARVIS_ARTIFACT_MAX_METADATA_BYTES,
    )
    _validate_jarvis_artifact_location(typed)
    _validate_jarvis_artifact_optional_fields(typed)
    _bounded_finite_json(typed, "JARVIS artifact entry", _JARVIS_ARTIFACT_MAX_EVENT_BYTES)
    for field_name in ("package_id", "role", "state", "artifact_id"):
        expected = query.get(field_name)
        if expected is not None and typed.get(field_name) != expected:
            raise _McpProtocolFailure(
                f"MCP JARVIS artifact did not satisfy the {field_name} filter"
            )
    return typed


def _validate_jarvis_artifact_location(value: dict[str, Any]) -> None:
    """Validate transport-neutral location and ownership semantics."""
    location = value.get("location")
    if location is None:
        if value["state"] in {"available", "finalized"}:
            raise _McpProtocolFailure("MCP JARVIS available artifact omitted its location")
        return
    if not isinstance(location, dict) or set(cast(dict[object, object], location)) != {
        "kind",
        "value",
    }:
        raise _McpProtocolFailure("MCP JARVIS artifact location was invalid")
    typed_location = cast(dict[str, Any], location)
    kind = typed_location.get("kind")
    if kind not in _JARVIS_ARTIFACT_LOCATION_KINDS:
        raise _McpProtocolFailure("MCP JARVIS artifact location kind was invalid")
    rendered = _jarvis_artifact_text(
        typed_location.get("value"),
        "location",
        maximum=4096,
    )
    if kind == "execution_path":
        path = PurePosixPath(rendered)
        if (
            "\\" in rendered
            or path.is_absolute()
            or rendered.startswith("/")
            or rendered.endswith("/")
            or "//" in rendered
            or any(part in {"", ".", ".."} for part in path.parts)
            or (bool(path.parts) and ":" in path.parts[0])
            or path.as_posix() != rendered
        ):
            raise _McpProtocolFailure("MCP JARVIS execution artifact path was invalid")
    elif kind == "cluster_path":
        path = PurePosixPath(rendered)
        if (
            "\\" in rendered
            or not path.is_absolute()
            or not rendered.startswith("/")
            or rendered == "/"
            or rendered.endswith("/")
            or "//" in rendered
            or any(part in {"", ".", ".."} for part in path.parts[1:])
            or path.as_posix() != rendered
        ):
            raise _McpProtocolFailure("MCP JARVIS cluster artifact path was invalid")
    else:
        try:
            parsed = urlsplit(rendered)
            has_user_info = parsed.username is not None or parsed.password is not None
        except ValueError as exc:
            raise _McpProtocolFailure("MCP JARVIS external artifact URI was invalid") from exc
        scheme = parsed.scheme.lower()
        if (
            not scheme
            or _JARVIS_ARTIFACT_URI_SCHEME.fullmatch(scheme) is None
            or len(scheme) == 1
            or scheme in _JARVIS_ARTIFACT_UNSAFE_URI_SCHEMES
            or has_user_info
            or (scheme in {"gs", "http", "https", "s3"} and not parsed.netloc)
        ):
            raise _McpProtocolFailure("MCP JARVIS external artifact URI was invalid")
    if (kind == "execution_path") is not (value["ownership"] == "execution"):
        raise _McpProtocolFailure("MCP JARVIS artifact location ownership was invalid")


def _validate_jarvis_artifact_optional_fields(value: dict[str, Any]) -> None:
    """Validate optional generated-artifact metadata fields."""
    for field_name, maximum in (("format", 256), ("message", 4096)):
        if field_name in value:
            _jarvis_artifact_text(value[field_name], field_name, maximum=maximum)
    media_type = value.get("media_type")
    if media_type is not None and (
        not isinstance(media_type, str) or _JARVIS_ARTIFACT_MEDIA_TYPE.fullmatch(media_type) is None
    ):
        raise _McpProtocolFailure("MCP JARVIS artifact media_type was invalid")
    if "size_bytes" in value:
        size = value["size_bytes"]
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise _McpProtocolFailure("MCP JARVIS artifact size_bytes was invalid")
    checksum = value.get("checksum")
    if checksum is not None and (
        not isinstance(checksum, str) or _JARVIS_ARTIFACT_CHECKSUM.fullmatch(checksum) is None
    ):
        raise _McpProtocolFailure("MCP JARVIS artifact checksum was invalid")


def _jarvis_artifact_text(value: object, field_name: str, *, maximum: int) -> str:
    """Return one bounded nonblank artifact field without control characters."""
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise _McpProtocolFailure(f"MCP JARVIS artifact {field_name} was invalid")
    return value
