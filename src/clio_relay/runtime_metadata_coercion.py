"""Loose, best-effort coercion of heterogeneous producer payload shapes.

Extracted from ``runtime_metadata.py`` (clio-relay split/runtime-metadata-w2):
these are the small, dependency-shallow primitives the *loose* JARVIS/clio-kit
compatibility flow (``runtime_metadata_mcp_normalize.py``) uses to pull
typed values out of an untrusted, heterogeneously-shaped payload -- optional
string/bool/int/timestamp extraction, dict/list coercion, package-provenance
parsing into :class:`~clio_relay.runtime_metadata_core_model.PackageProvenance`,
and the two producer/tool identity predicates
(``_looks_like_runtime_payload``, ``_is_jarvis_run_tool``). These never raise:
a value that does not fit the expected shape is simply dropped, which is why
this family is kept separate from the *strict*, fail-closed native producer
validators in ``runtime_metadata_native_validators.py``.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, cast

from clio_relay.runtime_metadata_core_model import PackageProvenance


def _package_provenance(value: object) -> list[PackageProvenance]:
    if isinstance(value, dict):
        items: list[object] = [value]
    elif isinstance(value, list):
        items = cast(list[object], value)
    else:
        return []
    packages: list[PackageProvenance] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        typed = cast(dict[str, Any], item)
        name = _first_str(typed, "package_name", "name", "pkg_type", "pkg_name")
        if name is None:
            continue
        known = {
            "package_name",
            "name",
            "pkg_name",
            "version",
            "package_version",
            "pkg_version",
            "package_type",
            "pkg_type",
            "type",
            "package_id",
            "pkg_id",
            "global_id",
            "source",
            "path",
            "package_path",
            "config_path",
        }
        packages.append(
            PackageProvenance(
                name=name,
                version=_first_str(typed, "package_version", "version", "pkg_version"),
                package_type=_first_str(typed, "package_type", "pkg_type", "type"),
                package_id=_first_str(typed, "package_id", "pkg_id", "global_id"),
                source=_first_str(typed, "source"),
                path=_first_str(typed, "package_path", "path", "config_path"),
                metadata={key: value for key, value in typed.items() if key not in known},
            )
        )
    return packages


def _nodes(value: object) -> list[str]:
    if isinstance(value, str) and value:
        return [value]
    if isinstance(value, list):
        return [item for item in cast(list[object], value) if isinstance(item, str) and item]
    return []


def _path_value(
    runtime: dict[str, Any],
    paths: dict[str, Any],
    *keys: str,
) -> str | None:
    value = _first_str(runtime, *keys) or _first_str(paths, *keys)
    if value is not None:
        return value
    for key in keys:
        nested = _mapping(runtime.get(key)) or _mapping(paths.get(key))
        if nested is not None:
            nested_path = _first_str(nested, "path", "uri")
            if nested_path is not None:
                return nested_path
    return None


def _looks_like_runtime_payload(value: dict[str, Any]) -> bool:
    return bool(
        {
            "runtime_metadata",
            "runtime",
            "pipeline_id",
            "scheduler",
            "scheduler_job_id",
            "script_path",
            "hostfile_path",
            "allocated_nodes",
            "package_provenance",
            "terminal",
            "execution_handle",
            "execution_record",
            "progress",
        }
        & set(value)
    )


def _is_jarvis_run_tool(tool: str) -> bool:
    normalized = tool.replace("-", "_").lower()
    return normalized == "jarvis_run" or normalized.endswith(".jarvis_run")


def _mapping(value: object) -> dict[str, Any] | None:
    return cast(dict[str, Any], value) if isinstance(value, dict) else None


def _first_str(value: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        candidate = _optional_str(value.get(key))
        if candidate is not None:
            return candidate
    return None


def _optional_str(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _optional_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _optional_int(value: object) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _timestamp_string(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        return datetime.fromtimestamp(value, tz=UTC).isoformat().replace("+00:00", "Z")
    except (OverflowError, OSError, ValueError):
        return None


def _json_object(value: dict[str, Any]) -> dict[str, Any]:
    """Round-trip producer metadata to guarantee durable JSON values."""
    try:
        decoded = json.loads(json.dumps(value, default=str))
    except (TypeError, ValueError):
        return {}
    return cast(dict[str, Any], decoded) if isinstance(decoded, dict) else {}
