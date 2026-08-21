"""Request-parameter coercion and the relay-owned JARVIS input-manifest contract.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3). Pure
leaf validation -- no facade reach-back needed.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, cast

from clio_relay.constants import _QUERY_CONTRACTS


def _required_str(params: dict[str, Any], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} is required")
    return value


def _required_optional_str(value: str | None, key: str) -> str:
    if value is None or not value:
        raise ValueError(f"{key} is required")
    return value


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError("tool must be a non-empty string")
    return value


def _operation(value: Any) -> str:
    if not isinstance(value, str) or value not in {"tools/call", "tools/list"}:
        raise ValueError("operation must be tools/call or tools/list")
    return value


def _object(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("arguments must be an object")
    return cast(dict[str, Any], value)


def _jarvis_input_manifest(
    value: Any,
    *,
    operation: str,
    tool: str | None,
    arguments: dict[str, Any],
    expected_registered_contract: str | None,
    expected_jarvis_cd_lock_binding: dict[str, str] | None,
) -> dict[str, Any] | None:
    """Validate one relay-owned resolved-input manifest before MCP launch."""
    if value is None:
        return None
    if (
        operation != "tools/call"
        or tool != "jarvis_run"
        or expected_registered_contract != _QUERY_CONTRACTS[-1]
        or expected_jarvis_cd_lock_binding is not None
        or not isinstance(value, dict)
    ):
        raise ValueError("JARVIS input manifest requires the registered jarvis_run contract")
    manifest = cast(dict[str, Any], value)
    expected_fields = {
        "schema_version",
        "route",
        "route_sha256",
        "idempotency_key",
        "resolutions",
        "artifact_uses",
        "manifest_sha256",
        "created_at",
        "document_sha256",
    }
    if (
        set(manifest) != expected_fields
        or manifest.get("schema_version") != "clio-relay.jarvis-run-input-manifest.v1"
    ):
        raise ValueError("JARVIS input manifest fields are invalid")
    route = manifest.get("route")
    expected_route_fields = {
        "schema_version",
        "cluster",
        "server_name",
        "contract",
        "cluster_route_revision",
        "registration_revision",
        "expected_server_artifact_digest",
        "pipeline_id",
        "owner_session_id",
        "owner_session_generation_id",
    }
    if not isinstance(route, dict):
        raise ValueError("JARVIS input manifest route is invalid")
    typed_route = cast(dict[str, Any], route)
    if (
        set(typed_route) != expected_route_fields
        or typed_route.get("schema_version") != "clio-relay.jarvis-pipeline-input-route.v1"
        or typed_route.get("contract") != _QUERY_CONTRACTS[-1]
        or typed_route.get("pipeline_id") != arguments.get("pipeline_id")
    ):
        raise ValueError("JARVIS input manifest route is invalid")
    route_sha256 = _canonical_json_sha256(typed_route)
    if manifest.get("route_sha256") != route_sha256:
        raise ValueError("JARVIS input manifest route checksum is invalid")
    if not isinstance(manifest.get("idempotency_key"), str) or not manifest["idempotency_key"]:
        raise ValueError("JARVIS input manifest idempotency key is invalid")
    raw_resolutions = manifest.get("resolutions")
    if not isinstance(raw_resolutions, list):
        raise ValueError("JARVIS input manifest resolutions are invalid")
    typed_resolutions = cast(list[object], raw_resolutions)
    if not typed_resolutions or len(typed_resolutions) > 1_000:
        raise ValueError("JARVIS input manifest resolutions are invalid")
    resolutions: list[dict[str, Any]] = []
    identities: list[tuple[str, str]] = []
    expected_uses: list[dict[str, Any]] = []
    for raw_resolution in typed_resolutions:
        if not isinstance(raw_resolution, dict):
            raise ValueError("JARVIS input manifest resolution fields are invalid")
        resolution = cast(dict[str, Any], raw_resolution)
        if set(resolution) != {
            "binding",
            "disposition",
            "previous_sha256",
        }:
            raise ValueError("JARVIS input manifest resolution fields are invalid")
        binding = resolution.get("binding")
        if not isinstance(binding, dict):
            raise ValueError("JARVIS input manifest binding fields are invalid")
        typed_binding = cast(dict[str, Any], binding)
        if set(typed_binding) != {
            "step_id",
            "canonical_setting",
            "accepted_names",
            "workspace_relative_path",
            "logical_name",
            "size_bytes",
            "sha256",
            "remote_path",
            "artifact_use",
        }:
            raise ValueError("JARVIS input manifest binding fields are invalid")
        step_id = typed_binding.get("step_id")
        setting = typed_binding.get("canonical_setting")
        accepted_names = typed_binding.get("accepted_names")
        relative_path = typed_binding.get("workspace_relative_path")
        remote_path = typed_binding.get("remote_path")
        sha256 = typed_binding.get("sha256")
        artifact_use = typed_binding.get("artifact_use")
        typed_accepted_names = (
            cast(list[object], accepted_names) if isinstance(accepted_names, list) else []
        )
        typed_artifact_use = (
            cast(dict[str, Any], artifact_use) if isinstance(artifact_use, dict) else {}
        )
        if (
            not isinstance(step_id, str)
            or not step_id
            or not isinstance(setting, str)
            or not setting
            or not isinstance(accepted_names, list)
            or not accepted_names
            or accepted_names[0] != setting
            or not all(isinstance(item, str) and item for item in typed_accepted_names)
            or len(typed_accepted_names) != len(set(cast(list[str], typed_accepted_names)))
            or not isinstance(relative_path, str)
            or not relative_path
            or "\\" in relative_path
            or relative_path.startswith("/")
            or any(part in {"", ".", ".."} for part in relative_path.split("/"))
            or not isinstance(remote_path, str)
            or not remote_path.startswith("/")
            or remote_path.startswith("//")
            or not _is_sha256(sha256)
            or not isinstance(artifact_use, dict)
            or typed_artifact_use.get("sha256") != sha256
        ):
            raise ValueError("JARVIS input manifest binding identity is invalid")
        previous_sha256 = resolution.get("previous_sha256")
        disposition = resolution.get("disposition")
        if not _is_sha256(previous_sha256) or (
            (disposition == "reused") != (previous_sha256 == sha256)
        ):
            raise ValueError("JARVIS input manifest resolution disposition is invalid")
        identities.append((step_id, setting))
        expected_uses.append(typed_artifact_use)
        resolutions.append(resolution)
    if identities != sorted(identities) or len(identities) != len(set(identities)):
        raise ValueError("JARVIS input manifest binding identities are not canonical")
    expected_uses.sort(key=lambda item: (str(item.get("artifact_id")), str(item.get("sha256"))))
    if manifest.get("artifact_uses") != expected_uses:
        raise ValueError("JARVIS input manifest artifact uses do not match its bindings")
    expected_manifest_sha256 = _canonical_json_sha256(
        {
            "route_sha256": route_sha256,
            "idempotency_key": manifest["idempotency_key"],
            "resolutions": resolutions,
            "artifact_uses": expected_uses,
        }
    )
    if manifest.get("manifest_sha256") != expected_manifest_sha256:
        raise ValueError("JARVIS input manifest resolution checksum is invalid")
    document = dict(manifest)
    document.pop("document_sha256")
    if manifest.get("document_sha256") != _canonical_json_sha256(document):
        raise ValueError("JARVIS input manifest document checksum is invalid")
    if (
        len(
            json.dumps(
                manifest,
                allow_nan=False,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        > 1 * 1024 * 1024
    ):
        raise ValueError("JARVIS input manifest exceeded its byte limit")
    return manifest


def _is_sha256(value: object) -> bool:
    """Return whether a value is one canonical lowercase SHA-256 digest."""
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _canonical_json_sha256(value: object) -> str:
    """Return the SHA-256 of one finite canonical JSON value."""
    return hashlib.sha256(
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _str_list(value: Any, *, key: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{key} must be a string array")
    items = cast(list[object], value)
    if not all(isinstance(item, str) for item in items):
        raise ValueError(f"{key} must be a string array")
    return [item for item in items if isinstance(item, str)]


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_sha256(value: Any, *, key: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a SHA-256 string")
    normalized = value.lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{key} must be a SHA-256 string")
    return normalized
