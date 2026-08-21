"""Cross-domain constants, identity helpers, and canonical-JSON primitives.

Every other ``models_*`` owner module depends on this one: the durable
record-id constructors (:func:`utc_now`, :func:`new_id`), the credential/
metadata-key constants, the MCP env-passthrough validator, and the
canonical-JSON hashing/size-bound helpers used across artifact provenance,
JARVIS input records, and MCP task projections. Nothing here depends on any
other ``models_*`` module, so it is safe for all of them to import from.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import cast
from uuid import uuid4

from clio_relay.identifiers import validate_durable_record_id

RELAY_CREDENTIAL_ENV_NAMES = frozenset(
    {
        "CLIO_RELAY_API_TOKEN",
        "CLIO_RELAY_FRP_TOKEN",
        "CLIO_RELAY_PROGRESS_TOKEN",
        "CLIO_RELAY_RUNTIME_METADATA_TOKEN",
        "CLIO_RELAY_STCP_SECRET",
    }
)
MCP_ADMISSION_AUTHORITY_METADATA_KEY = "mcp_admission_authority"
INPUT_INGEST_POLICY_METADATA_KEY = "input_ingest_policy"
CLIO_PROVENANCE_METADATA_KEY = "clio.provenance.v1"
MAX_ARTIFACT_USE_PROVENANCE_BYTES = 8 * 1024
MAX_ARTIFACT_USE_AGGREGATE_BYTES = 256 * 1024
MAX_JARVIS_PACKAGE_INPUT_CONTRACT_BYTES = 256 * 1024
MAX_JARVIS_PIPELINE_INPUT_BINDINGS_BYTES = 1 * 1024 * 1024
MAX_JARVIS_RUN_INPUT_MANIFEST_BYTES = 1 * 1024 * 1024
MAX_TRANSFORM_ENVIRONMENT_BYTES = 16 * 1024
MAX_TRANSFORM_REF_BYTES = 192 * 1024
MAX_TRANSFORM_USED_EVIDENCE = 1_000
MAX_MCP_TASK_ARGUMENT_BYTES = 512 * 1024
MAX_MCP_TASK_INPUT_ROUND_BYTES = 256 * 1024
MAX_MCP_TASK_PROJECTION_BYTES = 768 * 1024
MAX_MCP_TASK_JSON_DEPTH = 64
MAX_MCP_TASK_JSON_NODES = 100_000
REGISTERED_JARVIS_USER_CONTRACT = "clio-kit-jarvis-user-v3.7.2"
REGISTERED_JARVIS_EXECUTION_CONTRACTS = frozenset(
    {"clio-kit-jarvis-user-v3.6", REGISTERED_JARVIS_USER_CONTRACT}
)


def validate_mcp_env_from(value: dict[str, str]) -> dict[str, str]:
    """Validate child-to-source environment references without resolving values."""
    for child_name, source_name in value.items():
        if not _valid_environment_name(child_name) or not _valid_environment_name(source_name):
            raise ValueError("MCP env_from keys and values must be environment names")
        forbidden = {
            name
            for name in (child_name, source_name)
            if name in RELAY_CREDENTIAL_ENV_NAMES
            or (
                name.startswith("CLIO_RELAY_")
                and (name.endswith("_TOKEN") or name.endswith("_SECRET"))
            )
        }
        if forbidden:
            credential = sorted(forbidden)[0]
            raise ValueError(f"MCP env_from cannot expose relay credential {credential}")
    return value


def _valid_environment_name(value: str) -> bool:
    return (
        bool(value)
        and (value[0].isalpha() or value[0] == "_")
        and all(character.isalnum() or character == "_" for character in value)
    )


def utc_now() -> datetime:
    """Return an aware UTC timestamp."""
    return datetime.now(UTC)


def new_id(prefix: str) -> str:
    """Create a readable portable relay identifier."""
    return validate_durable_record_id(f"{prefix}_{uuid4().hex}")


def _canonical_json_sha256(value: object) -> str:
    """Hash one finite canonical JSON value."""
    return hashlib.sha256(
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _canonical_json_bytes(value: object) -> bytes:
    """Serialize one finite JSON value deterministically for size enforcement."""
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("provenance must contain finite JSON values") from exc


def _require_canonical_json_size(value: object, *, label: str, maximum: int) -> None:
    """Reject a provenance document whose canonical UTF-8 encoding is oversized."""
    if len(_canonical_json_bytes(value)) > maximum:
        raise ValueError(f"{label} exceeds {maximum} UTF-8 bytes")


def _require_bounded_mcp_task_json(
    value: object,
    *,
    label: str,
    maximum_bytes: int,
) -> None:
    """Require one finite, bounded JSON tree before durable task persistence."""
    stack: list[tuple[object, int]] = [(value, 0)]
    nodes = 0
    while stack:
        current, depth = stack.pop()
        nodes += 1
        if nodes > MAX_MCP_TASK_JSON_NODES:
            raise ValueError(f"{label} exceeds {MAX_MCP_TASK_JSON_NODES} JSON nodes")
        if depth > MAX_MCP_TASK_JSON_DEPTH:
            raise ValueError(f"{label} exceeds {MAX_MCP_TASK_JSON_DEPTH} nesting levels")
        if isinstance(current, dict):
            for key, item in cast(dict[object, object], current).items():
                if not isinstance(key, str):
                    raise ValueError(f"{label} contains a non-string JSON object key")
                stack.append((item, depth + 1))
        elif isinstance(current, list):
            stack.extend((item, depth + 1) for item in cast(list[object], current))
        elif not isinstance(current, str | int | float | bool | None):
            raise ValueError(f"{label} contains a non-JSON value")
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must contain finite JSON values") from exc
    if len(encoded) > maximum_bytes:
        raise ValueError(f"{label} exceeds {maximum_bytes} UTF-8 bytes")
