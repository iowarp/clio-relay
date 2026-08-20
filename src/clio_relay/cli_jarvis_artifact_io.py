"""JARVIS artifact record reading and decoding (iowarp/clio-relay#231
continuation): local- and remote-path artifact-kind readers and the
envelope decoder shared across the JARVIS execution-query engine."""

from __future__ import annotations

import base64
import binascii
import json
from json import JSONDecodeError
from typing import Any, cast

import clio_relay.cli_remote_collection_pagination as cli_remote_collection_pagination
import clio_relay.cli_remote_worker_probe as cli_remote_worker_probe
import clio_relay.core_queue as core_queue
from clio_relay.bounded_payload import describe_delivery_refusal, is_delivery_refusal
from clio_relay.cluster_config import (
    ClusterDefinition,
)
from clio_relay.errors import RelayError
from clio_relay.relay_ops import (
    read_artifact_bytes,
)


def _remote_artifact_records(
    definition: ClusterDefinition,
    job_id: str,
    *,
    deadline: float | None = None,
) -> list[dict[str, Any]]:
    return cli_remote_collection_pagination._complete_remote_collection(
        definition,
        ["job", "list-artifacts", job_id],
        record_key="artifacts",
        label=f"remote artifacts for {job_id}",
        deadline=deadline,
    )


def _artifact_record(
    artifacts: list[dict[str, Any]],
    *,
    kind: str,
) -> dict[str, Any] | None:
    matches = [artifact for artifact in artifacts if artifact.get("kind") == kind]
    if len(matches) > 1:
        raise RelayError(
            f"durable artifact authority is ambiguous: found {len(matches)} {kind} artifacts"
        )
    return matches[0] if matches else None


def _read_remote_json_artifact_kind(
    definition: ClusterDefinition,
    artifacts: list[dict[str, Any]],
    *,
    kind: str,
    deadline: float | None = None,
) -> dict[str, Any] | None:
    payload = _read_remote_artifact_kind_bytes(
        definition,
        artifacts,
        kind=kind,
        deadline=deadline,
    )
    return _decode_json_artifact(payload, kind=kind) if payload is not None else None


def _read_remote_artifact_kind_bytes(
    definition: ClusterDefinition,
    artifacts: list[dict[str, Any]],
    *,
    kind: str,
    deadline: float | None = None,
) -> bytes | None:
    """Read the exact remote artifact bytes recorded by the durable queue."""
    artifact = _artifact_record(artifacts, kind=kind)
    if artifact is None:
        return None
    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise RelayError(f"remote {kind} artifact has no artifact_id")
    envelope = cli_remote_collection_pagination._json_output(
        cli_remote_worker_probe._run_remote_clio_before_deadline(
            definition,
            ["job", "read-artifact", artifact_id],
            deadline=deadline,
        ),
        f"remote {kind} artifact payload",
    )
    return _decode_artifact_envelope(envelope)


def _read_local_json_artifact_kind(
    queue: core_queue.ClioCoreQueue,
    artifacts: list[dict[str, Any]],
    *,
    kind: str,
) -> dict[str, Any] | None:
    payload = _read_local_artifact_kind_bytes(queue, artifacts, kind=kind)
    return _decode_json_artifact(payload, kind=kind) if payload is not None else None


def _read_local_artifact_kind_bytes(
    queue: core_queue.ClioCoreQueue,
    artifacts: list[dict[str, Any]],
    *,
    kind: str,
) -> bytes | None:
    """Read the exact local artifact bytes recorded by the durable queue."""
    artifact = _artifact_record(artifacts, kind=kind)
    if artifact is None:
        return None
    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise RelayError(f"local {kind} artifact has no artifact_id")
    envelope = read_artifact_bytes(queue, artifact_id)
    return _decode_artifact_envelope(envelope)


def _decode_json_artifact(payload: bytes, *, kind: str) -> dict[str, Any]:
    try:
        decoded = json.loads(payload.decode("utf-8-sig"))
    except (UnicodeDecodeError, JSONDecodeError) as exc:
        raise RelayError(f"{kind} artifact must contain UTF-8 JSON") from exc
    if not isinstance(decoded, dict):
        raise RelayError(f"{kind} artifact must contain a JSON object")
    typed = cast(dict[object, object], decoded)
    return {str(key): value for key, value in typed.items()}


def _mcp_response_job_id(response: dict[str, Any] | None) -> str:
    if response is None:
        raise RelayError("virtual remote MCP call returned no JSON-RPC response")
    error = response.get("error")
    if isinstance(error, dict):
        typed_error = cast(dict[object, object], error)
        raise RelayError(f"virtual remote MCP call failed: {typed_error.get('message')}")
    result = response.get("result")
    if not isinstance(result, dict):
        raise RelayError("virtual remote MCP call returned no result object")
    structured = cast(dict[object, object], result).get("structuredContent")
    if not isinstance(structured, dict):
        raise RelayError("virtual remote MCP call returned no structuredContent")
    job_id = cast(dict[object, object], structured).get("job_id")
    if not isinstance(job_id, str) or not job_id:
        raise RelayError("virtual remote MCP call returned no durable job_id")
    return job_id


def _decode_artifact_envelope(envelope: dict[str, object]) -> bytes:
    if is_delivery_refusal(envelope):
        # F5 (#231 R6 review): a T2 refusal (doc §6.4) is not a malformed
        # envelope -- report the refusal's own message/code instead of the
        # generic "must use base64 encoding" below, which misdescribes why
        # the artifact is unavailable. Shared by every caller of this
        # function -- fixed once here for all callers, including
        # cli_remote_mcp.py's _read_remote_mcp_result_artifact /
        # _read_local_mcp_result_artifact, which reach this cli.py-resident
        # function via cli._decode_artifact_envelope.
        # A2 (#231 R6 review): the message extraction itself now delegates
        # to bounded_payload.describe_delivery_refusal, the single owner.
        code = cast(dict[str, object], envelope.get("delivery", {})).get("code")
        raise RelayError(
            f"artifact delivery refused ({code}): {describe_delivery_refusal(envelope)}"
        )
    if envelope.get("encoding") != "base64":
        raise RelayError("remote MCP result artifact must use base64 encoding")
    encoded = envelope.get("data")
    if not isinstance(encoded, str):
        raise RelayError("remote MCP result artifact data must be a base64 string")
    try:
        return base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise RelayError("remote MCP result artifact contains invalid base64") from exc
