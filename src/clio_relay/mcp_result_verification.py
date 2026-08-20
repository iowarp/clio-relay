"""MCP result decoding/verification and terminal-evidence shaping: reading
and SHA-verifying a job's durable `mcp_result` artifact (local,
remote-CLI, or owned-session), the bounded/redacted public projection of a
verified result, and the terminal-evidence block every submission and
status/wait tool attaches to its receipt.

Split out of mcp_server.py (iowarp/clio-relay#231) as the other half of
the result-verification/artifact-completion cluster's real seam split (see
mcp_remote_transport.py's own docstring). Needs three names from that
other half: `_owned_json` (not monkeypatched -- a plain cross-module
import) and `_remote_json`/`_complete_local_artifacts` (both directly
monkeypatched by tests, so `_verified_mcp_result` and
`_verified_local_mcp_result` reach them through the function-scope
`_mcp_server.<name>(...)` back-reference regardless of which module
originally defined them -- the split doesn't change that requirement,
only where the plain, unpatched dependency `_owned_json` comes from).

`_VerifiedMcpResult`, the small frozen dataclass every function here
returns, moves here too -- it belongs to this cluster semantically.
mcp_server.py's own remaining use of it
(`_owned_session_submission_result`'s `parsed_result` parameter) is a type
annotation only, so mcp_server.py's plain cross-module import back needs
no back-reference -- it is not monkeypatched, only re-exported, the same
"used elsewhere via plain import" pattern as mcp_arguments.py's
MAX_AGENT_LOG_READ_BYTES in slices 1-2 -- but tests also construct
`mcp_server_module._VerifiedMcpResult(...)` directly, so the import must
be real, not TYPE_CHECKING-only.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any, cast

from pydantic import ValidationError

from clio_relay.bounded_payload import (
    build_delivery_refusal,
    is_delivery_refusal,
    is_delivery_refusal_failed,
)
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.jarvis_mcp import render_virtual_jarvis_agent_context
from clio_relay.jarvis_service_runtime import (
    JARVIS_SERVICE_RUNTIME_SCHEMA_V2,
    JarvisServiceAuthorization,
    derive_jarvis_service_runtime_handoffs,
)
from clio_relay.mcp_remote_catalog import _bound_virtual_jarvis_clusters
from clio_relay.mcp_remote_transport import _owned_json
from clio_relay.models import ArtifactRef, JobKind, JobState, McpCallSpec, RelayJob
from clio_relay.relay_ops import read_artifact_bytes
from clio_relay.remote_mcp import VirtualRemoteMcpCatalog
from clio_relay.session_api import OwnedSessionApiClient
from clio_relay.validation_report import redact_sensitive_values

JSON = dict[str, Any]

MAX_INLINE_MCP_RESULT_BYTES = 65_536
MCP_RESULT_INLINE_LIMIT_CODE = "inline_result_limit_exceeded"
MCP_RESULT_INLINE_LIMIT_MESSAGE = (
    "The remote MCP operation reached a terminal state, but its result exceeded the safe "
    "inline response limit and is unavailable to the agent. Immutable private evidence was "
    "preserved for operator diagnosis. Remote side effects may have occurred; inspect the "
    "job before retrying."
)


@dataclass(frozen=True)
class _VerifiedMcpResult:
    """SHA-verified full MCP artifact plus its bounded public projection."""

    document: JSON
    public: JSON


def _verified_mcp_result(
    definition: ClusterDefinition,
    job_id: str,
    artifacts: list[JSON],
) -> _VerifiedMcpResult | None:
    from clio_relay import mcp_server as _mcp_server

    artifact = next(
        (
            item
            for item in artifacts
            if item.get("kind") == "mcp_result" and item.get("job_id") == job_id
        ),
        None,
    )
    if artifact is None:
        return None
    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise ValueError("remote MCP result artifact has no artifact_id")
    envelope = _mcp_server._remote_json(
        definition,
        ["job", "read-artifact", artifact_id],
        "remote MCP result artifact",
    )
    return _decode_verified_mcp_result(envelope, artifact=artifact, job_id=job_id)


def _owned_mcp_result_is_required(job: RelayJob) -> bool:
    """Whether a succeeded job's worker is expected to have written an mcp_result.

    True for any MCP_CALL job that reached ``SUCCEEDED``, ``WORKLOAD`` and
    ``CONTROL_QUERY`` admission classes alike: the worker unconditionally
    writes the ``mcp_result`` artifact before marking success (D17), so a
    missing one there is always a defect (D14/D15), never a legitimate
    state.

    C2 (review): this used to exempt ``CONTROL_QUERY`` on the theory that it
    is "answered outside the ordinary spooled worker/artifact pipeline" --
    false against live evidence. ``CONTROL_QUERY`` is only a worker-lane and
    admission-cap distinction (``endpoint.py``'s ``_serve_worker_slots``
    builds an identical ``EndpointWorker`` per lane; ``core_queue.py`` uses
    it solely to pick the concurrency ceiling); the SAME spooled
    worker/artifact pipeline runs both classes. A live production
    ``jarvis_describe``/``jarvis_get_execution`` job -- always admitted as
    CONTROL_QUERY, since the owned session path requires the pinned
    read-only ``expected_server_artifact_digest`` that triggers that
    admission class -- carries its own ``mcp_result`` artifact just like any
    WORKLOAD job. The old exclusion silently exempted exactly the two
    curated read operations this guard exists to protect.
    """
    return isinstance(job.spec, McpCallSpec) and job.state is JobState.SUCCEEDED


def _verified_owned_mcp_result(
    client: OwnedSessionApiClient,
    job_id: str,
    artifacts: list[JSON],
    *,
    require_result: bool = False,
) -> _VerifiedMcpResult | None:
    """Find and verify one job's durable MCP result artifact, if present.

    ``require_result`` is set by callers that already know this job kind
    always produces an ``mcp_result`` artifact on success (an MCP_CALL job
    that reached ``SUCCEEDED``): for them, a missing artifact is never a
    legitimate "nothing to attach" case (D14/D15/D17) -- it means a
    succeeded job would otherwise report a bounded receipt with no result
    and ``isError`` left false, indistinguishable from real success. Raise
    loud and typed instead of returning a bare ``None`` a caller could
    silently accept. Ordinary callers observing a job of unknown/mixed kind
    (e.g. ``relay_wait`` on any job) leave this ``False`` and keep the
    existing silent-None contract for job kinds that never have one.
    """
    artifact = next(
        (
            item
            for item in artifacts
            if item.get("kind") == "mcp_result" and item.get("job_id") == job_id
        ),
        None,
    )
    if artifact is None:
        if require_result:
            raise ValueError(
                f"job {job_id} succeeded but no mcp_result artifact was found among "
                f"{len(artifacts)} indexed artifact(s)"
            )
        return None
    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise ValueError("owned remote MCP result artifact has no artifact_id")
    envelope = _owned_json(
        client,
        method="GET",
        path=f"/artifacts/{artifact_id}/content",
        label="owned remote MCP result artifact",
    )
    return _decode_verified_mcp_result(envelope, artifact=artifact, job_id=job_id)


def _verified_local_mcp_result(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    artifacts: list[JSON] | None = None,
) -> _VerifiedMcpResult | None:
    from clio_relay import mcp_server as _mcp_server

    artifact_records = (
        artifacts if artifacts is not None else _mcp_server._complete_local_artifacts(queue, job_id)
    )
    artifact = next(
        (
            item
            for item in artifact_records
            if item.get("kind") == "mcp_result" and item.get("job_id") == job_id
        ),
        None,
    )
    if artifact is None:
        return None
    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise ValueError("local MCP result artifact has no artifact_id")
    envelope = cast(JSON, read_artifact_bytes(queue, artifact_id))
    if is_delivery_refusal(envelope):
        # T2 (doc §6.4): the durable mcp_result artifact itself exceeded
        # relay_ops.MAX_ARTIFACT_CONTENT_BYTES -- read_artifact_bytes already
        # refused delivery with a typed document instead of raising. Surface
        # that refusal as-is rather than let it fall into
        # _decode_verified_mcp_result, which expects a base64 envelope and
        # would otherwise misreport this as a generic malformed-artifact
        # ValueError.
        return _VerifiedMcpResult(document=envelope, public=envelope)
    return _decode_verified_mcp_result(
        envelope,
        artifact=artifact,
        job_id=job_id,
    )


def _decode_verified_mcp_result(
    envelope: JSON,
    *,
    artifact: JSON,
    job_id: str,
) -> _VerifiedMcpResult:
    envelope_artifact = envelope.get("artifact")
    if not isinstance(envelope_artifact, dict):
        raise ValueError("MCP result artifact envelope is missing durable metadata")
    typed_envelope_artifact = cast(JSON, envelope_artifact)
    for key in ("artifact_id", "job_id", "sha256"):
        if typed_envelope_artifact.get(key) != artifact.get(key):
            raise ValueError(f"MCP result artifact envelope {key} does not match durable metadata")
    if artifact.get("job_id") != job_id:
        raise ValueError("MCP result artifact belongs to a different job")
    expected_sha256 = artifact.get("sha256")
    encoded = envelope.get("data")
    if not isinstance(expected_sha256, str) or len(expected_sha256) != 64:
        raise ValueError("MCP result artifact has no valid SHA-256")
    if envelope.get("encoding") != "base64" or not isinstance(encoded, str):
        raise ValueError("MCP result artifact envelope must contain base64 data")
    try:
        payload = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise ValueError("MCP result artifact contains invalid base64") from exc
    observed_sha256 = hashlib.sha256(payload).hexdigest()
    if not hmac.compare_digest(observed_sha256, expected_sha256):
        raise ValueError("MCP result artifact SHA-256 does not match durable metadata")
    try:
        document = json.loads(payload.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("MCP result artifact must contain UTF-8 JSON") from exc
    if not isinstance(document, dict):
        raise ValueError("MCP result artifact must contain a JSON object")
    typed = cast(JSON, document)
    public = {
        key: typed.get(key)
        for key in (
            "operation",
            "tool",
            "returncode",
            "timed_out",
            "protocol_error",
            "structured_result",
            "protocol_result",
            "protocol_version",
            "server_info",
            "result_validation",
        )
    }
    return _VerifiedMcpResult(document=typed, public=public)


def _mcp_result_artifact(artifacts: list[JSON], *, job_id: str) -> JSON | None:
    """Return the unique durable MCP-result artifact for one job, if present."""

    matches = [
        artifact
        for artifact in artifacts
        if artifact.get("job_id") == job_id and artifact.get("kind") == "mcp_result"
    ]
    if len(matches) > 1:
        raise ValueError(f"job {job_id} has multiple MCP result artifacts")
    return matches[0] if matches else None


def _bounded_mcp_result(
    result: JSON,
    *,
    preserve_verified_jarvis_authorization_descriptors: bool = False,
) -> JSON:
    """Return a bounded agent projection while the artifact retains full protocol evidence."""

    original = copy.deepcopy(result)
    sanitized = redact_sensitive_values(original)
    if not isinstance(sanitized, dict):
        raise ValueError("MCP result redaction did not preserve its object shape")
    projected = cast(JSON, sanitized)
    if preserve_verified_jarvis_authorization_descriptors:
        _restore_jarvis_service_authorization_descriptors(
            original=original,
            projected=projected,
        )
    sensitive_values_redacted = projected != result
    protocol_result = projected.get("protocol_result")
    protocol_result_is_error = (
        isinstance(protocol_result, dict)
        and cast(dict[str, object], protocol_result).get("isError") is True
    )
    if (
        projected.get("structured_result") is not None
        and "protocol_result" in projected
        and not protocol_result_is_error
    ):
        projected.pop("protocol_result")
        projected["protocol_result_omitted"] = "redundant_with_structured_result"
    if sensitive_values_redacted:
        projected["sensitive_values_redacted"] = True

    encoded = json.dumps(
        projected,
        allow_nan=False,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(encoded) <= MAX_INLINE_MCP_RESULT_BYTES:
        return projected

    # Arbitrary MCP output has no generic, safe pagination or redaction contract.
    # Returning a successful-looking partial projection would lose the only
    # agent-readable result, while exposing selected fields could disclose
    # application-defined secrets. Keep the full artifact private and fail the
    # MCP delivery explicitly without changing the immutable remote job state.
    # F7 (#231 R6 review): built through bounded_payload.build_delivery_refusal
    # -- the T2 precedent this function originated -- instead of its own
    # inline dict literal, so every raw payload path shares one constructor
    # (single owner, the slice's own rule).
    failure: JSON = build_delivery_refusal(
        code=MCP_RESULT_INLINE_LIMIT_CODE,
        message=MCP_RESULT_INLINE_LIMIT_MESSAGE,
        max_bytes=MAX_INLINE_MCP_RESULT_BYTES,
        remote_side_effects_may_have_occurred=True,
    )
    if sensitive_values_redacted:
        failure["sensitive_values_redacted"] = True
    return failure


def _restore_jarvis_service_authorization_descriptors(
    *,
    original: JSON,
    projected: JSON,
) -> None:
    """Restore only exact public JARVIS bearer fingerprints after source verification.

    The caller enables this only after the durable MCP artifact has passed the
    exact built-in or registered JARVIS contract validation. This second,
    deliberately narrow check accepts only service-runtime v2 authorization
    descriptors with the strict public shape defined by JARVIS. Raw bearer
    capabilities, additional keys, and malformed digests remain redacted.
    """

    original_runtimes = _jarvis_service_runtime_items(original)
    projected_runtimes = _jarvis_service_runtime_items(projected)
    if original_runtimes is None or projected_runtimes is None:
        return
    if len(original_runtimes) != len(projected_runtimes):
        return
    for original_runtime, projected_runtime in zip(
        original_runtimes,
        projected_runtimes,
        strict=True,
    ):
        if (
            original_runtime.get("schema_version") != JARVIS_SERVICE_RUNTIME_SCHEMA_V2
            or projected_runtime.get("schema_version") != JARVIS_SERVICE_RUNTIME_SCHEMA_V2
        ):
            continue
        raw_authorization = original_runtime.get("authorization")
        try:
            authorization = JarvisServiceAuthorization.model_validate(raw_authorization)
        except ValidationError:
            continue
        descriptor = authorization.model_dump(mode="json")
        if raw_authorization != descriptor:
            continue
        projected_runtime["authorization"] = descriptor


def _jarvis_service_runtime_items(result: JSON) -> list[JSON] | None:
    """Return the exact execution-v2 service-runtime list, if structurally present."""

    structured = result.get("structured_result")
    if not isinstance(structured, dict):
        return None
    snapshot = cast(dict[str, object], structured).get("service_runtimes")
    if not isinstance(snapshot, dict):
        return None
    raw_runtimes = cast(dict[str, object], snapshot).get("service_runtimes")
    if not isinstance(raw_runtimes, list):
        return None
    typed_runtimes = cast(list[object], raw_runtimes)
    if not all(isinstance(runtime, dict) for runtime in typed_runtimes):
        return None
    return [cast(JSON, runtime) for runtime in typed_runtimes]


def _mcp_tool_result_failed(result: JSON) -> bool:
    """Keep failed terminal remote MCP operations failed at the agent tool boundary."""

    if is_delivery_refusal_failed(result):
        # F5 (#231 R6 review): a tool's OWN result document can be a T2
        # refusal directly (e.g. relay_read_artifact/_read_model_artifact_
        # bytes reading a too-large artifact) -- not only nested under
        # mcp_result below (relay_wait's job-status shape).
        return True

    if (
        result.get("kind") == JobKind.MCP_CALL.value
        and result.get("terminal") is True
        and result.get("state") in {JobState.FAILED.value, JobState.CANCELED.value}
    ):
        return True

    mcp_result = result.get("mcp_result")
    if not isinstance(mcp_result, dict):
        return False
    typed_result = cast(dict[str, object], mcp_result)
    returncode = typed_result.get("returncode")
    if (
        typed_result.get("timed_out") is True
        or isinstance(typed_result.get("protocol_error"), str)
        or (isinstance(returncode, int) and not isinstance(returncode, bool) and returncode != 0)
    ):
        return True
    protocol_result = typed_result.get("protocol_result")
    if (
        isinstance(protocol_result, dict)
        and cast(dict[str, object], protocol_result).get("isError") is True
    ):
        return True
    return is_delivery_refusal_failed(typed_result)


def _public_mcp_result_artifact(artifact: JSON) -> JSON:
    """Return the compact immutable binding for a durable MCP result artifact."""

    return {
        key: artifact.get(key)
        for key in (
            "artifact_id",
            "job_id",
            "kind",
            "size_bytes",
            "sha256",
            "created_at",
        )
    }


def _attach_terminal_mcp_evidence(
    receipt: JSON,
    *,
    source_job: RelayJob,
    last_error: str | None,
    artifacts: list[JSON],
    parsed_result: _VerifiedMcpResult | None,
) -> None:
    """Attach bounded terminal MCP evidence to a waited submission receipt."""

    receipt["last_error"] = last_error
    if parsed_result is None:
        return
    artifact = _mcp_result_artifact(artifacts, job_id=source_job.job_id)
    if artifact is None:
        raise ValueError(f"verified MCP result for {source_job.job_id} has no durable artifact")
    receipt["mcp_result_artifact"] = _public_mcp_result_artifact(artifact)
    verified_jarvis_authorization_descriptors = False
    if (
        source_job.state is JobState.SUCCEEDED
        and isinstance(source_job.spec, McpCallSpec)
        and source_job.spec.tool == "jarvis_get_execution"
        and source_job.spec.arguments.get("include_service_runtimes") is True
    ):
        source_artifact = ArtifactRef.model_validate(artifact)
        service_runtime_handoffs = derive_jarvis_service_runtime_handoffs(
            cluster=source_job.cluster,
            source_job=source_job,
            source_artifact=source_artifact,
            document=parsed_result.document,
        )
        receipt["service_runtime_bindings"] = [
            handoff.model_dump(mode="json") for handoff in service_runtime_handoffs
        ]
        verified_jarvis_authorization_descriptors = True
    receipt["mcp_result"] = _bounded_mcp_result(
        parsed_result.public,
        preserve_verified_jarvis_authorization_descriptors=(
            verified_jarvis_authorization_descriptors
        ),
    )


def _render_remote_mcp_context(catalog: VirtualRemoteMcpCatalog) -> str:
    jarvis_clusters = _bound_virtual_jarvis_clusters(catalog)
    jarvis = (
        render_virtual_jarvis_agent_context()
        + " Built-in virtual JARVIS tools are available on: "
        + ", ".join(jarvis_clusters)
        + "."
        if jarvis_clusters
        else (
            "Built-in virtual JARVIS tools are not advertised because no configured cluster "
            "has a verified JARVIS MCP artifact binding. Use an available registered remote "
            "MCP alias or ask an operator to refresh the built-in JARVIS discovery."
        )
    )
    generic = (
        " Registered remote MCP tools are exposed with remote_<server>_<tool> aliases; "
        "their cluster argument selects the execution target and is not forwarded to the "
        "remote tool. Operators explicitly refresh the durable schema cache before new or "
        "changed tools appear. Treat cluster, job_id, and the opaque 64-character "
        "route_revision returned by one submission as an indivisible handle. A route "
        "revision is never interchangeable with this tool catalog's revision or a "
        "scientific dataset's catalog revision."
    )
    available = ""
    if catalog.tools:
        available = " Available registered aliases: " + ", ".join(sorted(catalog.tools)) + "."
    return jarvis + generic + available
