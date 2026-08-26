"""Authenticated client for one exact owned relay session API.

Every operation here rides the connection-scoped channel the local relay
established once, at bring-up, for this cluster.  This module owns owned-session
*semantics* -- submission receipts, transforms, identity documents -- and never
owns transport: see :mod:`clio_relay.control_channel` for the held link and
:mod:`clio_relay.remote_connection` for the connection that holds it.
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Final, cast

from pydantic import ValidationError

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.control_channel import ChannelDropped
from clio_relay.errors import RelayError
from clio_relay.jarvis_mcp import is_virtual_jarvis_control_query
from clio_relay.models import (
    MCP_ADMISSION_AUTHORITY_METADATA_KEY,
    REGISTERED_JARVIS_USER_CONTRACT,
    ArtifactUse,
    JarvisRunSpec,
    JobKind,
    McpAdmissionAuthority,
    McpAdmissionClass,
    McpCallSpec,
    McpControlQueryEvidence,
    McpOperation,
    RelayJob,
    RemoteAgentTaskSpec,
    TransformRef,
    deterministic_jarvis_execution_id,
    is_owned_jarvis_run_spec,
)
from clio_relay.remote_connection import (
    MAX_SESSION_API_RESPONSE_BYTES,
    RemoteConnection,
    RemoteConnectionRegistry,
    connection_registry,
    validate_channel_request,
)
from clio_relay.remote_mcp import resolve_pinned_mcp_admission

SESSION_IDENTITY_SCHEMA: Final = "clio-relay.session-identity.v1"
__all__ = [
    "MAX_SESSION_API_RESPONSE_BYTES",
    "OWNED_SESSION_WAIT_RESPONSE_GRACE_SECONDS",
    "SESSION_IDENTITY_SCHEMA",
    "ChannelReconnectRequired",
    "OwnedSessionApiClient",
    "get_owned_session_transform",
    "record_owned_session_transform",
    "request_owned_session_json",
    "session_identity_document",
    "submit_owned_session_job",
]


class ChannelReconnectRequired(RelayError):
    """The held owned-session channel dropped; only an explicit, user-authorized
    reconnect may replace it (iowarp/clio-relay#276 B2).

    Raised at the session-API boundary -- :class:`OwnedSessionApiClient`'s
    ``__enter__`` (bring-up/resume) and ``request_json`` (an in-flight
    operation that discovers the drop) -- whenever a
    :class:`~clio_relay.control_channel.ChannelDropped` reaches an
    owned-session operation. Per the 2FA operating assumption
    (docs/connection-model.md:141-157) this is never turned into a redial
    here, or anywhere upstream of an explicit user action: the connection's
    own :attr:`~clio_relay.remote_connection.RemoteConnection.state` is
    already ``"authorization_required"`` by the time this is raised (the
    channel-level ``dropped``/``reestablishing`` events already recorded the
    typed transition), and the only way past it is the single authorized
    reconnect ``clio-relay session attach``/``session reconnect`` performs.
    """

    reason = "authorization_required"

    def __init__(self, *, cluster: str, source: ChannelDropped) -> None:
        self.cluster = cluster
        self.source = source
        super().__init__(
            f"owned session channel for {cluster!r} dropped; run `clio-relay session "
            f"attach --cluster {cluster}` (or `session reconnect`) to authorize exactly "
            "one new transport -- it is never redialed automatically"
        )


# Leave part of clio-agent's ordinary 30-second transport budget available for
# propagating the completed MCP result after this inner long-poll returns.
OWNED_SESSION_WAIT_RESPONSE_GRACE_SECONDS: Final = 10.0

_JOB_SUBMISSION_PATHS = frozenset(
    {
        "/jobs/jarvis",
        "/jobs/jarvis-pipeline",
        "/jobs/remote-agent",
        "/jobs/mcp-call",
        "/jobs/jarvis-mcp-call",
    }
)


class OwnedSessionApiClient:
    """Identity-proven client for one exact owned session generation.

    This is a handle onto the channel the local relay already holds for the
    cluster, not a transport of its own.  Entering it costs no new connection:
    the channel was established once, at connection bring-up, and every
    owned-session operation -- status, identity, submission, ingest, artifact
    content, watch -- rides that one link.  Leaving the context releases the
    handle and leaves the channel held for the next operation.
    """

    def __init__(
        self,
        *,
        definition: ClusterDefinition,
        settings: RelaySettings,
        timeout_seconds: float = 30.0,
        registry: RemoteConnectionRegistry | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._definition = definition
        self._settings = settings
        self._timeout_seconds = timeout_seconds
        self._registry = registry
        self._connection: RemoteConnection | None = None

    def __enter__(self) -> OwnedSessionApiClient:
        """Bind to the held channel for this cluster, establishing it once.

        clio-relay#276 B2: a channel that dropped since it was last held
        surfaces here as :class:`ChannelDropped` (``RemoteConnection.
        connect()``'s own refusal) -- caught and re-raised as the typed
        :class:`ChannelReconnectRequired` so every session-API caller sees
        the same actionable, typed condition instead of a bare internal
        transport exception. Never redialed here.
        """
        registry = self._registry or connection_registry()
        try:
            self._connection = registry.connection(
                definition=self._definition,
                settings=self._settings,
                timeout_seconds=self._timeout_seconds,
            )
        except ChannelDropped as exc:
            raise ChannelReconnectRequired(cluster=self._definition.name, source=exc) from exc
        return self

    def __exit__(self, *_args: object) -> None:
        """Release the handle; the connection keeps its channel held."""
        self._connection = None

    @property
    def connection(self) -> RemoteConnection:
        """Return the held connection backing this handle."""
        connection = self._connection
        if connection is None:
            raise RuntimeError("owned session API client is not open")
        return connection

    def request_json(
        self,
        *,
        method: str,
        path: str,
        query: dict[str, object] | None = None,
        body: dict[str, object] | None = None,
        response_timeout_seconds: float | None = None,
    ) -> object:
        """Issue one authenticated JSON request over the held channel.

        ``response_timeout_seconds`` changes only the response deadline for this
        request. It never relaxes the channel's bring-up deadlines, and the
        stream's ordinary timeout is restored before a later request reuses it.

        clio-relay#276 B2: a channel that dropped mid-flight (discovered by
        the connection's own stream acquisition, not by this call) surfaces
        as :class:`ChannelDropped` here too, and is re-raised the same typed
        way as ``__enter__`` -- see :class:`ChannelReconnectRequired`.
        """
        try:
            return self.connection.request_json(
                method=method,
                path=path,
                query=query,
                body=body,
                response_timeout_seconds=response_timeout_seconds,
            )
        except ChannelDropped as exc:
            raise ChannelReconnectRequired(cluster=self.connection.cluster, source=exc) from exc


def submit_owned_session_job(
    *,
    definition: ClusterDefinition,
    settings: RelaySettings,
    path: str,
    payload: dict[str, object],
    timeout_seconds: float = 30.0,
) -> RelayJob:
    """Submit one job through an authenticated, exact-generation remote session API."""
    if path not in _JOB_SUBMISSION_PATHS:
        raise ValueError(f"unsupported owned session submission path: {path}")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if payload.get("cluster") != definition.name:
        raise ValueError("owned session submission cluster does not match the selected route")
    document = request_owned_session_json(
        definition=definition,
        settings=settings,
        method="POST",
        path=path,
        body=payload,
        timeout_seconds=timeout_seconds,
    )
    try:
        if not isinstance(document, dict):
            raise TypeError("response is not a JSON object")
        job = RelayJob.model_validate(cast(dict[str, object], document))
    except (TypeError, ValueError, ValidationError) as exc:
        raise RelayError("owned session API returned an invalid relay job") from exc
    session_id = settings.owner_session_id
    generation_id = settings.owner_session_generation_id
    assert session_id is not None and generation_id is not None
    if job.cluster != definition.name:
        raise RelayError("owned session API returned a job for a different cluster")
    _validate_submission_receipt(job, path=path, payload=payload)
    if (
        job.owner_session_id != session_id
        or job.owner_session_generation_id != generation_id
        or job.metadata.get("owner") != "clio-relay"
        or job.metadata.get("owner_session_id") != session_id
        or job.metadata.get("owner_session_generation_id") != generation_id
        or "owner_session_admission_id" in job.metadata
    ):
        raise RelayError("owned session API returned a job without exact server-stamped ownership")
    expected_digest = payload.get("expected_server_artifact_digest")
    if expected_digest is not None and (
        not isinstance(job.spec, McpCallSpec)
        or job.spec.expected_server_artifact_digest != expected_digest
    ):
        raise RelayError("owned session API did not retain the expected MCP artifact binding")
    return job


def record_owned_session_transform(
    *,
    definition: ClusterDefinition,
    settings: RelaySettings,
    transform: TransformRef,
    timeout_seconds: float = 30.0,
) -> TransformRef:
    """Record one immutable transform through an exact-generation session API."""
    document = request_owned_session_json(
        definition=definition,
        settings=settings,
        method="POST",
        path=f"/jobs/{transform.job_id}/transform",
        body=cast(dict[str, object], transform.model_dump(mode="json")),
        timeout_seconds=timeout_seconds,
    )
    try:
        recorded = TransformRef.model_validate(document)
    except ValidationError as exc:
        raise RelayError("owned session API returned an invalid transform ref") from exc
    if recorded != transform:
        raise RelayError("owned session API did not retain the exact transform ref")
    return recorded


def get_owned_session_transform(
    *,
    definition: ClusterDefinition,
    settings: RelaySettings,
    job_id: str,
    timeout_seconds: float = 30.0,
) -> TransformRef | None:
    """Read one nullable transform through an exact-generation session API."""
    document = request_owned_session_json(
        definition=definition,
        settings=settings,
        method="GET",
        path=f"/jobs/{job_id}/transform",
        timeout_seconds=timeout_seconds,
    )
    if document is None:
        return None
    try:
        transform = TransformRef.model_validate(document)
    except ValidationError as exc:
        raise RelayError("owned session API returned an invalid transform ref") from exc
    if transform.job_id != job_id:
        raise RelayError("owned session API returned a transform for a different job")
    return transform


def request_owned_session_json(
    *,
    definition: ClusterDefinition,
    settings: RelaySettings,
    method: str,
    path: str,
    query: dict[str, object] | None = None,
    body: dict[str, object] | None = None,
    timeout_seconds: float = 30.0,
) -> object:
    """Call one exact-generation session API over the connection's held channel."""
    validate_channel_request(method=method, path=path)
    with OwnedSessionApiClient(
        definition=definition,
        settings=settings,
        timeout_seconds=timeout_seconds,
    ) as client:
        return client.request_json(
            method=method,
            path=path,
            query=query,
            body=body,
        )


def session_identity_document(
    *,
    owner_token: str,
    cluster: str,
    session_id: str,
    generation_id: str,
    nonce: str,
) -> dict[str, str]:
    """Return the domain-separated HMAC identity for one session challenge."""
    if len(nonce) != 64 or any(character not in "0123456789abcdef" for character in nonce):
        raise ValueError("session identity nonce must be a lowercase 256-bit hexadecimal value")
    message = "\n".join(
        (SESSION_IDENTITY_SCHEMA, cluster, session_id, generation_id, nonce)
    ).encode("utf-8")
    return {
        "schema_version": SESSION_IDENTITY_SCHEMA,
        "cluster": cluster,
        "session_id": session_id,
        "session_generation_id": generation_id,
        "nonce": nonce,
        "hmac_sha256": hmac.new(
            owner_token.encode("utf-8"),
            message,
            hashlib.sha256,
        ).hexdigest(),
    }


def _validate_submission_receipt(
    job: RelayJob,
    *,
    path: str,
    payload: dict[str, object],
) -> None:
    """Reject a validly shaped receipt that does not match the exact submitted request."""
    if job.idempotency_key != payload.get("idempotency_key"):
        raise RelayError("owned session API returned a different idempotency identity")
    raw_uses = payload.get("used_artifact_refs", [])
    if not isinstance(raw_uses, list):
        raise RelayError("owned session submission has invalid artifact dependencies")
    try:
        expected_uses = sorted(
            (ArtifactUse.model_validate(item) for item in cast(list[object], raw_uses)),
            key=lambda item: item.artifact_id,
        )
    except ValidationError as exc:
        raise RelayError("owned session submission has invalid artifact dependencies") from exc
    if job.used_artifact_refs != expected_uses:
        raise RelayError("owned session API did not retain exact artifact dependencies")
    if path == "/jobs/mcp-call":
        if job.kind is not JobKind.MCP_CALL or not isinstance(job.spec, McpCallSpec):
            raise RelayError("owned session API returned the wrong job kind")
        try:
            operation = McpOperation(payload.get("operation", McpOperation.TOOLS_CALL.value))
            raw_evidence = payload.get("control_query_evidence")
            evidence = (
                McpControlQueryEvidence.model_validate(raw_evidence)
                if raw_evidence is not None
                else None
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise RelayError("owned session submission has invalid MCP admission evidence") from exc
        expected_authority: McpAdmissionAuthority | None
        if operation is McpOperation.TOOLS_LIST:
            if job.spec.admission_class is McpAdmissionClass.CONTROL_QUERY:
                _expected_class = McpAdmissionClass.CONTROL_QUERY
                expected_authority = McpAdmissionAuthority(
                    source="intrinsic_tools_list",
                    operation=McpOperation.TOOLS_LIST,
                )
            else:
                _expected_class = McpAdmissionClass.WORKLOAD
                expected_authority = None
        elif evidence is not None:
            tool = payload.get("tool")
            expected_digest = payload.get("expected_server_artifact_digest")
            if not isinstance(tool, str) or not isinstance(expected_digest, str):
                raise RelayError("owned session MCP evidence is missing its call binding")
            _expected_class = McpAdmissionClass.CONTROL_QUERY
            expected_authority = McpAdmissionAuthority(
                source="registered_discovery_artifact",
                operation=operation,
                tool=tool,
                expected_server_artifact_digest=expected_digest,
                evidence=evidence,
            )
        else:
            _expected_class = McpAdmissionClass.WORKLOAD
            expected_authority = None
        expected_arguments = payload.get("arguments", {})
        raw_tool = payload.get("tool")
        normalized_tool = raw_tool.replace("-", "_").lower() if isinstance(raw_tool, str) else ""
        if (
            normalized_tool == "jarvis_run"
            and payload.get("expected_registered_contract") == REGISTERED_JARVIS_USER_CONTRACT
        ):
            expected_arguments = _expected_jarvis_mcp_arguments(job, payload=payload)
        expected = {
            "server": payload.get("server"),
            "server_args": payload.get("server_args", []),
            "env_from": payload.get("env_from", {}),
            "expected_server_artifact_digest": payload.get("expected_server_artifact_digest"),
            "expected_registered_contract": payload.get("expected_registered_contract"),
            "admission_class": _expected_class.value,
            "operation": operation.value,
            "tool": payload.get("tool"),
            "arguments": expected_arguments,
            "jarvis_input_manifest": payload.get("jarvis_input_manifest"),
            "timeout_seconds": payload.get("timeout_seconds"),
        }
        observed = {
            "server": job.spec.server,
            "server_args": job.spec.server_args,
            "env_from": job.spec.env_from,
            "expected_server_artifact_digest": (job.spec.expected_server_artifact_digest),
            "expected_registered_contract": job.spec.expected_registered_contract,
            "admission_class": job.spec.admission_class.value,
            "operation": job.spec.operation.value,
            "tool": job.spec.tool,
            "arguments": job.spec.arguments,
            "jarvis_input_manifest": (
                job.spec.jarvis_input_manifest.model_dump(mode="json")
                if job.spec.jarvis_input_manifest is not None
                else None
            ),
            "timeout_seconds": job.spec.timeout_seconds,
        }
        if observed != expected:
            raise RelayError("owned session API returned a different MCP call")
        _validate_admission_authority(job, expected=expected_authority)
        return
    if path == "/jobs/jarvis-mcp-call":
        if job.kind is not JobKind.MCP_CALL or not isinstance(job.spec, McpCallSpec):
            raise RelayError("owned session API returned the wrong job kind")
        try:
            operation = McpOperation(payload.get("operation", McpOperation.TOOLS_CALL.value))
        except ValueError as exc:
            raise RelayError(
                "owned session submission has an invalid JARVIS MCP operation"
            ) from exc
        raw_tool = payload.get("tool")
        tool = raw_tool if isinstance(raw_tool, str) else None
        raw_expected_digest = payload.get("expected_server_artifact_digest")
        expected_digest = raw_expected_digest if isinstance(raw_expected_digest, str) else None
        raw_timeout = payload.get("timeout_seconds")
        if raw_timeout is not None and (
            isinstance(raw_timeout, bool) or not isinstance(raw_timeout, int)
        ):
            raise RelayError("owned session submission has an invalid JARVIS MCP timeout")
        try:
            expected_class, expected_authority = resolve_pinned_mcp_admission(
                operation=operation,
                tool=tool,
                expected_server_artifact_digest=expected_digest,
                pinned_control_query=(tool is not None and is_virtual_jarvis_control_query(tool)),
                timeout_seconds=raw_timeout,
            )
        except ValueError as exc:
            raise RelayError("owned session submission has an invalid JARVIS MCP timeout") from exc
        expected_arguments = _expected_jarvis_mcp_arguments(job, payload=payload)
        if (
            job.spec.operation is not operation
            or job.spec.tool != payload.get("tool")
            or job.spec.arguments != expected_arguments
            or job.spec.expected_server_artifact_digest
            != payload.get("expected_server_artifact_digest")
            or job.spec.admission_class is not expected_class
            or job.spec.timeout_seconds != payload.get("timeout_seconds")
        ):
            raise RelayError("owned session API returned a different JARVIS MCP call")
        _validate_admission_authority(job, expected=expected_authority)
        return
    if path == "/jobs/jarvis":
        if job.kind is not JobKind.JARVIS or not isinstance(job.spec, JarvisRunSpec):
            raise RelayError("owned session API returned the wrong job kind")
        if job.spec.pipeline_yaml != payload.get("pipeline_yaml"):
            raise RelayError("owned session API returned a different JARVIS pipeline")
        return
    if path == "/jobs/jarvis-pipeline":
        if job.kind is not JobKind.JARVIS or not isinstance(job.spec, JarvisRunSpec):
            raise RelayError("owned session API returned the wrong job kind")
        if job.spec.pipeline_name != payload.get("pipeline_name"):
            raise RelayError("owned session API returned a different JARVIS pipeline name")
        return
    if path == "/jobs/remote-agent":
        if job.kind is not JobKind.REMOTE_AGENT or not isinstance(
            job.spec,
            RemoteAgentTaskSpec,
        ):
            raise RelayError("owned session API returned the wrong job kind")
        observed_agent = {
            "prompt_path": job.spec.prompt_path,
            "mcp_config_path": job.spec.mcp_config_path,
            "model": job.spec.model,
            "workdir": job.spec.workdir,
            "timeout_seconds": job.spec.timeout_seconds,
        }
        expected_agent = {
            "prompt_path": payload.get("prompt_path"),
            "mcp_config_path": payload.get("mcp_config_path"),
            "model": payload.get("model"),
            "workdir": payload.get("workdir"),
            "timeout_seconds": payload.get("timeout_seconds"),
        }
        if observed_agent != expected_agent:
            raise RelayError("owned session API returned a different remote-agent task")


def _validate_admission_authority(
    job: RelayJob,
    *,
    expected: McpAdmissionAuthority | None,
) -> None:
    """Require the exact deterministic authority stamped by trusted HTTP ingress."""
    raw_authority = job.metadata.get(MCP_ADMISSION_AUTHORITY_METADATA_KEY)
    if raw_authority is None:
        observed = None
    else:
        try:
            observed = McpAdmissionAuthority.model_validate(raw_authority)
        except ValidationError as exc:
            raise RelayError("owned session API returned invalid MCP admission authority") from exc
    if observed != expected:
        raise RelayError("owned session API returned different MCP admission authority")


def _expected_jarvis_mcp_arguments(
    job: RelayJob,
    *,
    payload: dict[str, object],
) -> dict[str, object]:
    """Normalize only the server-owned identity added during JARVIS run admission.

    The authenticated cluster API admits ``jarvis_run`` before returning its receipt. Admission
    deterministically adds ``execution_id`` using the cluster, idempotency key, and server-owned
    relay job ID. That field is intentionally absent from an ordinary caller request. Recompute
    the one permitted normalization while retaining an exact comparison for every caller-owned
    argument. An explicit execution ID is accepted only for an exact idempotent replay whose
    returned job identity proves the same value.
    """

    raw_arguments = payload.get("arguments", {})
    if not isinstance(raw_arguments, dict):
        raise RelayError("owned session submission has invalid JARVIS MCP arguments")
    expected_arguments = cast(dict[str, object], raw_arguments)
    raw_tool = payload.get("tool")
    normalized_tool = raw_tool.replace("-", "_").lower() if isinstance(raw_tool, str) else ""
    if normalized_tool != "jarvis_run":
        return expected_arguments
    if not is_owned_jarvis_run_spec(job.kind, job.spec):
        raise RelayError("owned session API returned an unbound JARVIS run")

    expected_execution_id = deterministic_jarvis_execution_id(
        cluster=job.cluster,
        idempotency_key=job.idempotency_key,
        job_id=job.job_id,
    )
    supplied_execution_id = expected_arguments.get("execution_id")
    if supplied_execution_id is not None and supplied_execution_id != expected_execution_id:
        raise RelayError("owned session JARVIS run used a different execution identity")
    return {**expected_arguments, "execution_id": expected_execution_id}
