"""Authenticated client transport for one exact owned relay session API."""

from __future__ import annotations

import hashlib
import hmac
import http.client
import json
import math
import secrets
import socket
import subprocess
import time
import urllib.parse
from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from typing import Final, cast

import httpx
from pydantic import ValidationError

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.errors import ConfigurationError, ObservationTimeoutError, RelayError
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
from clio_relay.remote_mcp import resolve_pinned_mcp_admission
from clio_relay.session_lifecycle import (
    challenge_remote_session_identity,
    status_remote_session,
)

OWNER_SESSION_ID_HEADER: Final = "X-Clio-Relay-Owner-Session-Id"
SESSION_GENERATION_ID_HEADER: Final = "X-Clio-Relay-Session-Generation-Id"
SESSION_IDENTITY_SCHEMA: Final = "clio-relay.session-identity.v1"
MAX_SESSION_API_RESPONSE_BYTES: Final = 8 * 1024 * 1024
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
    """Identity-proven client for one exact owned session generation."""

    def __init__(
        self,
        *,
        definition: ClusterDefinition,
        settings: RelaySettings,
        timeout_seconds: float = 30.0,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._definition = definition
        self._settings = settings
        self._timeout_seconds = timeout_seconds
        self._stack: ExitStack | None = None
        self._connection: http.client.HTTPConnection | None = None
        self._session_id: str | None = None
        self._generation_id: str | None = None
        self._api_token: str | None = None

    def __enter__(self) -> OwnedSessionApiClient:
        """Prove the SSH-authenticated server and bind one persistent TCP stream."""
        session_id, generation_id, api_token = _owned_session_credentials(
            definition=self._definition,
            settings=self._settings,
        )
        remote_api_port = _verified_remote_api_port(
            definition=self._definition,
            session_id=session_id,
            generation_id=generation_id,
        )
        nonce = secrets.token_hex(32)
        expected_identity = challenge_remote_session_identity(
            definition=self._definition,
            session_id=session_id,
            session_generation_id=generation_id,
            nonce=nonce,
        )
        stack = ExitStack()
        try:
            readiness_client = stack.enter_context(httpx.Client(trust_env=False))
            local_port = stack.enter_context(
                _ssh_forward(
                    definition=self._definition,
                    remote_api_port=remote_api_port,
                    client=readiness_client,
                    timeout_seconds=self._timeout_seconds,
                )
            )
            connection = _open_identity_bound_connection(
                local_port=local_port,
                nonce=nonce,
                expected_identity=expected_identity,
                timeout_seconds=self._timeout_seconds,
            )
            stack.callback(connection.close)
        except BaseException:
            stack.close()
            raise
        self._stack = stack
        self._connection = connection
        self._session_id = session_id
        self._generation_id = generation_id
        self._api_token = api_token
        return self

    def __exit__(self, *_args: object) -> None:
        """Close the proven stream and its SSH forward without reconnecting."""
        stack = self._stack
        self._stack = None
        self._connection = None
        self._session_id = None
        self._generation_id = None
        self._api_token = None
        if stack is not None:
            stack.close()

    def request_json(
        self,
        *,
        method: str,
        path: str,
        query: dict[str, object] | None = None,
        body: dict[str, object] | None = None,
        response_timeout_seconds: float | None = None,
    ) -> object:
        """Issue one authenticated JSON request on the already proven TCP stream.

        ``response_timeout_seconds`` changes only the response deadline for this
        request. It does not relax the bounded SSH-forward or identity-proof
        deadlines, and the connection's ordinary timeout is restored before a
        later request reuses the same proven stream.
        """
        normalized_method = _validate_request(method=method, path=path)
        if response_timeout_seconds is not None and (
            not math.isfinite(response_timeout_seconds) or response_timeout_seconds <= 0
        ):
            raise ValueError("response_timeout_seconds must be positive and finite")
        connection = self._connection
        session_id = self._session_id
        generation_id = self._generation_id
        api_token = self._api_token
        if connection is None or session_id is None or generation_id is None or api_token is None:
            raise RuntimeError("owned session API client is not open")
        return _request_json_on_connection(
            connection=connection,
            method=normalized_method,
            path=path,
            query=query,
            body=body,
            api_token=api_token,
            session_id=session_id,
            generation_id=generation_id,
            response_timeout_seconds=response_timeout_seconds,
        )


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
        job.metadata.get("owner") != "clio-relay"
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
    """Call one exact-generation session API after proving its server identity."""
    _validate_request(method=method, path=path)
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


def _owned_session_credentials(
    *,
    definition: ClusterDefinition,
    settings: RelaySettings,
) -> tuple[str, str, str]:
    session_id = settings.owner_session_id
    generation_id = settings.owner_session_generation_id
    api_token = settings.api_token
    if session_id is None or generation_id is None:
        raise ConfigurationError(
            "owned remote request requires CLIO_RELAY_OWNER_SESSION_ID and "
            "CLIO_RELAY_SESSION_GENERATION_ID"
        )
    if settings.resolved_owner_session_cluster() != definition.name:
        raise ConfigurationError(
            "owned remote request requires CLIO_RELAY_OWNER_SESSION_CLUSTER to match the "
            "selected route"
        )
    if not api_token:
        raise ConfigurationError(
            "owned remote request requires CLIO_RELAY_API_TOKEN for authentication"
        )
    return session_id, generation_id, api_token


def _verified_remote_api_port(
    *,
    definition: ClusterDefinition,
    session_id: str,
    generation_id: str,
) -> int:
    remote_status = status_remote_session(definition=definition, session_id=session_id)
    remote_api_port = remote_status.get("remote_api_port")
    if (
        remote_status.get("owner") != "clio-relay"
        or remote_status.get("cluster") != definition.name
        or remote_status.get("session_id") != session_id
        or remote_status.get("session_generation_id") != generation_id
        or remote_status.get("running") is not True
        or remote_status.get("ownership_verified") is not True
        or isinstance(remote_api_port, bool)
        or not isinstance(remote_api_port, int)
        or not 1 <= remote_api_port <= 65_535
    ):
        raise RelayError(
            "remote relay session is not the active, ownership-verified generation requested "
            f"for {definition.name}/{session_id}"
        )
    return remote_api_port


def _validate_request(*, method: str, path: str) -> str:
    normalized_method = method.upper()
    if normalized_method not in {"GET", "POST"}:
        raise ValueError("owned session API method must be GET or POST")
    if (
        not path.startswith("/")
        or path.startswith("//")
        or any(character in path for character in ("\r", "\n", "?"))
    ):
        raise ValueError("owned session API path must be an absolute path without a query")
    return normalized_method


def _open_identity_bound_connection(
    *,
    local_port: int,
    nonce: str,
    expected_identity: dict[str, object],
    timeout_seconds: float,
) -> http.client.HTTPConnection:
    """Prove one non-reconnecting TCP stream before any credential is sent."""
    connection = http.client.HTTPConnection("127.0.0.1", local_port, timeout=timeout_seconds)
    try:
        connection.connect()
        connection.auto_open = 0
        connection.request(
            "GET",
            "/session-identity?" + urllib.parse.urlencode({"nonce": nonce}),
            headers={"Accept": "application/json", "Connection": "keep-alive"},
        )
        proof_response = connection.getresponse()
        proof_document = _read_json_response(proof_response, label="session identity challenge")
        if proof_response.status != 200 or not isinstance(proof_document, dict):
            raise RelayError("owned session API did not return a valid server identity challenge")
        _verify_session_identity(
            cast(dict[str, object], proof_document),
            expected=expected_identity,
        )
        if proof_response.will_close or connection.sock is None:
            raise RelayError(
                "owned session API closed the identity-proven connection before authentication"
            )
        return connection
    except (OSError, http.client.HTTPException) as exc:
        connection.close()
        raise RelayError("owned session API identity challenge failed") from exc
    except BaseException:
        connection.close()
        raise


def _request_json_on_connection(
    *,
    connection: http.client.HTTPConnection,
    method: str,
    path: str,
    query: dict[str, object] | None,
    body: dict[str, object] | None,
    api_token: str,
    session_id: str,
    generation_id: str,
    response_timeout_seconds: float | None,
) -> object:
    """Issue one request without permitting HTTPConnection to reconnect."""
    encoded_query = "" if query is None else "?" + urllib.parse.urlencode(query)
    encoded_body = None if body is None else json.dumps(body).encode("utf-8")
    proven_socket = connection.sock
    prior_connection_timeout = connection.timeout
    prior_socket_timeout: float | None = None
    response_timeout_applied = False
    try:
        if response_timeout_seconds is not None:
            if proven_socket is None:
                raise RelayError("owned session API identity-proven connection is not open")
            prior_socket_timeout = proven_socket.gettimeout()
            connection.timeout = response_timeout_seconds
            proven_socket.settimeout(response_timeout_seconds)
            response_timeout_applied = True
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {api_token}",
            OWNER_SESSION_ID_HEADER: session_id,
            SESSION_GENERATION_ID_HEADER: generation_id,
        }
        if encoded_body is not None:
            headers["Content-Type"] = "application/json"
        connection.request(
            method,
            path + encoded_query,
            body=encoded_body,
            headers=headers,
        )
        response = connection.getresponse()
        document = _read_json_response(response, label=f"{method} {path}")
        if not 200 <= response.status < 300:
            detail = json.dumps(document, ensure_ascii=False)[:2_000]
            raise RelayError(
                f"owned session API request failed: {method} {path}: "
                f"HTTP {response.status}: {detail}"
            )
        return document
    except (OSError, http.client.HTTPException) as exc:
        if response_timeout_applied and isinstance(exc, TimeoutError):
            raise ObservationTimeoutError(
                f"owned session API response observation timed out for {method} {path}"
            ) from exc
        raise RelayError(
            f"owned session API identity-bound request failed for {method} {path}: {exc}"
        ) from exc
    finally:
        if response_timeout_seconds is not None:
            connection.timeout = prior_connection_timeout
            if (
                response_timeout_applied
                and connection.sock is proven_socket
                and proven_socket is not None
            ):
                try:
                    proven_socket.settimeout(prior_socket_timeout)
                except OSError:
                    # The response may have closed the proven socket. Never let
                    # HTTPConnection reconnect it under the authenticated client.
                    connection.close()


def _read_json_response(response: http.client.HTTPResponse, *, label: str) -> object:
    payload = response.read(MAX_SESSION_API_RESPONSE_BYTES + 1)
    if len(payload) > MAX_SESSION_API_RESPONSE_BYTES:
        raise RelayError(f"owned session API {label} response exceeded its byte limit")
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RelayError(f"owned session API {label} response was not UTF-8 JSON") from exc


def _verify_session_identity(
    observed: dict[str, object],
    *,
    expected: dict[str, object],
) -> None:
    fields = (
        "schema_version",
        "cluster",
        "session_id",
        "session_generation_id",
        "nonce",
    )
    if any(observed.get(field) != expected.get(field) for field in fields):
        raise RelayError("owned session API server identity did not match the SSH-proven session")
    observed_signature = observed.get("hmac_sha256")
    expected_signature = expected.get("hmac_sha256")
    if (
        not isinstance(observed_signature, str)
        or not isinstance(expected_signature, str)
        or len(observed_signature) != 64
        or len(expected_signature) != 64
        or not hmac.compare_digest(observed_signature, expected_signature)
    ):
        raise RelayError("owned session API server identity HMAC did not verify")


@contextmanager
def _ssh_forward(
    *,
    definition: ClusterDefinition,
    remote_api_port: int,
    client: httpx.Client,
    timeout_seconds: float,
) -> Generator[int, None, None]:
    """Open a bounded loopback-only SSH forward and always stop it after the request."""
    local_port = _available_loopback_port()
    process = subprocess.Popen(
        [
            "ssh",
            "-N",
            "-T",
            "-o",
            "BatchMode=yes",
            "-o",
            "ExitOnForwardFailure=yes",
            "-L",
            f"127.0.0.1:{local_port}:127.0.0.1:{remote_api_port}",
            definition.ssh_host,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    base_url = f"http://127.0.0.1:{local_port}"
    try:
        _wait_for_forward(
            process,
            client=client,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
        )
        yield local_port
    finally:
        _terminate_forward(process)


def _available_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    if not isinstance(port, int) or port <= 0:
        raise RelayError("could not select a loopback port for the owned session API")
    return port


def _wait_for_forward(
    process: subprocess.Popen[bytes],
    *,
    client: httpx.Client,
    base_url: str,
    timeout_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error = "SSH forward did not become ready"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RelayError(_forward_error(process, "owned session SSH forward exited"))
        try:
            response = client.get(base_url + "/healthz", timeout=min(0.5, timeout_seconds))
            if response.status_code == 200 and response.json().get("ok") is True:
                return
            last_error = f"unexpected health response: HTTP {response.status_code}"
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            last_error = str(exc)
        time.sleep(0.05)
    raise RelayError(f"owned session SSH forward did not become ready: {last_error}")


def _terminate_forward(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _forward_error(process: subprocess.Popen[bytes], fallback: str) -> str:
    stderr = process.stderr.read() if process.stderr is not None else b""
    detail = stderr.decode("utf-8", errors="replace").strip()
    return detail or fallback
