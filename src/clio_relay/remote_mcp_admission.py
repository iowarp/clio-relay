"""MCP admission resolution: registered-route, pinned, and control-query paths.

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5, the deferred
"validator families" row). This module owns the admission-class derivation
the queue independently re-verifies before granting reserved control-query
capacity: :func:`resolve_registered_remote_mcp_admission` re-derives
admission from either intrinsic ``tools/list`` discovery or a caller-offered
route evidence bundle it must re-validate end to end (never trust the
caller's selection), and :func:`resolve_pinned_mcp_admission` derives the
built-in pinned-JARVIS-contract admission with no caller input at all. The
two private helpers (a bounded timeout guard and a durable discovery
artifact reader) exist only to serve those two entry points.

``resolve_registered_remote_mcp_admission`` and ``resolve_pinned_mcp_admission``
are re-exported under their original names (``cli.py``, ``cli_jarvis_mcp.py``,
``http_api.py``, ``cli_remote_mcp.py``, and tests import them directly from
``clio_relay.remote_mcp``). The two private helpers have no caller outside
``remote_mcp.py`` (confirmed by grep before the move), so they are imported
directly rather than re-exported.

``_validate_pinned_control_query_timeout`` reads
``MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS``, a bound that still lives in
``remote_mcp.py`` (unsequenced, post-campaign per the design doc). A
module-scope import back into ``remote_mcp.py`` (which imports this module
for the re-export above) would be a load-order circular import; importing it
inside the function body instead is the proven idiom for that shape (see
``remote_mcp_wire_schemas.py``'s own ``virtual_jarvis_job_output_schema``).
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
from datetime import datetime
from typing import TYPE_CHECKING, cast

from clio_relay.bounded_payload import describe_delivery_refusal, is_delivery_refusal
from clio_relay.cluster_config import ClusterDefinition, cluster_route_revision
from clio_relay.errors import NotFoundError, RelayError
from clio_relay.models import (
    JobKind,
    JobState,
    McpAdmissionAuthority,
    McpAdmissionClass,
    McpCallSpec,
    McpControlQueryEvidence,
    McpOperation,
)
from clio_relay.remote_mcp_cache import (
    cache_entry_from_discovery_artifact,
    remote_mcp_registration_revision,
    remote_mcp_server_artifact_digest,
)
from clio_relay.remote_mcp_tool_schema import _server_artifact_verified, is_remote_mcp_control_query

if TYPE_CHECKING:
    from clio_relay.core_queue import ClioCoreQueue


def resolve_registered_remote_mcp_admission(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition | None,
    cluster: str,
    server: str,
    server_args: list[str],
    env_from: dict[str, str],
    operation: McpOperation,
    tool: str | None,
    expected_server_artifact_digest: str | None,
    evidence: McpControlQueryEvidence | None,
    expected_registered_contract: str | None = None,
    timeout_seconds: int | None = None,
    now: datetime | None = None,
) -> tuple[McpAdmissionClass, McpAdmissionAuthority | None]:
    """Derive admission from intrinsic discovery or cluster-owned route evidence.

    A caller can offer evidence but can never select the resulting worker lane.
    The receiving queue independently reloads the operator registration and the
    exact durable ``tools/list`` artifact before granting reserved capacity.
    """
    if operation is McpOperation.TOOLS_LIST:
        if evidence is not None:
            raise ValueError("tools/list must not carry control-query route evidence")
        if expected_registered_contract is not None:
            raise ValueError("tools/list must not carry a registered semantic contract binding")
        if definition is None or definition.name != cluster:
            return McpAdmissionClass.WORKLOAD, None
        matches = [
            registration
            for registration in definition.remote_mcp_servers.values()
            if registration.enabled
            and registration.command == server
            and registration.args == server_args
            and registration.env_from == env_from
        ]
        if not matches:
            return McpAdmissionClass.WORKLOAD, None
        if len(matches) != 1:
            raise ValueError("tools/list route matches multiple operator registrations")
        if timeout_seconds is None:
            raise ValueError("registered tools/list control admission requires an explicit timeout")
        if timeout_seconds <= 0:
            raise ValueError("registered tools/list timeout must be positive")
        if timeout_seconds > matches[0].call_timeout_seconds:
            raise ValueError("tools/list timeout exceeds the operator registration limit")
        return (
            McpAdmissionClass.CONTROL_QUERY,
            McpAdmissionAuthority(
                source="intrinsic_tools_list",
                operation=McpOperation.TOOLS_LIST,
            ),
        )
    if evidence is None:
        if expected_registered_contract is None:
            return McpAdmissionClass.WORKLOAD, None
        if definition is None or definition.name != cluster:
            raise ValueError("registered semantic contract requires a configured cluster route")
        matches = [
            registration
            for registration in definition.remote_mcp_servers.values()
            if registration.enabled
            and registration.command == server
            and registration.args == server_args
            and registration.env_from == env_from
        ]
        if len(matches) != 1:
            raise ValueError(
                "registered semantic contract route must match exactly one operator registration"
            )
        registration = matches[0]
        if registration.contract != expected_registered_contract:
            raise ValueError("registered MCP semantic contract changed after discovery")
        if not tool or not registration.allows_tool(tool):
            raise ValueError("MCP tool is not allowlisted by its operator registration")
        if registration.allow_mutable_artifact:
            raise ValueError("registered semantic contracts require an immutable MCP artifact")
        if expected_server_artifact_digest is None:
            raise ValueError("registered semantic contract requires a server artifact binding")
        if timeout_seconds is None or timeout_seconds <= 0:
            raise ValueError("registered semantic contract requires an explicit positive timeout")
        if timeout_seconds > registration.call_timeout_seconds:
            raise ValueError("MCP call timeout exceeds the operator registration limit")
        return McpAdmissionClass.WORKLOAD, None
    if not tool:
        raise ValueError("control-query evidence requires one tools/call tool")
    if expected_server_artifact_digest is None:
        raise ValueError("control-query evidence requires an expected server artifact digest")
    if definition is None:
        raise ValueError("control-query evidence requires a configured cluster route")
    if evidence.cluster != cluster or definition.name != cluster:
        raise ValueError("control-query evidence does not match the selected cluster")
    if not hmac.compare_digest(
        evidence.cluster_route_revision,
        cluster_route_revision(definition),
    ):
        raise ValueError("cluster route changed after MCP discovery; refresh the registered server")
    registration = definition.remote_mcp_servers.get(evidence.registered_server_name)
    if registration is None or not registration.enabled:
        raise ValueError("registered MCP server is unavailable; refresh its discovery")
    if not hmac.compare_digest(
        evidence.registration_revision,
        remote_mcp_registration_revision(registration),
    ):
        raise ValueError("registered MCP server changed after discovery; refresh its discovery")
    if (
        registration.command != server
        or registration.args != server_args
        or registration.env_from != env_from
    ):
        raise ValueError("MCP call route does not match its operator registration")
    if not registration.allows_tool(tool):
        raise ValueError("MCP tool is not allowlisted by its operator registration")
    if (
        expected_registered_contract is not None
        and registration.contract != expected_registered_contract
    ):
        raise ValueError("registered MCP semantic contract changed after discovery")
    if registration.allow_mutable_artifact:
        raise ValueError("mutable MCP server artifacts cannot use reserved query capacity")
    if timeout_seconds is None:
        raise ValueError("registered MCP control admission requires an explicit timeout")
    if timeout_seconds <= 0:
        raise ValueError("registered MCP query timeout must be positive")
    if timeout_seconds > registration.call_timeout_seconds:
        raise ValueError("MCP query timeout exceeds the operator registration limit")
    if not hmac.compare_digest(
        evidence.expected_server_artifact_digest,
        expected_server_artifact_digest,
    ):
        raise ValueError("MCP call artifact binding does not match its discovery evidence")

    try:
        discovery_job = queue.get_job(evidence.discovery_job_id)
        discovery_artifact = queue.get_artifact(evidence.discovery_artifact_id)
    except NotFoundError as exc:
        raise ValueError(
            "MCP control-query discovery evidence is unavailable; refresh discovery"
        ) from exc
    if (
        discovery_job.cluster != cluster
        or discovery_job.kind is not JobKind.MCP_CALL
        or discovery_job.state is not JobState.SUCCEEDED
        or not isinstance(discovery_job.spec, McpCallSpec)
        or discovery_job.spec.operation is not McpOperation.TOOLS_LIST
        or discovery_job.spec.server != registration.command
        or discovery_job.spec.server_args != registration.args
        or discovery_job.spec.env_from != registration.env_from
    ):
        raise ValueError("MCP discovery job does not match the registered tools/list route")
    if (
        discovery_artifact.job_id != discovery_job.job_id
        or discovery_artifact.kind != "mcp_result"
        or discovery_artifact.sha256 is None
        or not hmac.compare_digest(
            discovery_artifact.sha256,
            evidence.discovery_artifact_sha256,
        )
    ):
        raise ValueError("MCP discovery artifact identity does not match its evidence")
    try:
        artifact_payload = _control_query_discovery_artifact_bytes(
            queue,
            evidence.discovery_artifact_id,
        )
    except RelayError as exc:
        raise ValueError(
            "MCP control-query discovery artifact is unavailable; refresh discovery"
        ) from exc
    observed_artifact_sha256 = hashlib.sha256(artifact_payload).hexdigest()
    if not hmac.compare_digest(
        observed_artifact_sha256,
        evidence.discovery_artifact_sha256,
    ):
        raise ValueError("MCP discovery artifact bytes changed after evidence was issued")
    entry = cache_entry_from_discovery_artifact(
        cluster=cluster,
        server_name=evidence.registered_server_name,
        registration=registration,
        discovery_job_id=evidence.discovery_job_id,
        artifact_id=evidence.discovery_artifact_id,
        artifact_sha256=evidence.discovery_artifact_sha256,
        artifact_payload=artifact_payload,
        discovered_at=discovery_job.updated_at,
    )
    if not hmac.compare_digest(entry.schema_digest, evidence.discovery_schema_digest):
        raise ValueError("MCP discovery schema does not match its route evidence")
    if not entry.is_fresh(now=now):
        raise ValueError("MCP control-query discovery evidence expired; refresh discovery")
    matching_tools = [candidate for candidate in entry.tools if candidate.name == tool]
    if len(matching_tools) != 1 or not is_remote_mcp_control_query(matching_tools[0]):
        raise ValueError("MCP tool is not explicitly classified as a non-destructive read query")
    from clio_relay.dev_mode import dev_mode_enabled

    if not _server_artifact_verified(entry.provenance.server_artifact):
        if not dev_mode_enabled():
            raise ValueError("MCP discovery did not verify an immutable server artifact")
    else:
        observed_server_digest = remote_mcp_server_artifact_digest(entry.provenance.server_artifact)
        if (
            not hmac.compare_digest(observed_server_digest, expected_server_artifact_digest)
            and not dev_mode_enabled()
        ):
            raise ValueError("MCP server artifact does not match the discovered route binding")
    return (
        McpAdmissionClass.CONTROL_QUERY,
        McpAdmissionAuthority(
            source="registered_discovery_artifact",
            operation=McpOperation.TOOLS_CALL,
            tool=tool,
            expected_server_artifact_digest=expected_server_artifact_digest,
            evidence=evidence,
        ),
    )


def resolve_pinned_mcp_admission(
    *,
    operation: McpOperation,
    tool: str | None,
    expected_server_artifact_digest: str | None,
    pinned_control_query: bool,
    timeout_seconds: int | None = None,
) -> tuple[McpAdmissionClass, McpAdmissionAuthority | None]:
    """Derive tools/list or built-in JARVIS query admission without caller input."""
    if operation is McpOperation.TOOLS_LIST:
        _validate_pinned_control_query_timeout(timeout_seconds)
        return (
            McpAdmissionClass.CONTROL_QUERY,
            McpAdmissionAuthority(
                source="intrinsic_tools_list",
                operation=McpOperation.TOOLS_LIST,
            ),
        )
    if pinned_control_query and tool is not None and expected_server_artifact_digest is not None:
        _validate_pinned_control_query_timeout(timeout_seconds)
        return (
            McpAdmissionClass.CONTROL_QUERY,
            McpAdmissionAuthority(
                source="pinned_jarvis_contract",
                operation=McpOperation.TOOLS_CALL,
                tool=tool,
                expected_server_artifact_digest=expected_server_artifact_digest,
            ),
        )
    return McpAdmissionClass.WORKLOAD, None


def _validate_pinned_control_query_timeout(timeout_seconds: int | None) -> None:
    """Reject an explicitly invalid timeout on a reserved pinned query."""
    from clio_relay.remote_mcp import MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS

    if timeout_seconds is None:
        return
    if timeout_seconds <= 0:
        raise ValueError("pinned MCP control-query timeout must be positive")
    if timeout_seconds > MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS:
        raise ValueError(
            "pinned MCP control-query timeout exceeds "
            f"{MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS} seconds"
        )


def _control_query_discovery_artifact_bytes(
    queue: ClioCoreQueue,
    artifact_id: str,
) -> bytes:
    """Read one queue-owned artifact envelope without trusting a caller path."""
    from clio_relay.relay_ops import read_artifact_bytes

    envelope = read_artifact_bytes(queue, artifact_id)
    if is_delivery_refusal(envelope):
        # F5 (#231 R6 review): a T2 refusal (doc §6.4) is not an unsupported
        # encoding -- report the refusal's own message/code rather than the
        # generic "encoding is unsupported", which misdescribes why the
        # artifact is unavailable.
        # A2 (#231 R6 review): the message extraction itself now delegates
        # to bounded_payload.describe_delivery_refusal, the single owner.
        code = cast(dict[str, object], envelope.get("delivery", {})).get("code")
        message = describe_delivery_refusal(envelope)
        raise ValueError(f"MCP discovery artifact delivery refused ({code}): {message}")
    if envelope.get("encoding") != "base64":
        raise ValueError("MCP discovery artifact encoding is unsupported")
    encoded = envelope.get("data")
    if not isinstance(encoded, str):
        raise ValueError("MCP discovery artifact payload is unavailable")
    try:
        return base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("MCP discovery artifact payload is not valid base64") from exc
