"""Owned-session SSH script rendering and bounded transport (#231 rework).

Extracted from ``session_lifecycle.py``: the cluster-local script generators
for every owned-session subcommand (start, status, start-status, identity
challenge, cleanup finalize, cleanup report read, teardown) plus the two SSH
transport wrappers (``_ssh_script`` for inline stdout scripts,
``_ssh_stdin_command`` for scripts carrying a separately bounded stdin
payload) and the dead-executable-pin refusal they share. A pure, one-directional
dependent of session_remote_command.py; nothing calls back into it, so every
remaining remote_session_*/status_remote_session_start/challenge_remote_session_identity
entry point in session_lifecycle.py builds its script and transport through
this module.
"""

from __future__ import annotations

import hashlib
import json
import shlex
from typing import TYPE_CHECKING, cast

import clio_relay.session_remote_command as session_remote_command
from clio_relay.cluster_config import (
    MAX_CLUSTER_REGISTRY_BYTES,
    ClusterRegistry,
    cluster_route_revision,
)
from clio_relay.errors import (
    SHELL_COMMAND_NOT_FOUND_STATUS,
    RelayError,
    relay_executable_missing,
)
from clio_relay.errors import BoundedCommandTimeout as _BoundedCommandTimeout
from clio_relay.remote_cli import remote_env
from clio_relay.remote_values import render_remote_shell_value
from clio_relay.session_wire_models import (
    OwnedSessionIdentityChallengeRequest,
    OwnedSessionStartRejection,
    OwnedSessionStartRequest,
    OwnedSessionTeardownRequest,
)

if TYPE_CHECKING:
    from clio_relay.cluster_config import ClusterDefinition
    from clio_relay.session_wire_models import (
        OwnedSessionInputPolicy,
        OwnedSessionStartStatusSelector,
        SessionApiReleaseIdentity,
    )

_REMOTE_SESSION_COMMAND_TIMEOUT_SECONDS = 120.0
_MAX_REMOTE_SESSION_SCRIPT_BYTES = MAX_CLUSTER_REGISTRY_BYTES + 128 * 1024
_MAX_REMOTE_SESSION_STDOUT_BYTES = 1024 * 1024
_MAX_REMOTE_SESSION_STDERR_BYTES = 1024 * 1024


def _owned_session_relay_executable(definition: ClusterDefinition) -> str:
    """Render the route-pinned Relay executable for cluster-local lifecycle calls."""

    return render_remote_shell_value(
        definition.relay_executable,
        field="relay_executable",
    )


def _start_script(
    *,
    cluster: str,
    definition: ClusterDefinition,
    session_id: str,
    start_operation_id: str,
    remote_api_port: int,
    api_token: str | None,
    expected_api_release_identity: SessionApiReleaseIdentity | None,
    input_policy: OwnedSessionInputPolicy,
    replace: bool,
    expected_cluster_route_revision: str,
) -> str:
    cluster_registry_json, cluster_registry_sha256, route_revision = (
        _session_cluster_registry_authority(cluster=cluster, definition=definition)
    )
    if route_revision != expected_cluster_route_revision:
        raise RelayError("owned-session start route revision changed after planning")
    request = OwnedSessionStartRequest(
        cluster=cluster,
        session_id=session_id,
        start_operation_id=start_operation_id,
        remote_api_port=remote_api_port,
        replace=replace,
        require_token=api_token is not None,
        input_policy=input_policy,
        expected_api_release_identity=expected_api_release_identity,
        cluster_registry=cast(dict[str, object], json.loads(cluster_registry_json)),
        cluster_registry_sha256=cluster_registry_sha256,
        cluster_route_revision=route_revision,
    )
    token_export = (
        f"export CLIO_RELAY_API_TOKEN={_shell_single_quote(api_token)}\n"
        if api_token is not None
        else ""
    )
    request_json = request.model_dump_json()
    relay_executable = _owned_session_relay_executable(definition)
    return (
        "set -euo pipefail\n"
        "umask 077\n"
        f"{remote_env(definition)}\n"
        f"{token_export}"
        f"printf '%s' {_shell_single_quote(request_json)} | "
        f"{relay_executable} session start-owned\n"
    )


def _session_cluster_registry_authority(
    *, cluster: str, definition: ClusterDefinition
) -> tuple[str, str, str]:
    """Return the exact registry payload and identities owned by one session API."""
    if definition.name != cluster:
        raise RelayError("session cluster does not match its cluster definition")
    registry = ClusterRegistry(clusters={cluster: definition})
    payload = json.dumps(
        registry.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )
    encoded_payload = payload.encode("utf-8")
    if len(encoded_payload) > MAX_CLUSTER_REGISTRY_BYTES:
        raise RelayError(
            "session cluster registry exceeds the "
            f"{MAX_CLUSTER_REGISTRY_BYTES}-byte configuration limit"
        )
    return (
        payload,
        hashlib.sha256(encoded_payload).hexdigest(),
        cluster_route_revision(definition),
    )


def _owned_status_script(
    *,
    definition: ClusterDefinition,
    cluster: str,
    session_id: str,
    pre_start_cleanup_probe: bool = False,
) -> str:
    """Use the bounded, lock-coordinated recovery contract for public status."""
    probe_argument = " --pre-start-cleanup-probe" if pre_start_cleanup_probe else ""
    relay_executable = _owned_session_relay_executable(definition)
    return (
        "set -euo pipefail\n"
        f"{remote_env(definition)}\n"
        f"{relay_executable} session recovery-status --cluster {shlex.quote(cluster)} "
        f"--session-id {shlex.quote(session_id)}{probe_argument}\n"
    )


def _owned_start_status_script(
    *,
    definition: ClusterDefinition,
    selector: OwnedSessionStartStatusSelector,
    wait_seconds: float = 0.0,
) -> str:
    """Render the exact-operation start-status command.

    ``wait_seconds`` makes the cluster-local command block against its own
    durable state until the start reaches a terminal observation, so one watch
    costs one command instead of one command per polling interval.
    """
    relay_executable = _owned_session_relay_executable(definition)
    wait_argument = "" if wait_seconds <= 0 else f" --wait-seconds {wait_seconds:g}"
    return (
        "set -euo pipefail\n"
        f"{remote_env(definition)}\n"
        f"{relay_executable} session start-status-owned "
        f"--cluster {shlex.quote(selector.cluster)} "
        f"--session-id {shlex.quote(selector.session_id)} "
        f"--start-operation-id {shlex.quote(selector.start_operation_id)} "
        "--cluster-route-revision "
        f"{shlex.quote(selector.cluster_route_revision)}{wait_argument}\n"
    )


def _owned_identity_challenge_script(
    *,
    definition: ClusterDefinition,
    cluster: str,
    session_id: str,
    session_generation_id: str,
    nonce: str,
) -> str:
    request = OwnedSessionIdentityChallengeRequest(
        cluster=cluster,
        session_id=session_id,
        session_generation_id=session_generation_id,
        nonce=nonce,
    )
    relay_executable = _owned_session_relay_executable(definition)
    return (
        "set -euo pipefail\n"
        "umask 077\n"
        f"{remote_env(definition)}\n"
        f"printf '%s' {_shell_single_quote(request.model_dump_json())} | "
        f"{relay_executable} session challenge-owned\n"
    )


def _owned_cleanup_finalize_script(
    *,
    definition: ClusterDefinition,
) -> str:
    """Run the bounded coordinator-report finalizer with SSH stdin left intact."""
    relay_executable = _owned_session_relay_executable(definition)
    return (
        "set -euo pipefail\n"
        "umask 077\n"
        f"{remote_env(definition)}\n"
        f"{relay_executable} session finalize-cleanup-owned\n"
    )


def _owned_cleanup_report_read_script(*, definition: ClusterDefinition) -> str:
    """Run the pinned coordinator-report reader with SSH stdin left intact."""
    relay_executable = _owned_session_relay_executable(definition)
    return (
        "set -euo pipefail\n"
        "umask 077\n"
        f"{remote_env(definition)}\n"
        f"{relay_executable} session read-cleanup-report-owned\n"
    )


def _owned_teardown_script(
    *,
    definition: ClusterDefinition,
    session_id: str,
    expected_session_generation_id: str,
    expected_cleanup_operation_id: str,
    stop_worker: bool,
    cancel_jobs: bool,
    cancel_scheduler_jobs: bool,
    cluster: str | None,
) -> str:
    if stop_worker and cluster is None:
        raise RelayError("cluster is required when stopping the worker service")
    if cluster is None:
        raise RelayError("cluster is required for owned session teardown")
    request = OwnedSessionTeardownRequest(
        cluster=cluster,
        session_id=session_id,
        expected_session_generation_id=expected_session_generation_id,
        expected_cleanup_operation_id=expected_cleanup_operation_id,
        stop_worker=stop_worker,
        cancel_jobs=cancel_jobs,
        cancel_scheduler_jobs=cancel_scheduler_jobs,
    )
    relay_executable = _owned_session_relay_executable(definition)
    return (
        "set -euo pipefail\n"
        "umask 077\n"
        f"{remote_env(definition)}\n"
        f"printf '%s' {_shell_single_quote(request.model_dump_json())} | "
        f"{relay_executable} session teardown-owned\n"
    )


def _raise_if_relay_executable_missing(
    definition: ClusterDefinition,
    *,
    returncode: int,
    detail: str,
) -> None:
    """Refuse a dead registry pin with a typed, repairable error.

    Shell status 127 proves the remote shell executed nothing, so no durable
    transition can have occurred -- there is no ambiguity to preserve and no
    session to poll (clio-relay#158).
    """
    if returncode != SHELL_COMMAND_NOT_FOUND_STATUS:
        return
    raise relay_executable_missing(
        cluster=definition.name,
        ssh_host=definition.ssh_host,
        relay_executable=definition.relay_executable,
        detail=detail,
        exit_status=returncode,
    )


def _ssh_script(
    definition: ClusterDefinition,
    script: str,
    *,
    timeout_seconds: float = _REMOTE_SESSION_COMMAND_TIMEOUT_SECONDS,
) -> str:
    if timeout_seconds <= 0:
        raise ValueError("remote session command timeout must be positive")
    encoded_script = script.encode("utf-8")
    if len(encoded_script) > _MAX_REMOTE_SESSION_SCRIPT_BYTES:
        raise RelayError("remote session command exceeds its byte limit")
    try:
        result = session_remote_command._run_bounded_command(
            ["ssh", definition.ssh_host, "bash", "-s"],
            input_bytes=encoded_script,
            timeout_seconds=timeout_seconds,
            stdout_limit=_MAX_REMOTE_SESSION_STDOUT_BYTES,
            stderr_limit=_MAX_REMOTE_SESSION_STDERR_BYTES,
        )
    except _BoundedCommandTimeout as exc:
        raise session_remote_command._RemoteSessionCommandDeadline(
            f"remote session command timed out after {timeout_seconds:g} seconds"
        ) from exc
    except RelayError as exc:
        raise RelayError(f"remote session command failed safely: {exc}") from exc
    if result.returncode != 0:
        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        detail = stderr or stdout
        _raise_if_relay_executable_missing(definition, returncode=result.returncode, detail=detail)
        try:
            rejection = OwnedSessionStartRejection.model_validate_json(stdout)
        except ValueError:
            raise session_remote_command._RemoteSessionCommandAmbiguous(
                "remote session transport ended without an exact structured response: "
                f"{detail or f'exit {result.returncode}'}"
            ) from None
        raise session_remote_command._RemoteSessionCommandRejected(rejection)
    return result.stdout.decode("utf-8", errors="replace")


def _ssh_stdin_command(
    definition: ClusterDefinition,
    script: str,
    *,
    input_bytes: bytes,
    input_limit: int,
    stdout_limit: int,
) -> str:
    """Run a small remote command while carrying a separately bounded stdin payload."""
    encoded_script = script.encode("utf-8")
    if len(encoded_script) > _MAX_REMOTE_SESSION_SCRIPT_BYTES:
        raise RelayError("remote session command exceeds its byte limit")
    if input_limit <= 0 or stdout_limit <= 0:
        raise ValueError("remote session input and output limits must be positive")
    if len(input_bytes) > input_limit:
        raise RelayError("remote session stdin exceeds its byte limit")
    remote_command = f"bash -lc {shlex.quote(script)}"
    try:
        result = session_remote_command._run_bounded_command(
            ["ssh", definition.ssh_host, remote_command],
            input_bytes=input_bytes,
            timeout_seconds=_REMOTE_SESSION_COMMAND_TIMEOUT_SECONDS,
            stdout_limit=stdout_limit,
            stderr_limit=_MAX_REMOTE_SESSION_STDERR_BYTES,
        )
    except _BoundedCommandTimeout as exc:
        raise session_remote_command._RemoteSessionCommandDeadline(
            "remote session command timed out after "
            f"{_REMOTE_SESSION_COMMAND_TIMEOUT_SECONDS:g} seconds"
        ) from exc
    except RelayError as exc:
        raise RelayError(f"remote session command failed safely: {exc}") from exc
    if result.returncode != 0:
        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        detail = stderr or stdout
        _raise_if_relay_executable_missing(definition, returncode=result.returncode, detail=detail)
        raise RelayError(f"remote session command failed: {detail}")
    return result.stdout.decode("utf-8", errors="replace")


def _shell_single_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"
