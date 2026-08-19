"""Owned-session API child startup/readiness substrate (#231 rework).

Extracted from ``session_lifecycle.py``: the locally installed release
identity lookup, request-carried cluster registry validation, the loopback
port pre-check, the auth-token selection, health-endpoint polling, the
redacted startup-log tail, and the signed cgroup-bound startup-receipt wait.
Used exclusively by the cluster-local start executor
(``execute_owned_session_start`` and its crash-recovery promotion helper).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import socket
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

import clio_relay.session_process_scope as session_process_scope
import clio_relay.session_startup_receipt as session_startup_receipt
from clio_relay.cluster_config import (
    MAX_CLUSTER_REGISTRY_BYTES,
    ClusterRegistry,
    cluster_route_revision,
)
from clio_relay.errors import RelayError
from clio_relay.session_transaction import _MAX_OWNED_SESSION_DOCUMENT_BYTES
from clio_relay.session_wire_models import MAX_SESSION_START_ERROR_CHARS, SessionApiReleaseIdentity

if TYPE_CHECKING:
    from pathlib import Path

    from clio_relay.session_process_scope import _OwnedGenerationProcess
    from clio_relay.session_transaction import _OwnedSessionTransaction
    from clio_relay.session_wire_models import OwnedSessionStartRequest

_REMOTE_API_READINESS_TIMEOUT_SECONDS = 60.0
_MAX_API_HEALTH_RESPONSE_BYTES = 64 * 1024


def _current_session_api_release_identity() -> SessionApiReleaseIdentity:
    """Return the exact locally installed release identity for an API child."""
    from clio_relay.installation import verified_session_api_install_receipt
    from clio_relay.session_install_identity import release_identity_from_receipt

    receipt = verified_session_api_install_receipt()
    return release_identity_from_receipt(receipt)


def _validated_start_registry(
    request: OwnedSessionStartRequest,
) -> tuple[ClusterRegistry, bytes]:
    """Validate one exact request-carried cluster registry and its route identity."""
    try:
        registry = ClusterRegistry.model_validate(request.cluster_registry)
    except ValueError as exc:
        raise RelayError(f"owned session cluster registry is invalid: {exc}") from exc
    payload = json.dumps(
        registry.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(payload) > MAX_CLUSTER_REGISTRY_BYTES:
        raise RelayError("owned session cluster registry exceeds its byte limit")
    if hashlib.sha256(payload).hexdigest() != request.cluster_registry_sha256:
        raise RelayError("owned session cluster registry digest does not match its payload")
    if set(registry.clusters) != {request.cluster}:
        raise RelayError("owned session cluster registry does not contain one exact cluster")
    definition = registry.clusters[request.cluster]
    if (
        definition.name != request.cluster
        or cluster_route_revision(definition) != request.cluster_route_revision
    ):
        raise RelayError("owned session cluster route identity does not match its registry")
    return registry, payload


def _assert_remote_port_available(port: int) -> None:
    """Fail before core admission changes when the requested loopback port is busy."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind(("127.0.0.1", port))
        except OSError as exc:
            raise RelayError(f"remote API port is already occupied: {port}") from exc


def _owned_session_api_token(*, require_token: bool) -> str | None:
    """Select the child API token while honoring an explicit auth-disabled plan."""
    ambient_token = os.environ.get("CLIO_RELAY_API_TOKEN")
    if require_token and not ambient_token:
        raise RelayError("owned session API token is required but unavailable")
    return ambient_token if require_token else None


def _wait_for_api_ready(
    *,
    process: subprocess.Popen[bytes],
    port: int,
    require_token: bool,
) -> float:
    """Wait boundedly for an API child to report the exact planned auth policy."""
    started = time.monotonic()
    deadline = started + _REMOTE_API_READINESS_TIMEOUT_SECONDS
    url = f"http://127.0.0.1:{port}/healthz"
    last_error = "API did not become ready"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RelayError("owned API process exited before readiness")
        try:
            with urllib.request.urlopen(url, timeout=0.25) as response:
                response_bytes = response.read(_MAX_API_HEALTH_RESPONSE_BYTES + 1)
                if len(response_bytes) > _MAX_API_HEALTH_RESPONSE_BYTES:
                    raise RelayError("owned API health response exceeded its byte limit")
                payload = cast(object, json.loads(response_bytes))
                if (
                    response.status == 200
                    and isinstance(payload, dict)
                    and cast(dict[str, object], payload).get("ok") is True
                    and cast(dict[str, object], payload).get("auth") is require_token
                ):
                    return time.monotonic() - started
                last_error = f"unexpected health response: {payload!r}"
        except (OSError, TimeoutError, urllib.error.URLError, json.JSONDecodeError) as exc:
            last_error = str(exc)
        time.sleep(0.1)
    raise RelayError(
        "owned API did not become ready within "
        f"{_REMOTE_API_READINESS_TIMEOUT_SECONDS:g} seconds: {last_error}"
    )


def _owned_api_startup_log_detail(
    transaction: _OwnedSessionTransaction,
    *,
    secret_values: Iterable[str],
) -> str:
    """Return one bounded credential-redacted API startup diagnostic."""
    redaction_values = tuple(
        sorted(
            {value for value in secret_values if len(value) >= 4},
            key=len,
            reverse=True,
        )
    )
    try:
        maximum_secret_bytes = max(
            (len(value.encode("utf-8")) for value in redaction_values),
            default=0,
        )
    except UnicodeEncodeError:
        return ""
    # Read enough overlap to include the beginning of every known secret whose
    # suffix could otherwise land in the retained diagnostic.  If an
    # unexpectedly enormous environment credential makes that impossible
    # within the owned-document bound, fail closed instead of returning a log
    # fragment that may contain an unrecognizable middle of the credential.
    if maximum_secret_bytes > (_MAX_OWNED_SESSION_DOCUMENT_BYTES - MAX_SESSION_START_ERROR_CHARS):
        return ""
    read_limit = MAX_SESSION_START_ERROR_CHARS + maximum_secret_bytes
    try:
        payload = transaction.read_tail(
            "api.log",
            maximum_bytes=read_limit,
            required=False,
        )
    except RelayError:
        return ""
    if not payload:
        return ""
    # A bounded tail can begin in an unknown credential that is not available
    # through ``secret_values``.  Discard its first partial log line before
    # decoding; retaining that fragment could expose the credential's suffix
    # without its identifying assignment or Authorization prefix.  Equality is
    # treated as truncated deliberately: dropping one complete line for an
    # exactly-sized log is safer than guessing whether the transaction saw the
    # whole file.
    if len(payload) == read_limit:
        _partial, separator, payload = payload.partition(b"\n")
        if not separator:
            return ""
    detail = payload.decode("utf-8", errors="replace").strip()
    for value in redaction_values:
        detail = detail.replace(value, "<redacted>")
    # Redact the complete Authorization value before generic assignments.  A
    # whitespace-delimited assignment rule would consume only ``Bearer`` and
    # leave the actual credential visible as its next token.
    detail = re.sub(
        r"(?im)(\bauthorization['\"]?\s*:\s*)[^\r\n,;]+",
        r"\1<redacted>",
        detail,
    )
    sensitive_assignment = re.compile(
        r"(?i)(\b[a-z0-9_.-]*(?:token|secret|password|credential|api[_-]?key)"
        r"[a-z0-9_.-]*['\"]?\s*[:=]\s*)(?:\"[^\"]*\"|'[^']*'|[^\s,;]+)"
    )
    detail = sensitive_assignment.sub(r"\1<redacted>", detail)
    return detail[-MAX_SESSION_START_ERROR_CHARS:]


def _wait_for_api_startup_receipt(
    *,
    transaction: _OwnedSessionTransaction,
    process: subprocess.Popen[Any],
    receipt_name: str,
    owner_token: str,
    expected: dict[str, object],
    proc_root: Path,
) -> _OwnedGenerationProcess:
    """Wait for and verify the API child's signed cgroup-bound startup receipt."""
    from clio_relay.process_containment import recorded_linux_systemd_scope_process_ids

    expected_keys = {
        "schema_version",
        "cluster",
        "session_id",
        "session_generation_id",
        "api_pid",
        "api_pgid",
        "process_start_ticks",
        "api_release_identity_sha256",
        "cluster_registry_path",
        "cluster_registry_sha256",
        "cluster_route_revision",
        "systemd_unit",
        "systemd_cgroup_path",
        "systemd_invocation_id",
        "systemd_description",
        "observed_at",
        "hmac_sha256",
    }
    deadline = time.monotonic() + _REMOTE_API_READINESS_TIMEOUT_SECONDS
    last_error = "startup receipt did not materialize"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RelayError("owned API containment exited before startup receipt")
        try:
            document = transaction.read_json(receipt_name, required=False)
            if document is None:
                time.sleep(0.05)
                continue
            observed_at = document.get("observed_at")
            parsed_observed_at = (
                datetime.fromisoformat(observed_at) if isinstance(observed_at, str) else None
            )
            api_pid = document.get("api_pid")
            api_pgid = document.get("api_pgid")
            process_start = document.get("process_start_ticks")
            signature = document.get("hmac_sha256")
            exact_expected = all(document.get(key) == value for key, value in expected.items())
            if not (
                set(document) == expected_keys
                and document.get("schema_version") == "clio-relay.owner-session-api-startup.v1"
                and exact_expected
                and isinstance(api_pid, int)
                and not isinstance(api_pid, bool)
                and api_pid > 1
                and isinstance(api_pgid, int)
                and not isinstance(api_pgid, bool)
                and api_pgid > 0
                and isinstance(process_start, str)
                and process_start.isdigit()
                and parsed_observed_at is not None
                and parsed_observed_at.tzinfo is not None
                and isinstance(signature, str)
                and hmac.compare_digest(
                    signature,
                    session_startup_receipt._startup_receipt_signature(
                        document, owner_token=owner_token
                    ),
                )
            ):
                raise RelayError("owned API startup receipt identity is invalid")
            process_identity = session_process_scope._read_proc_identity(
                proc_root=proc_root, pid=api_pid
            )
            if (
                process_identity.process_group_id != api_pgid
                or process_identity.start_ticks != process_start
                or not session_process_scope._is_clio_relay_api_leader(
                    proc_root=proc_root, pid=api_pid
                )
            ):
                raise RelayError("owned API startup receipt process identity changed")
            pids = recorded_linux_systemd_scope_process_ids(
                unit=cast(str, expected["systemd_unit"]),
                cgroup_path=cast(str, expected["systemd_cgroup_path"]),
                invocation_id=cast(str, expected["systemd_invocation_id"]),
                description=cast(str, expected["systemd_description"]),
            )
            if api_pid not in pids:
                raise RelayError("owned API startup receipt leader is outside its exact cgroup")
            return process_identity
        except (OSError, RelayError, ValueError) as exc:
            last_error = str(exc)
            time.sleep(0.05)
    raise RelayError(f"owned API startup receipt was not verified: {last_error}")
