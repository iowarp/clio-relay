"""Owned-session start-attempt journal validation and migration (#231 rework).

Extracted from ``session_lifecycle.py``: the structural validator for the
durable start-attempt journal (every schema generation, v1 through v3), the
retry-selector-scoped wrapper used by resumable starts, the legacy (v1)
identity-match check, and the v1-to-current migration writer. Also carries
``_write_session_attempt``, the shared atomic attempt-journal writer used by
both start and teardown attempts.

This module depends on nothing else in the split except the transaction
primitive and process-scope leaves -- in particular it does NOT call
``inspect_owned_session_recovery_status``, so ``session_lifecycle.py`` (which
stays resident and depends on ``_validated_start_attempt``) can import from
here without creating a cycle.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal, cast

import clio_relay.session_process_scope as session_process_scope
from clio_relay.errors import RelayError
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.session_wire_models import MAX_SESSION_START_ERROR_CHARS, OwnedSessionInputPolicy

if TYPE_CHECKING:
    from pathlib import Path

    from clio_relay.session_transaction import _OwnedSessionTransaction
    from clio_relay.session_wire_models import OwnedSessionStartRequest

_MAX_PROC_RECORD_BYTES = 1024 * 1024


def _write_session_attempt(
    transaction: _OwnedSessionTransaction,
    *,
    operation: Literal["start", "teardown"],
    identity: dict[str, object],
    error: str | None = None,
) -> None:
    """Write one atomic, resumable owner-session attempt record."""
    document = {
        "schema_version": (
            "clio-relay.owner-session-attempt.v3"
            if operation == "start"
            else "clio-relay.owner-session-attempt.v1"
        ),
        "operation": operation,
        **identity,
        "observed_at": datetime.now(UTC).isoformat(),
        "error": error[:MAX_SESSION_START_ERROR_CHARS] if error is not None else None,
    }
    transaction.atomic_write(
        f"{operation}-attempt.json",
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    )


def _validated_start_attempt(
    transaction: _OwnedSessionTransaction,
    *,
    cluster: str,
    session_id: str,
    start_operation_id: str | None = None,
    cluster_registry_sha256: str | None = None,
    cluster_route_revision_value: str | None = None,
    remote_api_port: int | None = None,
    replace: bool | None = None,
    require_token: bool | None = None,
    input_policy: OwnedSessionInputPolicy | None = None,
    expected_api_release_identity_sha256: str | None = None,
    allow_legacy: bool = False,
) -> dict[str, object] | None:
    """Return one structurally exact start journal matching optional selectors."""
    attempt = transaction.read_json("start-attempt.json", required=False)
    if attempt is None:
        return None
    expected_keys = {
        "schema_version",
        "operation",
        "cluster",
        "session_id",
        "start_operation_id",
        "session_generation_id",
        "owner_token",
        "owner_token_sha256",
        "api_release_identity_sha256",
        "expected_api_release_identity_sha256",
        "cluster_registry_path",
        "cluster_registry_sha256",
        "cluster_route_revision",
        "remote_api_port",
        "replace",
        "require_token",
        "input_policy",
        "start_phase",
        "systemd_unit",
        "systemd_description",
        "systemd_cgroup_path",
        "systemd_invocation_id",
        "containment_broker_pid",
        "containment_broker_start_identity",
        "observed_at",
        "error",
    }
    pre_policy_keys = expected_keys - {"input_policy"}
    legacy_keys = pre_policy_keys - {
        "start_operation_id",
        "expected_api_release_identity_sha256",
        "replace",
        "require_token",
    }
    legacy = attempt.get("schema_version") == "clio-relay.owner-session-attempt.v1"
    pre_policy = attempt.get("schema_version") == "clio-relay.owner-session-attempt.v2"
    current = attempt.get("schema_version") == "clio-relay.owner-session-attempt.v3"
    raw_input_policy = attempt.get("input_policy")
    try:
        recorded_input_policy = (
            OwnedSessionInputPolicy.model_validate(raw_input_policy) if current else None
        )
    except ValueError:
        recorded_input_policy = None
    generation = attempt.get("session_generation_id")
    operation_id = attempt.get("start_operation_id")
    observed_at = attempt.get("observed_at")
    try:
        validated_generation = (
            validate_durable_record_id(generation) if isinstance(generation, str) else None
        )
        validated_operation_id = (
            validate_durable_record_id(operation_id) if isinstance(operation_id, str) else None
        )
        parsed_observed_at = (
            datetime.fromisoformat(observed_at) if isinstance(observed_at, str) else None
        )
    except ValueError:
        validated_generation = None
        validated_operation_id = None
        parsed_observed_at = None
    registry_path = attempt.get("cluster_registry_path")
    owner_token = attempt.get("owner_token")
    owner_token_sha256 = attempt.get("owner_token_sha256")
    start_phase = attempt.get("start_phase")
    systemd_unit = attempt.get("systemd_unit")
    systemd_description = attempt.get("systemd_description")
    cgroup_path = attempt.get("systemd_cgroup_path")
    invocation_id = attempt.get("systemd_invocation_id")
    broker_pid = attempt.get("containment_broker_pid")
    broker_start = attempt.get("containment_broker_start_identity")
    expected_registry_path = (
        transaction.path / f"cluster-registry-{validated_generation}.json"
        if validated_generation is not None
        else None
    )
    if not (
        set(attempt)
        == (legacy_keys if legacy else pre_policy_keys if pre_policy else expected_keys)
        and (current or pre_policy or (allow_legacy and legacy))
        and attempt.get("operation") == "start"
        and attempt.get("cluster") == cluster
        and attempt.get("session_id") == session_id
        and (
            (not legacy and validated_operation_id is not None)
            or (legacy and validated_operation_id is None)
        )
        and (
            start_operation_id is None
            or (not legacy and validated_operation_id == start_operation_id)
        )
        and validated_generation is not None
        and isinstance(owner_token, str)
        and len(owner_token) == 64
        and all(character in "0123456789abcdef" for character in owner_token)
        and owner_token_sha256 == hashlib.sha256(owner_token.encode("utf-8")).hexdigest()
        and isinstance(attempt.get("api_release_identity_sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", cast(str, attempt.get("api_release_identity_sha256")))
        is not None
        and (
            legacy
            or (
                attempt.get("expected_api_release_identity_sha256") is None
                or (
                    isinstance(attempt.get("expected_api_release_identity_sha256"), str)
                    and re.fullmatch(
                        r"[0-9a-f]{64}",
                        cast(str, attempt.get("expected_api_release_identity_sha256")),
                    )
                    is not None
                )
            )
        )
        and (
            expected_api_release_identity_sha256 is None
            or (
                not legacy
                and attempt.get("expected_api_release_identity_sha256")
                == expected_api_release_identity_sha256
            )
        )
        and registry_path == str(expected_registry_path)
        and isinstance(attempt.get("cluster_registry_sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", cast(str, attempt.get("cluster_registry_sha256")))
        is not None
        and (
            cluster_registry_sha256 is None
            or attempt.get("cluster_registry_sha256") == cluster_registry_sha256
        )
        and isinstance(attempt.get("cluster_route_revision"), str)
        and bool(attempt.get("cluster_route_revision"))
        and (
            cluster_route_revision_value is None
            or attempt.get("cluster_route_revision") == cluster_route_revision_value
        )
        and isinstance(attempt.get("remote_api_port"), int)
        and not isinstance(attempt.get("remote_api_port"), bool)
        and 0 < cast(int, attempt.get("remote_api_port")) <= 65_535
        and (remote_api_port is None or attempt.get("remote_api_port") == remote_api_port)
        and (legacy or isinstance(attempt.get("replace"), bool))
        and (replace is None or (not legacy and attempt.get("replace") is replace))
        and (legacy or isinstance(attempt.get("require_token"), bool))
        and (
            require_token is None or (not legacy and attempt.get("require_token") is require_token)
        )
        and (not current or recorded_input_policy is not None)
        and (input_policy is None or (current and recorded_input_policy == input_policy))
        and start_phase in {"pending", "admitted", "scope_bound", "contained"}
        and systemd_unit == f"clio-relay-session-{validated_generation}.scope"
        and isinstance(systemd_description, str)
        and systemd_description.startswith(
            f"clio-relay-owned-session:{session_id}:{validated_generation}:"
        )
        and (
            (
                start_phase in {"pending", "admitted"}
                and cgroup_path is None
                and invocation_id is None
                and broker_pid is None
                and broker_start is None
            )
            or (
                start_phase in {"scope_bound", "contained"}
                and isinstance(cgroup_path, str)
                and bool(cgroup_path)
                and isinstance(invocation_id, str)
                and len(invocation_id) == 32
                and all(character in "0123456789abcdef" for character in invocation_id)
                and (
                    (start_phase == "scope_bound" and broker_pid is None and broker_start is None)
                    or (
                        start_phase == "contained"
                        and isinstance(broker_pid, int)
                        and not isinstance(broker_pid, bool)
                        and broker_pid > 1
                        and isinstance(broker_start, str)
                        and bool(broker_start)
                    )
                )
            )
        )
        and parsed_observed_at is not None
        and parsed_observed_at.tzinfo is not None
        and (
            attempt.get("error") is None
            or (
                isinstance(attempt.get("error"), str)
                and len(cast(str, attempt.get("error"))) <= MAX_SESSION_START_ERROR_CHARS
            )
        )
    ):
        raise RelayError("prior owned-session start attempt identity is invalid")
    return attempt


def _validated_resumable_start_attempt(
    transaction: _OwnedSessionTransaction,
    *,
    request: OwnedSessionStartRequest,
    release_identity_sha256: str,
) -> dict[str, object] | None:
    """Return the exact prior start attempt selected by a retry request."""
    expected_release_sha256 = (
        request.expected_api_release_identity.sha256()
        if request.expected_api_release_identity is not None
        else None
    )
    attempt = _validated_start_attempt(
        transaction,
        cluster=request.cluster,
        session_id=request.session_id,
        start_operation_id=request.start_operation_id,
        cluster_registry_sha256=request.cluster_registry_sha256,
        cluster_route_revision_value=request.cluster_route_revision,
        remote_api_port=request.remote_api_port,
        replace=request.replace,
        require_token=request.require_token,
        input_policy=request.input_policy,
        expected_api_release_identity_sha256=expected_release_sha256,
    )
    if attempt is not None and (
        attempt.get("expected_api_release_identity_sha256") != expected_release_sha256
        or attempt.get("api_release_identity_sha256") != release_identity_sha256
    ):
        raise RelayError("prior owned-session start release identity changed")
    return attempt


def _legacy_start_attempt_matches_metadata(
    *,
    attempt: dict[str, object],
    metadata: dict[str, object],
) -> bool:
    """Return whether a v1 start journal names the exact committed generation."""
    identity_fields = (
        "cluster",
        "session_id",
        "session_generation_id",
        "owner_token",
        "api_release_identity_sha256",
        "cluster_registry_path",
        "cluster_registry_sha256",
        "cluster_route_revision",
        "remote_api_port",
        "systemd_unit",
        "systemd_description",
        "systemd_cgroup_path",
        "systemd_invocation_id",
        "containment_broker_pid",
        "containment_broker_start_identity",
    )
    owner_token = metadata.get("owner_token")
    return bool(
        attempt.get("start_phase") == "contained"
        and all(attempt.get(field) == metadata.get(field) for field in identity_fields)
        and isinstance(owner_token, str)
        and attempt.get("owner_token_sha256")
        == hashlib.sha256(owner_token.encode("utf-8")).hexdigest()
    )


def _migrate_legacy_start_attempt(
    transaction: _OwnedSessionTransaction,
    *,
    request: OwnedSessionStartRequest,
    release_identity_sha256: str,
    replacement_identity_verified: bool = False,
) -> dict[str, object] | None:
    """Bind a valid pre-v2 attempt to a caller-supplied planned operation.

    Version 1 did not contain an operation selector or the complete request
    policy, so it is never exposed as a queryable start.  A new planned request
    may adopt its exact generation only after every identity v1 did record has
    matched; a failed legacy attempt additionally requires explicit replacement.
    """
    attempt = _validated_start_attempt(
        transaction,
        cluster=request.cluster,
        session_id=request.session_id,
        cluster_registry_sha256=request.cluster_registry_sha256,
        cluster_route_revision_value=request.cluster_route_revision,
        remote_api_port=request.remote_api_port,
        allow_legacy=True,
    )
    if attempt is None or attempt.get("schema_version") == "clio-relay.owner-session-attempt.v3":
        return attempt
    release_changed = attempt.get("api_release_identity_sha256") != release_identity_sha256
    if release_changed and not (request.replace and replacement_identity_verified):
        raise RelayError("legacy owned-session start release identity changed")
    if attempt.get("error") is not None and not request.replace:
        raise RelayError("a failed legacy owned-session start requires --replace")
    identity = {
        key: value
        for key, value in attempt.items()
        if key not in {"schema_version", "operation", "observed_at", "error"}
    }
    identity.update(
        {
            "start_operation_id": request.start_operation_id,
            "api_release_identity_sha256": release_identity_sha256,
            "expected_api_release_identity_sha256": (
                request.expected_api_release_identity.sha256()
                if request.expected_api_release_identity is not None
                else None
            ),
            "replace": request.replace,
            "require_token": request.require_token,
            "input_policy": request.input_policy.model_dump(mode="json"),
        }
    )
    _write_session_attempt(transaction, operation="start", identity=identity)
    return _validated_resumable_start_attempt(
        transaction,
        request=request,
        release_identity_sha256=release_identity_sha256,
    )


def _owned_api_requires_token(*, proc_root: Path, pid: int) -> bool:
    """Read the exact verified API leader argv and return its auth policy."""
    try:
        arguments = session_process_scope._read_bounded_proc_bytes(
            proc_root / str(pid) / "cmdline",
            maximum_bytes=_MAX_PROC_RECORD_BYTES,
        ).split(bytes([0]))
    except (FileNotFoundError, ProcessLookupError) as exc:
        raise RelayError("owned API leader disappeared during auth verification") from exc
    if not session_process_scope._is_clio_relay_api_leader(proc_root=proc_root, pid=pid):
        raise RelayError("owned API auth policy cannot be tied to the verified leader")
    return b"--require-token" in arguments
