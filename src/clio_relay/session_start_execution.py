"""Owned remote relay session start execution.

split/session-lifecycle slice J (#231): the crash-surviving start-promotion
helpers and the two execute_owned_session_* entry points that perform an
actual remote session start moved out of session_lifecycle.py, which stays
the resident home for inspect_owned_session_recovery_status (the read path
these functions verify against) and the handful of module-level constants
below that both sides still need to agree on. session_lifecycle is imported
back INSIDE each top-level function (not at module scope): session_lifecycle
imports this module for its cli.py-compatibility re-export block, so a
module-scope back-import here creates a load-order-dependent circular
import -- deferred to call time, it is import-order-independent, matching
the standard pattern for breaking a two-module cycle.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import subprocess
import sys
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast
from uuid import uuid4

import clio_relay.session_api_readiness as session_api_readiness
import clio_relay.session_process_scope as session_process_scope
import clio_relay.session_start_attempt_validation as session_start_attempt_validation
from clio_relay.cluster_config import MAX_CLUSTER_REGISTRY_BYTES
from clio_relay.config import ALLOW_UNAUTHENTICATED_OWNED_SESSION_ENV
from clio_relay.errors import RelayError
from clio_relay.session_validation import _validate_session
from clio_relay.session_wire_models import (
    OwnedSessionIdentityChallengeRequest,
    OwnedSessionRecoveryStatus,
    OwnedSessionStartReceipt,
    OwnedSessionStartRequest,
    SessionApiReleaseIdentity,
)

if TYPE_CHECKING:
    from clio_relay.session_transaction import _OwnedSessionTransaction


class _OwnedSessionQueue(Protocol):
    """Typed core-queue surface required by crash-surviving start promotion."""

    root: Path

    def clear_owner_session_closing(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
    ) -> None:
        """Clear a matching closing marker after exact API recovery."""


class _RecoveredStartProbe:
    """Minimal process observation used while adopting an exact persistent scope."""

    def poll(self) -> None:
        """The receipt and scope checks, not a stale parent handle, prove liveness."""
        return None


def _promote_resumable_contained_start(
    *,
    transaction: _OwnedSessionTransaction,
    attempt: dict[str, object],
    request: OwnedSessionStartRequest,
    release_identity: SessionApiReleaseIdentity,
    queue: _OwnedSessionQueue,
    proc_root: Path,
    home: Path | None,
) -> OwnedSessionStartReceipt | None:
    """Commit ready metadata when an exact crash-surviving API already exists."""
    # Deferred import: breaks the module-load-time cycle with session_lifecycle
    # (which imports this module back for its cli.py-compatibility re-export
    # block) -- see the module docstring.
    import clio_relay.session_lifecycle as session_lifecycle

    if attempt.get("start_phase") != "contained" or attempt.get("error") is not None:
        return None
    generation_id = cast(str, attempt["session_generation_id"])
    owner_token = cast(str, attempt["owner_token"])
    receipt_name = f"api-startup-{generation_id}.json"
    receipt_path = transaction.path / receipt_name
    expected_receipt = {
        "cluster": request.cluster,
        "session_id": request.session_id,
        "session_generation_id": generation_id,
        "api_release_identity_sha256": release_identity.sha256(),
        "cluster_registry_path": attempt["cluster_registry_path"],
        "cluster_registry_sha256": request.cluster_registry_sha256,
        "cluster_route_revision": request.cluster_route_revision,
        "systemd_unit": attempt["systemd_unit"],
        "systemd_cgroup_path": attempt["systemd_cgroup_path"],
        "systemd_invocation_id": attempt["systemd_invocation_id"],
        "systemd_description": attempt["systemd_description"],
    }
    probe = cast(subprocess.Popen[Any], _RecoveredStartProbe())
    try:
        process_identity = session_api_readiness._wait_for_api_startup_receipt(
            transaction=transaction,
            process=probe,
            receipt_name=receipt_name,
            owner_token=owner_token,
            expected=expected_receipt,
            proc_root=proc_root,
        )
        ready_seconds = session_api_readiness._wait_for_api_ready(
            process=cast(subprocess.Popen[bytes], probe),
            port=request.remote_api_port,
            require_token=request.require_token,
        )
        final_process_identity = session_api_readiness._wait_for_api_startup_receipt(
            transaction=transaction,
            process=probe,
            receipt_name=receipt_name,
            owner_token=owner_token,
            expected=expected_receipt,
            proc_root=proc_root,
        )
        if final_process_identity != process_identity:
            raise RelayError("recovered owned API identity changed after health verification")
    except RelayError:
        return None
    receipt_payload = transaction.read_bytes(
        receipt_name,
        maximum_bytes=session_lifecycle._MAX_API_STARTUP_RECEIPT_BYTES,
    )
    if receipt_payload is None:  # pragma: no cover - required read
        return None
    metadata = {
        "cluster": request.cluster,
        "session_id": request.session_id,
        "remote_api_port": request.remote_api_port,
        "api_pid": process_identity.pid,
        "api_pgid": process_identity.process_group_id,
        "owner_token": owner_token,
        "session_generation_id": generation_id,
        "api_release_identity": release_identity.model_dump(mode="json"),
        "api_release_identity_sha256": release_identity.sha256(),
        "cluster_registry_path": attempt["cluster_registry_path"],
        "cluster_registry_sha256": request.cluster_registry_sha256,
        "cluster_route_revision": request.cluster_route_revision,
        "cluster_authority_verified": True,
        "input_policy": request.input_policy.model_dump(mode="json"),
        "process_start_ticks": process_identity.start_ticks,
        "containment_mode": "linux_systemd_scope",
        "systemd_unit": attempt["systemd_unit"],
        "systemd_cgroup_path": attempt["systemd_cgroup_path"],
        "systemd_invocation_id": attempt["systemd_invocation_id"],
        "systemd_description": attempt["systemd_description"],
        "containment_broker_pid": attempt["containment_broker_pid"],
        "containment_broker_start_identity": attempt["containment_broker_start_identity"],
        "api_startup_receipt_path": str(receipt_path),
        "api_startup_receipt_sha256": hashlib.sha256(receipt_payload).hexdigest(),
        "started_at": datetime.now(UTC).isoformat(),
        "owner": "clio-relay",
    }
    transaction.atomic_write("api.pid", f"{process_identity.pid}\n".encode("ascii"))
    transaction.atomic_write("metadata.json", json.dumps(metadata, indent=2).encode("utf-8"))
    queue.clear_owner_session_closing(request.session_id, session_generation_id=generation_id)
    promoted_status = session_lifecycle.inspect_owned_session_recovery_status(
        cluster=request.cluster,
        session_id=request.session_id,
        core_dir=queue.root,
        home=home,
        proc_root=proc_root,
        transaction=transaction,
        expected_start_operation_id=request.start_operation_id,
        expected_cluster_route_revision=request.cluster_route_revision,
    )
    if not (
        promoted_status.recovery_verified
        and promoted_status.leader_process_state == "owned_running"
        and promoted_status.api_pid == process_identity.pid
        and promoted_status.ownership_verified
        and promoted_status.session_generation_id == generation_id
        and promoted_status.start_attempt_verified
    ):
        raise RelayError("recovered owned API did not pass post-commit identity verification")
    return OwnedSessionStartReceipt(
        cluster=request.cluster,
        session_id=request.session_id,
        start_operation_id=request.start_operation_id,
        cluster_route_revision=request.cluster_route_revision,
        session_generation_id=generation_id,
        remote_api_port=request.remote_api_port,
        api_pid=process_identity.pid,
        outcome="recovered",
        ready_seconds=ready_seconds,
    )


def execute_owned_session_identity_challenge(
    request: OwnedSessionIdentityChallengeRequest,
    *,
    home: Path | None = None,
    core_dir: Path | None = None,
    proc_root: Path = Path("/proc"),
) -> dict[str, object]:
    """Sign one nonce only after pinned metadata and live leader verification."""
    # Deferred import: breaks the module-load-time cycle with session_lifecycle
    # (which imports this module back for its cli.py-compatibility re-export
    # block) -- see the module docstring.
    import clio_relay.session_lifecycle as session_lifecycle
    from clio_relay.config import RelaySettings

    settings_core_dir = RelaySettings.from_env().core_dir if core_dir is None else core_dir
    get_effective_uid = cast(Callable[[], int] | None, getattr(os, "geteuid", None))
    if get_effective_uid is None:
        raise RelayError("owned session challenge cannot verify the effective user")
    uid = get_effective_uid()
    with session_lifecycle.open_owned_session_transaction(
        session_id=request.session_id,
        create=False,
        timeout_seconds=10.0,
        home=home,
    ) as transaction:
        document = transaction.read_json("metadata.json")
        if document is None:  # pragma: no cover - required read
            raise RelayError("owned session metadata is unavailable")
        status = session_lifecycle.inspect_owned_session_recovery_status(
            cluster=request.cluster,
            session_id=request.session_id,
            core_dir=settings_core_dir,
            home=home,
            proc_root=proc_root,
            effective_uid=uid,
            transaction=transaction,
        )
        if not (
            status.recovery_verified
            and status.session_generation_id == request.session_generation_id
            and status.running
            and status.leader_process_state == "owned_running"
            and status.api_pid is not None
            and status.api_pid in status.generation_process_pids
        ):
            detail = "; ".join(status.errors) or "live API leader proof was incomplete"
            raise RelayError(f"owned session identity challenge was refused: {detail}")
        owner_token = document.get("owner_token")
        if not isinstance(owner_token, str) or len(owner_token) != 64:
            raise RelayError("owned session identity challenge token is invalid")
        message = "\n".join(
            (
                "clio-relay.session-identity.v1",
                request.cluster,
                request.session_id,
                request.session_generation_id,
                request.nonce,
            )
        ).encode("utf-8")
        signature = hmac.new(
            owner_token.encode("utf-8"),
            message,
            hashlib.sha256,
        ).hexdigest()
        return {
            "schema_version": "clio-relay.session-identity.v1",
            "cluster": request.cluster,
            "session_id": request.session_id,
            "session_generation_id": request.session_generation_id,
            "nonce": request.nonce,
            "hmac_sha256": signature,
        }


def execute_owned_session_start(
    request: OwnedSessionStartRequest,
    *,
    home: Path | None = None,
    core_dir: Path | None = None,
    proc_root: Path = Path("/proc"),
) -> OwnedSessionStartReceipt:
    """Execute one exact cluster-local start under the pinned session transaction."""
    # Deferred import: breaks the module-load-time cycle with session_lifecycle
    # (which imports this module back for its cli.py-compatibility re-export
    # block) -- see the module docstring.
    import clio_relay.session_lifecycle as session_lifecycle
    from clio_relay.config import RelaySettings
    from clio_relay.core_queue import ClioCoreQueue

    _validate_session(
        session_id=request.session_id,
        remote_api_port=request.remote_api_port,
    )
    _, registry_payload = session_api_readiness._validated_start_registry(request)
    release_identity = session_api_readiness._current_session_api_release_identity()
    if (
        request.expected_api_release_identity is not None
        and release_identity != request.expected_api_release_identity
    ):
        from clio_relay.session_install_identity import release_identity_is_accepted

        if not release_identity_is_accepted(
            release_identity,
            request.expected_api_release_identity,
        ):
            raise RelayError("session API installation changed after compatibility verification")
    api_token = session_api_readiness._owned_session_api_token(require_token=request.require_token)
    settings_core_dir = RelaySettings.from_env().core_dir if core_dir is None else core_dir
    queue = ClioCoreQueue(settings_core_dir)
    get_effective_uid = cast(Callable[[], int] | None, getattr(os, "geteuid", None))
    if get_effective_uid is None:
        raise RelayError("owned session start cannot verify the effective user")
    uid = get_effective_uid()

    with session_lifecycle.open_owned_session_transaction(
        session_id=request.session_id,
        create=True,
        timeout_seconds=10.0,
        home=home,
    ) as transaction:
        existing = transaction.read_json("metadata.json", required=False)
        raw_attempt = transaction.read_json("start-attempt.json", required=False)
        legacy_migrated = bool(
            raw_attempt is not None
            and raw_attempt.get("schema_version")
            in {
                "clio-relay.owner-session-attempt.v1",
                "clio-relay.owner-session-attempt.v2",
            }
        )
        if legacy_migrated:
            legacy_attempt = session_start_attempt_validation._validated_start_attempt(
                transaction,
                cluster=request.cluster,
                session_id=request.session_id,
                cluster_registry_sha256=request.cluster_registry_sha256,
                cluster_route_revision_value=request.cluster_route_revision,
                remote_api_port=request.remote_api_port,
                allow_legacy=True,
            )
            if legacy_attempt is None:  # pragma: no cover - raw attempt exists
                raise RelayError("legacy owned-session start attempt disappeared")
            replacement_identity_verified = False
            if existing is not None:
                if not request.replace:
                    raise RelayError(
                        "a pre-policy owned session requires --replace before input policy "
                        "can be proven"
                    )
                legacy_status = session_lifecycle.inspect_owned_session_recovery_status(
                    cluster=request.cluster,
                    session_id=request.session_id,
                    core_dir=settings_core_dir,
                    home=home,
                    proc_root=proc_root,
                    effective_uid=uid,
                )
                if not (
                    legacy_status.recovery_verified
                    and legacy_status.ownership_verified
                    and legacy_status.session_generation_id
                    == legacy_attempt.get("session_generation_id")
                    and legacy_status.cluster_route_revision
                    == legacy_attempt.get("cluster_route_revision")
                    and legacy_status.remote_api_port == legacy_attempt.get("remote_api_port")
                    and legacy_status.api_release_identity is not None
                    and legacy_status.api_release_identity.sha256()
                    == legacy_attempt.get("api_release_identity_sha256")
                    and session_start_attempt_validation._legacy_start_attempt_matches_metadata(
                        attempt=legacy_attempt,
                        metadata=existing,
                    )
                ):
                    raise RelayError(
                        "legacy start journal does not match exact verified session metadata"
                    )
                replacement_identity_verified = True
                if not request.replace:
                    if not (
                        legacy_status.running
                        and legacy_status.leader_process_state == "owned_running"
                        and legacy_status.api_pid is not None
                    ):
                        raise RelayError(
                            "legacy owned session cannot be adopted without exact live proof; "
                            "use --replace"
                        )
                    if (
                        session_start_attempt_validation._owned_api_requires_token(
                            proc_root=proc_root,
                            pid=legacy_status.api_pid,
                        )
                        is not request.require_token
                    ):
                        raise RelayError("legacy owned session auth policy differs; use --replace")
            elif (
                request.replace
                and legacy_attempt.get("api_release_identity_sha256") != release_identity.sha256()
            ):
                legacy_generation = cast(str, legacy_attempt["session_generation_id"])
                legacy_phase = cast(str, legacy_attempt["start_phase"])
                admission = queue.owner_session_generation_status(
                    request.session_id,
                    session_generation_id=legacy_generation,
                )
                active_generation = admission.get("active_generation_id")
                admission_verified = bool(
                    admission.get("owner_session_id") == request.session_id
                    and admission.get("session_generation_id") == legacy_generation
                    and admission.get("closing_generation_id") is None
                    and (
                        (
                            legacy_phase == "pending"
                            and active_generation in {None, legacy_generation}
                        )
                        or (
                            legacy_phase != "pending"
                            and active_generation == legacy_generation
                            and admission.get("open") is True
                        )
                    )
                )
                if not admission_verified:
                    raise RelayError(
                        "legacy owned-session generation conflicts with durable core admission"
                    )
                if legacy_phase in {"scope_bound", "contained"}:
                    session_process_scope._recorded_scope_processes(
                        proc_root=proc_root,
                        systemd_unit=cast(str, legacy_attempt["systemd_unit"]),
                        systemd_cgroup_path=cast(
                            str,
                            legacy_attempt["systemd_cgroup_path"],
                        ),
                        systemd_invocation_id=cast(
                            str,
                            legacy_attempt["systemd_invocation_id"],
                        ),
                        systemd_description=cast(
                            str,
                            legacy_attempt["systemd_description"],
                        ),
                    )
                replacement_identity_verified = True
            elif (
                existing is None
                and legacy_attempt.get("start_phase") == "contained"
                and not request.replace
            ):
                raise RelayError(
                    "legacy contained start requires --replace because v1 did not bind auth policy"
                )
            prior_attempt = session_start_attempt_validation._migrate_legacy_start_attempt(
                transaction,
                request=request,
                release_identity_sha256=release_identity.sha256(),
                replacement_identity_verified=replacement_identity_verified,
            )
        else:
            prior_attempt = session_start_attempt_validation._validated_start_attempt(
                transaction,
                cluster=request.cluster,
                session_id=request.session_id,
            )
        exact_prior_attempt: dict[str, object] | None = None
        if (
            prior_attempt is not None
            and prior_attempt.get("start_operation_id") == request.start_operation_id
        ):
            exact_prior_attempt = (
                session_start_attempt_validation._validated_resumable_start_attempt(
                    transaction,
                    request=request,
                    release_identity_sha256=release_identity.sha256(),
                )
            )
        if (
            existing is None
            and prior_attempt is not None
            and prior_attempt.get("error") is not None
        ):
            if prior_attempt.get("start_operation_id") == request.start_operation_id:
                raise RelayError("owned-session start operation already failed terminally")
            if not request.replace:
                raise RelayError("a new start operation requires --replace after terminal failure")
            if not (
                prior_attempt.get("api_release_identity_sha256") == release_identity.sha256()
                and prior_attempt.get("cluster_registry_sha256") == request.cluster_registry_sha256
                and prior_attempt.get("cluster_route_revision") == request.cluster_route_revision
                and prior_attempt.get("remote_api_port") == request.remote_api_port
            ):
                raise RelayError(
                    "failed owned-session generation identity changed before replacement"
                )
            replacement_attempt = {
                key: value
                for key, value in prior_attempt.items()
                if key not in {"schema_version", "operation", "observed_at", "error"}
            }
            replacement_attempt.update(
                {
                    "start_operation_id": request.start_operation_id,
                    "replace": request.replace,
                    "require_token": request.require_token,
                    "input_policy": request.input_policy.model_dump(mode="json"),
                    "expected_api_release_identity_sha256": (
                        request.expected_api_release_identity.sha256()
                        if request.expected_api_release_identity is not None
                        else None
                    ),
                }
            )
            session_start_attempt_validation._write_session_attempt(
                transaction,
                operation="start",
                identity=replacement_attempt,
            )
        resumable_attempt = (
            exact_prior_attempt
            or session_start_attempt_validation._validated_resumable_start_attempt(
                transaction,
                request=request,
                release_identity_sha256=release_identity.sha256(),
            )
            if existing is None
            else None
        )
        recorded_generation: str | None = None
        existing_status: OwnedSessionRecoveryStatus | None = None
        if existing is not None:
            existing_status = session_lifecycle.inspect_owned_session_recovery_status(
                cluster=request.cluster,
                session_id=request.session_id,
                core_dir=settings_core_dir,
                home=home,
                proc_root=proc_root,
                effective_uid=uid,
                transaction=transaction,
            )
            if not existing_status.recovery_verified:
                detail = "; ".join(existing_status.errors) or "recovery proof was incomplete"
                raise RelayError(f"existing owned session recovery was refused: {detail}")
            recorded_generation = existing_status.session_generation_id
            if recorded_generation is None:
                raise RelayError("existing owned session has no durable generation")
            if existing_status.cleanup_receipt:
                if existing.get("cleanup_paths_pending") is True:
                    raise RelayError(
                        "owned session cleanup receipt still has pending file deletion"
                    )
                if not (
                    isinstance(existing_status.admission_status, dict)
                    and existing_status.admission_status.get("closed") is True
                ):
                    raise RelayError(
                        "owned session cleanup is complete but its authoritative generation "
                        "is still closing; retry after the teardown coordinator marks it closed"
                    )
            else:
                same_completed_operation = bool(
                    not legacy_migrated
                    and prior_attempt is not None
                    and prior_attempt.get("start_operation_id") == request.start_operation_id
                    and existing_status.start_attempt_verified
                    and existing_status.start_state == "ready"
                )
                if same_completed_operation and (request.replace or not existing_status.running):
                    raise RelayError(
                        "owned-session start operation already completed; use a fresh operation id"
                    )
                existing_release = existing_status.api_release_identity
                registry_matches = bool(
                    existing.get("cluster_registry_sha256") == request.cluster_registry_sha256
                    and existing.get("cluster_route_revision") == request.cluster_route_revision
                )
                release_matches = existing_release == release_identity
                port_matches = existing_status.remote_api_port == request.remote_api_port
                input_policy_matches = existing.get("input_policy") == (
                    request.input_policy.model_dump(mode="json")
                )
                if existing_status.running and existing_status.leader_process_state != (
                    "owned_running"
                ):
                    if not request.replace:
                        raise RelayError(
                            "owned session API leader is absent while generation children remain; "
                            "use --replace"
                        )
                elif existing_status.running and not request.replace:
                    if not (
                        registry_matches
                        and release_matches
                        and port_matches
                        and input_policy_matches
                    ):
                        raise RelayError("existing owned session identity differs; use --replace")
                    if (
                        prior_attempt is None
                        or prior_attempt.get("require_token") is not request.require_token
                    ):
                        raise RelayError(
                            "existing owned session token policy is not proven; use --replace"
                        )
                    existing_owner_token = cast(str, existing["owner_token"])
                    session_start_attempt_validation._write_session_attempt(
                        transaction,
                        operation="start",
                        identity={
                            "cluster": request.cluster,
                            "session_id": request.session_id,
                            "start_operation_id": request.start_operation_id,
                            "session_generation_id": recorded_generation,
                            "owner_token": existing_owner_token,
                            "owner_token_sha256": hashlib.sha256(
                                existing_owner_token.encode("utf-8")
                            ).hexdigest(),
                            "api_release_identity_sha256": release_identity.sha256(),
                            "expected_api_release_identity_sha256": (
                                request.expected_api_release_identity.sha256()
                                if request.expected_api_release_identity is not None
                                else None
                            ),
                            "cluster_registry_path": existing["cluster_registry_path"],
                            "cluster_registry_sha256": request.cluster_registry_sha256,
                            "cluster_route_revision": request.cluster_route_revision,
                            "remote_api_port": request.remote_api_port,
                            "replace": request.replace,
                            "require_token": request.require_token,
                            "input_policy": request.input_policy.model_dump(mode="json"),
                            "start_phase": "contained",
                            "systemd_unit": existing["systemd_unit"],
                            "systemd_description": existing["systemd_description"],
                            "systemd_cgroup_path": existing["systemd_cgroup_path"],
                            "systemd_invocation_id": existing["systemd_invocation_id"],
                            "containment_broker_pid": existing["containment_broker_pid"],
                            "containment_broker_start_identity": existing[
                                "containment_broker_start_identity"
                            ],
                        },
                    )
                    queue.clear_owner_session_closing(
                        request.session_id,
                        session_generation_id=recorded_generation,
                    )
                    if existing_status.api_pid is None:
                        raise RelayError("verified existing owned session omitted its API pid")
                    return OwnedSessionStartReceipt(
                        cluster=request.cluster,
                        session_id=request.session_id,
                        start_operation_id=request.start_operation_id,
                        cluster_route_revision=request.cluster_route_revision,
                        session_generation_id=recorded_generation,
                        remote_api_port=request.remote_api_port,
                        api_pid=existing_status.api_pid,
                        outcome="already_running",
                    )
                if not registry_matches:
                    raise RelayError(
                        "an owned generation cannot change cluster authority during restart"
                    )
                if existing_status.generation_process_pids:
                    if not request.replace:
                        raise RelayError("owned generation processes remain; use --replace")
                    existing_unit = existing.get("systemd_unit")
                    existing_cgroup = existing.get("systemd_cgroup_path")
                    existing_invocation = existing.get("systemd_invocation_id")
                    existing_description = existing.get("systemd_description")
                    if not all(
                        isinstance(value, str)
                        for value in (
                            existing_unit,
                            existing_cgroup,
                            existing_invocation,
                            existing_description,
                        )
                    ):
                        raise RelayError("owned generation process identity is incomplete")
                    session_process_scope._terminate_recorded_session_scope(
                        systemd_unit=cast(str, existing_unit),
                        systemd_cgroup_path=cast(str, existing_cgroup),
                        systemd_invocation_id=cast(str, existing_invocation),
                        systemd_description=cast(str, existing_description),
                    )
        elif resumable_attempt is not None:
            recorded_generation = cast(str, resumable_attempt["session_generation_id"])
            attempt_phase = cast(str, resumable_attempt["start_phase"])
            if attempt_phase in {"pending", "admitted"}:
                from clio_relay.process_containment import adopt_linux_systemd_scope_identity

                try:
                    adopted_scope = adopt_linux_systemd_scope_identity(
                        unit=cast(str, resumable_attempt["systemd_unit"]),
                        description=cast(str, resumable_attempt["systemd_description"]),
                    )
                except RuntimeError as exc:
                    raise RelayError(f"prior owned-session scope recovery failed: {exc}") from exc
                if adopted_scope is not None:
                    resumable_attempt.update(
                        {
                            "start_phase": "scope_bound",
                            "systemd_cgroup_path": adopted_scope["cgroup_path"],
                            "systemd_invocation_id": adopted_scope["systemd_invocation_id"],
                        }
                    )
                    session_start_attempt_validation._write_session_attempt(
                        transaction,
                        operation="start",
                        identity={
                            key: value
                            for key, value in resumable_attempt.items()
                            if key not in {"schema_version", "operation", "observed_at", "error"}
                        },
                    )
                    attempt_phase = "scope_bound"
            if attempt_phase in {"scope_bound", "contained"}:
                if attempt_phase == "contained":
                    promoted = _promote_resumable_contained_start(
                        transaction=transaction,
                        attempt=resumable_attempt,
                        request=request,
                        release_identity=release_identity,
                        queue=queue,
                        proc_root=proc_root,
                        home=home,
                    )
                    if promoted is not None:
                        return promoted
                    if legacy_migrated and not request.replace:
                        raise RelayError(
                            "legacy contained start could not be adopted exactly; use --replace"
                        )
                session_process_scope._recorded_scope_processes(
                    proc_root=proc_root,
                    systemd_unit=cast(str, resumable_attempt["systemd_unit"]),
                    systemd_cgroup_path=cast(str, resumable_attempt["systemd_cgroup_path"]),
                    systemd_invocation_id=cast(
                        str,
                        resumable_attempt["systemd_invocation_id"],
                    ),
                    systemd_description=cast(str, resumable_attempt["systemd_description"]),
                )
                session_process_scope._terminate_recorded_session_scope(
                    systemd_unit=cast(str, resumable_attempt["systemd_unit"]),
                    systemd_cgroup_path=cast(str, resumable_attempt["systemd_cgroup_path"]),
                    systemd_invocation_id=cast(
                        str,
                        resumable_attempt["systemd_invocation_id"],
                    ),
                    systemd_description=cast(str, resumable_attempt["systemd_description"]),
                )
                resumable_attempt.update(
                    {
                        "start_phase": "admitted",
                        "systemd_cgroup_path": None,
                        "systemd_invocation_id": None,
                        "containment_broker_pid": None,
                        "containment_broker_start_identity": None,
                    }
                )
                session_start_attempt_validation._write_session_attempt(
                    transaction,
                    operation="start",
                    identity={
                        key: value
                        for key, value in resumable_attempt.items()
                        if key not in {"schema_version", "operation", "observed_at", "error"}
                    },
                )

        session_api_readiness._assert_remote_port_available(request.remote_api_port)
        release_sha256 = release_identity.sha256()
        expected_release_sha256 = (
            request.expected_api_release_identity.sha256()
            if request.expected_api_release_identity is not None
            else None
        )
        if existing is None:
            if resumable_attempt is None:
                candidate_generation = uuid4().hex
                owner_token = secrets.token_hex(32)
                owner_token_sha256 = hashlib.sha256(owner_token.encode("utf-8")).hexdigest()
                registry_name = f"cluster-registry-{candidate_generation}.json"
                registry_path = transaction.path / registry_name
                systemd_unit = f"clio-relay-session-{candidate_generation}.scope"
                systemd_description = (
                    f"clio-relay-owned-session:{request.session_id}:{candidate_generation}:"
                    f"{secrets.token_hex(16)}"
                )
                attempt_identity: dict[str, object] = {
                    "cluster": request.cluster,
                    "session_id": request.session_id,
                    "start_operation_id": request.start_operation_id,
                    "session_generation_id": candidate_generation,
                    "owner_token": owner_token,
                    "owner_token_sha256": owner_token_sha256,
                    "api_release_identity_sha256": release_sha256,
                    "expected_api_release_identity_sha256": expected_release_sha256,
                    "cluster_registry_path": str(registry_path),
                    "cluster_registry_sha256": request.cluster_registry_sha256,
                    "cluster_route_revision": request.cluster_route_revision,
                    "remote_api_port": request.remote_api_port,
                    "replace": request.replace,
                    "require_token": request.require_token,
                    "input_policy": request.input_policy.model_dump(mode="json"),
                    "start_phase": "pending",
                    "systemd_unit": systemd_unit,
                    "systemd_description": systemd_description,
                    "systemd_cgroup_path": None,
                    "systemd_invocation_id": None,
                    "containment_broker_pid": None,
                    "containment_broker_start_identity": None,
                }
                session_start_attempt_validation._write_session_attempt(
                    transaction,
                    operation="start",
                    identity=attempt_identity,
                )
            else:
                attempt_identity = {
                    key: value
                    for key, value in resumable_attempt.items()
                    if key not in {"schema_version", "operation", "observed_at", "error"}
                }
                candidate_generation = cast(str, attempt_identity["session_generation_id"])
                owner_token = cast(str, attempt_identity["owner_token"])
                owner_token_sha256 = cast(str, attempt_identity["owner_token_sha256"])
                registry_name = f"cluster-registry-{candidate_generation}.json"
                registry_path = transaction.path / registry_name
                systemd_unit = cast(str, attempt_identity["systemd_unit"])
                systemd_description = cast(str, attempt_identity["systemd_description"])
            admission = queue.owner_session_generation_status(
                request.session_id,
                session_generation_id=candidate_generation,
            )
            active_generation = admission.get("active_generation_id")
            if active_generation is None:
                selected_generation = queue.prepare_owner_session_start(
                    request.session_id,
                    recorded_generation_id=None,
                    candidate_generation_id=candidate_generation,
                )
            elif active_generation == candidate_generation and admission.get("closing") is False:
                selected_generation = candidate_generation
            else:
                raise RelayError("core selected a different unrecorded owned-session generation")
            if selected_generation != candidate_generation:
                raise RelayError("core selected an unrecorded owned-session generation")
            if attempt_identity["start_phase"] != "contained":
                attempt_identity["start_phase"] = "admitted"
                session_start_attempt_validation._write_session_attempt(
                    transaction,
                    operation="start",
                    identity=attempt_identity,
                )
        else:
            candidate_generation = uuid4().hex
            selected_generation = queue.prepare_owner_session_start(
                request.session_id,
                recorded_generation_id=recorded_generation,
                candidate_generation_id=candidate_generation,
            )
            registry_name = f"cluster-registry-{selected_generation}.json"
            registry_path = transaction.path / registry_name
            owner_token = secrets.token_hex(32)
            owner_token_sha256 = hashlib.sha256(owner_token.encode("utf-8")).hexdigest()
            systemd_unit = f"clio-relay-session-{selected_generation}.scope"
            systemd_description = (
                f"clio-relay-owned-session:{request.session_id}:{selected_generation}:"
                f"{secrets.token_hex(16)}"
            )
            attempt_identity = {
                "cluster": request.cluster,
                "session_id": request.session_id,
                "start_operation_id": request.start_operation_id,
                "session_generation_id": selected_generation,
                "owner_token": owner_token,
                "owner_token_sha256": owner_token_sha256,
                "api_release_identity_sha256": release_sha256,
                "expected_api_release_identity_sha256": expected_release_sha256,
                "cluster_registry_path": str(registry_path),
                "cluster_registry_sha256": request.cluster_registry_sha256,
                "cluster_route_revision": request.cluster_route_revision,
                "remote_api_port": request.remote_api_port,
                "replace": request.replace,
                "require_token": request.require_token,
                "input_policy": request.input_policy.model_dump(mode="json"),
                "start_phase": "admitted",
                "systemd_unit": systemd_unit,
                "systemd_description": systemd_description,
                "systemd_cgroup_path": None,
                "systemd_invocation_id": None,
                "containment_broker_pid": None,
                "containment_broker_start_identity": None,
            }
            session_start_attempt_validation._write_session_attempt(
                transaction,
                operation="start",
                identity=attempt_identity,
            )
        registry_name = f"cluster-registry-{selected_generation}.json"
        registry_path = transaction.path / registry_name
        existing_registry = transaction.read_bytes(
            registry_name,
            maximum_bytes=MAX_CLUSTER_REGISTRY_BYTES,
            required=False,
        )
        if existing_registry is None:
            transaction.atomic_write(registry_name, registry_payload)
        elif hashlib.sha256(existing_registry).hexdigest() != request.cluster_registry_sha256:
            raise RelayError("owned generation cluster registry changed before restart")

        log_descriptor = transaction.open_output("api.log")
        process: subprocess.Popen[Any] | None = None
        metadata_committed = False
        child_environment: dict[str, str] = {}
        startup_secret_values: set[str] = set()
        try:
            from clio_relay.process_containment import (
                broker_child_environment_payload,
                process_start_identity,
                spawn_owned_process,
            )

            child_environment = {
                "CLIO_RELAY_SESSION_OWNER_TOKEN": owner_token,
                "CLIO_RELAY_SESSION_GENERATION_ID": selected_generation,
                "CLIO_RELAY_API_RELEASE_IDENTITY_SHA256": release_sha256,
                "CLIO_RELAY_CLUSTER_REGISTRY": str(registry_path),
                "CLIO_RELAY_SESSION_REGISTRY_SHA256": request.cluster_registry_sha256,
                "CLIO_RELAY_SESSION_ROUTE_REVISION": request.cluster_route_revision,
                "CLIO_RELAY_OWNER_SESSION_ID": request.session_id,
                "CLIO_RELAY_OWNER_SESSION_CLUSTER": request.cluster,
                "CLIO_RELAY_OWNER_SESSION_API_PORT": str(request.remote_api_port),
                "CLIO_RELAY_REMOTE_CLUSTER": request.cluster,
                **request.input_policy.environment(),
            }
            if not request.require_token:
                child_environment[ALLOW_UNAUTHENTICATED_OWNED_SESSION_ENV] = "1"
            if api_token is not None:
                child_environment["CLIO_RELAY_API_TOKEN"] = api_token
            environment = dict(os.environ)
            startup_secret_values.update(
                value
                for name, value in {**environment, **child_environment}.items()
                if value
                and any(
                    marker in name.upper()
                    for marker in (
                        "TOKEN",
                        "SECRET",
                        "PASSWORD",
                        "CREDENTIAL",
                        "API_KEY",
                        "AUTHORIZATION",
                    )
                )
            )
            environment.pop(ALLOW_UNAUTHENTICATED_OWNED_SESSION_ENV, None)
            for name in child_environment:
                environment.pop(name, None)
            provider_interpreter = Path(sys.executable).absolute()
            interpreter_identity = provider_interpreter.stat()
            command = [
                str(provider_interpreter),
                "-I",
                "-c",
                "from clio_relay.cli import app; app()",
                "api",
                "start",
                "--host",
                "127.0.0.1",
                "--port",
                str(request.remote_api_port),
            ]
            if request.require_token:
                command.append("--require-token")
            receipt_name = f"api-startup-{selected_generation}.json"
            receipt_path = transaction.path / receipt_name
            containment_identity: dict[str, object] = {}

            def persist_containment(
                broker_pid: int,
                containment: dict[str, object],
            ) -> None:
                if not (
                    containment.get("mode") == "linux_systemd_scope"
                    and containment.get("enforceable") is True
                    and containment.get("systemd_unit") == systemd_unit
                    and containment.get("systemd_description") == systemd_description
                    and isinstance(containment.get("cgroup_path"), str)
                    and isinstance(containment.get("systemd_invocation_id"), str)
                ):
                    raise RelayError("owned API containment identity is incomplete")
                broker_start = process_start_identity(broker_pid)
                if broker_start is None:
                    raise RelayError("owned API containment broker identity is unavailable")
                containment_identity.update(containment)
                attempt_identity.update(
                    {
                        "start_phase": "contained",
                        "systemd_cgroup_path": containment["cgroup_path"],
                        "systemd_invocation_id": containment["systemd_invocation_id"],
                        "containment_broker_pid": broker_pid,
                        "containment_broker_start_identity": broker_start,
                    }
                )
                session_start_attempt_validation._write_session_attempt(
                    transaction,
                    operation="start",
                    identity=attempt_identity,
                )

            def release_child_environment(
                _broker_pid: int,
                containment: dict[str, object],
            ) -> str:
                gated_environment = dict(child_environment)
                gated_environment.update(
                    {
                        session_lifecycle._API_STARTUP_RECEIPT_ENV: str(receipt_path),
                        session_lifecycle._SYSTEMD_UNIT_ENV: cast(str, containment["systemd_unit"]),
                        session_lifecycle._SYSTEMD_CGROUP_ENV: cast(
                            str, containment["cgroup_path"]
                        ),
                        session_lifecycle._SYSTEMD_INVOCATION_ENV: cast(
                            str,
                            containment["systemd_invocation_id"],
                        ),
                        session_lifecycle._SYSTEMD_DESCRIPTION_ENV: cast(
                            str,
                            containment["systemd_description"],
                        ),
                    }
                )
                return broker_child_environment_payload(gated_environment)

            process = cast(
                subprocess.Popen[Any],
                spawn_owned_process(
                    command,
                    stdout=log_descriptor,
                    stderr=subprocess.STDOUT,
                    env=environment,
                    close_fds=True,
                    require_enforceable=True,
                    linux_systemd_unit_base=systemd_unit.removesuffix(".scope"),
                    linux_systemd_description=systemd_description,
                    on_ready=persist_containment,
                    credential_payload_factory=release_child_environment,
                ),
            )
            final_interpreter_identity = provider_interpreter.stat()
            if (final_interpreter_identity.st_dev, final_interpreter_identity.st_ino) != (
                interpreter_identity.st_dev,
                interpreter_identity.st_ino,
            ):
                raise RelayError("verified provider interpreter changed during API spawn")
            expected_receipt = {
                "cluster": request.cluster,
                "session_id": request.session_id,
                "session_generation_id": selected_generation,
                "api_release_identity_sha256": release_sha256,
                "cluster_registry_path": str(registry_path),
                "cluster_registry_sha256": request.cluster_registry_sha256,
                "cluster_route_revision": request.cluster_route_revision,
                "systemd_unit": containment_identity["systemd_unit"],
                "systemd_cgroup_path": containment_identity["cgroup_path"],
                "systemd_invocation_id": containment_identity["systemd_invocation_id"],
                "systemd_description": containment_identity["systemd_description"],
            }
            process_identity = session_api_readiness._wait_for_api_startup_receipt(
                transaction=transaction,
                process=process,
                receipt_name=receipt_name,
                owner_token=owner_token,
                expected=expected_receipt,
                proc_root=proc_root,
            )
            ready_seconds = session_api_readiness._wait_for_api_ready(
                process=cast(subprocess.Popen[bytes], process),
                port=request.remote_api_port,
                require_token=request.require_token,
            )
            receipt_payload = transaction.read_bytes(
                receipt_name,
                maximum_bytes=session_lifecycle._MAX_API_STARTUP_RECEIPT_BYTES,
            )
            if receipt_payload is None:  # pragma: no cover - required read
                raise RelayError("owned API startup receipt disappeared before metadata commit")
            metadata = {
                "cluster": request.cluster,
                "session_id": request.session_id,
                "remote_api_port": request.remote_api_port,
                "api_pid": process_identity.pid,
                "api_pgid": process_identity.process_group_id,
                "owner_token": owner_token,
                "session_generation_id": selected_generation,
                "api_release_identity": release_identity.model_dump(mode="json"),
                "api_release_identity_sha256": release_sha256,
                "cluster_registry_path": str(registry_path),
                "cluster_registry_sha256": request.cluster_registry_sha256,
                "cluster_route_revision": request.cluster_route_revision,
                "cluster_authority_verified": True,
                "input_policy": request.input_policy.model_dump(mode="json"),
                "process_start_ticks": process_identity.start_ticks,
                "containment_mode": "linux_systemd_scope",
                "systemd_unit": containment_identity["systemd_unit"],
                "systemd_cgroup_path": containment_identity["cgroup_path"],
                "systemd_invocation_id": containment_identity["systemd_invocation_id"],
                "systemd_description": containment_identity["systemd_description"],
                "containment_broker_pid": process.pid,
                "containment_broker_start_identity": attempt_identity[
                    "containment_broker_start_identity"
                ],
                "api_startup_receipt_path": str(receipt_path),
                "api_startup_receipt_sha256": hashlib.sha256(receipt_payload).hexdigest(),
                "started_at": datetime.now(UTC).isoformat(),
                "owner": "clio-relay",
            }
            transaction.atomic_write("api.pid", f"{process_identity.pid}\n".encode("ascii"))
            transaction.atomic_write(
                "metadata.json",
                json.dumps(metadata, indent=2).encode("utf-8"),
            )
            metadata_committed = True
            queue.clear_owner_session_closing(
                request.session_id,
                session_generation_id=selected_generation,
            )
            return OwnedSessionStartReceipt(
                cluster=request.cluster,
                session_id=request.session_id,
                start_operation_id=request.start_operation_id,
                cluster_route_revision=request.cluster_route_revision,
                session_generation_id=selected_generation,
                remote_api_port=request.remote_api_port,
                api_pid=process_identity.pid,
                outcome="started",
                ready_seconds=ready_seconds,
            )
        except BaseException as exc:
            if not metadata_committed:
                startup_detail = session_api_readiness._owned_api_startup_log_detail(
                    transaction,
                    secret_values=startup_secret_values,
                )
                try:
                    if attempt_identity.get("start_phase") == "contained":
                        session_process_scope._terminate_recorded_session_scope(
                            systemd_unit=cast(str, attempt_identity["systemd_unit"]),
                            systemd_cgroup_path=cast(
                                str,
                                attempt_identity["systemd_cgroup_path"],
                            ),
                            systemd_invocation_id=cast(
                                str,
                                attempt_identity["systemd_invocation_id"],
                            ),
                            systemd_description=cast(str, attempt_identity["systemd_description"]),
                        )
                        attempt_identity.update(
                            {
                                "start_phase": "admitted",
                                "systemd_cgroup_path": None,
                                "systemd_invocation_id": None,
                                "containment_broker_pid": None,
                                "containment_broker_start_identity": None,
                            }
                        )
                except (RelayError, RuntimeError) as cleanup_error:
                    cleanup_detail = f"{exc}; cleanup failed: {cleanup_error}"
                else:
                    cleanup_detail = str(exc)
                if startup_detail:
                    cleanup_detail = f"{cleanup_detail}; api_log={startup_detail}"
                with suppress(RelayError):
                    session_start_attempt_validation._write_session_attempt(
                        transaction,
                        operation="start",
                        identity=attempt_identity,
                        error=cleanup_detail,
                    )
                if startup_detail and isinstance(exc, RelayError):
                    raise RelayError(cleanup_detail) from exc
            raise
        finally:
            os.close(log_descriptor)
