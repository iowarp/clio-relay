"""Crash-surviving start promotion for an already-running owned API.

split/session-start-execution-w3 (#231): the ``_OwnedSessionQueue`` typed
core-queue surface, the ``_RecoveredStartProbe`` liveness stand-in, and
``_promote_resumable_contained_start`` moved out of session_start_execution.py,
which stays the resident home for ``execute_owned_session_start`` (the sole
caller of the promotion helper below) and re-exports every name here under
its original name for compatibility. session_lifecycle is imported back
INSIDE the function (not at module scope): session_lifecycle imports
session_start_execution.py for its cli.py-compatibility re-export block, so
a module-scope back-import here would create a load-order-dependent
circular import -- deferred to call time, it is import-order-independent,
matching the pattern the original module established.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast

import clio_relay.session_api_readiness as session_api_readiness
from clio_relay.errors import RelayError
from clio_relay.session_wire_models import (
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
    # (which imports session_start_execution.py back for its cli.py-
    # compatibility re-export block) -- see the module docstring.
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
