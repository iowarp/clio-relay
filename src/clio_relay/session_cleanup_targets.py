"""Owned-session cleanup-target capture/validation/deletion (#231 rework).

Extracted from ``session_lifecycle.py``: the receipt-authorized cleanup-target
identity primitives shared by both the recovery-inspection readers (a failed
or cleaned start's receipt) and the teardown/failed-start executors that
actually delete the captured inodes.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, cast

from clio_relay.cluster_config import MAX_CLUSTER_REGISTRY_BYTES
from clio_relay.errors import RelayError
from clio_relay.session_transaction import _MAX_OWNED_SESSION_DOCUMENT_BYTES
from clio_relay.session_wire_models import OwnedSessionCleanupTarget, OwnedSessionTeardownRequest

if TYPE_CHECKING:
    from clio_relay.session_transaction import _OwnedSessionTransaction


def _capture_cleanup_target(
    transaction: _OwnedSessionTransaction,
    *,
    name: str,
    maximum_bytes: int | None,
) -> OwnedSessionCleanupTarget:
    """Capture an exact cleanup target identity through the pinned directory."""
    linked = transaction.stat_regular(name, required=False)
    if linked is None:
        return OwnedSessionCleanupTarget(name=name, present=False)
    payload = (
        transaction.read_bytes(name, maximum_bytes=maximum_bytes)
        if maximum_bytes is not None
        else None
    )
    final = transaction.stat_regular(name)
    if final is None:  # pragma: no cover - required stat
        raise RelayError(f"owned cleanup target disappeared: {name}")
    if (linked.st_dev, linked.st_ino, linked.st_size) != (
        final.st_dev,
        final.st_ino,
        final.st_size,
    ):
        raise RelayError(f"owned cleanup target changed while it was captured: {name}")
    return OwnedSessionCleanupTarget(
        name=name,
        present=True,
        device=linked.st_dev,
        inode=linked.st_ino,
        size=linked.st_size,
        sha256=hashlib.sha256(payload).hexdigest() if payload is not None else None,
        identity_mode="content_sha256" if payload is not None else "inode",
    )


def _validate_cleanup_targets(
    raw_targets: object,
    *,
    generation_id: str,
) -> list[OwnedSessionCleanupTarget]:
    """Validate an exact, duplicate-free cleanup target identity collection."""
    if not isinstance(raw_targets, list):
        raise RelayError("owned session cleanup receipt targets are unavailable")
    try:
        targets = [
            OwnedSessionCleanupTarget.model_validate(target)
            for target in cast(list[object], raw_targets)
        ]
    except ValueError as exc:
        raise RelayError(f"owned session cleanup receipt target is invalid: {exc}") from exc
    expected_names = sorted(
        (
            "api.log",
            "api.pid",
            f"api-startup-{generation_id}.json",
            f"cluster-registry-{generation_id}.json",
        )
    )
    if [target.name for target in targets] != expected_names:
        raise RelayError("owned session cleanup receipt target names are invalid")
    if not all(target.identity_is_complete() for target in targets):
        raise RelayError("owned session cleanup receipt target identity is incomplete")
    return targets


def _delete_cleanup_targets(
    transaction: _OwnedSessionTransaction,
    targets: list[OwnedSessionCleanupTarget],
) -> None:
    """Delete only receipt-authorized inodes, accepting already-absent retry targets."""
    for target in targets:
        current = transaction.stat_regular(target.name, required=False)
        if not target.present:
            if current is not None:
                raise RelayError(f"an absent cleanup target appeared during retry: {target.name}")
            continue
        if current is None:
            continue
        if target.device is None or target.inode is None or target.size is None:
            raise RelayError(f"cleanup target identity is incomplete: {target.name}")
        transaction.unlink_verified(
            target.name,
            expected_device=target.device,
            expected_inode=target.inode,
            expected_size=target.size,
            expected_sha256=target.sha256,
            maximum_bytes=(
                MAX_CLUSTER_REGISTRY_BYTES
                if target.name.startswith("cluster-registry-")
                else _MAX_OWNED_SESSION_DOCUMENT_BYTES
                if target.identity_mode == "content_sha256"
                else None
            ),
        )


def _cleanup_intent_matches_request(
    intent: dict[str, object],
    request: OwnedSessionTeardownRequest,
) -> bool:
    """Return whether a durable intent is the request's exact immutable policy."""
    return bool(
        intent.get("schema_version") == "clio-relay.owner-session-cleanup-intent.v1"
        and intent.get("operation_id") == request.expected_cleanup_operation_id
        and intent.get("owner_session_id") == request.session_id
        and intent.get("session_generation_id") == request.expected_session_generation_id
        and intent.get("stop_worker") is request.stop_worker
        and intent.get("cancel_jobs") is request.cancel_jobs
        and intent.get("cancel_scheduler_jobs") is request.cancel_scheduler_jobs
    )
