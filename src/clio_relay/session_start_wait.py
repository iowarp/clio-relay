"""Cluster-local exact-start-selector wait/poll (#231 rework).

Extracted from ``session_lifecycle.py``: the single-observation start-status
inspector (pins its own transaction and calls into
``inspect_owned_session_recovery_status``, which stays resident), the
terminal-state predicate, and the bounded poll loop the desktop's start watch
ultimately blocks on cluster-side.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable
from pathlib import Path

import clio_relay.session_lifecycle as session_lifecycle
from clio_relay.errors import RelayError
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.session_transaction import open_owned_session_transaction
from clio_relay.session_validation import _validate_session
from clio_relay.session_wire_models import MAX_SESSION_START_ERROR_CHARS, OwnedSessionRecoveryStatus


def inspect_owned_session_start_status(
    *,
    cluster: str,
    session_id: str,
    start_operation_id: str,
    cluster_route_revision: str,
    core_dir: Path,
    home: Path | None = None,
    proc_root: Path = Path("/proc"),
    lock_timeout_seconds: float = 0.05,
) -> OwnedSessionRecoveryStatus:
    """Inspect one exact start selector without waiting for its transition writer."""
    _validate_session(session_id=session_id, remote_api_port=1)
    try:
        validated_operation_id = validate_durable_record_id(start_operation_id)
    except (TypeError, ValueError) as exc:
        raise RelayError(f"invalid start_operation_id: {exc}") from exc
    if not cluster_route_revision:
        raise RelayError("cluster_route_revision must not be empty")
    if lock_timeout_seconds <= 0:
        raise ValueError("lock_timeout_seconds must be positive")
    selected_home = home or Path.home()
    try:
        with open_owned_session_transaction(
            session_id=session_id,
            create=False,
            timeout_seconds=lock_timeout_seconds,
            home=selected_home,
        ) as transaction:
            return session_lifecycle.inspect_owned_session_recovery_status(
                cluster=cluster,
                session_id=session_id,
                core_dir=core_dir,
                home=selected_home,
                proc_root=proc_root,
                transaction=transaction,
                expected_start_operation_id=validated_operation_id,
                expected_cluster_route_revision=cluster_route_revision,
            )
    except RelayError as exc:
        return OwnedSessionRecoveryStatus(
            cluster=cluster,
            session_id=session_id,
            start_operation_id=validated_operation_id,
            cluster_route_revision=cluster_route_revision,
            start_state="starting",
            start_retryable=True,
            errors=[str(exc)[:MAX_SESSION_START_ERROR_CHARS]],
        )


def owned_session_start_status_is_terminal(status: OwnedSessionRecoveryStatus) -> bool:
    """Return whether one start observation needs no further waiting."""
    if status.start_state in {"failed", "failed_cleaned", "not_current"}:
        return True
    return (
        status.start_state == "ready"
        and status.recovery_verified
        and status.ownership_verified
        and status.session_generation_id is not None
    )


def wait_owned_session_start_status(
    *,
    cluster: str,
    session_id: str,
    start_operation_id: str,
    cluster_route_revision: str,
    core_dir: Path,
    wait_seconds: float = 0.0,
    poll_seconds: float = 0.25,
    inspect: Callable[..., OwnedSessionRecoveryStatus] | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> OwnedSessionRecoveryStatus:
    """Observe one exact start, blocking cluster-side until it is terminal.

    This is the cluster-local half of a start watch.  The desktop issues one
    command carrying its remaining deadline instead of redialing per interval,
    so a watch costs one transport connection regardless of how long the start
    takes.  The wait is bounded and always returns the latest exact observation.
    """
    if not math.isfinite(wait_seconds) or wait_seconds < 0:
        raise ValueError("start status wait_seconds must be finite and nonnegative")
    if not math.isfinite(poll_seconds) or poll_seconds <= 0:
        raise ValueError("start status poll_seconds must be finite and positive")
    observe = inspect or inspect_owned_session_start_status
    deadline = monotonic() + wait_seconds
    while True:
        status = observe(
            cluster=cluster,
            session_id=session_id,
            start_operation_id=start_operation_id,
            cluster_route_revision=cluster_route_revision,
            core_dir=core_dir,
        )
        if owned_session_start_status_is_terminal(status):
            return status
        remaining = deadline - monotonic()
        if remaining <= 0:
            return status
        sleep(min(poll_seconds, remaining))
