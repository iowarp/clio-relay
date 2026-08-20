"""Owned-session identity-challenge signing.

split/session-start-execution-w3 (#231):
``execute_owned_session_identity_challenge`` moved out of
session_start_execution.py, which stays the resident home for
``execute_owned_session_start`` and re-exports this function under its
original name for compatibility. session_lifecycle is imported back INSIDE
the function (not at module scope): session_lifecycle imports
session_start_execution.py for its cli.py-compatibility re-export block, so
a module-scope back-import here would create a load-order-dependent
circular import -- deferred to call time, it is import-order-independent,
matching the pattern the original module established.
"""

from __future__ import annotations

import hashlib
import hmac
import os
from collections.abc import Callable
from pathlib import Path
from typing import cast

from clio_relay.errors import RelayError
from clio_relay.session_wire_models import OwnedSessionIdentityChallengeRequest


def execute_owned_session_identity_challenge(
    request: OwnedSessionIdentityChallengeRequest,
    *,
    home: Path | None = None,
    core_dir: Path | None = None,
    proc_root: Path = Path("/proc"),
) -> dict[str, object]:
    """Sign one nonce only after pinned metadata and live leader verification."""
    # Deferred import: breaks the module-load-time cycle with session_lifecycle
    # (which imports session_start_execution.py back for its cli.py-
    # compatibility re-export block) -- see the module docstring.
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
