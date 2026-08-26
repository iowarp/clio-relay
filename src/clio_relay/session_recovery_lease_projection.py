"""The owned-session lease projection for ``session recovery-status`` (iowarp/clio-relay#277).

Kept as its own small module rather than folded into
:mod:`clio_relay.session_recovery_inspection` (an already large, heavily
audited module at its own recorded ratchet baseline,
`scripts/check_file_size.py`) -- see :func:`owner_session_lease_status_projection`.
Applied by ``session recovery-status`` (`cli_session_owned.py`) as a
`model_copy` post-processing step onto the wrapped
:func:`~clio_relay.session_recovery_inspection.inspect_owned_session_recovery_status`
result, not a body edit to that function.

``session recovery-status`` is the one dial-free command
:func:`clio_relay.control_channel.owned_session_channel_bootstrap_script`
already runs inline as part of every channel bring-up's single SSH dial --
so this projection reaches :class:`~clio_relay.remote_connection.
RemoteConnection`'s bootstrap verification at zero extra transport cost.

The projection is read entirely independently of the durable-generation
admission check the wrapped function performs: a lease-read failure is
swallowed here (never raised, never appended to the wrapped status's
``errors``) and never changes ``recovery_verified``. It exists to align
:mod:`clio_relay.session_attach`'s typed vocabulary against it
(:class:`~clio_relay.remote_connection_lease.SessionLeaseExpiredError`) -- a
missing lease record (a session started before this feature existed, or
whose API process never took a single authenticated request) is not an
error either; the projection simply returns ``None``.
"""

from __future__ import annotations

from pathlib import Path

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import RelayError


def owner_session_lease_status_projection(
    *,
    core_dir: Path,
    session_id: str,
    session_generation_id: str,
) -> dict[str, object] | None:
    """Return the lease record as a plain dict, or ``None`` when absent/unreadable."""
    try:
        lease = ClioCoreQueue(core_dir).owner_session_lease_status(
            session_id,
            session_generation_id=session_generation_id,
        )
    except (OSError, RelayError, ValueError):
        return None
    if lease is None:
        return None
    return lease.model_dump(mode="json")
