"""Session and execution-identity validation shared across owned-session modules.

Extracted from ``session_lifecycle.py`` (#231 rework slice, split/session-lifecycle
branch): both checks are used by nearly every owned-session lifecycle module
(the transaction primitive, the recovery inspector, the remote SSH facade, the
cluster-local start/teardown executors) before any lifecycle I/O, so they live
in their own dependency-free leaf module rather than any one owner.
"""

from __future__ import annotations

from clio_relay.errors import RelayError
from clio_relay.identifiers import validate_durable_record_id


def _validate_session(*, session_id: str, remote_api_port: int) -> None:
    if not session_id or not all(item.isalnum() or item in {"-", "_"} for item in session_id):
        raise RelayError("session_id must contain only letters, numbers, hyphen, or underscore")
    if remote_api_port <= 0:
        raise RelayError("remote_api_port must be positive")


def _validate_durable_session_identity(value: str, *, field: str) -> str:
    """Validate an execution identity before any remote lifecycle I/O."""
    try:
        return validate_durable_record_id(value)
    except ValueError as error:
        raise RelayError(f"invalid {field}: {error}") from error
