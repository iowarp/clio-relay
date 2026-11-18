"""Storage runtime failure types and typed-decision-to-error conversion.

Owns the stable, machine-readable exception hierarchy the storage runtime and
its managed queue raise on admission refusal or a running-child safety
violation, plus the two small helpers that turn a policy-level
``StorageDecision`` (or a caught
:class:`~clio_relay.storage_policy.StoragePolicyError`) into one of those
exceptions.
"""

from __future__ import annotations

import json

from clio_relay.errors import PublicMessageError, RelayError
from clio_relay.storage_policy import StorageDecision, StoragePolicyError, StorageReason


class StorageRuntimeError(PublicMessageError, RelayError):
    """Base class for a stable machine-readable storage runtime failure."""

    def __init__(self, decision: StorageDecision) -> None:
        self.decision = decision
        super().__init__(json.dumps(decision.to_dict(), sort_keys=True, separators=(",", ":")))

    @property
    def public_message(self) -> str:
        """Return the storage policy's relay-authored course correction."""
        return self.decision.message


class StorageAdmissionError(StorageRuntimeError):
    """Raised when a genuinely new queue admission cannot be reserved safely."""


class StorageRuntimeViolation(StorageRuntimeError):
    """Raised after a running child crosses a durable storage safety boundary."""


def _denied_decision(
    reason: StorageReason,
    message: str,
    *,
    details: dict[str, object] | None = None,
) -> StorageDecision:
    return StorageDecision(
        allowed=False,
        reason=reason,
        message=message,
        details=details,
    )


def _policy_error_decision(error: StoragePolicyError) -> StorageDecision:
    return StorageDecision(
        allowed=False,
        reason=error.reason,
        message=str(error),
        details=error.details,
    )
