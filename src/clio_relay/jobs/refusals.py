"""Typed refusal reasons shared by the surface, the adapters and the hooks.

A refusal is how relay says no. It always carries one reason from
:class:`RefusalReason`, a message a person can act on, and optional data.
Clients branch on the reason, never on the message text.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any


class RefusalReason(StrEnum):
    """Every reason relay may refuse a call with."""

    VERB_NOT_IMPLEMENTED = "verb_not_implemented"
    SITE_NOT_FOUND = "site_not_found"
    SITE_EXISTS = "site_exists"
    UNKNOWN_ADAPTER = "unknown_adapter"
    ADAPTER_UNAVAILABLE = "adapter_unavailable"
    INVALID_CONFIG = "invalid_config"
    INVALID_SPEC = "invalid_spec"
    INVALID_ARGUMENTS = "invalid_arguments"
    GUARD_UNAVAILABLE = "guard_unavailable"
    POLICY_REFUSED = "policy_refused"
    NEEDS_APPROVAL = "needs_approval"
    NEEDS_SIGN_IN = "needs_sign_in"
    DECLINED = "declined"
    INTERACTIVE_AUTH_REQUIRED = "interactive_auth_required"
    OPERATION_NOT_FOUND = "operation_not_found"
    ASK_NOT_FOUND = "ask_not_found"
    APPROVAL_MISMATCH = "approval_mismatch"
    INPUT_MISSING = "input_missing"
    PATH_NOT_FOUND = "path_not_found"
    PATH_EXISTS = "path_exists"
    NOT_CANCELLABLE = "not_cancellable"
    SUBMIT_FAILED = "submit_failed"
    UNREACHABLE = "unreachable"
    PUNCH_FAILED = "punch_failed"
    TIMEOUT = "timeout"


class Refusal(Exception):
    """A typed refusal. Raised by adapters and hooks, rendered by the surface."""

    def __init__(self, reason: RefusalReason, message: str, **data: Any) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message
        self.data: dict[str, Any] = data

    def to_dict(self) -> dict[str, Any]:
        """Return the wire form used in structured tool results."""
        payload: dict[str, Any] = {"reason": self.reason.value, "message": self.message}
        if self.data:
            payload["data"] = self.data
        return payload

    def __repr__(self) -> str:
        return f"Refusal({self.reason.value!r}, {self.message!r})"
