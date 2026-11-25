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
    HOST_KEY_UNVERIFIED = "host_key_unverified"
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


DECLINED_PREFIX = "the person declined: "


def declined(
    question: str, not_done: str, *, ask_id: str, operation_id: str | None = None, **data: Any
) -> Refusal:
    """The person said no to ``question``; ``not_done`` says what relay therefore did not do.

    It travels as a refusal so every call path stops the same way; the surface
    shows it as a ``declined`` outcome (:func:`declined_outcome`), not as an error.
    """
    return Refusal(
        RefusalReason.DECLINED,
        f"{DECLINED_PREFIX}{question}",
        operation_id=operation_id,
        ask_id=ask_id,
        question=question,
        not_done=not_done,
        **data,
    )


def declined_outcome(
    error: dict[str, Any] | None, *, operation_id: str | None = None, not_done: str | None = None
) -> dict[str, Any] | None:
    """The typed ``declined`` outcome for a refusal's wire form, or None for any other.

    It is ``{operation_id, ask_id, question, not_done}``. A record declined
    before the outcome was typed carries only the message, so the question is
    read from it and ``not_done`` comes from the caller.
    """
    if not error or error.get("reason") != RefusalReason.DECLINED.value:
        return None
    data: dict[str, Any] = error.get("data") or {}
    message = str(error.get("message") or "")
    question = data.get("question") or message.removeprefix(DECLINED_PREFIX)
    return {
        "operation_id": data.get("operation_id") or operation_id,
        "ask_id": data.get("ask_id"),
        "question": question,
        "not_done": data.get("not_done") or not_done or "Relay did nothing.",
    }
