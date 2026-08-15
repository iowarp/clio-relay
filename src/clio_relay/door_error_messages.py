"""Stable public-message formatting for relay error reasons."""

from __future__ import annotations

from typing import Final

from clio_relay.errors import PublicMessageError

_MESSAGE_OVERRIDES: Final[dict[str, str]] = {
    "internal_error": "relay encountered an internal error.",
}


def public_message(*, reason: str, title: str) -> str:
    """Return a stable public message without inspecting an exception."""
    return _MESSAGE_OVERRIDES.get(reason, f"{title}.")


def resolved_public_message(
    exc: BaseException,
    *,
    logged_detail: str | None,
    explicit: str | None,
    reason: str,
    title: str,
) -> str:
    """Select explicit, typed-authored, or stable generic public detail."""
    if explicit is not None:
        return explicit
    if isinstance(exc, PublicMessageError) and logged_detail is not None:
        return logged_detail
    return public_message(reason=reason, title=title)
