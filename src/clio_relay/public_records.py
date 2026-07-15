"""Stable public projections for durable relay records."""

from __future__ import annotations

from typing import Any, cast

from clio_relay.models import GatewaySession
from clio_relay.validation_report import redact_sensitive_values


def public_gateway_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a gateway-related payload without nested capability values."""
    return cast(dict[str, Any], redact_sensitive_values(payload))


def public_gateway_session(session: GatewaySession) -> dict[str, Any]:
    """Return a gateway document with every nested credential value redacted.

    Gateway records retain ownership capabilities so supervisors can verify and
    clean up resources after the creating process exits. Public CLI, MCP, and
    HTTP projections must be derived from a copy and must never mutate that
    durable internal identity.
    """
    return public_gateway_payload(session.model_dump(mode="json"))
