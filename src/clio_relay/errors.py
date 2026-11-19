"""Relay-specific exceptions."""

from __future__ import annotations


class PublicMessageError:
    """Mark an exception whose message is relay-authored public guidance."""

    @property
    def public_message(self) -> str:
        """Return the authored message that may be served on relay wires."""
        return str(self)


class RelayError(RuntimeError):
    """Base class for relay errors."""


class ConfigurationError(RelayError):
    """Raised when required external configuration is absent."""
