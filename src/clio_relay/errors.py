"""Relay-specific exceptions."""

from __future__ import annotations


class RelayError(RuntimeError):
    """Base class for relay errors."""


class ConfigurationError(RelayError):
    """Raised when required external configuration is absent."""
