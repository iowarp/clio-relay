"""Relay-specific exceptions."""


class RelayError(RuntimeError):
    """Base class for relay errors."""


class ConfigurationError(RelayError):
    """Raised when required external configuration is absent."""


class QueueConflictError(RelayError):
    """Raised when a queue operation violates an invariant."""


class NotFoundError(RelayError):
    """Raised when a requested record is missing."""
