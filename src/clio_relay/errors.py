"""Relay-specific exceptions."""

from __future__ import annotations

import logging


class PublicMessageError:
    """Mark an exception whose message is relay-authored public guidance."""

    @property
    def public_message(self) -> str:
        """Return the authored message that may be served on relay wires."""
        return str(self)


class RelayError(RuntimeError):
    """Base class for relay errors."""


class RelayAuthoredError(PublicMessageError, RelayError):
    """Carry an explicitly reviewed exception message across a door boundary."""

    def __init__(self, source: BaseException) -> None:
        self.source = source
        super().__init__()

    @property
    def public_message(self) -> str:
        """Return the reviewed source exception's authored message."""
        return str(self.source)


def public_message_error(exc: BaseException) -> BaseException:
    """Explicitly mark a reviewed relay-authored exception for wire delivery."""
    return exc if isinstance(exc, PublicMessageError) else RelayAuthoredError(exc)


class ObservationTimeoutError(PublicMessageError, RelayError):
    """A bounded observation transport expired without changing durable work."""


class ConfigurationError(RelayError):
    """Raised when required external configuration is absent."""


class QueueConflictError(RelayError):
    """Raised when a queue operation violates an invariant."""


class McpTaskIdentityConflictError(QueueConflictError):
    """A caller reused one durable MCP task identity for different semantics."""


def queue_conflict_from_cause(
    message: str,
    *,
    cause: BaseException,
    logger: logging.Logger,
) -> QueueConflictError:
    """Log foreign cause detail once and return a curated queue conflict."""
    if not isinstance(cause, QueueConflictError):
        logger.warning(
            "clio-relay: %s; cause_type=%s",
            message,
            type(cause).__name__,
            exc_info=(type(cause), cause, cause.__traceback__),
        )
    return QueueConflictError(message)


class TaskInputParkConflictError(QueueConflictError):
    """A durable MCP task's post-admission input round exhausted CAS retries.

    Raised only by ``RelayMcpRuntime._park_agent_input`` when
    ``update_mcp_task_projection``'s optimistic-concurrency check keeps
    losing a race after every retry attempt. This is a transient
    concurrency conflict, never a client parameter problem -- unlike
    ``put_mcp_task``'s genuine task-identity-reuse conflict,
    it must never be surfaced as ``INVALID_PARAMS`` (clio-relay#218 rework).
    A distinct subtype, rather than a message/keyword match, is what lets
    ``intercept_tool_call`` discriminate the two conflict sources by type.
    """


class NotFoundError(PublicMessageError, RelayError):
    """Raised when a requested record is missing."""
