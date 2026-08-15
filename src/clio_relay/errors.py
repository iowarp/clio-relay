"""Relay-specific exceptions."""


class RelayError(RuntimeError):
    """Base class for relay errors."""


class ObservationTimeoutError(RelayError):
    """A bounded observation transport expired without changing durable work."""


class ConfigurationError(RelayError):
    """Raised when required external configuration is absent."""


class QueueConflictError(RelayError):
    """Raised when a queue operation violates an invariant."""


class TaskInputParkConflictError(QueueConflictError):
    """A durable MCP task's post-admission input round exhausted CAS retries.

    Raised only by ``RelayMcpRuntime._park_agent_input`` when
    ``update_mcp_task_projection``'s optimistic-concurrency check keeps
    losing a race after every retry attempt. This is a transient
    concurrency conflict, never a client parameter problem -- unlike
    ``put_mcp_task``'s genuine task-identity-reuse ``QueueConflictError``,
    it must never be surfaced as ``INVALID_PARAMS`` (clio-relay#218 rework).
    A distinct subtype, rather than a message/keyword match, is what lets
    ``intercept_tool_call`` discriminate the two conflict sources by type.
    """


class NotFoundError(RelayError):
    """Raised when a requested record is missing."""
