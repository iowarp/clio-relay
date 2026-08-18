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
    ``put_mcp_task``'s genuine task-identity-reuse ``QueueConflictError``,
    it must never be surfaced as ``INVALID_PARAMS`` (clio-relay#218 rework).
    A distinct subtype, rather than a message/keyword match, is what lets
    ``intercept_tool_call`` discriminate the two conflict sources by type.
    """


class NotFoundError(PublicMessageError, RelayError):
    """Raised when a requested record is missing."""


SHELL_COMMAND_NOT_FOUND_STATUS = 127
"""POSIX shell exit status for "command not found".

A structured protocol signal (not prose), so classifying on it is typed
discrimination rather than message matching.
"""


class BoundedCommandTimeout(RelayError):
    """A locally bounded child command exceeded its own deadline.

    Carried as a DISTINCT TYPE so callers discriminate a real transport
    deadline structurally. Flattening the underlying timeout into a prose
    ``RelayError`` forced callers to re-sniff ``"timed out" in str(exc)``,
    which misclassified any remote failure whose message merely mentioned a
    timeout -- routing it into the deadline RETRY path (clio-relay#158).
    """


class RemoteCommandFailed(RelayError):
    """A cluster-targeted remote command exited non-zero.

    Carries the exit status so callers discriminate failure shapes
    structurally instead of matching on the raw stderr blob.
    """

    reason = "remote_command_failed"

    def __init__(self, message: str, *, exit_status: int | None = None) -> None:
        super().__init__(message)
        self.exit_status = exit_status


class RemoteExecutableMissingError(RemoteCommandFailed):
    """The cluster's configured relay executable is absent on the remote host.

    A POSIX shell reports exit status 127 when it cannot find the command it
    was asked to run. For a cluster-targeted invocation that means the
    registry's ``relay_executable`` points at a path that no longer exists --
    typically a generation directory that was garbage-collected out from under
    the pin (clio-relay#158).

    Typed separately because it is a REPAIRABLE DEPLOYMENT state, not a remote
    error: the cure is to re-run ``cluster bootstrap``, which reinstalls the
    relay and re-points the registry at what it produced.
    """

    reason = "relay_executable_missing"


def relay_executable_missing(
    *,
    cluster: str,
    ssh_host: str,
    relay_executable: str,
    detail: str,
    exit_status: int,
) -> RemoteExecutableMissingError:
    """Build the one authored message for a dead remote relay pointer.

    Shared by every transport so the operator sees the same repair
    instruction regardless of which command tripped over the stale pin.
    """
    return RemoteExecutableMissingError(
        f"configured relay_executable is absent on the remote host ({ssh_host}): "
        f"{relay_executable}. Re-run `clio-relay cluster bootstrap --cluster "
        f"{cluster}` to reinstall the relay and re-point the registry at what "
        f"it produced. remote detail: {detail}",
        exit_status=exit_status,
    )
