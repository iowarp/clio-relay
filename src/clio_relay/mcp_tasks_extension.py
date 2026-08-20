"""The SEP-2663 wire adapter: ``RelayTasksExtension``.

Extracted from ``fastmcp_server.py`` (clio-relay#231 decomposition): the
``ServerExtension`` binding ``tasks/get``/``tasks/update``/``tasks/cancel``
and intercepting every task-capable ``tools/call`` so a durable relay job is
materialized into a SEP-2663 task only after ``RelayMcpRuntime`` durably
admits it (relay#234). Depends on both ``mcp_task_runtime.py``
(``RelayMcpRuntime``, the projection/admission operations this extension
wraps) and ``mcp_tool_provider.py`` (``RelayTool``, for the
``intercept_tool_call`` isinstance check) -- neither of those two depends
back on this module, so the dependency graph stays one-directional.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import mcp_types
from fastmcp.exceptions import NotFoundError as FastMcpNotFoundError
from fastmcp.server.dependencies import extract_version_spec, get_http_request
from fastmcp.server.extensions import (
    MethodBinding,
    ServerExtension,
    read_client_extension_settings,
)
from fastmcp.tools import InputRequiredToolResult, ToolResult
from fastmcp.utilities.tasks import TASKS_EXTENSION_ID
from fastmcp.utilities.versions import VersionSpec
from fastmcp_tasks import wire_production
from fastmcp_tasks.models import (
    MISSING_REQUIRED_CLIENT_CAPABILITY,
    CancelTaskParams,
    CancelTaskResult,
    CreateTaskResult,
    GetTaskParams,
    GetTaskResult,
    UpdateTaskParams,
    UpdateTaskResult,
    missing_capability_error_data,
)
from mcp.server.context import ServerRequestContext
from mcp.shared.exceptions import MCPError
from mcp.shared.inbound import MCP_NAME_HEADER, decode_header_value
from mcp_types.jsonrpc import HEADER_MISMATCH
from mcp_types.version import MODERN_PROTOCOL_VERSIONS

from clio_relay import door_error_adapters, door_errors
from clio_relay.errors import (
    McpTaskIdentityConflictError,
    NotFoundError,
    TaskInputParkConflictError,
)
from clio_relay.mcp_task_projection import TASK_POLL_INTERVAL_MS, TASK_TTL_MS
from clio_relay.mcp_task_runtime import RelayMcpRuntime
from clio_relay.mcp_tool_provider import RelayTool
from clio_relay.models import TERMINAL_STATES, JobState, RelayMcpTaskRecord

if TYPE_CHECKING:
    from fastmcp.server.context import Context
    from fastmcp.server.extensions import ToolCallContinuation, ToolCallOutcome

# Logs under the pre-split module identity, not this file's own __name__:
# test_fastmcp_server.py's reconciliation-failure assertions (and any
# external log filter) key on `record.name == "clio_relay.fastmcp_server"`,
# the logger fastmcp_server.py's own `logger.exception(...)` call used before
# this handler moved here (clio-relay#231 decomposition). A bare
# `getLogger(__name__)` would silently rename every log record this module
# emits to "clio_relay.mcp_tasks_extension" -- a behavior change a pure move
# must not make.
logger = logging.getLogger("clio_relay.fastmcp_server")

_TASK_METHOD_VERSIONS = frozenset(MODERN_PROTOCOL_VERSIONS)


class RelayTasksExtension(ServerExtension):
    """SEP-2663 wire adapter projecting the relay's own durable jobs."""

    identifier = TASKS_EXTENSION_ID

    def __init__(self, runtime: RelayMcpRuntime) -> None:
        self._runtime = runtime

    def settings(self) -> dict[str, Any]:
        """Advertise the standard tasks extension without private settings."""
        return {}

    def methods(self) -> Sequence[MethodBinding]:
        """Bind the three SEP-2663 task-management methods."""
        return (
            MethodBinding(
                method="tasks/get",
                params_type=GetTaskParams,
                handler=self._handle_get,
                protocol_versions=_TASK_METHOD_VERSIONS,
            ),
            MethodBinding(
                method="tasks/update",
                params_type=UpdateTaskParams,
                handler=self._handle_update,
                protocol_versions=_TASK_METHOD_VERSIONS,
            ),
            MethodBinding(
                method="tasks/cancel",
                params_type=CancelTaskParams,
                handler=self._handle_cancel,
                protocol_versions=_TASK_METHOD_VERSIONS,
            ),
        )

    @asynccontextmanager
    async def lifespan(self) -> AsyncGenerator[None]:
        """Install only the official task-result wire claim producer."""
        wire_production.install()
        try:
            yield
        finally:
            wire_production.uninstall()

    @staticmethod
    def _require_capability(ctx: ServerRequestContext[Any, Any]) -> None:
        if read_client_extension_settings(ctx, TASKS_EXTENSION_ID) is None:
            raise MCPError(
                code=MISSING_REQUIRED_CLIENT_CAPABILITY,
                message=(
                    "This request targets io.modelcontextprotocol/tasks, but the "
                    "client did not declare that extension for this request."
                ),
                data=missing_capability_error_data(),
            )

    @staticmethod
    def _require_matching_route(task_id: str) -> None:
        try:
            request = get_http_request()
        except RuntimeError:
            return
        header = request.headers.get(MCP_NAME_HEADER)
        if header is not None and decode_header_value(header) != task_id:
            raise MCPError(
                code=HEADER_MISMATCH,
                message=(
                    f"{MCP_NAME_HEADER} header does not match the request body's 'taskId' parameter"
                ),
            )

    def _check_request(
        self,
        ctx: ServerRequestContext[Any, Any],
        task_id: str,
    ) -> None:
        self._require_capability(ctx)
        self._require_matching_route(task_id)

    async def _record(self, task_id: str) -> RelayMcpTaskRecord:
        try:
            return await asyncio.to_thread(self._runtime.queue.get_mcp_task, task_id)
        except NotFoundError as exc:
            raise door_error_adapters.as_mcp_error(door_errors.classify(exc)) from exc

    async def _handle_get(
        self,
        ctx: ServerRequestContext[Any, Any],
        params: GetTaskParams,
    ) -> GetTaskResult:
        self._check_request(ctx, params.task_id)
        record = await self._record(params.task_id)
        try:
            return await self._runtime.task_status(record)
        except MCPError:
            raise
        except Exception as exc:
            # relay#215, grounding + rationale now owned by
            # door_errors.REASONS["mcp_task_status_reconciliation_failed"]'s
            # docstring. Raising via door_errors short-circuits the SDK's own
            # logger.exception(...) (mcp/server/runner.py's modern_error_data
            # only logs when the handler exception is left for its generic
            # catch-all), so the traceback is logged here or it is lost
            # entirely.
            logger.exception("relay could not reconcile task %r's status", params.task_id)
            raise door_error_adapters.as_mcp_error(
                door_errors.classify(
                    exc,
                    reason="mcp_task_status_reconciliation_failed",
                    message="relay could not reconcile this task's status.",
                    data={"task_id": params.task_id},
                )
            ) from exc

    async def _handle_update(
        self,
        ctx: ServerRequestContext[Any, Any],
        params: UpdateTaskParams,
    ) -> UpdateTaskResult:
        self._check_request(ctx, params.task_id)
        await self._runtime.update_task(
            await self._record(params.task_id),
            params.input_responses,
        )
        return UpdateTaskResult()

    async def _handle_cancel(
        self,
        ctx: ServerRequestContext[Any, Any],
        params: CancelTaskParams,
    ) -> CancelTaskResult:
        self._check_request(ctx, params.task_id)
        await self._runtime.cancel_task(await self._record(params.task_id))
        return CancelTaskResult()

    async def intercept_tool_call(
        self,
        params: mcp_types.CallToolRequestParams,
        context: Context,
        call_next: ToolCallContinuation,
    ) -> ToolCallOutcome:
        """Materialize a task only after the relay has durably admitted its job."""
        version_value = extract_version_spec(params.meta)
        version = VersionSpec(eq=version_value) if version_value else None
        try:
            tool = await context.fastmcp.get_tool(params.name, version)
        except FastMcpNotFoundError:
            tool = None
        if tool is None or not tool.task_config.supports_tasks():
            return await call_next()

        request_context = context.request_context
        opted_in = (
            request_context is not None
            and request_context.protocol_version in MODERN_PROTOCOL_VERSIONS
            and context.client_extension_settings(TASKS_EXTENSION_ID) is not None
        )
        if not opted_in:
            if tool.task_config.mode == "required":
                raise MCPError(
                    code=MISSING_REQUIRED_CLIENT_CAPABILITY,
                    message=f"Tool {tool.name!r} requires io.modelcontextprotocol/tasks.",
                    data=missing_capability_error_data(),
                )
            return await call_next()

        outcome = await call_next()
        if isinstance(outcome, InputRequiredToolResult):
            # SEP-2663 requires pre-creation MRTR to finish synchronously. The
            # client re-enters this tools/call with answers; only the leg that
            # durably admits a relay job is converted into a task.
            return outcome
        if not isinstance(outcome, ToolResult) or not isinstance(tool, RelayTool):
            return outcome
        try:
            task = await self._runtime.create_task(
                tool=tool,
                arguments=dict(params.arguments or {}),
                result=outcome,
                context=context,
            )
        except TaskInputParkConflictError as exc:
            # relay#218 rework, grounding now owned by
            # door_errors.REASONS["mcp_task_input_park_conflict"]'s
            # docstring. A distinct exception TYPE (checked first, so it
            # never reaches the broader except below) is what makes this a
            # non-heuristic discrimination rather than a message/keyword
            # match against the genuine task-identity conflict below.
            raise door_error_adapters.as_mcp_error(door_errors.classify(exc)) from exc
        except McpTaskIdentityConflictError as exc:
            # Only this type carries a reviewed public queue-conflict message.
            conflicting_task_id = (
                outcome.structured_content.get("job_id")
                if isinstance(outcome.structured_content, dict)
                else None
            )
            # clio-relay#242 actionability audit (R9 doctrine): an agent
            # meeting this refusal must be told what to do next, not just
            # that a conflict happened -- door_errors.classify() falls back
            # to the generic reason title ("MCP task conflict.") for an
            # unmarked QueueConflictError with no explicit message=, which
            # left the caller nothing to act on. Name the conflicting task
            # and the tasks/get query verb explicitly.
            conflict_message = (
                (
                    f"a task ({conflicting_task_id}) already handles this exact "
                    "submission; not retryable with the same input -- poll it via "
                    f"tasks/get with task_id={conflicting_task_id!r}, or change an "
                    "input field (e.g. supply a fresh idempotency key) to submit a "
                    "genuinely new task"
                )
                if conflicting_task_id is not None
                else (
                    "an existing task already handles this exact submission; not "
                    "retryable with the same input -- change an input field (e.g. "
                    "supply a fresh idempotency key) to submit a genuinely new task"
                )
            )
            raise door_error_adapters.as_mcp_error(
                door_errors.classify(
                    door_errors.public_message_error(exc),
                    reason="mcp_task_conflict",
                    message=conflict_message,
                    data={"task_id": conflicting_task_id},
                )
            ) from exc
        except MCPError:
            # relay#234 adversarial review: create_task -- or anything it
            # calls -- can itself raise an MCPError that already carries
            # its own typed wire shape (e.g. a nested dispatch failure).
            # Re-classifying an already-typed error here would erase its
            # real reason behind a generic internal_error, so it passes
            # through untouched.
            raise
        except Exception as exc:
            # relay#234 adversarial review: put_mcp_task (and everything
            # else create_task calls -- _terminal_completed_result's
            # wait_mcp_job round trip, _park_agent_input's projection
            # update) can raise something other than the two conflict
            # types above -- disk-full, permission, or any other untyped
            # domain/OS failure. Left uncaught here, that exception
            # previously escaped through FastMCP's own generic handler
            # with no relay reason at all -- a silent, untyped fallback
            # the no-silent-fallback doctrine forbids. door_errors.classify
            # maps it to whatever typed reason its exception type carries
            # (dispatch rule 3), or to "internal_error" (rule 4) -- either
            # way logging the traceback exactly once, here, via classify's
            # own logger.exception call, and never letting the exception's
            # internals reach the wire.
            raise door_errors.as_mcp_error(door_errors.classify(exc)) from exc
        if task is None:
            return outcome
        return CreateTaskResult(
            task_id=task.task_id,
            status=(
                "input_required"
                if task.projection.input_round is not None
                and task.projection.input_round.outstanding
                else "cancelled"
                if task.state is JobState.CANCELED
                else "completed"
                if task.state in TERMINAL_STATES
                else "working"
            ),
            status_message=f"Relay job is {task.state.value}",
            created_at=task.created_at.isoformat(),
            last_updated_at=task.updated_at.isoformat(),
            ttl_ms=TASK_TTL_MS,
            poll_interval_ms=TASK_POLL_INTERVAL_MS,
        )
