"""The relay-backed SEP-2663 task runtime: ``RelayMcpRuntime``.

Extracted from ``fastmcp_server.py`` (clio-relay#231 decomposition): the
shared queue/profile/task-projection operations for one MCP server --
dispatching a tool call, admitting a durable SEP-2663 task from its receipt,
projecting the task's current status, applying replay-safe input-round
answers, and cancelling -- plus the one CallToolResult wire-document builder
(``_call_tool_result_document``) both the eager (create-time) and lazy
(first-``tasks/get``) resolution paths share, so the two can never diverge
(see ``_terminal_completed_result``'s own docstring, C1). At ~590 lines this
sits above the 150-500 sweet spot; ``RelayMcpRuntime`` is one cohesive class
(the doc's own "shared queue/profile/projection operations for one MCP
server" boundary) and splitting it along call/create/status/cancel lines
would break the single-source-of-truth guarantee C1 depends on, so it stays
one module rather than a forced cut.

**Patch-seam note.** ``call_mcp_tool``/``status_mcp_job``/``wait_mcp_job`` are
looked up through the ``clio_relay.fastmcp_server`` facade module object via
a function-local ``import clio_relay.fastmcp_server as fastmcp_server`` at
each of their four call sites (the same established discipline
``cli_jarvis_mcp.py`` documents for reaching a collaborator's *current* name
binding), never imported by bare name into this module's own globals. The
existing test suite patches these three names via
``monkeypatch.setattr(fastmcp_server_module, "call_mcp_tool", ...)`` (and the
``status_mcp_job``/``wait_mcp_job`` siblings) against the facade module
object test_fastmcp_server.py imports as ``clio_relay.fastmcp_server`` --
patching only this module's own (would-be) ``from ... import call_mcp_tool``
binding would leave that patch dead, since Python name bindings are copied
at import time and do not follow a later rebind of the origin module's
attribute. A function-local import (deferred to call time, long after every
module has finished loading) also sidesteps the circular-import ordering a
module-level ``import clio_relay.fastmcp_server`` would otherwise depend on.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast

import mcp_types
from fastmcp import FastMCP
from fastmcp.server.dependencies import get_context
from fastmcp.tools import ToolResult
from fastmcp_tasks.models import GetTaskResult

from clio_relay import door_error_adapters, door_errors
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import (
    ObservationPatternError,
    QueueConflictError,
    TaskInputParkConflictError,
)
from clio_relay.mcp_agent_input_guard import (
    _AGENT_INPUT_KEY,
    _agent_input_enabled,
    _post_admission_agent_input_guard,
    _requests_document,
)
from clio_relay.mcp_server import (
    mcp_tool_result_failed,
    normalize_mcp_profile,
    serialize_mcp_tool_result,
)
from clio_relay.mcp_session_state_codec import _load_session, _save_session
from clio_relay.mcp_task_projection import (
    TASK_POLL_INTERVAL_MS,
    TASK_TTL_MS,
    _relay_state_projection,
)
from clio_relay.models import (
    MAX_MCP_TASK_ARGUMENT_BYTES,
    TERMINAL_STATES,
    JobState,
    RelayJob,
    RelayMcpInputRound,
    RelayMcpTaskProjection,
    RelayMcpTaskRecord,
)
from clio_relay.storage_runtime import StorageManagedQueue, storage_managed_queue

if TYPE_CHECKING:
    from fastmcp.server.context import Context

    from clio_relay.mcp_tool_provider import RelayTool

# Logs under the pre-split module identity, not this file's own __name__:
# a bare `getLogger(__name__)` would silently rename every log record
# `create_task`'s deferred-eager-resolution path emits from
# "clio_relay.fastmcp_server" to "clio_relay.mcp_task_runtime" -- a behavior
# change a pure move must not make (see mcp_tasks_extension.py's matching
# note; test_fastmcp_server.py's own reconciliation-failure assertions key on
# the same fixed logger name for the sibling handler there).
logger = logging.getLogger("clio_relay.fastmcp_server")

JSON = dict[str, Any]
MAX_TASK_ARGUMENT_BYTES = MAX_MCP_TASK_ARGUMENT_BYTES


class RelayMcpRuntime:
    """Shared queue, profile, and projection operations for one MCP server."""

    def __init__(
        self,
        *,
        settings: RelaySettings,
        profile: str,
        queue: ClioCoreQueue | None = None,
    ) -> None:
        self.settings = settings
        self.profile = normalize_mcp_profile(profile)
        if queue is None:
            owned_queue = storage_managed_queue(settings)
            self.queue: ClioCoreQueue = owned_queue
            self._owned_queue: StorageManagedQueue | None = owned_queue
        else:
            self.queue = queue
            self._owned_queue = None

    @asynccontextmanager
    async def lifespan(self, _server: FastMCP[Any]) -> AsyncGenerator[dict[str, Any]]:
        """Initialize and, when owned, close the relay queue."""
        await asyncio.to_thread(self.queue.initialize)
        try:
            yield {"relay_runtime": self}
        finally:
            if self._owned_queue is not None:
                await asyncio.to_thread(self._owned_queue.close)

    async def call_tool(
        self,
        *,
        name: str,
        arguments: JSON,
        catalog_revision: str | None,
    ) -> ToolResult:
        """Execute the established relay tool dispatcher behind FastMCP."""
        import clio_relay.fastmcp_server as fastmcp_server

        session = await _load_session()
        try:
            raw = await asyncio.to_thread(
                lambda: fastmcp_server.call_mcp_tool(
                    {"name": name, "arguments": arguments},
                    queue=self.queue,
                    settings=self.settings,
                    profile=self.profile,
                    session=session,
                    observed_remote_mcp_catalog_revision=catalog_revision,
                    require_advertised_remote_mcp_catalog=catalog_revision is not None,
                )
            )
        except ObservationPatternError as exc:
            raise door_error_adapters.as_mcp_error(
                door_errors.classify(
                    exc,
                    reason=exc.reason,
                    message=str(exc),
                )
            ) from exc
        await _save_session(session)
        content = [
            mcp_types.TextContent.model_validate(item)
            for item in cast(list[object], raw.get("content", []))
        ]
        structured = raw.get("structuredContent")
        if not isinstance(structured, dict):
            raise ValueError(f"relay tool {name!r} did not return structured content")
        typed_structured = cast(JSON, structured)
        return ToolResult(
            content=content,
            structured_content=typed_structured,
            is_error=raw.get("isError") is True,
        )

    async def create_task(
        self,
        *,
        tool: RelayTool,
        arguments: JSON,
        result: ToolResult,
        context: Context | None = None,
    ) -> RelayMcpTaskRecord | None:
        """Create a durable SEP task when a tool returned a relay job receipt."""
        structured = result.structured_content
        if not isinstance(structured, dict):
            return None
        job_id = structured.get("job_id")
        state_value = structured.get("state")
        if not isinstance(job_id, str) or not isinstance(state_value, str):
            return None
        try:
            state = JobState(state_value)
        except ValueError:
            return None
        # relay#234: task creation itself is gated ONLY on relay admission
        # succeeding (docs/mcp-tasks.md:122-139) -- ``request_followup_message``
        # is a per-call opt-in for ONE extra post-admission input round on the
        # agent lane (docs/mcp-tasks.md:147-153), never a precondition for
        # minting a task at all. A prior gate here
        # (``if tool.task_requires_post_admission_input and not
        # _agent_input_enabled(...): return None``) conflated the two: every
        # ``relay_submit_agent``/``relay_submit_remote_agent`` call that did
        # not set ``request_followup_message=True`` silently fell back to a
        # plain ``CallToolResult`` for a client that explicitly declared task
        # semantics, regardless of whether the underlying job was already
        # terminal or still queued/running -- the admission gate never
        # engaged for the agent lane's default (no-follow-up) case. Whether
        # this call actually parks an input round remains solely
        # ``_agent_input_enabled``'s decision, a few lines below.
        encoded_arguments = json.dumps(
            arguments,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        if len(encoded_arguments) > MAX_TASK_ARGUMENT_BYTES:
            raise ValueError("task arguments exceed the durable MCP projection limit")
        projection = RelayMcpTaskProjection(
            tool_name=tool.name,
            profile=self.profile,
            arguments=arguments,
            catalog_revision=tool.catalog_revision,
            initial_result=structured,
        )
        if state in TERMINAL_STATES and state is not JobState.CANCELED:
            # The synchronous call that produced ``structured`` already ran the
            # job to completion (relay#215 / D22). Resolve the durable
            # ``completed_result`` now, through the IDENTICAL ``wait_mcp_job``
            # -based document builder ``task_status``'s lazy re-derivation uses
            # (``_terminal_completed_result``) -- never by wrapping the
            # create-time receipt directly. The two are structurally different
            # documents (the receipt is flat: ``job_id``/``state`` at the top
            # level; the wait document nests the job under ``job`` alongside
            # ``relay_queue``/``scheduler``/``transform``), and a client that
            # only recognizes the wait shape silently received relay's own
            # bookkeeping instead of the tool's result (C1). Sharing the exact
            # builder makes the two paths structurally incapable of
            # diverging, at the cost of paying the re-derivation now instead
            # of on first poll -- correctness over the round-trip saving.
            # CANCELED is excluded: it must keep reporting the honest
            # ``cancelled`` status (C3), never a completed-looking result.
            provisional = RelayMcpTaskRecord(
                task_id=job_id,
                job_id=job_id,
                state=state,
                projection=projection,
            )
            # N1: this re-derivation performs a network round trip
            # (``wait_mcp_job`` over the owned session, possibly SSH-tunnelled)
            # *inside* ``tools/call``, which has no surrounding try/except
            # (``intercept_tool_call``). Left unguarded, a transport failure
            # here would kill the whole dispatch AND leave no durable task
            # record at all -- relay#215's defect class, relocated from
            # ``_handle_get`` (which already has its own guard) into
            # ``create_task`` (which did not). Degrade to the lazy path
            # instead: it is fully intact, and per C1 it produces the
            # byte-identical document through this exact same builder on
            # first ``tasks/get`` -- so deferring is a safe, silent-wrong-
            # answer-free degradation, never a lost dispatch.
            try:
                completed_result = await self._terminal_completed_result(provisional)
            except Exception:
                logger.exception(
                    "mcp_task_eager_result_deferred: eager completed_result "
                    "resolution failed for task %r; deferring to tasks/get",
                    job_id,
                )
                completed_result = None
            projection = RelayMcpTaskProjection.model_validate(
                {
                    **projection.model_dump(mode="python"),
                    "completed_result": completed_result,
                }
            )
        record = RelayMcpTaskRecord(
            task_id=job_id,
            job_id=job_id,
            state=state,
            projection=projection,
        )
        # put_mcp_task's QueueConflictError (a genuine task-identity reuse
        # conflict) is intentionally left unwrapped here -- this is the
        # RUNTIME/domain layer, and a direct caller (e.g. a test invoking
        # create_task() without going through the MCP wire) must see the
        # plain domain exception. intercept_tool_call, the protocol-facing
        # interceptor, is what translates it into a typed MCPError for the
        # wire (clio-relay#218 rework: TYPE, not call site, is what lets it
        # discriminate this leg from _park_agent_input's own
        # TaskInputParkConflictError below).
        saved = await asyncio.to_thread(self.queue.put_mcp_task, record)
        if not _agent_input_enabled(tool.name, arguments):
            return saved
        if context is None:
            raise ValueError("post-admission agent input requires an MCP task context")
        return await self._park_agent_input(saved, context)

    async def _terminal_completed_result(self, record: RelayMcpTaskRecord) -> JSON:
        """Serialize one terminal job's result through the SAME builder
        ``task_status``'s lazy re-derivation uses (``wait_mcp_job`` then
        ``_call_tool_result_document``). Sharing this one function is what
        makes an eager (create-time) and lazy (first-``tasks/get``)
        resolution of the identical job byte-for-byte identical -- see C1.
        """
        import clio_relay.fastmcp_server as fastmcp_server

        route_arguments = self._route_arguments(record)
        waited = await asyncio.to_thread(
            lambda: fastmcp_server.wait_mcp_job(
                {
                    **route_arguments,
                    "timeout_seconds": 0.01,
                    "poll_seconds": 0.01,
                    "include_logs": False,
                },
                queue=self.queue,
                settings=self.settings,
            )
        )
        return _call_tool_result_document(waited)

    async def _park_agent_input(
        self,
        record: RelayMcpTaskRecord,
        context: Context,
    ) -> RelayMcpTaskRecord:
        """Persist the guard's first post-admission leg with CAS retry."""
        candidate = record
        for _attempt in range(8):
            if candidate.projection.input_round is not None:
                return candidate
            outcome = _post_admission_agent_input_guard(context, task_id=candidate.task_id)
            if not isinstance(outcome, mcp_types.InputRequiredResult):
                raise ValueError("post-admission agent input did not originate an input round")
            outstanding = _requests_document(outcome)
            if set(outstanding) != {_AGENT_INPUT_KEY}:
                raise ValueError("post-admission agent input produced an unexpected request set")
            issued_keys = [*candidate.projection.issued_input_keys, *outstanding]
            projection = RelayMcpTaskProjection.model_validate(
                {
                    **candidate.projection.model_dump(mode="python"),
                    "issued_input_keys": issued_keys,
                    "input_round": RelayMcpInputRound(
                        leg=1,
                        outstanding=outstanding,
                        request_state=outcome.request_state,
                    ),
                }
            )
            try:
                return await asyncio.to_thread(
                    self.queue.update_mcp_task_projection,
                    candidate.task_id,
                    projection,
                    expected_updated_at=candidate.updated_at,
                )
            except QueueConflictError:
                candidate = await asyncio.to_thread(
                    self.queue.get_mcp_task,
                    candidate.task_id,
                )
        # clio-relay#218 rework: a distinct subtype (never the base
        # QueueConflictError put_mcp_task raises for a genuine identity
        # conflict) is what lets intercept_tool_call refuse to mistype this
        # transient CAS-exhaustion conflict as INVALID_PARAMS -- it is not a
        # client parameter problem, unlike put_mcp_task's conflict.
        raise TaskInputParkConflictError(
            f"MCP task input could not park after concurrent updates: {record.task_id}"
        )

    @staticmethod
    def _route_arguments(record: RelayMcpTaskRecord) -> JSON:
        arguments: JSON = {"job_id": record.job_id}
        initial = record.projection.initial_result
        if initial.get("remote") is True:
            cluster = initial.get("cluster")
            route_revision = initial.get("route_revision")
            if not isinstance(cluster, str) or not isinstance(route_revision, str):
                raise ValueError("remote MCP task omitted its immutable relay route")
            arguments.update({"cluster": cluster, "route_revision": route_revision})
        return arguments

    async def task_status(self, record: RelayMcpTaskRecord) -> GetTaskResult:
        """Project the current relay job state into a SEP-2663 task result."""
        import clio_relay.fastmcp_server as fastmcp_server

        projection = record.projection
        common: JSON = {
            "task_id": record.task_id,
            "created_at": record.created_at.isoformat(),
            "last_updated_at": record.updated_at.isoformat(),
            "ttl_ms": TASK_TTL_MS,
            "poll_interval_ms": TASK_POLL_INTERVAL_MS,
        }
        if projection.protocol_error is not None:
            status, _ = _relay_state_projection("protocol_error")
            return GetTaskResult(status=status, error=projection.protocol_error, **common)
        if projection.input_round is not None and projection.input_round.outstanding:
            status, _ = _relay_state_projection("durable_input_round")
            return GetTaskResult(
                status=status,
                input_requests=projection.input_round.outstanding,
                **common,
            )
        if projection.completed_result is not None:
            observation = (
                "tool_failure"
                if projection.completed_result.get("isError") is True
                else "succeeded"
            )
            status, _ = _relay_state_projection(observation)
            return GetTaskResult(
                status=status,
                result=projection.completed_result,
                **common,
            )

        route_arguments = self._route_arguments(record)
        if projection.initial_result.get("remote") is True:
            status = await asyncio.to_thread(
                lambda: fastmcp_server.status_mcp_job(
                    route_arguments,
                    queue=self.queue,
                    settings=self.settings,
                )
            )
            raw_job = status.get("job")
            if not isinstance(raw_job, dict):
                raise ValueError("relay status omitted its durable job record")
            job = RelayJob.model_validate(raw_job)
        else:
            job = await asyncio.to_thread(self.queue.get_job, record.job_id)
        if job.state is JobState.CANCELED:
            status, _ = _relay_state_projection(job.state.value)
            return GetTaskResult(
                status=status,
                status_message=job.last_error,
                last_updated_at=job.updated_at.isoformat(),
                **{key: value for key, value in common.items() if key != "last_updated_at"},
            )
        if job.state not in TERMINAL_STATES:
            status, _ = _relay_state_projection(job.state.value)
            return GetTaskResult(
                status=status,
                status_message=f"Relay job is {job.state.value}",
                last_updated_at=job.updated_at.isoformat(),
                **{key: value for key, value in common.items() if key != "last_updated_at"},
            )

        final = await self._terminal_completed_result(record)
        completed_projection = RelayMcpTaskProjection.model_validate(
            {
                **projection.model_dump(mode="python"),
                "completed_result": final,
            }
        )
        try:
            updated = await asyncio.to_thread(
                lambda: self.queue.update_mcp_task_projection(
                    record.task_id,
                    completed_projection,
                    expected_updated_at=record.updated_at,
                    state=job.state,
                )
            )
        except QueueConflictError:
            updated = await asyncio.to_thread(self.queue.get_mcp_task, record.task_id)
            if updated.projection.completed_result is None:
                raise
            final = updated.projection.completed_result
        observation = "tool_failure" if final.get("isError") is True else "succeeded"
        status, _ = _relay_state_projection(observation)
        return GetTaskResult(
            status=status,
            result=final,
            status_message=f"Relay job {job.state.value}",
            last_updated_at=job.updated_at.isoformat(),
            **{key: value for key, value in common.items() if key != "last_updated_at"},
        )

    async def update_task(self, record: RelayMcpTaskRecord, responses: JSON) -> None:
        """Apply replay-safe answers to the current durable input round."""
        candidate = record
        for _attempt in range(8):
            current = candidate.projection.input_round
            if current is None or not current.outstanding:
                return
            matched = {key: value for key, value in responses.items() if key in current.outstanding}
            if not matched:
                return
            outstanding = {
                key: value for key, value in current.outstanding.items() if key not in matched
            }
            updated_round = RelayMcpInputRound.model_validate(
                {
                    **current.model_dump(mode="python"),
                    "outstanding": outstanding,
                    "answered": {**current.answered, **matched},
                }
            )
            if (
                not outstanding
                and current.leg == 1
                and current.request_state is not None
                and _agent_input_enabled(
                    candidate.projection.tool_name,
                    candidate.projection.arguments,
                )
            ):
                updated_round = self._resume_agent_input(candidate.task_id, updated_round)
            projection = RelayMcpTaskProjection.model_validate(
                {
                    **candidate.projection.model_dump(mode="python"),
                    "input_round": updated_round,
                }
            )
            try:
                await asyncio.to_thread(
                    self.queue.update_mcp_task_projection,
                    candidate.task_id,
                    projection,
                    expected_updated_at=candidate.updated_at,
                )
                return
            except QueueConflictError:
                candidate = await asyncio.to_thread(
                    self.queue.get_mcp_task,
                    candidate.task_id,
                )
        raise QueueConflictError(
            f"MCP task input could not converge after concurrent updates: {record.task_id}"
        )

    @staticmethod
    def _resume_agent_input(
        task_id: str,
        input_round: RelayMcpInputRound,
    ) -> RelayMcpInputRound:
        """Re-enter the guard with durable answers without blocking a worker."""
        context = get_context()
        task_context = cast(Any, context)
        previous_task_id = task_context._task_id
        previous_request_state = task_context._task_request_state
        previous_input_responses = task_context._task_input_responses
        typed_responses = {
            key: mcp_types.ElicitResult.model_validate(value)
            for key, value in input_round.answered.items()
        }
        try:
            task_context._task_id = task_id
            task_context._task_request_state = input_round.request_state
            task_context._task_input_responses = typed_responses
            outcome = _post_admission_agent_input_guard(context, task_id=task_id)
        finally:
            task_context._task_id = previous_task_id
            task_context._task_request_state = previous_request_state
            task_context._task_input_responses = previous_input_responses
        if isinstance(outcome, mcp_types.InputRequiredResult):
            raise ValueError("post-admission agent input did not consume its answer")
        return RelayMcpInputRound.model_validate(
            {
                **input_round.model_dump(mode="python"),
                "leg": input_round.leg + 1,
                "request_state": None,
            }
        )

    async def cancel_task(self, record: RelayMcpTaskRecord) -> None:
        """Request cancellation through the relay's existing cancellation path."""
        import clio_relay.fastmcp_server as fastmcp_server
        from clio_relay.queue_management import cancel_queue_job

        initial = record.projection.initial_result
        if initial.get("remote") is True:
            await asyncio.to_thread(
                lambda: fastmcp_server.call_mcp_tool(
                    {
                        "name": "relay_cancel",
                        "arguments": {
                            **self._route_arguments(record),
                            "cancel_scheduler_job": True,
                        },
                    },
                    queue=self.queue,
                    settings=self.settings,
                    profile=self.profile,
                    session=None,
                    observed_remote_mcp_catalog_revision=None,
                    require_advertised_remote_mcp_catalog=False,
                )
            )
        else:
            await asyncio.to_thread(
                lambda: cancel_queue_job(
                    self.queue,
                    record.job_id,
                    scheduler_policy="request-scheduler",
                )
            )


def _call_tool_result_document(result: JSON) -> JSON:
    """Build the exact CallToolResult wire object for one relay result."""
    raw_job = result.get("job")
    job_failed = (
        isinstance(raw_job, dict)
        and cast(dict[str, object], raw_job).get("state") == JobState.FAILED.value
    )
    call_result = mcp_types.CallToolResult(
        content=[
            mcp_types.TextContent(
                type="text",
                text=serialize_mcp_tool_result(result),
            )
        ],
        structured_content=result,
        is_error=(job_failed or mcp_tool_result_failed(result)),
    )
    return call_result.model_dump(by_alias=True, mode="json", exclude_none=True)
