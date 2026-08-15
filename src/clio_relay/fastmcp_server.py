"""Native FastMCP server and relay-backed SEP-2663 task projection."""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Final, Literal, cast

import mcp_types
from fastmcp import FastMCP
from fastmcp.exceptions import NotFoundError as FastMcpNotFoundError
from fastmcp.server.auth import AccessToken, AuthProvider, TokenVerifier
from fastmcp.server.dependencies import extract_version_spec, get_context, get_http_request
from fastmcp.server.extensions import (
    MethodBinding,
    ServerExtension,
    read_client_extension_settings,
)
from fastmcp.server.providers import Provider
from fastmcp.tools import InputRequiredToolResult, Tool, ToolResult
from fastmcp.utilities.tasks import TASKS_EXTENSION_ID, TaskConfig
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
from pydantic import PrivateAttr

from clio_relay import __version__, door_error_adapters, door_errors
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import NotFoundError, QueueConflictError, TaskInputParkConflictError
from clio_relay.jarvis_mcp import is_virtual_jarvis_tool
from clio_relay.mcp_server import (
    McpSessionState,
    call_mcp_tool,
    mcp_tool_definitions_and_remote_catalog,
    mcp_tool_result_failed,
    normalize_mcp_profile,
    serialize_mcp_tool_result,
    static_mcp_tool_names,
    status_mcp_job,
    wait_mcp_job,
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
    from fastmcp.server.extensions import ToolCallContinuation, ToolCallOutcome

logger = logging.getLogger(__name__)

JSON = dict[str, Any]
SESSION_STATE_KEY = "clio-relay/mcp-session-state"
TASK_POLL_INTERVAL_MS = 1_000
TASK_TTL_MS = 30 * 24 * 60 * 60 * 1_000
MAX_TASK_ARGUMENT_BYTES = MAX_MCP_TASK_ARGUMENT_BYTES
_TASK_METHOD_VERSIONS = frozenset(MODERN_PROTOCOL_VERSIONS)
_AGENT_TASK_TOOL_NAMES = frozenset({"relay_submit_agent", "relay_submit_remote_agent"})
_AGENT_INPUT_KEY = "agent_message"
_AGENT_INPUT_REQUEST_STATE_SCHEMA = "clio-relay.agent-message-input.v1"
_MAX_AGENT_INPUT_MESSAGE_BYTES = 128 * 1_024

type McpTaskStatus = Literal[
    "working",
    "input_required",
    "completed",
    "failed",
    "cancelled",
]
type RelayStateMapRow = tuple[tuple[str, ...], McpTaskStatus, bool | None]

# Cross-repo federation contract: keep this table identical to clio-agent's
# RELAY_STATE_MAP. The source rationale is docs/mcp-tasks.md:99-114.
RELAY_STATE_MAP: Final[tuple[RelayStateMapRow, ...]] = (
    (("queued", "leased", "running"), "working", None),
    (("durable_input_round",), "input_required", None),
    (("succeeded",), "completed", False),
    (("tool_failure",), "completed", True),
    (("protocol_error",), "failed", None),
    (("canceled",), "cancelled", None),
)
_RELAY_STATE_PROJECTIONS: Final[dict[str, tuple[McpTaskStatus, bool | None]]] = {
    observation: (status, is_error)
    for observations, status, is_error in RELAY_STATE_MAP
    for observation in observations
}


def _relay_state_projection(observation: str) -> tuple[McpTaskStatus, bool | None]:
    """Return the committed MCP task projection for one relay observation."""
    try:
        return _RELAY_STATE_PROJECTIONS[observation]
    except KeyError as exc:
        raise ValueError(f"relay observation has no MCP task projection: {observation}") from exc


def _agent_input_request_state(task_id: str) -> str:
    """Bind one post-admission agent input round to its durable relay task."""
    return json.dumps(
        {
            "schema_version": _AGENT_INPUT_REQUEST_STATE_SCHEMA,
            "task_id": task_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _post_admission_agent_input_guard(
    ctx: Context,
    *,
    task_id: str,
) -> mcp_types.InputRequiredResult | mcp_types.ElicitResult:
    """Ask for one follow-up agent message, then consume it on guarded re-entry."""
    responses = ctx.input_responses
    if responses is None:
        return mcp_types.InputRequiredResult(
            input_requests={
                _AGENT_INPUT_KEY: mcp_types.ElicitRequest(
                    params=mcp_types.ElicitRequestFormParams(
                        message="Send a follow-up message to the running remote agent.",
                        requested_schema={
                            "type": "object",
                            "properties": {
                                "message": {
                                    "type": "string",
                                    "minLength": 1,
                                    "maxLength": 32_768,
                                }
                            },
                            "required": ["message"],
                            "additionalProperties": False,
                        },
                    )
                )
            },
            request_state=_agent_input_request_state(task_id),
        )
    if ctx.request_state != _agent_input_request_state(task_id):
        raise ValueError("post-admission agent input lost its durable request state")
    raw_response = responses.get(_AGENT_INPUT_KEY)
    if raw_response is None:
        raise ValueError("post-admission agent input omitted its requested response")
    response = (
        raw_response
        if isinstance(raw_response, mcp_types.ElicitResult)
        else mcp_types.ElicitResult.model_validate(raw_response)
    )
    if response.action == "accept":
        content = response.content
        if not isinstance(content, dict):
            raise ValueError("accepted post-admission agent input omitted its content")
        message = content.get("message")
        if not isinstance(message, str) or not message:
            raise ValueError("accepted post-admission agent input omitted its message")
        if len(message.encode("utf-8")) > _MAX_AGENT_INPUT_MESSAGE_BYTES:
            raise ValueError("post-admission agent input message exceeds its byte limit")
    return response


def _requests_document(result: mcp_types.InputRequiredResult) -> dict[str, JSON]:
    """Serialize one bounded guard result for the durable task projection."""
    requests = result.input_requests or {}
    return {
        key: request.model_dump(
            by_alias=True,
            mode="json",
            exclude_none=True,
        )
        for key, request in requests.items()
    }


def _agent_input_enabled(tool_name: str, arguments: JSON) -> bool:
    """Return whether an agent task explicitly opted into one follow-up round."""
    return tool_name in _AGENT_TASK_TOOL_NAMES and arguments.get("request_followup_message") is True


def _session_to_json(session: McpSessionState) -> JSON:
    """Serialize the compatibility session cache into FastMCP session state."""
    return {
        "remote_mcp_catalog_revisions": dict(session.remote_mcp_catalog_revisions),
        "remote_job_routes": {
            job_id: [list(route) for route in sorted(routes)]
            for job_id, routes in session.remote_job_routes.items()
        },
    }


def _session_from_json(value: object) -> McpSessionState:
    """Restore a validated compatibility session cache."""
    session = McpSessionState()
    if value is None:
        return session
    if not isinstance(value, dict):
        raise ValueError("stored MCP session state is not an object")
    document = cast(JSON, value)
    revisions_value = document.get("remote_mcp_catalog_revisions", {})
    routes_value = document.get("remote_job_routes", {})
    if not isinstance(revisions_value, dict) or not isinstance(routes_value, dict):
        raise ValueError("stored MCP session state contains invalid collections")
    revisions = cast(dict[str, object], revisions_value)
    routes = cast(dict[str, object], routes_value)
    session.remote_mcp_catalog_revisions = {
        str(key): str(revision) for key, revision in revisions.items()
    }
    restored_routes: dict[str, set[tuple[str, str]]] = {}
    for job_id, raw_routes in routes.items():
        if not isinstance(raw_routes, list):
            raise ValueError("stored MCP job routes are not a list")
        route_items = cast(list[object], raw_routes)
        parsed_routes: set[tuple[str, str]] = set()
        for route in route_items:
            if not isinstance(route, list):
                raise ValueError("stored MCP job route is malformed")
            route_parts = cast(list[object], route)
            if len(route_parts) != 2:
                raise ValueError("stored MCP job route is malformed")
            parsed_routes.add((str(route_parts[0]), str(route_parts[1])))
        restored_routes[str(job_id)] = parsed_routes
        if len(restored_routes[str(job_id)]) != len(route_items):
            raise ValueError("stored MCP job route is malformed")
    session.remote_job_routes = restored_routes
    return session


async def _load_session() -> McpSessionState:
    context = get_context()
    return _session_from_json(await context.get_state(SESSION_STATE_KEY))


async def _save_session(session: McpSessionState) -> None:
    context = get_context()
    await context.set_state(SESSION_STATE_KEY, _session_to_json(session))


class RelayBearerTokenVerifier(TokenVerifier):
    """Constant-time verifier for the existing clio-relay API bearer token."""

    def __init__(self, token: str, *, base_url: str) -> None:
        super().__init__(base_url=base_url, resource_base_url=base_url)
        self._token = token

    async def verify_token(self, token: str) -> AccessToken | None:
        """Accept only the configured relay API token."""
        if not hmac.compare_digest(token, self._token):
            return None
        return AccessToken(
            token=token,
            client_id="clio-relay-mcp",
            scopes=[],
            subject="clio-relay",
        )


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
        session = await _load_session()
        raw = await asyncio.to_thread(
            lambda: call_mcp_tool(
                {"name": name, "arguments": arguments},
                queue=self.queue,
                settings=self.settings,
                profile=self.profile,
                session=session,
                observed_remote_mcp_catalog_revision=catalog_revision,
                require_advertised_remote_mcp_catalog=catalog_revision is not None,
            )
        )
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
        route_arguments = self._route_arguments(record)
        waited = await asyncio.to_thread(
            lambda: wait_mcp_job(
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
                lambda: status_mcp_job(
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
        from clio_relay.queue_management import cancel_queue_job

        initial = record.projection.initial_result
        if initial.get("remote") is True:
            await asyncio.to_thread(
                lambda: call_mcp_tool(
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


class RelayTool(Tool):
    """FastMCP component delegating to one existing relay MCP tool definition."""

    _runtime: RelayMcpRuntime = PrivateAttr()
    _catalog_revision: str | None = PrivateAttr(default=None)

    def __init__(
        self,
        definition: JSON,
        *,
        runtime: RelayMcpRuntime,
        catalog_revision: str | None,
        task_capable: bool,
    ) -> None:
        meta = dict(cast(dict[str, Any], definition.get("_meta") or {}))
        if catalog_revision is not None:
            meta["clio-relay/catalog-revision"] = catalog_revision
        raw_annotations = definition.get("annotations")
        annotations = (
            None
            if raw_annotations is None
            else mcp_types.ToolAnnotations.model_validate(raw_annotations)
        )
        super().__init__(
            name=cast(str, definition["name"]),
            title=cast(str | None, definition.get("title")),
            description=cast(str | None, definition.get("description")),
            parameters=cast(JSON, definition["inputSchema"]),
            output_schema=cast(JSON | None, definition.get("outputSchema")),
            annotations=annotations,
            meta=meta or None,
            task_config=TaskConfig(
                mode="optional" if task_capable else "forbidden",
                poll_interval=timedelta(milliseconds=TASK_POLL_INTERVAL_MS),
            ),
        )
        self._runtime = runtime
        self._catalog_revision = catalog_revision

    @property
    def catalog_revision(self) -> str | None:
        """Return the exact dynamic catalog revision bound at dispatch."""
        return self._catalog_revision

    async def run(self, arguments: dict[str, Any]) -> ToolResult:
        """Execute the established relay dispatcher without a second tool runtime."""
        return await self._runtime.call_tool(
            name=self.name,
            arguments=arguments,
            catalog_revision=self._catalog_revision,
        )


class RelayToolProvider(Provider):
    """Expose the complete static and dynamic relay catalog through FastMCP."""

    def __init__(self, runtime: RelayMcpRuntime) -> None:
        super().__init__()
        self._runtime = runtime

    async def _definitions(self) -> tuple[list[JSON], str]:
        return await asyncio.to_thread(lambda: _definitions_with_revision(self._runtime.profile))

    async def _list_tools(self) -> Sequence[Tool]:
        definitions, revision = await self._definitions()
        session = await _load_session()
        session.observe_remote_mcp_catalog(
            profile=self._runtime.profile,
            revision=revision,
        )
        await _save_session(session)
        static_names = static_mcp_tool_names()
        return [
            RelayTool(
                definition,
                runtime=self._runtime,
                catalog_revision=(
                    None if cast(str, definition["name"]) in static_names else revision
                ),
                task_capable=_task_capable_tool_name(
                    cast(str, definition["name"]),
                    static_names,
                ),
            )
            for definition in definitions
        ]

    async def _get_tool(
        self,
        name: str,
        version: VersionSpec | None = None,
    ) -> Tool | None:
        if version is not None:
            return None
        definitions, revision = await self._definitions()
        static_names = static_mcp_tool_names()
        for definition in definitions:
            if definition.get("name") != name:
                continue
            return RelayTool(
                definition,
                runtime=self._runtime,
                catalog_revision=None if name in static_names else revision,
                task_capable=_task_capable_tool_name(name, static_names),
            )
        return None


def _task_capable_tool_name(name: str, static_names: set[str]) -> bool:
    """Return whether one admitted-job tool supports SEP-2663 task projection."""
    return (
        name not in static_names
        or name == "relay_call_jarvis_mcp"
        or name in _AGENT_TASK_TOOL_NAMES
        or is_virtual_jarvis_tool(name)
    )


def _definitions_with_revision(profile: str) -> tuple[list[JSON], str]:
    definitions, catalog = mcp_tool_definitions_and_remote_catalog(profile=profile)
    return definitions, catalog.revision


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
        except QueueConflictError as exc:
            # relay#218, grounding now owned by
            # door_errors.REASONS["mcp_task_conflict"]'s docstring.
            # Call-path-scoped: a bare QueueConflictError means this only on
            # this MCP-task-creation path (it is raised 651 other times
            # elsewhere in core_queue.py for unrelated invariants), so the
            # reason is supplied here rather than derived from the type alone.
            conflicting_task_id = (
                outcome.structured_content.get("job_id")
                if isinstance(outcome.structured_content, dict)
                else None
            )
            raise door_error_adapters.as_mcp_error(
                door_errors.classify(
                    door_errors.public_message_error(exc),
                    reason="mcp_task_conflict",
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


def create_fastmcp_server(
    *,
    settings: RelaySettings | None = None,
    profile: str = "user",
    queue: ClioCoreQueue | None = None,
    http_base_url: str | None = None,
) -> FastMCP[dict[str, Any]]:
    """Create the native FastMCP relay server without a second task backend."""
    resolved = settings or RelaySettings.from_env()
    runtime = RelayMcpRuntime(settings=resolved, profile=profile, queue=queue)
    auth: AuthProvider | None = None
    if http_base_url is not None:
        if resolved.api_token is None:
            raise ValueError("CLIO_RELAY_API_TOKEN is required for MCP HTTP transport")
        auth = RelayBearerTokenVerifier(resolved.api_token, base_url=http_base_url)
    server: FastMCP[dict[str, Any]] = FastMCP(
        "clio-relay",
        version=__version__,
        instructions=(
            "Durable relay operations. Long-running virtual remote and JARVIS tools "
            "may be returned through io.modelcontextprotocol/tasks. Relay jobs remain "
            "the sole execution and cancellation authority."
        ),
        providers=[RelayToolProvider(runtime)],
        lifespan=runtime.lifespan,
        auth=auth,
        tasks=False,
        strict_input_validation=True,
    )
    server.add_extension(RelayTasksExtension(runtime))
    return server


def run_fastmcp_stdio(
    *,
    settings: RelaySettings | None = None,
    profile: str = "user",
) -> None:
    """Run the native FastMCP server over stdio."""
    create_fastmcp_server(settings=settings, profile=profile).run(
        transport="stdio",
        show_banner=False,
    )


def run_fastmcp_http(
    *,
    settings: RelaySettings | None = None,
    profile: str = "user",
    host: str = "127.0.0.1",
    port: int = 8766,
    path: str = "/mcp",
) -> None:
    """Run authenticated Streamable HTTP with the existing relay API token."""
    normalized_path = "/" + path.strip("/")
    public_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    base_url = f"http://{public_host}:{port}{normalized_path}"
    create_fastmcp_server(
        settings=settings,
        profile=profile,
        http_base_url=base_url,
    ).run(
        transport="http",
        host=host,
        port=port,
        path=normalized_path,
        show_banner=False,
    )
