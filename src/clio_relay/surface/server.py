"""Relay's MCP server: five verbs over a standard MCP server (RL1, RL2).

Any MCP client can use it. On protocol ``2026-07-28`` a client that opts into
``io.modelcontextprotocol/tasks`` gets a task for ``run``; asks then arrive as
``input_required`` and are answered with ``tasks/update``. Without tasks,
``run`` waits a bounded time and returns the operation's handle, which
``describe(job=...)`` and ``run(resume=...)`` pick up (RL2a). Asks go out as
multi-round-trip input requests on a modern connection, as elicitation on a
legacy one, and otherwise as a typed refusal that names the ask and its page.
"""

from __future__ import annotations

import asyncio
import hmac
import json
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Annotated, Any, Literal, cast

import mcp_types
from fastmcp import Context, FastMCP
from fastmcp.server.auth import AccessToken, AuthProvider, TokenVerifier
from fastmcp.server.extensions import (
    MethodBinding,
    ServerExtension,
    read_client_extension_settings,
)
from fastmcp.tools import FunctionTool, InputRequiredToolResult, ToolResult
from fastmcp.utilities.tasks import TASKS_EXTENSION_ID, TaskConfig
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
from mcp_types.version import MODERN_PROTOCOL_VERSIONS
from pydantic import Field

from clio_relay import shutdown as shutdown_ops
from clio_relay.adapters.hooks import Ask, MountsPages, Transport
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, ServiceSpec
from clio_relay.surface import instructions
from clio_relay.surface.a2ui import install as install_compute_tab
from clio_relay.surface.changes import ServerChanges
from clio_relay.surface.elicit import (
    ask_mode,
    elicit_document,
    elicit_request,
    elicitation_modes,
    modern,
    refusal_result,
    response_parts,
    tasks_opted_in,
)
from clio_relay.surface.endpoints import endpoint_summary, service_message, service_ready
from clio_relay.surface.exec_plan import EXEC_PLAN_KIND, ExecPlan
from clio_relay.surface.files_approval import NeedsPerson
from clio_relay.surface.files_verb import MAX_INLINE_BYTES, FilesArgs, files_tool
from clio_relay.surface.http_limits import allow_chunk_bodies
from clio_relay.surface.progress import latest_note, observation_line
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from clio_relay.surface.skills import SkillResources
from clio_relay.wording import with_article

PROTOCOL_VERSION = "2026-07-28"
TASK_TTL_MS = 7 * 24 * 3600 * 1000
TASK_POLL_INTERVAL_MS = 2000
OPERATION_META = "ai.iowarp.relay/operation"
SERVICE_META = "ai.iowarp.relay/service"
TASK_TOOLS = frozenset({"run", "serve"})
_NOT_A_START = ("cancel", "resume", "service", "stop")

_TASK_VERSIONS = frozenset(MODERN_PROTOCOL_VERSIONS)


def _text(value: dict[str, Any]) -> str:
    return json.dumps(value, indent=2, sort_keys=True, default=str)


def _plan_needs_person(service: RelayService, ask: Ask) -> ToolResult:
    refusal = Refusal(
        RefusalReason.NEEDS_APPROVAL,
        "the site is guarded, so relay asks the person before it runs a command that is not "
        "read-only. Open the page, or run `clio-relay asks answer`, then call exec again "
        "with the same command and plan",
        ask_id=ask.ask_id,
        asks=[
            {
                "ask_id": ask.ask_id,
                "kind": ask.kind,
                "message": ask.message,
                "page": service.ask_provider.page_address(ask),
            }
        ],
    )
    return refusal_result(refusal)


def _summary(view: dict[str, Any]) -> str:
    line = f"Operation {view['operation_id']} on {view['site']} is {view['state']}"
    if view.get("exit_code") is not None:
        line += f" with exit code {view['exit_code']}"
    line += "."
    result: dict[str, Any] = view.get("result") or {}
    if not OperationState(view["state"]).final:
        seen = observation_line(view)
        if seen:
            line += f"\n{seen}."
    endpoint: dict[str, Any] = result.get("endpoint") or {}
    ports: list[dict[str, Any]] = endpoint.get("ports") or []
    for port in ports:
        line += f"\n{port['name']}: {port['host']}:{port['port']}"
    tail: str | None = result.get("stdout_tail")
    if tail:
        line += f"\n\nstdout (tail):\n{tail}"
    if view.get("error"):
        line += f"\n\n{view['error'].get('reason')}: {view['error'].get('message')}"
    return line


def operation_result(view: dict[str, Any]) -> ToolResult:
    """Render an operation. A failed job is an error result; a cancelled one is not."""
    return ToolResult(
        content=[mcp_types.TextContent(type="text", text=_summary(view))],
        structured_content=view,
        meta={OPERATION_META: view["operation_id"]},
        is_error=view["state"] == OperationState.FAILED.value,
    )


def record_result(service: RelayService, record: OperationRecord) -> ToolResult:
    """Render an operation, with a service's panel data in ``_meta`` only (RL22)."""
    result = operation_result(service.view(record))
    if record.kind == "serve":
        ui = service.serve_hook.ui_data(record)
        if ui is not None:
            result.meta = {**(result.meta or {}), SERVICE_META: ui}
    return result


class RelayTasksExtension(ServerExtension):
    """``io.modelcontextprotocol/tasks`` over relay's durable operations. Task id is the op id."""

    identifier = TASKS_EXTENSION_ID

    def __init__(self, service: RelayService) -> None:
        self.service = service

    def settings(self) -> dict[str, Any]:
        return {}

    def methods(self) -> Sequence[MethodBinding]:
        return (
            MethodBinding(
                method="tasks/get",
                params_type=GetTaskParams,
                handler=self._get,
                protocol_versions=_TASK_VERSIONS,
            ),
            MethodBinding(
                method="tasks/update",
                params_type=UpdateTaskParams,
                handler=self._update,
                protocol_versions=_TASK_VERSIONS,
            ),
            MethodBinding(
                method="tasks/cancel",
                params_type=CancelTaskParams,
                handler=self._cancel,
                protocol_versions=_TASK_VERSIONS,
            ),
        )

    @asynccontextmanager
    async def lifespan(self) -> AsyncGenerator[None]:
        wire_production.install()
        try:
            yield
        finally:
            wire_production.uninstall()

    @staticmethod
    def _require(ctx: ServerRequestContext[Any, Any]) -> None:
        if read_client_extension_settings(ctx, TASKS_EXTENSION_ID) is None:
            raise MCPError(
                code=MISSING_REQUIRED_CLIENT_CAPABILITY,
                message="This request needs the io.modelcontextprotocol/tasks extension.",
                data=missing_capability_error_data(),
            )

    def _record(self, task_id: str) -> OperationRecord:
        record = self.service.operations.get(task_id)
        if record is None or not record.task:
            raise MCPError(code=-32602, message=f"no task {task_id!r}")
        return record

    def _status(self, record: OperationRecord) -> str:
        if record.state == OperationState.AWAITING_INPUT and self.service.outstanding(record):
            return "input_required"
        if record.state == OperationState.CANCELLED:
            return "cancelled"
        if record.state.final:
            return "completed"
        return "working"

    def _message(self, record: OperationRecord) -> str:
        if record.state == OperationState.AWAITING_INPUT:
            if self.service.outstanding(record):
                return "waiting for the person to answer"
            return "waiting for a sign-in to finish"
        if service_ready(record):
            return f"the service is running: {endpoint_summary(record)}"
        latest = service_message(record)
        if latest is not None:
            shown = record.state.value if record.state.final else "starting"
            return f"the service is {shown}: {latest}"
        note = latest_note(record)
        return f"relay operation is {record.state.value}" + (f": {note}" if note else "")

    def _fields(self, record: OperationRecord) -> dict[str, Any]:
        return {
            "task_id": record.operation_id,
            "status": self._status(record),
            "status_message": self._message(record),
            "created_at": record.created_at.isoformat(),
            "last_updated_at": record.updated_at.isoformat(),
            "ttl_ms": TASK_TTL_MS,
            "poll_interval_ms": TASK_POLL_INTERVAL_MS,
        }

    def create_result(self, record: OperationRecord) -> CreateTaskResult:
        """Return the task a ``run`` became."""
        return CreateTaskResult(**self._fields(record))

    async def task_result(self, record: OperationRecord) -> GetTaskResult:
        """Project an operation onto a task."""
        fields = self._fields(record)
        if fields["status"] == "input_required":
            return GetTaskResult(
                **fields,
                input_requests={
                    ask.ask_id: elicit_document(ask) for ask in self.service.outstanding(record)
                },
            )
        if record.state.final:
            result = record_result(self.service, record)
            final = mcp_types.CallToolResult.model_validate(
                {
                    "content": result.content,
                    "structuredContent": result.structured_content,
                    "isError": result.is_error,
                    "_meta": result.meta,
                }
            )
            return GetTaskResult(
                **fields, result=final.model_dump(by_alias=True, mode="json", exclude_none=True)
            )
        return GetTaskResult(**fields)

    async def _get(
        self, ctx: ServerRequestContext[Any, Any], params: GetTaskParams
    ) -> GetTaskResult:
        self._require(ctx)
        self._record(params.task_id)
        try:
            record = await self.service.refresh(params.task_id)
        except Refusal as refusal:
            raise MCPError(
                code=-32603, message=refusal.message, data=refusal.to_dict()
            ) from refusal
        return await self.task_result(record)

    async def _update(
        self, ctx: ServerRequestContext[Any, Any], params: UpdateTaskParams
    ) -> UpdateTaskResult:
        self._require(ctx)
        record = self._record(params.task_id)
        for ask_id, response in params.input_responses.items():
            if ask_id not in record.asks:
                continue
            action, content = response_parts(response)
            self.service.answer(ask_id, action, content, by="tasks/update")
        await self.service.advance(params.task_id)
        return UpdateTaskResult()

    async def _cancel(
        self, ctx: ServerRequestContext[Any, Any], params: CancelTaskParams
    ) -> CancelTaskResult:
        self._require(ctx)
        self._record(params.task_id)
        await self.service.cancel(params.task_id)
        return CancelTaskResult()

    async def intercept_tool_call(
        self,
        params: mcp_types.CallToolRequestParams,
        context: Context,
        call_next: Any,
    ) -> Any:
        """Turn a task ``run`` or ``serve`` that starts an operation into a task."""
        outcome = await call_next()
        if params.name not in TASK_TOOLS or not tasks_opted_in(context):
            return outcome
        arguments = params.arguments or {}
        if any(arguments.get(name) is not None for name in _NOT_A_START):
            return outcome
        if not isinstance(outcome, ToolResult) or isinstance(outcome, InputRequiredToolResult):
            return outcome
        operation_id = (outcome.meta or {}).get(OPERATION_META)
        if not isinstance(operation_id, str):
            return outcome
        record = self.service.operations.get(operation_id)
        if record is None or not record.task:
            return outcome
        return self.create_result(record)


class RelayBearer(TokenVerifier):
    """Accept one bearer token, compared in constant time."""

    def __init__(self, token: str, *, base_url: str) -> None:
        super().__init__(base_url=base_url, resource_base_url=base_url)
        self._token = token

    async def verify_token(self, token: str) -> AccessToken | None:
        if not hmac.compare_digest(token, self._token):
            return None
        return AccessToken(token=token, client_id="clio-relay", scopes=[], subject="clio-relay")


class RelayServer:
    """Builds the FastMCP server around one :class:`RelayService`."""

    def __init__(self, service: RelayService) -> None:
        self.service = service
        self.tasks = RelayTasksExtension(service)
        self.changes = ServerChanges()
        self.skills = SkillResources(service.paths.config_dir, service.sites)

    async def _ask_inline(self, ctx: Context, record: OperationRecord) -> OperationRecord:
        modes = elicitation_modes(ctx)
        for ask in self.service.outstanding(record):
            if ask_mode(ask) not in modes:
                return record
            if ask.kind == "url":
                response = await ctx.session.elicit_url(
                    message=ask.message, url=ask.url or "", elicitation_id=ask.ask_id
                )
            else:
                response = await ctx.session.elicit_form(
                    message=ask.message,
                    requested_schema=ask.requested_schema or {"type": "object", "properties": {}},  # pyright: ignore[reportArgumentType]
                )
            action, content = response_parts(response)
            self.service.answer(ask.ask_id, action, content, by="elicitation")
        return await self.service.advance(record.operation_id)

    def _needs_person(self, record: OperationRecord) -> ToolResult:
        asks = self.service.operation_asks(record)
        pending = [ask for ask in asks if ask.state == "pending"] or asks
        url_only = all(ask.kind == "url" for ask in pending)
        shown: list[dict[str, Any]] = []
        for ask in pending:
            shown.append(
                {
                    "ask_id": ask.ask_id,
                    "kind": ask.kind,
                    "message": ask.message,
                    "page": self.service.ask_provider.page_address(ask),
                }
            )
        refusal = Refusal(
            RefusalReason.NEEDS_SIGN_IN if url_only else RefusalReason.NEEDS_APPROVAL,
            "relay needs the person before it can go on. Open the page, or run "
            "`clio-relay asks answer`, then call run with resume set to the operation id",
            operation_id=record.operation_id,
            asks=shown,
        )
        return refusal_result(refusal)

    async def _deliver(
        self, ctx: Context, record: OperationRecord, wait_s: float
    ) -> ToolResult | InputRequiredToolResult:
        if record.state == OperationState.AWAITING_INPUT:
            outstanding = self.service.outstanding(record)
            if outstanding and modern(ctx):
                modes = elicitation_modes(ctx)
                if all(ask_mode(ask) in modes for ask in outstanding):
                    return InputRequiredToolResult(
                        mcp_types.InputRequiredResult(
                            input_requests={ask.ask_id: elicit_request(ask) for ask in outstanding},
                            request_state=record.operation_id,
                        )
                    )
            elif outstanding:
                record = await self._ask_inline(ctx, record)
        record = await self.service.wait(record.operation_id, wait_s)
        if record.state == OperationState.AWAITING_INPUT:
            return self._needs_person(record)
        return record_result(self.service, record)

    async def run(
        self,
        ctx: Context,
        site: str | None,
        spec: JobSpec | None,
        cancel: str | None,
        resume: str | None,
        wait_s: float,
    ) -> ToolResult | InputRequiredToolResult:
        """The ``run`` verb."""
        try:
            if cancel is not None:
                record = await self.service.cancel(cancel)
                return record_result(self.service, record)
            if ctx.request_state and ctx.input_responses is not None:
                return await self._resume_round(ctx, site, spec, wait_s)
            if resume is not None:
                self.service.same_request(self.service.operation(resume), site, spec)
                record = await self.service.advance(resume)
                return await self._deliver(ctx, record, wait_s)
            if site is None or spec is None:
                raise Refusal(
                    RefusalReason.INVALID_ARGUMENTS,
                    "run needs site and spec, or cancel, or resume",
                )
            task = tasks_opted_in(ctx)
            record = await self.service.start_run(site, spec, task=task)
            if task:
                return operation_result(self.service.view(record))
            return await self._deliver(ctx, record, wait_s)
        except Refusal as refusal:
            return refusal_result(refusal)

    async def serve(
        self,
        ctx: Context,
        site: str | None,
        spec: ServiceSpec | None,
        service: str | None,
        stop: str | None,
        wait_s: float,
    ) -> ToolResult | InputRequiredToolResult:
        """The ``serve`` verb: start, observe or stop a service."""
        try:
            if stop is not None:
                record = await self.service.cancel(self._service_op(stop).operation_id)
                return record_result(self.service, record)
            if ctx.request_state and ctx.input_responses is not None:
                return await self._resume_round(ctx, site, spec, wait_s)
            if service is not None:
                record = await self.service.advance(self._service_op(service).operation_id)
                return await self._deliver(ctx, record, wait_s)
            if site is None or spec is None:
                raise Refusal(
                    RefusalReason.INVALID_ARGUMENTS,
                    "serve needs site and spec, or service, or stop",
                )
            task = tasks_opted_in(ctx)
            record = await self.service.start_serve(site, spec, task=task)
            if task:
                return record_result(self.service, record)
            return await self._deliver(ctx, record, wait_s)
        except Refusal as refusal:
            return refusal_result(refusal)

    def _service_op(self, operation_id: str) -> OperationRecord:
        record = self.service.operation(operation_id)
        if record.kind != "serve":
            raise Refusal(
                RefusalReason.INVALID_ARGUMENTS,
                f"{operation_id!r} is {with_article(record.kind)} operation, not a service",
                operation_id=operation_id,
            )
        return record

    async def _resume_round(
        self,
        ctx: Context,
        site: str | None,
        spec: JobSpec | ServiceSpec | None,
        wait_s: float,
    ) -> ToolResult | InputRequiredToolResult:
        operation_id = ctx.request_state or ""
        record = self.service.operation(operation_id)
        self.service.same_request(record, site, spec)
        for ask_id, response in (ctx.input_responses or {}).items():
            if ask_id in record.asks:
                action, content = response_parts(response)
                self.service.answer(ask_id, action, content, by="input_required")
        record = await self.service.advance(operation_id)
        return await self._deliver(ctx, record, wait_s)

    async def exec(
        self,
        ctx: Context,
        site: str,
        command: list[str],
        timeout_s: float | None,
        max_output_bytes: int,
        plan: ExecPlan | None = None,
    ) -> ToolResult | InputRequiredToolResult:
        """The ``exec`` verb. With no ``timeout_s`` it runs until the command ends,
        and tells the person when it goes quiet; cancelling the call stops it.

        On a guarded site a ``plan`` asks the person before a command that is
        not read-only: in the call when the client shows a form, else as a
        ``needs_approval`` refusal to retry after the answer."""

        async def quiet(idle_s: float, next_s: float) -> None:
            note = (
                f"no output for {idle_s:.0f} s; the command is still running; relay checks "
                f"again in {next_s:.0f} s; cancel the call to stop it"
            )
            await ctx.report_progress(idle_s, message=note)

        async def call() -> dict[str, Any]:
            return await self.service.exec(
                site,
                command,
                timeout_s=timeout_s,
                max_output_bytes=max_output_bytes,
                on_stall=quiet,
                plan=plan,
            )

        try:
            if ctx.request_state and ctx.input_responses is not None:
                self._answer_plan(site, ctx.request_state, ctx.input_responses)
            try:
                result = await call()
            except NeedsPerson as waiting:
                ask = waiting.ask
                if "form" not in elicitation_modes(ctx):
                    return _plan_needs_person(self.service, ask)
                if modern(ctx):
                    return InputRequiredToolResult(
                        mcp_types.InputRequiredResult(
                            input_requests={ask.ask_id: elicit_request(ask)},
                            request_state=ask.ask_id,
                        )
                    )
                response = await ctx.session.elicit_form(
                    message=ask.message,
                    requested_schema=ask.requested_schema or {"type": "object", "properties": {}},  # pyright: ignore[reportArgumentType]
                )
                action, content = response_parts(response)
                self.service.answer(ask.ask_id, action, content, by="elicitation")
                try:
                    result = await call()
                except NeedsPerson as still:
                    return _plan_needs_person(self.service, still.ask)
        except Refusal as refusal:
            return refusal_result(refusal)
        text = result["stdout"]
        if result["stderr"]:
            text += f"\n[stderr]\n{result['stderr']}"
        text += f"\n[exit {result['exit_code']}{', timed out' if result['timed_out'] else ''}]"
        return ToolResult(
            content=[mcp_types.TextContent(type="text", text=text)],
            structured_content=result,
            is_error=result["exit_code"] != 0,
        )

    def _answer_plan(self, site: str, ask_id: str, responses: dict[str, Any]) -> None:
        """Record the answer a retried exec call carries, for this site's plan asks only."""
        ask = self.service.asks.get(ask_id)
        if ask is None or ask_id not in responses or ask.site != site:
            return
        if ask.binding.get("kind") != EXEC_PLAN_KIND:
            return
        action, content = response_parts(responses[ask_id])
        self.service.answer(ask_id, action, content, by="input_required")

    async def files(
        self, ctx: Context, site: str, args: FilesArgs
    ) -> ToolResult | InputRequiredToolResult:
        """The ``files`` verb."""
        return await files_tool(self.service, ctx, site, args)

    def build(self, *, auth: AuthProvider | None = None) -> FastMCP[Any]:
        """Return the FastMCP server."""
        server: FastMCP[Any] = FastMCP(
            "clio-relay",
            instructions=instructions.current(self.service),
            auth=auth,
            tasks=False,
            strict_input_validation=True,
            lifespan=self._lifespan,
        )
        relay = self

        @server.tool(
            name="describe",
            title="Describe relay, a site, or an operation",
            annotations=mcp_types.ToolAnnotations(read_only_hint=True, open_world_hint=True),
        )
        async def describe(  # pyright: ignore[reportUnusedFunction]
            site: Annotated[str | None, Field(description="A site id")] = None,
            job: Annotated[str | None, Field(description="An operation id")] = None,
        ) -> ToolResult:
            """Without arguments, list the sites. With site, describe it, its routes and
            the JARVIS packages relay can run there (packages, with their parameters),
            and return its guide (skill.text) when it has one: read it before the first
            run there. With job, observe one operation: its state, when relay last looked
            (result.observed_at), the latest step (result.progress) and the outputs."""
            try:
                described = await relay.service.describe(site, job)
            except Refusal as refusal:
                return refusal_result(refusal)
            if job is not None:
                return operation_result(described)
            content = [mcp_types.TextContent(type="text", text=_text(described))]
            skill = cast("dict[str, Any] | None", described.get("skill")) if site else None
            guide = skill.get("text") if skill else None
            if skill and isinstance(guide, str) and guide:
                listed = {**described, "skill": {**skill, "text": "(below)"}}
                content = [
                    mcp_types.TextContent(type="text", text=_text(listed)),
                    mcp_types.TextContent(type="text", text=guide),
                ]
            return ToolResult(content=content, structured_content=described)

        async def run(
            ctx: Context,
            site: Annotated[str | None, Field(description="The site id")] = None,
            spec: Annotated[
                JobSpec | None, Field(description="The job, as a PSI/J JobSpec")
            ] = None,
            cancel: Annotated[str | None, Field(description="An operation id to cancel")] = None,
            resume: Annotated[
                str | None, Field(description="An operation id waiting on a person")
            ] = None,
            wait_s: Annotated[
                float, Field(ge=0, le=300, description="Seconds to wait without a task")
            ] = 20,
        ) -> ToolResult | InputRequiredToolResult:
            """Submit a job: an executable, or a JARVIS package as package: {name,
            parameters} (relay pins its version and SHA-256). With tasks this returns a
            task; without, it waits up to wait_s and returns the operation, which
            describe(job=...) observes."""
            return await relay.run(ctx, site, spec, cancel, resume, wait_s)

        server.add_tool(_task_optional(run, "run", "Run a job on a site"))

        @server.tool(
            name="exec",
            title="Run one short command on a site",
            annotations=mcp_types.ToolAnnotations(destructive_hint=True, open_world_hint=True),
        )
        async def exec_(  # pyright: ignore[reportUnusedFunction]
            site: Annotated[str, Field(description="The site id")],
            command: Annotated[list[str], Field(min_length=1, description="argv, no shell")],
            ctx: Context,
            timeout_s: Annotated[
                float | None,
                Field(
                    gt=0, description="Optional limit; by default the command runs until it ends"
                ),
            ] = None,
            max_output_bytes: Annotated[int, Field(ge=1, le=MAX_INLINE_BYTES)] = 65536,
            plan: Annotated[
                ExecPlan | None,
                Field(
                    description=(
                        "On a guarded site, what a command that is not read-only is for. "
                        "Relay asks the person once for that exact argv, then runs it lightly"
                    )
                ),
            ] = None,
        ) -> ToolResult | InputRequiredToolResult:
            """Run one command on the site's login side and return bounded output."""
            return await relay.exec(ctx, site, command, timeout_s, max_output_bytes, plan)

        @server.tool(
            name="files",
            title="List, read, write, delete and stage files on a site",
            annotations=mcp_types.ToolAnnotations(destructive_hint=True, open_world_hint=True),
        )
        async def files(  # pyright: ignore[reportUnusedFunction]
            ctx: Context,
            site: Annotated[str, Field(description="The site id")],
            op: Literal["list", "stat", "read", "write", "delete", "stage", "has"],
            path: Annotated[
                str | None,
                Field(description="Absolute, ~/ for home, or relative to relay's work directory"),
            ] = None,
            content: Annotated[str | None, Field(description="Text to write or stage")] = None,
            content_base64: Annotated[
                str | None, Field(description="Bytes to write or stage, base64")
            ] = None,
            offset: Annotated[
                int, Field(ge=0, description="Where a read starts, or where a write chunk goes")
            ] = 0,
            limit: Annotated[int | None, Field(ge=1)] = None,
            overwrite: bool = False,
            final: Annotated[bool, Field(description="False while more chunks follow")] = True,
            recursive: Annotated[bool, Field(description="Delete a non-empty directory")] = False,
            digest: Annotated[str | None, Field(description="A SHA-256, for has")] = None,
        ) -> ToolResult | InputRequiredToolResult:
            """Files up to 4 MiB move inline. A larger upload is chunks of up to 4 MiB:
            write each at its offset with final=false and the last with final=true; after
            a dropped link, stat "<path>.relay-part" gives the offset to resume at. A
            larger download is reads at increasing offsets. stage puts bytes in the
            site's content store and returns the SHA-256 a JobSpec input names. On a
            guarded site, delete and a write that replaces a file wait for the person's yes."""
            return await relay.files(
                ctx,
                site,
                FilesArgs(
                    op=op,
                    path=path,
                    content=content,
                    content_base64=content_base64,
                    offset=offset,
                    limit=limit,
                    overwrite=overwrite,
                    final=final,
                    recursive=recursive,
                    digest=digest,
                ),
            )

        async def serve(
            ctx: Context,
            site: Annotated[str | None, Field(description="The site id")] = None,
            spec: Annotated[
                ServiceSpec | None, Field(description="The service: a job with ports")
            ] = None,
            service: Annotated[
                str | None, Field(description="A service's operation id, to observe it")
            ] = None,
            stop: Annotated[
                str | None, Field(description="A service's operation id, to stop it")
            ] = None,
            wait_s: Annotated[
                float, Field(ge=0, le=300, description="Seconds to wait without a task")
            ] = 20,
        ) -> ToolResult | InputRequiredToolResult:
            """Start a job with ports and return how to reach it. With tasks this returns
            a task whose status names the endpoint; without, it waits up to wait_s for
            the service to be ready. service observes one; stop ends it."""
            return await relay.serve(ctx, site, spec, service, stop, wait_s)

        server.add_tool(_task_optional(serve, "serve", "Start a service on a site"))

        @server.tool(
            name="shutdown",
            title="Stop relay's long-lived processes",
            annotations=mcp_types.ToolAnnotations(destructive_hint=True, open_world_hint=True),
        )
        async def shutdown(  # pyright: ignore[reportUnusedFunction]
            all: Annotated[  # noqa: A002 - the contract names it all
                bool | None, Field(description="True: every site, and relay on this host")
            ] = None,
            site: Annotated[str | None, Field(description="One site id")] = None,
            timeout_s: Annotated[
                float | None,
                Field(gt=0, le=shutdown_ops.MAX_TIMEOUT_S, description="Per site; default 15"),
            ] = None,
        ) -> ToolResult:
            """Stop what relay keeps running for a site, or for all of them: the worker's
            site-serve, login hops, desktop tunnels and connectors, frpc, and a detached
            relay server. Give exactly one of all=true or site. Jobs keep running and are
            listed in still_running; an unreachable site is listed in failures. A host
            calls this when it closes."""
            arguments: dict[str, Any] = {"all": all, "site": site, "timeout_s": timeout_s}
            try:
                stopped = await shutdown_ops.shutdown(relay.service, arguments)
            except Refusal as refusal:
                return refusal_result(refusal)
            return ToolResult(
                content=[mcp_types.TextContent(type="text", text=_text(stopped))],
                structured_content=stopped,
            )

        server.add_extension(self.tasks)
        server.add_provider(self.skills)
        self.changes.install(server)
        self.service.serve_hook.attach(server, self.changes)
        install_compute_tab(server, self.service, self.changes)
        return server

    @asynccontextmanager
    async def _lifespan(self, server: FastMCP[Any]) -> AsyncGenerator[dict[str, Any]]:
        # A client reads the instructions when it connects; give it today's sites.
        server.instructions = await asyncio.to_thread(instructions.current, self.service)
        # A service that ended while relay was down is finished without anyone asking.
        observing = asyncio.create_task(self.service.observe_open_services())
        watching = asyncio.create_task(self.skills.watch(self.changes))
        refreshing = asyncio.create_task(instructions.keep_current(server, self.service))
        try:
            yield {}
        finally:
            observing.cancel()
            watching.cancel()
            refreshing.cancel()
            await asyncio.gather(observing, watching, refreshing, return_exceptions=True)
            self.changes.close()
            await self.service.close()


def _task_optional(fn: Any, name: str, title: str) -> FunctionTool:
    tool = FunctionTool.from_function(
        fn,
        name=name,
        title=title,
        annotations=mcp_types.ToolAnnotations(destructive_hint=False, open_world_hint=True),
        task=TaskConfig(
            mode="optional", poll_interval=timedelta(milliseconds=TASK_POLL_INTERVAL_MS)
        ),
    )
    # Tasks are the 2026-07-28 extension only. A 2025-11-25 client gets the
    # handle path, so its listing must not claim core task support.
    tool.execution = mcp_types.ToolExecution(task_support="forbidden")
    return tool


def create_server(
    paths: RelayPaths | None = None,
    *,
    transport: Transport = "stdio",
    token: str | None = None,
    base_url: str | None = None,
    public_base_url: str | None = None,
) -> FastMCP[Any]:
    """Build relay's MCP server. HTTP needs a bearer token.

    ``public_base_url`` is the origin people reach relay's HTTP server at. The
    ask provider gets it in its settings and, when it serves pages, mounts them.
    """
    service = RelayService(
        paths or RelayPaths.from_env(), transport=transport, public_base_url=public_base_url
    )
    auth: AuthProvider | None = None
    if transport == "http":
        if not token or not base_url:
            raise ValueError("HTTP transport needs a bearer token and a base URL")
        auth = RelayBearer(token, base_url=base_url)
        allow_chunk_bodies()
    server = RelayServer(service).build(auth=auth)
    if transport == "http" and isinstance(service.ask_provider, MountsPages):
        service.ask_provider.mount(server)
    return server
