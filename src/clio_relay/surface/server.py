"""Relay's MCP server: five verbs over a standard MCP server (RL1, RL2).

Any MCP client can use it. On protocol ``2026-07-28`` a client that opts into
``io.modelcontextprotocol/tasks`` gets a task for ``run``, ``serve`` and ``install``, and its
asks arrive as ``input_required``, answered with ``tasks/update``. Without tasks, ``run`` waits
a bounded time and returns the operation's handle, which ``describe(job=...)`` and
``run(resume=...)`` pick up (RL2a). Asks go out as multi-round-trip input requests on a modern
connection, as elicitation on a legacy one, else as a typed refusal naming the ask and its page.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Annotated, Any, Literal

import mcp_types
from fastmcp import Context, FastMCP
from fastmcp.server.auth import AuthProvider
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
from clio_relay.surface import confirm, install_op, instructions, job_card, job_watch, step_wait
from clio_relay.surface.a2ui import install as install_compute_tab
from clio_relay.surface.bearer import RelayBearer
from clio_relay.surface.changes import ServerChanges
from clio_relay.surface.elicit import (
    ask_mode,
    elicit_document,
    elicit_inline,
    elicitation_modes,
    input_required,
    modern,
    refusal_result,
    response_parts,
    tasks_opted_in,
)
from clio_relay.surface.endpoints import (
    endpoint_summary,
    service_done,
    service_message,
    service_ready,
)
from clio_relay.surface.exec_plan import EXEC_PLAN_KIND, ExecPlan
from clio_relay.surface.exec_quiet import QuietReports
from clio_relay.surface.files_tool import install as install_files_tool
from clio_relay.surface.files_verb import MAX_INLINE_BYTES, FilesArgs, files_tool
from clio_relay.surface.http_limits import allow_chunk_bodies
from clio_relay.surface.install_share import advance_holders
from clio_relay.surface.install_tool import install_tool
from clio_relay.surface.progress import latest_note
from clio_relay.surface.read_tools import install as install_read_tools
from clio_relay.surface.results import CHANGE as _CHANGE
from clio_relay.surface.results import OPERATION_META, operation_result, record_result, text
from clio_relay.surface.results import START as _START
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from clio_relay.surface.site_tools import install as install_site_tools
from clio_relay.surface.skills import SkillResources
from clio_relay.surface.staging import PackagesArgs
from clio_relay.surface.staging import packages as packages_op
from clio_relay.wording import with_article

logger = logging.getLogger(__name__)

PROTOCOL_VERSION = "2026-07-28"
TASK_TTL_MS = 7 * 24 * 3600 * 1000
TASK_POLL_INTERVAL_MS = 2000
TASK_TOOLS = frozenset({"run", "serve", "install"})
_NOT_A_START = ("cancel", "resume", "service", "stop")

_TASK_VERSIONS = frozenset(MODERN_PROTOCOL_VERSIONS)
_text = text


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
        if service_done(record):
            return "completed"
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
            shown = record.state.value if record.state.final or service_done(record) else "starting"
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
        """Project an operation onto a task, with its job card in ``_meta``."""
        fields = self._fields(record)
        if (card := job_card.snapshot(self.service, record)) is not None:
            fields["_meta"] = {job_card.JOB_META: card}
        if fields["status"] == "input_required":
            return job_card.JobTaskResult(
                **fields,
                input_requests={
                    ask.ask_id: elicit_document(ask) for ask in self.service.outstanding(record)
                },
            )
        if record.state.final or fields["status"] == "completed":
            result = record_result(self.service, record)
            final = mcp_types.CallToolResult.model_validate(
                {
                    "content": result.content,
                    "structuredContent": result.structured_content,
                    "isError": result.is_error,
                    "_meta": result.meta,
                }
            )
            return job_card.JobTaskResult(
                **fields, result=final.model_dump(by_alias=True, mode="json", exclude_none=True)
            )
        return job_card.JobTaskResult(**fields)

    async def _get(
        self, ctx: ServerRequestContext[Any, Any], params: GetTaskParams
    ) -> GetTaskResult:
        self._require(ctx)
        if step_wait.operation_of(params.task_id) is not None:
            return await step_wait.get(self.service, params.task_id)
        self._record(params.task_id)
        try:
            record = await self.service.refresh(params.task_id)
        except Refusal as refusal:
            raise MCPError(
                code=-32603, message=refusal.message, data=refusal.to_dict()
            ) from refusal
        except Exception:
            # A fault in one look must not end the client's custody of the task:
            # its asks and their answers live on the record, which still stands.
            logger.exception("refreshing task %s", params.task_id)
            record = self._record(params.task_id)
        return await self.task_result(record)

    async def _update(
        self, ctx: ServerRequestContext[Any, Any], params: UpdateTaskParams
    ) -> UpdateTaskResult:
        self._require(ctx)
        record = self._record(params.task_id)
        answered: list[Ask] = []
        for ask_id, response in params.input_responses.items():
            if ask_id not in record.asks:
                continue
            action, content = response_parts(response)
            answered.append(self.service.answer(ask_id, action, content, by="tasks/update"))
        await advance_holders(self.service, params.task_id, answered)
        return UpdateTaskResult()

    async def _cancel(
        self, ctx: ServerRequestContext[Any, Any], params: CancelTaskParams
    ) -> CancelTaskResult:
        self._require(ctx)
        if step_wait.operation_of(params.task_id) is not None:
            return step_wait.cancel(self.service, params.task_id)
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
        if not isinstance(outcome, ToolResult) or isinstance(outcome, InputRequiredToolResult):
            return outcome
        if (waiting := step_wait.intercepted(self.service, arguments, outcome)) is not None:
            return waiting
        if any(arguments.get(name) is not None for name in _NOT_A_START):
            return outcome
        operation_id = (outcome.meta or {}).get(OPERATION_META)
        if not isinstance(operation_id, str):
            return outcome
        record = self.service.operations.get(operation_id)
        if record is None or not record.task:
            return outcome
        return self.create_result(record)


class RelayServer:
    """Builds the FastMCP server around one :class:`RelayService`."""

    def __init__(self, service: RelayService) -> None:
        self.service = service
        self.tasks = RelayTasksExtension(service)
        self.changes = ServerChanges()
        self.skills = SkillResources(service.paths.config_dir, service.sites)

    async def _ask_inline(self, ctx: Context, record: OperationRecord) -> OperationRecord:
        modes = elicitation_modes(ctx)
        asked: set[str] = set()
        for _ in range(3):  # an answer can raise a new question, e.g. JARVIS's plan changed
            answered: list[Ask] = []
            for ask in self.service.outstanding(record):
                if ask_mode(ask) not in modes or ask.ask_id in asked:
                    break
                asked.add(ask.ask_id)
                action, content = response_parts(await elicit_inline(ctx, ask))
                answered.append(self.service.answer(ask.ask_id, action, content, by="elicitation"))
            if not answered:
                return record
            record = await advance_holders(self.service, record.operation_id, answered)
            if record.state != OperationState.AWAITING_INPUT:
                break
        return record

    def _needs_person(self, record: OperationRecord) -> ToolResult:
        pending = [ask for ask in self.service.operation_asks(record) if ask.state == "pending"]
        if not pending:  # answers from earlier rounds are not questions
            return record_result(self.service, record)
        url_only = all(ask.kind == "url" for ask in pending)
        shown = [
            {
                "ask_id": ask.ask_id,
                "kind": ask.kind,
                "message": ask.message,
                "page": self.service.ask_provider.page_address(ask),
            }
            for ask in pending
        ]
        again = (
            "call serve with service set to the operation id"
            if record.kind == "serve"
            else f"call {record.kind} with resume set to the operation id"
        )
        refusal = Refusal(
            RefusalReason.NEEDS_SIGN_IN if url_only else RefusalReason.NEEDS_APPROVAL,
            "relay needs the person before it can go on. Open the page, or run "
            f"`clio-relay asks answer`, then {again}",
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
                        input_required(list(outstanding), record.operation_id)
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
        kind: Literal["run", "install"] = "run",
    ) -> ToolResult | InputRequiredToolResult:
        """The ``run`` verb, and ``install``: the same calls for an install-only operation."""
        flagged = job_card.flagged(ctx)
        try:
            if cancel is not None:
                return await self._cancel_call(ctx, cancel, "cancel", flagged=flagged)
            if ctx.request_state and ctx.input_responses is not None:
                return await self._resume_round(ctx, site, spec, wait_s)
            if resume is not None:
                await self.service.same_request(self.service.operation(resume), site, spec)
                record = await self.service.advance(resume)
                return await self._observed(ctx, record, wait_s, flagged=flagged)
            if site is None or spec is None:
                raise Refusal(
                    RefusalReason.INVALID_ARGUMENTS,
                    f"{kind} needs site and spec, or cancel, or resume",
                )
            task = tasks_opted_in(ctx)
            if kind == "install":
                record = await install_op.start(self.service, site, spec, task=task)
            else:
                record = await self.service.start_run(site, spec, task=task, confirm=flagged)
            if flagged and kind == "run" and "form" not in elicitation_modes(ctx):
                return self._needs_person(record)
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
        until: str | None = None,
    ) -> ToolResult | InputRequiredToolResult:
        """The ``serve`` verb: start, observe or stop a service, or wait for its compute steps."""
        flagged = job_card.flagged(ctx)
        try:
            if stop is not None:
                operation_id = self._service_op(stop).operation_id
                return await self._cancel_call(ctx, operation_id, "stop", flagged=flagged)
            if ctx.request_state and ctx.input_responses is not None:
                return await self._resume_round(ctx, site, spec, wait_s)
            if service is not None and until is not None:
                op = self._service_op(service).operation_id
                return await step_wait.call(self.service, op, tasks_opted_in(ctx), wait_s)
            if service is not None:
                record = await self.service.advance(self._service_op(service).operation_id)
                return await self._observed(ctx, record, wait_s, flagged=flagged)
            if site is None or spec is None:
                raise Refusal(
                    RefusalReason.INVALID_ARGUMENTS,
                    "serve needs site and spec, or service, or stop",
                )
            task = tasks_opted_in(ctx)
            record = await self.service.start_serve(site, spec, task=task, confirm=flagged)
            if flagged and "form" not in elicitation_modes(ctx):
                return self._needs_person(record)
            if task:
                return record_result(self.service, record)
            return await self._deliver(ctx, record, wait_s)
        except Refusal as refusal:
            return refusal_result(refusal)

    async def _observed(
        self, ctx: Context, record: OperationRecord, wait_s: float, *, flagged: bool
    ) -> ToolResult | InputRequiredToolResult:
        """Deliver a resumed or observed operation; a flagged call with no ask says so."""
        asked = bool(self.service.outstanding(record))
        result = await self._deliver(ctx, record, wait_s)
        if flagged and not asked and not isinstance(result, InputRequiredToolResult):
            result.meta = job_card.not_needed(result.meta)
        return result

    async def _cancel_call(
        self, ctx: Context, operation_id: str, verb: str, *, flagged: bool
    ) -> ToolResult | InputRequiredToolResult:
        """Cancel or stop; a flagged call asks the person once first, with the job's card."""
        record = self.service.operation(operation_id)
        if not flagged or record.state.final:
            result = record_result(self.service, await self.service.cancel(operation_id))
            if flagged:
                result.meta = job_card.not_needed(result.meta)
            return result
        if ctx.request_state and ctx.input_responses is not None:
            kinds = {confirm.CANCEL_KIND}
            confirm.answer_round(self.service, ctx.request_state, ctx.input_responses, kinds)

        async def call() -> OperationRecord:
            ask = await confirm.cancel_confirm(self.service, record, verb)
            try:
                return await self.service.cancel(operation_id)
            finally:
                confirm.used(self.service, ask)

        again = f"call {'serve with stop' if verb == 'stop' else 'run with cancel'} again"
        done = await confirm.with_person(ctx, self.service, call, again=again)
        return record_result(self.service, done) if isinstance(done, OperationRecord) else done

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
        await self.service.same_request(record, site, spec)
        answered: list[Ask] = []
        for ask_id, response in (ctx.input_responses or {}).items():
            if ask_id in record.asks:
                action, content = response_parts(response)
                answered.append(self.service.answer(ask_id, action, content, by="input_required"))
        record = await advance_holders(self.service, operation_id, answered)
        return await self._deliver(ctx, record, wait_s)

    async def exec(
        self,
        ctx: Context,
        site: str,
        command: list[str],
        timeout_s: float | None,
        max_output_bytes: int,
        plan: ExecPlan | None = None,
        *,
        read_only: bool = False,
    ) -> ToolResult | InputRequiredToolResult:
        """The ``exec`` verb. With no ``timeout_s`` it runs until the command ends,
        and tells the person when it goes quiet; cancelling the call stops it.

        On a guarded site a ``plan`` asks the person before a command that is
        not read-only: in the call when the client shows a form, else as a
        ``needs_approval`` refusal to retry after the answer. With ``read_only``
        (``exec_read``) only a read-only command runs, and nothing asks."""

        quiet = QuietReports(ctx, read_only=read_only)
        flagged = not read_only and job_card.flagged(ctx)

        async def call() -> dict[str, Any]:
            return await self.service.exec(
                site,
                command,
                timeout_s=timeout_s,
                max_output_bytes=max_output_bytes,
                on_stall=quiet,
                plan=plan,
                read_only=read_only,
                confirm=flagged,
            )

        try:
            if ctx.request_state and ctx.input_responses is not None and not read_only:
                self._answer_plan(site, ctx.request_state, ctx.input_responses)
            again = "call exec again with the same command and plan"
            result = await confirm.with_person(ctx, self.service, call, again=again)
            if not isinstance(result, dict):
                return result
        except Refusal as refusal:
            return refusal_result(refusal)
        if (stretches := quiet.summary()) is not None:
            result["quiet"] = stretches
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
        if ask is not None and ask.site == site:
            kinds = {EXEC_PLAN_KIND, confirm.EXEC_CONFIRM_KIND}
            confirm.answer_round(self.service, ask_id, responses, kinds)

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
        install_read_tools(server, relay)
        install_site_tools(server, relay)
        install_files_tool(server, relay)

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

        server.add_tool(_task_optional(run, "run", "Run a job on a site", confirms=True))
        server.add_tool(_task_optional(install_tool(relay), "install", "Install a package"))

        @server.tool(
            name="exec",
            title="Run one short command on a site",
            annotations=_CHANGE,
            meta={job_card.CONFIRM_META: job_card.CONFIRM_TOOL},
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
            """Run one command on the site's login side and return bounded output. A
            command that only reads (ls, squeue, jarvis sched info +json and the like)
            runs through exec_read, which never asks."""
            return await relay.exec(ctx, site, command, timeout_s, max_output_bytes, plan)

        @server.tool(
            name="packages",
            title="Stage and remove the packages relay can run",
            annotations=_CHANGE,
        )
        async def packages(  # pyright: ignore[reportUnusedFunction]
            op: Literal["stage", "remove"],
            package: Annotated[str, Field(description="<repository>.<package>")],
            dir: Annotated[  # noqa: A002 - the source is a directory
                str | None,
                Field(description="Stage from this absolute directory on the relay host"),
            ] = None,
            version: Annotated[
                str | None,
                Field(
                    description="stage: when the package has no contract version or tag. "
                    "remove: only this version"
                ),
            ] = None,
            sha256: Annotated[str | None, Field(description="remove: only this archive")] = None,
        ) -> ToolResult:
            """run needs a staged package: stage one before its first run. stage builds the
            package's archive on the relay host and adds it to relay's catalog: naming only
            package, from relay's package sources (directories its host installed, listed
            in sources); with dir, from that directory on the relay host, such as a package
            the person wrote. Staging fetches nothing and runs no git. remove takes a
            package out (version or sha256 narrow it). When the package sources change, a
            stage or run by name uses their new archive and lists the old one with
            superseded_by; pin its sha256 to run it. Staging runs nothing: a guarded site
            reviews the archive by its SHA-256 before its first run there, and the archive
            reaches the site through relay's own transfer. To list what is staged and what
            the package sources hold (available), use packages_read, which only reads."""
            args = PackagesArgs(op=op, package=package, dir=dir, version=version, sha256=sha256)

            try:
                result = await packages_op(relay.service, args)
            except Refusal as refusal:
                return refusal_result(refusal)
            return ToolResult(
                content=[mcp_types.TextContent(type="text", text=_text(result))],
                structured_content=result,
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
            until: step_wait.Until = None,
        ) -> ToolResult | InputRequiredToolResult:
            """Start a job with ports and return how to reach it. With tasks this returns
            a task whose status names the endpoint; without, it waits up to wait_s for
            the service to be ready. service observes one; stop ends it, as cancel does. Each
            endpoint port has an address, host:port: that is what you hand to clients. A job
            computing beside its service runs on after ready: service with until="steps"
            waits, as a task, for its compute steps to end and returns their result."""
            return await relay.serve(ctx, site, spec, service, stop, wait_s, until)

        server.add_tool(_task_optional(serve, "serve", "Start a service on a site", confirms=True))

        @server.tool(
            name="shutdown",
            title="Stop relay's long-lived processes",
            annotations=_CHANGE,
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
        job_watch.install(server, self.service, self.changes)
        return server

    @asynccontextmanager
    async def _lifespan(self, server: FastMCP[Any]) -> AsyncGenerator[dict[str, Any]]:
        # A client reads the instructions when it connects; give it today's sites.
        server.instructions = await asyncio.to_thread(instructions.current, self.service)
        # A service that ended while relay was down is finished without anyone asking.
        observing = asyncio.create_task(self.service.observe_open_services())
        watching = asyncio.create_task(self.skills.watch(self.changes))
        refreshing = asyncio.create_task(instructions.keep_current(server, self.service))
        jobs = asyncio.create_task(job_watch.watch(self.service))
        try:
            yield {}
        finally:
            observing.cancel()
            watching.cancel()
            refreshing.cancel()
            jobs.cancel()
            await asyncio.gather(observing, watching, refreshing, jobs, return_exceptions=True)
            self.changes.close()
            await self.service.close()


def _task_optional(fn: Any, name: str, title: str, *, confirms: bool = False) -> FunctionTool:
    tool = FunctionTool.from_function(
        fn,
        name=name,
        title=title,
        annotations=_START,
        meta={job_card.CONFIRM_META: job_card.CONFIRM_TOOL} if confirms else None,
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
