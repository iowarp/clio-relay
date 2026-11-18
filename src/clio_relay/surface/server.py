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

import base64
import hmac
import json
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Annotated, Any, Literal

import mcp_types
from fastmcp import Context, FastMCP
from fastmcp.server.auth import AccessToken, AuthProvider, TokenVerifier
from fastmcp.server.extensions import (
    MethodBinding,
    ServerExtension,
    read_client_extension_settings,
)
from fastmcp.tools import InputRequiredToolResult, ToolResult
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

from clio_relay.adapters.hooks import Ask, Transport
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, ServiceSpec
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

PROTOCOL_VERSION = "2026-07-28"
TASK_TTL_MS = 7 * 24 * 3600 * 1000
TASK_POLL_INTERVAL_MS = 2000
OPERATION_META = "ai.iowarp.relay/operation"
MAX_INLINE_BYTES = 4 * 1024 * 1024

_TASK_VERSIONS = frozenset(MODERN_PROTOCOL_VERSIONS)

INSTRUCTIONS = (
    "Relay runs work on the sites in its sites file. Call describe first. "
    "run submits a job (a PSI/J JobSpec) and is a task when the client supports tasks; "
    "exec runs one short command; files lists, reads, writes and stages files; "
    "serve starts a service. Refusals carry a typed reason in structuredContent.refusal."
)


def refusal_result(refusal: Refusal) -> ToolResult:
    """Render a refusal as an error tool result with a typed reason."""
    return ToolResult(
        content=[mcp_types.TextContent(type="text", text=f"{refusal.reason.value}: {refusal.message}")],
        structured_content={"refusal": refusal.to_dict()},
        is_error=True,
    )


def _text(value: dict[str, Any]) -> str:
    return json.dumps(value, indent=2, sort_keys=True, default=str)


def _summary(view: dict[str, Any]) -> str:
    line = f"Operation {view['operation_id']} on {view['site']} is {view['state']}"
    if view.get("exit_code") is not None:
        line += f" with exit code {view['exit_code']}"
    line += "."
    result = view.get("result") or {}
    tail = result.get("stdout_tail")
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


def _modern(ctx: Context) -> bool:
    rc = ctx.request_context
    return rc is not None and rc.protocol_version in MODERN_PROTOCOL_VERSIONS


def _tasks_opted_in(ctx: Context) -> bool:
    return _modern(ctx) and ctx.client_extension_settings(TASKS_EXTENSION_ID) is not None


def _elicitation_modes(ctx: Context) -> set[str]:
    try:
        capabilities = ctx.session.client_capabilities
    except (AttributeError, RuntimeError):
        return set()
    elicitation = getattr(capabilities, "elicitation", None) if capabilities else None
    if elicitation is None:
        return set()
    form = getattr(elicitation, "form", None)
    url = getattr(elicitation, "url", None)
    modes: set[str] = set()
    if form is not None or url is None:
        modes.add("form")
    if url is not None:
        modes.add("url")
    return modes


def _mode(ask: Ask) -> str:
    return "url" if ask.kind == "url" else "form"


def elicit_request(ask: Ask) -> mcp_types.ElicitRequest:
    """Return the elicitation for one ask."""
    if ask.kind == "url":
        return mcp_types.ElicitRequest(
            params=mcp_types.ElicitRequestURLParams(
                message=ask.message, url=ask.url or "", elicitation_id=ask.ask_id
            )
        )
    return mcp_types.ElicitRequest(
        params=mcp_types.ElicitRequestFormParams(
            message=ask.message,
            requested_schema=ask.requested_schema or {"type": "object", "properties": {}},
        )
    )


def _elicit_document(ask: Ask) -> dict[str, Any]:
    return elicit_request(ask).model_dump(by_alias=True, mode="json", exclude_none=True)


def _response_parts(response: Any) -> tuple[str, dict[str, Any] | None]:
    if isinstance(response, mcp_types.ElicitResult):
        return response.action, dict(response.content) if response.content else None
    if isinstance(response, dict):
        action = str(response.get("action", "cancel"))  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        content = response.get("content")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        return action, dict(content) if isinstance(content, dict) else None  # pyright: ignore[reportUnknownArgumentType]
    return "cancel", None


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
        return f"relay operation is {record.state.value}"

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
                    ask.ask_id: _elicit_document(ask) for ask in self.service.outstanding(record)
                },
            )
        if record.state.final:
            result = operation_result(self.service.view(record))
            return GetTaskResult(
                **fields,
                result=result.to_mcp_result().model_dump(by_alias=True, mode="json", exclude_none=True)
                if hasattr(result, "to_mcp_result")
                else {
                    "content": [item.model_dump(mode="json") for item in result.content],
                    "structuredContent": result.structured_content,
                    "isError": result.is_error,
                },
            )
        return GetTaskResult(**fields)

    async def _get(self, ctx: ServerRequestContext[Any, Any], params: GetTaskParams) -> GetTaskResult:
        self._require(ctx)
        self._record(params.task_id)
        try:
            record = await self.service.refresh(params.task_id)
        except Refusal as refusal:
            raise MCPError(code=-32603, message=refusal.message, data=refusal.to_dict()) from refusal
        return await self.task_result(record)

    async def _update(
        self, ctx: ServerRequestContext[Any, Any], params: UpdateTaskParams
    ) -> UpdateTaskResult:
        self._require(ctx)
        record = self._record(params.task_id)
        for ask_id, response in params.input_responses.items():
            if ask_id not in record.asks:
                continue
            action, content = _response_parts(response)
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
        """Turn a task ``run`` into a task once its operation is recorded."""
        outcome = await call_next()
        if params.name != "run" or not _tasks_opted_in(context):
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

    async def _ask_inline(self, ctx: Context, record: OperationRecord) -> OperationRecord:
        modes = _elicitation_modes(ctx)
        for ask in self.service.outstanding(record):
            if _mode(ask) not in modes:
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
            action, content = _response_parts(response)
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
            "`clio-relay-next asks answer`, then call run with resume set to the operation id",
            operation_id=record.operation_id,
            asks=shown,
        )
        return refusal_result(refusal)

    async def _deliver(
        self, ctx: Context, record: OperationRecord, wait_s: float
    ) -> ToolResult | InputRequiredToolResult:
        if record.state == OperationState.AWAITING_INPUT:
            outstanding = self.service.outstanding(record)
            if outstanding and _modern(ctx):
                modes = _elicitation_modes(ctx)
                if all(_mode(ask) in modes for ask in outstanding):
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
        return operation_result(self.service.view(record))

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
                return operation_result(self.service.view(record))
            if ctx.request_state and ctx.input_responses is not None:
                return await self._resume_round(ctx, site, spec, wait_s)
            if resume is not None:
                record = await self.service.advance(resume)
                return await self._deliver(ctx, record, wait_s)
            if site is None or spec is None:
                raise Refusal(
                    RefusalReason.INVALID_ARGUMENTS,
                    "run needs site and spec, or cancel, or resume",
                )
            task = _tasks_opted_in(ctx)
            record = await self.service.start_run(site, spec, task=task)
            if task:
                return operation_result(self.service.view(record))
            return await self._deliver(ctx, record, wait_s)
        except Refusal as refusal:
            return refusal_result(refusal)

    async def _resume_round(
        self, ctx: Context, site: str | None, spec: JobSpec | None, wait_s: float
    ) -> ToolResult | InputRequiredToolResult:
        operation_id = ctx.request_state or ""
        record = self.service.operation(operation_id)
        if site is not None and spec is not None:
            current = self.service.apply_defaults(self.service.site(site), spec)
            if site != record.site or current.digest() != record.spec_digest:
                raise Refusal(
                    RefusalReason.APPROVAL_MISMATCH,
                    "the retry's arguments differ from what was asked about; call run again",
                    operation_id=operation_id,
                )
        for ask_id, response in (ctx.input_responses or {}).items():
            if ask_id in record.asks:
                action, content = _response_parts(response)
                self.service.answer(ask_id, action, content, by="input_required")
        record = await self.service.advance(operation_id)
        return await self._deliver(ctx, record, wait_s)

    async def exec(
        self, site: str, command: list[str], timeout_s: float, max_output_bytes: int
    ) -> ToolResult:
        """The ``exec`` verb."""
        try:
            result = await self.service.exec(
                site, command, timeout_s=timeout_s, max_output_bytes=max_output_bytes
            )
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

    async def files(
        self,
        site: str,
        op: str,
        path: str | None,
        content: str | None,
        content_base64: str | None,
        offset: int,
        limit: int | None,
        overwrite: bool,
        digest: str | None,
    ) -> ToolResult:
        """The ``files`` verb."""
        try:
            entry = self.service.site(site)
            adapter = self.service.adapter_for(entry, "files")
            access = await adapter.files()
            structured: dict[str, Any]
            if op == "list":
                listing = await access.list(path or "")
                structured = {"entries": [item.model_dump(mode="json") for item in listing]}
                text = "\n".join(
                    f"{item.kind[0]} {item.size or 0:>12} {item.path}" for item in listing
                )
            elif op == "stat":
                stat = await access.stat(_need(path, "path"))
                structured = stat.model_dump(mode="json")
                text = _text(structured)
            elif op == "read":
                cap = min(limit or MAX_INLINE_BYTES, MAX_INLINE_BYTES)
                data = await access.read(_need(path, "path"), offset=offset, limit=cap)
                try:
                    decoded = data.decode("utf-8")
                    structured = {"path": path, "offset": offset, "size": len(data), "text": decoded}
                    text = decoded
                except UnicodeDecodeError:
                    encoded = base64.b64encode(data).decode("ascii")
                    structured = {
                        "path": path,
                        "offset": offset,
                        "size": len(data),
                        "content_base64": encoded,
                    }
                    text = f"{len(data)} bytes of binary data, in structuredContent.content_base64"
            elif op in ("write", "stage"):
                data = _payload(content, content_base64)
                if op == "write":
                    written = await access.write(_need(path, "path"), data, overwrite=overwrite)
                    structured = written.model_dump(mode="json")
                    text = f"wrote {written.size} bytes to {written.path}"
                else:
                    staged = await access.stage(data)
                    structured = {"sha256": staged, "size": len(data)}
                    text = f"staged {len(data)} bytes as {staged}"
            elif op == "has":
                present = await access.has(_need(digest, "digest"))
                structured = {"sha256": digest, "present": present}
                text = "present" if present else "absent"
            else:
                raise Refusal(
                    RefusalReason.VERB_NOT_IMPLEMENTED,
                    f"files op {op!r} is not available yet",
                    op=op,
                )
        except Refusal as refusal:
            return refusal_result(refusal)
        return ToolResult(
            content=[mcp_types.TextContent(type="text", text=text)],
            structured_content=structured,
        )

    async def serve(self, site: str, spec: ServiceSpec) -> ToolResult:
        """The ``serve`` verb, through the serve hook."""
        try:
            entry = self.service.site(site)
            adapter = self.service.adapter_for(entry, "serve")
            raise Refusal(
                RefusalReason.VERB_NOT_IMPLEMENTED,
                "serve needs a service runtime, and none is installed",
                adapter=adapter.name,
                spec_digest=spec.digest(),
            )
        except Refusal as refusal:
            return refusal_result(refusal)

    def build(self, *, auth: AuthProvider | None = None) -> FastMCP[Any]:
        """Return the FastMCP server."""
        server: FastMCP[Any] = FastMCP(
            "clio-relay",
            instructions=INSTRUCTIONS,
            auth=auth,
            tasks=False,
            strict_input_validation=True,
            lifespan=self._lifespan,
        )
        relay = self

        @server.tool(
            name="describe",
            title="Describe relay, a site, or an operation",
            annotations=mcp_types.ToolAnnotations(readOnlyHint=True, openWorldHint=True),
        )
        async def describe(
            site: Annotated[str | None, Field(description="A site id")] = None,
            job: Annotated[str | None, Field(description="An operation id")] = None,
        ) -> ToolResult:
            """Without arguments, list the sites. With site, describe it and its routes.
            With job, observe one operation."""
            try:
                described = await relay.service.describe(site, job)
            except Refusal as refusal:
                return refusal_result(refusal)
            if job is not None:
                return operation_result(described)
            return ToolResult(
                content=[mcp_types.TextContent(type="text", text=_text(described))],
                structured_content=described,
            )

        @server.tool(
            name="run",
            title="Run a job on a site",
            annotations=mcp_types.ToolAnnotations(destructiveHint=False, openWorldHint=True),
            task=TaskConfig(
                mode="optional", poll_interval=timedelta(milliseconds=TASK_POLL_INTERVAL_MS)
            ),
        )
        async def run(
            ctx: Context,
            site: Annotated[str | None, Field(description="The site id")] = None,
            spec: Annotated[JobSpec | None, Field(description="The job, as a PSI/J JobSpec")] = None,
            cancel: Annotated[str | None, Field(description="An operation id to cancel")] = None,
            resume: Annotated[
                str | None, Field(description="An operation id waiting on a person")
            ] = None,
            wait_s: Annotated[
                float, Field(ge=0, le=300, description="Seconds to wait without a task")
            ] = 20,
        ) -> ToolResult | InputRequiredToolResult:
            """Submit a job. With tasks this returns a task; without, it waits up to
            wait_s and returns the operation, which describe(job=...) observes."""
            return await relay.run(ctx, site, spec, cancel, resume, wait_s)

        @server.tool(
            name="exec",
            title="Run one short command on a site",
            annotations=mcp_types.ToolAnnotations(destructiveHint=True, openWorldHint=True),
        )
        async def exec_(
            site: Annotated[str, Field(description="The site id")],
            command: Annotated[list[str], Field(min_length=1, description="argv, no shell")],
            timeout_s: Annotated[float, Field(gt=0, le=600)] = 60,
            max_output_bytes: Annotated[int, Field(ge=1, le=MAX_INLINE_BYTES)] = 65536,
        ) -> ToolResult:
            """Run one command on the site's login side and return bounded output."""
            return await relay.exec(site, command, timeout_s, max_output_bytes)

        @server.tool(
            name="files",
            title="List, read, write and stage files on a site",
            annotations=mcp_types.ToolAnnotations(destructiveHint=True, openWorldHint=True),
        )
        async def files(
            site: Annotated[str, Field(description="The site id")],
            op: Literal["list", "stat", "read", "write", "stage", "has"],
            path: str | None = None,
            content: Annotated[str | None, Field(description="Text to write or stage")] = None,
            content_base64: Annotated[
                str | None, Field(description="Bytes to write or stage, base64")
            ] = None,
            offset: Annotated[int, Field(ge=0)] = 0,
            limit: Annotated[int | None, Field(ge=1)] = None,
            overwrite: bool = False,
            digest: Annotated[str | None, Field(description="A SHA-256, for has")] = None,
        ) -> ToolResult:
            """Small files inline. stage puts bytes in the site's content store and
            returns the SHA-256 a JobSpec input names."""
            return await relay.files(
                site, op, path, content, content_base64, offset, limit, overwrite, digest
            )

        @server.tool(
            name="serve",
            title="Start a service on a site",
            annotations=mcp_types.ToolAnnotations(destructiveHint=False, openWorldHint=True),
        )
        async def serve(
            site: Annotated[str, Field(description="The site id")],
            spec: ServiceSpec,
        ) -> ToolResult:
            """Start a job with ports and return how to reach it."""
            return await relay.serve(site, spec)

        server.add_extension(self.tasks)
        return server

    @asynccontextmanager
    async def _lifespan(self, _server: FastMCP[Any]) -> AsyncGenerator[dict[str, Any]]:
        try:
            yield {}
        finally:
            await self.service.close()


def _need(value: str | None, name: str) -> str:
    if not value:
        raise Refusal(RefusalReason.INVALID_ARGUMENTS, f"this op needs {name}")
    return value


def _payload(content: str | None, content_base64: str | None) -> bytes:
    if (content is None) == (content_base64 is None):
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS, "give exactly one of content or content_base64"
        )
    if content is not None:
        data = content.encode("utf-8")
    else:
        try:
            data = base64.b64decode(content_base64 or "", validate=True)
        except ValueError as exc:
            raise Refusal(RefusalReason.INVALID_ARGUMENTS, "content_base64 is not base64") from exc
    if len(data) > MAX_INLINE_BYTES:
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS,
            f"inline content is limited to {MAX_INLINE_BYTES} bytes; stage larger files",
        )
    return data


def create_server(
    paths: RelayPaths | None = None,
    *,
    transport: Transport = "stdio",
    token: str | None = None,
    base_url: str | None = None,
) -> FastMCP[Any]:
    """Build relay's MCP server. HTTP needs a bearer token."""
    service = RelayService(paths or RelayPaths.from_env(), transport=transport)
    auth: AuthProvider | None = None
    if transport == "http":
        if not token or not base_url:
            raise ValueError("HTTP transport needs a bearer token and a base URL")
        auth = RelayBearer(token, base_url=base_url)
    return RelayServer(service).build(auth=auth)
