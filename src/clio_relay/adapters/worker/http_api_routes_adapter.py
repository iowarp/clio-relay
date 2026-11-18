"""The worker's adapter routes: the site half of the ``worker`` adapter (RL11).

The worker on the site serves the adapter verbs by running the ``local``
adapter where it is. The relay that holds the link calls these routes over it.
Every route is a POST that answers 200 with either ``{"ok": ...}`` or
``{"refusal": {...}}``, so a typed refusal crosses the link unchanged. File
bytes travel as base64.
"""

# pyright: reportUnusedFunction=false

from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI
from fastapi.params import Depends
from pydantic import ValidationError

from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.local import LocalAdapter, LocalConfig
from clio_relay.adapters.worker.http_api_context import RelayApiContext
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import ExecResult, FileAccess, JobHandle
from clio_relay.jobs.records import new_id
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec

ADAPTER_ROUTE_PREFIX = "/adapter"
_POLL_WAIT_S = 20.0


@dataclass
class _RunningExec:
    """One command started by ``exec/start``, until a poll collects its result."""

    task: asyncio.Task[ExecResult] | None = None
    stalls: list[tuple[float, float]] = field(default_factory=list[tuple[float, float]])
    changed: asyncio.Event = field(default_factory=asyncio.Event)


_NAMESPACE = re.compile(r"^[0-9a-f]{8,64}$")


def worker_adapter_root(ctx: RelayApiContext, namespace: str) -> Path:
    """Return where one relay's jobs and content live on this site.

    Each relay that holds a link here sends its own namespace, so two relays
    sharing a site never see each other's jobs.
    """
    if _NAMESPACE.fullmatch(namespace) is None:
        raise Refusal(RefusalReason.INVALID_ARGUMENTS, "namespace is not valid")
    return Path(ctx.resolved.core_dir).expanduser().parent / "adapter" / namespace


def _site_adapter(ctx: RelayApiContext, body: dict[str, Any]) -> LocalAdapter:
    raw = body.get("config") or {}
    if not isinstance(raw, dict):
        raise Refusal(RefusalReason.INVALID_CONFIG, "config must be an object")
    try:
        config = LocalConfig.model_validate(raw)
    except ValidationError as exc:
        raise Refusal(RefusalReason.INVALID_CONFIG, str(exc)) from exc
    root = worker_adapter_root(ctx, str(body.get("namespace") or ""))
    context = AdapterContext(
        site_id=ctx.owner_session_cluster or "worker",
        state_dir=root,
        content=ContentStore(root / "content"),
    )
    return LocalAdapter(config, context)


def _decode(value: object) -> bytes:
    if not isinstance(value, str):
        raise Refusal(RefusalReason.INVALID_ARGUMENTS, "data must be base64 text")
    try:
        return base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise Refusal(RefusalReason.INVALID_ARGUMENTS, "data is not valid base64") from exc


def _text(body: dict[str, Any], key: str) -> str:
    value = body.get(key)
    if not isinstance(value, str) or not value:
        raise Refusal(RefusalReason.INVALID_ARGUMENTS, f"{key} must be a non-empty string")
    return value


def _handle(body: dict[str, Any]) -> JobHandle:
    try:
        return JobHandle.model_validate(body.get("handle"))
    except ValidationError as exc:
        raise Refusal(RefusalReason.INVALID_ARGUMENTS, f"handle is not valid: {exc}") from exc


async def _answer(action: Callable[[], Awaitable[Any]]) -> dict[str, Any]:
    try:
        return {"ok": await action()}
    except Refusal as refusal:
        return {"refusal": refusal.to_dict()}


async def _file_op(adapter: LocalAdapter, op: str, body: dict[str, Any]) -> Any:
    files: FileAccess = await adapter.files()
    if op == "list":
        return [entry.model_dump(mode="json") for entry in await files.list(_text(body, "path"))]
    if op == "stat":
        return (await files.stat(_text(body, "path"))).model_dump(mode="json")
    if op == "read":
        offset = body.get("offset", 0)
        limit = body.get("limit")
        if not isinstance(offset, int) or (limit is not None and not isinstance(limit, int)):
            raise Refusal(RefusalReason.INVALID_ARGUMENTS, "offset and limit must be integers")
        data = await files.read(_text(body, "path"), offset=offset, limit=limit)
        return base64.b64encode(data).decode("ascii")
    if op == "write":
        written = await files.write(
            _text(body, "path"), _decode(body.get("data")), overwrite=body.get("overwrite") is True
        )
        return written.model_dump(mode="json")
    if op == "stage":
        return await files.stage(_decode(body.get("data")))
    if op == "has":
        return await files.has(_text(body, "digest"))
    raise Refusal(RefusalReason.INVALID_ARGUMENTS, f"unknown file operation {op!r}")


def register_adapter_routes(
    app: FastAPI,
    ctx: RelayApiContext,
    *,
    auth_dependency: Depends,
) -> None:
    """Register the adapter verbs on an owned session's API."""
    prefix = ADAPTER_ROUTE_PREFIX

    @app.post(f"{prefix}/describe", dependencies=[auth_dependency])
    async def adapter_describe(body: dict[str, Any] = _BODY) -> dict[str, Any]:
        async def action() -> Any:
            info = await _site_adapter(ctx, body).describe()
            return info.model_dump(mode="json")

        return await _answer(action)

    @app.post(f"{prefix}/run", dependencies=[auth_dependency])
    async def adapter_run(body: dict[str, Any] = _BODY) -> dict[str, Any]:
        async def action() -> Any:
            try:
                spec = JobSpec.model_validate(body.get("spec"))
            except ValidationError as exc:
                raise Refusal(RefusalReason.INVALID_SPEC, str(exc)) from exc
            adapter = _site_adapter(ctx, body)
            for item in spec.inputs:
                if not adapter.context.content.has(item.sha256):
                    raise Refusal(
                        RefusalReason.INPUT_MISSING,
                        f"input {item.name} ({item.sha256}) is not staged on the site",
                        name=item.name,
                        sha256=item.sha256,
                    )
            handle = await adapter.run(spec, operation_id=_text(body, "operation_id"))
            return handle.model_dump(mode="json")

        return await _answer(action)

    @app.post(f"{prefix}/status", dependencies=[auth_dependency])
    async def adapter_status(body: dict[str, Any] = _BODY) -> dict[str, Any]:
        async def action() -> Any:
            status = await _site_adapter(ctx, body).status(_handle(body))
            return status.model_dump(mode="json")

        return await _answer(action)

    @app.post(f"{prefix}/cancel", dependencies=[auth_dependency])
    async def adapter_cancel(body: dict[str, Any] = _BODY) -> dict[str, Any]:
        async def action() -> Any:
            status = await _site_adapter(ctx, body).cancel(_handle(body))
            return status.model_dump(mode="json")

        return await _answer(action)

    @app.post(f"{prefix}/find", dependencies=[auth_dependency])
    async def adapter_find(body: dict[str, Any] = _BODY) -> dict[str, Any]:
        async def action() -> Any:
            found = await _site_adapter(ctx, body).find(_text(body, "operation_id"))
            return None if found is None else found.model_dump(mode="json")

        return await _answer(action)

    running: dict[str, _RunningExec] = {}

    @app.post(f"{prefix}/exec/start", dependencies=[auth_dependency])
    async def adapter_exec_start(
        body: dict[str, Any] = _BODY,
    ) -> dict[str, Any]:
        async def action() -> Any:
            command = body.get("command")
            timeout_s = body.get("timeout_s")
            max_output_bytes = body.get("max_output_bytes")
            if (
                not isinstance(command, list)
                or not command
                or not all(isinstance(part, str) for part in command)
            ):
                raise Refusal(RefusalReason.INVALID_ARGUMENTS, "command must be a list of strings")
            if timeout_s is not None and (not isinstance(timeout_s, int | float) or timeout_s <= 0):
                raise Refusal(RefusalReason.INVALID_ARGUMENTS, "timeout_s must be positive")
            if not isinstance(max_output_bytes, int) or max_output_bytes <= 0:
                raise Refusal(
                    RefusalReason.INVALID_ARGUMENTS, "max_output_bytes must be a positive integer"
                )
            argv: list[str] = [str(part) for part in command]  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
            adapter = _site_adapter(ctx, body)
            exec_id = new_id("exec")
            entry = _RunningExec()

            async def on_stall(idle_s: float, next_s: float) -> None:
                entry.stalls.append((idle_s, next_s))
                entry.changed.set()

            entry.task = asyncio.create_task(
                adapter.exec(
                    argv,
                    timeout_s=None if timeout_s is None else float(timeout_s),
                    max_output_bytes=max_output_bytes,
                    on_stall=on_stall,
                )
            )
            entry.task.add_done_callback(lambda _: entry.changed.set())
            running[exec_id] = entry
            return {"exec_id": exec_id}

        return await _answer(action)

    @app.post(f"{prefix}/exec/poll", dependencies=[auth_dependency])
    async def adapter_exec_poll(
        body: dict[str, Any] = _BODY,
    ) -> dict[str, Any]:
        async def action() -> Any:
            exec_id = _text(body, "exec_id")
            entry = running.get(exec_id)
            if entry is None or entry.task is None:
                raise Refusal(
                    RefusalReason.OPERATION_NOT_FOUND, f"no command {exec_id} is running here"
                )
            after = body.get("after", 0)
            wait_s = body.get("wait_s", _POLL_WAIT_S)
            if not isinstance(after, int) or after < 0:
                raise Refusal(RefusalReason.INVALID_ARGUMENTS, "after must be a count")
            if not isinstance(wait_s, int | float) or wait_s < 0:
                raise Refusal(RefusalReason.INVALID_ARGUMENTS, "wait_s must not be negative")
            if len(entry.stalls) <= after and not entry.task.done():
                entry.changed.clear()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        entry.changed.wait(), timeout=min(float(wait_s), _POLL_WAIT_S)
                    )
            stalls = [list(item) for item in entry.stalls[after:]]
            if not entry.task.done():
                return {"done": False, "stalls": stalls}
            running.pop(exec_id, None)
            if entry.task.cancelled():
                raise Refusal(RefusalReason.OPERATION_NOT_FOUND, f"command {exec_id} was cancelled")
            result = entry.task.result()
            return {"done": True, "stalls": stalls, "result": result.model_dump(mode="json")}

        return await _answer(action)

    @app.post(f"{prefix}/exec/cancel", dependencies=[auth_dependency])
    async def adapter_exec_cancel(
        body: dict[str, Any] = _BODY,
    ) -> dict[str, Any]:
        async def action() -> Any:
            entry = running.pop(_text(body, "exec_id"), None)
            if entry is None or entry.task is None:
                return False
            entry.task.cancel()
            with contextlib.suppress(BaseException):
                await entry.task
            return True

        return await _answer(action)

    @app.post(f"{prefix}/files/{{op}}", dependencies=[auth_dependency])
    async def adapter_files(
        op: str, body: dict[str, Any] = _BODY
    ) -> dict[str, Any]:
        return await _answer(lambda: _file_op(_site_adapter(ctx, body), op, body))
