"""The service runtime behind W1's ``ServeHook`` (RL19 to RL23).

Adapters start a service with :func:`start_for`. Relay's surface drives the
rest through the hook: ``status`` brings the desktop port up once the service
answers, mounts a served MCP server, and follows the job it is bound to;
``stop`` closes the desktop port at once, gives the service its grace period
and returns the artifacts it wrote.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import inspect
import json
import logging
import re
import shlex
import time
from pathlib import Path
from typing import Any, cast

from clio_relay.jobs.handles import (
    Endpoint,
    ExecResult,
    FileAccess,
    JobHandle,
    JobState,
    JobStatus,
    OutputRef,
    utc_now,
)
from clio_relay.jobs.records import OperationRecord, RecordStore
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import ServiceSpec
from clio_relay.serve import config as serve_settings
from clio_relay.serve.config import SiteServe
from clio_relay.serve.forward import Forward
from clio_relay.serve.mount import Mounter, tool_prefix
from clio_relay.serve.plan import (
    SERVE_DIR,
    HopState,
    ServePlan,
    check_wrappable,
    new_secret,
    plan_ports,
    service_name,
    site_file,
    site_files,
    wrapped_job,
)
from clio_relay.serve.store import PlanStore

logger = logging.getLogger(__name__)

HOP_IDLE_EXIT_S = 3600
EXEC_TIMEOUT_S = 120.0
EXEC_OUTPUT = 65536
STOP_MARGIN_S = 30.0
CANCEL_RETRY_S = 60.0
LOG_TAIL = 4096
# frpc's log line: "2026-11-18 12:38:59.277 [W] [client/service.go:319] text".
_FRPC_LINE = re.compile(r"^\S+ \S+ \[([A-Z])\] \[[^\]]*\] (.*)$")

_runtime: ServeRuntime | None = None


def factory(context: Any) -> ServeRuntime:
    """Build the runtime from W1's ``HookContext`` (entry point ``clio_relay.hooks:serve``)."""
    global _runtime
    _runtime = ServeRuntime(
        Path(context.state_dir),
        settings=dict(getattr(context, "settings", None) or {}),
        transport=str(getattr(context, "transport", "stdio")),
    )
    return _runtime


def current() -> ServeRuntime | None:
    """Return the runtime relay built, if it built one."""
    return _runtime


def _state_dir_of(adapter: Any) -> Path:
    state_dir = Path(adapter.context.state_dir)
    if state_dir.parent.parent.name == "sites":
        return state_dir.parent.parent.parent
    return state_dir


async def start_for(adapter: Any, spec: ServiceSpec, *, operation_id: str) -> Endpoint:
    """Start ``spec`` as a job on ``adapter``'s site. An adapter's ``serve`` calls this."""
    global _runtime
    if _runtime is None:
        _runtime = ServeRuntime(_state_dir_of(adapter))
    return await _runtime.start_for(adapter, spec, operation_id=operation_id)


async def _maybe(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


def _site_id(site: Any) -> str:
    return site if isinstance(site, str) else str(site.id)


class ServeRuntime:
    """Every service this relay host started, and their desktop ports."""

    def __init__(
        self,
        state_dir: Path,
        *,
        settings: dict[str, Any] | None = None,
        transport: str = "stdio",
    ) -> None:
        self.state_dir = state_dir
        self.settings_file = serve_settings.settings_path(state_dir, settings)
        self.transport = transport
        self.store = PlanStore(state_dir)
        self.records: RecordStore[OperationRecord] = RecordStore(
            state_dir / "operations", OperationRecord
        )
        self.plans: dict[str, ServePlan] = {}
        self.forwards: dict[str, Forward] = {}
        self.mounter = Mounter()
        self.sites: dict[str, Any] = {}
        self.notes: dict[str, str] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._resumed = False

    # The hook

    async def start(
        self, site: Any, adapter: Any, spec: ServiceSpec, record: OperationRecord
    ) -> Endpoint | None:
        """Start a service through the adapter's ``serve`` verb."""
        self.sites[_site_id(site)] = site
        try:
            await adapter.serve(spec, operation_id=record.operation_id)
        except Refusal as refusal:
            if refusal.reason != RefusalReason.VERB_NOT_IMPLEMENTED:
                raise
            await self.start_for(adapter, spec, operation_id=record.operation_id)
        status, endpoint = await self.status(site, adapter, record)
        if status.state.final:
            raise Refusal(
                RefusalReason.SUBMIT_FAILED,
                status.message or "the service ended as it started",
                state=status.state.value,
                exit_code=status.exit_code,
            )
        return endpoint

    async def status(
        self, site: Any, adapter: Any, record: OperationRecord
    ) -> tuple[JobStatus, Endpoint | None]:
        """Observe a service, and bring its desktop port up once it answers."""
        await self.resume()
        plan = self._plan(record.operation_id)
        async with self._lock(plan.operation_id):
            if plan.final is not None:
                return JobStatus.model_validate(plan.final), None
            handle = await self._handle(plan, adapter, record)
            if handle is None:
                return JobStatus(state=JobState.QUEUED, message="submitting the service"), None
            status = await adapter.status(handle)
            if status.state.final:
                return await self._finish(plan, adapter, status), None
            await self._follow_binding(plan, adapter)
            if status.state != JobState.RUNNING:
                return status, None
            return await self._running(plan, adapter, status)

    async def stop(self, site: Any, adapter: Any, record: OperationRecord) -> JobStatus:
        """Close the desktop port, then stop the service with its grace period."""
        await self.resume()
        plan = self._plan(record.operation_id)
        if plan.final is not None:
            return JobStatus.model_validate(plan.final)
        handle = await self._handle(plan, adapter, record)
        await self._close_desktop(plan)
        if plan.stop_requested_at is None:
            plan.stop_requested_at = utc_now()
            self._save(plan)
        if handle is None:
            final = JobStatus(state=JobState.CANCELLED, message="stopped before it was submitted")
            plan.final = final.model_dump(mode="json")
            self._save(plan)
            return final
        await self._request_stop(plan, adapter, handle)
        grace = plan.site_serve.stop_grace_s + STOP_MARGIN_S
        started = time.monotonic()
        last_cancel = 0.0
        delay = 1.0
        while True:
            async with self._lock(plan.operation_id):
                if plan.final is not None:
                    return JobStatus.model_validate(plan.final)
                status = await adapter.status(handle)
                if status.state.final:
                    return await self._finish(plan, adapter, status)
            waited = time.monotonic() - started
            if waited > grace and time.monotonic() - last_cancel > CANCEL_RETRY_S:
                last_cancel = time.monotonic()
                plan.cancel_sent = True
                self._save(plan)
                self.notes[plan.operation_id] = (
                    f"the service did not stop within {int(grace)} s; cancelling its job"
                )
                logger.warning("service %s: %s", plan.operation_id, self.notes[plan.operation_id])
                with contextlib.suppress(Refusal):
                    await adapter.cancel(handle)
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 10.0)

    async def close(self) -> None:
        """Release the desktop ports and mounted sessions. Services keep running."""
        for forward in list(self.forwards.values()):
            await forward.close()
        self.forwards.clear()
        await self.mounter.close()

    # What the surface may also call

    def attach(self, server: Any, changes: Any) -> None:
        """Let the runtime mount served MCP servers on relay's own server (RL21)."""
        self.mounter.attach(server, changes)
        self.mounter.provider.on_start = self.resume
        with contextlib.suppress(RuntimeError):
            asyncio.get_running_loop().create_task(self.resume())

    def ui_data(self, record: OperationRecord) -> dict[str, Any] | None:
        """Return what only a view may see: port URLs and protected ports' secrets (RL22).

        Relay strips every URL from the endpoint it records, so this is the one
        place a client finds them.
        """
        plan = self._cached(record.operation_id)
        if plan is None:
            return None
        ports: dict[str, Any] = {}
        for port in plan.ports:
            entry: dict[str, Any] = {"address": f"127.0.0.1:{port.local_port}"}
            url = port.url()
            if url:
                entry["url"] = url
            if port.secret_env and port.secret_env in plan.secrets:
                entry["secret"] = plan.secrets[port.secret_env]
                entry["secret_env"] = port.secret_env
            ports[port.name] = entry
        return {"ports": ports} if ports else None

    async def resume(self) -> None:
        """Bring back the desktop ports of services that were ready when relay stopped."""
        if self._resumed:
            return
        self._resumed = True
        for plan in self.store.all():
            self.plans.setdefault(plan.operation_id, plan)
            if plan.final is None and plan.ready_at is not None and plan.stop_requested_at is None:
                record = self.records.get(plan.operation_id)
                if record is not None and record.state.final:
                    continue
                try:
                    await self._ensure_forward(plan, None)
                except Refusal as refusal:
                    self.notes[plan.operation_id] = str(refusal)

    # Starting

    async def start_for(self, adapter: Any, spec: ServiceSpec, *, operation_id: str) -> Endpoint:
        """Plan the service, stage its files and submit the wrapped job."""
        existing = self._cached(operation_id)
        if existing is not None and existing.handle is not None:
            return existing.endpoint(
                note="starting", connect_command=self.connect_command(existing)
            )
        site_id = str(adapter.context.site_id)
        loaded = serve_settings.load(self.settings_file)
        entry = self.sites.get(site_id)
        site = serve_settings.site_serve(loaded, site_id, getattr(entry, "serve", None))
        check_wrappable(spec)
        mode = self._mode(adapter, site, loaded.rendezvous is not None)
        ports = plan_ports(spec)
        idle = spec.lifetime.idle_release
        plan = ServePlan(
            operation_id=operation_id,
            site=site_id,
            adapter=str(adapter.name),
            service=service_name(spec),
            mode=mode,
            ports=ports,
            secret_key=new_secret() if mode == "frp" else "",
            secrets={port.secret_env: new_secret() for port in ports if port.secret_env},
            rendezvous=loaded.rendezvous if mode == "frp" else None,
            site_serve=site,
            bound_to=spec.lifetime.bound_to,
            idle_release_s=idle.total_seconds() if idle else None,
        )
        if mode == "ssh":
            plan.ssh_argv = [str(part) for part in await _maybe(adapter.ssh_argv())]
            plan.login_host = (await self._exec(adapter, plan, ["hostname"])).stdout.strip() or None
        if mode == "frp" and site.hop == "login":
            plan.hop = await self._start_hop(adapter, plan)
        self._save(plan)
        try:
            files: FileAccess = await adapter.files()
            staged = {name: await files.stage(data) for name, data in site_files(plan).items()}
            handle: JobHandle = await adapter.run(
                wrapped_job(spec, plan, staged), operation_id=operation_id
            )
        except BaseException:
            await self._stop_hop(adapter, plan)
            plan.final = JobStatus(
                state=JobState.FAILED, message="the service could not be submitted"
            ).model_dump(mode="json")
            self._save(plan)
            raise
        plan.handle = handle
        job_dir = handle.data.get("job_dir")
        plan.job_dir = str(job_dir) if job_dir else None
        self._save(plan)
        return plan.endpoint(note="starting", connect_command=self.connect_command(plan))

    def _mode(self, adapter: Any, site: SiteServe, has_rendezvous: bool) -> Any:
        has_ssh = callable(getattr(adapter, "ssh_argv", None))
        if site.path == "frp" or (site.path == "auto" and has_rendezvous):
            if not has_rendezvous:
                raise Refusal(
                    RefusalReason.INVALID_CONFIG,
                    "no rendezvous host is set; see docs/rendezvous-host.md",
                )
            return "frp"
        if has_ssh:
            return "ssh"
        raise Refusal(
            RefusalReason.INVALID_CONFIG,
            f"site {adapter.context.site_id!r} has no way to carry a port: set a rendezvous host "
            "(docs/rendezvous-host.md), or reach the site through an adapter that uses ssh",
            site=str(adapter.context.site_id),
        )

    async def _exec(
        self, adapter: Any, plan: ServePlan, argv: list[str], *, check: bool = True
    ) -> ExecResult:
        hop_exec: Any = getattr(adapter, "hop_exec", None)
        if hop_exec is not None:
            raw: Any = await hop_exec(plan.site, argv)
        else:
            raw = await adapter.exec(argv, timeout_s=EXEC_TIMEOUT_S, max_output_bytes=EXEC_OUTPUT)
        result = cast(ExecResult, raw)
        if check and result.exit_code != 0:
            detail = (result.stderr or result.stdout).strip().splitlines()
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"{argv[0]} on {plan.site} failed: {detail[-1] if detail else result.exit_code}",
                site=plan.site,
                exit_code=result.exit_code,
            )
        return result

    async def _start_hop(self, adapter: Any, plan: ServePlan) -> HopState:
        rendezvous = plan.rendezvous
        if rendezvous is None:
            raise Refusal(RefusalReason.INVALID_CONFIG, "no rendezvous host is set")
        site = plan.site_serve
        argv = [
            site.python,
            "-c",
            site_file("hop_proxy.py").decode(),
            site.address or rendezvous.address,
            str(site.port or rendezvous.port),
            site.hop_bind,
            site.hop_advertise or "-",
            str(HOP_IDLE_EXIT_S),
            plan.operation_id,
        ]
        result = await self._exec(adapter, plan, argv)
        lines = [line for line in result.stdout.splitlines() if line.strip().startswith("{")]
        try:
            return HopState.model_validate(json.loads(lines[-1]))
        except (IndexError, ValueError) as exc:
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"the login hop on {plan.site} did not start",
                site=plan.site,
            ) from exc

    async def _stop_hop(self, adapter: Any, plan: ServePlan) -> None:
        if plan.hop is None:
            return
        with contextlib.suppress(Exception):
            await self._exec(adapter, plan, ["kill", str(plan.hop.pid)], check=False)

    # Observing

    def _cached(self, operation_id: str) -> ServePlan | None:
        plan = self.plans.get(operation_id)
        if plan is None:
            plan = self.store.get(operation_id)
            if plan is not None:
                self.plans[operation_id] = plan
        return plan

    def _plan(self, operation_id: str) -> ServePlan:
        plan = self._cached(operation_id)
        if plan is None:
            raise Refusal(
                RefusalReason.OPERATION_NOT_FOUND,
                f"no service {operation_id} on this relay host",
                operation_id=operation_id,
            )
        return plan

    def _save(self, plan: ServePlan) -> None:
        self.plans[plan.operation_id] = plan
        self.store.put(plan)

    def _lock(self, operation_id: str) -> asyncio.Lock:
        return self._locks.setdefault(operation_id, asyncio.Lock())

    async def _handle(
        self, plan: ServePlan, adapter: Any, record: OperationRecord
    ) -> JobHandle | None:
        if plan.handle is None:
            plan.handle = record.handle or await adapter.find(plan.operation_id)
            if plan.handle is not None:
                job_dir = plan.handle.data.get("job_dir")
                plan.job_dir = plan.job_dir or (str(job_dir) if job_dir else None)
                self._save(plan)
        return plan.handle

    def _path(self, plan: ServePlan, name: str) -> str:
        if not plan.job_dir:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                f"adapter {plan.adapter} gave no job_dir in its handle; relay cannot read serve/",
            )
        return f"{plan.job_dir.rstrip('/')}/{SERVE_DIR}/{name}"

    async def _read(
        self, plan: ServePlan, adapter: Any, name: str, *, tail: int = 0
    ) -> bytes | None:
        try:
            files: FileAccess = await adapter.files()
            path = self._path(plan, name)
            if tail:
                entry = await files.stat(path)
                offset = max(0, (entry.size or 0) - tail)
                return await files.read(path, offset=offset)
            return await files.read(path)
        except Refusal as refusal:
            if refusal.reason in (RefusalReason.PATH_NOT_FOUND, RefusalReason.INVALID_CONFIG):
                return None
            raise

    def _redact(self, plan: ServePlan, text: str) -> str:
        for secret in [*plan.secrets.values(), plan.secret_key, plan.hop.token if plan.hop else ""]:
            if secret:
                text = text.replace(secret, "***")
        return text

    async def _service_line(self, plan: ServePlan, adapter: Any) -> str | None:
        data = await self._read(plan, adapter, "service.log", tail=LOG_TAIL)
        if not data:
            return None
        lines = [line for line in data.decode(errors="replace").splitlines() if line.strip()]
        return self._redact(plan, lines[-1][-300:]) if lines else None

    async def _frpc_problem(self, plan: ServePlan, adapter: Any) -> str | None:
        """Return frpc's latest warning or error on the site, unless it logged in after it."""
        if plan.mode != "frp":
            return None
        data = await self._read(plan, adapter, "frpc.log", tail=LOG_TAIL)
        if not data:
            return None
        lines = [line for line in data.decode(errors="replace").splitlines() if line.strip()]
        for line in reversed(lines):
            if "login to server success" in line:
                return None
            match = _FRPC_LINE.match(line)
            if match is not None and match.group(1) in ("W", "E"):
                return self._redact(plan, match.group(2).strip()[-300:])
        return None

    async def _running(
        self, plan: ServePlan, adapter: Any, status: JobStatus
    ) -> tuple[JobStatus, Endpoint | None]:
        if plan.ready_at is None:
            raw = await self._read(plan, adapter, "ready.json")
            if raw is None:
                line = await self._service_line(plan, adapter)
                problem = await self._frpc_problem(plan, adapter)
                message = "starting: waiting for the service's ports to answer"
                if plan.mode == "frp":
                    message += " and for frpc to register them"
                if line:
                    message += f". Last service output: {line}"
                if problem:
                    message += f". frpc on the site, retrying: {problem}"
                return status.model_copy(update={"message": message}), None
            with contextlib.suppress(ValueError):
                ready: dict[str, Any] = json.loads(raw)
                plan.host = str(ready.get("host") or "") or None
            plan.ready_at = utc_now()
            self._save(plan)
        forward = await self._ensure_forward(plan, adapter)
        if not forward.bound():
            message = "the service is up; binding the desktop port"
            error = forward.last_error()
            if error:
                message += f". Forward: {self._redact(plan, error)}"
            return status.model_copy(update={"message": message}), None
        notes: list[str] = []
        mount_note = await self._mount(plan)
        if mount_note:
            notes.append(mount_note)
        if plan.rebinds:
            notes.append(f"desktop port bound again {plan.rebinds} times")
        extra = self.notes.get(plan.operation_id)
        if extra:
            notes.append(extra)
        message = "serving" + (f": {'; '.join(notes)}" if notes else "")
        endpoint = plan.endpoint(note=message, connect_command=self.connect_command(plan))
        return status.model_copy(update={"message": message}), endpoint

    async def _ensure_forward(self, plan: ServePlan, adapter: Any) -> Forward:
        forward = self.forwards.get(plan.operation_id)
        if forward is None:

            def restarted(count: int, pid: int | None) -> None:
                plan.rebinds = count
                plan.forward_pid = pid
                self._save(plan)

            forward = Forward(
                plan, self.state_dir / "serve" / "forwards" / plan.operation_id, restarted
            )
            self.forwards[plan.operation_id] = forward
        if adapter is not None and plan.mode == "ssh":
            transport = getattr(adapter, "transport", None)
            environment = getattr(transport, "environment", None)
            if callable(environment):
                forward.ssh_env = dict(await _maybe(environment()) or {})
        await forward.start()
        return forward

    async def _mount(self, plan: ServePlan) -> str | None:
        port = next((item for item in plan.ports if item.protocol == "mcp"), None)
        if port is None:
            return None
        url = port.url()
        if url is None:
            return None
        if self.mounter.server is None:
            return f"MCP server at {url}"
        if self.mounter.mounted(plan.operation_id) is not None:
            return f"tools mounted under {tool_prefix(plan.site, plan.service)}"
        secret = plan.secrets.get(port.secret_env) if port.secret_env else None
        try:
            mount = await self.mounter.mount(
                plan.operation_id, tool_prefix(plan.site, plan.service), url, secret=secret
            )
        except Exception as exc:
            return f"mounting its tools: {self._redact(plan, str(exc))[:200]}"
        return f"{len(mount.tools)} tools mounted under {mount.prefix}"

    async def _follow_binding(self, plan: ServePlan, adapter: Any) -> None:
        if not plan.bound_to or plan.stop_requested_at is not None:
            return
        bound = self.records.get(plan.bound_to)
        if bound is None or not bound.state.final:
            return
        plan.stop_requested_at = utc_now()
        self.notes[plan.operation_id] = f"stopping: the job it is bound to ({plan.bound_to}) ended"
        self._save(plan)
        await self._close_desktop(plan)
        if plan.handle is not None:
            await self._request_stop(plan, adapter, plan.handle)

    # Stopping

    async def _close_desktop(self, plan: ServePlan) -> None:
        forward = self.forwards.pop(plan.operation_id, None)
        if forward is not None:
            await forward.close()
        await self.mounter.unmount(plan.operation_id)

    async def _request_stop(self, plan: ServePlan, adapter: Any, handle: JobHandle) -> None:
        status = await adapter.status(handle)
        if status.state.final:
            return
        if status.state == JobState.RUNNING and plan.job_dir:
            try:
                files: FileAccess = await adapter.files()
                await files.write(self._path(plan, "stop"), b"stop\n", overwrite=True)
                return
            except Refusal as refusal:
                logger.info("service %s: no stop file (%s); cancelling", plan.operation_id, refusal)
        plan.cancel_sent = True
        self._save(plan)
        with contextlib.suppress(Refusal):
            await adapter.cancel(handle)

    async def _finish(self, plan: ServePlan, adapter: Any, status: JobStatus) -> JobStatus:
        await self._close_desktop(plan)
        stopped: dict[str, Any] = {}
        raw = await self._read(plan, adapter, "stopped.json")
        if raw:
            with contextlib.suppress(ValueError):
                stopped = json.loads(raw)
        outputs = [item for item in status.outputs if _is_artifact(item)]
        seen = {item.path for item in outputs}
        listing = await self._read(plan, adapter, "artifacts.txt")
        if listing and plan.job_dir:
            for line in listing.decode(errors="replace").splitlines():
                size_text, _, name = line.partition("\t")
                name = name.strip()
                if not name:
                    continue
                path = f"{plan.job_dir.rstrip('/')}/{name}"
                if path in seen:
                    continue
                seen.add(path)
                size = int(size_text) if size_text.strip().isdigit() else None
                outputs.append(OutputRef(name=name, path=path, size=size))
        await self._stop_hop(adapter, plan)
        message = self._final_message(plan, status, stopped)
        final = status.model_copy(update={"outputs": outputs, "message": message})
        plan.final = final.model_dump(mode="json")
        self._save(plan)
        return final

    def _final_message(self, plan: ServePlan, status: JobStatus, stopped: dict[str, Any]) -> str:
        reason = str(stopped.get("reason") or "")
        parts: list[str] = []
        if reason == "stopped":
            parts.append("stopped")
        elif reason == "idle":
            parts.append("released after its idle time")
        elif reason == "port_taken":
            node = stopped.get("host") or plan.host or "the node"
            port = stopped.get("port")
            named = f"port {port} ({stopped.get('port_name')})" if port else "a port it needs"
            parts.append(f"refused: {named} was already in use on {node}")
        elif reason == "exited":
            parts.append(f"the service exited with {stopped.get('exit_code')}")
        elif reason == "signal":
            parts.append("the job was signalled")
        elif status.message:
            parts.append(self._redact(plan, status.message))
        else:
            parts.append(f"the job is {status.state.value}")
        if plan.cancel_sent:
            parts.append("its job was cancelled")
        restarts = stopped.get("frpc_restarts")
        if restarts:
            parts.append(f"the site's frp client restarted {restarts} times")
        if plan.rebinds:
            parts.append(f"the desktop port was bound again {plan.rebinds} times")
        return "; ".join(parts)

    # Reaching the service from another machine

    def connect_command(self, plan: ServePlan) -> str | None:
        """Return ``clio-relay connect <handle>`` when relay runs away from the desktop."""
        if self.transport != "http" or plan.mode != "frp" or plan.rendezvous is None:
            return None
        handle = connect_handle(plan)
        return shlex.join(["clio-relay", "connect", handle])


def connect_handle(plan: ServePlan) -> str:
    """Encode what a desktop needs to reach the service through the rendezvous host.

    It carries the proxies' shared key, not the rendezvous token: the desktop
    reads the token from its own rendezvous setting.
    """
    rendezvous = plan.rendezvous
    payload = {
        "v": 1,
        "op": plan.operation_id,
        "sk": plan.secret_key,
        "direct": bool(rendezvous and rendezvous.direct),
        "ports": [[port.name, port.protocol, port.local_port] for port in plan.ports],
    }
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


# The wrapper's own files. Logs stay, because a person reads them after a failure.
_SERVE_FILES = {"service.log", "launch.log", "frpc.log"}


def _is_artifact(item: OutputRef) -> bool:
    """Return whether an adapter-listed output is the service's, not the wrapper's."""
    parts = item.name.replace("\\", "/").split("/")
    if "serve" in parts[:-1]:
        return parts[-1] in _SERVE_FILES
    return True
