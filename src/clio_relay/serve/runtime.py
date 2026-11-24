"""The service runtime behind W1's ``ServeHook`` (RL19 to RL23).

Adapters start a service with :func:`start_for`. Relay's surface drives the
rest through the hook: ``status`` brings the desktop port up once the service
answers, mounts a served MCP server, and follows the job it is bound to;
``stop`` closes the desktop port at once, gives the service its grace period
and returns the artifacts it wrote.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import shlex
import time
from pathlib import Path
from typing import Any, cast

from clio_relay.adapters.hooks import service_choices as choices_of
from clio_relay.jobs.handles import (
    Endpoint,
    ExecResult,
    FileAccess,
    JobHandle,
    JobState,
    JobStatus,
    utc_now,
)
from clio_relay.jobs.records import OperationRecord, RecordStore
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, ServiceSpec
from clio_relay.login_load import light_argv
from clio_relay.serve import config as serve_settings
from clio_relay.serve import planning
from clio_relay.serve.forward import CLOSED_MARKER, Forward, forward_marker, kill_recorded
from clio_relay.serve.keys import ForwardCare
from clio_relay.serve.mount import Mounter, tool_prefix
from clio_relay.serve.plan import (
    HopState,
    ServePlan,
    adopt_ports,
    new_secret,
    package_service_job,
    port_choices,
    ports_file,
    runtime_endpoints,
    runtime_ready_host,
    service_name,
    site_file,
    site_files,
    site_frpc_config,
    wrapped_job,
)
from clio_relay.serve.provenance import with_provenance
from clio_relay.serve.report import (
    LOG_TAIL,
    connect_handle,
    final_message,
    frpc_problem,
    handle_job_dir,
    is_artifact,
    job_path,
    last_line,
    listed_artifacts,
    redact,
    times,
)
from clio_relay.serve.runtime_util import (
    maybe,
    scheduler_job_name,
    site_id_of,
    state_dir_of,
    tunnel_entry,
)
from clio_relay.serve.store import PlanStore

logger = logging.getLogger(__name__)

HOP_IDLE_EXIT_S = 3600
EXEC_TIMEOUT_S = 120.0
EXEC_OUTPUT = 65536
STOP_MARGIN_S = 30.0
CANCEL_RETRY_S = 60.0
FOLLOW_FIRST_S = 1.0
FOLLOW_MAX_S = 5.0
FRESH_S = 60.0
"""How old the follower's latest look may be for a status call to answer with it."""
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


async def start_for(adapter: Any, spec: ServiceSpec, *, operation_id: str) -> Endpoint:
    """Start ``spec`` as a job on ``adapter``'s site. An adapter's ``serve`` calls this."""
    global _runtime
    if _runtime is None:
        _runtime = ServeRuntime(state_dir_of(adapter))
    return await _runtime.start_for(adapter, spec, operation_id=operation_id)


class ServeRuntime(ForwardCare):
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
        self._followers: dict[str, asyncio.Task[None]] = {}
        self._latest: dict[str, tuple[float, JobStatus]] = {}
        self._adapters: dict[str, Any] = {}
        self._choices: dict[str, dict[str, Any]] = {}
        self._resumed = False

    # The hook

    async def start(
        self, site: Any, adapter: Any, spec: ServiceSpec, record: OperationRecord
    ) -> Endpoint | None:
        """Start a service through the adapter's ``serve`` verb."""
        self.sites[site_id_of(site)] = site
        choices = choices_of(record)
        if choices is not None:
            self._choices[record.operation_id] = choices
        try:
            await adapter.serve(spec, operation_id=record.operation_id)
        except Refusal as refusal:
            if refusal.reason != RefusalReason.VERB_NOT_IMPLEMENTED:
                raise
            await self.start_for(adapter, spec, operation_id=record.operation_id)
        finally:
            self._choices.pop(record.operation_id, None)
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
        self._adapters[plan.operation_id] = adapter
        seen = self._following(plan)
        if seen is not None:
            return seen, None
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
            if plan.stop_requested_at is not None:
                note = (
                    self.notes.get(plan.operation_id) or "stopping: waiting for the service to end"
                )
                return status.model_copy(update={"message": note}), None
            if status.state != JobState.RUNNING:
                return status, None
            if await self._idle(plan, adapter, handle):
                return status.model_copy(update={"message": self.notes[plan.operation_id]}), None
            status, endpoint = await self._running(plan, adapter, status)
        if endpoint is None:
            self._follow(plan, adapter, handle, status)
        return status, endpoint

    async def stop(self, site: Any, adapter: Any, record: OperationRecord) -> JobStatus:
        """Close the desktop port, then stop the service with its grace period."""
        await self.resume()
        plan = self._plan(record.operation_id)
        if plan.final is not None:
            return JobStatus.model_validate(plan.final)
        follower = self._followers.pop(plan.operation_id, None)
        if follower is not None:
            follower.cancel()
        # Under the lock, so a status call in flight cannot bind or mount it again.
        async with self._lock(plan.operation_id):
            if plan.final is not None:
                return JobStatus.model_validate(plan.final)
            if plan.stop_requested_at is None:
                plan.stop_requested_at = utc_now()
                self._save(plan)
            handle = await self._handle(plan, adapter, record)
            await self._close_desktop(plan)
            if handle is None:
                final = JobStatus(
                    state=JobState.CANCELLED, message="stopped before it was submitted"
                )
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
        """Release the mounted sessions. Services and their desktop ports keep running.

        A host may restart relay's MCP server while a service runs; the next relay
        process adopts the forwards left up, at its start (``resume``)."""
        for follower in list(self._followers.values()):
            follower.cancel()
        self._followers.clear()
        for forward in list(self.forwards.values()):
            await forward.release()
        self.forwards.clear()
        await self.mounter.close()

    async def shutdown_site(self, site_id: str, adapter: Any | None) -> list[dict[str, Any]]:
        """Close the site's desktop ports and stop its login hop. Services keep running.

        It closes this process's forwards, and those another relay process recorded and
        left, and keeps them closed. The hop is stopped when a service still uses it. It
        returns one ``{kind, pid?, detail}`` per process stopped. A hop that will not
        stop raises a refusal, after the ports are closed.
        """
        stopped: list[dict[str, Any]] = []
        plans = {plan.operation_id: plan for plan in self.store.all() if plan.site == site_id}
        plans.update({op: plan for op, plan in self.plans.items() if plan.site == site_id})
        hop_plan: ServePlan | None = None
        for op, plan in plans.items():
            if plan.hop is not None and plan.final is None:
                hop_plan = plan
            async with self._lock(op):
                if plan.final is None and plan.desktop_closed_at is None:
                    plan.desktop_closed_at = utc_now()
                    self._save(plan)
                directory = self.state_dir / "serve" / "forwards" / op
                with contextlib.suppress(OSError):
                    directory.mkdir(parents=True, exist_ok=True)
                    (directory / CLOSED_MARKER).touch()
                forward = self.forwards.pop(op, None)
                await self.mounter.unmount(op)
                if forward is not None:
                    held = forward.alive() or forward.bound()
                    pid = forward.pid()
                    await forward.close()
                    if held:
                        stopped.append(tunnel_entry(op, plan, pid))
                    continue
                marker = forward_marker(plan, directory)
                if plan.final is None and plan.ports and kill_recorded(plan.forward_pid, marker):
                    stopped.append(tunnel_entry(op, plan, plan.forward_pid))
        if adapter is None or hop_plan is None:
            return stopped
        site = hop_plan.site_serve
        hop_dir = serve_settings.hop_dir_for(site, adapter, site_id)
        argv = [site.python, "-c", site_file("hop_proxy.py").decode(), "shutdown", hop_dir, "-"]
        run: Any = getattr(adapter, "hop_exec", None) or adapter.exec
        result = cast(
            ExecResult,
            await run(light_argv(argv, site.login_limits), timeout_s=20.0, max_output_bytes=4096),
        )
        lines = [line for line in result.stdout.splitlines() if line.strip().startswith("{")]
        if result.exit_code != 0 or not lines:
            detail = (result.stderr or result.stdout).strip().splitlines()
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"stopping the login hop on {site_id} failed: "
                f"{detail[-1] if detail else result.exit_code}",
                site=site_id,
            )
        report = json.loads(lines[-1])
        if report.get("running"):
            hop: dict[str, Any] = {"kind": "hop_proxy", "detail": f"login hop in {hop_dir}"}
            if isinstance(report.get("pid"), int) and report["pid"]:
                hop["pid"] = report["pid"]
            stopped.append(hop)
        return stopped

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
        """At relay's start: adopt the desktop ports left up for ready services, or bind them."""
        if self._resumed:
            return
        self._resumed = True
        for plan in self.store.all():
            self.plans.setdefault(plan.operation_id, plan)
            if (
                plan.final is None
                and plan.ready_at is not None
                and plan.stop_requested_at is None
                and plan.desktop_closed_at is None
                and plan.forward_error is None
            ):
                record = self.records.get(plan.operation_id)
                if record is not None and record.state.final:
                    continue
                try:
                    async with self._lock(plan.operation_id):
                        await self._ensure_forward(plan, None)
                except Refusal as refusal:
                    self.notes[plan.operation_id] = str(refusal)

    # Starting

    async def plan_job(
        self, site: Any, adapter: Any, spec: ServiceSpec, record: OperationRecord
    ) -> tuple[JobSpec, dict[str, Any]] | None:
        """The package job a start of ``spec`` submits, for JARVIS to plan, and what it keeps.

        It is :func:`package_service_job` with the connector, its settings and the
        service's environment, as the start builds it. The start stages the
        ``serve/`` files, the secrets among them, as inputs, which JARVIS does not
        seal, so none is made here. The start keeps the returned port choices
        (``record.result[SERVICE_CHOICES]``), so its job is this one. A service
        that is not a package job has no JARVIS plan (``None``).
        """
        self.sites[site_id_of(site)] = site
        plan = planning.serve_plan(
            adapter,
            spec,
            operation_id=record.operation_id,
            settings_file=self.settings_file,
            entry=self.sites.get(str(adapter.context.site_id)),
            choices=choices_of(record),
        )
        if not plan.package:
            return None
        return package_service_job(spec, plan, {}), port_choices(plan)

    async def start_for(self, adapter: Any, spec: ServiceSpec, *, operation_id: str) -> Endpoint:
        """Plan the service, stage its files and submit the wrapped job."""
        existing = self._cached(operation_id)
        if existing is not None and existing.handle is not None:
            return existing.endpoint(
                note="starting", connect_command=self.connect_command(existing)
            )
        plan = planning.serve_plan(
            adapter,
            spec,
            operation_id=operation_id,
            settings_file=self.settings_file,
            entry=self.sites.get(str(adapter.context.site_id)),
            choices=self._choices.pop(operation_id, None),
        )
        site_id, mode, ports, package = plan.site, plan.mode, plan.ports, plan.package
        if any(port.protocol == "mcp" for port in ports):
            self._refuse_a_second_mount(site_id, service_name(spec), operation_id)
        plan.secret_key = new_secret() if mode == "frp" else ""
        plan.secrets = {port.secret_env: new_secret() for port in ports if port.secret_env}
        if mode == "ssh":
            plan.ssh_argv = [str(part) for part in await maybe(adapter.ssh_argv())]
            plan.login_host = (await self._exec(adapter, plan, ["hostname"])).stdout.strip() or None
        if mode == "frp" and plan.site_serve.hop == "login":
            plan.hop = await self._start_hop(adapter, plan)
        self._save(plan)
        try:
            files: FileAccess = await adapter.files()
            staged = {name: await files.stage(data) for name, data in site_files(plan).items()}
            build = package_service_job if package else wrapped_job
            handle: JobHandle = await adapter.run(
                build(spec, plan, staged), operation_id=operation_id
            )
        except BaseException:
            await self._stop_hop(adapter, plan)
            plan.final = JobStatus(
                state=JobState.FAILED, message="the service could not be submitted"
            ).model_dump(mode="json")
            self._save(plan)
            raise
        plan.handle = handle
        plan.job_dir = handle_job_dir(handle)
        self._save(plan)
        self._record_handle(plan)
        await self._lease_hop(adapter, plan)
        return plan.endpoint(note="starting", connect_command=self.connect_command(plan))

    def _refuse_a_second_mount(self, site_id: str, service: str, operation_id: str) -> None:
        """Refuse a served MCP server whose tool names a live one on this host already uses."""
        for other in self.store.all():
            if other.operation_id == operation_id or other.final is not None:
                continue
            if other.stop_requested_at is not None or (other.site, other.service) != (
                site_id,
                service,
            ):
                continue
            if not any(port.protocol == "mcp" for port in other.ports):
                continue
            record = self.records.get(other.operation_id)
            if record is not None and record.state.final:
                continue
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                f"{other.operation_id} already serves an MCP server named {service!r} on "
                f"{site_id}, and its tools are mounted as {tool_prefix(site_id, service)}*. "
                "Stop it, or give this job another name",
                site=site_id,
                operation_id=other.operation_id,
            )

    async def _exec(
        self, adapter: Any, plan: ServePlan, argv: list[str], *, check: bool = True
    ) -> ExecResult:
        run: Any = getattr(adapter, "hop_exec", None) or adapter.exec
        raw: Any = await run(argv, timeout_s=EXEC_TIMEOUT_S, max_output_bytes=EXEC_OUTPUT)
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

    async def _hop(self, adapter: Any, plan: ServePlan, mode: str, *args: str) -> ExecResult:
        """Run the login hop's ``mode`` on the site, light (:mod:`clio_relay.login_load`)."""
        site = plan.site_serve
        hop_dir = serve_settings.hop_dir_for(site, adapter, plan.site)
        argv = [
            site.python,
            "-c",
            site_file("hop_proxy.py").decode(),
            mode,
            hop_dir,
            plan.operation_id,
            *args,
        ]
        return await self._exec(adapter, plan, light_argv(argv, site.login_limits))

    async def _start_hop(self, adapter: Any, plan: ServePlan) -> HopState:
        rendezvous = plan.rendezvous
        if rendezvous is None:
            raise Refusal(RefusalReason.INVALID_CONFIG, "no rendezvous host is set")
        site = plan.site_serve
        result = await self._hop(
            adapter,
            plan,
            "start",
            site.address or rendezvous.address,
            str(site.port or rendezvous.port),
            site.hop_bind,
            site.hop_advertise or "-",
            str(HOP_IDLE_EXIT_S),
        )
        lines = [line for line in result.stdout.splitlines() if line.strip().startswith("{")]
        try:
            return HopState.model_validate(json.loads(lines[-1]))
        except (IndexError, ValueError) as exc:
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"the login hop on {plan.site} did not start",
                site=plan.site,
            ) from exc

    async def _lease_hop(self, adapter: Any, plan: ServePlan) -> None:
        """Tie the hop's lease to the submitted job, so the hop ends when the job does."""
        if plan.hop is None or not plan.job_dir:
            return
        try:
            await self._hop(
                adapter, plan, "lease", plan.job_dir, scheduler_job_name(plan.operation_id)
            )
        except Refusal as refusal:
            # The lease still ends SUBMIT_GRACE_S after the start without a job.
            logger.info("service %s: hop lease not tied (%s)", plan.operation_id, refusal)

    async def _stop_hop(self, adapter: Any, plan: ServePlan) -> None:
        """Release this job's lease. The hop exits once no job holds one."""
        if plan.hop is None:
            return
        with contextlib.suppress(Exception):
            await self._hop(adapter, plan, "release")

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
                self._save(plan)
        if plan.handle is not None and plan.job_dir is None:
            plan.job_dir = handle_job_dir(plan.handle)
            if plan.job_dir is not None:
                self._save(plan)
                await self._lease_hop(adapter, plan)
        if plan.handle is not None and record.handle is None:
            record.handle = plan.handle
            self._record_handle(plan)
        return plan.handle

    def _record_handle(self, plan: ServePlan) -> None:
        """Put the job's handle on the operation record, so its native id is reported."""
        if plan.handle is None:
            return
        with self.records.locked(plan.operation_id):
            record = self.records.get(plan.operation_id)
            if record is None or record.handle is not None:
                return
            record.handle = plan.handle
            self.records.put(plan.operation_id, record)

    async def _read(
        self, plan: ServePlan, adapter: Any, name: str, *, tail: int = 0
    ) -> bytes | None:
        try:
            files: FileAccess = await adapter.files()
            path = job_path(plan, name)
            if tail:
                entry = await files.stat(path)
                offset = max(0, (entry.size or 0) - tail)
                return await files.read(path, offset=offset)
            return await files.read(path)
        except Refusal as refusal:
            if refusal.reason in (RefusalReason.PATH_NOT_FOUND, RefusalReason.INVALID_CONFIG):
                return None
            raise

    async def _frpc_problem(self, plan: ServePlan, adapter: Any) -> str | None:
        if plan.mode != "frp":
            return None
        return frpc_problem(plan, await self._read(plan, adapter, "frpc.log", tail=LOG_TAIL))

    async def _adopt_reported_ports(self, plan: ServePlan, adapter: Any) -> tuple[bool, str | None]:
        """Take the ports JARVIS recorded for the package service.

        Returns whether a port is still unknown, and the host JARVIS says the
        service is ready on. The package binds its own port and reports it in
        its ready event; relay forwards that port and never injects one. A
        changed port rewrites the site's frpc settings first, then
        ``serve/ports``, which the connector rereads.
        """
        if not plan.package or plan.handle is None:
            return False, None
        if not getattr(adapter, "reads_service_runtimes", False):
            return False, None
        try:
            document = await adapter.service_runtimes(plan.handle)
        except Refusal as refusal:
            logger.info("serve %s: service runtimes unread: %s", plan.operation_id, refusal)
            document = None
        if adopt_ports(plan, runtime_endpoints(document)):
            self._save(plan)
            files: FileAccess = await adapter.files()
            if plan.mode == "frp":
                await files.write(
                    job_path(plan, "frpc.toml"), site_frpc_config(plan), overwrite=True
                )
            await files.write(job_path(plan, "ports"), ports_file(plan), overwrite=True)
        unknown = any(port.remote_port == 0 for port in plan.ports)
        return unknown, None if unknown else runtime_ready_host(document, plan)

    async def _running(
        self, plan: ServePlan, adapter: Any, status: JobStatus
    ) -> tuple[JobStatus, Endpoint | None]:
        if plan.ready_at is None:
            unknown, ready_host = await self._adopt_reported_ports(plan, adapter)
            if ready_host is not None and plan.mode == "ssh":
                # JARVIS saw the package's ready event, so the forward can go to
                # it now; frp still waits for the connector to register the ports.
                raw = json.dumps({"host": ready_host}).encode()
            else:
                raw = await self._read(plan, adapter, "ready.json")
            if raw is None:
                log = await self._read(plan, adapter, "service.log", tail=LOG_TAIL)
                line = last_line(plan, log)
                problem = await self._frpc_problem(plan, adapter)
                message = "starting: waiting for the service's ports to answer"
                if unknown:
                    message = "starting: waiting for JARVIS to report the port the service bound"
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
            self._save(plan)
            blocked = await self._keys_blocked(plan, adapter, status)
            if blocked is not None:
                return blocked, None
            plan.ready_at = utc_now()
            self._save(plan)
        if plan.desktop_closed_at is not None:
            message = "running; relay's desktop ports were closed by shutdown"
            return status.model_copy(update={"message": message}), None
        blocked = await self._keys_blocked(plan, adapter, status)
        if blocked is not None:
            return blocked, None
        forward = await self._ensure_forward(plan, adapter)
        blocked = await self._forward_blocked(plan, adapter, forward, status)
        if blocked is not None:
            return blocked, None
        if not forward.serving():
            message = "the service is up; binding the desktop port"
            error = forward.last_error()
            if error:
                message += f". Forward: {redact(plan, error)}"
            return status.model_copy(update={"message": message}), None
        notes: list[str] = []
        mount_note = await self._mount(plan)
        if mount_note:
            notes.append(mount_note)
        if plan.rebinds:
            notes.append(f"desktop port bound again {times(plan.rebinds)}")
        direct = forward.direct_note()
        if direct:
            notes.append(redact(plan, direct))
        extra = self.notes.get(plan.operation_id)
        if extra:
            notes.append(extra)
        message = "serving" + (f": {'; '.join(notes)}" if notes else "")
        endpoint = plan.endpoint(note=message, connect_command=self.connect_command(plan))
        return status.model_copy(update={"message": message}), endpoint

    def _following(self, plan: ServePlan) -> JobStatus | None:
        """The follower's fresh latest look at a service coming up, so a task poll does not
        wait a whole round of site calls for its lock and see readiness a round late."""
        follower = self._followers.get(plan.operation_id)
        if follower is None or follower.done():
            return None
        stopping = plan.final is not None or plan.stop_requested_at
        if stopping or plan.ready_at is not None or plan.forward_error is not None:
            return None
        seen = self._latest.get(plan.operation_id)
        if seen is None or time.monotonic() - seen[0] > FRESH_S:
            return None
        return seen[1]

    def _follow(self, plan: ServePlan, adapter: Any, handle: JobHandle, status: JobStatus) -> None:
        """Keep bringing a running service up until it is served, with no client poll.

        A client may stop polling once the task says the service is running and wait
        for ``tools/list_changed`` instead; this binds the desktop port and mounts an
        MCP server for it.
        """
        if plan.operation_id in self._followers:
            return
        self._latest[plan.operation_id] = (time.monotonic(), status)

        async def follow() -> None:
            delay = FOLLOW_FIRST_S
            try:
                while plan.final is None and plan.stop_requested_at is None:
                    await asyncio.sleep(delay)
                    delay = min(delay * 1.5, FOLLOW_MAX_S)
                    try:
                        async with self._lock(plan.operation_id):
                            if plan.final is not None or plan.stop_requested_at is not None:
                                return
                            current = await adapter.status(handle)
                            if current.state != JobState.RUNNING:
                                return
                            seen, endpoint = await self._running(plan, adapter, current)
                            self._latest[plan.operation_id] = (time.monotonic(), seen)
                        if endpoint is not None or plan.forward_error is not None:
                            return
                    except Exception:
                        logger.warning(
                            "service %s: bringing it up, retrying in %.0f s",
                            plan.operation_id,
                            delay,
                            exc_info=True,
                        )
            finally:
                self._followers.pop(plan.operation_id, None)
                self._latest.pop(plan.operation_id, None)

        with contextlib.suppress(RuntimeError):
            self._followers[plan.operation_id] = asyncio.get_running_loop().create_task(follow())

    async def _ensure_forward(self, plan: ServePlan, adapter: Any) -> Forward:
        forward = self.forwards.get(plan.operation_id)
        if forward is None:

            def restarted(count: int, pid: int | None) -> None:
                plan.rebinds = count
                plan.forward_pid = pid
                self._save(plan)

            directory = self.state_dir / "serve" / "forwards" / plan.operation_id
            failed = self._failure_callback(plan.operation_id)
            forward = Forward(plan, directory, restarted, on_failure=failed)
            self.forwards[plan.operation_id] = forward
        if adapter is not None and plan.mode == "ssh":
            transport = getattr(adapter, "transport", None)
            environment = getattr(transport, "environment", None)
            if callable(environment):
                forward.ssh_env = dict(await maybe(environment()) or {})
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
            return f"mounting its tools: {redact(plan, str(exc))[:200]}"
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
        elif plan.forward_pid and plan.ports:
            # A forward an earlier relay process left up, for a service that has ended.
            directory = self.state_dir / "serve" / "forwards" / plan.operation_id
            kill_recorded(plan.forward_pid, forward_marker(plan, directory))
        await self.mounter.unmount(plan.operation_id)

    async def _idle(self, plan: ServePlan, adapter: Any, handle: JobHandle) -> bool:
        """For a package service: stop it once the connector wrote ``serve/idle``."""
        if not plan.package or not plan.idle_release_s or plan.ready_at is None:
            return False
        if await self._read(plan, adapter, "idle") is None:
            return False
        plan.stop_requested_at = utc_now()
        self.notes[plan.operation_id] = "stopping: released after its idle time"
        self._save(plan)
        await self._close_desktop(plan)
        await self._request_stop(plan, adapter, handle)
        return True

    async def _request_stop(self, plan: ServePlan, adapter: Any, handle: JobHandle) -> None:
        status = await adapter.status(handle)
        if status.state.final:
            return
        if plan.package:
            # JARVIS started the service: jarvis execution stop runs ppl stop and finalizes.
            try:
                await adapter.stop(handle)
                return
            except Refusal as refusal:
                logger.info("service %s: stop refused (%s); cancelling", plan.operation_id, refusal)
        elif status.state == JobState.RUNNING and plan.job_dir:
            try:
                files: FileAccess = await adapter.files()
                await files.write(job_path(plan, "stop"), b"stop\n", overwrite=True)
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
        outputs = [item for item in status.outputs if is_artifact(item)]
        listing = await self._read(plan, adapter, "artifacts.txt")
        outputs += listed_artifacts(plan, listing, {item.path for item in outputs})
        record = self.records.get(plan.operation_id)
        if record is not None:
            if record.handle is None and plan.handle is not None:
                self._record_handle(plan)
                record.handle = plan.handle
            outputs = with_provenance(record, outputs)
        await self._stop_hop(adapter, plan)
        message = final_message(plan, status, stopped)
        final = status.model_copy(
            update={"outputs": outputs, "message": message, "service_error": plan.forward_error}
        )
        plan.final = final.model_dump(mode="json")
        self._save(plan)
        return final

    # Reaching the service from another machine

    def connect_command(self, plan: ServePlan) -> str | None:
        """Return ``clio-relay connect <handle>`` when relay runs away from the desktop."""
        if self.transport != "http" or plan.mode != "frp" or plan.rendezvous is None:
            return None
        handle = connect_handle(plan)
        return shlex.join(["clio-relay", "connect", handle])
