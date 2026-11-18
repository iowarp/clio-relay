"""The ``ssh`` adapter (RL10, RL16 to RL18): a site reached with the system ``ssh``.

Jobs go to the site's batch scheduler through ``psij-python``. Relay renders
the submit script here with PSI/J, copies it and any missing inputs with one
``scp``, and submits it with one ``ssh``. Status, cancel and the job id parser
are PSI/J's too, run on the site. With no scheduler, a small ``sh`` supervisor
runs the job detached and writes its exit code to the job directory.

The job record lives in the job directory on the site (RL17), so a poll from
any relay process, after any restart, reads it. No ``ssh`` process is kept
between calls.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import io
import json
import logging
import shlex
import shutil
import threading
from datetime import timedelta
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, ClassVar, cast

from pydantic import BaseModel

from clio_relay.adapters.base import AdapterContext, BaseAdapter, SignInNeed, SiteInfo, Verb
from clio_relay.adapters.hooks import Charge
from clio_relay.adapters.ssh import pricing, scripts
from clio_relay.adapters.ssh.config import SchedulerName, SshConfig
from clio_relay.adapters.ssh.files import SshFileAccess
from clio_relay.adapters.ssh.scripts import parse_sections
from clio_relay.adapters.ssh.transport import SshTransport
from clio_relay.jobs.handles import (
    Endpoint,
    ExecResult,
    FileAccess,
    JobHandle,
    JobState,
    JobStatus,
    OutputRef,
    StallReport,
)
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, ServiceSpec

if TYPE_CHECKING:
    from psij import Job
    from psij.executors.batch.batch_scheduler_executor import BatchSchedulerExecutor

logger = logging.getLogger(__name__)

_PSIJ_STATES = {
    "NEW": JobState.QUEUED,
    "QUEUED": JobState.QUEUED,
    "ACTIVE": JobState.RUNNING,
    "COMPLETED": JobState.COMPLETED,
    "FAILED": JobState.FAILED,
    "CANCELED": JobState.CANCELLED,
}

_SCHEDULERS: frozenset[str] = frozenset({"slurm", "pbs", "lsf", "cobalt"})

_executors: dict[tuple[str, str], BatchSchedulerExecutor] = {}
_executors_lock = threading.Lock()


def _executor(scheduler: SchedulerName, work_dir: Path) -> BatchSchedulerExecutor:
    """Return PSI/J's executor for ``scheduler``. It only renders and parses here."""
    from psij import JobExecutor
    from psij.executors.batch.batch_scheduler_executor import (
        BatchSchedulerExecutor,
        BatchSchedulerExecutorConfig,
    )

    key = (scheduler, str(work_dir))
    with _executors_lock:
        found = _executors.get(key)
        if found is None:
            config = BatchSchedulerExecutorConfig(work_directory=work_dir)
            created = JobExecutor.get_instance(scheduler, config=config)
            if not isinstance(created, BatchSchedulerExecutor):
                raise Refusal(
                    RefusalReason.ADAPTER_UNAVAILABLE,
                    f"psij executor {scheduler!r} is not a batch scheduler executor",
                )
            created.work_directory.mkdir(parents=True, exist_ok=True)
            _executors[key] = found = created
        return found


class _SitePath(PurePosixPath):
    """A path on the site. PSI/J calls ``absolute()``, which must not touch this machine."""

    def absolute(self) -> _SitePath:
        return self


def _remote_path(base: PurePosixPath, value: str | None) -> PurePosixPath | None:
    if value is None:
        return None
    path = PurePosixPath(value.replace("\\", "/"))
    return path if path.is_absolute() else base / path


def job_name(operation_id: str) -> str:
    """Return the scheduler job name relay gives an operation, for reconciling."""
    return f"relay-{operation_id}"


def walltime(spec: JobSpec) -> timedelta | None:
    """Return the walltime the spec asks for, or None when it gives none.

    The duration comes from the caller or from the site's ``defaults.duration``.
    With none, no walltime is rendered and the scheduler's default applies.
    """
    return spec.attributes.duration


class _Rendered:
    """What a submit needs: files to copy, and the command that submits."""

    def __init__(self) -> None:
        self.files: dict[str, str] = {}
        self.launcher_dir: str | None = None
        self.launcher_files: list[Path] = []
        self.submit_command = ""


class SshAdapter(BaseAdapter[SshConfig]):
    """A site reached with the person's own ``ssh``."""

    name: ClassVar[str] = "ssh"
    Config: ClassVar[type[BaseModel]] = SshConfig
    implements: ClassVar[frozenset[Verb]] = frozenset({"describe", "run", "exec", "files", "serve"})

    def __init__(self, config: SshConfig, context: AdapterContext) -> None:
        super().__init__(config, context)
        self.transport = SshTransport(config, site_id=context.site_id, state_dir=context.state_dir)
        self.render_dir = context.state_dir / "ssh-psij"
        self.outbox = context.state_dir / "ssh-outbox"

    # helpers

    def ssh_argv(self) -> list[str]:
        """Return the base ``ssh`` argv (program, options, host) for ``serve``."""
        return self.transport.ssh_argv()

    async def _ssh(self, script: str, what: str, *, data: bytes | None = None) -> str:
        result = await self.transport.ssh(
            script, data=data, timeout_s=self.config.command_timeout_s, what=what
        )
        return result.text()

    # describe

    async def describe(self) -> SiteInfo:
        text = await self._ssh(scripts.describe_script(self.config.scheduler), "describe")
        sections = parse_sections(text)

        def first(key: str) -> str | None:
            values = [value for value in sections.get(key, []) if value.strip()]
            return values[0].strip() if values else None

        cpus = first("cpus")
        queues = sorted({line.strip() for line in sections.get("queues", []) if line.strip()})
        return SiteInfo(
            adapter=self.name,
            address=self.config.ssh_profile,
            scheduler=first("scheduler") or "none",
            queues=queues,
            facts={
                "host": first("host"),
                "os": first("os"),
                "machine": first("machine"),
                "cpus": int(cpus) if cpus and cpus.isdigit() else None,
                "home": first("home"),
                "remote_dir": self.config.remote_dir,
            },
            verbs=sorted(self.implements),
        )

    # price

    async def price(self, spec: JobSpec) -> Charge:
        """Return the most ``spec`` may use, in node-hours (RL25)."""
        return pricing.charge(self.context.site_id, spec)

    # run

    async def run(self, spec: JobSpec, *, operation_id: str) -> JobHandle:
        if spec.executable is None:
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                "the ssh adapter runs an executable; a package job needs the package runner",
            )
        name = job_name(operation_id)
        digests = {item.sha256 for item in spec.inputs}
        check = parse_sections(
            await self._ssh(
                scripts.check_script(
                    self.config.remote_dir, operation_id, self.config.scheduler, digests
                ),
                "checking the site",
            )
        )
        root = PurePosixPath(_one(check, "root") or "")
        if not root.is_absolute():
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"{self.config.ssh_profile} did not report relay's directory",
            )
        job_dir = root / "jobs" / operation_id
        existing = self._existing(check, root, job_dir, name)
        if existing is not None:
            return existing
        if "ready" not in check:
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"cannot create {root}/incoming on {self.config.ssh_profile}",
            )
        detected = _one(check, "scheduler") or "none"
        scheduler = cast(SchedulerName, detected) if detected in _SCHEDULERS else None

        have = set(check.get("have", []))
        missing = sorted(digests - have)
        blobs: list[Path] = []
        for digest in missing:
            if not self.context.content.has(digest):
                raise Refusal(
                    RefusalReason.INPUT_MISSING,
                    f"content {digest} is staged neither here nor on {self.config.ssh_profile}",
                    digest=digest,
                )
            blobs.append(self.context.content.path_for(digest))

        if scheduler is not None:
            _executor(scheduler, self.render_dir)  # import PSI/J on this thread, not a worker
        rendered = await asyncio.to_thread(self._render, spec, scheduler, root, job_dir, name)
        rendered.files["spec.json"] = spec.model_dump_json(indent=2)
        rendered.files["inputs.json"] = json.dumps(
            [{"name": item.name, "sha256": item.sha256} for item in spec.inputs], indent=2
        )
        rendered.files["scheduler"] = (scheduler or "none") + "\n"

        outbox = self.outbox / operation_id
        await asyncio.to_thread(_write_outbox, outbox, rendered.files)
        try:
            incoming = f"{root}/incoming/{operation_id}"
            sources = [outbox / file for file in sorted(rendered.files)]
            await self.transport.upload([*sources, *rendered.launcher_files, *blobs], incoming)
            script = scripts.submit_script(
                self.config.remote_dir,
                operation_id,
                blobs=missing,
                inputs=[(item.name, item.sha256) for item in spec.inputs],
                job_files=sorted(rendered.files),
                launcher_dir=rendered.launcher_dir,
                launcher_files=[path.name for path in rendered.launcher_files],
                submit_command=rendered.submit_command,
            )
            result = await self.transport.ssh(script, timeout_s=None, what="submitting")
        finally:
            await asyncio.to_thread(shutil.rmtree, outbox, True)
        sections = parse_sections(result.text())
        if "corrupt" in sections or "missing" in sections:
            bad = (sections.get("corrupt") or sections.get("missing") or ["?"])[0]
            raise Refusal(
                RefusalReason.INPUT_MISSING,
                f"content {bad} did not arrive intact on {self.config.ssh_profile}",
                digest=bad,
            )
        rc = _one(sections, "rc")
        output = "\n".join(sections.get("out", [])).strip()
        if rc != "0":
            error = "\n".join(sections.get("err", [])).strip() or output
            raise Refusal(
                RefusalReason.SUBMIT_FAILED,
                f"{self.config.ssh_profile} refused the job: {error or 'no output'}",
                scheduler=scheduler or "none",
                exit_code=int(rc) if rc and rc.lstrip("-").isdigit() else None,
            )
        return self._handle(scheduler, output, root, job_dir, name)

    def _existing(
        self, sections: dict[str, list[str]], root: PurePosixPath, job_dir: PurePosixPath, name: str
    ) -> JobHandle | None:
        if _one(sections, "rc") != "0":
            return None
        used = _one(sections, "used") or "none"
        scheduler = cast(SchedulerName, used) if used in _SCHEDULERS else None
        output = "\n".join(sections.get("out", [])).strip()
        return self._handle(scheduler, output, root, job_dir, name)

    def _handle(
        self,
        scheduler: SchedulerName | None,
        submit_output: str,
        root: PurePosixPath,
        job_dir: PurePosixPath,
        name: str,
    ) -> JobHandle:
        data: dict[str, Any] = {
            "root": str(root),
            "job_dir": str(job_dir),
            "job_name": name,
        }
        if scheduler is None:
            native = submit_output.strip().splitlines()[-1] if submit_output.strip() else ""
            if not native.startswith("pid:"):
                raise Refusal(
                    RefusalReason.SUBMIT_FAILED,
                    f"the job did not start on {self.config.ssh_profile}",
                )
            data["mode"] = "detached"
            return JobHandle(adapter=self.name, native_id=native, data=data)
        executor = _executor(scheduler, self.render_dir)
        try:
            native = executor.job_id_from_submit_output(submit_output)
        except Exception as exc:
            raise Refusal(
                RefusalReason.SUBMIT_FAILED,
                f"cannot read the job id from the scheduler's answer: {submit_output!r}",
                scheduler=scheduler,
            ) from exc
        data["mode"] = "scheduler"
        data["scheduler"] = scheduler
        return JobHandle(adapter=self.name, native_id=native, data=data)

    def _render(
        self,
        spec: JobSpec,
        scheduler: SchedulerName | None,
        root: PurePosixPath,
        job_dir: PurePosixPath,
        name: str,
    ) -> _Rendered:
        workdir = _remote_path(job_dir, spec.directory) or job_dir
        stdout = _remote_path(workdir, spec.stdout_path) or job_dir / "stdout"
        stderr = _remote_path(workdir, spec.stderr_path) or job_dir / "stderr"
        stdin = _remote_path(workdir, spec.stdin_path)
        rendered = _Rendered()
        if scheduler is None:
            assert spec.executable is not None
            rendered.files["supervise.sh"] = scripts.supervise_script(job_dir)
            rendered.files["child.sh"] = scripts.child_script(
                directory=workdir,
                executable=spec.executable,
                arguments=spec.arguments,
                environment=spec.environment,
                inherit_environment=spec.inherit_environment,
                stdin_path=stdin,
                stdout_path=stdout,
                stderr_path=stderr,
            )
            rendered.submit_command = scripts.DETACHED_SUBMIT
            return rendered
        from psij import Job

        executor = _executor(scheduler, self.render_dir)
        psij_spec = spec.to_psij(name=name)
        # Relay never sets a walltime of its own (RL18). With none in the spec, PSI/J
        # gets None and writes no directive, so the scheduler's default applies.
        psij_attributes: Any = psij_spec.attributes
        psij_attributes.duration = walltime(spec)
        # PSI/J would make these absolute against this machine. They are the site's paths.
        psij_spec._directory = workdir  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue]
        psij_spec._stdout_path = stdout  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue]
        psij_spec._stderr_path = stderr  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue]
        psij_spec._stdin_path = stdin  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue]
        job = Job(psij_spec)
        # PSI/J's templates name the job from ``job.name``, which its Job lacks.
        job.name = name  # pyright: ignore[reportAttributeAccessIssue]
        launcher_dir, launcher_files = _launcher(executor, job)
        context: dict[str, Any] = executor._create_script_context(job)  # pyright: ignore[reportPrivateUsage]
        psij_context = cast(dict[str, Any], context["psij"])
        command = [str(part) for part in psij_context["launch_command"]]
        command[1] = str(root / "psij" / launcher_dir / Path(command[1]).name)
        command[3] = ""
        psij_context["launch_command"] = command
        psij_context["script_dir"] = str(job_dir)
        buffer = io.StringIO()
        executor.generate_submit_script(job, context, buffer)
        script = buffer.getvalue()
        if scheduler == "pbs":
            # PSI/J 0.9.11's pbspro template writes -N="name", which qsub refuses.
            script = script.replace(f'#PBS -N="{name}"', f"#PBS -N {name}")
        elif scheduler == "slurm":
            script = scripts.forward_signals(script, command[1], str(job_dir))
        rendered.files["submit.sh"] = script
        script_path = cast(Path, _SitePath(job_dir / "submit.sh"))
        rendered.submit_command = shlex.join(executor.get_submit_command(job, script_path))
        rendered.launcher_dir = launcher_dir
        rendered.launcher_files = launcher_files
        return rendered

    async def find(self, operation_id: str) -> JobHandle | None:
        sections = parse_sections(
            await self._ssh(
                scripts.find_script(self.config.remote_dir, operation_id), "finding the job"
            )
        )
        root = PurePosixPath(_one(sections, "root") or "")
        job_dir = root / "jobs" / operation_id
        return self._existing(sections, root, job_dir, job_name(operation_id))

    # status

    async def status(self, handle: JobHandle) -> JobStatus:
        job_dir = str(handle.data["job_dir"])
        mode = str(handle.data.get("mode", "scheduler"))
        executor: BatchSchedulerExecutor | None = None
        status_command: str | None = None
        accounting_command: str | None = None
        if mode == "scheduler":
            scheduler = cast(SchedulerName, handle.data["scheduler"])
            executor = _executor(scheduler, self.render_dir)
            assert handle.native_id is not None
            status_command = shlex.join(executor.get_status_command([handle.native_id]))
            if scheduler == "slurm":
                accounting_command = shlex.join(
                    ["sacct", "-X", "-n", "-P", "-j", handle.native_id, "-o", "State,ExitCode"]
                )
        text = await self._ssh(
            scripts.status_script(
                job_dir,
                mode=mode,
                status_command=status_command,
                native_id=handle.native_id,
                accounting_command=accounting_command,
            ),
            "status",
        )
        sections = parse_sections(text)
        if "nodir" in sections:
            return JobStatus(
                state=JobState.FAILED,
                native_state="lost",
                message=f"the job directory {job_dir} is gone from {self.config.ssh_profile}",
            )
        if "final" in sections:
            status = _stored_final(sections["final"])
            status.outputs = self._outputs(handle, sections.get("outputs", []))
            return status
        last = (sections.get("last") or [""])[0].split()
        recorded = last[1] if len(last) > 1 else None
        if executor is not None:
            status = self._scheduler_status(executor, handle, sections, recorded)
        else:
            status = _detached_status(sections)
        if status.state.final:
            payload = status.model_dump_json(exclude={"outputs"}).encode()
            line = f"{status.state.value} exit {status.exit_code}"
            result = await self.transport.ssh(
                scripts.record_script(
                    job_dir,
                    line,
                    final=True,
                    root=str(handle.data["root"]),
                    inputs=scripts.input_entries(sections.get("inputs", [])),
                ),
                data=payload,
                timeout_s=self.config.command_timeout_s,
                what="recording the job's end",
            )
            status.outputs = self._outputs(handle, parse_sections(result.text()).get("outputs", []))
        elif recorded != status.state.value:
            line = status.state.value
            if recorded is None or recorded == "submitted":
                line = f"{line} {handle.native_id}"
            with contextlib.suppress(Refusal):
                await self.transport.ssh(
                    scripts.record_script(job_dir, line, final=False),
                    timeout_s=self.config.command_timeout_s,
                    what="recording the job's state",
                )
        return status

    def _scheduler_status(
        self,
        executor: BatchSchedulerExecutor,
        handle: JobHandle,
        sections: dict[str, list[str]],
        recorded: str | None = None,
    ) -> JobStatus:
        native_id = handle.native_id
        assert native_id is not None
        exit_code = _int(_one(sections, "ec"))
        cancelled = "cancel" in sections
        rc = _int(_one(sections, "schedrc"))
        output = "\n".join(sections.get("sched", []))
        observed = None
        if rc is None:
            raise Refusal(RefusalReason.UNREACHABLE, "the scheduler status command did not run")
        try:
            observed = executor.parse_status_output(rc, output).get(native_id)
        except Exception as exc:
            if exit_code is None and not cancelled and "unknown job" not in output.lower():
                raise Refusal(
                    RefusalReason.UNREACHABLE,
                    f"cannot read the scheduler's status: {output.strip()[:500] or exc}",
                ) from exc
        if observed is None:
            signalled = _one(sections, "signalled")
            if exit_code is not None and signalled and not cancelled:
                # The scheduler stopped the job; accounting says why.
                accounted = accounted_status(sections.get("acct", []))
                if accounted is not None:
                    return accounted
                if exit_code != 0:
                    return JobStatus(
                        state=JobState.FAILED,
                        exit_code=exit_code,
                        native_state="gone",
                        message=(
                            f"the scheduler stopped job {native_id} with SIG{signalled}: "
                            "a cancel, its time limit or preemption"
                        ),
                    )
            if exit_code is not None:
                if cancelled and exit_code != 0:
                    return JobStatus(
                        state=JobState.CANCELLED, exit_code=exit_code, native_state="gone"
                    )
                state = JobState.COMPLETED if exit_code == 0 else JobState.FAILED
                return JobStatus(state=state, exit_code=exit_code, native_state="gone")
            if cancelled:
                return JobStatus(state=JobState.CANCELLED, native_state="gone")
            accounted = accounted_status(sections.get("acct", []))
            if accounted is not None:
                return accounted
            # Schedulers forget finished jobs (Slurm after MinJobAge). With no exit
            # code and no accounting, how the job ended is unknown, not a failure.
            # The state stays open, so a later poll can still read an exit code,
            # and cancel ends it.
            return JobStatus(
                state=JobState.QUEUED if recorded == JobState.QUEUED.value else JobState.RUNNING,
                native_state="unknown",
                message=(
                    f"the scheduler no longer lists job {native_id}, and the job left no "
                    "exit code; how it ended is unknown"
                ),
            )
        state = _PSIJ_STATES.get(observed.state.name, JobState.RUNNING)
        if state == JobState.COMPLETED and exit_code not in (None, 0):
            state = JobState.FAILED
        if state == JobState.FAILED and cancelled:
            state = JobState.CANCELLED
        if state == JobState.COMPLETED and exit_code is None and cancelled:
            state = JobState.CANCELLED
        return JobStatus(
            state=state,
            exit_code=exit_code if state.final else None,
            native_state=observed.state.name,
            message=observed.message,
        )

    def _outputs(self, handle: JobHandle, lines: list[str]) -> list[OutputRef]:
        job_dir = PurePosixPath(str(handle.data["job_dir"]))
        native = handle.native_id or ""
        internal = set(scripts.INTERNAL_FILES)
        internal.update({f"{native}.out", f"{native}.ec", f"{native}.nodefile"})
        found: list[OutputRef] = []
        for relative, size in scripts.listing_entries(lines):
            parts = PurePosixPath(relative).parts
            if relative in internal or any(part.startswith(".") for part in parts):
                continue
            found.append(OutputRef(name=relative, path=str(job_dir / relative), size=size))
        return sorted(found, key=lambda ref: ref.name)

    # cancel

    async def cancel(self, handle: JobHandle) -> JobStatus:
        job_dir = str(handle.data["job_dir"])
        cancel_command: str | None = None
        if handle.data.get("mode", "scheduler") == "scheduler":
            scheduler = cast(SchedulerName, handle.data["scheduler"])
            executor = _executor(scheduler, self.render_dir)
            assert handle.native_id is not None
            cancel_command = shlex.join(executor.get_cancel_command(handle.native_id))
        sections = parse_sections(
            await self._ssh(scripts.cancel_script(job_dir, cancel_command=cancel_command), "cancel")
        )
        if "final" in sections or "nodir" in sections:
            return await self.status(handle)
        rc = _one(sections, "rc")
        if rc != "0":
            current = await self.status(handle)
            if current.state.final:
                return current
            detail = "\n".join(sections.get("rc", [])[1:]).strip()
            raise Refusal(
                RefusalReason.NOT_CANCELLABLE,
                f"the scheduler did not cancel the job: {detail or 'exit ' + str(rc)}",
            )
        current = await self.status(handle)
        for _ in range(10):
            if current.state.final:
                return current
            await asyncio.sleep(1)
            current = await self.status(handle)
        return current

    # exec

    async def exec(
        self,
        command: list[str],
        *,
        timeout_s: float | None = None,
        max_output_bytes: int,
        on_stall: StallReport | None = None,
    ) -> ExecResult:
        if not command:
            raise Refusal(RefusalReason.INVALID_ARGUMENTS, "give a command")
        result = await self.transport.ssh(
            scripts.exec_script(command),
            timeout_s=timeout_s,
            max_output_bytes=max_output_bytes,
            what="exec",
            check_timeout=False,
            hold_stdin=True,
            on_stall=on_stall,
        )
        exec_result = result.to_exec_result()
        exec_result.argv = list(command)
        return exec_result

    # files

    async def files(self) -> FileAccess:
        return SshFileAccess(
            self.transport,
            remote_dir=self.config.remote_dir,
            timeout_s=self.config.command_timeout_s,
        )

    # serve

    async def serve(self, spec: ServiceSpec, *, operation_id: str) -> Endpoint:
        try:
            serve_module: Any = importlib.import_module("clio_relay.serve")
        except ModuleNotFoundError as exc:
            if exc.name not in ("clio_relay.serve", "clio_relay"):
                raise
            raise Refusal(
                RefusalReason.VERB_NOT_IMPLEMENTED,
                "serve needs the service runtime, and it is not installed",
                verb="serve",
            ) from exc
        return cast(Endpoint, await serve_module.start_for(self, spec, operation_id=operation_id))

    # sign-in

    async def sign_in_needed(self) -> SignInNeed | None:
        if not self.config.interactive_sign_in:
            return None
        if await self.transport.master_up():
            return None
        failed = ""
        if self.config.askpass:
            # A configured program answers the prompts, so try it before asking anyone.
            try:
                await self.transport.starter()(self.transport.environment() or {})
            except RuntimeError as exc:
                failed = f". The askpass program could not sign in: {exc}"
            if not failed and await self.transport.master_up():
                return None
        self.transport.register_sign_in()
        return SignInNeed(
            provider="ssh",
            message=(
                f"Sign in to {self.config.ssh_profile}. Relay keeps that connection "
                f"and reuses it, so you sign in once{failed}"
            ),
            url=None,
        )

    async def sign_in_done(self, answer: dict[str, Any]) -> None:
        if self.config.interactive_sign_in and not await self.transport.master_up():
            raise Refusal(
                RefusalReason.NEEDS_SIGN_IN,
                f"the sign-in to {self.config.ssh_profile} did not leave a connection open",
                site=self.context.site_id,
            )


def _one(sections: dict[str, list[str]], key: str) -> str | None:
    values = [value.strip() for value in sections.get(key, []) if value.strip()]
    return values[0] if values else None


def _int(value: str | None) -> int | None:
    if value is None:
        return None
    with contextlib.suppress(ValueError):
        return int(value)
    return None


def _write_outbox(outbox: Path, files: dict[str, str]) -> None:
    shutil.rmtree(outbox, ignore_errors=True)
    outbox.mkdir(parents=True, exist_ok=True)
    for name, content in files.items():
        (outbox / name).write_bytes(content.replace("\r\n", "\n").encode("utf-8"))


def _launcher(executor: BatchSchedulerExecutor, job: Job) -> tuple[str, list[Path]]:
    """Deploy PSI/J's launcher scripts here, and return their directory name and files."""
    launcher = executor._get_launcher_from_job(job)  # pyright: ignore[reportPrivateUsage]
    ensure = getattr(launcher, "_ensure_launcher_deployed", None)
    if ensure is None:
        raise Refusal(
            RefusalReason.INVALID_SPEC,
            f"the {type(launcher).__name__} launcher is not script based",
        )
    ensure()
    deployed = Path(launcher._deployed_script_path)  # pyright: ignore
    deploy = cast(list[Path], launcher._files_to_deploy())  # pyright: ignore
    files = [deployed.parent / path.name for path in deploy]
    return deployed.parent.name, files


_ACCOUNTED_FAILURES = (
    "FAILED",
    "TIMEOUT",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "BOOT_FAIL",
    "DEADLINE",
    "PREEMPTED",
)


def accounted_status(lines: list[str]) -> JobStatus | None:
    """Read Slurm accounting (``State|ExitCode``) for a job the queue has forgotten."""
    for line in lines:
        state, _, code = line.strip().partition("|")
        if not state:
            continue
        native = state.split()[0]
        exit_code = _int(code.split(":", 1)[0]) if code else None
        if native == "COMPLETED":
            return JobStatus(
                state=JobState.COMPLETED if exit_code in (0, None) else JobState.FAILED,
                exit_code=exit_code,
                native_state=native,
            )
        if native == "CANCELLED":
            return JobStatus(state=JobState.CANCELLED, exit_code=exit_code, native_state=native)
        if native in _ACCOUNTED_FAILURES:
            return JobStatus(
                state=JobState.FAILED,
                exit_code=exit_code,
                native_state=native,
                message=f"Slurm accounting says {state.lower()}",
            )
        return None
    return None


def _stored_final(lines: list[str]) -> JobStatus:
    text = "\n".join(lines).strip()
    try:
        return JobStatus.model_validate_json(text)
    except ValueError:
        return JobStatus(state=JobState.FAILED, native_state="final", message=text[:500] or None)


def _detached_status(sections: dict[str, list[str]]) -> JobStatus:
    exit_code = _int(_one(sections, "exit"))
    cancelled = "cancel" in sections
    if exit_code is not None:
        if cancelled and exit_code != 0:
            return JobStatus(state=JobState.CANCELLED, exit_code=exit_code, native_state="exited")
        state = JobState.COMPLETED if exit_code == 0 else JobState.FAILED
        return JobStatus(state=state, exit_code=exit_code, native_state="exited")
    if "alive" in sections:
        if "started" in sections:
            return JobStatus(state=JobState.RUNNING, native_state="running")
        return JobStatus(state=JobState.QUEUED, native_state="starting")
    if cancelled:
        return JobStatus(state=JobState.CANCELLED, native_state="gone")
    return JobStatus(
        state=JobState.FAILED,
        native_state="lost",
        message="the supervisor is gone without an exit code; the host may have restarted",
    )
