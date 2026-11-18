"""The ``local`` adapter (RL9): the machine relay runs on.

Jobs go to the local batch scheduler through ``psij-python`` when one exists.
Relay renders the submit script with PSI/J, submits it, and polls with PSI/J's
own status command, so no PSI/J thread owns the job and any relay process can
observe it. With no scheduler, a job runs under a detached supervisor that
writes its state to the job directory.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

from pydantic import BaseModel, ConfigDict, Field

from clio_relay.adapters._files import LocalFileAccess
from clio_relay.adapters._process import POSIX, run_bounded
from clio_relay.adapters.base import AdapterContext, BaseAdapter, SiteInfo, Verb
from clio_relay.jobs.handles import (
    Endpoint,
    ExecResult,
    FileAccess,
    JobHandle,
    JobState,
    JobStatus,
    OutputRef,
)
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, ServiceSpec

if TYPE_CHECKING:
    from psij.executors.batch.batch_scheduler_executor import BatchSchedulerExecutor

SchedulerName = Literal["slurm", "pbs", "lsf", "cobalt"]
SchedulerChoice = Literal["auto", "none", "slurm", "pbs", "lsf", "cobalt"]

_SUBMIT_COMMANDS: dict[SchedulerName, str] = {
    "slurm": "sbatch",
    "pbs": "qsub",
    "lsf": "bsub",
    "cobalt": "cqsub",
}

_INTERNAL_FILES = frozenset(
    {"job.json", "spec.json", "state.json", "exit_code", "submit.sh", "native_id", "cancel"}
)

_executors: dict[tuple[str, str], BatchSchedulerExecutor] = {}
_executors_lock = threading.Lock()


def _executor(scheduler: SchedulerName, work_dir: Path) -> BatchSchedulerExecutor:
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


def detect_scheduler() -> SchedulerName | None:
    """Return the batch scheduler whose submit command is on ``PATH``."""
    for name, command in _SUBMIT_COMMANDS.items():
        if shutil.which(command):
            return name
    return None


class LocalConfig(BaseModel):
    """Settings for the ``local`` adapter."""

    model_config = ConfigDict(extra="forbid")

    scheduler: SchedulerChoice = Field(
        default="auto",
        description="auto picks the scheduler whose submit command is on PATH",
    )
    work_dir: str | None = Field(
        default=None,
        description="Where job directories go. Defaults to relay's state directory",
    )
    submit_timeout_s: float = Field(default=60, gt=0)


_PSIJ_STATES = {
    "NEW": JobState.QUEUED,
    "QUEUED": JobState.QUEUED,
    "ACTIVE": JobState.RUNNING,
    "COMPLETED": JobState.COMPLETED,
    "FAILED": JobState.FAILED,
    "CANCELED": JobState.CANCELLED,
}


class LocalAdapter(BaseAdapter[LocalConfig]):
    """The machine relay runs on."""

    name: ClassVar[str] = "local"
    Config: ClassVar[type[BaseModel]] = LocalConfig
    implements: ClassVar[frozenset[Verb]] = frozenset({"describe", "run", "exec", "files", "serve"})

    def __init__(self, config: LocalConfig, context: AdapterContext) -> None:
        super().__init__(config, context)
        base = Path(os.path.expanduser(config.work_dir)) if config.work_dir else context.state_dir
        self.work_dir = base
        self.jobs_root = base / "jobs"
        self.psij_dir = base / "psij"

    def scheduler(self) -> SchedulerName | None:
        """Return the scheduler jobs go to, or ``None`` for supervised processes."""
        choice = self.config.scheduler
        if choice == "none":
            return None
        if choice == "auto":
            return detect_scheduler()
        return choice

    async def describe(self) -> SiteInfo:
        scheduler = self.scheduler()
        queues: list[str] = []
        if scheduler == "slurm":
            result = await run_bounded(
                ["sinfo", "-h", "-o", "%R"], timeout_s=15, max_output_bytes=65536
            )
            if result.exit_code == 0:
                queues = sorted({line.strip() for line in result.stdout.splitlines() if line})
        return SiteInfo(
            adapter=self.name,
            address=socket.gethostname(),
            scheduler=scheduler or "none",
            queues=queues,
            facts={
                "os": platform.system(),
                "machine": platform.machine(),
                "cpus": os.cpu_count(),
                "python": platform.python_version(),
                "work_dir": str(self.work_dir),
            },
            verbs=sorted(self.implements),
        )

    def _job_dir(self, operation_id: str) -> Path:
        return self.jobs_root / operation_id

    @staticmethod
    def job_name(operation_id: str) -> str:
        """Return the scheduler job name relay gives an operation, for reconciling."""
        return f"relay-{operation_id}"

    async def run(self, spec: JobSpec, *, operation_id: str) -> JobHandle:
        if spec.executable is None:
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                "the local adapter runs an executable; a package job needs the package runner",
            )
        job_dir = self._job_dir(operation_id)
        await asyncio.to_thread(job_dir.mkdir, parents=True, exist_ok=True)
        for item in spec.inputs:
            await asyncio.to_thread(
                self.context.content.materialize, item.sha256, job_dir / item.name
            )
        await asyncio.to_thread(
            (job_dir / "spec.json").write_text, spec.model_dump_json(indent=2), "utf-8"
        )
        scheduler = self.scheduler()
        if scheduler is None:
            return await self._start_supervised(spec, job_dir)
        return await self._submit_batch(scheduler, spec, job_dir, operation_id)

    async def _submit_batch(
        self, scheduler: SchedulerName, spec: JobSpec, job_dir: Path, operation_id: str
    ) -> JobHandle:
        from psij import Job

        executor = _executor(scheduler, self.psij_dir)
        name = self.job_name(operation_id)
        job = Job(
            spec.to_psij(
                directory=str(job_dir),
                stdout_path=str(job_dir / "stdout"),
                stderr_path=str(job_dir / "stderr"),
                name=name,
            )
        )
        script = job_dir / "submit.sh"

        def _render() -> list[str]:
            context = executor._create_script_context(job)  # pyright: ignore[reportPrivateUsage]
            with script.open("w", encoding="utf-8") as handle:
                executor.generate_submit_script(job, context, handle)
            return executor.get_submit_command(job, script)

        argv = await asyncio.to_thread(_render)
        result = await run_bounded(
            argv,
            timeout_s=self.config.submit_timeout_s,
            max_output_bytes=65536,
            cwd=job_dir,
        )
        if result.exit_code != 0:
            raise Refusal(
                RefusalReason.SUBMIT_FAILED,
                f"{argv[0]} refused the job: {result.stderr.strip() or result.stdout.strip()}",
                scheduler=scheduler,
                exit_code=result.exit_code,
            )
        native_id = executor.job_id_from_submit_output(result.stdout)
        await asyncio.to_thread((job_dir / "native_id").write_text, native_id, "utf-8")
        return self._batch_handle(scheduler, native_id, job_dir, name)

    def _batch_handle(
        self, scheduler: SchedulerName, native_id: str, job_dir: Path, name: str
    ) -> JobHandle:
        return JobHandle(
            adapter=self.name,
            native_id=native_id,
            data={
                "mode": "scheduler",
                "scheduler": scheduler,
                "job_dir": str(job_dir),
                "job_name": name,
                "script_dir": str(self.psij_dir / scheduler),
            },
        )

    async def _start_supervised(self, spec: JobSpec, job_dir: Path) -> JobHandle:
        assert spec.executable is not None
        job = {
            "executable": spec.executable,
            "arguments": spec.arguments,
            "directory": spec.directory or str(job_dir),
            "environment": spec.environment,
            "inherit_environment": spec.inherit_environment,
            "stdin_path": spec.stdin_path,
            "stdout_path": spec.stdout_path or str(job_dir / "stdout"),
            "stderr_path": spec.stderr_path or str(job_dir / "stderr"),
        }

        def _launch() -> int:
            (job_dir / "job.json").write_text(json.dumps(job, indent=2), encoding="utf-8")
            log = (job_dir / "supervisor.log").open("ab")
            flags = 0
            if not POSIX:
                flags = getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(
                    subprocess, "CREATE_NEW_PROCESS_GROUP", 0
                )
            with log:
                process = subprocess.Popen(
                    [sys.executable, "-m", "clio_relay.adapters._supervise", str(job_dir)],
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=log,
                    close_fds=True,
                    start_new_session=POSIX,
                    creationflags=flags,
                )
            threading.Thread(target=process.wait, name="relay-reap", daemon=True).start()
            return process.pid

        pid = await asyncio.to_thread(_launch)
        return JobHandle(
            adapter=self.name,
            native_id=f"pid:{pid}",
            data={"mode": "supervised", "job_dir": str(job_dir), "supervisor_pid": pid},
        )

    async def find(self, operation_id: str) -> JobHandle | None:
        job_dir = self._job_dir(operation_id)
        native_file = job_dir / "native_id"
        scheduler = self.scheduler()
        if native_file.is_file() and scheduler is not None:
            native_id = native_file.read_text(encoding="utf-8").strip()
            return self._batch_handle(scheduler, native_id, job_dir, self.job_name(operation_id))
        state_file = job_dir / "state.json"
        if state_file.is_file():
            state: dict[str, Any] = json.loads(state_file.read_text(encoding="utf-8"))
            pid = state.get("supervisor_pid")
            return JobHandle(
                adapter=self.name,
                native_id=f"pid:{pid}",
                data={"mode": "supervised", "job_dir": str(job_dir), "supervisor_pid": pid},
            )
        if (job_dir / "job.json").is_file():
            return JobHandle(
                adapter=self.name,
                native_id=None,
                data={"mode": "supervised", "job_dir": str(job_dir)},
            )
        if scheduler == "slurm":
            name = self.job_name(operation_id)
            for argv in (
                ["squeue", "--me", "-h", "-t", "all", "-n", name, "-o", "%i"],
                ["sacct", "-n", "-X", "-P", "--name", name, "-o", "JobID"],
            ):
                result = await run_bounded(argv, timeout_s=15, max_output_bytes=4096)
                ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
                if result.exit_code == 0 and ids:
                    return self._batch_handle(scheduler, ids[0], job_dir, name)
        return None

    async def status(self, handle: JobHandle) -> JobStatus:
        if handle.data.get("mode") == "scheduler":
            status = await self._batch_status(handle)
        else:
            status = await asyncio.to_thread(self._supervised_status, handle)
        if status.state.final:
            status.outputs = await asyncio.to_thread(self._outputs, handle)
        return status

    async def _batch_status(self, handle: JobHandle) -> JobStatus:
        scheduler = cast(SchedulerName, handle.data["scheduler"])
        native_id = handle.native_id
        assert native_id is not None
        executor = _executor(scheduler, self.psij_dir)
        argv = executor.get_status_command([native_id])
        result = await run_bounded(argv, timeout_s=30, max_output_bytes=1 << 20)
        if result.timed_out or result.exit_code is None:
            raise Refusal(RefusalReason.TIMEOUT, f"{argv[0]} did not answer", scheduler=scheduler)
        try:
            found = executor.parse_status_output(result.exit_code, result.stdout)
        except Exception as exc:
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"cannot read {argv[0]} output: {exc}",
                scheduler=scheduler,
            ) from exc
        observed = found.get(native_id)
        exit_code = self._batch_exit_code(handle)
        cancelled = Path(handle.data["job_dir"], "cancel").exists()
        if observed is None:
            if exit_code is not None:
                state = JobState.COMPLETED if exit_code == 0 else JobState.FAILED
                return JobStatus(state=state, exit_code=exit_code, native_state="gone")
            if cancelled:
                return JobStatus(state=JobState.CANCELLED, native_state="gone")
            return JobStatus(
                state=JobState.FAILED,
                native_state="gone",
                message="the job left the queue without writing an exit code",
            )
        state = _PSIJ_STATES.get(observed.state.name, JobState.RUNNING)
        if state == JobState.COMPLETED and exit_code not in (None, 0):
            state = JobState.FAILED
        if state == JobState.FAILED and cancelled:
            state = JobState.CANCELLED
        return JobStatus(
            state=state,
            exit_code=exit_code if state.final else None,
            native_state=observed.state.name,
            message=observed.message,
        )

    @staticmethod
    def _batch_exit_code(handle: JobHandle) -> int | None:
        path = Path(handle.data["script_dir"], f"{handle.native_id}.ec")
        with contextlib.suppress(FileNotFoundError, ValueError):
            return int(path.read_text(encoding="utf-8").strip())
        return None

    @staticmethod
    def _alive(pid: object) -> bool:
        if not isinstance(pid, int) or pid <= 0:
            return False
        if not POSIX:
            return True
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        with contextlib.suppress(OSError):
            stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
            if stat.rsplit(")", 1)[-1].split()[0] == "Z":
                return False
        return True

    def _supervised_status(self, handle: JobHandle) -> JobStatus:
        job_dir = Path(handle.data["job_dir"])
        try:
            state: dict[str, Any] = json.loads((job_dir / "state.json").read_text("utf-8"))
        except FileNotFoundError:
            if self._alive(handle.data.get("supervisor_pid")):
                return JobStatus(state=JobState.QUEUED, native_state="starting")
            return JobStatus(
                state=JobState.FAILED,
                native_state="lost",
                message="the supervisor ended before it recorded the job",
            )
        except json.JSONDecodeError:
            return JobStatus(state=JobState.QUEUED, native_state="starting")
        name = str(state.get("state", "queued"))
        exit_code = state.get("exit_code")
        try:
            job_state = JobState(name)
        except ValueError:
            job_state = JobState.RUNNING
        if not job_state.final and not self._alive(state.get("supervisor_pid")):
            return JobStatus(
                state=JobState.FAILED,
                native_state="lost",
                message="the supervisor is gone; the machine may have restarted",
            )
        return JobStatus(
            state=job_state,
            exit_code=exit_code if isinstance(exit_code, int) else None,
            native_state=name,
        )

    def _outputs(self, handle: JobHandle) -> list[OutputRef]:
        job_dir = Path(handle.data["job_dir"])
        found: list[OutputRef] = []
        if not job_dir.is_dir():
            return found
        for path in sorted(job_dir.rglob("*")):
            relative = path.relative_to(job_dir).as_posix()
            if not path.is_file() or relative in _INTERNAL_FILES or relative.startswith("."):
                continue
            if relative == "supervisor.log":
                continue
            found.append(OutputRef(name=relative, path=str(path), size=path.stat().st_size))
        return found

    async def cancel(self, handle: JobHandle) -> JobStatus:
        job_dir = Path(handle.data["job_dir"])
        await asyncio.to_thread(job_dir.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread((job_dir / "cancel").write_text, "cancel", "utf-8")
        if handle.data.get("mode") == "scheduler":
            scheduler = cast(SchedulerName, handle.data["scheduler"])
            executor = _executor(scheduler, self.psij_dir)
            assert handle.native_id is not None
            argv = executor.get_cancel_command(handle.native_id)
            result = await run_bounded(argv, timeout_s=30, max_output_bytes=65536)
            if result.exit_code != 0:
                current = await self.status(handle)
                if current.state.final:
                    return current
                raise Refusal(
                    RefusalReason.NOT_CANCELLABLE,
                    f"{argv[0]} failed: {result.stderr.strip() or result.stdout.strip()}",
                )
            for _ in range(20):
                current = await self.status(handle)
                if current.state.final:
                    return current
                await asyncio.sleep(0.5)
            return JobStatus(state=JobState.CANCELLED, native_state="cancel requested")
        await asyncio.to_thread(self._signal_supervised, job_dir)
        for _ in range(20):
            current = await self.status(handle)
            if current.state.final:
                return current
            await asyncio.sleep(0.25)
        return await self.status(handle)

    @staticmethod
    def _signal_supervised(job_dir: Path) -> None:
        try:
            state: dict[str, Any] = json.loads((job_dir / "state.json").read_text("utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return
        child = state.get("child_pid")
        if not isinstance(child, int):
            return
        with contextlib.suppress(ProcessLookupError, PermissionError, OSError):
            if POSIX:
                os.killpg(child, signal.SIGTERM)
            else:
                os.kill(child, signal.SIGTERM)

    async def exec(
        self, command: list[str], *, timeout_s: float, max_output_bytes: int
    ) -> ExecResult:
        return await run_bounded(
            command,
            timeout_s=timeout_s,
            max_output_bytes=max_output_bytes,
            cwd=self.work_dir if self.work_dir.is_dir() else None,
        )

    async def files(self) -> FileAccess:
        await asyncio.to_thread(self.work_dir.mkdir, parents=True, exist_ok=True)
        return LocalFileAccess(self.work_dir, self.context.content)

    async def serve(self, spec: ServiceSpec, *, operation_id: str) -> Endpoint:
        try:
            runtime: Any = importlib.import_module("clio_relay.serve")
        except ModuleNotFoundError as exc:
            if exc.name not in ("clio_relay.serve", "clio_relay"):
                raise
            raise Refusal(
                RefusalReason.VERB_NOT_IMPLEMENTED,
                "serve needs the service runtime, and it is not installed",
                verb="serve",
            ) from exc
        endpoint: Endpoint = await runtime.start_for(self, spec, operation_id=operation_id)
        return endpoint
