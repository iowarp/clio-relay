"""The ``local`` adapter (RL9): the machine relay runs on.

Jobs go to the local batch scheduler through ``psij-python`` when one exists.
Relay renders the submit script with PSI/J, submits it, and polls with PSI/J's
own status command, so no PSI/J thread owns the job and any relay process can
observe it. With no scheduler, a job runs under a detached supervisor that
writes its state to the job directory.

A package job is JARVIS's (:mod:`clio_relay.jarvis_submit`). With Slurm, relay
writes the pipeline with its ``scheduler:`` block and runs ``jarvis ppl submit``;
status and outputs come from ``jarvis execution get`` and ``artifacts``, and
cancel and stop are ``jarvis execution cancel`` and ``stop``. A submit that
finds software to install becomes the install ask, and ``install`` submits the
install job. With no scheduler, the supervisor runs JARVIS's direct run on this
machine. The CLIO site home, where relay installs jarvis-cd and keeps the
install answers, is the work directory.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import io
import json
import os
import platform
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, ClassVar, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator

from clio_relay import jarvis_exits, jarvis_plan, jarvis_submit, site_outputs
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
    StallReport,
)
from clio_relay.jobs.psij_executors import batch_executor as _executor
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, ServiceSpec
from clio_relay.login_load import batch_hints, direct_refused

SchedulerName = Literal["slurm", "pbs", "lsf", "cobalt"]
SchedulerChoice = Literal["auto", "none", "slurm", "pbs", "lsf", "cobalt"]

_SUBMIT_COMMANDS: dict[SchedulerName, str] = {
    "slurm": "sbatch",
    "pbs": "qsub",
    "lsf": "bsub",
    "cobalt": "cqsub",
}

_INTERNAL_FILES = frozenset(
    {
        "job.json",
        "spec.json",
        "state.json",
        "exit_code",
        "submit.sh",
        "native_id",
        "scheduler_id",
        "cancel",
        *jarvis_submit.SITE_FILES,
    }
)

# The variable each scheduler sets to the job's own id, inside the job.
_JOB_ID_VARIABLES: dict[SchedulerName, str] = {
    "slurm": "SLURM_JOB_ID",
    "pbs": "PBS_JOBID",
    "lsf": "LSB_JOBID",
    "cobalt": "COBALT_JOBID",
}


def name_submit(scheduler: SchedulerName, argv: list[str], name: str) -> list[str]:
    """Add the job name to the submit command, so ``find`` can reconcile by name.

    psij 0.9.11's templates read a ``job.name`` its ``Job`` does not have, so a
    Slurm job is named after its script; relay names the job on the command line.
    """
    flags = {"slurm": [f"--job-name={name}"], "pbs": ["-N", name], "lsf": ["-J", name]}
    return [argv[0], *flags.get(scheduler, []), *argv[1:]]


def mark_script(script: str, scheduler: SchedulerName, job_dir: Path) -> str:
    """Have the job write its scheduler id into its own directory when it starts.

    That record survives the scheduler forgetting the job, and links the job
    directory to psij's exit-code file when relay died before keeping the id.
    """
    variable = _JOB_ID_VARIABLES[scheduler]
    target = shlex.quote(str(job_dir / "scheduler_id"))
    line = f'printf "%s\\n" "${{{variable}:-}}" > {target}'
    lines = script.split("\n")
    at = 1
    while at < len(lines) and (not lines[at].strip() or lines[at].startswith("#")):
        at += 1
    return "\n".join([*lines[:at], line, *lines[at:]])


def detect_scheduler() -> SchedulerName | None:
    """Return the batch scheduler whose submit command is on ``PATH``."""
    for name, command in _SUBMIT_COMMANDS.items():
        if shutil.which(command):
            return name
    return None


def _in_job(job_dir: Path, path: str | None) -> str | None:
    """Return ``path`` as an absolute path, relative ones under the job directory."""
    if path is None:
        return None
    found = Path(path)
    return (found if found.is_absolute() else job_dir / found).as_posix()


class LocalConfig(BaseModel):
    """Settings for the ``local`` adapter."""

    model_config = ConfigDict(extra="forbid")

    scheduler: SchedulerChoice = Field(
        default="auto",
        description=(
            "auto picks the scheduler whose submit command is on PATH, and refuses to run jobs "
            "here when it finds none but the host shows a batch system. none runs them here"
        ),
    )
    work_dir: str | None = Field(
        default=None, description="Where job directories go. Defaults to relay's state directory"
    )
    outputs_dir: str | None = Field(default=None, description=site_outputs.HELP)
    submit_timeout_s: float = Field(default=60, gt=0)
    jarvis: str = Field(default="", description=jarvis_submit.JARVIS_SETTING_HELP)
    jarvis_graph: str = Field(default="", description=jarvis_submit.GRAPH_SETTING_HELP)
    jarvis_python: str = Field(default="", description=jarvis_submit.PYTHON_SETTING_HELP)
    install_walltime_s: int | None = Field(
        default=None,
        ge=60,
        description=jarvis_submit.INSTALL_WALLTIME_HELP,
    )

    @field_validator("jarvis")
    @classmethod
    def _jarvis_path(cls, value: str) -> str:
        return jarvis_submit.explicit_jarvis(value)

    @field_validator("jarvis_graph")
    @classmethod
    def _jarvis_graph(cls, value: str) -> str:
        return jarvis_submit.published_graph(value)

    @field_validator("jarvis_python")
    @classmethod
    def _jarvis_python(cls, value: str) -> str:
        return jarvis_submit.explicit_python(value)


_PSIJ_STATES = {
    "NEW": JobState.QUEUED,
    "QUEUED": JobState.QUEUED,
    "ACTIVE": JobState.RUNNING,
    "COMPLETED": JobState.COMPLETED,
    "FAILED": JobState.FAILED,
    "CANCELED": JobState.CANCELLED,
}


def pid_alive(pid: object) -> bool:
    """Say whether process ``pid`` still runs (a zombie does not)."""
    if not isinstance(pid, int) or pid <= 0:
        return False
    if not POSIX:
        from clio_relay.shutdown import probe_windows  # noqa: PLC0415

        # A process relay cannot open (another user, a sandbox) counts as alive.
        return probe_windows(pid)[0] != "gone"
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


class LocalAdapter(BaseAdapter[LocalConfig]):
    """The machine relay runs on."""

    name: ClassVar[str] = "local"
    Config: ClassVar[type[BaseModel]] = LocalConfig
    implements: ClassVar[frozenset[Verb]] = frozenset({"describe", "run", "exec", "files", "serve"})
    runs_packages: ClassVar[bool] = True
    reads_service_runtimes: ClassVar[bool] = True

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
            scheduler=scheduler,
            queues=queues,
            facts={
                "os": platform.system(),
                "machine": platform.machine(),
                "cpus": os.cpu_count(),
                "python": platform.python_version(),
                "work_dir": str(self.work_dir),
                "outputs_dir": self.config.outputs_dir,
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
        return cast(JobHandle, await self._start(spec, operation_id, check_only=False))

    async def install_check(self, spec: JobSpec, *, operation_id: str) -> dict[str, Any]:
        return cast(dict[str, Any], await self._start(spec, operation_id, check_only=True))

    async def plan_job(self, spec: JobSpec, *, operation_id: str) -> dict[str, Any] | None:
        found = await self._start(spec, operation_id, check_only=False, plan=True)
        return cast(dict[str, Any] | None, found)

    async def _start(
        self, spec: JobSpec, operation_id: str, *, check_only: bool, plan: bool = False
    ) -> JobHandle | dict[str, Any] | None:
        job_dir = self._job_dir(operation_id)
        scheduler = self.scheduler()
        if scheduler is None and self.config.scheduler == "auto":
            hints = await asyncio.to_thread(batch_hints)
            if hints:
                raise direct_refused(hints, site=self.context.site_id)
        jarvis_mode = False
        if spec.steps():
            if scheduler is None:
                spec = jarvis_submit.direct_job(
                    site_outputs.with_outputs(spec, self.config.outputs_dir),
                    jarvis=self.config.jarvis,
                    job_dir=job_dir.as_posix(),
                    home=self._home(),
                    site=self.context.site_id,
                    graph=self.config.jarvis_graph,
                )
            elif scheduler == "slurm":
                spec = jarvis_submit.with_archive(spec)
                jarvis_mode = True
            else:
                raise jarvis_submit.scheduler_refusal(scheduler, site=self.context.site_id)
        elif spec.executable is None:
            raise Refusal(RefusalReason.INVALID_SPEC, "give an executable or a package")
        if check_only and not jarvis_mode:
            raise jarvis_exits.install_only_refused(scheduler, site=self.context.site_id)
        if plan and not jarvis_mode:
            return None
        if jarvis_mode and not check_only and not plan:
            existing = await asyncio.to_thread(self._jarvis_found, job_dir)
            if existing is not None:
                return existing
        await asyncio.to_thread(job_dir.mkdir, parents=True, exist_ok=True)
        for item in spec.inputs:
            await asyncio.to_thread(
                self.context.content.materialize, item.sha256, job_dir / item.name
            )
        await asyncio.to_thread(
            (job_dir / "spec.json").write_text, spec.model_dump_json(indent=2), "utf-8"
        )
        if jarvis_mode:
            mode: jarvis_submit.SiteMode = "check" if check_only else "plan" if plan else "submit"
            result = await self._site_script(spec, job_dir, operation_id, mode)
            if plan:
                return jarvis_plan.planned(str(result.exit_code), result.stdout)
            if check_only:
                return jarvis_exits.checked(result.stdout)
            found = jarvis_submit.parse_handle(result.stdout)
            if found is None:
                raise Refusal(
                    RefusalReason.SUBMIT_FAILED,
                    "jarvis ppl submit printed no execution handle: "
                    f"{result.stdout.strip()[-500:] or 'no output'}",
                    scheduler="jarvis",
                )
            return self._jarvis_handle(found, job_dir, operation_id)
        if scheduler is None:
            return await self._start_supervised(spec, job_dir)
        return await self._submit_batch(scheduler, spec, job_dir, operation_id)

    def _home(self) -> str:
        return self.work_dir.as_posix()

    def _jarvis_document(
        self, spec: JobSpec, job_dir: Path, operation_id: str
    ) -> dict[str, object]:
        directory = job_dir.as_posix()
        block = jarvis_submit.scheduler_block(
            spec,
            job_name=self.job_name(operation_id),
            stdout=_in_job(job_dir, spec.stdout_path),
            stderr=_in_job(job_dir, spec.stderr_path),
            job_dir=directory,
        )
        return jarvis_submit.pipeline_document(
            spec,
            pipeline=jarvis_submit.pipeline_id(operation_id),
            scheduler=block,
            job_dir=directory,
        )

    async def _site_script(
        self, spec: JobSpec, job_dir: Path, operation_id: str, mode: jarvis_submit.SiteMode
    ) -> ExecResult:
        document = self._jarvis_document(spec, job_dir, operation_id)

        def _write() -> None:
            pipeline = jarvis_submit.render_pipeline(document)
            (job_dir / jarvis_submit.PIPELINE_FILE).write_text(pipeline, "utf-8")
            (job_dir / jarvis_submit.SCRIPT_FILE).write_text(jarvis_submit.SITE_SCRIPT, "utf-8")

        await asyncio.to_thread(_write)
        out = site_outputs.with_outputs(spec, self.config.outputs_dir)
        command = jarvis_submit.submit_command(
            out,
            operation_id=operation_id,
            jarvis=self.config.jarvis,
            home=self._home(),
            mode=mode,
            graph=self.config.jarvis_graph,
        )
        result = await run_bounded(
            shlex.split(command),
            timeout_s=self.config.submit_timeout_s,
            max_output_bytes=1 << 20,
            cwd=job_dir,
        )
        if mode == "plan":
            # No plan (JARVIS refused, or cannot plan) is not a refusal: the submit says why.
            return result
        self._refuse_jarvis_exit(result, spec)
        if result.exit_code != 0:
            what = "jarvis ppl install +check" if mode == "check" else "jarvis ppl submit"
            raise Refusal(
                RefusalReason.SUBMIT_FAILED,
                f"{what} refused the job: "
                f"{jarvis_exits.failure_text(result.stdout, result.stderr)[-500:] or 'no output'}",
                scheduler="jarvis",
                exit_code=result.exit_code,
            )
        return result

    def _refuse_jarvis_exit(self, result: ExecResult, spec: JobSpec) -> None:
        """Raise the refusal for the site script's typed exits (69, 3, 4, 5 and 6)."""
        r, site, w = result, self.context.site_id, self.config.install_walltime_s
        jarvis_exits.refuse(
            r.exit_code, r.stdout, r.stderr, spec=spec, site=site, install_walltime_s=w
        )

    async def install(
        self, spec: JobSpec, *, operation_id: str, answers: dict[str, Any]
    ) -> JobHandle:
        """Submit ``ppl install +submit`` from the job directory the refused submit left."""
        job_dir = self._job_dir(operation_id)
        if not await asyncio.to_thread((job_dir / jarvis_submit.PIPELINE_FILE).is_file):
            raise Refusal(
                RefusalReason.INPUT_MISSING,
                f"the job directory {job_dir} has no pipeline; run the job again",
            )
        spec = jarvis_submit.with_archive(spec)
        document = self._jarvis_document(spec, job_dir, operation_id)
        argv = shlex.split(
            jarvis_submit.install_command(
                site_outputs.with_outputs(spec, self.config.outputs_dir),
                operation_id=operation_id,
                jarvis=self.config.jarvis,
                answers=answers,
                pipeline=jarvis_submit.install_pipeline(
                    document, self.config.install_walltime_s, answers
                ),
                home=self._home(),
                graph=self.config.jarvis_graph,
            )
        )
        result = await run_bounded(
            argv, timeout_s=self.config.submit_timeout_s, max_output_bytes=1 << 20, cwd=job_dir
        )
        self._refuse_jarvis_exit(result, spec)
        found, out = jarvis_submit.parse_handle(result.stdout), result.stdout
        if here := jarvis_exits.without_job(self.name, result.exit_code, out, found, "supervised"):
            return here
        if result.exit_code != 0 or found is None:
            raise Refusal(
                RefusalReason.SUBMIT_FAILED,
                "jarvis ppl install refused the install job: "
                f"{(result.stderr.strip() or result.stdout.strip())[-500:] or 'no output'}",
                scheduler="jarvis",
                exit_code=result.exit_code,
            )
        handle = self._jarvis_handle(found, job_dir, operation_id)
        handle.data["install"] = True
        return handle

    async def install_jarvis(self) -> str:
        await asyncio.to_thread(self.work_dir.mkdir, parents=True, exist_ok=True)
        result = await run_bounded(
            shlex.split(jarvis_submit.bootstrap_command(self._home(), self.config.jarvis_python)),
            timeout_s=None,
            max_output_bytes=1 << 20,
            cwd=self.work_dir,
        )
        return jarvis_submit.bootstrap_result(
            result.exit_code, result.stdout + "\n" + result.stderr, site=self.context.site_id
        )

    def _jarvis_handle(self, found: dict[str, Any], job_dir: Path, operation_id: str) -> JobHandle:
        try:
            said = (job_dir / jarvis_submit.SHARED_FILE).read_text("utf-8").strip()
        except OSError:
            said = ""
        native, data = jarvis_submit.handle_data(
            found,
            job_dir=job_dir.as_posix(),
            shared=Path(said).as_posix() if said and Path(said).is_absolute() else None,
        )
        data["job_name"] = self.job_name(operation_id)
        if (
            planned := jarvis_plan.read(job_dir / jarvis_plan.PLAN_FILE, jarvis_plan.parse)
        ) is not None:
            data[jarvis_plan.KEY] = planned
        return JobHandle(adapter=self.name, native_id=native, data=data)

    def _jarvis_found(self, job_dir: Path) -> JobHandle | None:
        """Return the handle of a package job JARVIS already accepted, if there is one."""
        try:
            text = (job_dir / jarvis_submit.SUBMISSION_FILE).read_text("utf-8")
        except FileNotFoundError:
            return None
        found = jarvis_submit.parse_handle(text)
        if found is None:
            return None
        return self._jarvis_handle(found, job_dir, job_dir.name)

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
            rendered = io.StringIO()
            executor.generate_submit_script(job, context, rendered)
            script.write_text(mark_script(rendered.getvalue(), scheduler, job_dir), "utf-8")
            return name_submit(scheduler, executor.get_submit_command(job, script), name)

        argv = await asyncio.to_thread(_render)
        result = await run_bounded(
            argv, timeout_s=self.config.submit_timeout_s, max_output_bytes=65536, cwd=job_dir
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
        submitted = await asyncio.to_thread(self._jarvis_found, job_dir)
        if submitted is not None:
            return submitted
        scheduler = self.scheduler()
        for kept in ("native_id", "scheduler_id"):
            native_file = job_dir / kept
            if scheduler is None or not native_file.is_file():
                continue
            native_id = native_file.read_text(encoding="utf-8").strip()
            if native_id:
                name = self.job_name(operation_id)
                return self._batch_handle(scheduler, native_id, job_dir, name)
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
        if handle.data.get("mode") == "jarvis":
            return await self._jarvis_status(handle)
        if handle.data.get("mode") == "scheduler":
            status = await self._batch_status(handle)
        else:
            status = await asyncio.to_thread(self._supervised_status, handle)
        if status.state.final:
            await asyncio.to_thread(self._drop_transient, handle)
            status.outputs = await asyncio.to_thread(self._outputs, handle)
        return status

    async def _execution(self, handle: JobHandle, verb: jarvis_submit.ExecutionVerb) -> str:
        """Run ``jarvis execution <verb>`` for the job, with the JARVIS that submitted it."""
        result = await self._run_execution(handle, verb)
        if result.exit_code != 0:
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"jarvis execution {verb} did not answer: "
                f"{(result.stderr.strip() or result.stdout.strip())[-500:] or 'no output'}",
            )
        return result.stdout

    async def service_runtimes(self, handle: JobHandle) -> dict[str, Any] | None:
        """``jarvis execution service-runtimes +json`` for a JARVIS job."""
        if handle.data.get("mode") != "jarvis" or not handle.data.get("job_dir"):
            return None
        result = await self._run_execution(handle, "service-runtimes")
        if result.exit_code != 0:
            return None
        return jarvis_submit.parse_document(result.stdout, jarvis_submit.SERVICE_RUNTIMES_SCHEMA)

    async def _run_execution(
        self, handle: JobHandle, verb: jarvis_submit.ExecutionVerb
    ) -> ExecResult:
        job_dir = Path(handle.data["job_dir"])
        try:
            jarvis = (job_dir / jarvis_submit.JARVIS_FILE).read_text("utf-8").strip()
        except FileNotFoundError:
            jarvis = ""
        if not jarvis:
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"the job directory {job_dir} does not say which JARVIS submitted the job",
            )
        execution_id, pipeline = jarvis_submit.handle_pipeline(handle.data)
        argv = jarvis_submit.execution_argv(
            verb, execution_id, job_dir=job_dir.as_posix(), jarvis=jarvis, pipeline=pipeline
        )
        # stop waits for nothing here: JARVIS signals the job and returns its record.
        return await run_bounded(argv, timeout_s=120, max_output_bytes=1 << 20, cwd=job_dir)

    async def _jarvis_status(self, handle: JobHandle) -> JobStatus:
        """Read the job from JARVIS's execution record; relay keeps no state of its own."""
        job_dir = Path(handle.data["job_dir"])
        if not await asyncio.to_thread(job_dir.is_dir):
            return JobStatus(
                state=JobState.FAILED,
                native_state="lost",
                message=f"the job directory {job_dir} is gone",
            )
        answer = await self._execution(handle, "get")
        record = jarvis_submit.parse_document(answer, jarvis_submit.RECORD_SCHEMA)
        if record is None:
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"jarvis execution get printed no record: {answer.strip()[-500:] or 'no output'}",
            )
        status = jarvis_submit.record_status(record, job_id=handle.native_id)
        execution_id, pipeline = jarvis_submit.handle_pipeline(handle.data)
        execution = str(handle.data.get("execution_dir") or "") or (
            jarvis_submit.execution_dir(job_dir.as_posix(), execution_id, pipeline=pipeline)
        )
        if not handle.data.get("install"):
            states = Path(execution) / jarvis_plan.STATES_FILE
            found = await asyncio.to_thread(jarvis_plan.read, states, jarvis_plan.component_states)
            status.components = found or []
        if not status.state.final or handle.data.get("install"):
            return status
        snapshot = jarvis_submit.parse_document(
            await self._execution(handle, "artifacts"), jarvis_submit.ARTIFACTS_SCHEMA
        )
        if snapshot is not None:
            status.outputs = jarvis_submit.artifact_outputs(snapshot, execution=execution)
        await asyncio.to_thread(self._drop_transient, handle)
        return status

    @staticmethod
    def _drop_transient(handle: JobHandle) -> None:
        """Remove the job directory's copies of inputs marked transient (RL16)."""
        job_dir = Path(handle.data["job_dir"])
        try:
            spec = JobSpec.model_validate_json((job_dir / "spec.json").read_text("utf-8"))
        except (FileNotFoundError, ValueError):
            return
        for item in spec.inputs:
            if item.transient:
                (job_dir / item.name).unlink(missing_ok=True)

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
            # The scheduler forgot the job and it left no exit code: there is
            # no evidence it failed, so it stays open until someone cancels it.
            return JobStatus(
                state=JobState.RUNNING,
                native_state="unknown",
                message=(
                    f"{argv[0]} no longer lists job {native_id} and it wrote no exit code, "
                    "so relay cannot tell how it ended; cancel ends the operation"
                ),
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

    def _supervised_status(self, handle: JobHandle) -> JobStatus:
        job_dir = Path(handle.data["job_dir"])
        try:
            state: dict[str, Any] = json.loads((job_dir / "state.json").read_text("utf-8"))
        except FileNotFoundError:
            if pid_alive(handle.data.get("supervisor_pid")):
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
        if not job_state.final and not pid_alive(state.get("supervisor_pid")):
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
        if handle.data.get("mode") == "jarvis":
            return await self._jarvis_control(handle, "cancel")
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

    async def stop(self, handle: JobHandle) -> JobStatus:
        """``jarvis execution stop`` for a JARVIS job; any other job is cancelled."""
        if handle.data.get("mode") == "jarvis":
            return await self._jarvis_control(handle, "stop")
        return await self.cancel(handle)

    async def _jarvis_control(
        self, handle: JobHandle, verb: jarvis_submit.ExecutionVerb
    ) -> JobStatus:
        current = await self.status(handle)
        if current.state.final:
            return current
        result = await self._run_execution(handle, verb)
        if result.exit_code != 0:
            current = await self.status(handle)
            if current.state.final:
                return current
            raise jarvis_submit.execution_failed(verb, result.stderr + "\n" + result.stdout)
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
        self,
        command: list[str],
        *,
        timeout_s: float | None = None,
        max_output_bytes: int,
        on_stall: StallReport | None = None,
    ) -> ExecResult:
        return await run_bounded(
            command,
            timeout_s=timeout_s,
            max_output_bytes=max_output_bytes,
            cwd=self.work_dir if self.work_dir.is_dir() else None,
            on_stall=on_stall,
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
