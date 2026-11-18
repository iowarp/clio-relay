"""The ``sky`` adapter (RL14): SkyPilot managed jobs on clouds, Kubernetes and Slurm.

Relay never imports SkyPilot. SkyPilot pins versions of ``click`` and
``uvicorn`` that relay's own install cannot take, so it lives in its own
environment and the adapter runs ``bridge.py`` with that environment's Python.
``run`` is a SkyPilot managed job, and :meth:`SkyAdapter.price` returns the
price SkyPilot's catalog gives for the job's resources, for the spend gate.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import json
import os
import re
import shlex
import shutil
import sys
import time
from pathlib import Path
from typing import Any, ClassVar, cast

from pydantic import BaseModel, ConfigDict, Field

from clio_relay.adapters.base import AdapterContext, BaseAdapter, SignInNeed, SiteInfo, Verb
from clio_relay.adapters.hooks import Charge
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus, OutputRef
from clio_relay.jobs.records import write_json_atomic
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec

BRIDGE = Path(__file__).with_name("bridge.py")
MARKER = "CLIO_RELAY_SKY "
DEFAULT_ENV = Path("~/.clio-relay/sky")
INSTALL_HINT = (
    "install SkyPilot in its own environment, for example "
    "`uv venv ~/.clio-relay/sky && uv pip install --python ~/.clio-relay/sky "
    "'skypilot[aws]==0.14.3'`, then set python in the site's sky settings "
    "if it is somewhere else"
)
# ManagedJobStatus values in SkyPilot 0.14.3.
STATES = {
    "PENDING": JobState.QUEUED,
    "SUBMITTED": JobState.QUEUED,
    "STARTING": JobState.QUEUED,
    "RUNNING": JobState.RUNNING,
    "WINDING_DOWN": JobState.RUNNING,
    "RECOVERING": JobState.RUNNING,
    "CANCELLING": JobState.RUNNING,
    "SUCCEEDED": JobState.COMPLETED,
    "CANCELLED": JobState.CANCELLED,
}
RESOURCE_KEYS = (
    "infra",
    "instance_type",
    "cpus",
    "memory",
    "accelerators",
    "use_spot",
    "disk_size",
    "region",
    "zone",
)
_EXIT_CODE = re.compile(r"exit code[:= ]+(-?\d+)", re.IGNORECASE)


class SkyCredentials(BaseModel):
    """Cloud keys the person typed on relay's page, handed to SkyPilot as variables."""

    model_config = ConfigDict(extra="forbid")

    provider: str = Field(description="Cloud named when a key is missing, such as aws")
    env: dict[str, str] = Field(
        description="Variable to secret name in relay's store, "
        "such as AWS_ACCESS_KEY_ID: aws-access-key-id"
    )


class SkyConfig(BaseModel):
    """Settings for the ``sky`` adapter."""

    model_config = ConfigDict(extra="forbid")

    python: str | None = Field(
        default=None,
        description="Python of the SkyPilot environment; found from ~/.clio-relay/sky or "
        "the sky command on PATH when empty",
    )
    infra: str | None = Field(default=None, description="Where to run, such as aws or k8s")
    instance_type: str | None = None
    cpus: str | None = Field(default=None, description="Such as 2+ or 8")
    memory: str | None = Field(default=None, description="GB, such as 8+")
    accelerators: str | None = Field(default=None, description="Such as A100:1")
    use_spot: bool = False
    disk_size: int | None = Field(default=None, ge=1, description="GB")
    region: str | None = None
    zone: str | None = None
    setup: str | None = Field(default=None, description="Commands run once before the job")
    workdir: str | None = Field(default=None, description="Local directory synced to the job")
    env: dict[str, str] = Field(
        default_factory=dict[str, str], description="Variables for SkyPilot itself"
    )
    credentials: SkyCredentials | None = None
    stall_s: float = Field(
        default=1800.0,
        gt=0,
        description="How long SkyPilot may print nothing before relay stops waiting for it",
    )


def find_python(configured: str | None) -> Path | None:
    """Return the SkyPilot environment's Python, or None when there is none."""
    if configured:
        path = Path(configured).expanduser()
        return path if path.is_file() else None
    folder = "Scripts" if sys.platform == "win32" else "bin"
    name = "python.exe" if sys.platform == "win32" else "python"
    default = DEFAULT_ENV.expanduser() / folder / name
    if default.is_file():
        return default
    command = shutil.which("sky")
    if command is None:
        return None
    candidate = Path(command).with_name(name)
    if candidate.is_file():
        return candidate
    with contextlib.suppress(OSError, UnicodeDecodeError):
        first = Path(command).read_text(encoding="utf-8").splitlines()[0]
        if first.startswith("#!"):
            interpreter = Path(first[2:].strip().split()[0])
            if interpreter.is_file():
                return interpreter
    return None


def _exit_code(state: JobState, reason: str | None) -> int | None:
    if state == JobState.COMPLETED:
        return 0
    if state != JobState.FAILED:
        return None
    match = _EXIT_CODE.search(reason or "")
    return int(match.group(1)) if match else 1


def run_command(spec: JobSpec) -> str:
    """Render the job as the shell command SkyPilot runs."""
    if spec.executable is None:
        raise Refusal(
            RefusalReason.INVALID_SPEC,
            "the sky adapter runs an executable; a package job needs the package runner",
        )
    argv = [spec.executable, *spec.arguments]
    if spec.launcher:
        argv = [*shlex.split(spec.launcher), *argv]
    command = shlex.join(argv)
    if spec.stdin_path:
        command += f" < {shlex.quote(spec.stdin_path)}"
    if spec.stdout_path:
        command += f" > {shlex.quote(spec.stdout_path)}"
    if spec.stderr_path:
        command += f" 2> {shlex.quote(spec.stderr_path)}"
    if spec.directory:
        directory = shlex.quote(spec.directory)
        command = f"mkdir -p {directory} && cd {directory} && {command}"
    return command


class SkyAdapter(BaseAdapter[SkyConfig]):
    """SkyPilot managed jobs, through SkyPilot's own Python."""

    name: ClassVar[str] = "sky"
    Config: ClassVar[type[BaseModel]] = SkyConfig
    implements: ClassVar[frozenset[Verb]] = frozenset({"describe", "run"})

    def __init__(self, config: SkyConfig, context: AdapterContext) -> None:
        super().__init__(config, context)
        python = find_python(config.python)
        if python is None:
            where = f" at {config.python}" if config.python else ""
            raise Refusal(
                RefusalReason.ADAPTER_UNAVAILABLE,
                f"the sky adapter found no SkyPilot Python{where}; {INSTALL_HINT}",
            )
        self.python = python
        self.records = context.state_dir / "sky-jobs"

    def _secret(self, name: str) -> str | None:
        """Return a secret the person typed on relay's page, from relay's token store."""
        try:
            store_module = importlib.import_module("clio_relay.identity.store")
        except ImportError:
            return None
        value: object = store_module.TokenStore().secret(name)
        return value if isinstance(value, str) else None

    def _credential_env(self) -> dict[str, str]:
        credentials = self.config.credentials
        if credentials is None:
            return {}
        found: dict[str, str] = {}
        for variable, name in credentials.env.items():
            value = self._secret(name)
            if not value:
                raise Refusal(
                    RefusalReason.NEEDS_SIGN_IN,
                    f"relay has no {credentials.provider} key {name}; add it on relay's page",
                    provider=f"keyring:{name}",
                    cloud=credentials.provider,
                    secret=name,
                )
            found[variable] = value
        return found

    def _env(self) -> dict[str, str]:
        env = {
            key: value
            for key, value in os.environ.items()
            if key not in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV", "PYTHONSAFEPATH")
        }
        env.update(self.config.env)
        env.update(self._credential_env())
        env["PYTHONUNBUFFERED"] = "1"
        return env

    async def call(self, op: str, request: dict[str, Any]) -> dict[str, Any]:
        """Run one bridge operation and return its result."""
        try:
            process = await asyncio.create_subprocess_exec(
                str(self.python),
                str(BRIDGE),
                op,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=self._env(),
                limit=1 << 20,
            )
        except OSError as exc:
            raise Refusal(
                RefusalReason.ADAPTER_UNAVAILABLE,
                f"cannot start SkyPilot's Python {self.python}: {exc}; {INSTALL_HINT}",
            ) from exc
        assert process.stdin is not None and process.stdout is not None
        assert process.stderr is not None
        process.stdin.write(json.dumps(request).encode())
        await process.stdin.drain()
        process.stdin.close()
        last = time.monotonic()
        tail: list[str] = []

        async def follow(stream: asyncio.StreamReader) -> None:
            nonlocal last
            async for line in stream:
                last = time.monotonic()
                tail.append(line.decode("utf-8", "replace").rstrip())
                del tail[:-40]

        stdout_task = asyncio.create_task(process.stdout.read())
        stderr_task = asyncio.create_task(follow(process.stderr))
        try:
            while True:
                done, _ = await asyncio.wait({stdout_task}, timeout=5)
                if done:
                    break
                if time.monotonic() - last > self.config.stall_s:
                    raise Refusal(
                        RefusalReason.UNREACHABLE,
                        f"SkyPilot printed nothing for {self.config.stall_s:.0f}s during {op}",
                        tail="\n".join(tail[-10:]),
                    )
            await process.wait()
            await stderr_task
        except BaseException:
            # Kill the bridge and close its pipes on this loop, also when cancelled.
            if process.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    process.kill()
            rest = [asyncio.ensure_future(process.wait()), stdout_task, stderr_task]
            await asyncio.wait(rest, timeout=5)
            for step in rest:
                step.cancel()
            await asyncio.gather(*rest, return_exceptions=True)
            raise
        answer: dict[str, Any] | None = None
        for line in stdout_task.result().decode("utf-8", "replace").splitlines():
            if line.startswith(MARKER):
                loaded: Any = json.loads(line.removeprefix(MARKER))
                answer = cast(dict[str, Any], loaded)
        if answer is None:
            raise Refusal(
                RefusalReason.ADAPTER_UNAVAILABLE,
                f"SkyPilot's Python {self.python} gave no answer to {op} "
                f"(exit {process.returncode}): " + " | ".join(tail[-5:]),
            )
        if not answer.get("ok"):
            kind = str(answer.get("kind", ""))
            reason = (
                RefusalReason.ADAPTER_UNAVAILABLE
                if kind == "import"
                else RefusalReason.SUBMIT_FAILED
                if op == "launch"
                else RefusalReason.UNREACHABLE
            )
            message = str(answer.get("error", "SkyPilot failed"))
            if kind == "import":
                message = f"{message}; {INSTALL_HINT}"
            raise Refusal(reason, f"SkyPilot {op}: {message}", kind=kind)
        result: Any = answer.get("result") or {}
        return cast(dict[str, Any], result)

    def resources(self, spec: JobSpec | None = None) -> dict[str, Any]:
        """Return the resource settings, with ``sky.`` attributes from ``spec`` on top."""
        values: dict[str, Any] = {key: getattr(self.config, key) for key in RESOURCE_KEYS}
        if spec is not None:
            for name, value in spec.attributes.custom_attributes.items():
                key = name.removeprefix("sky.")
                if name.startswith("sky.") and key in RESOURCE_KEYS:
                    values[key] = value
        return {key: value for key, value in values.items() if value is not None}

    async def price(self, spec: JobSpec) -> Charge:
        """Return what ``spec`` may cost, in US dollars, from SkyPilot's catalog."""
        nodes = spec.resources.nodes()
        duration = spec.attributes.duration
        if duration is None:
            return Charge(site=self.context.site_id, nodes=nodes, unit="USD")
        walltime_s = duration.total_seconds()
        found = await self.call(
            "price", {**self.resources(spec), "seconds": walltime_s, "nodes": nodes}
        )
        return Charge(
            site=self.context.site_id,
            nodes=nodes,
            walltime_s=walltime_s,
            node_hours=spec.node_hours(),
            amount=float(found["amount"]),
            unit=str(found.get("unit", "USD")),
        )

    async def describe(self) -> SiteInfo:
        facts: dict[str, Any] = {"python": str(self.python), **self.resources()}
        try:
            facts["skypilot_version"] = (await self.call("check", {})).get("version")
        except Refusal as refusal:
            facts["skypilot_error"] = refusal.message
        return SiteInfo(adapter=self.name, facts=facts, verbs=sorted(self.implements))

    async def sign_in_needed(self) -> SignInNeed | None:
        try:
            self._credential_env()
        except Refusal as refusal:
            provider = str(refusal.data.get("provider", "keyring"))
            return SignInNeed(provider=provider, message=refusal.message)
        return None

    def _job_dir(self, operation_id: str) -> Path:
        return self.records / operation_id

    def _record_path(self, operation_id: str) -> Path:
        return self._job_dir(operation_id) / "record.json"

    def _load(self, operation_id: str) -> dict[str, Any] | None:
        try:
            loaded: Any = json.loads(self._record_path(operation_id).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return cast(dict[str, Any], loaded) if isinstance(loaded, dict) else None

    def _workdir(self, spec: JobSpec, operation_id: str) -> str | None:
        if not spec.inputs:
            return self.config.workdir
        if self.config.workdir:
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                "inputs go in the job's own workdir; this site sets workdir, so pass files there",
            )
        workdir = self._job_dir(operation_id) / "workdir"
        workdir.mkdir(parents=True, exist_ok=True)
        for item in spec.inputs:
            self.context.content.materialize(item.sha256, workdir / item.name)
        return str(workdir)

    async def run(self, spec: JobSpec, *, operation_id: str) -> JobHandle:
        command = run_command(spec)
        for item in spec.inputs:
            self.context.content.get(item.sha256)
        workdir = self._workdir(spec, operation_id)
        record: dict[str, Any] = {
            "operation_id": operation_id,
            "name": f"relay-{operation_id}",
            "job_id": None,
            "submitted_at": time.time(),
        }
        write_json_atomic(self._record_path(operation_id), record)
        request = {
            **self.resources(spec),
            "name": record["name"],
            "run": command,
            "setup": self.config.setup,
            "envs": dict(spec.environment),
            "workdir": workdir,
            "num_nodes": spec.resources.node_count,
        }
        launched = await self.call("launch", request)
        record["job_id"] = int(launched["job_id"])
        write_json_atomic(self._record_path(operation_id), record)
        return self._handle(record)

    def _handle(self, record: dict[str, Any]) -> JobHandle:
        return JobHandle(
            adapter=self.name,
            native_id=str(record["job_id"]),
            data={"operation_id": record["operation_id"], "name": record["name"]},
        )

    async def find(self, operation_id: str) -> JobHandle | None:
        record = self._load(operation_id)
        if record is None or record.get("job_id") is None:
            return None
        return self._handle(record)

    async def _outputs(self, operation_id: str, job_id: str) -> tuple[list[OutputRef], str | None]:
        local_dir = self._job_dir(operation_id) / "logs"
        try:
            found = await self.call("logs", {"job_id": job_id, "local_dir": str(local_dir)})
        except Refusal as refusal:
            return [], f"logs not downloaded: {refusal.message}"
        outputs: list[OutputRef] = []
        paths: Any = found.get("paths") or {}
        for value in cast(dict[str, Any], paths).values():
            path = Path(str(value))
            files = (
                sorted(item for item in path.rglob("*") if item.is_file())
                if path.is_dir()
                else [path]
            )
            for item in files:
                size = item.stat().st_size if item.is_file() else None
                outputs.append(OutputRef(name=item.name, path=str(item), size=size))
        return outputs, None

    async def status(self, handle: JobHandle) -> JobStatus:
        operation_id = str(handle.data.get("operation_id", ""))
        record = self._load(operation_id) if operation_id else None
        if record is not None and isinstance(record.get("final"), dict):
            return JobStatus.model_validate(record["final"])
        if handle.native_id is None:
            raise Refusal(RefusalReason.OPERATION_NOT_FOUND, "the job has no SkyPilot job id")
        found = await self.call("queue", {"job_ids": [int(handle.native_id)]})
        jobs: Any = found.get("jobs") or []
        match = next(
            (
                cast(dict[str, Any], item)
                for item in cast(list[Any], jobs)
                if str(cast(dict[str, Any], item).get("job_id")) == handle.native_id
            ),
            None,
        )
        if match is None:
            raise Refusal(
                RefusalReason.OPERATION_NOT_FOUND,
                f"SkyPilot has no managed job {handle.native_id}",
            )
        native = str(match.get("status") or "PENDING")
        state = STATES.get(
            native, JobState.FAILED if native.startswith("FAILED") else JobState.QUEUED
        )
        reason = match.get("failure_reason")
        message = str(reason) if reason else None
        exit_code = _exit_code(state, message)
        if state == JobState.FAILED and not _EXIT_CODE.search(message or ""):
            message = f"{message or native}; SkyPilot reports no exit code, so relay records 1"
        outputs: list[OutputRef] = []
        if state.final and operation_id:
            outputs, note = await self._outputs(operation_id, handle.native_id)
            if note:
                message = f"{message}; {note}" if message else note
        status = JobStatus(
            state=state,
            exit_code=exit_code,
            native_state=native,
            message=message,
            outputs=outputs,
        )
        if state.final and record is not None:
            record["final"] = status.model_dump(mode="json")
            write_json_atomic(self._record_path(operation_id), record)
        return status

    async def cancel(self, handle: JobHandle) -> JobStatus:
        current = await self.status(handle)
        if current.state.final or handle.native_id is None:
            return current
        await self.call("cancel", {"job_ids": [int(handle.native_id)]})
        return await self.status(handle)
