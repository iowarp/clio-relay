"""A fixture adapter: simulated jobs with real persistence, for tests and demos.

Jobs move from queued to running to done on a clock, and their record is a
file, so a new relay process observes them exactly as it would a real job.
``exec`` runs a few commands in process. With ``sign_in`` set, the adapter
needs a sign-in first, which exercises relay's URL ask.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field

from clio_relay.adapters._files import LocalFileAccess
from clio_relay.adapters.base import AdapterContext, BaseAdapter, SignInNeed, SiteInfo, Verb
from clio_relay.jobs.handles import (
    ExecResult,
    FileAccess,
    JobHandle,
    JobState,
    JobStatus,
    OutputRef,
)
from clio_relay.jobs.records import write_json_atomic
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec


class FixtureConfig(BaseModel):
    """Settings for the fixture adapter."""

    model_config = ConfigDict(extra="forbid")

    queue_s: float = Field(default=0.5, ge=0, description="Seconds a job stays queued")
    run_s: float = Field(default=1.0, ge=0, description="Seconds a job runs")
    sign_in: bool = Field(default=False, description="Require a sign-in before any verb")
    queues: list[str] = Field(default_factory=lambda: ["debug", "batch"])


class FixtureAdapter(BaseAdapter[FixtureConfig]):
    """Simulated site."""

    name: ClassVar[str] = "fixture"
    Config: ClassVar[type[BaseModel]] = FixtureConfig
    implements: ClassVar[frozenset[Verb]] = frozenset({"describe", "run", "exec", "files"})

    def __init__(self, config: FixtureConfig, context: AdapterContext) -> None:
        super().__init__(config, context)
        self.root = context.state_dir
        self.jobs_root = self.root / "jobs"

    async def describe(self) -> SiteInfo:
        return SiteInfo(
            adapter=self.name,
            address="fixture.invalid",
            scheduler="fixture",
            queues=list(self.config.queues),
            accounts=["fixture-account"],
            limits={"max_nodes": 4},
            facts={"simulated": True},
            verbs=sorted(self.implements),
        )

    def _signed_in_marker(self) -> Path:
        return self.root / "signed_in"

    async def sign_in_needed(self) -> SignInNeed | None:
        if not self.config.sign_in or self._signed_in_marker().exists():
            return None
        return SignInNeed(provider="fixture", message="Sign in to the fixture site")

    async def sign_in_done(self, answer: dict[str, Any]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self._signed_in_marker().write_text(json.dumps({"at": time.time()}), encoding="utf-8")

    async def _require_sign_in(self) -> None:
        if await self.sign_in_needed() is not None:
            raise Refusal(RefusalReason.NEEDS_SIGN_IN, "sign in to the fixture site first")

    def _job_file(self, operation_id: str) -> Path:
        return self.jobs_root / operation_id / "fixture-job.json"

    async def run(self, spec: JobSpec, *, operation_id: str) -> JobHandle:
        await self._require_sign_in()
        if spec.executable is None:
            raise Refusal(RefusalReason.INVALID_SPEC, "the fixture runs an executable")
        job_dir = self.jobs_root / operation_id
        job_dir.mkdir(parents=True, exist_ok=True)
        for item in spec.inputs:
            self.context.content.materialize(item.sha256, job_dir / item.name)
        run_s = self.config.run_s
        if spec.executable == "sleep" and spec.arguments:
            try:
                run_s = float(spec.arguments[0])
            except ValueError as exc:
                raise Refusal(RefusalReason.INVALID_SPEC, "sleep takes seconds") from exc
        record = {
            "operation_id": operation_id,
            "executable": spec.executable,
            "arguments": spec.arguments,
            "submitted_at": time.time(),
            "queue_s": self.config.queue_s,
            "run_s": run_s,
            "cancelled_at": None,
        }
        write_json_atomic(self._job_file(operation_id), record)
        return JobHandle(
            adapter=self.name,
            native_id=f"fixture-{operation_id}",
            data={"job_dir": str(job_dir)},
        )

    async def find(self, operation_id: str) -> JobHandle | None:
        if not self._job_file(operation_id).is_file():
            return None
        return JobHandle(
            adapter=self.name,
            native_id=f"fixture-{operation_id}",
            data={"job_dir": str(self.jobs_root / operation_id)},
        )

    def _load(self, handle: JobHandle) -> dict[str, Any]:
        path = Path(handle.data["job_dir"]) / "fixture-job.json"
        try:
            loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise Refusal(
                RefusalReason.OPERATION_NOT_FOUND,
                f"the fixture has no job {handle.native_id}",
            ) from exc
        return loaded

    def _result(self, job: dict[str, Any], job_dir: Path) -> tuple[int, str]:
        executable = str(job["executable"])
        arguments = [str(item) for item in job["arguments"]]
        if executable == "false":
            return 1, ""
        if executable == "echo":
            return 0, " ".join(arguments) + "\n"
        if executable == "hostname":
            return 0, "fixture-node-1\n"
        if executable == "cat":
            text = ""
            for name in arguments:
                path = job_dir / name
                if not path.is_file():
                    return 1, ""
                text += path.read_text(encoding="utf-8", errors="replace")
            return 0, text
        return 0, f"fixture ran {executable} {' '.join(arguments)}".rstrip() + "\n"

    async def status(self, handle: JobHandle) -> JobStatus:
        job = self._load(handle)
        job_dir = Path(handle.data["job_dir"])
        now = time.time()
        started = float(job["submitted_at"]) + float(job["queue_s"])
        ended = started + float(job["run_s"])
        cancelled_at = job.get("cancelled_at")
        if cancelled_at is not None and float(cancelled_at) < ended:
            return JobStatus(state=JobState.CANCELLED, native_state="CA")
        if now < started:
            return JobStatus(state=JobState.QUEUED, native_state="PD")
        if now < ended:
            return JobStatus(state=JobState.RUNNING, native_state="R")
        stdout = job_dir / "stdout"
        exit_code, text = self._result(job, job_dir)
        if not stdout.exists():
            stdout.write_text(text, encoding="utf-8")
        outputs = [
            OutputRef(name=path.name, path=str(path), size=path.stat().st_size)
            for path in sorted(job_dir.iterdir())
            if path.is_file() and path.name != "fixture-job.json"
        ]
        return JobStatus(
            state=JobState.COMPLETED if exit_code == 0 else JobState.FAILED,
            exit_code=exit_code,
            native_state="CD" if exit_code == 0 else "F",
            outputs=outputs,
        )

    async def cancel(self, handle: JobHandle) -> JobStatus:
        current = await self.status(handle)
        if current.state.final:
            return current
        job = self._load(handle)
        job["cancelled_at"] = time.time()
        write_json_atomic(Path(handle.data["job_dir"]) / "fixture-job.json", job)
        return await self.status(handle)

    async def exec(
        self, command: list[str], *, timeout_s: float, max_output_bytes: int
    ) -> ExecResult:
        await self._require_sign_in()
        started = time.monotonic()
        if not command:
            raise Refusal(RefusalReason.INVALID_ARGUMENTS, "exec needs a command")
        name, arguments = command[0], command[1:]
        exit_code: int | None = 0
        stdout = ""
        stderr = ""
        timed_out = False
        if name == "echo":
            stdout = " ".join(arguments) + "\n"
        elif name == "hostname":
            stdout = "fixture-login-1\n"
        elif name == "yes":
            count = int(arguments[0]) if arguments else 100000
            stdout = "y\n" * count
        elif name == "sleep":
            seconds = float(arguments[0]) if arguments else 1.0
            if seconds > timeout_s:
                await asyncio.sleep(timeout_s)
                exit_code, timed_out = None, True
            else:
                await asyncio.sleep(seconds)
        elif name == "false":
            exit_code = 1
        else:
            exit_code, stderr = 127, f"{name}: command not found\n"
        encoded = stdout.encode()
        truncated = len(encoded) > max_output_bytes
        if truncated:
            stdout = encoded[:max_output_bytes].decode("utf-8", errors="ignore")
        return ExecResult(
            argv=command,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            stdout_truncated=truncated,
            timed_out=timed_out,
            duration_s=round(time.monotonic() - started, 3),
        )

    async def files(self) -> FileAccess:
        await self._require_sign_in()
        root = self.root / "files"
        root.mkdir(parents=True, exist_ok=True)
        return LocalFileAccess(root, self.context.content)
