"""How the Tapis stub runs a job: the ZIP runtime's launch and cancel, on its own machine.

FORK starts the launcher in the background and cancels with `pkill -9 -P <pid>`,
then `kill -9 <pid>` only if that fails. Its exit code is the app's own
`tapisjob.exitcode`, 0 when the app wrote none, as Tapis reads it. BATCH runs the launcher in the
foreground of a batch script, after the job's cmdPrefix, and cancels as Slurm
does: TERM to the step's processes, then KILL once the batch process is gone or
after KillWait. With ``StubState.slurm`` set, a BATCH job goes to the machine's
real Slurm instead: sbatch with the batch script, and scancel to cancel it.
"""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import tarfile
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from .app import StubState


QUEUED_STATES = (
    "PENDING",
    "PROCESSING_INPUTS",
    "STAGING_INPUTS",
    "STAGING_JOB",
    "SUBMITTING_JOB",
    "QUEUED",
)
FINAL_STATES = ("FINISHED", "CANCELLED", "FAILED")
# Slurm's KillWait on Ares: the time between a cancel's TERM and its KILL.
KILL_WAIT_S = 30


def _fork_exit_code(out_dir: Path) -> int:
    """Tapis's FORK exit code: the app's tapisjob.exitcode, 0 when missing or not a number.

    The ZIP runtime's FORK wrapper never writes the file (tapis-jobs ZipRunCmd), and its
    monitor takes the app's file as written (AbstractJobMonitor.readExitCodeFile).
    """
    try:
        return int((out_dir / "tapisjob.exitcode").read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return 0


@dataclass
class StubJob:
    uuid: str
    request: dict[str, Any]
    owner: str
    status: str = "PENDING"
    last_message: str = ""
    created: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    remote_job_id: str | None = None
    process: subprocess.Popen[bytes] | None = None
    released: threading.Event = field(default_factory=threading.Event)
    in_slurm: bool = False
    history: list[str] = field(default_factory=list[str])


class JobRunner:
    """Runs a job the way the ZIP runtime does, on the stub's own machine."""

    def __init__(self, state: StubState) -> None:
        self.state = state

    def start(self, job: StubJob) -> None:
        threading.Thread(target=self._run, args=(job,), daemon=True).start()

    def _move(self, job: StubJob, status: str, message: str = "") -> bool:
        with self.state.lock:
            if job.status in FINAL_STATES:
                return False
            job.status = status
            job.history.append(status)
            if message:
                job.last_message = message
            return True

    def _run(self, job: StubJob) -> None:
        request = job.request
        system_id = str(request["execSystemId"])
        state = self.state
        try:
            for status in QUEUED_STATES[1:5]:
                if not self._move(job, status):
                    return
            app = state.apps[(str(request["appId"]), str(request["appVersion"]))]
            exec_dir = state.host_path(system_id, str(request["execSystemExecDir"]))
            out_dir = state.host_path(system_id, str(request["execSystemOutputDir"]))
            exec_dir.mkdir(parents=True, exist_ok=True)
            out_dir.mkdir(parents=True, exist_ok=True)
            image = state.host_path(system_id, str(app["containerImage"]))
            with tarfile.open(image) as archive:
                archive.extractall(exec_dir, filter="data")
            if not self._move(job, "QUEUED"):
                return
            job.remote_job_id = str(10_000 + len(state.jobs))
            while state.hold and not job.released.is_set():
                if job.status in FINAL_STATES:
                    return
                job.released.wait(0.05)
            if not self._move(job, "RUNNING"):
                return
            params = cast(dict[str, Any], request.get("parameterSet") or {})
            app_args = cast(list[dict[str, Any]], params.get("appArgs") or [])
            args = [str(item["arg"]) for item in app_args]
            env = dict(os.environ)
            for item in cast(list[dict[str, Any]], params.get("envVariables") or []):
                env[str(item["key"])] = str(item.get("value") or "")
            env.update(
                {
                    "_tapisJobUUID": job.uuid,
                    "_tapisExecSystemExecDir": str(exec_dir),
                    "_tapisExecSystemOutputDir": str(out_dir),
                    "_tapisExecSystemInputDir": str(
                        state.host_path(system_id, str(request["execSystemInputDir"]))
                    ),
                    "_tapisSysRootDir": str(state.systems[system_id].get("rootDir") or "/"),
                }
            )
            script = exec_dir / "tapisjob_app.sh"
            script.chmod(0o755)
            # Tapis joins the app arguments with spaces in its wrapper script.
            command = f"./tapisjob_app.sh {' '.join(args)}"
            if request.get("jobType") == "BATCH" and state.slurm:
                code = self._sbatch(job, state.systems[system_id], exec_dir, out_dir, env, command)
                (out_dir / "tapisjob.exitcode").write_text(f"{code}\n", encoding="utf-8")
            elif request.get("jobType") == "BATCH":
                code = self._local(job, exec_dir, out_dir, env, command)
                (out_dir / "tapisjob.exitcode").write_text(f"{code}\n", encoding="utf-8")
            else:
                self._local(job, exec_dir, out_dir, env, command)
                code = _fork_exit_code(out_dir)
            with state.lock:
                if job.status == "CANCELLED":
                    return
            if code != 0:
                self._move(job, "FAILED", f"application exited with code {code}")
                return
            if not self._move(job, "ARCHIVING"):
                return
            self._move(job, "FINISHED", "job finished")
        except Exception as exc:  # the stub reports any failure as Tapis would
            self._move(job, "FAILED", f"stub job failure: {exc}")

    def _local(
        self, job: StubJob, exec_dir: Path, out_dir: Path, env: dict[str, str], command: str
    ) -> int:
        """Run the job's wrapper on this machine and wait for it."""
        batch = job.request.get("jobType") == "BATCH"
        if batch:
            # The batch script runs the launcher in the foreground, after the
            # job's cmdPrefix. The trailing line keeps bash from exec'ing it.
            prefix = str(job.request.get("cmdPrefix") or "").strip()
            wrapper = f"{prefix + ' ' if prefix else ''}{command}\nexit $?\n"
        else:
            # FORK runs the launcher itself in the background and keeps its pid.
            wrapper = f"exec {command}"
        with (out_dir / "tapisjob.out").open("wb") as log:
            process = subprocess.Popen(
                ["/bin/bash", "-c", wrapper],
                cwd=exec_dir,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            job.process = process
            code = process.wait()
        if batch and job.status == "CANCELLED":
            # Slurm ends the step once its batch process is gone.
            self._kill_group(process.pid)
        return code

    def _sbatch(
        self,
        job: StubJob,
        system: dict[str, Any],
        exec_dir: Path,
        out_dir: Path,
        env: dict[str, str],
        command: str,
    ) -> int:
        """Submit the job's batch script to Slurm, as Tapis does, and wait for it to leave."""
        request = job.request
        queues = {
            str(item["name"]): item
            for item in cast(list[dict[str, Any]], system.get("batchLogicalQueues") or [])
        }
        name = request.get("execSystemLogicalQueue") or system.get("batchDefaultLogicalQueue")
        queue = queues.get(str(name), {})
        prefix = str(request.get("cmdPrefix") or "").strip()
        lines = [
            "#!/bin/bash",
            f"#SBATCH --job-name=tapis-stub-{job.uuid[:8]}",
            f"#SBATCH --nodes={int(request.get('nodeCount') or 1)}",
            f"#SBATCH --time={int(request['maxMinutes'])}",
            f"#SBATCH --output={out_dir / 'tapisjob.out'}",
            f"#SBATCH --chdir={exec_dir}",
        ]
        if queue:
            lines.append(f"#SBATCH --partition={queue.get('hpcQueueName') or queue['name']}")
        lines.append(f"{prefix + ' ' if prefix else ''}{command}")
        script = exec_dir / "tapisjob.sh"
        script.write_text("\n".join(lines) + "\n", encoding="utf-8")
        submitted = subprocess.run(
            ["sbatch", "--parsable", str(script)],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        job.remote_job_id = submitted.stdout.strip().split(";")[0]
        job.in_slurm = True
        while True:
            seen = subprocess.run(
                ["squeue", "-h", "-j", job.remote_job_id, "-o", "%T"],
                capture_output=True,
                text=True,
                check=False,
            )
            if seen.returncode == 0 and not seen.stdout.strip():
                break
            if seen.returncode != 0 and "Invalid job id" in seen.stderr:
                break
            time.sleep(1)
        exit_file = out_dir / "relay.exitcode"
        return int(exit_file.read_text().strip()) if exit_file.is_file() else 1

    def cancel(self, job: StubJob) -> None:
        with self.state.lock:
            if job.status in FINAL_STATES:
                return
            job.status = "CANCELLED"
            job.history.append("CANCELLED")
            job.last_message = "job cancelled by user"
        job.released.set()
        if job.in_slurm and job.remote_job_id:
            subprocess.run(["scancel", job.remote_job_id], check=False)
            return
        process = job.process
        if process is None or process.poll() is not None:
            return
        if job.request.get("jobType") == "BATCH":
            # scancel: TERM to the step's processes, KILL after KillWait.
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
            timer = threading.Timer(KILL_WAIT_S, self._kill_group, args=(process.pid,))
            timer.daemon = True
            timer.start()
            return
        # Tapis's FORK cancel: `pkill -9 -P <pid>`, and `kill -9 <pid>` only if that fails.
        killed = subprocess.run(["pkill", "-9", "-P", str(process.pid)], check=False)
        if killed.returncode != 0:
            with contextlib.suppress(ProcessLookupError):
                os.kill(process.pid, signal.SIGKILL)

    @staticmethod
    def _kill_group(pgid: int) -> None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(pgid, signal.SIGKILL)
