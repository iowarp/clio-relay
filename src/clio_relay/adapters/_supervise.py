"""The detached supervisor for a job on a host with no scheduler (RL9).

Run as ``python -m clio_relay.adapters._supervise <job dir>``. It reads
``job.json``, runs the command, and writes ``state.json`` and ``exit_code`` to
the job directory, so any later relay process can poll the job. A ``cancel``
file in the job directory marks a cancel that relay asked for.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from typing import Any

POSIX = os.name == "posix"


def _now() -> str:
    return datetime.now(UTC).isoformat()


def write_state(job_dir: Path, **fields: Any) -> None:
    """Merge ``fields`` into ``state.json`` atomically."""
    path = job_dir / "state.json"
    try:
        state: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        state = {}
    state.update(fields)
    state["updated_at"] = _now()
    fd, tmp = tempfile.mkstemp(prefix=".state.", dir=job_dir)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(state, handle)
    os.replace(tmp, path)


def supervise(job_dir: Path) -> int:
    """Run the job described in ``job_dir`` to its end."""
    job: dict[str, Any] = json.loads((job_dir / "job.json").read_text(encoding="utf-8"))
    write_state(job_dir, state="queued", supervisor_pid=os.getpid(), queued_at=_now())
    if (job_dir / "cancel").exists():
        write_state(job_dir, state="cancelled", ended_at=_now())
        return 0
    env = dict(os.environ) if job.get("inherit_environment", True) else {}
    env.update({str(key): str(value) for key, value in dict(job.get("environment", {})).items()})
    cwd = job.get("directory") or str(job_dir)
    stdin_path = job.get("stdin_path")
    with (
        open(job["stdout_path"], "ab") as stdout,
        open(job["stderr_path"], "ab") as stderr,
        open(stdin_path, "rb") if stdin_path else open(os.devnull, "rb") as stdin,
    ):
        try:
            child = subprocess.Popen(
                [job["executable"], *job.get("arguments", [])],
                cwd=cwd,
                env=env,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr,
                start_new_session=POSIX,
            )
        except OSError as exc:
            stderr.write(f"relay: cannot start {job['executable']}: {exc}\n".encode())
            code = 127 if isinstance(exc, FileNotFoundError) else 126
            (job_dir / "exit_code").write_text(str(code), encoding="utf-8")
            write_state(job_dir, state="failed", exit_code=code, ended_at=_now())
            return code

        def _forward(signum: int, _frame: FrameType | None) -> None:
            try:
                if POSIX:
                    os.killpg(child.pid, signum)
                else:
                    child.terminate()
            except (ProcessLookupError, PermissionError):
                pass

        signal.signal(signal.SIGTERM, _forward)
        write_state(job_dir, state="running", child_pid=child.pid, started_at=_now())
        code = child.wait()
    (job_dir / "exit_code").write_text(str(code), encoding="utf-8")
    if (job_dir / "cancel").exists():
        final = "cancelled"
    else:
        final = "completed" if code == 0 else "failed"
    write_state(job_dir, state=final, exit_code=code, ended_at=_now())
    return 0


def main() -> None:
    """Entry point for ``python -m``."""
    if len(sys.argv) != 2:
        sys.stderr.write("usage: python -m clio_relay.adapters._supervise <job dir>\n")
        raise SystemExit(2)
    raise SystemExit(supervise(Path(sys.argv[1])))


if __name__ == "__main__":
    main()
