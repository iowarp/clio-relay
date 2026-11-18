"""``sky.jobs`` of the stand-in: managed jobs as local shell processes.

Each job is a directory under ``$FAKE_SKY_ROOT/jobs/<id>`` holding its task, its
state and its log. ``python -m sky.jobs <dir>`` is the worker that runs one.
"""

from __future__ import annotations

import contextlib
import enum
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


class Request:
    """What the SDK calls return; ``sky.get`` unwraps it."""

    def __init__(self, value: Any) -> None:
        self.value = value


class ManagedJobStatus(enum.Enum):
    PENDING = "PENDING"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


def _root() -> Path:
    return Path(os.environ["FAKE_SKY_ROOT"]) / "jobs"


def _write(path: Path, value: dict[str, Any]) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(value, handle)
    os.replace(tmp, path)


def _read(path: Path) -> dict[str, Any]:
    loaded: Any = json.loads(path.read_text(encoding="utf-8"))
    return loaded


def launch(task: Any, name: str | None = None, **_: Any) -> Request:
    root = _root()
    root.mkdir(parents=True, exist_ok=True)
    job_id = 1
    while True:
        try:
            (root / str(job_id)).mkdir()
            break
        except FileExistsError:
            job_id += 1
    job_dir = root / str(job_id)
    resources = task.resources.fields if task.resources is not None else {}
    spec = {"name": name, **task.fields, "resources": resources}
    _write(job_dir / "task.json", spec)
    _write(job_dir / "state.json", {"status": "PENDING"})
    with open(job_dir / "worker.log", "ab") as log:
        subprocess.Popen(
            [sys.executable, "-m", "sky.jobs", str(job_dir)],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,
        )
    return Request(([job_id], None))


def queue_v2(refresh: bool, job_ids: list[int] | None = None, **_: Any) -> Request:
    records: list[dict[str, Any]] = []
    root = _root()
    for job_id in job_ids or []:
        job_dir = root / str(job_id)
        if not job_dir.is_dir():
            continue
        state = _read(job_dir / "state.json")
        records.append(
            {
                "job_id": job_id,
                "status": ManagedJobStatus(state["status"]),
                "failure_reason": state.get("failure_reason"),
                "cloud": "AWS",
                "region": "us-east-1",
            }
        )
    return Request((records, len(records), {}, len(records)))


def cancel(job_ids: list[int] | None = None, **_: Any) -> Request:
    for job_id in job_ids or []:
        job_dir = _root() / str(job_id)
        (job_dir / "cancel").write_text("", encoding="utf-8")
        state = _read(job_dir / "state.json")
        pid = state.get("pid")
        if isinstance(pid, int):
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(pid, signal.SIGTERM)
        if state["status"] in ("PENDING", "STARTING"):
            _write(job_dir / "state.json", {"status": "CANCELLED"})
    return Request(None)


def download_logs(
    name: str | None, job_id: int | None, refresh: bool, controller: bool, local_dir: str
) -> dict[int, str]:
    assert job_id is not None
    source = _root() / str(job_id) / "run.log"
    target = Path(local_dir).expanduser() / f"managed-{job_id}"
    target.mkdir(parents=True, exist_ok=True)
    if source.is_file():
        shutil.copyfile(source, target / "run.log")
    return {job_id: str(target)}


def _work(job_dir: Path) -> None:
    task = _read(job_dir / "task.json")
    if (job_dir / "cancel").exists():
        _write(job_dir / "state.json", {"status": "CANCELLED"})
        return
    workdir = task.get("workdir") or str(job_dir)
    env = dict(os.environ)
    env.update(task.get("envs") or {})
    with open(job_dir / "run.log", "wb") as log:
        process = subprocess.Popen(
            ["sh", "-c", task["run"]],
            cwd=workdir,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        _write(job_dir / "state.json", {"status": "RUNNING", "pid": process.pid})
        if (job_dir / "cancel").exists():
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(process.pid, signal.SIGTERM)
        code = process.wait()
    if (job_dir / "cancel").exists():
        _write(job_dir / "state.json", {"status": "CANCELLED"})
    elif code == 0:
        _write(job_dir / "state.json", {"status": "SUCCEEDED"})
    else:
        _write(job_dir / "state.json", {"status": "FAILED", "failure_reason": None})


if __name__ == "__main__":
    _work(Path(sys.argv[1]))
