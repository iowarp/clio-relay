"""The site wrapper itself, run as a job would run it (RL19, RL23)."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from clio_relay.serve.plan import free_loopback_port, site_file

pytestmark = pytest.mark.release_platform("posix")

SERVICE = """
import os, signal, socket, sys
port = int(sys.argv[1])
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", port))
s.listen()
open("seen-secret.txt", "w").write(os.environ.get("SVC_SECRET", "") + "\\n")
open("frp-token-seen.txt", "w").write(os.environ.get("CLIO_RELAY_FRP_TOKEN", "-") + "\\n")
def stop(*_):
    open("final.json", "w").write("{}")
    sys.exit(0)
signal.signal(signal.SIGTERM, stop)
if len(sys.argv) > 2:
    sys.exit(int(sys.argv[2]))
signal.pause()
"""


def wait_for(check: Callable[[], bool]) -> None:
    # A ceiling only so a broken wrapper fails the test instead of hanging it.
    for _ in range(1200):
        if check():
            return
        time.sleep(0.1)
    raise AssertionError("the wrapper never reached the expected state")


class Job:
    def __init__(
        self, root: Path, port: int, *extra: str, idle: int = 0, env: dict[str, str] | None = None
    ) -> None:
        self.dir = root
        self.serve = root / "serve"
        self.serve.mkdir(parents=True)
        (self.serve / "launch.sh").write_bytes(site_file("launch.sh"))
        (self.serve / "ports").write_text(f"main {port} tcp -\n", encoding="utf-8")
        (self.serve / "secrets.env").write_text(
            "export SVC_SECRET='s3cret'\nexport CLIO_RELAY_FRP_TOKEN='tok'\n", encoding="utf-8"
        )
        (root / "service.py").write_text(SERVICE, encoding="utf-8")
        environment = {
            **os.environ,
            "CLIO_RELAY_STOP_GRACE": "30",
            "CLIO_RELAY_IDLE_S": str(idle),
            "CLIO_RELAY_OPERATION": "op_test",
            **(env or {}),
        }
        environment.pop("CLIO_RELAY_FRPC", None)
        self.process = subprocess.Popen(
            ["/bin/sh", "serve/launch.sh", "--", sys.executable, "service.py", str(port), *extra],
            cwd=root,
            env=environment,
            stdin=subprocess.DEVNULL,
        )

    def stopped(self) -> dict[str, object]:
        wait_for(lambda: (self.serve / "stopped.json").is_file())
        self.process.wait()
        return json.loads((self.serve / "stopped.json").read_text(encoding="utf-8"))


@pytest.fixture
def jobs(tmp_path: Path) -> Iterator[list[Job]]:
    started: list[Job] = []
    yield started
    for job in started:
        if job.process.poll() is None:
            job.process.kill()
            job.process.wait()


def test_ready_then_stop_lists_artifacts(tmp_path: Path, jobs: list[Job]) -> None:
    port = free_loopback_port()
    job = Job(tmp_path, port)
    jobs.append(job)
    wait_for(lambda: (job.serve / "ready.json").is_file())
    ready = json.loads((job.serve / "ready.json").read_text(encoding="utf-8"))
    assert ready["ports"] == {"main": port}
    assert not (job.serve / "secrets.env").exists()
    assert (tmp_path / "seen-secret.txt").read_text(encoding="utf-8") == "s3cret\n"
    assert (tmp_path / "frp-token-seen.txt").read_text(encoding="utf-8") == "-\n"
    (job.serve / "stop").write_text("", encoding="utf-8")
    stopped = job.stopped()
    assert stopped["reason"] == "stopped"
    assert job.process.returncode == 0
    artifacts = (job.serve / "artifacts.txt").read_text(encoding="utf-8").splitlines()
    names = {line.split("\t", 1)[1] for line in artifacts}
    assert "final.json" in names
    assert "service.py" not in names
    assert not any(name.startswith("serve/") for name in names)


def test_artifacts_list_each_file_once_without_the_schedulers_records(
    tmp_path: Path, jobs: list[Job]
) -> None:
    root = tmp_path / "real"
    root.mkdir()
    link = tmp_path / "home"
    link.symlink_to(root)
    port = free_loopback_port()
    job = Job(
        root, port, env={"CLIO_RELAY_SERVICE_DIR": str(link), "SLURM_JOB_ID": "4242"}
    )
    jobs.append(job)
    wait_for(lambda: (job.serve / "ready.json").is_file())
    (root / "4242.out").write_text("Terminated\n", encoding="utf-8")
    (root / "signalled").write_text("TERM\n", encoding="utf-8")
    (job.serve / "stop").write_text("", encoding="utf-8")
    job.stopped()
    artifacts = (job.serve / "artifacts.txt").read_text(encoding="utf-8").splitlines()
    names = [line.split("\t", 1)[1] for line in artifacts]
    assert sorted(names) == sorted(set(names)), names
    assert "final.json" in names
    assert "4242.out" not in names and "signalled" not in names


def test_a_taken_port_is_refused_before_the_program_starts(
    tmp_path: Path, jobs: list[Job]
) -> None:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen()
        port = sock.getsockname()[1]
        job = Job(tmp_path, port)
        jobs.append(job)
        stopped = job.stopped()
    assert stopped["reason"] == "port_taken"
    assert stopped["port"] == port
    assert stopped["port_name"] == "main"
    assert job.process.returncode == 98
    assert not (tmp_path / "seen-secret.txt").exists()


def test_the_program_exiting_ends_the_service(tmp_path: Path, jobs: list[Job]) -> None:
    job = Job(tmp_path, free_loopback_port(), "4")
    jobs.append(job)
    stopped = job.stopped()
    assert (stopped["reason"], stopped["exit_code"]) == ("exited", 4)


def test_idle_release_with_no_connection(tmp_path: Path, jobs: list[Job]) -> None:
    job = Job(tmp_path, free_loopback_port(), idle=1)
    jobs.append(job)
    assert job.stopped()["reason"] == "idle"
