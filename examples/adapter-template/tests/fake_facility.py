"""A pretend facility with a REST API, for the template's tests.

It stands in for a real facility: an API token, a job queue and a file system,
over HTTP on 127.0.0.1. Jobs run as local processes and the site's file system
is a directory on this machine, so the conformance kit runs end to end with no
network and no account. Point the tests at your facility's sandbox instead, or
keep a fake like this one for fast tests.
"""

# pyright: reportUnusedFunction=false

from __future__ import annotations

import os
import posixpath
import subprocess
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response

USER = "ada"


@dataclass
class _Job:
    id: str
    name: str
    process: subprocess.Popen[bytes] | None
    exit_code: int | None = None
    cancelled: bool = False

    def view(self) -> dict[str, Any]:
        if self.process is not None and self.exit_code is None:
            self.exit_code = self.process.poll()
        if self.cancelled:
            state = "CANCELLED"
        elif self.exit_code is None:
            state = "RUNNING"
        else:
            state = "DONE" if self.exit_code == 0 else "FAILED"
        return {"id": self.id, "name": self.name, "state": state, "exit_code": self.exit_code}


class FakeFacility:
    """The facility. Site paths map onto ``root`` on this machine."""

    def __init__(self, root: Path, token: str) -> None:
        self.root = root
        self.token = token
        self.jobs: dict[str, _Job] = {}
        self.app = self._build()

    def host(self, path: str) -> Path:
        """Return where a site path lives on this machine."""
        return self.root / posixpath.normpath("/" + path).lstrip("/")

    def _stat(self, path: str) -> dict[str, Any]:
        host = self.host(path)
        if not host.exists():
            raise HTTPException(404, f"{path} does not exist")
        info = host.stat()
        kind = "dir" if host.is_dir() else "file"
        return {"path": path, "kind": kind, "size": info.st_size, "mtime": info.st_mtime}

    def _start(self, job_id: str, body: dict[str, Any]) -> _Job:
        workdir = self.host(body["workdir"])
        workdir.mkdir(parents=True, exist_ok=True)
        with (workdir / "stdout.txt").open("wb") as out, (workdir / "stderr.txt").open("wb") as err:
            try:
                process = subprocess.Popen(
                    body["command"],
                    cwd=workdir,
                    env={**os.environ, **body.get("env", {})},
                    stdout=out,
                    stderr=err,
                )
            except OSError:
                return _Job(job_id, body["name"], None, exit_code=127)
        return _Job(job_id, body["name"], process)

    def _build(self) -> FastAPI:
        def authorized(authorization: str = Header(default="")) -> None:
            if authorization != f"Bearer {self.token}":
                raise HTTPException(401, "bad token")

        app = FastAPI(dependencies=[Depends(authorized)])

        @app.get("/whoami")
        def whoami() -> dict[str, str]:
            return {"user": USER, "home": f"/home/{USER}", "scratch": f"/scratch/{USER}"}

        @app.post("/jobs")
        def submit(body: dict[str, Any]) -> dict[str, str]:
            job_id = str(len(self.jobs) + 1)
            self.jobs[job_id] = self._start(job_id, body)
            return {"id": job_id}

        @app.get("/jobs")
        def find(name: str) -> list[dict[str, Any]]:
            return [job.view() for job in self.jobs.values() if job.name == name]

        @app.get("/jobs/{job_id}")
        def job(job_id: str) -> dict[str, Any]:
            if job_id not in self.jobs:
                raise HTTPException(404, "no such job")
            return self.jobs[job_id].view()

        @app.delete("/jobs/{job_id}")
        def cancel(job_id: str) -> dict[str, Any]:
            found = self.jobs[job_id]
            if found.view()["state"] == "RUNNING" and found.process is not None:
                found.cancelled = True
                found.process.kill()
                found.process.wait()
            return found.view()

        @app.get("/fs/kind")
        def kind(path: str) -> dict[str, str]:
            host = self.host(path)
            if host.is_symlink():
                return {"kind": "link"}
            return {"kind": "dir" if host.is_dir() else "file" if host.exists() else "missing"}

        @app.get("/fs/stat")
        def stat(path: str) -> dict[str, Any]:
            return self._stat(path)

        @app.get("/fs/list")
        def listing(path: str) -> list[dict[str, Any]]:
            names = sorted(child.name for child in self.host(path).iterdir())
            return [self._stat(posixpath.join(path, name)) for name in names]

        @app.get("/fs/read")
        def read(path: str, offset: int = 0, limit: int = -1) -> Response:
            with self.host(path).open("rb") as handle:
                handle.seek(offset)
                return Response(handle.read(limit), media_type="application/octet-stream")

        @app.put("/fs/write")
        async def write(path: str, request: Request) -> None:
            host = self.host(path)
            host.parent.mkdir(parents=True, exist_ok=True)
            host.write_bytes(await request.body())

        @app.delete("/fs")
        def remove(path: str) -> None:
            host = self.host(path)
            if host.is_symlink() or not host.is_dir():
                host.unlink()
            elif any(host.iterdir()):
                raise HTTPException(409, f"{path} is not empty")
            else:
                host.rmdir()

        return app


@contextmanager
def running(facility: FakeFacility) -> Generator[str]:
    """Serve ``facility`` on a free port on 127.0.0.1 and yield its URL."""
    config = uvicorn.Config(facility.app, host="127.0.0.1", port=0, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    while not server.started:
        time.sleep(0.01)
    port: int = server.servers[0].sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        for job in facility.jobs.values():
            if job.process is not None and job.process.poll() is None:
                job.process.kill()
                job.process.wait()
