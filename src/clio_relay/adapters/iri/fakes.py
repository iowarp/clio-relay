"""A stand-in IRI facility for tests and offline demos.

:class:`FakeIriFacility` is an ``httpx`` transport handler that answers the IRI
Facility API calls relay makes, in either version. It follows the published
OpenAPI documents (NERSC and ALCF, versions 1.0.0 and 2.0.0): job specs with
unknown fields are rejected as the API's ``additionalProperties: false`` does,
file calls return a task to poll, and errors are RFC 9457 problem documents.
Files live under ``root`` on this machine; jobs run as local processes through
the globus adapter's :func:`relay_task`, so a fresh adapter sees them as a
restarted relay would. Nothing here is a live facility.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx

from clio_relay.adapters.globus.task import relay_task
from clio_relay.jobs.records import write_json_atomic

# Property lists of the published schemas (JobSpec, ResourceSpec, JobAttributes).
JOB_SPEC_FIELDS = frozenset(
    {
        "attributes",
        "executable",
        "container",
        "arguments",
        "directory",
        "name",
        "inherit_environment",
        "environment",
        "stdin_path",
        "stdout_path",
        "stderr_path",
        "resources",
        "pre_launch",
        "post_launch",
        "launcher",
    }
)
RESOURCE_FIELDS = frozenset(
    {
        "node_count",
        "process_count",
        "processes_per_node",
        "cpu_cores_per_process",
        "gpu_cores_per_process",
        "exclusive_node_use",
        "memory",
    }
)
ATTRIBUTE_FIELDS = frozenset(
    {"duration", "queue_name", "account", "reservation_id", "custom_attributes"}
)
_STATES = {
    "running": "active",
    "completed": "completed",
    "failed": "failed",
    "cancelled": "canceled",
}
# The v1 methods of the file calls, from the facilities' published v1 specs.
_V1_METHODS = {
    "rm": "DELETE",
    "chmod": "PUT",
    "chown": "PUT",
    "mkdir": "POST",
    "symlink": "POST",
    "compress": "POST",
    "extract": "POST",
    "mv": "POST",
    "cp": "POST",
}


def problem(status: int, title: str, detail: str) -> httpx.Response:
    """Return an RFC 9457 problem document."""
    return httpx.Response(
        status,
        json={"type": "about:blank", "status": status, "title": title, "detail": detail},
        headers={"Content-Type": "application/problem+json"},
    )


def _multipart_file(request: httpx.Request) -> bytes:
    content_type = request.headers.get("content-type", "")
    boundary = content_type.split("boundary=", 1)[-1].strip('"').encode()
    for part in request.content.split(b"--" + boundary):
        head, _, body = part.partition(b"\r\n\r\n")
        if b'name="file"' in head:
            return body.removesuffix(b"\r\n")
    return b""


@dataclass
class FakeIriFacility:
    """An IRI facility whose files are under ``root`` and whose jobs are local processes."""

    root: Path
    resource_id: str = "compute"
    filesystem_resource_id: str = "compute"
    token: str = "test-token"
    task_polls: int = 0
    requests: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])

    def transport(self) -> httpx.MockTransport:
        """Return an ``httpx`` transport that this facility answers."""
        return httpx.MockTransport(self.handle)

    @property
    def state_root(self) -> Path:
        return self.root / ".facility"

    def submitted(self) -> list[dict[str, Any]]:
        """Return the job specs submitted so far, oldest first."""
        jobs = self.state_root / "jobs"
        if not jobs.is_dir():
            return []
        found = sorted(jobs.iterdir(), key=lambda path: path.stat().st_mtime_ns)
        return [json.loads((job / "job_spec.json").read_text("utf-8")) for job in found]

    def handle(self, request: httpx.Request) -> httpx.Response:
        """Answer one request."""
        parts = request.url.path.strip("/").split("/")
        entry: dict[str, Any] = {
            "method": request.method,
            "path": request.url.path,
            "params": dict(request.url.params),
            "headers": {key.lower(): value for key, value in request.headers.items()},
        }
        self.requests.append(entry)
        if request.headers.get("authorization") != f"Bearer {self.token}":
            return problem(401, "Unauthorized", "the bearer token is missing or not valid")
        if len(parts) < 3 or parts[0] != "api" or parts[1] not in ("v1", "v2"):
            return problem(404, "Not Found", request.url.path)
        version, rest = parts[1], parts[2:]
        if request.headers.get("content-type", "").startswith("application/json"):
            entry["json"] = json.loads(request.content or b"null")
        if rest[0] == "compute":
            return self._compute(request, rest[1:])
        if rest[0] == "filesystem":
            return self._filesystem(request, version, rest[1:])
        if rest[0] == "task" and len(rest) == 2:
            return self._task_status(rest[1])
        return problem(404, "Not Found", request.url.path)

    def _compute(self, request: httpx.Request, rest: list[str]) -> httpx.Response:
        if rest == ["resources"] and request.method == "GET":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": self.resource_id,
                        "name": "fake compute",
                        "resource_type": "compute",
                        "current_status": "up",
                    }
                ],
            )
        if len(rest) == 2 and rest[0] == "job" and request.method == "POST":
            if rest[1] != self.resource_id:
                return problem(404, "Not Found", f"no resource {rest[1]}")
            return self._submit(request)
        if len(rest) == 3 and rest[0] == "status" and request.method == "GET":
            return self._job(rest[2])
        if len(rest) == 3 and rest[0] == "cancel" and request.method == "DELETE":
            return self._cancel(rest[2])
        return problem(404, "Not Found", request.url.path)

    def _submit(self, request: httpx.Request) -> httpx.Response:
        body: Any = json.loads(request.content or b"{}")
        if not isinstance(body, dict):
            return problem(422, "Unprocessable Entity", "the job spec is not an object")
        spec = cast(dict[str, Any], body)
        unknown = set(spec) - JOB_SPEC_FIELDS
        unknown |= set(cast(dict[str, Any], spec.get("resources") or {})) - RESOURCE_FIELDS
        unknown |= set(cast(dict[str, Any], spec.get("attributes") or {})) - ATTRIBUTE_FIELDS
        if unknown:
            return problem(422, "Unprocessable Entity", f"unknown fields {sorted(unknown)}")
        duration = cast(dict[str, Any], spec.get("attributes") or {}).get("duration")
        if duration is not None and not isinstance(duration, int):
            return problem(422, "Unprocessable Entity", "duration must be whole seconds")
        key = request.headers.get("idempotency-key")
        canonical = json.dumps(spec, sort_keys=True)
        key_path = self.state_root / "idempotency" / f"{key}.json"
        if key is not None and key_path.is_file():
            seen: Any = json.loads(key_path.read_text("utf-8"))
            if seen["spec"] != canonical:
                return problem(422, "Unprocessable Entity", "idempotency key reused")
            return self._job(str(seen["job_id"]))
        job_id = str(uuid.uuid4())
        if key is not None:
            write_json_atomic(key_path, {"spec": canonical, "job_id": job_id})
        job_dir = self.state_root / "jobs" / job_id
        job_dir.mkdir(parents=True)
        write_json_atomic(job_dir / "job_spec.json", spec)
        payload: dict[str, Any] = {
            "job_dir": str(job_dir),
            "argv": [spec["executable"], *spec.get("arguments", [])],
            "directory": spec.get("directory"),
            "environment": spec.get("environment", {}),
            "inherit_environment": spec.get("inherit_environment", True),
            "stdin_path": spec.get("stdin_path"),
            "stdout_path": spec.get("stdout_path"),
            "stderr_path": spec.get("stderr_path"),
            "poll_s": 0.2,
            "grace_s": 2.0,
            "operation_id": job_id,
        }
        write_json_atomic(job_dir / "payload.json", payload)
        with open(job_dir / "worker.log", "ab") as log:
            subprocess.Popen(
                [sys.executable, "-m", "clio_relay.adapters.globus.fakes", str(job_dir)],
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                start_new_session=os.name == "posix",
            )
        return self._job(job_id)

    def _job(self, job_id: str) -> httpx.Response:
        job_dir = self.state_root / "jobs" / job_id
        if not job_dir.is_dir():
            return problem(404, "Not Found", f"no job {job_id}")
        spec: Any = json.loads((job_dir / "job_spec.json").read_text("utf-8"))
        report = relay_task({"mode": "status", "job_dir": str(job_dir)})
        native = _STATES.get(str(report.get("state")), "queued")
        status: dict[str, Any] = {
            "state": native,
            "time": datetime.now(UTC).timestamp(),
            "message": report.get("message"),
            "exit_code": report.get("exit_code"),
            "meta_data": {},
        }
        return httpx.Response(200, json={"id": job_id, "status": status, "job_spec": spec})

    def _cancel(self, job_id: str) -> httpx.Response:
        job_dir = self.state_root / "jobs" / job_id
        if not job_dir.is_dir():
            return problem(404, "Not Found", f"no job {job_id}")
        relay_task({"mode": "cancel", "job_dir": str(job_dir)})
        return httpx.Response(204)

    def _local(self, path: str) -> Path | None:
        target = Path(os.path.normpath(path))
        root = Path(os.path.normpath(self.root))
        if not target.is_absolute() or (target != root and root not in target.parents):
            return None
        return target

    def _filesystem(self, request: httpx.Request, version: str, rest: list[str]) -> httpx.Response:
        if len(rest) != 2:
            return problem(404, "Not Found", request.url.path)
        command, resource = rest
        if resource != self.filesystem_resource_id:
            return problem(404, "Not Found", f"no resource {resource}")
        if command == "upload":
            args: dict[str, Any] = {"path": request.url.params.get("path", "")}
        elif version == "v2":
            if request.method != "POST":
                return problem(405, "Method Not Allowed", f"{command} is a POST in {version}")
            loaded: Any = json.loads(request.content or b"{}")
            args = cast(dict[str, Any], loaded) if isinstance(loaded, dict) else {}
        else:
            expected = _V1_METHODS.get(command, "GET")
            if request.method != expected:
                return problem(405, "Method Not Allowed", f"{command} is a {expected} in v1")
            if expected == "POST":
                loaded = json.loads(request.content or b"{}")
                args = cast(dict[str, Any], loaded) if isinstance(loaded, dict) else {}
            else:
                args = dict(request.url.params)
        target = self._local(str(args.get("path", "")))
        if target is None:
            return problem(403, "Forbidden", f"{args.get('path')} is outside the facility")
        status, result = self._run_file(command, target, request, args)
        task_id = str(uuid.uuid4())
        write_json_atomic(
            self.state_root / "tasks" / f"{task_id}.json",
            {"id": task_id, "status": status, "result": result, "command": command, "polls": 0},
        )
        code = 201 if command == "mkdir" else 200
        return httpx.Response(
            code, json={"task_id": task_id, "task_uri": f"/api/{version}/task/{task_id}"}
        )

    def _run_file(  # noqa: C901, PLR0911, PLR0912
        self, command: str, target: Path, request: httpx.Request, args: dict[str, Any]
    ) -> tuple[str, dict[str, Any]]:
        missing = ("failed", {"output": f"Error: {target}: No such file or directory"})
        present = target.exists() or target.is_symlink()
        if command == "ls":
            if not target.is_dir():
                return missing
            items: list[dict[str, Any]] = []
            for child in sorted(target.iterdir(), key=lambda item: item.name):
                info = child.lstat()
                items.append(
                    {
                        "name": child.name,
                        "type": "d" if child.is_dir() else "-",
                        "link_target": None,
                        "user": str(info.st_uid),
                        "group": str(info.st_gid),
                        "permissions": stat.filemode(info.st_mode)[1:],
                        "last_modified": datetime.fromtimestamp(info.st_mtime, UTC).isoformat(),
                        "size": str(info.st_size),
                    }
                )
            return "completed", {"output": items}
        if command == "stat":
            follow = str(args.get("dereference", "false")).lower() == "true"
            if not (target.exists() if follow else present):
                return missing
            info = target.stat() if follow else target.lstat()
            fields = (
                "mode",
                "ino",
                "dev",
                "nlink",
                "uid",
                "gid",
                "size",
                "atime",
                "ctime",
                "mtime",
            )
            return "completed", {
                "output": {name: int(getattr(info, f"st_{name}")) for name in fields}
            }
        if command == "download":
            if not target.is_file():
                return missing
            encoded = base64.b64encode(target.read_bytes()).decode("ascii")
            return "completed", {"output": {"content_base64": encoded}}
        if command == "checksum":
            if not target.is_file():
                return missing
            digest = hashlib.sha256(target.read_bytes()).hexdigest()
            return "completed", {"output": {"algorithm": "SHA-256", "checksum": digest}}
        if command == "mkdir":
            target.mkdir(parents=True, exist_ok=True)
            return "completed", {"output": {"path": str(target)}}
        if command == "upload":
            if not target.parent.is_dir():
                return missing
            target.write_bytes(_multipart_file(request))
            return "completed", {"output": "uploaded"}
        if command == "rm":
            # Like the facilities' rm: a directory goes with its contents.
            if not present:
                return missing
            if target.is_dir() and not target.is_symlink():
                shutil.rmtree(target)
            else:
                target.unlink()
            return "completed", {"output": "removed"}
        if command == "mv":
            destination = self._local(str(args.get("target_path", "")))
            if destination is None:
                return "failed", {"output": f"Error: {args.get('target_path')} is outside"}
            if not present or not destination.parent.is_dir():
                return missing
            os.replace(target, destination)
            return "completed", {"output": "moved"}
        return "failed", {"output": f"Error: {command} is not supported here"}

    def _task_status(self, task_id: str) -> httpx.Response:
        path = self.state_root / "tasks" / f"{task_id}.json"
        if not path.is_file():
            return problem(404, "Not Found", f"no task {task_id}")
        task: Any = json.loads(path.read_text("utf-8"))
        record = cast(dict[str, Any], task)
        if record["polls"] < self.task_polls:
            record["polls"] += 1
            write_json_atomic(path, record)
            return httpx.Response(
                200, json={"id": task_id, "status": "active", "result": None, "command": {}}
            )
        return httpx.Response(
            200,
            json={
                "id": task_id,
                "status": record["status"],
                "result": record["result"],
                "command": {"name": record["command"]},
            },
        )
