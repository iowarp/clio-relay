"""The function relay sends to a Globus Compute endpoint.

Globus Compute ships a function by its source, so :func:`relay_task` is one
self-contained definition: it imports only the standard library, inside its own
body, and takes one JSON-shaped ``payload``.

Each relay job is one task in ``run`` mode. The task writes the job's inputs and
a ``relay-state.json`` record into a job directory on the site, starts the
command in its own process group, and polls for a ``relay-cancel`` marker while
it waits. A ``cancel`` task writes that marker. The Compute service has no way to
cancel a task once it is submitted, so the marker is how relay stops a job. A
task that the endpoint retries finds the final record and does not run the job
again.
"""

from typing import Any


def relay_task(payload: "dict[str, Any]") -> "dict[str, Any]":  # noqa: C901, PLR0915
    """Run, cancel or report one relay job on the endpoint."""
    import base64
    import hashlib
    import json
    import os
    import signal
    import socket
    import subprocess
    import tempfile
    import time

    job_dir = os.path.normpath(os.path.expanduser(str(payload["job_dir"])))
    os.makedirs(job_dir, exist_ok=True)
    state_path = os.path.join(job_dir, "relay-state.json")
    cancel_path = os.path.join(job_dir, "relay-cancel")
    stdout_path = os.path.join(job_dir, os.path.expanduser(str(payload.get("stdout_path") or "stdout")))
    stderr_path = os.path.join(job_dir, os.path.expanduser(str(payload.get("stderr_path") or "stderr")))
    final_states = ("completed", "failed", "cancelled")

    def read_state() -> "dict[str, Any]":
        try:
            with open(state_path, encoding="utf-8") as handle:
                loaded = json.load(handle)
        except (OSError, ValueError):
            return {}
        return loaded if isinstance(loaded, dict) else {}

    def write_state(**fields: "Any") -> "dict[str, Any]":
        state = read_state()
        state.update(fields)
        state["updated_at"] = time.time()
        fd, tmp = tempfile.mkstemp(prefix=".relay-state.", dir=job_dir)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle)
        os.replace(tmp, state_path)
        return state

    def tail(path: str, limit: int = 4096) -> str:
        try:
            with open(path, "rb") as handle:
                handle.seek(0, os.SEEK_END)
                size = handle.tell()
                handle.seek(max(0, size - limit))
                return handle.read().decode("utf-8", errors="replace")
        except OSError:
            return ""

    def outputs() -> "list[dict[str, Any]]":
        found: list[dict[str, Any]] = []
        for name in sorted(os.listdir(job_dir)):
            path = os.path.join(job_dir, name)
            if name in ("relay-state.json", "relay-cancel") or name.startswith(".relay-"):
                continue
            if os.path.isfile(path):
                found.append({"name": name, "path": path, "size": os.path.getsize(path)})
        for path in (stdout_path, stderr_path):
            if os.path.dirname(path) != job_dir and os.path.isfile(path):
                name = os.path.basename(path)
                found.append({"name": name, "path": path, "size": os.path.getsize(path)})
        return found

    def report(state: "dict[str, Any]") -> "dict[str, Any]":
        return {
            "state": state.get("state", "unknown"),
            "exit_code": state.get("exit_code"),
            "message": state.get("message"),
            "host": state.get("host"),
            "job_dir": job_dir,
            "outputs": outputs(),
            "stdout_tail": tail(stdout_path),
            "stderr_tail": tail(stderr_path),
        }

    def signal_group(pid: int, signum: int) -> None:
        try:
            os.killpg(pid, signum)
        except (ProcessLookupError, PermissionError):
            pass

    mode = payload.get("mode", "run")
    if mode == "status":
        return report(read_state())
    if mode == "cancel":
        with open(cancel_path, "w", encoding="utf-8") as handle:
            handle.write(str(time.time()))
        state = read_state()
        pid = state.get("pid")
        if (
            state.get("state") == "running"
            and isinstance(pid, int)
            and state.get("host") == socket.gethostname()
        ):
            signal_group(pid, signal.SIGTERM)
        return report(state)

    state = read_state()
    if state.get("state") in final_states:
        return report(state)
    if os.path.exists(cancel_path):
        return report(write_state(state="cancelled", message="cancelled before it started"))

    for item in payload.get("inputs", []):
        name = str(item["name"])
        target = os.path.normpath(os.path.join(job_dir, name))
        if not target.startswith(job_dir + os.sep):
            return report(write_state(state="failed", message=f"input {name} leaves the job"))
        data = base64.b64decode(str(item["data"]))
        if hashlib.sha256(data).hexdigest() != item["sha256"]:
            return report(write_state(state="failed", message=f"input {name} is corrupt"))
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "wb") as handle:
            handle.write(data)

    env = dict(os.environ) if payload.get("inherit_environment", True) else {}
    env.update({str(k): str(v) for k, v in dict(payload.get("environment") or {}).items()})
    argv = [str(part) for part in payload["argv"]]
    directory = os.path.expanduser(str(payload.get("directory") or job_dir))
    stdin_path = payload.get("stdin_path")
    poll_s = float(payload.get("poll_s", 5.0))
    grace_s = float(payload.get("grace_s", 10.0))
    with (
        open(stdout_path, "ab") as stdout,
        open(stderr_path, "ab") as stderr,
        open(
            os.path.join(job_dir, os.path.expanduser(str(stdin_path))) if stdin_path else os.devnull,
            "rb",
        ) as stdin,
    ):
        try:
            child = subprocess.Popen(
                argv,
                cwd=directory,
                env=env,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
            )
        except OSError as exc:
            code = 127 if isinstance(exc, FileNotFoundError) else 126
            stderr.write(f"relay: cannot start {argv[0]}: {exc}\n".encode())
            return report(
                write_state(state="failed", exit_code=code, message=f"cannot start {argv[0]}")
            )
        write_state(
            state="running",
            pid=child.pid,
            host=socket.gethostname(),
            started_at=time.time(),
            operation_id=payload.get("operation_id"),
        )
        cancelled = False
        kill_at = 0.0
        while True:
            try:
                code = child.wait(timeout=poll_s)
                break
            except subprocess.TimeoutExpired:
                pass
            if not cancelled and os.path.exists(cancel_path):
                cancelled = True
                signal_group(child.pid, signal.SIGTERM)
                kill_at = time.monotonic() + grace_s
            elif cancelled and time.monotonic() >= kill_at:
                signal_group(child.pid, signal.SIGKILL)
    # A cancel task may have signalled the job itself before this loop saw the marker.
    cancelled = cancelled or os.path.exists(cancel_path)
    with open(os.path.join(job_dir, "exit_code"), "w", encoding="utf-8") as handle:
        handle.write(str(code))
    final = "cancelled" if cancelled else ("completed" if code == 0 else "failed")
    return report(write_state(state=final, exit_code=code, ended_at=time.time()))
