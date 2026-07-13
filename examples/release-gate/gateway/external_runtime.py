"""Manage one ownership-bound process runtime for a nonscheduler validation target."""

from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

_STATE_SCHEMA_VERSION = 1
_OWNER_ENVIRONMENT_VARIABLE = "CLIO_RELAY_EXTERNAL_RUNTIME_OWNER_TOKEN"
_TOKEN_PATTERN = re.compile(r"[0-9a-f]{64}")
_RUNTIME_ID_PATTERN = re.compile(r"[0-9a-f]{32}")
_HEALTH_NONCE_PATTERN = re.compile(r"[0-9a-f]{64}")
_PROCESS_ROOT = Path("/proc")
_CANCEL_TIMEOUT_SECONDS = 15.0
_CANCEL_POLL_SECONDS = 0.1


class OwnershipError(RuntimeError):
    """Report that a live process no longer matches durable ownership state."""


@dataclass(frozen=True)
class ProcessObservation:
    """One stable Linux process identity observation."""

    pid: int
    pgid: int
    session_id: int
    start_ticks: int
    command_argv: tuple[bytes, ...]
    environment: frozenset[bytes]


@dataclass(frozen=True)
class RuntimeState:
    """Durable identity required to observe or cancel one external runtime."""

    runtime_id: str
    pid: int
    pgid: int
    session_id: int
    proc_start_ticks: int
    command_argv: tuple[str, ...]
    owner_token: str
    service_host: str
    log_path: str

    def as_json(self) -> dict[str, object]:
        """Return the private machine-readable state document."""
        return {
            "schema_version": _STATE_SCHEMA_VERSION,
            "runtime_id": self.runtime_id,
            "pid": self.pid,
            "pgid": self.pgid,
            "session_id": self.session_id,
            "proc_start_ticks": self.proc_start_ticks,
            "command_argv": list(self.command_argv),
            "owner_environment_variable": _OWNER_ENVIRONMENT_VARIABLE,
            "owner_token": self.owner_token,
            "service_host": self.service_host,
            "log_path": self.log_path,
        }


def _state_path(root: Path, runtime_id: str) -> Path:
    """Return the state path for a validated runtime identifier."""
    if _RUNTIME_ID_PATTERN.fullmatch(runtime_id) is None:
        raise ValueError("invalid runtime id")
    return root / f"{runtime_id}.json"


def _required_int(document: dict[str, Any], name: str, *, minimum: int = 1) -> int:
    """Read one strict positive integer from a state document."""
    value = document.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"runtime state field {name} must be an integer >= {minimum}")
    return value


def _required_str(document: dict[str, Any], name: str) -> str:
    """Read one nonempty string from a state document."""
    value = document.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"runtime state field {name} must be a nonempty string")
    return value


def _read_state(root: Path, runtime_id: str) -> RuntimeState:
    """Read and strictly validate one private runtime state document."""
    path = _state_path(root, runtime_id)
    metadata = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError("runtime state path must be a regular file")
    if metadata.st_mode & 0o077:
        raise ValueError("runtime state file must not be accessible by group or other users")
    current_user_id = (_PROCESS_ROOT / "self").stat().st_uid
    if metadata.st_uid != current_user_id:
        raise ValueError("runtime state file is not owned by the current user")
    value = cast(object, json.loads(path.read_text(encoding="utf-8")))
    if not isinstance(value, dict):
        raise ValueError("runtime state must be an object")
    document = {str(key): item for key, item in cast(dict[object, object], value).items()}
    if document.get("schema_version") != _STATE_SCHEMA_VERSION:
        raise ValueError("unsupported runtime state schema version")
    if document.get("runtime_id") != runtime_id:
        raise ValueError("runtime state identifier does not match its filename")
    if document.get("owner_environment_variable") != _OWNER_ENVIRONMENT_VARIABLE:
        raise ValueError("runtime state owner environment identity is invalid")
    owner_token = _required_str(document, "owner_token")
    if _TOKEN_PATTERN.fullmatch(owner_token) is None:
        raise ValueError("runtime state owner token is not a 256-bit lowercase hexadecimal value")
    raw_command = document.get("command_argv")
    if not isinstance(raw_command, list) or not raw_command:
        raise ValueError("runtime state command_argv must contain nonempty strings")
    command_items = cast(list[object], raw_command)
    if any(not isinstance(item, str) or not item for item in command_items):
        raise ValueError("runtime state command_argv must contain nonempty strings")
    command_argv = tuple(cast(str, item) for item in command_items)
    return RuntimeState(
        runtime_id=runtime_id,
        pid=_required_int(document, "pid"),
        pgid=_required_int(document, "pgid"),
        session_id=_required_int(document, "session_id"),
        proc_start_ticks=_required_int(document, "proc_start_ticks", minimum=0),
        command_argv=command_argv,
        owner_token=owner_token,
        service_host=_required_str(document, "service_host"),
        log_path=_required_str(document, "log_path"),
    )


def _write_state(root: Path, state: RuntimeState) -> None:
    """Atomically persist private runtime identity before reporting submission."""
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    destination = _state_path(root, state.runtime_id)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=root,
            prefix=f".{state.runtime_id}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            os.chmod(temporary_path, 0o600)
            json.dump(state.as_json(), temporary, sort_keys=True)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, destination)
        os.chmod(destination, 0o600)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _parse_proc_stat(content: str, *, expected_pid: int) -> tuple[int, int, int, int]:
    """Parse PID, process group, session, and start ticks from Linux proc stat."""
    closing = content.rfind(") ")
    opening = content.find("(")
    if opening <= 0 or closing <= opening:
        raise OwnershipError(f"/proc/{expected_pid}/stat has an invalid command field")
    raw_pid = content[:opening].strip()
    if raw_pid != str(expected_pid):
        raise OwnershipError(f"/proc/{expected_pid}/stat reported a different pid")
    fields = content[closing + 2 :].split()
    if len(fields) < 20:
        raise OwnershipError(f"/proc/{expected_pid}/stat is missing identity fields")
    try:
        pgid = int(fields[2])
        session_id = int(fields[3])
        start_ticks = int(fields[19])
    except ValueError as exc:
        raise OwnershipError(f"/proc/{expected_pid}/stat contains invalid identity fields") from exc
    if pgid <= 0 or session_id <= 0 or start_ticks < 0:
        raise OwnershipError(f"/proc/{expected_pid}/stat contains out-of-range identity fields")
    return expected_pid, pgid, session_id, start_ticks


def _split_nul_document(content: bytes) -> tuple[bytes, ...]:
    """Split one proc NUL document while preserving empty arguments."""
    if content.endswith(b"\0"):
        content = content[:-1]
    return tuple(content.split(b"\0")) if content else ()


def _observe_process(process_root: Path, pid: int) -> ProcessObservation | None:
    """Read one process identity twice so PID reuse during observation fails closed."""
    process_path = process_root / str(pid)
    try:
        first = _parse_proc_stat(
            (process_path / "stat").read_text(encoding="utf-8"),
            expected_pid=pid,
        )
        command = _split_nul_document((process_path / "cmdline").read_bytes())
        environment = frozenset(_split_nul_document((process_path / "environ").read_bytes()))
        second = _parse_proc_stat(
            (process_path / "stat").read_text(encoding="utf-8"),
            expected_pid=pid,
        )
    except FileNotFoundError:
        return None
    except PermissionError as exc:
        raise OwnershipError(f"permission denied while observing process {pid}") from exc
    if first != second:
        raise OwnershipError(f"process {pid} identity changed during observation")
    return ProcessObservation(
        pid=first[0],
        pgid=first[1],
        session_id=first[2],
        start_ticks=first[3],
        command_argv=command,
        environment=environment,
    )


def _expected_command_bytes(state: RuntimeState) -> tuple[bytes, ...]:
    """Encode persisted argv exactly as Linux exposes it through proc."""
    return tuple(os.fsencode(argument) for argument in state.command_argv)


def _owner_environment_entry(state: RuntimeState) -> bytes:
    """Return the exact environment entry that proves runtime ownership."""
    return os.fsencode(f"{_OWNER_ENVIRONMENT_VARIABLE}={state.owner_token}")


def _verify_leader(state: RuntimeState, observed: ProcessObservation) -> None:
    """Require every durable process identity field to match the live leader."""
    if observed.pid != state.pid:
        raise OwnershipError("runtime leader pid does not match durable ownership")
    if observed.pgid != state.pgid:
        raise OwnershipError("runtime leader process group does not match durable ownership")
    if observed.session_id != state.session_id:
        raise OwnershipError("runtime leader session does not match durable ownership")
    if observed.start_ticks != state.proc_start_ticks:
        raise OwnershipError("runtime leader start ticks do not match; refusing a reused pid")
    if observed.command_argv != _expected_command_bytes(state):
        raise OwnershipError("runtime leader exact command identity does not match")
    if _owner_environment_entry(state) not in observed.environment:
        raise OwnershipError("runtime leader owner token does not match")


def _process_group_exists(pgid: int) -> bool:
    """Use the kernel process-group lookup to prove exact group absence."""
    killpg = getattr(os, "killpg", None)
    if killpg is None:
        raise RuntimeError("external runtime ownership requires Linux process groups")
    try:
        killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError as exc:
        raise OwnershipError(f"process group {pgid} exists but cannot be observed") from exc
    return True


def _inspect_runtime(
    state: RuntimeState,
    *,
    process_root: Path = _PROCESS_ROOT,
    group_exists: Callable[[int], bool] = _process_group_exists,
) -> bool:
    """Return true only for a live runtime with exact ownership; false means absent."""
    leader = _observe_process(process_root, state.pid)
    if leader is None:
        if group_exists(state.pgid):
            raise OwnershipError(
                "runtime leader is absent but its process group still exists; "
                "ownership is unresolved"
            )
        return False
    _verify_leader(state, leader)
    if not group_exists(state.pgid):
        raise OwnershipError("runtime leader is present but its process group is absent")
    return True


def _terminate_runtime(
    state: RuntimeState,
    *,
    timeout_seconds: float,
    process_root: Path = _PROCESS_ROOT,
    group_exists: Callable[[int], bool] = _process_group_exists,
    signal_group: Callable[[int, int], None] | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Signal one verified group once, then poll exact group absence boundedly."""
    if timeout_seconds <= 0:
        raise ValueError("cancel timeout must be positive")
    if not _inspect_runtime(state, process_root=process_root, group_exists=group_exists):
        return
    selected_signal = signal_group
    if selected_signal is None:
        selected_signal = getattr(os, "killpg", None)
    if selected_signal is None:
        raise RuntimeError("external runtime cancellation requires Linux process groups")
    try:
        selected_signal(state.pgid, signal.SIGTERM)
    except ProcessLookupError:
        if not group_exists(state.pgid):
            return
        raise OwnershipError("verified process group changed before cancellation") from None
    deadline = monotonic() + timeout_seconds
    while group_exists(state.pgid):
        if monotonic() >= deadline:
            raise TimeoutError(
                f"process group {state.pgid} remained present after {timeout_seconds:g} seconds"
            )
        sleep(min(_CANCEL_POLL_SECONDS, max(0.0, deadline - monotonic())))


def _require_linux_process_contract() -> None:
    """Reject hosts that cannot provide the Linux proc/process-group contract."""
    if sys.platform != "linux" or not _PROCESS_ROOT.is_dir() or not hasattr(os, "killpg"):
        raise RuntimeError("external runtime fixture requires Linux /proc and process groups")


def _submit(args: argparse.Namespace) -> None:
    """Start a detached bounded service and persist its exact private identity."""
    _require_linux_process_contract()
    root = Path(args.state_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    runtime_id = uuid4().hex
    log_path = root / f"{runtime_id}.log"
    python_executable = str(Path(sys.executable).resolve())
    service_script = str(Path(args.service_script).expanduser().resolve())
    command = (
        python_executable,
        service_script,
        "--port",
        str(args.port),
        "--lifetime-seconds",
        str(args.lifetime_seconds),
        "--health-nonce",
        str(args.health_nonce),
    )
    owner_token = secrets.token_hex(32)
    environment = os.environ.copy()
    environment[_OWNER_ENVIRONMENT_VARIABLE] = owner_token
    process: subprocess.Popen[bytes] | None = None
    try:
        descriptor = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "ab") as log:
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                env=environment,
            )
        observed = _observe_process(_PROCESS_ROOT, process.pid)
        if observed is None:
            raise RuntimeError("external runtime exited before ownership could be recorded")
        state = RuntimeState(
            runtime_id=runtime_id,
            pid=process.pid,
            pgid=observed.pgid,
            session_id=observed.session_id,
            proc_start_ticks=observed.start_ticks,
            command_argv=command,
            owner_token=owner_token,
            service_host=socket.getfqdn(),
            log_path=str(log_path),
        )
        if state.pgid != state.pid or state.session_id != state.pid:
            raise OwnershipError(
                "detached runtime did not become its own process group and session"
            )
        _verify_leader(state, observed)
        _write_state(root, state)
    except (Exception, KeyboardInterrupt):
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        raise
    print(json.dumps({"scheduler_job_id": runtime_id}, sort_keys=True))


def _status(args: argparse.Namespace) -> None:
    """Emit status only after proving exact live identity or exact group absence."""
    _require_linux_process_contract()
    state = _read_state(Path(args.state_dir).expanduser().resolve(), args.runtime_id)
    running = _inspect_runtime(state)
    print(
        json.dumps(
            {
                "state": "running" if running else "completed",
                "service_host": state.service_host,
                "reason": None,
            },
            sort_keys=True,
        )
    )


def _cancel(args: argparse.Namespace) -> None:
    """Terminate only an exactly verified process group and prove its absence."""
    _require_linux_process_contract()
    state = _read_state(Path(args.state_dir).expanduser().resolve(), args.runtime_id)
    _terminate_runtime(state, timeout_seconds=args.timeout_seconds)


def _parser() -> argparse.ArgumentParser:
    """Build the command-line parser for the runtime driver."""
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    submit = subparsers.add_parser("submit")
    submit.add_argument("--service-script", required=True)
    submit.add_argument("--port", type=int, required=True)
    submit.add_argument("--state-dir", required=True)
    submit.add_argument("--lifetime-seconds", type=int, default=180)
    submit.add_argument("--health-nonce", required=True)
    submit.set_defaults(handler=_submit)
    status = subparsers.add_parser("status")
    status.add_argument("--state-dir", required=True)
    status.add_argument("runtime_id")
    status.set_defaults(handler=_status)
    cancel = subparsers.add_parser("cancel")
    cancel.add_argument("--state-dir", required=True)
    cancel.add_argument("--timeout-seconds", type=float, default=_CANCEL_TIMEOUT_SECONDS)
    cancel.add_argument("runtime_id")
    cancel.set_defaults(handler=_cancel)
    return parser


def main() -> None:
    """Run one validated process-runtime operation."""
    args = _parser().parse_args()
    if hasattr(args, "port") and not 1 <= args.port <= 65535:
        raise SystemExit("port must be between 1 and 65535")
    if hasattr(args, "lifetime_seconds") and not 30 <= args.lifetime_seconds <= 900:
        raise SystemExit("lifetime must be between 30 and 900 seconds")
    if hasattr(args, "health_nonce") and _HEALTH_NONCE_PATTERN.fullmatch(args.health_nonce) is None:
        raise SystemExit("health nonce must be a 256-bit lowercase hexadecimal value")
    if hasattr(args, "timeout_seconds") and not 1 <= args.timeout_seconds <= 60:
        raise SystemExit("cancel timeout must be between 1 and 60 seconds")
    try:
        args.handler(args)
    except (OSError, RuntimeError, ValueError) as exc:
        raise SystemExit(f"external runtime operation failed: {exc}") from exc


if __name__ == "__main__":
    main()
