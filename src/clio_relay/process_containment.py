"""Shared process-tree ownership for relay and embedded JARVIS runners."""

from __future__ import annotations

import errno
import json
import os
import shutil
import signal
import stat as stat_module
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import uuid4

CONTAINMENT_ENV = "CLIO_RELAY_PROCESS_CONTAINMENT"
CONTAINMENT_VALUE = "relay-owned-v1"
BROKER_CREDENTIAL_FD_ENV = "CLIO_RELAY_BROKER_CREDENTIAL_FD"
BROKER_READY_FD_ENV = "CLIO_RELAY_BROKER_READY_FD"
BROKER_PROTOCOL_MAX_BYTES = 16 * 1024
BROKER_HANDSHAKE_TIMEOUT_SECONDS = 5.0
BROKER_READY_TIMEOUT_SECONDS = 10.0
DISCOVERY_TIMEOUT_SECONDS = 5.0
TERMINATION_TIMEOUT_SECONDS = 10.0
POLL_SECONDS = 0.05
DISCOVERY_ROUNDS = 3


class _ResourceModule(Protocol):
    RLIMIT_CORE: int

    def setrlimit(self, resource_id: int, limits: tuple[int, int]) -> None: ...

    def getrlimit(self, resource_id: int) -> tuple[int, int]: ...


def enforce_linux_secret_memory_gate() -> None:
    """Disable core dumps and same-UID tracing before secret material exists."""
    if not sys.platform.startswith("linux"):
        raise RuntimeError("secure JARVIS runtime signing requires Linux PR_SET_DUMPABLE")
    import ctypes

    resource_module = cast(_ResourceModule, __import__("resource"))

    try:
        resource_module.setrlimit(resource_module.RLIMIT_CORE, (0, 0))
        core_limits = resource_module.getrlimit(resource_module.RLIMIT_CORE)
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"could not disable secret-bearing core dumps: {exc}") from exc
    if core_limits != (0, 0):
        raise RuntimeError(f"secret-bearing core dump limits remained enabled: {core_limits}")
    libc = ctypes.CDLL(None, use_errno=True)
    prctl = libc.prctl
    prctl.argtypes = [
        ctypes.c_int,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
    ]
    prctl.restype = ctypes.c_int
    pr_set_dumpable = 4
    pr_get_dumpable = 3
    if prctl(pr_set_dumpable, 0, 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        raise RuntimeError(f"could not disable secret-bearing process dumps: errno {error_number}")
    dumpable = prctl(pr_get_dumpable, 0, 0, 0, 0)
    if dumpable != 0:
        error_number = ctypes.get_errno()
        raise RuntimeError(
            f"secret-bearing process remained dumpable: state {dumpable}, errno {error_number}"
        )


_BROKER_SCRIPT = r"""
import json
import os
import select
import subprocess
import sys
import time

MAX_PROTOCOL_BYTES = 16 * 1024
HANDSHAKE_TIMEOUT_SECONDS = 5.0
FD_ENV = "CLIO_RELAY_BROKER_CREDENTIAL_FD"
READY_FD_ENV = "CLIO_RELAY_BROKER_READY_FD"

# Import only the relay's exact stdlib-only containment module before reading
# the setup pipe. The module root is a non-secret parent-supplied path.
module_root = sys.argv[4]
if not os.path.isabs(module_root) or not os.path.isdir(module_root):
    raise SystemExit(125)
sys.path.insert(0, module_root)
try:
    from clio_relay.process_containment import enforce_linux_secret_memory_gate
except (ImportError, RuntimeError):
    raise SystemExit(125)
if sys.platform.startswith("linux"):
    enforce_linux_secret_memory_gate()


def publish_ready(token):
    flags = (
        os.O_WRONLY
        | os.O_TRUNC
        | int(getattr(os, "O_CLOEXEC", 0))
        | int(getattr(os, "O_NOFOLLOW", 0))
    )
    descriptor = os.open(sys.argv[2], flags)
    try:
        opened = os.fstat(descriptor)
        expected = json.loads(sys.argv[3])
        observed = {
            "device": int(opened.st_dev),
            "inode": int(opened.st_ino),
            "owner": int(opened.st_uid),
            "link_count": int(opened.st_nlink),
            "mode": int(opened.st_mode & 0o7777),
        }
        if (
            not isinstance(expected, dict)
            or observed != expected
            or not os.path.isfile(sys.argv[2])
            or opened.st_nlink != 1
        ):
            raise RuntimeError("broker readiness file identity changed")
        payload = token.encode("ascii")
        if not payload or len(payload) > 128 or os.write(descriptor, payload) != len(payload):
            raise RuntimeError("broker readiness write was incomplete")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def close_fd(descriptor):
    if descriptor is None:
        return
    try:
        os.close(descriptor)
    except OSError:
        pass


raw_message = sys.stdin.buffer.readline(MAX_PROTOCOL_BYTES + 1)
if not raw_message.endswith(b"\n") or len(raw_message) > MAX_PROTOCOL_BYTES:
    raise SystemExit(125)
try:
    message = json.loads(raw_message)
    command = json.loads(sys.argv[1])
except (json.JSONDecodeError, TypeError):
    raise SystemExit(125)
if not isinstance(message, dict) or message.get("release") is not True:
    raise SystemExit(125)
credential = message.get("credential")
readiness_token = message.get("readiness_token")
if credential is not None and (os.name == "nt" or not isinstance(credential, str)):
    raise SystemExit(125)
if not isinstance(readiness_token, str) or not readiness_token.isascii() or not readiness_token:
    raise SystemExit(125)

read_fd = None
write_fd = None
ready_read_fd = None
ready_write_fd = None
process = None
try:
    popen_kwargs = {}
    if credential is not None:
        credential_bytes = credential.encode("utf-8")
        if len(credential_bytes) > MAX_PROTOCOL_BYTES:
            raise RuntimeError("broker credential exceeded its byte limit")
        read_fd, write_fd = os.pipe()
        ready_read_fd, ready_write_fd = os.pipe()
        child_env = os.environ.copy()
        child_env[FD_ENV] = str(read_fd)
        child_env[READY_FD_ENV] = str(ready_write_fd)
        popen_kwargs = {
            "env": child_env,
            "pass_fds": (read_fd, ready_write_fd),
        }
    process = subprocess.Popen(command, **popen_kwargs)
    close_fd(read_fd)
    read_fd = None
    close_fd(ready_write_fd)
    ready_write_fd = None
    if write_fd is not None:
        os.set_blocking(write_fd, False)
        view = memoryview(credential_bytes)
        deadline = time.monotonic() + HANDSHAKE_TIMEOUT_SECONDS
        while view:
            if process.poll() is not None:
                raise RuntimeError("credential consumer exited before broker readiness")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("broker credential write timed out")
            _, writable, _ = select.select([], [write_fd], [], remaining)
            if not writable:
                raise RuntimeError("broker credential write timed out")
            try:
                written = os.write(write_fd, view)
            except BlockingIOError:
                continue
            if written <= 0:
                raise RuntimeError("broker credential write made no progress")
            view = view[written:]
        close_fd(write_fd)
        write_fd = None
        os.set_blocking(ready_read_fd, False)
        deadline = time.monotonic() + HANDSHAKE_TIMEOUT_SECONDS
        while True:
            if process.poll() is not None:
                raise RuntimeError("credential consumer exited before broker readiness")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("broker readiness acknowledgement timed out")
            readable, _, _ = select.select([ready_read_fd], [], [], remaining)
            if not readable:
                raise RuntimeError("broker readiness acknowledgement timed out")
            try:
                acknowledgement = os.read(ready_read_fd, 2)
            except BlockingIOError:
                continue
            if acknowledgement != b"1":
                raise RuntimeError("broker readiness acknowledgement did not match")
            break
        close_fd(ready_read_fd)
        ready_read_fd = None
    publish_ready(readiness_token)
except BaseException:
    close_fd(read_fd)
    close_fd(write_fd)
    close_fd(ready_read_fd)
    close_fd(ready_write_fd)
    if process is not None and process.poll() is None:
        process.kill()
        process.wait()
    raise
raise SystemExit(process.wait())
"""


@dataclass(frozen=True, slots=True)
class _OwnedProcessState:
    mode: str
    enforceable: bool
    job_handle: int | None = None
    cgroup_path: Path | None = None
    systemd_unit: str | None = None


@dataclass(slots=True)
class _BrokerReadiness:
    """Pinned bounded readiness channel shared with one containment broker."""

    path: Path
    descriptor: int | None
    token: str
    device: int
    inode: int
    owner: int
    link_count: int
    mode: int

    def anchor(self) -> dict[str, int]:
        """Return the non-secret filesystem identity supplied to the broker."""
        return {
            "device": self.device,
            "inode": self.inode,
            "owner": self.owner,
            "link_count": self.link_count,
            "mode": self.mode,
        }


_OWNED_PROCESSES: dict[int, _OwnedProcessState] = {}
_OWNED_PROCESSES_LOCK = threading.Lock()


@lru_cache(maxsize=1)
def containment_capability() -> dict[str, object]:
    """Return whether this host offers kernel-enforced descendant containment."""
    if os.name == "nt":
        try:
            handle = _create_windows_job()
        except RuntimeError as exc:
            return {"mode": "windows_job_object", "enforceable": False, "reason": str(exc)}
        _close_windows_handle(handle)
        return {
            "mode": "windows_job_object",
            "enforceable": True,
            "reason": "kill-on-close Job Object available",
        }
    if sys.platform.startswith("linux"):
        return _probe_linux_systemd_scope_capability()
    return {
        "mode": "cooperative_process_group",
        "enforceable": False,
        "reason": "no supported kernel containment provider",
    }


def spawn_owned_process(
    command: list[str],
    *,
    on_ready: Callable[[int, dict[str, object]], None] | None = None,
    credential_payload: str | None = None,
    **popen_kwargs: Any,
) -> subprocess.Popen[str]:
    """Spawn a root process after establishing enforceable containment when available."""
    _validate_broker_credential_payload(credential_payload)
    capability = containment_capability()
    mode = str(capability["mode"])
    enforceable = capability.get("enforceable") is True
    if enforceable and mode == "windows_job_object":
        handle = _create_windows_job()
        process, readiness = _spawn_broker(command, popen_kwargs)
        registered = False
        try:
            _assign_windows_job(handle, process)
            _register_owned_process(
                process.pid,
                _OwnedProcessState(mode=mode, enforceable=True, job_handle=handle),
            )
            registered = True
            _notify_containment_ready(process, on_ready)
            _release_broker(
                process,
                readiness=readiness,
                credential_payload=credential_payload,
            )
        except BaseException:
            _remove_broker_readiness(readiness)
            if registered:
                terminate_owned_process(process)
                release_owned_process(process)
            else:
                process.kill()
                process.wait(timeout=TERMINATION_TIMEOUT_SECONDS)
                _close_windows_handle(handle)
            raise
        return process
    if enforceable and mode == "linux_systemd_scope":
        process, unit, scope, readiness = _spawn_linux_systemd_scope(
            command,
            popen_kwargs,
        )
        try:
            _register_owned_process(
                process.pid,
                _OwnedProcessState(
                    mode=mode,
                    enforceable=True,
                    cgroup_path=scope,
                    systemd_unit=unit,
                ),
            )
            _notify_containment_ready(process, on_ready)
            _release_broker(
                process,
                readiness=readiness,
                credential_payload=credential_payload,
            )
        except BaseException:
            _remove_broker_readiness(readiness)
            terminate_owned_process(process)
            release_owned_process(process)
            raise
        return process
    process, readiness = _spawn_broker(command, popen_kwargs)
    try:
        _register_owned_process(
            process.pid,
            _OwnedProcessState(mode="cooperative_process_group", enforceable=False),
        )
        _notify_containment_ready(process, on_ready)
        _release_broker(
            process,
            readiness=readiness,
            credential_payload=credential_payload,
        )
    except BaseException:
        _remove_broker_readiness(readiness)
        terminate_owned_process(process)
        release_owned_process(process)
        raise
    return process


def owned_process_metadata(process_id: int) -> dict[str, object]:
    """Return persisted ownership evidence for one process started by this relay."""
    with _OWNED_PROCESSES_LOCK:
        state = _OWNED_PROCESSES.get(process_id)
    if state is None:
        return {
            "mode": "unregistered",
            "enforceable": False,
            "cgroup_path": None,
        }
    return {
        "mode": state.mode,
        "enforceable": state.enforceable,
        "cgroup_path": None if state.cgroup_path is None else str(state.cgroup_path),
        "systemd_unit": state.systemd_unit,
    }


def _notify_containment_ready(
    process: subprocess.Popen[str],
    callback: Callable[[int, dict[str, object]], None] | None,
) -> None:
    if callback is not None:
        callback(process.pid, owned_process_metadata(process.pid))


def terminate_owned_process(process: subprocess.Popen[str]) -> None:
    """Terminate a registered root process through its strongest ownership provider."""
    with _OWNED_PROCESSES_LOCK:
        state = _OWNED_PROCESSES.get(process.pid)
    if state is None or not state.enforceable:
        terminate_process_tree(process, owns_group=True)
        return
    if state.job_handle is not None:
        _terminate_windows_job(state.job_handle)
        if process.poll() is None:
            process.wait(timeout=TERMINATION_TIMEOUT_SECONDS)
        if _windows_job_active_processes(state.job_handle) != 0:
            raise RuntimeError("Windows Job Object remained populated after termination")
        return
    if state.cgroup_path is not None and state.systemd_unit is not None:
        _terminate_linux_systemd_scope(state.systemd_unit, state.cgroup_path)
        if process.poll() is None:
            process.wait(timeout=TERMINATION_TIMEOUT_SECONDS)
        residual = _linux_cgroup_process_ids(state.cgroup_path)
        if residual:
            raise RuntimeError(f"Linux cgroup remained populated after termination: {residual}")
        return
    raise RuntimeError(f"invalid enforceable containment state for process {process.pid}")


def release_owned_process(process: subprocess.Popen[str]) -> None:
    """Release an empty containment provider after execution observation completes."""
    with _OWNED_PROCESSES_LOCK:
        state = _OWNED_PROCESSES.pop(process.pid, None)
    if state is None:
        return
    if state.job_handle is not None:
        _close_windows_handle(state.job_handle)
    if state.systemd_unit is not None:
        _release_linux_systemd_scope(state.systemd_unit)


def owner_environment(environment: Mapping[str, str] | None) -> dict[str, str]:
    """Return an execution environment marking one relay-owned process tree."""
    owned = dict(os.environ if environment is None else environment)
    owned[CONTAINMENT_ENV] = CONTAINMENT_VALUE
    return owned


def owner_popen_kwargs() -> dict[str, Any]:
    """Return platform flags that create the outer relay-owned process group."""
    return {
        "start_new_session": os.name != "nt",
        "creationflags": (
            int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)) if os.name == "nt" else 0
        ),
    }


def nested_popen_kwargs(environment: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Keep embedded processes in the relay group, or own a group when standalone."""
    source = os.environ if environment is None else environment
    if source.get(CONTAINMENT_ENV) == CONTAINMENT_VALUE:
        return {"start_new_session": False, "creationflags": 0}
    return owner_popen_kwargs()


def inherited_relay_containment(environment: Mapping[str, str] | None = None) -> bool:
    """Return whether the current embedded runner belongs to a relay-owned group."""
    source = os.environ if environment is None else environment
    return source.get(CONTAINMENT_ENV) == CONTAINMENT_VALUE


def terminate_process_tree(
    process: subprocess.Popen[str],
    *,
    owns_group: bool,
    timeout_seconds: float = TERMINATION_TIMEOUT_SECONDS,
) -> None:
    """Terminate and verify a process tree without signaling the caller's group."""
    if os.name == "nt":
        _terminate_windows_tree(process, timeout_seconds=timeout_seconds)
        return
    discovery_error: RuntimeError | None = None
    descendants: list[int] = []
    for round_index in range(DISCOVERY_ROUNDS):
        try:
            observed = _posix_descendant_process_ids(process.pid)
        except RuntimeError as exc:
            discovery_error = exc
            break
        descendants.extend(item for item in observed if item not in descendants)
        if round_index + 1 < DISCOVERY_ROUNDS:
            time.sleep(POLL_SECONDS)
    process_ids = [process.pid, *descendants]
    groups = [process.pid] if owns_group and process.pid != _current_posix_group() else []
    _signal_posix_tree(process_ids, groups, signal.SIGTERM)
    if process.poll() is None:
        try:
            process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            _signal_posix_tree(process_ids, groups, signal.SIGKILL)
            process.wait(timeout=timeout_seconds)
    residual = _wait_for_exit(
        process_ids=descendants,
        process_group=process.pid if owns_group else None,
        timeout_seconds=timeout_seconds,
    )
    if residual:
        _signal_posix_tree(
            residual,
            [process.pid] if owns_group and process.pid != _current_posix_group() else [],
            signal.SIGKILL,
        )
        residual = _wait_for_exit(
            process_ids=residual,
            process_group=process.pid if owns_group else None,
            timeout_seconds=timeout_seconds,
        )
    if residual:
        raise RuntimeError(f"relay-owned descendant processes survived cleanup: {residual}")
    if discovery_error is not None:
        raise RuntimeError(
            f"process tree was terminated without complete descendant discovery: {discovery_error}"
        )


def terminate_nested_process(process: subprocess.Popen[str]) -> None:
    """Terminate a child from an embedded runner without killing its relay parent."""
    terminate_process_tree(
        process,
        owns_group=not inherited_relay_containment(),
    )


def ensure_owned_process_tree_empty(process: subprocess.Popen[str]) -> None:
    """Reject a completed outer process that left owned descendants."""
    with _OWNED_PROCESSES_LOCK:
        state = _OWNED_PROCESSES.get(process.pid)
    if state is not None and state.enforceable:
        if state.job_handle is not None:
            residual_count = _windows_job_active_processes(state.job_handle)
            if residual_count == 0:
                return
            _terminate_windows_job(state.job_handle)
            raise RuntimeError(
                f"completed process left {residual_count} Windows Job Object descendants"
            )
        if state.cgroup_path is not None and state.systemd_unit is not None:
            residual = _linux_cgroup_process_ids(state.cgroup_path)
            if not residual:
                return
            _terminate_linux_systemd_scope(state.systemd_unit, state.cgroup_path)
            raise RuntimeError(f"completed process left systemd-scope descendants: {residual}")
        raise RuntimeError(f"invalid enforceable containment state for process {process.pid}")
    if os.name == "nt":
        return
    residual = _posix_process_group_ids(process.pid)
    residual = [process_id for process_id in residual if process_id != process.pid]
    if not residual:
        return
    _signal_posix_tree(residual, [process.pid], signal.SIGKILL)
    remaining = _wait_for_exit(
        process_ids=residual,
        process_group=process.pid,
        timeout_seconds=TERMINATION_TIMEOUT_SECONDS,
    )
    if remaining:
        raise RuntimeError(f"completed process left relay-owned descendants: {remaining}")
    raise RuntimeError(f"completed process left relay-owned descendants: {residual}")


def process_start_identity(process_id: int) -> str | None:
    """Return a stable per-process start identity, or ``None`` after exit."""
    if process_id <= 0:
        raise ValueError("process_id must be positive")
    if os.name == "nt":
        return _windows_process_start_identity(process_id)
    proc_stat = Path("/proc") / str(process_id) / "stat"
    try:
        raw = proc_stat.read_text(encoding="utf-8")
    except (FileNotFoundError, OSError, UnicodeDecodeError):
        raw = ""
    closing = raw.rfind(")")
    if closing >= 0:
        fields = raw[closing + 1 :].split()
        if len(fields) > 19:
            return f"linux-proc-start:{fields[19]}"
    try:
        result = subprocess.run(
            ["ps", "-p", str(process_id), "-o", "lstart="],
            check=False,
            capture_output=True,
            text=True,
            timeout=DISCOVERY_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"could not inspect process start identity {process_id}: {exc}") from exc
    value = " ".join(result.stdout.split())
    if result.returncode == 0 and value:
        return f"posix-ps-start:{value}"
    if _process_exists(process_id):
        raise RuntimeError(f"process exists but its start identity is unavailable: {process_id}")
    return None


def terminate_recorded_process_tree(
    *,
    process_id: int,
    expected_start_identity: str,
    process_group_id: int | None,
    containment_mode: str | None = None,
    systemd_unit: str | None = None,
    cgroup_path: str | None = None,
) -> None:
    """Terminate a prior worker execution while refusing a reused process id."""
    observed_identity = process_start_identity(process_id)
    if observed_identity is not None and observed_identity != expected_start_identity:
        raise RuntimeError(
            f"refused cleanup for reused process id {process_id}: "
            f"expected {expected_start_identity}, observed {observed_identity}"
        )
    if containment_mode == "linux_systemd_scope":
        if (
            systemd_unit is None
            or not systemd_unit.startswith("clio-relay-")
            or not systemd_unit.endswith(".scope")
            or cgroup_path is None
        ):
            raise RuntimeError("recorded systemd execution has invalid scope identity")
        scope = Path(cgroup_path).resolve()
        cgroup_root = Path("/sys/fs/cgroup").resolve()
        try:
            scope.relative_to(cgroup_root)
        except ValueError as exc:
            raise RuntimeError(f"recorded cgroup is outside cgroup v2: {scope}") from exc
        _terminate_linux_systemd_scope(systemd_unit, scope)
        residual = _linux_cgroup_process_ids(scope)
        if residual:
            raise RuntimeError(f"recorded systemd scope survived cleanup: {residual}")
        _release_linux_systemd_scope(systemd_unit)
        return
    if os.name == "nt":
        if observed_identity is None:
            return
        try:
            result = subprocess.run(
                ["taskkill", "/PID", str(process_id), "/T", "/F"],
                check=False,
                capture_output=True,
                text=True,
                timeout=TERMINATION_TIMEOUT_SECONDS,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimeError(f"could not terminate recorded process {process_id}: {exc}") from exc
        if result.returncode not in {0, 128}:
            raise RuntimeError(
                result.stderr.strip() or f"taskkill failed for recorded process {process_id}"
            )
        if process_start_identity(process_id) is not None:
            raise RuntimeError(f"recorded process survived cleanup: {process_id}")
        return
    if process_group_id is None or process_group_id <= 0:
        raise RuntimeError("recorded POSIX execution has no process-group identity")
    if process_group_id == _current_posix_group():
        raise RuntimeError("refused to terminate the replacement worker process group")
    residual = _posix_process_group_ids(process_group_id)
    if not residual and observed_identity is None:
        return
    targets = sorted(set([process_id, *residual]))
    _signal_posix_tree(targets, [process_group_id], signal.SIGTERM)
    residual = _wait_for_exit(
        process_ids=targets,
        process_group=process_group_id,
        timeout_seconds=TERMINATION_TIMEOUT_SECONDS,
    )
    if residual:
        _signal_posix_tree(residual, [process_group_id], signal.SIGKILL)
        residual = _wait_for_exit(
            process_ids=residual,
            process_group=process_group_id,
            timeout_seconds=TERMINATION_TIMEOUT_SECONDS,
        )
    if residual:
        raise RuntimeError(f"recorded process tree survived cleanup: {residual}")


def _register_owned_process(process_id: int, state: _OwnedProcessState) -> None:
    with _OWNED_PROCESSES_LOCK:
        if process_id in _OWNED_PROCESSES:
            raise RuntimeError(f"process containment was already registered: {process_id}")
        _OWNED_PROCESSES[process_id] = state


def _spawn_broker(
    command: list[str],
    popen_kwargs: dict[str, Any],
) -> tuple[subprocess.Popen[str], _BrokerReadiness]:
    if "stdin" in popen_kwargs:
        raise RuntimeError("owned process launch reserves stdin for containment setup")
    readiness = _precreate_broker_readiness()
    try:
        process = subprocess.Popen(
            [
                sys.executable,
                "-I",
                "-S",
                "-u",
                "-c",
                _BROKER_SCRIPT,
                json.dumps(command),
                str(readiness.path),
                json.dumps(readiness.anchor(), separators=(",", ":")),
                str(Path(__file__).resolve().parent.parent),
            ],
            **popen_kwargs,
            stdin=subprocess.PIPE,
            **owner_popen_kwargs(),
        )
    except BaseException:
        _remove_broker_readiness(readiness)
        raise
    return process, readiness


def _validate_broker_credential_payload(payload: str | None) -> None:
    """Reject secret broker transport where a POSIX pipe cannot be guaranteed."""
    if payload is None:
        return
    if os.name == "nt":
        raise RuntimeError("secure broker credential transport requires POSIX")
    if not payload or len(payload.encode("utf-8")) > BROKER_PROTOCOL_MAX_BYTES:
        raise RuntimeError("broker credential payload is empty or exceeds its byte limit")


def _release_broker(
    process: subprocess.Popen[str],
    *,
    readiness: _BrokerReadiness,
    credential_payload: str | None = None,
) -> None:
    if process.stdin is None:
        raise RuntimeError("containment broker did not expose its setup channel")
    message = json.dumps(
        {
            "release": True,
            "credential": credential_payload,
            "readiness_token": readiness.token,
        },
        separators=(",", ":"),
    )
    encoded = (message + "\n").encode("utf-8")
    if len(encoded) > BROKER_PROTOCOL_MAX_BYTES:
        raise RuntimeError("containment broker setup message exceeded its byte limit")
    setup_channel = process.stdin
    completed = threading.Event()
    errors: list[BaseException] = []

    def write_setup() -> None:
        try:
            view = memoryview(encoded)
            while view:
                written = os.write(setup_channel.fileno(), view)
                if written <= 0:
                    raise RuntimeError("containment broker setup write made no progress")
                view = view[written:]
        except BaseException as exc:
            errors.append(exc)
        finally:
            completed.set()

    writer = threading.Thread(
        target=write_setup,
        name=f"clio-relay-broker-release-{process.pid}",
        daemon=True,
    )
    writer.start()
    if not completed.wait(BROKER_HANDSHAKE_TIMEOUT_SECONDS):
        if process.poll() is None:
            process.kill()
        with suppress(OSError):
            os.close(setup_channel.fileno())
        process.stdin = None
        process.wait(timeout=TERMINATION_TIMEOUT_SECONDS)
        raise RuntimeError("containment broker setup write timed out")
    try:
        if errors:
            raise RuntimeError(f"containment broker setup write failed: {errors[0]}")
    finally:
        setup_channel.close()
        process.stdin = None
    _await_broker_readiness(process, readiness)


def _precreate_broker_readiness() -> _BrokerReadiness:
    """Create a private, pinned, bounded broker-readiness channel."""
    path = Path(tempfile.gettempdir()) / f".clio-relay-broker-{uuid4().hex}.ready"
    flags = (
        os.O_RDWR
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(path, flags, 0o600)
    try:
        os.set_inheritable(descriptor, False)
        if os.name != "nt":
            os.fchmod(descriptor, 0o600)
        opened = os.fstat(descriptor)
        mode = stat_module.S_IMODE(opened.st_mode)
        if (
            not stat_module.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or (os.name != "nt" and (opened.st_uid != os.getuid() or mode != 0o600))
        ):
            raise RuntimeError("broker readiness channel was not a private regular file")
        return _BrokerReadiness(
            path=path,
            descriptor=descriptor,
            token=uuid4().hex,
            device=int(opened.st_dev),
            inode=int(opened.st_ino),
            owner=int(opened.st_uid),
            link_count=int(opened.st_nlink),
            mode=mode,
        )
    except BaseException:
        os.close(descriptor)
        with suppress(OSError):
            path.unlink()
        raise


def _await_broker_readiness(
    process: subprocess.Popen[str],
    readiness: _BrokerReadiness,
) -> None:
    """Wait until the released child has consumed credentials or fail boundedly."""
    descriptor = readiness.descriptor
    if descriptor is None:
        raise RuntimeError("containment broker readiness channel was already closed")
    expected = readiness.token.encode("ascii")
    deadline = time.monotonic() + BROKER_READY_TIMEOUT_SECONDS
    try:
        while time.monotonic() < deadline:
            try:
                opened = os.fstat(descriptor)
                if (
                    int(opened.st_dev) != readiness.device
                    or int(opened.st_ino) != readiness.inode
                    or int(opened.st_uid) != readiness.owner
                    or int(opened.st_nlink) != readiness.link_count
                    or stat_module.S_IMODE(opened.st_mode) != readiness.mode
                ):
                    raise RuntimeError("containment broker readiness identity changed")
                os.lseek(descriptor, 0, os.SEEK_SET)
                observed = os.read(descriptor, len(expected) + 1)
                if observed == expected:
                    return
                if len(observed) > len(expected):
                    raise RuntimeError("containment broker readiness payload exceeded its bound")
            except OSError as exc:
                raise RuntimeError("containment broker readiness channel failed") from exc
            if process.poll() is not None:
                raise RuntimeError(
                    "containment broker exited before child readiness "
                    f"with return code {process.returncode}"
                )
            time.sleep(POLL_SECONDS)
        raise RuntimeError("containment broker child readiness timed out")
    finally:
        _remove_broker_readiness(readiness)


def _remove_broker_readiness(readiness: _BrokerReadiness) -> None:
    descriptor = readiness.descriptor
    if descriptor is None:
        return
    readiness.descriptor = None
    deadline = time.monotonic() + DISCOVERY_TIMEOUT_SECONDS
    try:
        while True:
            try:
                path_stat = os.stat(readiness.path, follow_symlinks=False)
                if (
                    int(path_stat.st_dev) != readiness.device
                    or int(path_stat.st_ino) != readiness.inode
                    or int(path_stat.st_uid) != readiness.owner
                    or stat_module.S_IMODE(path_stat.st_mode) != readiness.mode
                ):
                    raise RuntimeError("refused to remove a replaced broker readiness path")
                if os.name == "nt" and descriptor >= 0:
                    os.close(descriptor)
                    descriptor = -1
                readiness.path.unlink()
                return
            except FileNotFoundError as exc:
                raise RuntimeError("broker readiness path disappeared before cleanup") from exc
            except PermissionError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(POLL_SECONDS)
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _probe_linux_systemd_scope_capability() -> dict[str, object]:
    if not (Path("/sys/fs/cgroup") / "cgroup.controllers").is_file():
        return {
            "mode": "linux_systemd_scope",
            "enforceable": False,
            "reason": "the host does not expose cgroup v2",
        }
    systemd_run = shutil.which("systemd-run")
    systemctl = shutil.which("systemctl")
    if systemd_run is None or systemctl is None:
        return {
            "mode": "linux_systemd_scope",
            "enforceable": False,
            "reason": "systemd-run and systemctl are required",
        }
    unit_base = f"clio-relay-probe-{uuid4().hex}"
    command = [
        systemd_run,
        "--user",
        "--scope",
        "--quiet",
        f"--unit={unit_base}",
        "--property=Delegate=yes",
        "--property=KillMode=control-group",
        "--",
        sys.executable,
        "-c",
        "pass",
    ]
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=TERMINATION_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {
            "mode": "linux_systemd_scope",
            "enforceable": False,
            "reason": f"systemd user-scope probe failed: {exc}",
        }
    _release_linux_systemd_scope(f"{unit_base}.scope")
    if result.returncode != 0:
        return {
            "mode": "linux_systemd_scope",
            "enforceable": False,
            "reason": result.stderr.strip() or "systemd user-scope probe returned nonzero",
        }
    return {
        "mode": "linux_systemd_scope",
        "enforceable": True,
        "reason": "named systemd user scopes with cgroup-v2 delegation are available",
    }


def _spawn_linux_systemd_scope(
    command: list[str],
    popen_kwargs: dict[str, Any],
) -> tuple[subprocess.Popen[str], str, Path, _BrokerReadiness]:
    if "stdin" in popen_kwargs:
        raise RuntimeError("owned process launch reserves stdin for containment setup")
    systemd_run = shutil.which("systemd-run")
    if systemd_run is None:
        raise RuntimeError("systemd-run disappeared after containment capability probing")
    unit_base = f"clio-relay-{uuid4().hex}"
    unit = f"{unit_base}.scope"
    readiness = _precreate_broker_readiness()
    effective_kwargs = dict(popen_kwargs)
    requested_environment = effective_kwargs.get("env")
    environment_source = (
        cast(Mapping[str, str], requested_environment)
        if isinstance(requested_environment, Mapping)
        else os.environ
    )
    systemd_environment = dict(environment_source)
    for name in ("XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS"):
        if name in os.environ:
            systemd_environment.setdefault(name, os.environ[name])
    effective_kwargs["env"] = systemd_environment
    wrapped = [
        systemd_run,
        "--user",
        "--scope",
        "--quiet",
        f"--unit={unit_base}",
        "--property=Delegate=yes",
        "--property=KillMode=control-group",
        "--",
        sys.executable,
        "-I",
        "-S",
        "-u",
        "-c",
        _BROKER_SCRIPT,
        json.dumps(command),
        str(readiness.path),
        json.dumps(readiness.anchor(), separators=(",", ":")),
        str(Path(__file__).resolve().parent.parent),
    ]
    try:
        process = subprocess.Popen(
            wrapped,
            **effective_kwargs,
            stdin=subprocess.PIPE,
            start_new_session=False,
            creationflags=0,
        )
    except BaseException:
        _remove_broker_readiness(readiness)
        raise
    try:
        control_group = _wait_for_systemd_control_group(unit, process=process)
    except BaseException:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=TERMINATION_TIMEOUT_SECONDS)
        _release_linux_systemd_scope(unit)
        _remove_broker_readiness(readiness)
        raise
    scope = Path("/sys/fs/cgroup") / control_group.lstrip("/")
    if not scope.is_dir() or not (scope / "cgroup.procs").is_file():
        process.kill()
        process.wait(timeout=TERMINATION_TIMEOUT_SECONDS)
        _release_linux_systemd_scope(unit)
        _remove_broker_readiness(readiness)
        raise RuntimeError(f"systemd scope did not expose its cgroup: {control_group}")
    return process, unit, scope, readiness


def _wait_for_systemd_control_group(
    unit: str,
    *,
    process: subprocess.Popen[str],
) -> str:
    deadline = time.monotonic() + TERMINATION_TIMEOUT_SECONDS
    last_error = "unit was not observable"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            diagnostic = ""
            if process.stderr is not None:
                try:
                    diagnostic = process.stderr.read(4096).strip()
                except (OSError, ValueError):
                    diagnostic = ""
            raise RuntimeError(
                f"systemd-run exited before scope setup with return code {process.returncode}"
                + (f": {diagnostic}" if diagnostic else "")
            )
        result = _systemctl_user(
            ["show", unit, "--property=ControlGroup", "--value"],
            timeout_seconds=DISCOVERY_TIMEOUT_SECONDS,
        )
        value = result.stdout.strip()
        if result.returncode == 0 and value:
            return value
        last_error = result.stderr.strip() or last_error
        time.sleep(POLL_SECONDS)
    raise RuntimeError(f"systemd scope setup timed out: {unit}: {last_error}")


def _terminate_linux_systemd_scope(unit: str, cgroup_path: Path) -> None:
    _systemctl_user(
        ["kill", "--kill-who=all", "--signal=SIGTERM", unit],
        timeout_seconds=TERMINATION_TIMEOUT_SECONDS,
    )
    residual = _wait_for_linux_cgroup_empty(
        cgroup_path,
        timeout_seconds=TERMINATION_TIMEOUT_SECONDS,
    )
    if residual:
        kill_result = _systemctl_user(
            ["kill", "--kill-who=all", "--signal=SIGKILL", unit],
            timeout_seconds=TERMINATION_TIMEOUT_SECONDS,
        )
        if kill_result.returncode != 0 and (cgroup_path / "cgroup.kill").is_file():
            (cgroup_path / "cgroup.kill").write_text("1", encoding="ascii")
        residual = _wait_for_linux_cgroup_empty(
            cgroup_path,
            timeout_seconds=TERMINATION_TIMEOUT_SECONDS,
        )
    stop_result = _systemctl_user(
        ["stop", unit],
        timeout_seconds=TERMINATION_TIMEOUT_SECONDS,
    )
    if stop_result.returncode != 0 and residual:
        raise RuntimeError(stop_result.stderr.strip() or f"could not stop systemd scope {unit}")
    if residual:
        raise RuntimeError(f"systemd scope remained populated after cleanup: {unit}: {residual}")


def _release_linux_systemd_scope(unit: str) -> None:
    if shutil.which("systemctl") is None:
        return
    _systemctl_user(["stop", unit], timeout_seconds=DISCOVERY_TIMEOUT_SECONDS)
    _systemctl_user(["reset-failed", unit], timeout_seconds=DISCOVERY_TIMEOUT_SECONDS)


def _systemctl_user(
    arguments: list[str],
    *,
    timeout_seconds: float,
) -> subprocess.CompletedProcess[str]:
    systemctl = shutil.which("systemctl") or "systemctl"
    try:
        return subprocess.run(
            [systemctl, "--user", *arguments],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return subprocess.CompletedProcess(
            [systemctl, "--user", *arguments],
            1,
            "",
            str(exc),
        )


def _linux_cgroup_process_ids(cgroup_path: Path) -> list[int]:
    if not cgroup_path.exists():
        return []
    process_ids: set[int] = set()
    files = [cgroup_path / "cgroup.procs", *cgroup_path.glob("**/cgroup.procs")]
    if len(files) > 1024:
        raise RuntimeError(f"systemd scope exceeded cgroup traversal bound: {cgroup_path}")
    for path in files:
        try:
            lines = path.read_text(encoding="ascii").splitlines()
        except OSError as exc:
            if exc.errno in {errno.ENOENT, errno.ENODEV}:
                continue
            raise
        for line in lines:
            try:
                process_ids.add(int(line))
            except ValueError as exc:
                raise RuntimeError(f"invalid process id in {path}: {line!r}") from exc
    return sorted(process_ids)


def _wait_for_linux_cgroup_empty(
    cgroup_path: Path,
    *,
    timeout_seconds: float,
) -> list[int]:
    deadline = time.monotonic() + timeout_seconds
    residual = _linux_cgroup_process_ids(cgroup_path)
    while residual and time.monotonic() < deadline:
        time.sleep(POLL_SECONDS)
        residual = _linux_cgroup_process_ids(cgroup_path)
    return residual


def _create_windows_job() -> int:
    import ctypes
    from ctypes import wintypes

    class _BasicLimitInformation(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_longlong),
            ("PerJobUserTimeLimit", ctypes.c_longlong),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class _IoCounters(ctypes.Structure):
        _fields_ = [
            (name, ctypes.c_ulonglong)
            for name in (
                "ReadOperationCount",
                "WriteOperationCount",
                "OtherOperationCount",
                "ReadTransferCount",
                "WriteTransferCount",
                "OtherTransferCount",
            )
        ]

    class _ExtendedLimitInformation(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", _BasicLimitInformation),
            ("IoInfo", _IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    handle = kernel32.CreateJobObjectW(None, None)
    if not handle:
        raise RuntimeError(f"CreateJobObjectW failed: {ctypes.get_last_error()}")
    information = _ExtendedLimitInformation()
    information.BasicLimitInformation.LimitFlags = 0x00002000
    if not kernel32.SetInformationJobObject(
        handle,
        9,
        ctypes.byref(information),
        ctypes.sizeof(information),
    ):
        error = ctypes.get_last_error()
        kernel32.CloseHandle(handle)
        raise RuntimeError(f"SetInformationJobObject failed: {error}")
    return int(handle)


def _windows_process_start_identity(process_id: int) -> str | None:
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    handle = kernel32.OpenProcess(0x1000, False, process_id)
    if not handle:
        error = ctypes.get_last_error()
        if error == 87:
            return None
        raise RuntimeError(f"OpenProcess failed for {process_id}: {error}")
    creation = wintypes.FILETIME()
    exit_time = wintypes.FILETIME()
    kernel = wintypes.FILETIME()
    user = wintypes.FILETIME()
    try:
        exit_code = wintypes.DWORD()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
            raise RuntimeError(
                f"GetExitCodeProcess failed for {process_id}: {ctypes.get_last_error()}"
            )
        if exit_code.value != 259:
            return None
        if not kernel32.GetProcessTimes(
            handle,
            ctypes.byref(creation),
            ctypes.byref(exit_time),
            ctypes.byref(kernel),
            ctypes.byref(user),
        ):
            raise RuntimeError(
                f"GetProcessTimes failed for {process_id}: {ctypes.get_last_error()}"
            )
        value = (int(creation.dwHighDateTime) << 32) | int(creation.dwLowDateTime)
        return f"windows-filetime:{value}"
    finally:
        kernel32.CloseHandle(handle)


def _assign_windows_job(job_handle: int, process: subprocess.Popen[str]) -> None:
    import ctypes
    from ctypes import wintypes

    raw_process_handle = getattr(process, "_handle", None)
    if raw_process_handle is None:
        raise RuntimeError("Popen did not expose a Windows process handle")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    if not kernel32.AssignProcessToJobObject(job_handle, int(raw_process_handle)):
        raise RuntimeError(f"AssignProcessToJobObject failed: {ctypes.get_last_error()}")


def _terminate_windows_job(job_handle: int) -> None:
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    if not kernel32.TerminateJobObject(job_handle, 1):
        raise RuntimeError(f"TerminateJobObject failed: {ctypes.get_last_error()}")


def _windows_job_active_processes(job_handle: int) -> int:
    import ctypes
    from ctypes import wintypes

    class _AccountingInformation(ctypes.Structure):
        _fields_ = [
            ("TotalUserTime", ctypes.c_longlong),
            ("TotalKernelTime", ctypes.c_longlong),
            ("ThisPeriodTotalUserTime", ctypes.c_longlong),
            ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
            ("TotalPageFaultCount", wintypes.DWORD),
            ("TotalProcesses", wintypes.DWORD),
            ("ActiveProcesses", wintypes.DWORD),
            ("TotalTerminatedProcesses", wintypes.DWORD),
        ]

    information = _AccountingInformation()
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    if not kernel32.QueryInformationJobObject(
        job_handle,
        1,
        ctypes.byref(information),
        ctypes.sizeof(information),
        None,
    ):
        raise RuntimeError(f"QueryInformationJobObject failed: {ctypes.get_last_error()}")
    return int(information.ActiveProcesses)


def _close_windows_handle(job_handle: int) -> None:
    import ctypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    if not kernel32.CloseHandle(job_handle):
        raise RuntimeError(f"CloseHandle failed: {ctypes.get_last_error()}")


def _terminate_windows_tree(
    process: subprocess.Popen[str],
    *,
    timeout_seconds: float,
) -> None:
    if process.poll() is not None:
        return
    taskkill_error: BaseException | None = None
    try:
        result = subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        taskkill_error = exc
        result = subprocess.CompletedProcess(["taskkill", str(process.pid)], 1, "", str(exc))
    if result.returncode not in {0, 128} and process.poll() is None:
        process.kill()
    try:
        process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=timeout_seconds)
    if taskkill_error is not None or result.returncode not in {0, 128}:
        raise RuntimeError(
            result.stderr.strip()
            or f"taskkill could not prove process-tree termination: {process.pid}"
        )


def _posix_process_snapshot(*, fields: tuple[str, ...]) -> str:
    command = ["ps", "-e"]
    for field in fields:
        command.extend(["-o", f"{field}="])
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=DISCOVERY_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"could not discover descendant processes: {exc}") from exc
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "ps could not discover descendant processes")
    return result.stdout


def _posix_descendant_process_ids(root_pid: int) -> list[int]:
    children: dict[int, list[int]] = {}
    for line in _posix_process_snapshot(fields=("pid", "ppid")).splitlines():
        fields = line.split()
        if len(fields) != 2:
            continue
        try:
            pid, parent_pid = (int(field) for field in fields)
        except ValueError:
            continue
        children.setdefault(parent_pid, []).append(pid)
    descendants: list[int] = []
    pending = list(children.get(root_pid, []))
    seen = {root_pid}
    while pending:
        candidate = pending.pop()
        if candidate in seen:
            continue
        seen.add(candidate)
        descendants.append(candidate)
        pending.extend(children.get(candidate, []))
    return descendants


def _posix_process_group_ids(process_group: int) -> list[int]:
    members: list[int] = []
    for line in _posix_process_snapshot(fields=("pid", "pgid", "stat")).splitlines():
        fields = line.split()
        if len(fields) < 3:
            continue
        try:
            process_id, group_id = int(fields[0]), int(fields[1])
        except ValueError:
            continue
        if group_id == process_group and not fields[2].startswith("Z"):
            members.append(process_id)
    return members


def _signal_posix_tree(
    process_ids: list[int],
    process_groups: list[int],
    requested_signal: signal.Signals,
) -> None:
    killpg = cast(Callable[[int, int], None], vars(os)["killpg"])
    for group in process_groups:
        try:
            killpg(group, requested_signal)
        except ProcessLookupError:
            continue
    for process_id in reversed(process_ids):
        try:
            os.kill(process_id, requested_signal)
        except ProcessLookupError:
            continue


def _wait_for_exit(
    *,
    process_ids: list[int],
    process_group: int | None,
    timeout_seconds: float,
) -> list[int]:
    deadline = time.monotonic() + timeout_seconds
    residual = _residual_process_ids(process_ids, process_group=process_group)
    while residual and time.monotonic() < deadline:
        time.sleep(POLL_SECONDS)
        residual = _residual_process_ids(residual, process_group=process_group)
    return residual


def _residual_process_ids(
    process_ids: list[int],
    *,
    process_group: int | None,
) -> list[int]:
    if process_group is not None:
        return _posix_process_group_ids(process_group)
    return [process_id for process_id in process_ids if _process_exists(process_id)]


def _process_exists(process_id: int) -> bool:
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    return True


def _current_posix_group() -> int:
    getpgrp = cast(Callable[[], int], vars(os)["getpgrp"])
    return getpgrp()
