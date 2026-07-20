"""Shared process-tree ownership for relay and embedded JARVIS runners."""

from __future__ import annotations

import base64
import errno
import json
import math
import os
import re
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
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import uuid4

CONTAINMENT_ENV = "CLIO_RELAY_PROCESS_CONTAINMENT"
CONTAINMENT_VALUE = "relay-owned-v1"
BROKER_CHILD_ENVIRONMENT_SCHEMA = "clio-relay.child-environment.v1"
BROKER_CREDENTIAL_FD_ENV = "CLIO_RELAY_BROKER_CREDENTIAL_FD"
BROKER_READY_FD_ENV = "CLIO_RELAY_BROKER_READY_FD"
BROKER_PROTOCOL_MAX_BYTES = 16 * 1024
BROKER_STDIN_MAX_BYTES = 4 * 1024 * 1024
BROKER_SETUP_MAX_BYTES = 6 * 1024 * 1024
BROKER_HANDSHAKE_TIMEOUT_SECONDS = 5.0
BROKER_READY_TIMEOUT_SECONDS = 10.0
DISCOVERY_TIMEOUT_SECONDS = 5.0
TERMINATION_TIMEOUT_SECONDS = 10.0
POLL_SECONDS = 0.05
DISCOVERY_ROUNDS = 3
SYSTEMCTL_OUTPUT_MAX_BYTES = 64 * 1024


class _ResourceModule(Protocol):
    RLIMIT_CORE: int

    def setrlimit(self, resource_id: int, limits: tuple[int, int]) -> None: ...

    def getrlimit(self, resource_id: int) -> tuple[int, int]: ...


class OwnedProcessSpawnError(RuntimeError):
    """Safe ownership evidence for a contained process that failed during startup."""

    def __init__(
        self,
        *,
        process_id: int,
        mode: str,
        cleanup_errors: list[str],
        cause: BaseException,
    ) -> None:
        self.process_id = process_id
        self.mode = mode
        self.cleanup_verified = not cleanup_errors
        self.cleanup_errors = tuple(cleanup_errors)
        self.startup_error_type = type(cause).__name__
        self.startup_error_message = str(cause)
        detail = ",".join(cleanup_errors) if cleanup_errors else "none"
        super().__init__(
            "owned process startup failed: "
            f"cause={self.startup_error_type}: {self.startup_error_message}; "
            f"pid={process_id} mode={mode} cleanup_verified={self.cleanup_verified} "
            f"cleanup_errors={detail}"
        )


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


def broker_child_environment_payload(environment: Mapping[str, str]) -> str:
    """Encode validated child-only environment values for the gated broker pipe."""
    validated: dict[str, str] = {}
    for name, value in environment.items():
        if not name or "=" in name or "\x00" in name or "\x00" in value:
            raise RuntimeError("broker child environment contained an invalid entry")
        validated[name] = value
    payload = json.dumps(
        {
            "schema_version": BROKER_CHILD_ENVIRONMENT_SCHEMA,
            "environment": validated,
        },
        allow_nan=False,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    if not validated or len(payload.encode("utf-8")) > BROKER_PROTOCOL_MAX_BYTES:
        raise RuntimeError("broker child environment payload was empty or exceeded its byte limit")
    return payload


def consume_broker_child_environment() -> bool:
    """Consume child-only environment values only after disabling Linux process inspection."""
    credential_fd_text = os.environ.get(BROKER_CREDENTIAL_FD_ENV)
    ready_fd_text = os.environ.get(BROKER_READY_FD_ENV)
    if credential_fd_text is None and ready_fd_text is None:
        return False
    if credential_fd_text is None or ready_fd_text is None:
        raise RuntimeError("broker child environment descriptors were incomplete")
    enforce_linux_secret_memory_gate()
    os.environ.pop(BROKER_CREDENTIAL_FD_ENV, None)
    os.environ.pop(BROKER_READY_FD_ENV, None)
    try:
        credential_fd = int(credential_fd_text)
        ready_fd = int(ready_fd_text)
    except ValueError:
        raise RuntimeError("broker child environment descriptors were invalid") from None
    payload = bytearray()
    try:
        while True:
            chunk = os.read(credential_fd, min(4096, BROKER_PROTOCOL_MAX_BYTES + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
            if len(payload) > BROKER_PROTOCOL_MAX_BYTES:
                raise RuntimeError("broker child environment payload exceeded its byte limit")
    except OSError:
        raise RuntimeError("broker child environment payload could not be read") from None
    finally:
        with suppress(OSError):
            os.close(credential_fd)
    try:
        decoded = cast(
            object,
            json.loads(
                payload.decode("utf-8"),
                object_pairs_hook=_reject_broker_duplicate_keys,
            ),
        )
        if not isinstance(decoded, dict):
            raise ValueError
        raw_document = cast(dict[object, object], decoded)
        if set(raw_document) != {"schema_version", "environment"}:
            raise ValueError
        document = cast(dict[str, object], raw_document)
        if document.get("schema_version") != BROKER_CHILD_ENVIRONMENT_SCHEMA or not isinstance(
            document.get("environment"), dict
        ):
            raise ValueError
        environment = cast(dict[object, object], document["environment"])
        if not environment:
            raise ValueError
        validated: dict[str, str] = {}
        for raw_name, raw_value in environment.items():
            if (
                not isinstance(raw_name, str)
                or not raw_name
                or "=" in raw_name
                or "\x00" in raw_name
                or not isinstance(raw_value, str)
                or "\x00" in raw_value
            ):
                raise ValueError
            validated[raw_name] = raw_value
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError):
        raise RuntimeError("broker child environment payload was invalid") from None
    os.environ.update(validated)
    try:
        if os.write(ready_fd, b"1") != 1:
            raise RuntimeError("broker child environment acknowledgement was incomplete")
    except OSError:
        raise RuntimeError("broker child environment acknowledgement failed") from None
    finally:
        with suppress(OSError):
            os.close(ready_fd)
    return True


def _reject_broker_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


_BROKER_SCRIPT = r"""
import base64
import binascii
import json
import os
import select
import stat
import subprocess
import sys
import time

MAX_CREDENTIAL_BYTES = 16 * 1024
MAX_STDIN_BYTES = 4 * 1024 * 1024
MAX_SETUP_BYTES = 6 * 1024 * 1024
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
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
        ):
            raise RuntimeError("broker readiness file identity changed")
        payload = token.encode("ascii")
        if not payload or len(payload) > 128:
            raise RuntimeError("broker readiness payload was invalid")
        os.ftruncate(descriptor, 0)
        if os.write(descriptor, payload) != len(payload):
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


raw_message = sys.stdin.buffer.readline(MAX_SETUP_BYTES + 1)
if not raw_message.endswith(b"\n") or len(raw_message) > MAX_SETUP_BYTES:
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
stdin_payload_encoded = message.get("stdin_payload")
interactive_stdin = message.get("interactive_stdin")
target_environment = message.get("target_environment")
if credential is not None and (os.name == "nt" or not isinstance(credential, str)):
    raise SystemExit(125)
if not isinstance(readiness_token, str) or not readiness_token.isascii() or not readiness_token:
    raise SystemExit(125)
if stdin_payload_encoded is not None and not isinstance(stdin_payload_encoded, str):
    raise SystemExit(125)
if not isinstance(interactive_stdin, bool):
    raise SystemExit(125)
if interactive_stdin and stdin_payload_encoded is not None:
    raise SystemExit(125)
if target_environment is not None:
    if os.name != "nt" or credential is not None or not isinstance(target_environment, dict):
        raise SystemExit(125)
    if not target_environment:
        raise SystemExit(125)
    for environment_name, environment_value in target_environment.items():
        if (
            not isinstance(environment_name, str)
            or not environment_name
            or "=" in environment_name
            or "\x00" in environment_name
            or not isinstance(environment_value, str)
            or "\x00" in environment_value
        ):
            raise SystemExit(125)
try:
    stdin_payload = (
        None
        if stdin_payload_encoded is None
        else base64.b64decode(stdin_payload_encoded.encode("ascii"), validate=True)
    )
except (UnicodeEncodeError, binascii.Error):
    raise SystemExit(125)
if stdin_payload is not None and len(stdin_payload) > MAX_STDIN_BYTES:
    raise SystemExit(125)

read_fd = None
write_fd = None
ready_read_fd = None
ready_write_fd = None
process = None
try:
    popen_kwargs = {}
    if target_environment is not None:
        child_env = os.environ.copy()
        child_env.update(target_environment)
        popen_kwargs["env"] = child_env
    if credential is not None:
        credential_bytes = credential.encode("utf-8")
        if len(credential_bytes) > MAX_CREDENTIAL_BYTES:
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
    process = subprocess.Popen(
        command,
        **popen_kwargs,
        stdin=(
            subprocess.PIPE
            if stdin_payload is not None or interactive_stdin
            else subprocess.DEVNULL
        ),
    )
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
    if stdin_payload is not None:
        if process.stdin is None:
            raise RuntimeError("stdin consumer did not expose its input pipe")
        process.stdin.write(stdin_payload)
        process.stdin.close()
    elif interactive_stdin:
        if process.stdin is None:
            raise RuntimeError("interactive stdin consumer did not expose its input pipe")
        while True:
            chunk = os.read(sys.stdin.fileno(), 64 * 1024)
            if not chunk:
                break
            process.stdin.write(chunk)
            process.stdin.flush()
        process.stdin.close()
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
    systemd_invocation_id: str | None = None
    systemd_description: str | None = None


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
_OWNED_PROCESSES_RELEASING: set[int] = set()
_containment_capability_cache: dict[str, object] | None = None
_CONTAINMENT_CAPABILITY_LOCK = threading.Lock()


def containment_capability(*, startup_deadline: float | None = None) -> dict[str, object]:
    """Return whether this host offers kernel-enforced descendant containment."""
    global _containment_capability_cache
    with _CONTAINMENT_CAPABILITY_LOCK:
        cached = _containment_capability_cache
    if cached is not None:
        return dict(cached)
    result: dict[str, object]
    if os.name == "nt":
        try:
            handle = _create_windows_job()
        except RuntimeError as exc:
            result = {
                "mode": "windows_job_object",
                "enforceable": False,
                "reason": str(exc),
            }
            with _CONTAINMENT_CAPABILITY_LOCK:
                _containment_capability_cache = result
            return dict(result)
        _close_windows_handle(handle)
        result = {
            "mode": "windows_job_object",
            "enforceable": True,
            "reason": "kill-on-close Job Object available",
        }
        with _CONTAINMENT_CAPABILITY_LOCK:
            _containment_capability_cache = result
        return dict(result)
    if sys.platform.startswith("linux"):
        result = _probe_linux_systemd_scope_capability(startup_deadline=startup_deadline)
        if result.pop("transient", False) is not True:
            with _CONTAINMENT_CAPABILITY_LOCK:
                _containment_capability_cache = result
        return dict(result)
    result = {
        "mode": "cooperative_process_group",
        "enforceable": False,
        "reason": "no supported kernel containment provider",
    }
    with _CONTAINMENT_CAPABILITY_LOCK:
        _containment_capability_cache = result
    return dict(result)


def spawn_owned_process(
    command: list[str],
    *,
    on_ready: Callable[[int, dict[str, object]], None] | None = None,
    credential_payload: str | None = None,
    credential_payload_factory: Callable[[int, dict[str, object]], str] | None = None,
    stdin_payload: bytes | None = None,
    interactive_stdin: bool = False,
    target_environment: Mapping[str, str] | None = None,
    startup_timeout_seconds: float = BROKER_READY_TIMEOUT_SECONDS,
    require_enforceable: bool = False,
    linux_systemd_unit_base: str | None = None,
    linux_systemd_description: str | None = None,
    **popen_kwargs: Any,
) -> subprocess.Popen[str]:
    """Spawn a root process after establishing enforceable containment when available."""
    if not math.isfinite(startup_timeout_seconds) or startup_timeout_seconds <= 0:
        raise ValueError("owned process startup timeout must be finite and positive")
    startup_deadline = time.monotonic() + startup_timeout_seconds
    if credential_payload is not None and credential_payload_factory is not None:
        raise ValueError("owned process credential payload sources are mutually exclusive")
    _validate_broker_credential_payload(credential_payload)
    validated_target_environment = _validate_broker_target_environment(target_environment)
    if interactive_stdin and stdin_payload is not None:
        raise ValueError("interactive owned process stdin cannot include a fixed payload")
    capability = containment_capability(startup_deadline=startup_deadline)
    mode = str(capability["mode"])
    enforceable = capability.get("enforceable") is True
    if require_enforceable and not enforceable:
        raise RuntimeError("enforceable owned process containment is unavailable")
    if credential_payload_factory is not None and not (
        enforceable and mode == "linux_systemd_scope"
    ):
        raise RuntimeError("deferred credential payload requires Linux systemd containment")
    if linux_systemd_unit_base is not None:
        if not (
            sys.platform.startswith("linux")
            and re.fullmatch(r"clio-relay-session-[A-Za-z0-9_-]+", linux_systemd_unit_base)
        ):
            raise ValueError("persistent Linux systemd unit identity is invalid")
        if mode != "linux_systemd_scope" or not enforceable:
            raise RuntimeError("persistent Linux session containment requires a systemd user scope")
    if linux_systemd_description is not None and (
        not linux_systemd_description
        or len(linux_systemd_description.encode("utf-8")) > 512
        or "\x00" in linux_systemd_description
        or "\n" in linux_systemd_description
    ):
        raise ValueError("persistent Linux systemd description is invalid")
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
                stdin_payload=stdin_payload,
                interactive_stdin=interactive_stdin,
                target_environment=validated_target_environment,
                startup_deadline=startup_deadline,
            )
        except BaseException as exc:
            cleanup_errors = _cleanup_failed_owned_spawn(
                process,
                readiness=readiness,
                registered=registered,
                unregistered_windows_handle=None if registered else handle,
            )
            raise OwnedProcessSpawnError(
                process_id=process.pid,
                mode=mode,
                cleanup_errors=cleanup_errors,
                cause=exc,
            ) from exc
        return process
    if enforceable and mode == "linux_systemd_scope":
        process, unit, scope, invocation_id, description, readiness = _spawn_linux_systemd_scope(
            command,
            popen_kwargs,
            startup_deadline=startup_deadline,
            unit_base=linux_systemd_unit_base,
            description=linux_systemd_description,
        )
        registered = False
        try:
            _register_owned_process(
                process.pid,
                _OwnedProcessState(
                    mode=mode,
                    enforceable=True,
                    cgroup_path=scope,
                    systemd_unit=unit,
                    systemd_invocation_id=invocation_id,
                    systemd_description=description,
                ),
            )
            registered = True
            metadata = owned_process_metadata(process.pid)
            if on_ready is not None:
                on_ready(process.pid, metadata)
            selected_credential_payload = (
                credential_payload_factory(process.pid, metadata)
                if credential_payload_factory is not None
                else credential_payload
            )
            _validate_broker_credential_payload(selected_credential_payload)
            _release_broker(
                process,
                readiness=readiness,
                credential_payload=selected_credential_payload,
                stdin_payload=stdin_payload,
                interactive_stdin=interactive_stdin,
                target_environment=validated_target_environment,
                startup_deadline=startup_deadline,
            )
        except BaseException as exc:
            cleanup_errors = _cleanup_failed_owned_spawn(
                process,
                readiness=readiness,
                registered=registered,
                unregistered_systemd_unit=None if registered else unit,
                unregistered_systemd_scope=None if registered else scope,
            )
            raise OwnedProcessSpawnError(
                process_id=process.pid,
                mode=mode,
                cleanup_errors=cleanup_errors,
                cause=exc,
            ) from exc
        return process
    process, readiness = _spawn_broker(command, popen_kwargs)
    registered = False
    try:
        _register_owned_process(
            process.pid,
            _OwnedProcessState(mode="cooperative_process_group", enforceable=False),
        )
        registered = True
        _notify_containment_ready(process, on_ready)
        _release_broker(
            process,
            readiness=readiness,
            credential_payload=credential_payload,
            stdin_payload=stdin_payload,
            interactive_stdin=interactive_stdin,
            target_environment=validated_target_environment,
            startup_deadline=startup_deadline,
        )
    except BaseException as exc:
        cleanup_errors = _cleanup_failed_owned_spawn(
            process,
            readiness=readiness,
            registered=registered,
        )
        raise OwnedProcessSpawnError(
            process_id=process.pid,
            mode="cooperative_process_group",
            cleanup_errors=cleanup_errors,
            cause=exc,
        ) from exc
    return process


def _cleanup_failed_owned_spawn(
    process: subprocess.Popen[str],
    *,
    readiness: _BrokerReadiness,
    registered: bool,
    unregistered_windows_handle: int | None = None,
    unregistered_systemd_unit: str | None = None,
    unregistered_systemd_scope: Path | None = None,
) -> list[str]:
    """Attempt every cleanup step after a failed broker launch, preserving ownership."""
    errors: list[str] = []
    if registered:
        try:
            terminate_owned_process(process)
        except BaseException as exc:
            errors.append(f"owned spawn termination failed: {type(exc).__name__}")
        try:
            release_owned_process(process)
        except BaseException as exc:
            errors.append(f"owned spawn provider release failed: {type(exc).__name__}")
    elif unregistered_systemd_unit is not None and unregistered_systemd_scope is not None:
        try:
            _terminate_linux_systemd_scope(
                unregistered_systemd_unit,
                unregistered_systemd_scope,
            )
        except BaseException as exc:
            errors.append(f"unregistered systemd scope termination failed: {type(exc).__name__}")
        try:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=TERMINATION_TIMEOUT_SECONDS)
        except BaseException as exc:
            errors.append(f"unregistered systemd broker termination failed: {type(exc).__name__}")
        try:
            _release_linux_systemd_scope(unregistered_systemd_unit)
        except BaseException as exc:
            errors.append(f"unregistered systemd scope release failed: {type(exc).__name__}")
    else:
        try:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=TERMINATION_TIMEOUT_SECONDS)
        except BaseException as exc:
            errors.append(f"unregistered broker termination failed: {type(exc).__name__}")
        if unregistered_windows_handle is not None:
            try:
                _close_windows_handle(unregistered_windows_handle)
            except BaseException as exc:
                errors.append(f"unregistered Job Object release failed: {type(exc).__name__}")
    try:
        _remove_broker_readiness(readiness)
    except BaseException as exc:
        errors.append(f"broker readiness cleanup failed: {type(exc).__name__}")
    return errors


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
        "systemd_invocation_id": state.systemd_invocation_id,
        "systemd_description": state.systemd_description,
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
        releasing = process.pid in _OWNED_PROCESSES_RELEASING
    if releasing:
        raise RuntimeError(f"process containment release is already in progress: {process.pid}")
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
        state = _OWNED_PROCESSES.get(process.pid)
        if state is None:
            return
        if process.pid in _OWNED_PROCESSES_RELEASING:
            raise RuntimeError(f"process containment release is already in progress: {process.pid}")
        _OWNED_PROCESSES_RELEASING.add(process.pid)
    try:
        ensure_owned_process_tree_empty(process)
        if state.job_handle is not None:
            _close_windows_handle(state.job_handle)
        if state.systemd_unit is not None:
            _release_linux_systemd_scope(state.systemd_unit)
    except BaseException:
        with _OWNED_PROCESSES_LOCK:
            _OWNED_PROCESSES_RELEASING.discard(process.pid)
        raise
    with _OWNED_PROCESSES_LOCK:
        _OWNED_PROCESSES_RELEASING.discard(process.pid)
        if _OWNED_PROCESSES.get(process.pid) is not state:
            raise RuntimeError("owned process registration changed during provider release")
        _OWNED_PROCESSES.pop(process.pid)


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


def terminate_nested_process(
    process: subprocess.Popen[str],
    *,
    timeout_seconds: float = TERMINATION_TIMEOUT_SECONDS,
) -> None:
    """Terminate a child from an embedded runner without killing its relay parent."""
    terminate_process_tree(
        process,
        owns_group=not inherited_relay_containment(),
        timeout_seconds=timeout_seconds,
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


def _terminate_recorded_windows_process_tree(
    process_id: int,
    expected_start_identity: str,
) -> None:
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
    observed_identity = process_start_identity(process_id)
    if observed_identity is None:
        return
    if observed_identity != expected_start_identity:
        raise RuntimeError(
            f"refused cleanup for reused process id {process_id}: "
            f"expected {expected_start_identity}, observed {observed_identity}"
        )
    detail = (result.stderr or "").strip()
    suffix = f": {detail}" if detail else ""
    raise RuntimeError(f"recorded process survived cleanup: {process_id}{suffix}")


def terminate_recorded_process_tree(
    *,
    process_id: int,
    expected_start_identity: str,
    process_group_id: int | None,
    containment_mode: str | None = None,
    systemd_unit: str | None = None,
    cgroup_path: str | None = None,
    systemd_invocation_id: str | None = None,
    systemd_description: str | None = None,
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
        exact_persistent_identity = (
            systemd_invocation_id is not None and systemd_description is not None
        )
        if (systemd_invocation_id is None) is not (systemd_description is None):
            raise RuntimeError("recorded systemd execution has partial persistent identity")
        if exact_persistent_identity:
            existing_pids = recorded_linux_systemd_scope_process_ids(
                unit=systemd_unit,
                cgroup_path=cgroup_path,
                invocation_id=cast(str, systemd_invocation_id),
                description=cast(str, systemd_description),
            )
            if not existing_pids and not Path(cgroup_path).exists():
                return
        scope = Path(cgroup_path).resolve(strict=True)
        if not exact_persistent_identity:
            cgroup_root = Path("/sys/fs/cgroup").resolve()
            try:
                scope.relative_to(cgroup_root)
            except ValueError as exc:
                raise RuntimeError(f"recorded cgroup is outside cgroup v2: {scope}") from exc
        _terminate_linux_systemd_scope(systemd_unit, scope)
        residual = (
            recorded_linux_systemd_scope_process_ids(
                unit=systemd_unit,
                cgroup_path=cgroup_path,
                invocation_id=cast(str, systemd_invocation_id),
                description=cast(str, systemd_description),
            )
            if exact_persistent_identity
            else _linux_cgroup_process_ids(scope)
        )
        if residual:
            raise RuntimeError(f"recorded systemd scope survived cleanup: {residual}")
        _release_linux_systemd_scope(systemd_unit)
        return
    if os.name == "nt":
        if observed_identity is None:
            return
        _terminate_recorded_windows_process_tree(process_id, expected_start_identity)
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


def _validate_broker_target_environment(
    environment: Mapping[str, str] | None,
) -> dict[str, str] | None:
    """Validate Windows target-only values delivered after Job Object assignment."""
    if environment is None:
        return None
    if os.name != "nt":
        raise RuntimeError("broker target environment setup is restricted to Windows")
    payload = broker_child_environment_payload(environment)
    if len(payload.encode("utf-8")) > BROKER_PROTOCOL_MAX_BYTES:
        raise RuntimeError("broker target environment exceeded its byte limit")
    return dict(environment)


def _release_broker(
    process: subprocess.Popen[str],
    *,
    readiness: _BrokerReadiness,
    credential_payload: str | None = None,
    stdin_payload: bytes | None = None,
    interactive_stdin: bool = False,
    target_environment: Mapping[str, str] | None = None,
    startup_deadline: float,
) -> None:
    if process.stdin is None:
        raise RuntimeError("containment broker did not expose its setup channel")
    if stdin_payload is not None and len(stdin_payload) > BROKER_STDIN_MAX_BYTES:
        raise RuntimeError("containment broker stdin payload exceeded its byte limit")
    message = json.dumps(
        {
            "release": True,
            "credential": credential_payload,
            "readiness_token": readiness.token,
            "stdin_payload": (
                None if stdin_payload is None else base64.b64encode(stdin_payload).decode("ascii")
            ),
            "interactive_stdin": interactive_stdin,
            "target_environment": (
                None if target_environment is None else dict(target_environment)
            ),
        },
        separators=(",", ":"),
    )
    setup = (message + "\n").encode("utf-8")
    if len(setup) > BROKER_SETUP_MAX_BYTES:
        raise RuntimeError("containment broker setup message exceeded its byte limit")
    encoded = setup
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
    if not completed.wait(max(0.0, startup_deadline - time.monotonic())):
        if process.poll() is None:
            process.kill()
        with suppress(OSError):
            os.close(setup_channel.fileno())
        process.stdin = None
        process.wait(timeout=TERMINATION_TIMEOUT_SECONDS)
        raise RuntimeError("containment broker setup write timed out")
    if errors:
        setup_channel.close()
        process.stdin = None
        raise RuntimeError(f"containment broker setup write failed: {errors[0]}")
    if not interactive_stdin:
        setup_channel.close()
        process.stdin = None
    _await_broker_readiness(process, readiness, startup_deadline=startup_deadline)


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
    *,
    startup_deadline: float | None = None,
) -> None:
    """Wait until the released child has consumed credentials or fail boundedly."""
    descriptor = readiness.descriptor
    if descriptor is None:
        raise RuntimeError("containment broker readiness channel was already closed")
    expected = readiness.token.encode("ascii")
    deadline = (
        startup_deadline
        if startup_deadline is not None
        else time.monotonic() + BROKER_READY_TIMEOUT_SECONDS
    )
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


def _probe_linux_systemd_scope_capability(
    *,
    startup_deadline: float | None,
) -> dict[str, object]:
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
            timeout=_remaining_deadline_seconds(
                startup_deadline,
                maximum=TERMINATION_TIMEOUT_SECONDS,
            ),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {
            "mode": "linux_systemd_scope",
            "enforceable": False,
            "reason": f"systemd user-scope probe failed: {type(exc).__name__}",
            "transient": isinstance(exc, subprocess.TimeoutExpired),
        }
    _release_linux_systemd_scope(
        f"{unit_base}.scope",
        startup_deadline=startup_deadline,
    )
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
    *,
    startup_deadline: float,
    unit_base: str | None = None,
    description: str | None = None,
) -> tuple[subprocess.Popen[str], str, Path, str, str | None, _BrokerReadiness]:
    if "stdin" in popen_kwargs:
        raise RuntimeError("owned process launch reserves stdin for containment setup")
    systemd_run = shutil.which("systemd-run")
    if systemd_run is None:
        raise RuntimeError("systemd-run disappeared after containment capability probing")
    selected_unit_base = unit_base or f"clio-relay-{uuid4().hex}"
    unit = f"{selected_unit_base}.scope"
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
        f"--unit={selected_unit_base}",
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
    if description is not None:
        wrapped[5:5] = [f"--description={description}"]
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
        properties = _wait_for_systemd_scope_identity(
            unit,
            process=process,
            startup_deadline=startup_deadline,
        )
        control_group = properties["ControlGroup"]
        invocation_id = properties["InvocationID"]
        observed_description = properties.get("Description")
        scope = _validated_systemd_cgroup_path(control_group, unit=unit)
        if description is not None and observed_description != description:
            raise RuntimeError("systemd scope description did not match its launch identity")
    except BaseException as exc:
        cleanup_errors = _cleanup_failed_linux_systemd_spawn(
            process,
            unit=unit,
            readiness=readiness,
            startup_deadline=startup_deadline,
        )
        raise OwnedProcessSpawnError(
            process_id=process.pid,
            mode="linux_systemd_scope",
            cleanup_errors=cleanup_errors,
            cause=exc,
        ) from exc
    return process, unit, scope, invocation_id, observed_description, readiness


def _cleanup_failed_linux_systemd_spawn(
    process: subprocess.Popen[str],
    *,
    unit: str,
    readiness: _BrokerReadiness,
    startup_deadline: float,
) -> list[str]:
    """Attempt every cleanup action for a scope that failed before registration."""
    errors: list[str] = []
    try:
        if process.poll() is None:
            process.kill()
    except BaseException as exc:
        errors.append(f"unregistered systemd broker termination failed: {type(exc).__name__}")
    try:
        process.wait(
            timeout=_remaining_deadline_seconds(
                startup_deadline,
                maximum=TERMINATION_TIMEOUT_SECONDS,
            )
        )
    except BaseException as exc:
        errors.append(f"unregistered systemd broker wait failed: {type(exc).__name__}")
    try:
        _release_linux_systemd_scope(unit, startup_deadline=startup_deadline)
    except BaseException as exc:
        errors.append(f"unregistered systemd scope release failed: {type(exc).__name__}")
    try:
        _remove_broker_readiness(readiness)
    except BaseException as exc:
        errors.append(f"broker readiness cleanup failed: {type(exc).__name__}")
    return errors


def _validated_systemd_cgroup_path(
    control_group: str,
    *,
    unit: str,
    cgroup_root: Path = Path("/sys/fs/cgroup"),
) -> Path:
    """Bind systemctl ControlGroup output to the exact newly-created delegated unit."""
    if (
        not control_group.startswith("/")
        or "\x00" in control_group
        or any(part in {"", ".", ".."} for part in control_group.split("/")[1:])
    ):
        raise RuntimeError("systemd scope returned an invalid ControlGroup path")
    try:
        root = cgroup_root.resolve(strict=True)
        candidate = (root / control_group.lstrip("/")).resolve(strict=True)
    except OSError as exc:
        raise RuntimeError("systemd scope ControlGroup path could not be resolved") from exc
    if candidate == root or not candidate.is_relative_to(root) or candidate.name != unit:
        raise RuntimeError("systemd scope ControlGroup did not match its exact unit")
    if not candidate.is_dir() or not (candidate / "cgroup.procs").is_file():
        raise RuntimeError("systemd scope did not expose its exact cgroup")
    return candidate


def _wait_for_systemd_scope_identity(
    unit: str,
    *,
    process: subprocess.Popen[str],
    startup_deadline: float,
) -> dict[str, str]:
    deadline = startup_deadline
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
            [
                "show",
                unit,
                "--property=ControlGroup",
                "--property=InvocationID",
                "--property=Description",
                "--property=LoadState",
            ],
            timeout_seconds=_remaining_deadline_seconds(
                deadline,
                maximum=DISCOVERY_TIMEOUT_SECONDS,
            ),
        )
        try:
            properties = _parse_systemd_properties(
                result.stdout,
                expected={"ControlGroup", "InvocationID", "Description", "LoadState"},
            )
        except RuntimeError as exc:
            properties = {}
            last_error = str(exc)
        if (
            result.returncode == 0
            and properties.get("LoadState") == "loaded"
            and properties.get("ControlGroup")
            and re.fullmatch(r"[0-9a-f]{32}", properties.get("InvocationID", ""))
        ):
            return properties
        last_error = result.stderr.strip() or last_error
        time.sleep(min(POLL_SECONDS, max(0.0, deadline - time.monotonic())))
    raise RuntimeError(f"systemd scope setup timed out: {unit}: {last_error}")


def _parse_systemd_properties(payload: str, *, expected: set[str]) -> dict[str, str]:
    """Parse one bounded duplicate-free systemctl show response."""
    properties: dict[str, str] = {}
    for line in payload.splitlines():
        name, separator, value = line.partition("=")
        if not separator or name not in expected or name in properties:
            raise RuntimeError("systemd scope returned invalid or duplicate properties")
        properties[name] = value
    if set(properties) != expected:
        raise RuntimeError("systemd scope omitted required identity properties")
    return properties


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


def _release_linux_systemd_scope(
    unit: str,
    *,
    startup_deadline: float | None = None,
) -> None:
    if shutil.which("systemctl") is None:
        return
    _systemctl_user(
        ["stop", unit],
        timeout_seconds=_remaining_deadline_seconds(
            startup_deadline,
            maximum=DISCOVERY_TIMEOUT_SECONDS,
        ),
    )
    _systemctl_user(
        ["reset-failed", unit],
        timeout_seconds=_remaining_deadline_seconds(
            startup_deadline,
            maximum=DISCOVERY_TIMEOUT_SECONDS,
        ),
    )


def _remaining_deadline_seconds(
    deadline: float | None,
    *,
    maximum: float,
) -> float:
    """Return one finite subprocess timeout capped by a shared absolute deadline."""
    if deadline is None:
        return maximum
    return max(0.001, min(maximum, deadline - time.monotonic()))


def _systemctl_user(
    arguments: list[str],
    *,
    timeout_seconds: float,
) -> subprocess.CompletedProcess[str]:
    systemctl = shutil.which("systemctl") or "systemctl"
    command = [systemctl, "--user", *arguments]
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
    except OSError as exc:
        return subprocess.CompletedProcess(
            command,
            1,
            "",
            str(exc),
        )

    stdout = bytearray()
    stderr = bytearray()
    overflow = threading.Event()

    def read_bounded(stream: Any, destination: bytearray) -> None:
        try:
            while True:
                remaining = SYSTEMCTL_OUTPUT_MAX_BYTES + 1 - len(destination)
                if remaining <= 0:
                    overflow.set()
                    return
                chunk = stream.read(min(8192, remaining))
                if not chunk:
                    return
                destination.extend(chunk)
                if len(destination) > SYSTEMCTL_OUTPUT_MAX_BYTES:
                    overflow.set()
                    return
        except OSError:
            overflow.set()

    if process.stdout is None or process.stderr is None:  # pragma: no cover - Popen contract
        process.kill()
        process.wait()
        return subprocess.CompletedProcess(command, 1, "", "systemctl pipes were unavailable")
    readers = [
        threading.Thread(target=read_bounded, args=(process.stdout, stdout), daemon=True),
        threading.Thread(target=read_bounded, args=(process.stderr, stderr), daemon=True),
    ]
    for reader in readers:
        reader.start()
    deadline = time.monotonic() + timeout_seconds
    timed_out = False
    while process.poll() is None and not overflow.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            timed_out = True
            break
        time.sleep(min(POLL_SECONDS, remaining))
    if process.poll() is None:
        try:
            killpg = cast(Callable[[int, int], None], vars(os)["killpg"])
            sigkill = cast(int, vars(signal)["SIGKILL"])
            killpg(process.pid, sigkill)
        except ProcessLookupError:
            pass
        except OSError:
            process.kill()
    try:
        process.wait(timeout=DISCOVERY_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=DISCOVERY_TIMEOUT_SECONDS)
    for reader in readers:
        reader.join(timeout=DISCOVERY_TIMEOUT_SECONDS)
    stdout_text = bytes(stdout[:SYSTEMCTL_OUTPUT_MAX_BYTES]).decode("utf-8", errors="replace")
    stderr_text = bytes(stderr[:SYSTEMCTL_OUTPUT_MAX_BYTES]).decode("utf-8", errors="replace")
    if overflow.is_set():
        return subprocess.CompletedProcess(
            command,
            1,
            stdout_text,
            "systemctl output exceeded its byte limit",
        )
    if timed_out:
        return subprocess.CompletedProcess(command, 1, stdout_text, "systemctl timed out")
    return subprocess.CompletedProcess(command, process.returncode, stdout_text, stderr_text)


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


def recorded_linux_systemd_scope_process_ids(
    *,
    unit: str,
    cgroup_path: str,
    invocation_id: str,
    description: str,
) -> list[int]:
    """Return PIDs only after exact persistent systemd scope identity verification."""
    if (
        re.fullmatch(r"clio-relay-session-[A-Za-z0-9_-]+\.scope", unit) is None
        or re.fullmatch(r"[0-9a-f]{32}", invocation_id) is None
        or not description
        or len(description.encode("utf-8")) > 512
    ):
        raise RuntimeError("recorded persistent systemd scope identity is invalid")
    result = _systemctl_user(
        [
            "show",
            unit,
            "--property=ControlGroup",
            "--property=InvocationID",
            "--property=Description",
            "--property=LoadState",
        ],
        timeout_seconds=DISCOVERY_TIMEOUT_SECONDS,
    )
    try:
        properties = _parse_systemd_properties(
            result.stdout,
            expected={"ControlGroup", "InvocationID", "Description", "LoadState"},
        )
    except RuntimeError:
        properties = {}
    recorded_path = Path(cgroup_path)
    if result.returncode != 0 or properties.get("LoadState") == "not-found":
        if recorded_path.exists():
            raise RuntimeError("recorded systemd unit vanished while its cgroup remained")
        return []
    if not (
        properties.get("LoadState") == "loaded"
        and properties.get("InvocationID") == invocation_id
        and properties.get("Description") == description
    ):
        raise RuntimeError("recorded systemd unit identity drifted or was reused")
    observed = _validated_systemd_cgroup_path(properties.get("ControlGroup", ""), unit=unit)
    try:
        expected = recorded_path.resolve(strict=True)
    except OSError as exc:
        raise RuntimeError("recorded systemd cgroup path is unavailable") from exc
    if observed != expected:
        raise RuntimeError("recorded systemd ControlGroup drifted")
    return _linux_cgroup_process_ids(observed)


def terminate_recorded_linux_systemd_scope(
    *,
    unit: str,
    cgroup_path: str,
    invocation_id: str,
    description: str,
) -> list[int]:
    """Terminate an exact persisted scope and prove its cgroup became absent or empty."""
    targeted = recorded_linux_systemd_scope_process_ids(
        unit=unit,
        cgroup_path=cgroup_path,
        invocation_id=invocation_id,
        description=description,
    )
    scope = Path(cgroup_path)
    if not targeted and not scope.exists():
        return []
    try:
        resolved_scope = scope.resolve(strict=True)
    except OSError as exc:
        raise RuntimeError("recorded systemd cgroup path is unavailable") from exc
    _terminate_linux_systemd_scope(unit, resolved_scope)
    residual = recorded_linux_systemd_scope_process_ids(
        unit=unit,
        cgroup_path=cgroup_path,
        invocation_id=invocation_id,
        description=description,
    )
    if residual:
        raise RuntimeError(f"recorded systemd scope survived cleanup: {residual}")
    _release_linux_systemd_scope(unit)
    return targeted


def adopt_linux_systemd_scope_identity(
    *,
    unit: str,
    description: str,
) -> dict[str, str] | None:
    """Recover an on-disk-predeclared scope before its launcher callback persisted identity."""
    if (
        re.fullmatch(r"clio-relay-session-[A-Za-z0-9_-]+\.scope", unit) is None
        or not description
        or len(description.encode("utf-8")) > 512
    ):
        raise RuntimeError("predeclared persistent systemd scope identity is invalid")
    result = _systemctl_user(
        [
            "show",
            unit,
            "--property=ControlGroup",
            "--property=InvocationID",
            "--property=Description",
            "--property=LoadState",
        ],
        timeout_seconds=DISCOVERY_TIMEOUT_SECONDS,
    )
    try:
        properties = _parse_systemd_properties(
            result.stdout,
            expected={"ControlGroup", "InvocationID", "Description", "LoadState"},
        )
    except RuntimeError:
        properties = {}
    if result.returncode != 0 or properties.get("LoadState") == "not-found":
        return None
    if not (
        properties.get("LoadState") == "loaded"
        and properties.get("Description") == description
        and re.fullmatch(r"[0-9a-f]{32}", properties.get("InvocationID", ""))
    ):
        raise RuntimeError("predeclared systemd scope identity drifted or was reused")
    scope = _validated_systemd_cgroup_path(properties.get("ControlGroup", ""), unit=unit)
    return {
        "systemd_unit": unit,
        "systemd_description": description,
        "systemd_invocation_id": properties["InvocationID"],
        "cgroup_path": str(scope),
    }


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
    if os.name != "nt":
        raise RuntimeError("Windows job objects require Windows")
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
    if os.name != "nt":
        raise RuntimeError("Windows process identity inspection requires Windows")
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
    if os.name != "nt":
        raise RuntimeError("Windows job assignment requires Windows")
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
    if os.name != "nt":
        raise RuntimeError("Windows job termination requires Windows")
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    if not kernel32.TerminateJobObject(job_handle, 1):
        raise RuntimeError(f"TerminateJobObject failed: {ctypes.get_last_error()}")


def _windows_job_active_processes(job_handle: int) -> int:
    if os.name != "nt":
        raise RuntimeError("Windows job inspection requires Windows")
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
    if os.name != "nt":
        raise RuntimeError("Windows handle cleanup requires Windows")
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
    # The Popen handle is PID-reuse safe; taskkill diagnostics are localized.
    exited_before_fallback = process.poll() is not None
    taskkill_failed = taskkill_error is not None or result.returncode not in {0, 128}
    if taskkill_failed and not exited_before_fallback:
        process.kill()
    try:
        process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=timeout_seconds)
    benign_exit_race = (
        taskkill_error is None and result.returncode not in {0, 128} and exited_before_fallback
    )
    if taskkill_failed and not benign_exit_race:
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
