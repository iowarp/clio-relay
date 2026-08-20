"""Local desktop-connector process discovery, identity, and signaling.

Extracted from ``service_runtime.py`` (#231 rework slice): the real,
distinct, local (non-embedded) process-identity implementation for the
desktop frpc connector -- rediscovering a connector from its durable
ownership intent (``_discover_local_connector``), verifying a live process
still carries that connector's unforgeable identity
(``_local_connector_identity_status``, ``_observed_connector_matches``),
enumerating same-owner or marker-matching candidate processes on POSIX and
Windows (``_local_process_ids``, ``_local_connector_group_members``,
``_windows_connector_descendants``), and terminating an owned connector
group through race-safe pidfds on POSIX or ``taskkill`` on Windows
(``_signal_owned_posix_connector_processes``, ``_terminate_local_connector``).
The pidfd primitives (``_linux_pidfd_open``/``_linux_pidfd_send_signal``) are
the local counterpart of the *embedded* remote pidfd helpers
``frp_remote_scripts.py`` ships as heredoc text for the remote host to run --
this module never sends those helpers anywhere, it calls them in-process.

Depends on ``service_runtime_primitives`` (coercion helpers, the shared
cleanup timeout), ``service_runtime_types`` (``_ObservedLocalProcess``,
``LocalConnectorIdentity``), and ``service_runtime_scheduler_contracts``
(``_required_intent_str``) -- never on the supervisor class, which imports
these names back qualified through this module instead.
"""

from __future__ import annotations

import ctypes
import errno
import json
import os
import platform
import secrets
import signal
import subprocess
import sys
import time
from collections.abc import Sequence
from contextlib import suppress
from pathlib import Path
from typing import Literal, cast

from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_scheduler_contracts as _scheduler_contracts
from clio_relay import service_runtime_types as _types
from clio_relay.errors import RelayError

# Linux assigns these values to both x86-64 and asm-generic syscall ABIs.
# libc symbols remain the preferred fallback when CPython omits its wrappers.
_LINUX_PIDFD_SEND_SIGNAL_SYSCALL_NUMBER = 424
_LINUX_PIDFD_OPEN_SYSCALL_NUMBER = 434
_LINUX_PIDFD_RAW_SYSCALL_MACHINES = frozenset({"aarch64", "amd64", "arm64", "x86_64"})


def _write_local_connector_sidecar(path: Path, connector: dict[str, object]) -> None:
    """Atomically persist exact local process identity next to its connector config."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    payload = {
        "schema_version": "clio-relay.desktop-connector-sidecar.v1",
        **connector,
    }
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _discover_local_connector(
    intent: dict[str, object],
    *,
    session_id: str,
) -> tuple[dict[str, object] | None, bool]:
    """Rediscover one local connector or prove its exact intent has no live process."""
    owner_token = _scheduler_contracts._required_intent_str(intent, "owner_token")
    generation_id = _scheduler_contracts._required_intent_str(intent, "connector_generation_id")
    config_path = _scheduler_contracts._required_intent_str(intent, "config_path")
    metadata_path = Path(_scheduler_contracts._required_intent_str(intent, "metadata_path"))
    sidecar: dict[str, object] | None = None
    try:
        loaded = json.loads(metadata_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        loaded = None
    except (OSError, json.JSONDecodeError) as exc:
        raise RelayError(f"desktop connector sidecar is unreadable: {exc}") from exc
    if isinstance(loaded, dict):
        candidate = cast(dict[str, object], loaded)
        if (
            candidate.get("schema_version") != "clio-relay.desktop-connector-sidecar.v1"
            or candidate.get("owner") != "clio-relay"
            or candidate.get("session_id") != session_id
            or candidate.get("owner_token") != owner_token
            or candidate.get("connector_generation_id") != generation_id
            or candidate.get("config_path") != config_path
        ):
            raise RelayError("desktop connector sidecar identity does not match its intent")
        sidecar = {key: value for key, value in candidate.items() if key != "schema_version"}
        status, _detail = _local_connector_identity_status(sidecar)
        if status == "owned":
            return sidecar, False

    observed_matches: list[_types._ObservedLocalProcess] = []
    observation_errors: list[str] = []
    for pid in _local_process_ids(
        command_markers=(owner_token, generation_id, config_path),
    ):
        try:
            observed = _observe_local_process(pid)
        except RelayError as exc:
            observation_errors.append(f"pid {pid}: {exc}")
            continue
        if observed is None:
            continue
        owned, _detail = _observed_connector_matches(
            observed,
            owner_token=owner_token,
            expected_config=config_path,
            expected_process_group_id=observed.pid,
        )
        if not owned:
            continue
        generation_marker = f"CLIO_RELAY_CONNECTOR_GENERATION_ID={generation_id}".encode()
        if observed.environment is not None:
            if generation_marker not in observed.environment.split(bytes([0])):
                continue
        elif generation_id.casefold() not in observed.command_line.casefold():
            continue
        observed_matches.append(observed)
    if observation_errors:
        raise RelayError(
            "desktop connector process observation was incomplete: "
            + "; ".join(observation_errors[:20])
        )
    if len(observed_matches) > 1:
        raise RelayError("multiple local processes matched one connector ownership intent")
    if observed_matches:
        observed = observed_matches[0]
        connector: dict[str, object] = {
            "owner": "clio-relay",
            "session_id": session_id,
            "pid": observed.pid,
            "process_group_id": observed.process_group_id,
            "process_start_marker": observed.process_start_marker,
            "owner_token": owner_token,
            "connector_generation_id": generation_id,
            "config_path": config_path,
            "stdout_path": intent.get("stdout_path"),
            "stderr_path": intent.get("stderr_path"),
            "metadata_path": str(metadata_path),
        }
        _write_local_connector_sidecar(metadata_path, connector)
        return connector, False
    if sidecar is not None and _local_connector_group_members(sidecar):
        raise RelayError("desktop connector descendants remain but the leader is unresolved")
    return None, True


def _local_process_ids(*, command_markers: tuple[str, ...] = ()) -> list[int]:
    """Enumerate same-owner or marker-matching connector candidate processes."""
    if os.name != "nt":
        try:
            candidates: list[int] = []
            for path in Path("/proc").iterdir():
                if not path.name.isdigit():
                    continue
                try:
                    owner = path.stat().st_uid
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    raise RelayError(
                        f"cannot inspect local process owner {path.name}: {exc}"
                    ) from exc
                if owner == os.geteuid():
                    candidates.append(int(path.name))
            return sorted(candidates)
        except OSError as exc:
            raise RelayError(f"cannot enumerate local processes: {exc}") from exc
    result = _run_bounded_local_cleanup(
        [
            "powershell",
            "-NoProfile",
            "-Command",
            "@(Get-CimInstance Win32_Process | Select-Object ProcessId,CommandLine) "
            "| ConvertTo-Json -Compress",
        ],
    )
    if result.returncode != 0:
        raise RelayError("cannot enumerate local Windows processes")
    try:
        loaded: object = json.loads(result.stdout) if result.stdout.strip() else []
    except json.JSONDecodeError as exc:
        raise RelayError("local Windows process enumeration returned invalid JSON") from exc
    raw_ids = cast(list[object], loaded) if isinstance(loaded, list) else [loaded]
    folded_markers = tuple(marker.casefold() for marker in command_markers)
    process_ids: list[int] = []
    for item in raw_ids:
        if not isinstance(item, dict):
            raise RelayError("local Windows process enumeration returned an invalid record")
        record = cast(dict[str, object], item)
        raw_process_id = record.get("ProcessId")
        command_line = record.get("CommandLine")
        if (
            isinstance(raw_process_id, bool)
            or not isinstance(raw_process_id, int)
            or raw_process_id < 0
        ):
            raise RelayError("local Windows process enumeration returned an invalid process id")
        # Win32_Process includes the System Idle Process as PID 0. It cannot own
        # or be signaled as a connector, while every relay process identity is
        # strictly positive, so omit only this Windows sentinel from discovery.
        if raw_process_id == 0:
            continue
        process_id = raw_process_id
        if folded_markers:
            if not isinstance(command_line, str):
                continue
            folded_command = command_line.casefold()
            if not all(marker in folded_command for marker in folded_markers):
                continue
        process_ids.append(process_id)
    return sorted(process_ids)


def _remote_cleanup_proven(result: dict[str, object]) -> bool:
    """Return whether remote cleanup proved the exact owned group absent."""
    return (
        result.get("outcome") in {"stopped", "missing"}
        and result.get("ownership_verified") is True
        and result.get("verified_after_operation") is True
        and result.get("residual") is False
        and result.get("remaining_pids") == []
    )


def _capture_local_connector_identity(
    *,
    pid: int,
    owner_token: str,
    expected_config: str,
) -> _types.LocalConnectorIdentity:
    deadline = time.time() + 5
    last_detail = "process did not appear"
    while time.time() < deadline:
        observed = _observe_local_process(pid)
        if observed is None:
            time.sleep(0.05)
            continue
        owned, last_detail = _observed_connector_matches(
            observed,
            owner_token=owner_token,
            expected_config=expected_config,
            expected_process_group_id=pid,
        )
        if owned:
            return _types.LocalConnectorIdentity(
                pid=pid,
                process_group_id=observed.process_group_id,
                process_start_marker=observed.process_start_marker,
                owner_token=owner_token,
            )
        time.sleep(0.05)
    raise RelayError(f"desktop connector did not establish owned process identity: {last_detail}")


def _local_connector_identity_status(
    connector: dict[str, object],
) -> tuple[Literal["owned", "missing", "replaced", "unverified"], str | None]:
    pid = _primitives._optional_int(connector.get("pid"))
    if pid is None:
        return "missing", "connector record has no process id"
    owner_token = _primitives._optional_str(connector.get("owner_token"))
    config_path = _primitives._optional_str(connector.get("config_path"))
    process_group_id = _primitives._optional_int(connector.get("process_group_id"))
    start_marker = _primitives._optional_str(connector.get("process_start_marker"))
    if (
        owner_token is None
        or config_path is None
        or process_group_id is None
        or start_marker is None
    ):
        return "unverified", "connector record lacks token, start, or process-group identity"
    try:
        group_members = _local_connector_group_members(connector)
        observed = _observe_local_process(pid)
    except RelayError as exc:
        return "unverified", str(exc)
    if observed is None:
        if group_members:
            return "owned", "owned connector descendants remain after the group leader exited"
        return "missing", "recorded connector process is no longer running"
    if observed.process_start_marker != start_marker:
        if os.name == "nt":
            return "replaced", "recorded connector PID now belongs to a different process"
        if group_members:
            return "owned", "owned connector group remains after leader PID reuse"
        return "replaced", "recorded connector PID now belongs to a different process"
    owned, detail = _observed_connector_matches(
        observed,
        owner_token=owner_token,
        expected_config=config_path,
        expected_process_group_id=process_group_id,
    )
    return ("owned", None) if owned else ("unverified", detail)


def _local_connector_group_members(connector: dict[str, object]) -> list[int]:
    """Return all live processes carrying the connector's unforgeable identity."""
    pid = _primitives._optional_int(connector.get("pid"))
    process_group_id = _primitives._optional_int(connector.get("process_group_id"))
    owner_token = _primitives._optional_str(connector.get("owner_token"))
    generation_id = _primitives._optional_str(connector.get("connector_generation_id"))
    config_path = _primitives._optional_str(connector.get("config_path"))
    if (
        pid is None
        or process_group_id is None
        or owner_token is None
        or generation_id is None
        or config_path is None
    ):
        return []
    if os.name == "nt":
        return _windows_connector_descendants(pid=pid, expected_config=config_path)
    token_marker = f"CLIO_RELAY_CONNECTOR_OWNER_TOKEN={owner_token}".encode()
    generation_marker = f"CLIO_RELAY_CONNECTOR_GENERATION_ID={generation_id}".encode()
    matches: list[int] = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        member_pid = int(proc.name)
        try:
            if proc.stat().st_uid != os.geteuid():
                continue
            fields = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
        except (FileNotFoundError, ProcessLookupError):
            continue
        except (OSError, IndexError, ValueError) as exc:
            raise RelayError(
                f"cannot inspect local process group member {member_pid}: {exc}"
            ) from exc
        if fields[0] == "Z":
            continue
        try:
            command_line = (
                (proc / "cmdline")
                .read_bytes()
                .replace(bytes([0]), b" ")
                .decode("utf-8", errors="replace")
            )
        except (FileNotFoundError, ProcessLookupError):
            continue
        except OSError as exc:
            raise RelayError(
                f"cannot inspect local connector group member {member_pid}: {exc}"
            ) from exc
        if "frpc" not in command_line.casefold() or not _command_contains_path(
            command_line,
            config_path,
        ):
            continue
        try:
            environment = (proc / "environ").read_bytes().split(bytes([0]))
        except (FileNotFoundError, ProcessLookupError):
            continue
        except OSError as exc:
            raise RelayError(
                f"cannot verify local connector group member {member_pid}: {exc}"
            ) from exc
        if token_marker in environment and generation_marker in environment:
            matches.append(member_pid)
    return sorted(matches)


def _windows_connector_descendants(*, pid: int, expected_config: str) -> list[int]:
    command = (
        "$items = @(Get-CimInstance Win32_Process | "
        "Select-Object ProcessId,ParentProcessId,CommandLine); "
        "$items | ConvertTo-Json -Compress"
    )
    result = _run_bounded_local_cleanup(
        ["powershell", "-NoProfile", "-Command", command],
    )
    if result.returncode != 0:
        raise RelayError(
            "cannot enumerate local Windows connector descendants: "
            + (result.stderr.strip() or f"exit {result.returncode}")
        )
    try:
        loaded = cast(object, json.loads(result.stdout))
    except json.JSONDecodeError as exc:
        raise RelayError("local Windows connector descendant query returned invalid JSON") from exc
    raw_items = cast(list[object], loaded) if isinstance(loaded, list) else [loaded]
    processes = [cast(dict[str, object], item) for item in raw_items if isinstance(item, dict)]
    descendants = {pid}
    changed = True
    while changed:
        changed = False
        for item in processes:
            child = _primitives._optional_int(item.get("ProcessId"))
            parent = _primitives._optional_int(item.get("ParentProcessId"))
            if child is not None and parent in descendants and child not in descendants:
                descendants.add(child)
                changed = True
    matches: list[int] = []
    for item in processes:
        child = _primitives._optional_int(item.get("ProcessId"))
        command_line = _primitives._optional_str(item.get("CommandLine"))
        if (
            child is not None
            and child in descendants
            and command_line is not None
            and "frpc" in command_line.casefold()
            and _command_contains_path(command_line, expected_config)
        ):
            matches.append(child)
    return sorted(matches)


def _observed_connector_matches(
    observed: _types._ObservedLocalProcess,
    *,
    owner_token: str,
    expected_config: str,
    expected_process_group_id: int,
) -> tuple[bool, str]:
    if observed.process_group_id != expected_process_group_id:
        return False, "connector process-group identity does not match"
    command = observed.command_line.casefold()
    if "frpc" not in command:
        return False, "connector command does not contain frpc"
    if owner_token.casefold() not in command:
        return False, "connector command does not contain its owner token"
    if not _command_contains_path(observed.command_line, expected_config):
        return False, "connector command does not contain its owned config path"
    if observed.environment is not None:
        expected_environment = f"CLIO_RELAY_CONNECTOR_OWNER_TOKEN={owner_token}".encode()
        if expected_environment not in observed.environment.split(bytes([0])):
            return False, "connector environment does not contain its owner token"
    return True, "owned connector identity verified"


def _command_contains_path(command_line: str, expected_path: str) -> bool:
    normalized_command = command_line.replace("\\", "/").casefold()
    candidates = {expected_path}
    with suppress(OSError):
        candidates.add(str(Path(expected_path).resolve()))
    return any(
        candidate.replace("\\", "/").casefold() in normalized_command for candidate in candidates
    )


def _observe_local_process(pid: int) -> _types._ObservedLocalProcess | None:
    if pid <= 0:
        return None
    if os.name == "nt":
        return _observe_windows_process(pid)
    proc = Path("/proc") / str(pid)
    try:
        stat_fields = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
        if stat_fields[0] == "Z":
            return None
        command_line = (
            (proc / "cmdline")
            .read_bytes()
            .replace(bytes([0]), b" ")
            .decode("utf-8", errors="replace")
        )
        environment = (proc / "environ").read_bytes()
        process_group_id = os.getpgid(pid)
    except (FileNotFoundError, ProcessLookupError):
        return None
    except (OSError, IndexError) as exc:
        raise RelayError(f"cannot observe local connector candidate {pid}: {exc}") from exc
    return _types._ObservedLocalProcess(
        pid=pid,
        process_group_id=process_group_id,
        process_start_marker=stat_fields[19],
        command_line=command_line,
        environment=environment,
    )


def _observe_windows_process(pid: int) -> _types._ObservedLocalProcess | None:
    command = (
        f"$cim = Get-CimInstance Win32_Process -Filter 'ProcessId = {pid}'; "
        "if ($null -eq $cim) { exit 3 }; "
        f"$process = Get-Process -Id {pid} -ErrorAction Stop; "
        "$value = [pscustomobject]@{"
        "command_line=$cim.CommandLine; "
        "start_marker=$process.StartTime.ToUniversalTime().Ticks.ToString()}; "
        "$value | ConvertTo-Json -Compress"
    )
    result = _run_bounded_local_cleanup(
        ["powershell", "-NoProfile", "-Command", command],
    )
    if result.returncode == 3:
        return None
    if result.returncode != 0:
        raise RelayError(
            f"cannot query local Windows connector candidate {pid}: "
            + (result.stderr.strip() or f"exit {result.returncode}")
        )
    try:
        loaded = cast(object, json.loads(result.stdout))
    except json.JSONDecodeError as exc:
        raise RelayError(f"local Windows connector candidate {pid} returned invalid JSON") from exc
    if not isinstance(loaded, dict):
        raise RelayError(f"local Windows connector candidate {pid} returned an invalid record")
    payload = cast(dict[str, object], loaded)
    command_line = payload.get("command_line")
    start_marker = payload.get("start_marker")
    if not isinstance(command_line, str) or not isinstance(start_marker, str):
        raise RelayError(f"local Windows connector candidate {pid} lacks identity fields")
    return _types._ObservedLocalProcess(
        pid=pid,
        process_group_id=pid,
        process_start_marker=start_marker,
        command_line=command_line,
        environment=None,
    )


def _signal_owned_posix_connector_processes(
    connector: dict[str, object],
    sig: int,
) -> list[int]:
    """Signal only revalidated connector identities through race-safe pidfds."""
    signaled: list[int] = []
    for member_pid in _local_connector_group_members(connector):
        try:
            raw_process_fd = _open_posix_process_fd(member_pid)
        except ProcessLookupError:
            continue
        except OSError as exc:
            raise RelayError(f"cannot open connector pidfd for {member_pid}: {exc}") from exc
        process_fd = raw_process_fd
        try:
            if member_pid not in _local_connector_group_members(connector):
                continue
            try:
                _send_posix_process_fd_signal(process_fd, sig)
            except ProcessLookupError:
                continue
            except OSError as exc:
                raise RelayError(f"cannot signal owned connector pid {member_pid}: {exc}") from exc
            signaled.append(member_pid)
        finally:
            os.close(process_fd)
    return signaled


def _open_posix_process_fd(pid: int) -> int:
    """Open a Linux process descriptor even when CPython omitted pidfd wrappers."""
    native_open = getattr(os, "pidfd_open", None)
    descriptor = native_open(pid, 0) if callable(native_open) else _linux_pidfd_open(pid)
    if not isinstance(descriptor, int):
        raise OSError("pidfd_open returned a non-integer descriptor")
    return descriptor


def _send_posix_process_fd_signal(process_fd: int, sig: int) -> None:
    """Signal a Linux process descriptor through CPython or libc."""
    native_send = getattr(signal, "pidfd_send_signal", None)
    if callable(native_send):
        native_send(process_fd, sig, None, 0)
        return
    _linux_pidfd_send_signal(process_fd, sig)


def _linux_pidfd_open(pid: int) -> int:
    """Invoke pidfd_open through libc, falling back to the Linux syscall ABI."""
    if not sys.platform.startswith("linux"):
        raise RelayError("race-safe pidfd connector cleanup is unavailable on this platform")
    library = ctypes.CDLL(None, use_errno=True)
    libc_open = getattr(library, "pidfd_open", None)
    ctypes.set_errno(0)
    if libc_open is not None:
        libc_open.argtypes = [ctypes.c_int, ctypes.c_uint]
        libc_open.restype = ctypes.c_int
        descriptor = int(libc_open(pid, 0))
    else:
        if platform.machine().lower() not in _LINUX_PIDFD_RAW_SYSCALL_MACHINES:
            raise RelayError("raw pidfd_open syscall ABI is unavailable on this architecture")
        syscall = library.syscall
        syscall.restype = ctypes.c_long
        descriptor = int(
            syscall(
                ctypes.c_long(_LINUX_PIDFD_OPEN_SYSCALL_NUMBER),
                ctypes.c_int(pid),
                ctypes.c_uint(0),
            )
        )
    if descriptor < 0:
        error_number = ctypes.get_errno() or errno.ENOSYS
        raise OSError(error_number, os.strerror(error_number))
    return descriptor


def _linux_pidfd_send_signal(process_fd: int, sig: int) -> None:
    """Invoke pidfd_send_signal through libc or the stable Linux syscall ABI."""
    if not sys.platform.startswith("linux"):
        raise RelayError("race-safe pidfd connector cleanup is unavailable on this platform")
    library = ctypes.CDLL(None, use_errno=True)
    libc_send = getattr(library, "pidfd_send_signal", None)
    ctypes.set_errno(0)
    if libc_send is not None:
        libc_send.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_void_p,
            ctypes.c_uint,
        ]
        libc_send.restype = ctypes.c_int
        result = int(libc_send(process_fd, sig, None, 0))
    else:
        if platform.machine().lower() not in _LINUX_PIDFD_RAW_SYSCALL_MACHINES:
            raise RelayError(
                "raw pidfd_send_signal syscall ABI is unavailable on this architecture"
            )
        syscall = library.syscall
        syscall.restype = ctypes.c_long
        result = int(
            syscall(
                ctypes.c_long(_LINUX_PIDFD_SEND_SIGNAL_SYSCALL_NUMBER),
                ctypes.c_int(process_fd),
                ctypes.c_int(sig),
                ctypes.c_void_p(),
                ctypes.c_uint(0),
            )
        )
    if result < 0:
        error_number = ctypes.get_errno() or errno.ENOSYS
        raise OSError(error_number, os.strerror(error_number))


def _terminate_local_connector(connector: dict[str, object]) -> int | None:
    pid = _primitives._optional_int(connector.get("pid"))
    process_group_id = _primitives._optional_int(connector.get("process_group_id"))
    if pid is None or process_group_id is None:
        return None
    if _local_connector_identity_status(connector)[0] != "owned":
        return None
    if os.name == "nt":
        result = _run_bounded_local_cleanup(["taskkill", "/PID", str(pid), "/T", "/F"])
        if result.returncode not in {0, 128}:
            return None
    else:
        _signal_owned_posix_connector_processes(connector, signal.SIGTERM)
        deadline = time.time() + 5
        while time.time() < deadline:
            if not _local_connector_group_members(connector):
                return pid
            time.sleep(0.2)
        _signal_owned_posix_connector_processes(connector, signal.SIGKILL)
    deadline = time.time() + 5
    while time.time() < deadline:
        if not _local_connector_group_members(connector):
            return pid
        time.sleep(0.2)
    return None


def _run_bounded_local_cleanup(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    """Run one local ownership/cleanup command with a strict wall-clock bound."""
    try:
        return subprocess.run(
            list(command),
            capture_output=True,
            text=True,
            check=False,
            timeout=_primitives._LOCAL_CLEANUP_COMMAND_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise RelayError(
            "local cleanup command timed out after "
            f"{_primitives._LOCAL_CLEANUP_COMMAND_TIMEOUT_SECONDS:g} seconds: {command[0]}"
        ) from exc
