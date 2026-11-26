"""Start a process that outlives relay's own, whatever a host does to relay's.

A host may restart relay's MCP server at any time, and how it stops the old one
varies: a signal to its process group, a kill of its process tree, or on Windows
the close of a job object that kills every process in it (the ``mcp`` SDK's
stdio client does that, and its job lets no process break away). A desktop
forward has to live through all of them.

So relay starts a short launcher, which starts the process and exits. The
process is then an orphan in a session or group of its own, with no parent a
tree kill could follow. On Windows the launcher is started by the system's WMI
service, outside every job relay is in; a breakaway from relay's job is the
fallback, and failing that the process ends with relay and the next relay
restores it. The launcher takes the argv and environment from a file only the
person can read, and removes it before starting anything.
"""

from __future__ import annotations

import contextlib
import functools
import json
import logging
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger(__name__)

LAUNCH_TIMEOUT_S = 30.0

CREATE_NO_WINDOW = 0x08000000
CREATE_NEW_PROCESS_GROUP = 0x00000200
CREATE_BREAKAWAY_FROM_JOB = 0x01000000

LAUNCHER = r"""
import json, os, subprocess, sys
path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    spec = json.load(handle)
os.remove(path)
if os.name == "nt":
    way = {"creationflags": 0x08000000 | 0x00000200}
else:
    way = {"start_new_session": True}
try:
    with open(spec["log"], "ab") as log:
        child = subprocess.Popen(
            spec["argv"], stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
            env=spec["env"], cwd=spec["cwd"], **way,
        )
    out = {"pid": child.pid}
except OSError as exc:
    out = {"error": f"{type(exc).__name__}: {exc}"}
with open(spec["result"] + ".tmp", "w", encoding="utf-8") as handle:
    json.dump(out, handle)
os.replace(spec["result"] + ".tmp", spec["result"])
"""

WMI_CREATE = (
    "$ErrorActionPreference='Stop';"
    "$s=New-CimInstance -ClassName Win32_ProcessStartup -ClientOnly"
    " -Property @{ShowWindow=[uint16]0};"
    "$r=Invoke-CimMethod -ClassName Win32_Process -MethodName Create"
    " -Arguments @{CommandLine=$env:CLIO_RELAY_LAUNCH;"
    "CurrentDirectory=$env:CLIO_RELAY_LAUNCH_DIR;ProcessStartupInformation=$s};"
    "Write-Output $r.ReturnValue"
)


class Process(Protocol):
    """What relay needs of a forward's process: a ``Popen`` or a ``Detached``."""

    @property
    def pid(self) -> int: ...

    def poll(self) -> int | None: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...

    def wait(self, timeout: float | None = None) -> int: ...


class Detached:
    """A process relay started but is not the parent of, watched by its pid.

    On Windows relay holds a handle to it from the start, so a reused pid is never
    taken for it.
    """

    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.returncode: int | None = None
        self._handle: int | None = _open(pid) if os.name == "nt" else None

    def poll(self) -> int | None:
        """Return the exit code once it has ended, else None."""
        if self.returncode is None and not _running(self.pid, self._handle):
            self.returncode = _exit_code(self._handle)
        return self.returncode

    def terminate(self) -> None:
        """Ask it to stop; on Windows that is the same as ``kill``."""
        _signal(self.pid, self._handle, hard=False)

    def kill(self) -> None:
        """Stop it at once."""
        _signal(self.pid, self._handle, hard=True)

    def wait(self, timeout: float | None = None) -> int:
        """Wait for it to end, raising ``subprocess.TimeoutExpired`` as ``Popen`` does."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while (code := self.poll()) is None:
            if deadline is not None and time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired(str(self.pid), timeout or 0)
            time.sleep(0.05)
        return code

    def __del__(self) -> None:
        if self._handle:
            with contextlib.suppress(Exception):
                _kernel32().CloseHandle(self._handle)
            self._handle = None


def spawn(argv: list[str], env: dict[str, str], cwd: Path, log: Path) -> Detached:
    """Start ``argv`` so that it outlives relay's process; raises ``OSError`` when it cannot."""
    if not sys.executable:
        raise OSError("relay's Python is unknown, so it cannot start a lasting process")
    cwd.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex
    spec_path = cwd / f".launch-{token}.json"
    result = cwd / f".launch-{token}.out"
    spec = {"argv": argv, "env": env, "cwd": str(cwd), "log": str(log), "result": str(result)}
    descriptor = os.open(spec_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(spec, handle)
    launcher = [sys.executable, "-c", LAUNCHER, str(spec_path)]
    try:
        _launch(launcher, cwd)
        out = _result(result)
    finally:
        for path in (spec_path, result, Path(f"{result}.tmp")):
            with contextlib.suppress(OSError):
                path.unlink()
    pid = out.get("pid")
    if not isinstance(pid, int):
        raise OSError(str(out.get("error") or "the launcher said nothing"))
    return Detached(pid)


def _launch(launcher: list[str], cwd: Path) -> None:
    quiet: dict[str, Any] = {
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "cwd": str(cwd),
    }
    if os.name != "nt":
        subprocess.Popen(launcher, start_new_session=True, **quiet).wait(LAUNCH_TIMEOUT_S)
        return
    flags = CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP
    # A breakaway that works can still leave the process in an outer job: jobs nest
    # (a launcher's job inside a host's), and breaking away from the inner one is
    # not leaving the outer. The WMI service starts it outside all of them.
    try:
        _wmi(launcher, cwd)
        return
    except OSError as exc:
        logger.info("starting a lasting process through WMI failed (%s); trying a breakaway", exc)
    try:
        process = subprocess.Popen(
            launcher, creationflags=flags | CREATE_BREAKAWAY_FROM_JOB, **quiet
        )
    except PermissionError:
        # It ends with relay's job; the next relay restores it when it starts.
        process = subprocess.Popen(launcher, creationflags=flags, **quiet)
    process.wait(LAUNCH_TIMEOUT_S)


def _wmi(launcher: list[str], cwd: Path) -> None:
    env = {
        **os.environ,
        "CLIO_RELAY_LAUNCH": subprocess.list2cmdline(launcher),
        "CLIO_RELAY_LAUNCH_DIR": str(cwd),
    }
    try:
        done = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", WMI_CREATE],
            capture_output=True,
            text=True,
            env=env,
            timeout=LAUNCH_TIMEOUT_S,
            creationflags=CREATE_NO_WINDOW,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise OSError("the WMI service did not start the forward's launcher in time") from exc
    if done.returncode != 0 or done.stdout.strip() != "0":
        detail = (done.stderr.strip() or done.stdout.strip())[-300:]
        raise OSError(f"the WMI service did not start the forward's launcher: {detail}")


def _result(path: Path) -> dict[str, object]:
    deadline = time.monotonic() + LAUNCH_TIMEOUT_S
    while time.monotonic() < deadline:
        with contextlib.suppress(OSError, ValueError):
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                return dict(loaded)  # pyright: ignore[reportUnknownArgumentType]
        time.sleep(0.05)
    raise OSError("the forward's launcher did not report a process in time")


SYNCHRONIZE = 0x00100000
PROCESS_TERMINATE = 0x0001
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000


@functools.cache
def _kernel32() -> Any:
    if sys.platform != "win32":
        raise OSError("kernel32 exists only on Windows")
    import ctypes
    from ctypes import wintypes

    kernel32: Any = ctypes.WinDLL("kernel32", use_last_error=True)  # pyright: ignore[reportAttributeAccessIssue]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.WaitForSingleObject.restype = wintypes.DWORD
    kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel32.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    return kernel32


def _open(pid: int) -> int | None:
    access = SYNCHRONIZE | PROCESS_TERMINATE | PROCESS_QUERY_LIMITED_INFORMATION
    handle = _kernel32().OpenProcess(access, False, pid)
    return int(handle) if handle else None


def _running(pid: int, handle: int | None) -> bool:
    if os.name == "nt":
        return bool(handle) and _kernel32().WaitForSingleObject(handle, 0) != 0
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    with contextlib.suppress(OSError, IndexError):
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        return stat.rsplit(")", 1)[1].split()[0] != "Z"
    return True


def _exit_code(handle: int | None) -> int:
    if sys.platform != "win32" or not handle:
        return -1
    import ctypes
    from ctypes import wintypes

    code = wintypes.DWORD()
    _kernel32().GetExitCodeProcess(handle, ctypes.byref(code))
    return int(code.value)


def _signal(pid: int, handle: int | None, *, hard: bool) -> None:
    if os.name == "nt":
        if handle:
            _kernel32().TerminateProcess(handle, 1)
        return
    import signal

    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.kill(pid, signal.SIGKILL if hard else signal.SIGTERM)
