"""Identify the exact ``uv`` executable in this process's OS ancestor chain (#231).

Extracted from :mod:`clio_relay.validation_report`
(``docs/design/relay-architecture-2026-08.md``, #231). Install-source
detection wants to know whether *this* process was actually launched by a
specific, verified ``uv`` binary -- not merely that one exists on ``PATH``.
:func:`uv_process_ancestor` walks a bounded OS-native parent-process chain
(:func:`linux_process_ancestors` reads procfs;
:func:`windows_process_ancestors` / :func:`windows_process_image` use a
Toolhelp snapshot plus a least-privilege ``QueryFullProcessImageNameW``
handle) looking for a process image whose regular-file identity
(:mod:`clio_relay.regular_file_identity`) matches the candidate executable.
:func:`uv_executable_identity` independently versions and hashes one exact
regular ``uv``/``uv.exe`` path, re-checking its identity before and after
running ``--version`` so a swap mid-probe is caught rather than trusted.
"""

from __future__ import annotations

import ctypes
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

from clio_relay.regular_file_identity import hash_open_regular_file, regular_file_identity
from clio_relay.validation_limits import MAX_LAUNCHER_PROCESS_ANCESTORS


def strictly_contains(parent: Path, child: Path) -> bool:
    """Return whether ``child`` is below, but is not equal to, ``parent``."""
    try:
        return child != parent and child.is_relative_to(parent)
    except (OSError, ValueError):
        return False


def within_or_equal(path: Path, root: Path) -> bool:
    """Return whether a resolved path is equal to or below a resolved root."""
    try:
        return path == root or path.is_relative_to(root)
    except (OSError, ValueError):
        return False


def uv_process_ancestor(executable: Path) -> tuple[bool, dict[str, Any] | None]:
    """Find the exact uv file identity in a bounded OS process ancestor chain."""
    expected_identity = regular_file_identity(executable)
    if expected_identity is None:
        return False, None
    if os.name == "nt":
        ancestors = windows_process_ancestors(os.getpid())
    elif sys.platform.startswith("linux"):
        ancestors = linux_process_ancestors(os.getpid())
    else:
        return False, None
    for depth, (pid, image) in enumerate(ancestors, start=1):
        if regular_file_identity(image) != expected_identity:
            continue
        return True, {"pid": pid, "depth": depth, "executable": str(image)}
    return False, None


def linux_process_ancestors(pid: int) -> list[tuple[int, Path]]:
    """Read a bounded Linux parent chain from procfs."""
    ancestors: list[tuple[int, Path]] = []
    seen = {pid}
    current = pid
    for _ in range(MAX_LAUNCHER_PROCESS_ANCESTORS):
        try:
            stat_text = Path(f"/proc/{current}/stat").read_text(encoding="utf-8")
            closing = stat_text.rfind(")")
            fields = stat_text[closing + 2 :].split() if closing >= 0 else []
            parent = int(fields[1]) if len(fields) > 1 else 0
        except (OSError, UnicodeDecodeError, ValueError):
            break
        if parent <= 0 or parent in seen:
            break
        seen.add(parent)
        try:
            image = Path(f"/proc/{parent}/exe").resolve(strict=True)
        except OSError:
            break
        ancestors.append((parent, image))
        current = parent
    return ancestors


def windows_process_ancestors(pid: int) -> list[tuple[int, Path]]:
    """Read a bounded Windows parent chain with Toolhelp and process-image handles."""
    if os.name != "nt":
        return []
    from ctypes import wintypes

    class ProcessEntry32W(ctypes.Structure):
        _fields_ = [
            ("dwSize", wintypes.DWORD),
            ("cntUsage", wintypes.DWORD),
            ("th32ProcessID", wintypes.DWORD),
            ("th32DefaultHeapID", ctypes.c_size_t),
            ("th32ModuleID", wintypes.DWORD),
            ("cntThreads", wintypes.DWORD),
            ("th32ParentProcessID", wintypes.DWORD),
            ("pcPriClassBase", wintypes.LONG),
            ("dwFlags", wintypes.DWORD),
            ("szExeFile", wintypes.WCHAR * 260),
        ]

    loader = cast(Any, ctypes.WinDLL)("kernel32", use_last_error=True)
    create_snapshot = loader.CreateToolhelp32Snapshot
    process_first = loader.Process32FirstW
    process_next = loader.Process32NextW
    open_process = loader.OpenProcess
    query_image = loader.QueryFullProcessImageNameW
    close_handle = loader.CloseHandle
    create_snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
    create_snapshot.restype = wintypes.HANDLE
    process_first.argtypes = [wintypes.HANDLE, ctypes.POINTER(ProcessEntry32W)]
    process_first.restype = wintypes.BOOL
    process_next.argtypes = [wintypes.HANDLE, ctypes.POINTER(ProcessEntry32W)]
    process_next.restype = wintypes.BOOL
    open_process.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    open_process.restype = wintypes.HANDLE
    query_image.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.LPWSTR,
        ctypes.POINTER(wintypes.DWORD),
    ]
    query_image.restype = wintypes.BOOL
    close_handle.argtypes = [wintypes.HANDLE]
    close_handle.restype = wintypes.BOOL
    snapshot = create_snapshot(0x00000002, 0)
    invalid_handle = ctypes.c_void_p(-1).value
    if snapshot in {None, invalid_handle}:
        return []
    parents: dict[int, int] = {}
    try:
        entry = ProcessEntry32W()
        entry.dwSize = ctypes.sizeof(ProcessEntry32W)
        found = bool(process_first(snapshot, ctypes.byref(entry)))
        while found:
            parents[int(entry.th32ProcessID)] = int(entry.th32ParentProcessID)
            entry.dwSize = ctypes.sizeof(ProcessEntry32W)
            found = bool(process_next(snapshot, ctypes.byref(entry)))
    finally:
        close_handle(snapshot)

    ancestors: list[tuple[int, Path]] = []
    seen = {pid}
    current = pid
    for _ in range(MAX_LAUNCHER_PROCESS_ANCESTORS):
        parent = parents.get(current, 0)
        if parent <= 0 or parent in seen:
            break
        seen.add(parent)
        image = windows_process_image(parent, open_process, query_image, close_handle)
        if image is None:
            break
        ancestors.append((parent, image))
        current = parent
    return ancestors


def windows_process_image(
    pid: int,
    open_process: Any,
    query_image: Any,
    close_handle: Any,
) -> Path | None:
    """Resolve one Windows process image using a least-privilege query handle."""
    from ctypes import wintypes

    handle = open_process(0x1000, False, pid)
    if not handle:
        return None
    try:
        buffer = ctypes.create_unicode_buffer(32768)
        length = wintypes.DWORD(len(buffer))
        if not query_image(handle, 0, buffer, ctypes.byref(length)):
            return None
        return Path(buffer.value[: length.value]).resolve(strict=True)
    except OSError:
        return None
    finally:
        close_handle(handle)


def uv_executable_identity(executable: str | None) -> tuple[bool, str | None, str | None]:
    """Version and hash an exact regular uv executable without accepting indirection."""
    if executable is None:
        return False, None, None
    path = Path(executable)
    if not path.is_absolute() or path.name.casefold() not in {"uv", "uv.exe"}:
        return False, None, None
    before = regular_file_identity(path)
    if before is None:
        return False, None, None
    before_digest = hash_open_regular_file(path, before)
    if before_digest is None:
        return False, None, None
    try:
        completed = subprocess.run(
            [str(path), "--version"],
            capture_output=True,
            check=False,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False, None, None
    match = re.fullmatch(
        r"uv ([0-9]+\.[0-9]+\.[0-9]+(?:[A-Za-z0-9.+-]*))(?:\s+.*)?",
        completed.stdout.strip(),
    )
    if completed.returncode != 0 or match is None:
        return False, None, None
    after = regular_file_identity(path)
    if after != before:
        return False, None, None
    after_digest = hash_open_regular_file(path, after)
    if after_digest is None or after_digest != before_digest:
        return False, None, None
    return True, match.group(1), after_digest
