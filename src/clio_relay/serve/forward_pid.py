"""Telling a forward relay started from any other process on the desktop."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _windows_command_line(pid: int) -> str | None:
    """Return a Windows process's command line, or None when Windows will not say."""
    query = f"(Get-CimInstance Win32_Process -Filter 'ProcessId={int(pid)}').CommandLine"
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", query],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    line = result.stdout.strip()
    return line if result.returncode == 0 and line else None


def is_ours(pid: int, marker: str) -> bool:
    """Return whether ``pid`` still runs and is relay's forward, by ``marker`` in its command."""
    if sys.platform.startswith("linux"):
        try:
            cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ")
        except OSError:
            return False
        return marker.encode() in cmdline
    if os.name == "nt":
        command_line = _windows_command_line(pid)
        if command_line is not None:
            return marker in command_line
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            check=False,
        )
        return any(name in result.stdout.lower() for name in ("python", "ssh"))
    try:
        result = subprocess.run(
            ["ps", "-o", "command=", "-p", str(pid)], capture_output=True, text=True, check=False
        )
    except OSError:
        return False
    return marker in result.stdout
