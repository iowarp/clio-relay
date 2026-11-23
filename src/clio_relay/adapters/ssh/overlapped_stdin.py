"""An overlapped standard input for Win32-OpenSSH, so ssh cannot hang when a session ends.

Win32-OpenSSH (seen in 9.5p2) reads a piped stdin with a blocking ``ReadFile`` on a helper
thread. When the channel ends it cancels that read with ``CancelIoEx`` and then waits, with
no timeout, for the thread. A read that had not reached the kernel when the cancel ran is
not cancelled: it blocks on relay's still-open stdin, and ssh never writes the output it
holds. Relay keeps stdin open for a command's lifetime (``hold_stdin``), which meets that
race about once in a hundred calls.

An stdin that ssh opens as overlapped (``NONSOCK_FD`` in ssh's own ``POSIX_FD_STATE``) is
cancelled and closed without that wait. So relay hands such an ssh the read end of an
overlapped pipe and writes into the other end through asyncio, as into a plain pipe. The
choice follows the program, not the OS: a binary that carries the ``POSIX_FD_STATE`` name
is Win32-OpenSSH and reads it. Any other program keeps asyncio's plain pipe: MSYS or Git's
ssh, POSIX's ssh, or a launcher shim such as CLIO's fence, which applies the same fix to
the ssh it starts. A state already in the environment is left alone, so the fix is never
applied twice.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import functools
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass

FD_STATE = "c28fc6f98a2c44abbbd89d6a3037d0d9_POSIX_FD_STATE"
# struct std_fd_state {int num_inherited; char in_type, out_type, err_type, padding;}:
# stdin is NONSOCK_FD (2, overlapped); stdout and stderr are NONSOCK_SYNC_FD (3), as asyncio
# makes them.
OVERLAPPED_STDIN = base64.b64encode((0).to_bytes(4, "little") + bytes((2, 3, 3, 0))).decode("ascii")


@functools.lru_cache(maxsize=16)
def _carries_marker(path: str, size: int, mtime_ns: int) -> bool:
    del size, mtime_ns  # part of the cache key: a replaced binary is read again
    try:
        with open(path, "rb") as handle:
            return FD_STATE.encode("ascii") in handle.read()
    except OSError:
        return False


def speaks_fd_state(program: str) -> bool:
    """Whether ``program`` is a Win32-OpenSSH build that reads ``FD_STATE``."""
    try:
        stat = os.stat(program)
    except OSError:
        return False
    path = os.path.normcase(os.path.abspath(program))
    return _carries_marker(path, stat.st_size, stat.st_mtime_ns)


@dataclass
class OverlappedStdin:
    """The child's end (an fd to pass as ``stdin``), relay's writer, and the child's env."""

    child: int | None
    writer: asyncio.StreamWriter
    env: dict[str, str]

    def spawned(self) -> None:
        """Close relay's copy of the child's end, once the child holds its own."""
        if self.child is not None:
            os.close(self.child)
            self.child = None

    def close(self) -> None:
        """Close both ends; the child sees its stdin end once the writes are out."""
        self.spawned()
        with contextlib.suppress(OSError, RuntimeError):
            self.writer.close()


async def open_for(program: str, env: Mapping[str, str] | None) -> OverlappedStdin | None:
    """Return an overlapped stdin for ``program`` when it is Win32-OpenSSH, else None."""
    if sys.platform != "win32":
        return None
    base = dict(os.environ if env is None else env)
    if any(key.upper() == FD_STATE.upper() for key in base) or not speaks_fd_state(program):
        return None
    import _winapi
    import msvcrt
    from asyncio import windows_utils

    try:
        # The child reads the first end (overlapped); relay writes the second. Duplex,
        # because asyncio's write transport reads its end to learn when the child closes.
        child, mine = windows_utils.pipe(overlapped=(True, True), duplex=True)
    except OSError:
        return None
    loop = asyncio.get_running_loop()
    protocol = asyncio.streams.FlowControlMixin(loop)
    try:
        transport, _ = await loop.connect_write_pipe(
            lambda: protocol, windows_utils.PipeHandle(mine)
        )
    except (OSError, NotImplementedError):
        _winapi.CloseHandle(child)
        with contextlib.suppress(OSError):
            _winapi.CloseHandle(mine)
        return None
    return OverlappedStdin(
        child=msvcrt.open_osfhandle(child, os.O_RDONLY),
        writer=asyncio.StreamWriter(transport, protocol, None, loop),
        env={**base, FD_STATE: OVERLAPPED_STDIN},
    )
