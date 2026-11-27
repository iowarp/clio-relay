"""Share one signed-in connection with relay's other processes on this computer.

A site that asks for a code signs in once. Relay can run as more than one process
on the same state (the page server CLIO starts and the agent's own server, or a
CLI call), and each would otherwise ask the person again. The process that signs
in publishes its connection here, as OpenSSH's ControlMaster shares a master:

- it listens on a loopback port with a fresh host key, and accepts only the fresh
  client key it writes beside the port in ``<state>/ssh/<site>.json``, a file
  only the person's account reads;
- each command a joined process runs becomes a channel on the signed-in
  connection, through the same channel gate as the owner's own calls, and its
  exit status comes back;
- each forward a joined process opens is tunneled over the signed-in connection.

When the signed-in connection drops, or its process exits, every joined process
loses its connection too, and each refuses ``needs_sign_in`` until the person
signs in again (in any of them). SFTP does not pass through the share, so a
joined process copies files as a tar stream (``tar_upload``).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shlex
import stat
import tarfile
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any

import asyncssh

CONNECT_S = 5.0
CHUNK = 1 << 16
FAILED = 255

Acquire = Callable[[], Awaitable[Any]]
"""Take a channel slot on the signed-in connection: a slot with ``.conn``, or a failure."""
Release = Callable[[Any], Awaitable[None]]


def path_for(state_dir: Path, site: str) -> Path:
    """Return where a site's shared connection is published."""
    return state_dir / "ssh" / f"{site}.json"


class Shared:
    """A published connection: the listener, and the processes joined to it."""

    def __init__(self, path: Path, listener: asyncssh.SSHAcceptor, owner: str) -> None:
        self.path = path
        self.owner = owner
        self.listener = listener
        self.joined: set[asyncssh.SSHServerConnection] = set()
        self._watch: asyncio.Task[None] | None = None

    def close(self) -> None:
        """Stop sharing: close the port and every joined connection, and unpublish."""
        if self._watch is not None:
            self._watch.cancel()
        self.listener.close()
        for conn in list(self.joined):
            conn.close()
        self.joined.clear()
        with contextlib.suppress(OSError, ValueError):
            if json.loads(self.path.read_text(encoding="utf-8")).get("owner") == self.owner:
                self.path.unlink()


class _Server(asyncssh.SSHServer):
    def __init__(self, shared: Shared, upstream: asyncssh.SSHClientConnection) -> None:
        self.shared = shared
        self.upstream = upstream
        self.conn: asyncssh.SSHServerConnection | None = None

    def connection_made(self, conn: asyncssh.SSHServerConnection) -> None:
        self.conn = conn
        self.shared.joined.add(conn)

    def connection_lost(self, exc: Exception | None) -> None:
        if self.conn is not None:
            self.shared.joined.discard(self.conn)

    def connection_requested(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, dest_host: str, dest_port: int, orig_host: str, orig_port: int
    ) -> asyncssh.SSHClientConnection:
        return self.upstream


def _write(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(data, handle)
    os.replace(temp, path)


async def publish(
    path: Path,
    upstream: asyncssh.SSHClientConnection,
    lost: asyncio.Event,
    acquire: Acquire,
    release: Release,
    owner: str,
) -> Shared:
    """Share ``upstream`` until ``lost`` is set or the result is closed.

    ``owner`` names the publishing connection, so it never joins itself.
    """
    host_key = asyncssh.generate_private_key("ssh-ed25519")  # pyright: ignore[reportUnknownMemberType]
    client_key = asyncssh.generate_private_key("ssh-ed25519")  # pyright: ignore[reportUnknownMemberType]
    authorized = asyncssh.import_authorized_keys(client_key.export_public_key("openssh").decode())

    async def session(process: asyncssh.SSHServerProcess[bytes]) -> None:
        await _relay(process, acquire, release)

    holder: list[Shared] = []
    listener = await asyncssh.listen(
        "127.0.0.1",
        0,
        server_host_keys=[host_key],
        authorized_client_keys=authorized,
        server_factory=lambda: _Server(holder[0], upstream),
        process_factory=session,
        encoding=None,
        allow_scp=False,
        agent_forwarding=False,
        x11_forwarding=False,
        keepalive_interval=15,
        keepalive_count_max=4,
    )
    shared = Shared(path, listener, owner)
    holder.append(shared)
    _write(
        path,
        {
            "owner": owner,
            "pid": os.getpid(),
            "port": listener.get_port(),
            "host_key": host_key.export_public_key("openssh").decode().strip(),
            "client_key": client_key.export_private_key("openssh").decode(),
        },
    )

    async def watch() -> None:
        await lost.wait()
        shared.close()

    shared._watch = asyncio.ensure_future(watch())  # pyright: ignore[reportPrivateUsage]
    return shared


async def _relay(
    process: asyncssh.SSHServerProcess[bytes], acquire: Acquire, release: Release
) -> None:
    command = process.command
    if command is None:
        process.exit(FAILED)
        return
    slot = await acquire()
    if not hasattr(slot, "conn"):
        process.stderr.write(getattr(slot, "stderr", b"") or b"")
        process.exit(FAILED)
        return
    try:
        try:
            upstream = await slot.conn.create_process(
                command,
                encoding=None,
                stdin=process.stdin,
                stdout=process.stdout,
                stderr=process.stderr,
            )
        except (asyncssh.Error, OSError):
            process.channel.get_connection().close()
            return
        ended = asyncio.ensure_future(upstream.wait_closed())
        gone = asyncio.ensure_future(process.wait_closed())
        await asyncio.wait({ended, gone}, return_when=asyncio.FIRST_COMPLETED)
        gone.cancel()
        if not ended.done():
            upstream.close()
            return
        status = upstream.exit_status
        if status is None or status < 0:
            # The signed-in connection went: the joined process loses its own.
            process.channel.get_connection().close()
            return
        process.exit(status)
    finally:
        await release(slot)


async def join(
    path: Path, client: Callable[[], asyncssh.SSHClient], me: str
) -> asyncssh.SSHClientConnection | None:
    """Join the connection another relay process published, or ``None`` if there is none."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if data["owner"] == me:
            return None
        host_key = asyncssh.import_public_key(data["host_key"])
        key = asyncssh.import_private_key(data["client_key"])
        trusted: list[asyncssh.SSHKey] = [host_key]
        none: list[asyncssh.SSHKey] = []
        port = int(data["port"])
    except (OSError, ValueError, KeyError, TypeError, asyncssh.KeyImportError):
        return None
    try:
        conn, _ = await asyncssh.create_connection(
            client,
            "127.0.0.1",
            port=port,
            username="relay",
            config=None,
            known_hosts=(trusted, none, none),
            client_keys=[key],
            agent_path=None,
            preferred_auth="publickey",
            gss_auth=False,
            gss_kex=False,
            connect_timeout=CONNECT_S,
            keepalive_interval=15,
            keepalive_count_max=4,
        )
    except (asyncssh.Error, OSError):
        return None
    return conn


def _header(path: Path) -> bytes:
    info = path.stat()
    entry = tarfile.TarInfo(path.name)
    entry.size = info.st_size
    entry.mtime = int(info.st_mtime)
    entry.mode = stat.S_IMODE(info.st_mode)
    return entry.tobuf(format=tarfile.PAX_FORMAT)


async def tar_upload(
    conn: asyncssh.SSHClientConnection, sources: Sequence[Path], destination: str
) -> tuple[int, bytes]:
    """Copy files into a directory on the site as a tar stream; return the exit and stderr.

    It keeps modes and times as an SFTP copy does, and a destination that is not a
    directory fails with SFTP's words for it.
    """
    target = destination.rstrip("/")
    if target.startswith("~/"):
        target = target[2:]
    target = target or "."
    script = (
        f'd={shlex.quote(target)}; if [ ! -d "$d" ]; then '
        f"printf 'sftp: %s: No such file or directory\\n' {shlex.quote(destination)} >&2; "
        'exit 1; fi; exec tar -xpf - -C "$d"'
    )
    process = await conn.create_process(f"sh -c {shlex.quote(script)}", encoding=None)
    with contextlib.suppress(asyncssh.Error, OSError, BrokenPipeError):
        for source in sources:
            process.stdin.write(_header(source))
            size = 0
            with source.open("rb") as handle:
                while chunk := handle.read(CHUNK):
                    size += len(chunk)
                    process.stdin.write(chunk)
                    await process.stdin.drain()
            process.stdin.write(b"\0" * (-size % tarfile.BLOCKSIZE))
        process.stdin.write(b"\0" * (2 * tarfile.BLOCKSIZE))
        process.stdin.write_eof()
    err = await process.stderr.read()
    await process.wait_closed()
    code = process.exit_status
    return (FAILED if code is None else code), err
