"""Relay's held ssh connection against a loopback asyncssh server, on every OS.

The server asks for a password and then a code (two keyboard-interactive rounds,
as Duo sites do), or takes a key. It runs on the test's loop; relay's connection
runs on its own.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import io
import shlex
import socket
import tarfile
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import asyncssh
import pytest
import pytest_asyncio
from asyncssh import auth as ssh_auth

from clio_relay.adapters.ssh import errors, forwarder, held, profile
from clio_relay.adapters.ssh.process import Completed
from clio_relay.adapters.ssh.profile import Profile

pytestmark = pytest.mark.asyncio

CODE = "123456"


@dataclass
class Site:
    port: int
    key_file: Path
    root: Path
    conns: list[asyncssh.SSHServerConnection] = field(default_factory=list[Any])
    codes: list[str] = field(default_factory=lambda: [CODE])
    key_passed: list[asyncssh.SSHServerConnection] = field(default_factory=list[Any])

    def drop(self) -> None:
        for conn in self.conns:
            conn.abort()


def _server(site: Site, client_key: asyncssh.SSHKey) -> type[asyncssh.SSHServer]:
    class Server(asyncssh.SSHServer):
        def connection_made(self, conn: asyncssh.SSHServerConnection) -> None:
            site.conns.append(conn)
            self.conn = conn
            self.user = ""
            self.round = 0

        def begin_auth(self, username: str) -> bool:
            self.user = username
            return True

        def public_key_auth_supported(self) -> bool:
            return self.conn not in site.key_passed

        def validate_public_key(self, username: str, key: asyncssh.SSHKey) -> bool:
            return username in ("keyed", "keycode") and key.public_data == client_key.public_data

        def kbdint_auth_supported(self) -> bool:
            return self.user != "keycode" or self.conn in site.key_passed

        def get_kbdint_challenge(
            self, username: str, lang: str, submethods: str
        ) -> tuple[str, str, str, Sequence[tuple[str, bool]]]:
            if username == "keycode":
                self.round = 2
                return "", "Duo two-factor", "", [("Verification code: ", False)]
            self.round = 1
            return "", "", "", [("Password: ", False)]

        def validate_kbdint_response(
            self, username: str, responses: Sequence[str]
        ) -> bool | tuple[str, str, str, Sequence[tuple[str, bool]]]:
            if self.round == 1:
                if list(responses) != ["pw"]:
                    return False
                self.round = 2
                return "", "Duo two-factor", "", [("Verification code: ", False)]
            return list(responses) == site.codes[-1:]

        def server_requested(self, listen_host: str, listen_port: int) -> bool:
            return False

        def connection_requested(
            self, dest_host: str, dest_port: int, orig_host: str, orig_port: int
        ) -> bool:
            return dest_host == "127.0.0.1"

    return Server


async def _handle(process: asyncssh.SSHServerProcess[bytes], root: Path) -> None:
    command = process.command or ""
    if command.startswith("sleep "):
        await asyncio.sleep(float(command.split()[1]))
    if command.startswith("exit "):
        process.exit(int(command.split()[1]))
        return
    if "tar -xpf" in command:
        # Stands in for the site's sh and tar under a joined process's upload.
        script = shlex.split(command)[2]
        target = root / shlex.split(script.split(";")[0].removeprefix("d="))[0]
        data = await process.stdin.read()
        if not target.is_dir():
            process.stderr.write(b"sftp: nowhere: No such file or directory\n")
            process.exit(1)
            return
        with tarfile.open(fileobj=io.BytesIO(data)) as archive:
            archive.extractall(target, filter="data")
        process.exit(0)
        return
    process.stdout.write(f"ran:{command}\n".encode())
    process.exit(0)


@pytest_asyncio.fixture
async def site(tmp_path: Path) -> AsyncIterator[Site]:
    host_key = asyncssh.generate_private_key("ssh-ed25519")  # pyright: ignore[reportUnknownMemberType]
    client_key = asyncssh.generate_private_key("ssh-ed25519")  # pyright: ignore[reportUnknownMemberType]
    key_file = tmp_path / "id_ed25519"
    client_key.write_private_key(str(key_file))  # pyright: ignore[reportUnknownMemberType]
    root = tmp_path / "site"
    root.mkdir()
    holder: list[Site] = []

    def sftp(chan: asyncssh.SSHServerChannel[bytes]) -> asyncssh.SFTPServer:
        return asyncssh.SFTPServer(chan, chroot=str(root).encode())

    async def handle(process: asyncssh.SSHServerProcess[bytes]) -> None:
        await _handle(process, root)

    server = await asyncssh.create_server(
        lambda: _server(holder[0], client_key)(),
        "127.0.0.1",
        0,
        server_host_keys=[host_key],
        process_factory=handle,
        sftp_factory=sftp,
        encoding=None,
    )
    port = server.sockets[0].getsockname()[1]
    holder.append(Site(port=port, key_file=key_file, root=root))
    try:
        yield holder[0]
    finally:
        server.close()


def _profile(site: Site, *, user: str, keys: bool) -> Profile:
    preferred = [] if user == "keycode" else ["publickey"] if keys else ["keyboard-interactive"]
    return Profile(
        destination="loop",
        host="127.0.0.1",
        port=site.port,
        user=user,
        identity_files=[str(site.key_file)] if keys else [],
        identities_only=True,
        identity_agent="",
        check_host_keys=False,
        preferred_auth=preferred,
    )


def _held(
    monkeypatch: pytest.MonkeyPatch,
    site: Site,
    *,
    signs_in: bool,
    channels: int = 8,
    share: Path | None = None,
    user: str | None = None,
) -> held.Held:
    connection = held.Held(
        base=["ssh"],
        destination="loop",
        signs_in=signs_in,
        channels=channels,
        timeout_s=10,
        share=share,
    )
    if user == "keycode":
        profile = _profile(site, user=user, keys=True)
    else:
        profile = _profile(site, user="person" if signs_in else "keyed", keys=not signs_in)

    async def resolve() -> Profile:
        return profile

    monkeypatch.setattr(connection, "_resolve", resolve)
    return connection


class Person:
    """Stands in for relay's ask page."""

    def __init__(self, **answers: list[str]) -> None:
        self.answers = answers
        self.asked: list[str] = []

    async def ask(self, prompt: str, echo: bool) -> str | None:
        self.asked.append(prompt.strip().splitlines()[-1])
        key = "code" if "code" in prompt.lower() else "password"
        values = self.answers[key]
        return values.pop(0) if len(values) > 1 else values[0]


def _asks_through(monkeypatch: pytest.MonkeyPatch, person: Person) -> None:
    def asker(env: object) -> held.Ask:
        return person.ask

    monkeypatch.setattr(held, "asker", asker)


async def _run(connection: held.Held, command: str) -> Completed:
    return await connection.run(
        command,
        stdin=b"",
        hold_stdin=False,
        timeout_s=30,
        max_output_bytes=None,
        marker=b"",
        on_stall=None,
    )


async def _until(check: Any, seconds: float = 10) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + seconds
    while not check():
        assert loop.time() < deadline
        await asyncio.sleep(0.05)


async def test_one_sign_in_carries_every_call_and_a_lost_one_asks_again(
    site: Site, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection = _held(monkeypatch, site, signs_in=True)
    person = Person(password=["pw"], code=[CODE])
    _asks_through(monkeypatch, person)
    try:
        refused = await _run(connection, "echo before")
        assert b"Permission denied (keyboard-interactive)." in refused.stderr

        await asyncio.gather(connection.sign_in({}), connection.sign_in({}))
        assert person.asked == ["Password:", "Verification code:"]
        done = await asyncio.gather(*(_run(connection, f"sleep 0.1 {i}") for i in range(20)))
        assert all(item.exit_code == 0 for item in done)
        assert len(person.asked) == 2
        assert len(site.conns) == 1

        site.drop()
        await _until(lambda: not connection.signed_in())
        after = await _run(connection, "echo after")
        assert b"Permission denied (keyboard-interactive)." in after.stderr
        await connection.sign_in({})
        assert (await _run(connection, "echo again")).stdout == b"ran:echo again\n"
        assert len(person.asked) == 4
    finally:
        await connection.close()


async def test_a_wrong_code_is_asked_three_times_then_refused(
    site: Site, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection = _held(monkeypatch, site, signs_in=True)
    person = Person(password=["pw"], code=["000000"])
    _asks_through(monkeypatch, person)
    with pytest.raises(held.SshFailure) as failed:
        await connection.sign_in({})
    assert "Permission denied" in failed.value.text
    assert person.asked.count("Verification code:") == held.SIGN_IN_ATTEMPTS
    assert not connection.signed_in()


async def test_a_key_then_code_site_takes_the_key_and_asks_only_for_the_code(
    site: Site, monkeypatch: pytest.MonkeyPatch
) -> None:
    # asyncssh's server never answers "partial success", so this one does for a
    # key, as sshd does with AuthenticationMethods publickey,keyboard-interactive.
    key_auth: Any = ssh_auth._ServerPublicKeyAuth  # pyright: ignore[reportPrivateUsage]
    succeed = key_auth.send_success

    async def key_then_code(auth: Any) -> None:
        conn = auth._conn
        if auth._username == "keycode" and conn not in site.key_passed:
            site.key_passed.append(conn)
            auth.send_failure(partial_success=True)
            return
        await succeed(auth)

    monkeypatch.setattr(key_auth, "send_success", key_then_code)
    unset = _held(monkeypatch, site, signs_in=False, user="keycode")
    try:
        bare = await _run(unset, "echo bare")
    finally:
        await unset.close()
    assert bare.stderr.endswith(b": Permission denied (keyboard-interactive).")
    assert errors.cause(bare.stderr.decode(), bare.exit_code) == "sign_in"

    connection = _held(monkeypatch, site, signs_in=True, user="keycode")
    person = Person(code=[CODE])
    _asks_through(monkeypatch, person)
    try:
        refused = await _run(connection, "echo before")
        assert b"Permission denied (keyboard-interactive)." in refused.stderr

        await connection.sign_in({})
        assert person.asked == ["Verification code:"]
        done = await asyncio.gather(*(_run(connection, f"echo {i}") for i in range(5)))
        assert [item.stdout for item in done] == [f"ran:echo {i}\n".encode() for i in range(5)]
        assert site.conns[-1] in site.key_passed
        assert len(person.asked) == 1
    finally:
        await connection.close()


async def test_a_key_site_reconnects_by_itself(site: Site, monkeypatch: pytest.MonkeyPatch) -> None:
    connection = _held(monkeypatch, site, signs_in=False)
    try:
        assert (await _run(connection, "echo one")).exit_code == 0
        inflight = asyncio.ensure_future(_run(connection, "sleep 30"))
        await _until(lambda: len(site.conns) == 1 and connection.signed_in())
        await asyncio.sleep(0.3)
        site.drop()
        lost = await inflight
        assert lost.exit_code == held.FAILED
        assert b"closed by remote host" in lost.stderr
        assert (await _run(connection, "echo two")).stdout == b"ran:echo two\n"
        assert len(site.conns) == 2
    finally:
        await connection.close()


async def test_upload_copies_files_over_sftp(
    site: Site, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    connection = _held(monkeypatch, site, signs_in=False)
    (site.root / "dest").mkdir()
    source = tmp_path / "input.txt"
    source.write_text("staged\n", encoding="utf-8")
    try:
        done = await connection.upload([source], "~/dest")
        assert done.exit_code == 0, done.stderr
        assert (site.root / "dest" / "input.txt").read_text(encoding="utf-8") == "staged\n"
        missing = await connection.upload([source], "~/nowhere")
        assert missing.exit_code != 0
        assert b"No such file or directory" in missing.stderr
        assert not (site.root / "nowhere").exists()
    finally:
        await connection.close()


async def _echo_server() -> tuple[asyncio.Server, int]:
    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write(await reader.read(100))
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(echo, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def _through(port: int) -> bytes:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"hello")
    await writer.drain()
    data = await reader.read(100)
    writer.close()
    return data


async def test_a_code_site_serves_over_the_signed_in_connection(
    site: Site, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    connection = _held(monkeypatch, site, signs_in=True)
    _asks_through(monkeypatch, Person(password=["pw"], code=[CODE]))
    echo, remote = await _echo_server()
    local = _free_port()
    log = tmp_path / "forward.log"
    try:
        gone = forwarder.InRelay.start(
            connection, [(local, remote)], node=None, known_hosts=None, log=log
        )
        await asyncio.to_thread(gone.wait, 10)
        assert forwarder.NOT_SIGNED_IN in log.read_text(encoding="utf-8")

        await connection.sign_in({})
        forward = forwarder.InRelay.start(
            connection, [(local, remote)], node=None, known_hosts=None, log=log
        )
        assert forward.pid == 0
        await _until(lambda: _bound(local))
        assert await _through(local) == b"hello"
        assert len(site.conns) == 1  # no second sign-in
        forward.terminate()
        assert await asyncio.to_thread(forward.wait, 10) == -15
        assert not _bound(local)
    finally:
        await connection.close()
        echo.close()


async def test_relays_other_processes_share_one_sign_in(
    site: Site, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    share = tmp_path / "state" / "ssh" / "loop.json"
    owner = _held(monkeypatch, site, signs_in=True, share=share)
    other = _held(monkeypatch, site, signs_in=True, share=share)
    person = Person(password=["pw"], code=[CODE])
    _asks_through(monkeypatch, person)
    (site.root / "dest").mkdir()
    source = tmp_path / "input.txt"
    source.write_text("staged\n", encoding="utf-8")
    echo, remote = await _echo_server()
    local = _free_port()
    try:
        await owner.sign_in({})
        assert share.is_file()

        assert (await _run(other, "echo joined")).stdout == b"ran:echo joined\n"
        assert (await _run(other, "exit 3")).exit_code == 3
        done = await asyncio.gather(*(_run(other, f"sleep 0.1 {i}") for i in range(12)))
        assert all(item.exit_code == 0 for item in done)
        copied = await other.upload([source], "~/dest")
        assert copied.exit_code == 0, copied.stderr
        assert (site.root / "dest" / "input.txt").read_text(encoding="utf-8") == "staged\n"
        missing = await other.upload([source], "~/nowhere")
        assert missing.exit_code == 1
        assert b"No such file or directory" in missing.stderr

        forward = forwarder.InRelay.start(
            other, [(local, remote)], node=None, known_hosts=None, log=tmp_path / "f.log"
        )
        await _until(lambda: _bound(local))
        assert await _through(local) == b"hello"
        forward.terminate()
        await asyncio.to_thread(forward.wait, 10)
        assert len(person.asked) == 2
        assert len(site.conns) == 1

        site.drop()
        await _until(lambda: not other.signed_in() and not share.exists())
        after = await _run(other, "echo after")
        assert b"Permission denied (keyboard-interactive)." in after.stderr

        await other.sign_in({})
        assert len(person.asked) == 4
        assert (await _run(owner, "echo back")).stdout == b"ran:echo back\n"
        assert len(site.conns) == 2
    finally:
        await other.close()
        await owner.close()
        echo.close()


def _bound(port: int) -> bool:
    # Probe by connecting: a probe that binds would race the forwarder's own bind.
    with socket.socket() as sock:
        sock.settimeout(1)
        return sock.connect_ex(("127.0.0.1", port)) == 0


async def test_a_distro_wide_gssapi_default_stays_on_the_held_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("KRB5CCNAME", raising=False)
    debian = profile.parse("hostname ares\ngssapiauthentication yes\n")
    assert profile.from_values("ares", debian, "ssh").openssh_only is None

    kerberos = profile.parse(
        "hostname ares\ngssapiauthentication yes\n"
        "preferredauthentications gssapi-with-mic,publickey\n"
    )
    assert "Kerberos" in (profile.from_values("ares", kerberos, "ssh").openssh_only or "")
    monkeypatch.setenv("KRB5CCNAME", "FILE:/tmp/krb5cc_1000")
    assert profile.from_values("ares", debian, "ssh").openssh_only is not None


@dataclass
class Proxy:
    port: int
    asked: list[str] = field(default_factory=list[str])
    refuse: bool = False


async def _proxy(site: Site) -> tuple[asyncio.Server, Proxy]:
    """An HTTP ``CONNECT`` proxy: the only way to reach ``site.invalid``."""
    proxy = Proxy(port=0)

    async def pump(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        with contextlib.suppress(OSError):
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()
        writer.close()

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        head = await reader.readuntil(b"\r\n\r\n")
        target = head.split(b"\r\n", 1)[0].split()[1].decode()
        proxy.asked.append(target)
        if proxy.refuse or target != f"site.invalid:{site.port}":
            writer.write(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            writer.close()
            return
        up_reader, up_writer = await asyncio.open_connection("127.0.0.1", site.port)
        writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
        await asyncio.gather(pump(reader, up_writer), pump(up_reader, writer))

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    proxy.port = server.sockets[0].getsockname()[1]
    return server, proxy


def _no_proxy_set(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (*held.PROXY_VARIABLES, "NO_PROXY", "no_proxy"):
        monkeypatch.delenv(name, raising=False)


def _at(
    monkeypatch: pytest.MonkeyPatch, connection: held.Held, site: Site, host: str, port: int
) -> None:
    resolved = dataclasses.replace(_profile(site, user="keyed", keys=True), host=host, port=port)

    async def resolve() -> Profile:
        return resolved

    monkeypatch.setattr(connection, "_resolve", resolve)


async def test_relay_reaches_the_site_through_the_proxy_its_environment_names(
    site: Site, monkeypatch: pytest.MonkeyPatch
) -> None:
    server, proxy = await _proxy(site)
    _no_proxy_set(monkeypatch)
    connection = _held(monkeypatch, site, signs_in=False)
    _at(monkeypatch, connection, site, "site.invalid", site.port)
    try:
        monkeypatch.setenv("HTTPS_PROXY", f"http://127.0.0.1:{proxy.port}")
        done = await _run(connection, "echo one")
        assert done.stdout == b"ran:echo one\n", done.stderr
        assert proxy.asked == [f"site.invalid:{site.port}"]
        assert held.proxy_for("site.invalid") == f"http://127.0.0.1:{proxy.port}"
        monkeypatch.setenv("NO_PROXY", ".invalid")
        assert held.proxy_for("site.invalid") == ""
    finally:
        await connection.close()
        server.close()


async def test_a_failed_dial_says_how_relay_dialed(
    site: Site, monkeypatch: pytest.MonkeyPatch
) -> None:
    server, proxy = await _proxy(site)
    _no_proxy_set(monkeypatch)
    connection = _held(monkeypatch, site, signs_in=False)
    try:
        _at(monkeypatch, connection, site, "127.0.0.1", _free_port())
        closed = await _run(connection, "echo one")
        text = closed.stderr.decode()
        assert errors.cause(text, closed.exit_code) == "network", text
        assert "Connection refused\nrelay dialed 127.0.0.1:" in text
        assert "directly (no proxy in its environment) ([" in text

        monkeypatch.setenv("HTTPS_PROXY", f"http://127.0.0.1:{proxy.port}")
        proxy.refuse = True
        _at(monkeypatch, connection, site, "site.invalid", site.port)
        refused = (await _run(connection, "echo one")).stderr.decode()
        assert refused.startswith("ssh: Could not resolve hostname site.invalid")
        assert (
            f"relay's proxy http://127.0.0.1:{proxy.port} did not open site.invalid:{site.port}"
            " (the proxy answered HTTP/1.1 403 Forbidden), so relay dialed directly"
        ) in refused
    finally:
        await connection.close()
        server.close()
