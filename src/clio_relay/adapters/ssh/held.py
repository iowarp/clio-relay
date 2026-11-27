"""Relay's own ssh connections: one signed-in connection carries many calls.

Relay holds each site's connection itself (asyncssh), the same way on Windows,
macOS and Linux. The person's ``ssh`` still says how to reach the site
(``profile``: ``ssh -G`` reads their config), and their agent and keys sign in.

- A site with keys signs in silently. Calls share a few connections, opened
  when needed and opened again when one drops.
- A site that asks for a code (``interactive_sign_in``) signs in once through
  relay's ask, the way ``ssh`` asks through ``SSH_ASKPASS``. Later calls reuse
  that connection. When it drops, calls refuse with ``needs_sign_in`` and
  nothing asks until the person signs in again.

Relay dials the site the way any network client does: through the proxy its
environment names (``HTTPS_PROXY``, ``ALL_PROXY``; ``NO_PROXY`` exempts hosts),
unless the person's config gives a ``ProxyCommand`` or jump host. A sandbox that
lets programs out only through its proxy then reaches the site too.

Failures read as OpenSSH's own messages, so ``errors`` tells the cases apart for
both transports, with a line saying how relay dialed. Every connection lives
on one private loop, so the MCP server's loop, the sign-in page's loop and a
CLI's ``asyncio.run`` all share it.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import errno
import logging
import os
import secrets
import socket
import subprocess
import time
import urllib.request
from collections.abc import Awaitable, Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast
from urllib.parse import unquote, urlsplit

import asyncssh

from clio_relay import programs
from clio_relay.adapters.ssh import mux
from clio_relay.adapters.ssh import profile as profiles
from clio_relay.adapters.ssh.process import Completed, Sink, supervise
from clio_relay.adapters.ssh.profile import Profile
from clio_relay.asks import askpass
from clio_relay.asks.runner import Runner
from clio_relay.jobs.handles import STALL_S, StallReport

logger = logging.getLogger(__name__)

FAILED = 255
KEEPALIVE_S = 15
KEEPALIVE_COUNT = 4
POOL = 4
"""At most this many connections to a site that signs in with keys."""
SIGN_IN_ATTEMPTS = 3
"""Passes through a site's prompts before relay gives up, as ssh allows."""
MAX_HOPS = 8
BUSY_RETRIES = 20
PROXY_VARIABLES = (
    "HTTPS_PROXY",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
    "HTTP_PROXY",
    "http_proxy",
)
"""Where relay looks for its proxy, in this order; the first one set wins."""
_MAX_PROXY_HEAD = 64 * 1024
_bypass = cast(
    Callable[[str, dict[str, str]], bool],
    getattr(urllib.request, "proxy_bypass_environment"),  # noqa: B009
)

Ask = Callable[[str, bool], Awaitable[str | None]]
"""Ask the person one prompt (``echo``: the answer may show); ``None`` if they declined."""

runner = Runner(name="relay-ssh")


def asker(env: Mapping[str, str] | None) -> Ask | None:
    """Return how to ask the person, as ssh does: ``SSH_ASKPASS`` with ``SSH_ASKPASS_REQUIRE``.

    Relay's own askpass is asked in process; another program runs with the prompt.
    """
    if not env or env.get("SSH_ASKPASS_REQUIRE") != "force" or not env.get("SSH_ASKPASS"):
        return None
    program = env["SSH_ASKPASS"]
    addr, token = env.get(askpass.ENV_ADDR, ""), env.get(askpass.ENV_TOKEN, "")
    own = Path(program).stem.lower() == askpass.PROGRAM

    async def ask(prompt: str, echo: bool) -> str | None:
        if own and addr and token:
            kind = askpass.classify(prompt)
            status, answer = await asyncio.to_thread(
                askpass.ask, prompt, addr=addr, token=token, kind=kind
            )
            return answer if status == 0 else None
        argv = programs.resolved([program, prompt], env)
        if argv is None:
            return None
        done = await asyncio.to_thread(
            subprocess.run,
            argv,
            capture_output=True,
            text=True,
            env=dict(env),
            stdin=subprocess.DEVNULL,
            check=False,
        )
        return done.stdout.rstrip("\r\n") if done.returncode == 0 else None

    return ask


class SshFailure(Exception):
    """ssh failed itself. ``text`` reads as OpenSSH's message for it."""

    def __init__(self, text: str) -> None:
        super().__init__(text)
        self.text = text


class _Client(asyncssh.SSHClient):
    """One connection's side of signing in: the person's prompts and the site's notes."""

    def __init__(self, profile: Profile, ask: Ask | None) -> None:
        self.profile = profile
        self.ask = ask
        self.tries = 0
        self.declined = False
        self.offered: set[str] = set()
        self.keys_ran_out = False
        self.notes: list[str] = []
        self.lost = asyncio.Event()

    def connection_lost(self, exc: Exception | None) -> None:
        self.lost.set()

    def public_key_auth_requested(self) -> None:
        # asyncssh asks here only once the profile's keys are all refused.
        self.keys_ran_out = True

    def auth_banner_received(self, msg: str, lang: str) -> None:
        if msg.strip():
            self.notes.append(msg.strip())

    def _try(self, method: str) -> bool:
        self.offered.add(method)
        if self.ask is None or self.declined:
            return False
        self.tries += 1
        return self.tries <= SIGN_IN_ATTEMPTS

    def kbdint_auth_requested(self) -> str | None:
        return "" if self._try("keyboard-interactive") else None

    async def kbdint_challenge_received(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, name: str, instructions: str, lang: str, prompts: Sequence[tuple[str, bool]]
    ) -> list[str] | None:
        lead = "\n".join(part for part in (name.strip(), instructions.strip()) if part)
        if lead:
            self.notes.append(lead)
        answers: list[str] = []
        for index, (prompt, echo) in enumerate(prompts):
            text = f"{lead}\n{prompt}" if lead and index == 0 else prompt
            answer = await self.ask(text, echo) if self.ask is not None else None
            if answer is None:
                self.declined = True
                return None
            answers.append(answer)
        return answers

    async def password_auth_requested(self) -> str | None:  # pyright: ignore[reportIncompatibleMethodOverride]
        if not self._try("password") or self.ask is None:
            return None
        answer = await self.ask(f"{self.profile.user}@{self.profile.host}'s password: ", False)
        if answer is None:
            self.declined = True
        return answer


@dataclass
class Conn:
    """One held connection and the calls on it."""

    conn: asyncssh.SSHClientConnection
    client: _Client
    capacity: int
    hops: list[asyncssh.SSHClientConnection]
    active: int = 0
    joined: bool = False
    """Another relay process's signed-in connection, reached through ``mux``."""

    def alive(self) -> bool:
        return not self.client.lost.is_set()

    def close(self) -> None:
        self.conn.close()
        for hop in reversed(self.hops):
            hop.close()


def _where(profile: Profile) -> str:
    return f"{profile.host} port {profile.port}"


def _known_hosts(files: Sequence[str]) -> asyncssh.SSHKnownHosts:
    """Return the host keys on record. None on record means none is trusted."""
    if not files:
        return asyncssh.import_known_hosts("")
    return asyncssh.read_known_hosts(list(files))


def _recorded(profile: Profile, extra: Sequence[str]) -> bool:
    """Return whether any known_hosts file names this host at all."""
    name = profile.host_key_alias or profile.host
    port = None if profile.port == 22 else profile.port
    with contextlib.suppress(Exception):
        found = _known_hosts([*profile.known_hosts, *extra]).match(name, "", port)
        return bool(found[0] or found[1] or found[2])
    return False


async def _fingerprint(profile: Profile, tunnel: Any) -> str | None:
    with contextlib.suppress(Exception):
        direct = tunnel is None and not profile.proxy_command
        sock = (await _dial(profile, 15, 15))[0] if direct else None
        key = await asyncio.wait_for(
            asyncssh.get_server_host_key(
                profile.host,
                profile.port,
                tunnel=tunnel,
                proxy_command=(
                    _proxy_argv(profile.proxy_command, profile)
                    if tunnel is None and profile.proxy_command
                    else ()
                ),
                sock=sock,
            ),
            15,
        )
        if key is not None:
            return key.get_fingerprint()
    return None


class ProxyRefused(OSError):
    """The proxy would not open a tunnel to the site."""


def proxy_for(host: str, env: Mapping[str, str] | None = None) -> str:
    """Return the proxy URL the environment names for ``host``, or ``""`` to dial directly."""
    env = os.environ if env is None else env
    exempt = env.get("NO_PROXY") or env.get("no_proxy") or ""
    if exempt and _bypass(host, {"no": exempt}):
        return ""
    for key in PROXY_VARIABLES:
        value = (env.get(key) or "").strip()
        if value:
            return value if "://" in value else f"http://{value}"
    return ""


def _shown(proxy: str) -> str:
    """Return the proxy URL without its credentials."""
    parts = urlsplit(proxy)
    return f"{parts.scheme}://{parts.hostname or ''}{f':{parts.port}' if parts.port else ''}"


def _authority(host: str, port: int) -> str:
    return f"[{host}]:{port}" if ":" in host and not host.startswith("[") else f"{host}:{port}"


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ProxyRefused(errno.ECONNRESET, "the proxy closed the connection")
        data += chunk
    return data


def _proxy_open(proxy: str, host: str, port: int, dial_s: float, answer_s: float) -> socket.socket:
    """Return a socket joined to ``host:port`` through ``proxy`` (HTTP ``CONNECT`` or SOCKS5)."""
    parts = urlsplit(proxy)
    socks = parts.scheme.lower().startswith("socks")
    sock = socket.create_connection(
        (parts.hostname or "127.0.0.1", parts.port or (1080 if socks else 8080)), timeout=dial_s
    )
    try:
        sock.settimeout(answer_s)
        user, password = unquote(parts.username or ""), unquote(parts.password or "")
        if socks:
            methods = b"\x00\x02" if user else b"\x00"
            sock.sendall(b"\x05" + bytes([len(methods)]) + methods)
            chosen = _recv_exact(sock, 2)[1]
            if chosen == 0x02:
                sock.sendall(
                    b"\x01" + bytes([len(user)]) + user.encode()
                    + bytes([len(password)]) + password.encode()
                )  # fmt: skip
                if _recv_exact(sock, 2)[1] != 0:
                    raise ProxyRefused(0, "the SOCKS proxy refused the credentials")
            elif chosen != 0x00:
                raise ProxyRefused(0, "the SOCKS proxy offered no usable sign-in method")
            name = host.strip("[]").encode("idna")
            sock.sendall(b"\x05\x01\x00\x03" + bytes([len(name)]) + name + port.to_bytes(2, "big"))
            head = _recv_exact(sock, 4)
            if head[1] != 0:
                raise ProxyRefused(0, f"the SOCKS proxy answered {head[1]}")
            _recv_exact(sock, ({1: 4, 4: 16}.get(head[3]) or _recv_exact(sock, 1)[0]) + 2)
        else:
            target = _authority(host, port)
            lines = [f"CONNECT {target} HTTP/1.1", f"Host: {target}"]
            if parts.username is not None:
                token = base64.b64encode(f"{user}:{password}".encode()).decode()
                lines.append(f"Proxy-Authorization: Basic {token}")
            sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode())
            # One byte at a time: the site's banner may follow the reply head at once.
            head = b""
            while not head.endswith(b"\r\n\r\n"):
                head += _recv_exact(sock, 1)
                if len(head) > _MAX_PROXY_HEAD:
                    raise ProxyRefused(0, "the proxy's answer is too large")
            status = head.split(b"\r\n", 1)[0].decode("latin-1", "replace").strip()
            fields = status.split()
            if len(fields) < 2 or fields[1] != "200":
                raise ProxyRefused(0, f"the proxy answered {status or 'nothing'}")
        sock.settimeout(None)
        return sock
    except BaseException:
        sock.close()
        raise


async def _dial(
    profile: Profile, dial_s: float, answer_s: float
) -> tuple[socket.socket | None, list[str]]:
    """Return a socket through the environment's proxy (``None``: dial directly) and the route.

    When the proxy can't open the tunnel, relay dials directly, as ``ssh`` itself would.
    """
    where = _authority(profile.host, profile.port)
    proxy = proxy_for(profile.host)
    if not proxy:
        return None, [f"relay dialed {where} directly (no proxy in its environment)"]
    try:
        sock = await asyncio.to_thread(
            _proxy_open, proxy, profile.host, profile.port, dial_s, answer_s
        )
    except OSError as exc:
        return None, [
            f"relay's proxy {_shown(proxy)} did not open {where} ({_os_error(exc)}),"
            " so relay dialed directly"
        ]
    return sock, [f"relay dialed {where} through the proxy {_shown(proxy)}"]


def _os_error(exc: OSError) -> str:
    winerror = getattr(exc, "winerror", None)
    code = f"[WinError {winerror}] " if winerror else f"[Errno {exc.errno}] " if exc.errno else ""
    return f"{code}{exc.strerror or exc}"


_ERRNO_TEXT = {
    errno.ECONNREFUSED: "Connection refused",
    errno.ETIMEDOUT: "Connection timed out",
    errno.EHOSTUNREACH: "No route to host",
    errno.ENETUNREACH: "Network is unreachable",
    errno.ECONNRESET: "Connection reset by peer",
    10061: "Connection refused",
    1225: "Connection refused",
    10060: "Connection timed out",
    10065: "No route to host",
    10051: "Network is unreachable",
    10054: "Connection reset by peer",
}


async def failure_text(
    exc: BaseException,
    profile: Profile,
    client: _Client | None,
    tunnel: Any,
    extra: Sequence[str],
    route: Sequence[str] = (),
) -> str:
    """Return what OpenSSH would have said for a connection that did not open.

    A connection that failed to open also says how relay dialed (``route``) and the
    system's own error, since relay, not the person's ``ssh``, made it.
    """
    notes = "\n".join(client.notes) + "\n" if client is not None and client.notes else ""
    dialed = "".join(f"\n{line}" for line in route)
    who = f"{profile.user}@{profile.host}"
    if isinstance(exc, asyncssh.HostKeyNotVerifiable):
        name = profile.host_key_alias or profile.host
        if not _recorded(profile, extra):
            return (
                f"No matching host key is known for {name} and you have requested strict "
                "checking.\nHost key verification failed."
            )
        fingerprint = await _fingerprint(profile, tunnel)
        entry = name if profile.port == 22 else f"[{name}]:{profile.port}"
        return (
            "@@@ WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED! @@@\n"
            + (f"The key sent by the remote host is\n{fingerprint}.\n" if fingerprint else "")
            + f"ssh-keygen -R {entry}\nHost key verification failed."
        )
    if isinstance(exc, asyncssh.PermissionDenied):
        interactive = sorted(client.offered - {"publickey"}) if client is not None else []
        if client is not None and client.ask is not None:
            methods = interactive or ["keyboard-interactive"]
        elif client is not None and interactive and not client.keys_ran_out:
            # The site took the key and wants a code next, as OpenSSH reports it.
            methods = interactive
        else:
            methods = ["publickey", *interactive]
        return f"{notes}{who}: Permission denied ({','.join(methods)})."
    if isinstance(exc, asyncssh.DisconnectError):
        reason = exc.reason.strip()
        return f"{notes}{reason + chr(10) if reason else ''}Connection closed by {_where(profile)}"
    if isinstance(exc, socket.gaierror):
        what = (
            "Name or service not known"
            if exc.errno in (socket.EAI_NONAME, getattr(socket, "EAI_NODATA", -5), 11001)
            else "Temporary failure in name resolution"
        )
        return f"ssh: Could not resolve hostname {profile.host}: {what}{dialed}"
    if isinstance(exc, TimeoutError):
        return f"ssh: connect to host {_where(profile)}: Connection timed out{dialed}"
    if isinstance(exc, OSError):
        code = getattr(exc, "winerror", None) or exc.errno
        text = _ERRNO_TEXT.get(code or 0) or exc.strerror or str(exc)
        detail = f" ({_os_error(exc)})" if route else ""
        return f"ssh: connect to host {_where(profile)}: {text}{dialed}{detail}"
    if isinstance(exc, (asyncssh.KeyImportError, asyncssh.KeyEncryptionError)):
        return f"ssh: could not load a key for {profile.destination}: {exc}"
    return f"ssh: {type(exc).__name__}: {exc}"


def _proxy_argv(command: str, profile: Profile) -> list[str]:
    """Return ssh's ``ProxyCommand`` with its tokens, run by the shell as ssh runs it."""
    tokens = {"%h": profile.host, "%p": str(profile.port), "%r": profile.user, "%%": "%"}
    out, index = "", 0
    while index < len(command):
        pair = command[index : index + 2]
        if pair in tokens:
            out += tokens[pair]
            index += 2
            continue
        out += command[index]
        index += 1
    if os.name == "nt":
        return [os.environ.get("COMSPEC", "cmd.exe"), "/c", out]
    return [os.environ.get("SHELL") or "/bin/sh", "-c", f"exec {out}"]


async def connect(
    profile: Profile,
    *,
    ask: Ask | None,
    timeout_s: float,
    base: Sequence[str],
    env: Mapping[str, str] | None = None,
    extra_known: Sequence[str] = (),
    capacity: int = 8,
    depth: int = 0,
    via: asyncssh.SSHClientConnection | None = None,
) -> Conn:
    """Open one connection as the person's ssh would, through its jump hosts.

    ``via`` is a connection to tunnel through instead of the profile's own jumps.
    Raises ``SshFailure`` with OpenSSH's words for what went wrong.
    """
    hops: list[asyncssh.SSHClientConnection] = []
    tunnel: asyncssh.SSHClientConnection | None = via
    if via is None and profile.proxy_jump and depth < MAX_HOPS:
        for index, hop in enumerate(profile.proxy_jump):
            hop_profile = await profiles.resolve(list(base), hop, dict(env) if env else None)
            if index > 0:
                hop_profile.proxy_jump, hop_profile.proxy_command = [], None
            try:
                opened = await connect(
                    hop_profile,
                    ask=ask,
                    timeout_s=timeout_s,
                    base=base,
                    env=env,
                    depth=depth + 1,
                )
            except SshFailure:
                for done in reversed(hops):
                    done.close()
                raise
            hops += [*opened.hops, opened.conn] if index == 0 else [opened.conn]
            tunnel = opened.conn
    client = _Client(profile, ask)
    options: dict[str, Any] = {
        "port": profile.port,
        "username": profile.user,
        "config": None,
        "known_hosts": (
            _known_hosts([*profile.known_hosts, *extra_known]) if profile.check_host_keys else None
        ),
        "client_keys": profile.identity_files,
        "client_certs": profile.certificate_files,
        "ignore_encrypted": True,
        "preferred_auth": ",".join(
            profile.preferred_auth or ["publickey", "keyboard-interactive", "password"]
        ),
        "keepalive_interval": KEEPALIVE_S,
        "keepalive_count_max": KEEPALIVE_COUNT,
        "gss_auth": False,
        "gss_kex": False,
        "connect_timeout": profile.connect_timeout_s or timeout_s,
    }
    if profile.host_key_alias:
        options["host_key_alias"] = profile.host_key_alias
    if profile.identities_only and profile.identity_files:
        options["agent_identities"] = profile.identity_files
    if profile.identity_agent:
        options["agent_path"] = profile.identity_agent
    if tunnel is not None:
        options["tunnel"] = tunnel
    elif profile.proxy_command:
        options["proxy_command"] = _proxy_argv(profile.proxy_command, profile)
    route: list[str] = []
    try:
        if tunnel is None and not profile.proxy_command:
            options["sock"], route = await _dial(profile, options["connect_timeout"], timeout_s)
        conn, _ = await asyncssh.create_connection(lambda: client, profile.host, **options)
    except (asyncssh.Error, OSError, ValueError) as exc:
        text = await failure_text(exc, profile, client, tunnel, extra_known, route)
        for done in reversed(hops):
            done.close()
        raise SshFailure(text) from exc
    return Conn(conn=conn, client=client, capacity=capacity, hops=hops)


class _Busy(Exception):
    """The site refused another channel on a live connection (its ``MaxSessions``)."""


class Held:
    """The connections relay holds to one site, and the calls over them."""

    def __init__(
        self,
        *,
        base: Sequence[str],
        destination: str,
        signs_in: bool,
        channels: int,
        timeout_s: float,
        site: str = "",
        share: Path | None = None,
    ) -> None:
        self.base = list(base)
        self.share = share
        self._shared: mux.Shared | None = None
        self._token = secrets.token_hex(8)
        self.destination = destination
        self.signs_in = signs_in
        self.channels = channels
        self.timeout_s = timeout_s
        self.site = site
        self._conns: list[Conn] = []
        self._cond: asyncio.Condition | None = None
        self._signing: asyncio.Lock | None = None
        self._opening = False
        self._failures = 0
        self._failure: Completed | None = None
        self._profile: tuple[float, Profile] | None = None

    # any loop

    def signed_in(self) -> bool:
        """Return whether a connection is up."""
        return any(item.alive() for item in self._conns)

    def live(self) -> Conn | None:
        """Return a connection that is up, for a forward to ride. Call on ``runner``."""
        return next((item for item in self._conns if item.alive()), None)

    async def profile(self) -> Profile:
        """Return what the person's ssh makes of the destination (read again each minute)."""
        return await runner.call(self._resolve())

    async def run(
        self,
        command: str,
        *,
        stdin: bytes,
        hold_stdin: bool,
        timeout_s: float | None,
        max_output_bytes: int | None,
        marker: bytes,
        on_stall: StallReport | None,
        stall_s: float = STALL_S,
    ) -> Completed:
        """Run ``command`` on the site over a held connection."""
        stall = _back(on_stall, asyncio.get_running_loop())

        async def work(slot: Conn) -> Completed:
            return await self._exec(
                slot,
                command,
                stdin,
                hold_stdin,
                timeout_s,
                max_output_bytes,
                marker,
                stall,
                stall_s,
            )

        return await runner.call(self._call(work))

    async def upload(self, sources: Sequence[Path], destination: str) -> Completed:
        """Copy local files into a directory on the site over SFTP, keeping times and modes."""

        async def work(slot: Conn) -> Completed:
            return await self._sftp(slot, sources, destination)

        return await runner.call(self._call(work))

    async def sign_in(self, env: Mapping[str, str] | None) -> None:
        """Sign in through the person's prompts and hold the connection. Raises ``SshFailure``."""
        await runner.call(self._sign_in(env))

    async def close(self) -> int:
        """Close every connection; return how many were up."""
        return await runner.call(self._close())

    # the private loop

    def _condition(self) -> asyncio.Condition:
        if self._cond is None:
            self._cond = asyncio.Condition()
        return self._cond

    async def _resolve(self) -> Profile:
        now = time.monotonic()
        if self._profile is None or now - self._profile[0] > 60:
            self._profile = (
                now,
                await profiles.resolve(self.base, self.destination, site=self.site),
            )
        return self._profile[1]

    def _failed(self, text: str) -> Completed:
        return Completed(
            argv=[self.base[0], self.destination],
            exit_code=FAILED,
            stdout=b"",
            stderr=text.encode(),
        )

    async def _open(self, ask: Ask | None, env: Mapping[str, str] | None = None) -> Conn:
        self._profile = None
        profile = await self._resolve()
        return await connect(
            profile,
            ask=ask,
            timeout_s=self.timeout_s,
            base=self.base,
            env=env,
            capacity=self.channels,
        )

    async def _sign_in(self, env: Mapping[str, str] | None) -> None:
        if self._signing is None:
            self._signing = asyncio.Lock()
        async with self._signing:
            if self.signed_in():
                return
            opened = await self._join()
            if opened is None:
                opened = await self._open(asker(env), env)
                await self._publish(opened)
            cond = self._condition()
            async with cond:
                self._conns = [item for item in self._conns if item.alive()] + [opened]
                cond.notify_all()

    async def _join(self) -> Conn | None:
        """Join another relay process's signed-in connection to the site, if one is up."""
        if self.share is None:
            return None
        profile = await self._resolve()
        client = _Client(profile, None)
        conn = await mux.join(self.share, lambda: client, self._token)
        if conn is None:
            return None
        return Conn(conn=conn, client=client, capacity=self.channels, hops=[], joined=True)

    async def _publish(self, opened: Conn) -> None:
        """Offer a fresh sign-in to relay's other processes (see ``mux``)."""
        if self.share is None:
            return
        if self._shared is not None:
            self._shared.close()
        try:
            self._shared = await mux.publish(
                self.share,
                opened.conn,
                opened.client.lost,
                self._acquire,
                self._release,
                self._token,
            )
        except (OSError, asyncssh.Error) as exc:
            logger.warning(
                "%s: not sharing the sign-in with relay's other processes: %s",
                self.destination,
                exc,
            )

    async def _acquire(self) -> Conn | Completed:
        cond = self._condition()
        async with cond:
            seen = self._failures
            while True:
                self._conns = [item for item in self._conns if item.alive()]
                free = next((item for item in self._conns if item.active < item.capacity), None)
                if free is not None:
                    free.active += 1
                    return free
                if self._failures > seen and not self._conns and self._failure is not None:
                    return self._failure
                if self.signs_in:
                    if not self._conns and not self._opening:
                        self._opening = True
                        break
                elif not self._opening and len(self._conns) < POOL:
                    self._opening = True
                    break
                await cond.wait()
        try:
            if self.signs_in:
                denied = f"{self.destination}: Permission denied (keyboard-interactive)."
                opened: Conn | Completed = await self._join() or self._failed(denied)
            else:
                opened = await self._open(None)
        except SshFailure as failure:
            opened = self._failed(failure.text)
        async with cond:
            self._opening = False
            if isinstance(opened, Completed):
                self._failures += 1
                self._failure = opened
            else:
                opened.active += 1
                self._conns.append(opened)
            cond.notify_all()
        return opened

    async def _release(self, slot: Conn) -> None:
        cond = self._condition()
        async with cond:
            slot.active -= 1
            cond.notify_all()

    async def _call(self, work: Callable[[Conn], Awaitable[Completed]]) -> Completed:
        for _attempt in range(BUSY_RETRIES):
            slot = await self._acquire()
            if isinstance(slot, Completed):
                return slot
            try:
                return await work(slot)
            except _Busy:
                slot.capacity = max(1, slot.active - 1)
                logger.info("%s refused another channel; using %d", self.destination, slot.capacity)
            finally:
                await self._release(slot)
            await asyncio.sleep(0.5)
        return self._failed(f"channel open failed: {self.destination} refused every new channel")

    def _closed(self, slot: Conn) -> Completed:
        """A channel could not open because the connection went: nothing ran, so it may retry."""
        profile = slot.client.profile
        return self._failed(f"Connection closed by {_where(profile)}")

    async def _exec(
        self,
        slot: Conn,
        command: str,
        stdin: bytes,
        hold_stdin: bool,
        timeout_s: float | None,
        limit: int | None,
        marker: bytes,
        on_stall: StallReport | None,
        stall_s: float,
    ) -> Completed:
        started = time.monotonic()
        try:
            process = await slot.conn.create_process(command, encoding=None)
        except asyncssh.ChannelOpenError as exc:
            if not slot.alive():
                return self._closed(slot)
            raise _Busy() from exc
        except (asyncssh.Error, OSError):
            return self._closed(slot)
        out = Sink(limit)
        err = Sink(limit, marker)

        async def feed() -> None:
            with contextlib.suppress(asyncssh.Error, OSError, BrokenPipeError):
                if stdin:
                    process.stdin.write(stdin)
                    await process.stdin.drain()
                if not hold_stdin:
                    process.stdin.write_eof()

        async def pump(sink: Sink, stream: Any) -> None:
            with contextlib.suppress(asyncssh.Error, OSError):
                await sink.pump(stream)

        async def ended() -> None:
            with contextlib.suppress(asyncssh.Error, OSError):
                await process.wait_closed()

        work = asyncio.ensure_future(
            asyncio.gather(pump(out, process.stdout), pump(err, process.stderr), feed(), ended())
        )
        timed_out = await supervise(
            work,
            out,
            err,
            started=started,
            timeout_s=timeout_s,
            on_stall=on_stall,
            stall_s=stall_s,
            stop=process.close,
            name=self.destination,
        )
        process.close()
        remote = err.exited.is_set()
        stderr = bytes(err.data)
        code = process.exit_status
        if not remote and not timed_out and (code is None or not slot.alive()):
            code = FAILED
            if not slot.alive():
                host = slot.client.profile.host
                stderr += f"\nConnection to {host} closed by remote host.\n".encode()
        return Completed(
            argv=[self.base[0], self.destination],
            exit_code=out_code(remote, err.exit_code, timed_out, code),
            stdout=bytes(out.data),
            stderr=stderr,
            stdout_truncated=out.truncated,
            stderr_truncated=err.truncated,
            timed_out=timed_out and not remote,
            duration_s=time.monotonic() - started,
            remote_exit=remote,
        )

    async def _sftp(self, slot: Conn, sources: Sequence[Path], destination: str) -> Completed:
        if slot.joined:
            return await self._tar(slot, sources, destination)
        target = destination.rstrip("/")
        if target.startswith("~/"):
            target = target[2:]
        try:
            async with slot.conn.start_sftp_client() as sftp:
                if not await sftp.isdir(target or "."):
                    return Completed(
                        argv=["sftp", self.destination],
                        exit_code=1,
                        stdout=b"",
                        stderr=f"sftp: {destination}: No such file or directory".encode(),
                    )
                await sftp.put([str(path) for path in sources], f"{target}/", preserve=True)
        except asyncssh.ChannelOpenError as exc:
            if not slot.alive():
                return self._closed(slot)
            raise _Busy() from exc
        except asyncssh.SFTPError as exc:
            return Completed(
                argv=["sftp", self.destination],
                exit_code=1,
                stdout=b"",
                stderr=f"sftp: {destination}: {exc.reason}".encode(),
            )
        except (asyncssh.Error, OSError) as exc:
            if not slot.alive():
                host = slot.client.profile.host
                return self._failed(f"lost connection\nConnection to {host} closed by remote host.")
            return Completed(
                argv=["sftp", self.destination], exit_code=1, stdout=b"", stderr=str(exc).encode()
            )
        return Completed(argv=["sftp", self.destination], exit_code=0, stdout=b"", stderr=b"")

    async def _tar(self, slot: Conn, sources: Sequence[Path], destination: str) -> Completed:
        """Copy over a joined connection, which carries no SFTP (see ``mux``)."""
        try:
            code, err = await mux.tar_upload(slot.conn, sources, destination)
        except asyncssh.ChannelOpenError as exc:
            if not slot.alive():
                return self._closed(slot)
            raise _Busy() from exc
        except (asyncssh.Error, OSError):
            return self._closed(slot)
        if code and not slot.alive():
            host = slot.client.profile.host
            return self._failed(f"lost connection\nConnection to {host} closed by remote host.")
        return Completed(argv=["sftp", self.destination], exit_code=code, stdout=b"", stderr=err)

    async def _close(self) -> int:
        if self._shared is not None:
            self._shared.close()
            self._shared = None
        cond = self._condition()
        async with cond:
            conns, self._conns = self._conns, []
            cond.notify_all()
        up = 0
        for item in conns:
            up += item.alive()
            item.close()
        return up


def out_code(remote: bool, marked: int | None, timed_out: bool, code: int | None) -> int | None:
    """Return a call's exit: the marker's, else none on a timeout, else the channel's."""
    if remote:
        return marked
    return None if timed_out else code


def _back(on_stall: StallReport | None, loop: asyncio.AbstractEventLoop) -> StallReport | None:
    """Return ``on_stall`` run back on the caller's loop, from the private one."""
    if on_stall is None:
        return None
    report = on_stall

    async def stall(idle: float, next_in: float) -> None:
        coro: Coroutine[Any, Any, None] = report(idle, next_in)  # pyright: ignore[reportAssignmentType]
        await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(coro, loop))

    return stall
