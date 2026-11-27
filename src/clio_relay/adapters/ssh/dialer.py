"""How relay's held ssh connection reaches a site, and how it says so when it can't.

Relay dials through the proxy its environment names (:mod:`clio_relay.net.proxy`),
unless the person's config gives a ``ProxyCommand`` or jump host.
"""

from __future__ import annotations

import asyncio
import errno
import os
import socket

from clio_relay.adapters.ssh.profile import Profile
from clio_relay.net import proxy as proxies


def where(profile: Profile) -> str:
    return f"{profile.host} port {profile.port}"


def authority(host: str, port: int) -> str:
    return f"[{host}]:{port}" if ":" in host and not host.startswith("[") else f"{host}:{port}"


async def dial(
    profile: Profile, dial_s: float, answer_s: float
) -> tuple[socket.socket | None, list[str]]:
    """Return a socket through the environment's proxy (``None``: dial directly) and the route.

    When the proxy can't open the tunnel, relay dials directly, as ``ssh`` itself would.
    """
    where = authority(profile.host, profile.port)
    url = proxies.from_environment("ssh", profile.host, os.environ)
    if not url:
        return None, [f"relay dialed {where} directly (no proxy in its environment)"]
    shown = url.rpartition("@")[2]
    try:
        proxy = proxies.parse(url)
        shown = proxy.shown
        sock = await asyncio.wait_for(
            proxies.open_socket(proxy, profile.host, profile.port), dial_s + answer_s
        )
    except (TimeoutError, OSError, ValueError) as exc:
        late = isinstance(exc, TimeoutError) and not str(exc)
        reason = (
            "it did not answer in time"
            if late
            else str(exc).removeprefix(f"the proxy at {shown} ")
            if isinstance(exc, proxies.ProxyError)
            else os_error(exc)
            if isinstance(exc, OSError)
            else str(exc)
        )
        return None, [
            f"relay's proxy {shown} did not open {where} ({reason}), so relay dialed directly"
        ]
    return sock, [f"relay dialed {where} through the proxy {shown}"]


def os_error(exc: OSError) -> str:
    winerror = getattr(exc, "winerror", None)
    code = f"[WinError {winerror}] " if winerror else f"[Errno {exc.errno}] " if exc.errno else ""
    return f"{code}{exc.strerror or exc}"


ERRNO_TEXT = {
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


def proxy_argv(command: str, profile: Profile) -> list[str]:
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
