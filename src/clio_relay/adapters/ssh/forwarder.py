"""A served port's desktop end over ssh: loopback ports forwarded over relay's own connection.

Two ways, one routine (``hold``):

- detached (``python -m clio_relay.adapters.ssh.forwarder``): its own connection,
  signed in with the person's keys, so it outlives relay and the next relay
  process adopts it;
- in relay (``InRelay``): a site that signs in with a code forwards over the
  connection relay already holds, so the person signs in once. It ends with
  relay, and after a restart the person signs in again before it comes back.

Either way it logs OpenSSH's words for what went wrong, which ``forward_exit``
reads to tell a forward worth starting again from one that is not.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import contextlib
import dataclasses
import json
import os
import subprocess
import sys
import threading
from collections.abc import Callable, Sequence
from pathlib import Path

import asyncssh

from clio_relay.adapters.ssh import held
from clio_relay.adapters.ssh import profile as profiles
from clio_relay.jobs.refusals import Refusal

NOT_SIGNED_IN = "relay holds no signed-in connection to"
"""What an in-relay forward logs when the person has to sign in again first."""
CONNECT_TIMEOUT_S = 30.0

Ports = Sequence[tuple[int, int]]


def command(
    base: Sequence[str], destination: str, ports: Ports, node: str | None, known_hosts: Path | None
) -> list[str]:
    """Return the detached forwarder's argv. It holds ``127.0.0.1:local:127.0.0.1``."""
    argv = [
        sys.executable,
        "-m",
        "clio_relay.adapters.ssh.forwarder",
        "--base",
        json.dumps(list(base)),
        "--destination",
        destination,
    ]
    if node:
        argv += ["--node", node]
    if known_hosts is not None:
        argv += ["--known-hosts", str(known_hosts)]
    for local, remote in ports:
        argv += ["-L", f"127.0.0.1:{local}:127.0.0.1:{remote}"]
    return argv


async def hold(
    login: held.Conn,
    ports: Ports,
    *,
    node: str | None,
    extra_known: Sequence[str],
    base: Sequence[str],
    env: dict[str, str] | None,
) -> tuple[held.Conn, list[asyncssh.SSHListener]]:
    """Forward each loopback port to the site, or to ``node`` through the login host.

    The node gets the login host's user and keys, and its host key is checked
    against the person's known_hosts and ``extra_known`` (read on the login host).
    """
    target = login
    if node:
        profile = dataclasses.replace(
            login.client.profile,
            destination=node,
            host=node,
            port=22,
            proxy_jump=[],
            proxy_command=None,
            host_key_alias=None,
        )
        target = await held.connect(
            profile,
            ask=None,
            timeout_s=CONNECT_TIMEOUT_S,
            base=base,
            env=env,
            extra_known=extra_known,
            via=login.conn,
        )
    listeners: list[asyncssh.SSHListener] = []
    for local, remote in ports:
        try:
            listener = await target.conn.forward_local_port("127.0.0.1", local, "127.0.0.1", remote)
        except OSError as exc:
            for item in listeners:
                item.close()
            if target is not login:
                target.close()
            raise held.SshFailure(
                f"bind [127.0.0.1]:{local}: {exc.strerror or exc}\n"
                f"Could not request local forwarding."
            ) from exc
        listeners.append(listener)
    return target, listeners


async def _until_lost(login: held.Conn, target: held.Conn, say: Callable[[str], None]) -> int:
    waits = {asyncio.ensure_future(login.client.lost.wait())}
    if target is not login:
        waits.add(asyncio.ensure_future(target.client.lost.wait()))
    try:
        await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for wait in waits:
            wait.cancel()
    gone = login if login.client.lost.is_set() else target
    say(f"Connection to {gone.client.profile.host} closed by remote host.")
    return held.FAILED


# in relay


class InRelay:
    """A forward over relay's held connection, watched as ``Forward`` watches a process."""

    pid = 0
    """Not a process of its own: nothing outside relay may signal it."""

    def __init__(self, future: concurrent.futures.Future[int], ended: threading.Event) -> None:
        self._future = future
        self._ended = ended

    @classmethod
    def start(
        cls,
        connection: held.Held,
        ports: Ports,
        *,
        node: str | None,
        known_hosts: Path | None,
        log: Path,
    ) -> InRelay:
        """Start forwarding on relay's ssh loop; failures go to ``log``."""
        extra = [str(known_hosts)] if known_hosts is not None else []
        started, ended = threading.Event(), threading.Event()
        coro = _in_relay(connection, ports, node, extra, log, started, ended)
        future = held.runner.submit(coro)
        # Cancelled before it ran, it never reaches its own cleanup.
        future.add_done_callback(lambda _f: None if started.is_set() else ended.set())
        return cls(future, ended)

    def poll(self) -> int | None:
        """Return its exit once it has ended and closed its ports, else None."""
        if not self._future.done() or not self._ended.is_set():
            return None
        if self._future.cancelled():
            return -15
        try:
            return self._future.result()
        except Exception:
            return held.FAILED

    def terminate(self) -> None:
        """Close its ports."""
        self._future.cancel()

    def kill(self) -> None:
        """Close its ports."""
        self._future.cancel()

    def wait(self, timeout: float | None = None) -> int:
        """Wait for it to end, raising ``subprocess.TimeoutExpired`` as ``Popen`` does."""
        concurrent.futures.wait([self._future], timeout=timeout)
        self._ended.wait(timeout)
        code = self.poll()
        if code is None:
            raise subprocess.TimeoutExpired("forward", timeout or 0)
        return code


async def _in_relay(
    connection: held.Held,
    ports: Ports,
    node: str | None,
    extra: list[str],
    log: Path,
    started: threading.Event,
    ended: threading.Event,
) -> int:
    started.set()
    try:
        return await _forward_in_relay(connection, ports, node, extra, log)
    finally:
        ended.set()


async def _forward_in_relay(
    connection: held.Held, ports: Ports, node: str | None, extra: list[str], log: Path
) -> int:
    def say(line: str) -> None:
        with contextlib.suppress(OSError), log.open("a", encoding="utf-8") as handle:
            handle.write(line.rstrip("\n") + "\n")

    login = connection.live()
    if login is None:
        say(f"{NOT_SIGNED_IN} {connection.destination}; sign in again to serve")
        return held.FAILED
    try:
        target, listeners = await hold(
            login, ports, node=node, extra_known=extra, base=connection.base, env=None
        )
    except held.SshFailure as failure:
        say(failure.text)
        return held.FAILED
    try:
        return await _until_lost(login, target, say)
    finally:
        for listener in listeners:
            listener.close()
        if target is not login:
            target.close()


# detached


async def _detached(args: argparse.Namespace) -> int:
    def say(line: str) -> None:
        print(line, file=sys.stderr, flush=True)

    base: list[str] = json.loads(args.base)
    env = dict(os.environ)
    ports = [_port(spec) for spec in args.L]
    try:
        profile = await profiles.resolve(base, args.destination, env)
        login = await held.connect(
            profile,
            ask=held.asker(env),
            timeout_s=profile.connect_timeout_s or CONNECT_TIMEOUT_S,
            base=base,
            env=env,
        )
    except held.SshFailure as failure:
        say(failure.text)
        return held.FAILED
    except Refusal as refusal:
        say(f"ssh: {refusal.message}")
        return held.FAILED
    extra = [args.known_hosts] if args.known_hosts else []
    try:
        target, _listeners = await hold(
            login, ports, node=args.node, extra_known=extra, base=base, env=env
        )
    except held.SshFailure as failure:
        say(failure.text)
        login.close()
        return held.FAILED
    say(f"relay: forwarding {', '.join(str(local) for local, _ in ports)}")
    return await _until_lost(login, target, say)


def _port(spec: str) -> tuple[int, int]:
    """Read ``127.0.0.1:local:127.0.0.1:remote``."""
    parts = spec.split(":")
    return int(parts[1]), int(parts[3])


def main(argv: Sequence[str] | None = None) -> int:
    """Hold the forwards until the connection drops; exit 255 then, as ssh does."""
    parser = argparse.ArgumentParser(prog="clio-relay-forwarder")
    parser.add_argument("--base", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--node")
    parser.add_argument("--known-hosts")
    parser.add_argument("-L", action="append", default=[])
    args = parser.parse_args(argv)
    try:
        return asyncio.run(_detached(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
