"""Reading a desktop port, and why a forward that ended cannot work by starting it again."""

from __future__ import annotations

import errno
import os
import socket

from clio_relay.jobs.refusals import Refusal, RefusalReason

HOST_KEY_FAILED = (
    "host key verification failed",
    "remote host identification has changed",
    "host key is known for",
)
NODE_REFUSED_JOB_ENDED = ("pam_slurm_adopt", "no active jobs on this node")
LOGIN_REFUSED = ("permission denied (",)
PORT_UNUSABLE = ("cannot listen to port", "could not request local forwarding", "bind [")


def port_state(port: int) -> str:
    """Return ``free``, ``in_use`` (something listens there) or ``unusable``, without connecting.

    Windows refuses a bind in a reserved range (``netsh int ipv4 show excludedportrange``)
    with an access error, which says nothing listens there.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        if os.name != "nt":
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError as exc:
            in_use = exc.errno == errno.EADDRINUSE or getattr(exc, "winerror", None) == 10048
            return "in_use" if in_use else "unusable"
        return "free"


def port_bound(port: int) -> bool:
    """Return whether something listens on the loopback ``port``, without connecting."""
    return port_state(port) == "in_use"


def classify_exit(lines: list[str], host: str | None) -> Refusal | None:
    """Return why a forward that ended cannot work by starting it again, or ``None``.

    A node that refuses its key, or a node that refuses relay because the job on
    it ended, refuses every retry the same way, so relay stops and says so.
    """
    text = "\n".join(lines).lower()
    node = host or "the node"
    last = next((line.strip() for line in reversed(lines) if line.strip()), "")
    if any(mark in text for mark in HOST_KEY_FAILED):
        return Refusal(
            RefusalReason.HOST_KEY_UNVERIFIED,
            f"ssh could not verify {node}'s host key, so relay did not connect to it. "
            "Neither the login host's known_hosts files nor this desktop's list a key "
            f"that matches it. Ask the site's admins for {node}'s key, or connect to it "
            "once through the login host to check and accept it, then serve again",
            host=host,
            detail=last,
        )
    if any(mark in text for mark in NODE_REFUSED_JOB_ENDED):
        return Refusal(
            RefusalReason.UNREACHABLE,
            f"{node} refused the forward because the job on it has ended",
            host=host,
            job_ended=True,
            detail=last,
        )
    if any(mark in text for mark in LOGIN_REFUSED):
        return Refusal(
            RefusalReason.UNREACHABLE,
            f"{node} refused relay's ssh login through the login host: {last}",
            host=host,
            detail=last,
        )
    return None


def port_unusable(lines: list[str]) -> bool:
    """Return whether ssh ended because it could not bind a desktop port."""
    text = "\n".join(lines).lower()
    return any(mark in text for mark in PORT_UNUSABLE)
