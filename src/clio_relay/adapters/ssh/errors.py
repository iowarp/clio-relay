"""What an ``ssh`` failure means, and what the person can do about it.

``ssh`` exits with 255 when it fails itself (Windows' OpenSSH sometimes with
-1), and says why on stderr. Each refusal carries a ``cause`` so a client can
tell the cases apart:

- ``key_refused``: the site accepted none of the person's keys;
- ``host_key_unknown``: there is no host key on record, and relay never accepts
  one it cannot check;
- ``host_key_changed``: the host key differs from the one on record;
- ``unknown_host``: the profile is neither a ``Host`` nor a name DNS knows;
- ``network``: the connection could not be made or dropped, so a retry may work;
- ``connection_lost``: the session ended before the remote command reported its
  end, and ssh said nothing clearer (:func:`lost`);
- ``ssh``: anything else ``ssh`` reported.

A site that wants a code or password refuses with ``needs_sign_in`` or
``interactive_auth_required`` instead.
"""

from __future__ import annotations

import re

from clio_relay.jobs.refusals import Refusal, RefusalReason

_INTERACTIVE_METHODS = ("keyboard-interactive", "password")
SSH_FAILED = (255, -1)
"""The exits of ssh failing itself; Windows' OpenSSH may exit -1, read signed."""
_NETWORK_MARKERS = (
    "connection timed out",
    "connection refused",
    "temporary failure in name resolution",
    "no route to host",
    "network is unreachable",
    "connection closed",
    "closed by remote host",
    "connection reset",
    "kex_exchange_identification",
    "timeout, server",
    "broken pipe",
    "client_loop",
    "operation timed out",
    "lost connection",
    "control socket connect",
    "mux_client",
)
# Failures before a session opens: the remote command cannot have started.
_BEFORE_SESSION = re.compile(
    r"connect to host .* port \d+:"
    r"|kex_exchange_identification"
    r"|banner exchange"
    r"|temporary failure in name resolution"
    r"|connection closed by \S+ port \d+"
)
_METHODS = re.compile(r"permission denied \(([^)]*)\)")
_FINGERPRINT = re.compile(r"(SHA256:[A-Za-z0-9+/=]+)")
_UNREADABLE_KEY = re.compile(
    r"(?:no such identity: |identity file )(\S+?)(?::| not accessible)", re.IGNORECASE
)


def who(title: str, profile: str) -> str:
    """Name an ssh destination: the site's title, with the profile when the two differ."""
    return profile if title == profile else f"{title} (ssh {profile})"


def cause(stderr: str, exit_code: int | None) -> str | None:
    """Return why ``ssh`` itself failed, or ``None`` when the remote command did."""
    if exit_code not in SSH_FAILED:
        return None
    text = stderr.lower()
    if "remote host identification has changed" in text or re.search(
        r"host key for \S+ has changed", text
    ):
        return "host_key_changed"
    if "host key verification failed" in text or re.search(r"no \S+ host key is known", text):
        return "host_key_unknown"
    methods = _METHODS.search(text)
    if methods is not None:
        offered = [method.strip() for method in methods.group(1).split(",")]
        # Methods still listed are the ones left to try: with publickey among
        # them, no key got through; without it, the site wants a code or password.
        if "publickey" not in offered and any(m in offered for m in _INTERACTIVE_METHODS):
            return "sign_in"
        return "key_refused"
    if "too many authentication failures" in text:
        return "key_refused"
    if "could not resolve hostname" in text and "temporary failure" not in text:
        return "unknown_host"
    if any(marker in text for marker in _NETWORK_MARKERS):
        return "network"
    if "permission denied" in text or text.startswith("ssh:") or "\nssh:" in text:
        return "ssh"
    return None


def retryable(stderr: str, exit_code: int | None, *, once: bool = False) -> bool:
    """Return whether the connection failed or dropped, so the same call may work again.

    With ``once``, the call must not run twice, so only a connection that never
    opened a session counts.
    """
    if cause(stderr, exit_code) != "network":
        return False
    return not once or _BEFORE_SESSION.search(stderr.lower()) is not None


def lost(stderr: str, exit_code: int | None, *, who: str, what: str) -> Refusal:
    """Return the refusal for a session that ended before the remote command reported its end.

    The script always reports its end through relay's marker, so its absence
    means the connection went, whatever ssh's exit was.
    """
    lines = [line.strip() for line in stderr.strip().splitlines() if line.strip()]
    said = f" ssh said: {lines[-1]}." if lines else ""
    return Refusal(
        RefusalReason.UNREACHABLE,
        (
            f"the ssh connection to {who} was lost during {what}, before the command "
            f"reported its end (ssh exited {exit_code}).{said} The command may have run in "
            "part, so check before running one that changes things again."
        ),
        cause="connection_lost",
    )


def refusal(
    stderr: str,
    exit_code: int | None,
    *,
    profile: str,
    who: str,
    what: str,
    sign_in: bool,
) -> Refusal | None:
    """Return the refusal for an ``ssh`` failure, or ``None`` if ssh did not fail.

    ``who`` names the site as the person knows it, with its ssh ``profile``.
    """
    found = cause(stderr, exit_code)
    if found is None:
        return None
    lines = [line.strip() for line in stderr.strip().splitlines() if line.strip()]
    last = lines[-1] if lines else "ssh failed"
    if found == "sign_in":
        if sign_in:
            return Refusal(
                RefusalReason.NEEDS_SIGN_IN,
                f"{who} needs you to sign in again: {last}",
                cause=found,
            )
        return Refusal(
            RefusalReason.INTERACTIVE_AUTH_REQUIRED,
            (
                f"{who} asks for a code or password on every connection, so the ssh "
                "adapter cannot use it unattended. Set interactive_sign_in on the site to "
                "sign in once through relay, or use the worker adapter. "
                f"ssh said: {last}"
            ),
            adapter="worker",
            cause=found,
        )
    if found == "key_refused":
        unreadable = sorted(set(_UNREADABLE_KEY.findall(stderr)))
        message = f"{who} accepted none of your keys"
        if unreadable:
            message += f" (ssh could not read {', '.join(unreadable)})"
        message += (
            f". Load your key with ssh-add, or name it with IdentityFile under Host {profile} "
            "in ~/.ssh/config, and check that its public key is in ~/.ssh/authorized_keys "
            "on the site."
        )
        if "too many authentication failures" in stderr.lower():
            message += " ssh offered too many keys: set IdentitiesOnly yes for this host."
        methods = _METHODS.search(stderr.lower())
        if methods and any(m in methods.group(1) for m in _INTERACTIVE_METHODS):
            message += (
                " The site also takes a password or code: set interactive_sign_in on the "
                "site to sign in that way."
            )
        return Refusal(RefusalReason.UNREACHABLE, f"{message} ssh said: {last}", cause=found)
    if found == "host_key_changed":
        fingerprint = _FINGERPRINT.search(stderr)
        remove = next(
            (line for line in lines if line.startswith("ssh-keygen ")), "ssh-keygen -R <host>"
        )
        return Refusal(
            RefusalReason.UNREACHABLE,
            (
                f"{who}'s host key is not the one on record"
                + (f" (it now sends {fingerprint.group(1)})" if fingerprint else "")
                + ". The machine may have been reinstalled, or someone may be in the middle. "
                "Check the new fingerprint with the site's administrators. If it is right, "
                f"remove the old key with `{remove}` and connect "
                f"once with `ssh {profile}` to record the new one. Relay will not connect "
                "until then."
            ),
            cause=found,
        )
    if found == "host_key_unknown":
        return Refusal(
            RefusalReason.UNREACHABLE,
            (
                f"ssh has no host key on record for {profile}, and relay never accepts one "
                f"it cannot check. Connect once yourself with `ssh {profile}`, check the "
                "fingerprint and accept it, then try again."
            ),
            cause=found,
        )
    if found == "unknown_host":
        return Refusal(
            RefusalReason.UNREACHABLE,
            (
                f"ssh does not know the host {profile}: it is neither a Host in your ssh "
                f"config nor a name DNS can find. ssh said: {last}"
            ),
            cause=found,
        )
    if found == "network":
        return Refusal(
            RefusalReason.UNREACHABLE,
            f"the connection to {who} failed during {what}: {last}",
            cause=found,
        )
    return Refusal(RefusalReason.UNREACHABLE, f"ssh could not reach {who}: {last}", cause=found)
