"""The person's ssh profile, as their own ``ssh`` reads it (``ssh -G``).

Relay holds its ssh connections itself (``held``), but the person's
``~/.ssh/config`` stays the one authority on how to reach a site: ``ssh -G``
evaluates it (``Match``, ``Include``, tokens, Windows paths) and relay takes
the host, port, user, keys, known hosts and jump hosts from what it prints.

A profile that needs what the held connection cannot do (a PKCS#11 token,
GSSAPI, a FIDO key outside the agent, or a custom ssh program) names why in
``openssh_only``, and relay runs the system ``ssh`` for that site instead.
"""

from __future__ import annotations

import asyncio
import getpass
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from clio_relay import programs
from clio_relay.jobs.refusals import Refusal, RefusalReason

OPENSSH_PROGRAMS = ("ssh", "ssh.exe")
HELD_AUTH = ("publickey", "hostbased", "keyboard-interactive", "password")


@dataclass
class Profile:
    """What ``ssh -G <destination>`` says about one destination."""

    destination: str
    host: str
    port: int = 22
    user: str = ""
    identity_files: list[str] = field(default_factory=list[str])
    certificate_files: list[str] = field(default_factory=list[str])
    identities_only: bool = False
    identity_agent: str | None = None
    """``None``: the default agent; ``""``: no agent; else the agent's socket or pipe."""
    known_hosts: list[str] = field(default_factory=list[str])
    check_host_keys: bool = True
    host_key_alias: str | None = None
    proxy_jump: list[str] = field(default_factory=list[str])
    proxy_command: str | None = None
    connect_timeout_s: float | None = None
    preferred_auth: list[str] = field(default_factory=list[str])
    password_prompts: int = 3
    openssh_only: str | None = None
    """Why only the system ssh can reach this destination, or ``None``."""


def parse(text: str) -> dict[str, list[str]]:
    """Return ``ssh -G``'s ``key value`` lines as lists of values, keys lowercased."""
    values: dict[str, list[str]] = {}
    for line in text.splitlines():
        key, _sep, value = line.strip().partition(" ")
        if key:
            values.setdefault(key.lower(), []).append(value.strip())
    return values


def _expand(path: str, values: dict[str, list[str]]) -> str:
    """Expand what ssh leaves for its own use: ``~``, ``%d``, ``%u``, ``%h``, ``%r``."""
    home = str(Path.home())
    tokens = {
        "%d": home,
        "%u": getpass.getuser(),
        "%h": _one(values, "hostname") or "",
        "%r": _one(values, "user") or "",
        "%%": "%",
    }
    out = ""
    index = 0
    while index < len(path):
        pair = path[index : index + 2]
        if pair in tokens:
            out += tokens[pair]
            index += 2
            continue
        out += path[index]
        index += 1
    out = out.replace("__PROGRAMDATA__", os.environ.get("PROGRAMDATA", r"C:\ProgramData"))
    return os.path.expanduser(out)


def _one(values: dict[str, list[str]], key: str) -> str | None:
    found = values.get(key)
    if not found or found[0].lower() == "none":
        return None
    return found[0]


def _yes(values: dict[str, list[str]], key: str) -> bool:
    return (_one(values, key) or "").lower() in ("yes", "true")


def _sk_key(path: str) -> bool:
    """Return whether an identity is a FIDO security key, which held cannot sign with."""
    try:
        head = Path(f"{path}.pub").read_text(encoding="utf-8", errors="replace")[:32]
    except OSError:
        return False
    return head.startswith("sk-")


def from_values(destination: str, values: dict[str, list[str]], program: str) -> Profile:
    """Build a profile from parsed ``ssh -G`` output."""
    identities = [_expand(item, values) for item in values.get("identityfile", [])]
    identities = [item for item in identities if Path(item).is_file()]
    certificates = [_expand(item, values) for item in values.get("certificatefile", [])]
    known: list[str] = []
    for key in ("userknownhostsfile", "globalknownhostsfile"):
        for entry in values.get(key, []):
            known += [_expand(item, values) for item in entry.split() if item.lower() != "none"]
    agent = _one(values, "identityagent")
    if agent is None and values.get("identityagent"):
        agent = ""
    elif agent == "SSH_AUTH_SOCK":
        agent = None
    elif agent is not None:
        agent = _expand(agent, values)
    jump = _one(values, "proxyjump")
    timeout = _one(values, "connecttimeout")
    preferred = [
        method.strip()
        for method in (_one(values, "preferredauthentications") or "").split(",")
        if method.strip() in HELD_AUTH
    ]
    profile = Profile(
        destination=destination,
        host=_one(values, "hostname") or destination,
        port=int(_one(values, "port") or 22),
        user=_one(values, "user") or getpass.getuser(),
        identity_files=identities,
        certificate_files=[item for item in certificates if Path(item).is_file()],
        identities_only=_yes(values, "identitiesonly"),
        identity_agent=agent,
        known_hosts=[item for item in known if Path(item).is_file()],
        check_host_keys=(_one(values, "stricthostkeychecking") or "ask").lower()
        not in ("no", "off", "false"),
        host_key_alias=_one(values, "hostkeyalias"),
        proxy_jump=[hop.strip() for hop in jump.split(",")] if jump else [],
        proxy_command=_one(values, "proxycommand"),
        connect_timeout_s=float(timeout) if timeout and timeout.isdigit() else None,
        preferred_auth=preferred,
        password_prompts=int(_one(values, "numberofpasswordprompts") or 3),
    )
    profile.openssh_only = openssh_only(profile, values, program)
    return profile


def openssh_only(profile: Profile, values: dict[str, list[str]], program: str) -> str | None:
    """Return why this destination needs the system ssh, or ``None`` if held serves it."""
    if Path(program).name.lower() not in OPENSSH_PROGRAMS:
        return f"the site names its own ssh program ({Path(program).name})"
    if _one(values, "pkcs11provider"):
        return "the profile signs with a PKCS#11 token (PKCS11Provider)"
    if _yes(values, "gssapiauthentication") and _uses_kerberos(values):
        return "the profile signs in with Kerberos (GSSAPIAuthentication)"
    if profile.identity_agent == "" and any(_sk_key(item) for item in profile.identity_files):
        return "the profile's key is a FIDO security key and no agent holds it"
    return None


def _uses_kerberos(values: dict[str, list[str]]) -> bool:
    # Debian and Ubuntu turn GSSAPIAuthentication on for every host, so it counts
    # only when Kerberos comes first or the person holds a ticket cache.
    methods = (_one(values, "preferredauthentications") or "").split(",")
    return methods[0].strip().startswith("gssapi") or bool(os.environ.get("KRB5CCNAME"))


def _command(base: list[str], destination: str) -> list[str]:
    return [*base, "-G", destination]


async def resolve(
    base: list[str], destination: str, env: dict[str, str] | None = None, *, site: str = ""
) -> Profile:
    """Return what the person's ``ssh`` makes of ``destination`` with relay's options.

    ``base`` is the ssh program and the options relay passes (``-F``, ``-o``).
    """
    resolved = programs.resolved(_command(base, destination), env)
    if resolved is None:
        raise Refusal(
            RefusalReason.ADAPTER_UNAVAILABLE, programs.missing(base[0]), site=site, command=base[0]
        )
    try:
        done = await asyncio.to_thread(
            subprocess.run,
            resolved,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
            env=env,
        )
    except OSError as exc:
        raise Refusal(
            RefusalReason.ADAPTER_UNAVAILABLE,
            programs.start_failed(base[0], exc),
            site=site,
            command=base[0],
        ) from exc
    except subprocess.SubprocessError as exc:
        raise Refusal(
            RefusalReason.ADAPTER_UNAVAILABLE,
            f"{base[0]} -G {destination} did not finish: {exc}",
            site=site,
            command=base[0],
        ) from exc
    if done.returncode != 0:
        detail = (done.stderr or done.stdout).strip().splitlines()
        raise Refusal(
            RefusalReason.INVALID_CONFIG,
            f"ssh could not read the profile {destination}: "
            f"{detail[-1] if detail else 'no detail'}",
            site=site,
        )
    return from_values(destination, parse(done.stdout), resolved[0])
