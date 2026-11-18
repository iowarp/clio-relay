"""Registering a person's host as a Tapis system (RL48, RL49).

**A person's own host.** Relay makes a key pair for that system alone, with no
passphrase, as Tapis requires. It appends the public half to
``authorized_keys`` on the host through the site's ``ssh`` alternative, with a
comment naming relay and the system, registers the pair with
``systems.createUserCredential`` (which checks the login from Tapis), and then
deletes its own copy of the private half. The person's own keys are never
read.

**A TACC host.** No key is made. The credential is
``systems.createUserCredential`` with ``createTmsKeys=true``, and Tapis's trust
manager makes and holds the key.

Relay does this when the site is added, edited or switched to
(``on_site_added``), and again only while Tapis's check of the credential
fails, so relay's key is not replaced each time. Removing the site removes the
credential, the system relay made and the ``authorized_keys`` line.
"""

from __future__ import annotations

import asyncio
import shlex
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from clio_relay.adapters.tapis.adapter import TapisAdapter
from clio_relay.adapters.tapis.client import TapisCallError
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import SiteEntry

KEY_BITS = 4096
WORK_DIR_NAME = ".clio-relay/tapis"


@dataclass
class SshTarget:
    host: str
    port: int
    user: str


@dataclass
class SshHost:
    """The site's ``ssh`` alternative, reached with the system ``ssh``.

    The fields mirror the ``ssh`` adapter's settings (``ssh_profile``,
    ``ssh_config``, ``ssh_options``, ``ssh_command``) so an alternative's
    config can be passed as it is.
    """

    ssh_profile: str
    ssh_config: str | None = None
    ssh_options: list[str] = field(default_factory=list[str])
    ssh_command: str = "ssh"

    @classmethod
    def from_alternative(cls, config: dict[str, Any]) -> SshHost:
        profile = config.get("ssh_profile")
        if not isinstance(profile, str) or not profile:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                "the site's ssh alternative names no ssh_profile",
            )
        raw = cast(list[Any], config.get("ssh_options") or [])
        options = [str(item) for item in raw]
        return cls(
            ssh_profile=profile,
            ssh_config=config.get("ssh_config"),
            ssh_options=options,
            ssh_command=str(config.get("ssh_command") or "ssh"),
        )

    def _base(self) -> list[str]:
        argv = [self.ssh_command]
        if self.ssh_config:
            argv += ["-F", str(Path(self.ssh_config).expanduser())]
        for option in self.ssh_options:
            argv += ["-o", option]
        return argv

    def _run(self, argv: list[str], what: str) -> str:
        done = subprocess.run(argv, capture_output=True, text=True, check=False)
        if done.returncode != 0:
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"{what} over ssh to {self.ssh_profile} failed: "
                f"{done.stderr.strip() or done.returncode}",
                profile=self.ssh_profile,
            )
        return done.stdout

    def target(self) -> SshTarget:
        """Read the host, port and user the profile resolves to (``ssh -G``)."""
        text = self._run([*self._base(), "-G", self.ssh_profile], "reading the profile")
        values: dict[str, str] = {}
        for line in text.splitlines():
            key, _, value = line.partition(" ")
            values.setdefault(key.lower(), value.strip())
        return SshTarget(
            host=values.get("hostname", self.ssh_profile),
            port=int(values.get("port", "22")),
            user=values.get("user", ""),
        )

    def run(self, command: str, what: str) -> str:
        argv = [*self._base(), "-o", "BatchMode=yes", self.ssh_profile, command]
        return self._run(argv, what)

    def home(self) -> str:
        return self.run('printf %s "$HOME"', "reading the home directory").strip()

    def partition_limits(self) -> dict[str, int | None]:
        """Slurm partitions and their time limits in minutes (None when unlimited)."""
        text = self.run(
            "command -v sinfo >/dev/null 2>&1 && sinfo -h -o '%P %l' || true",
            "reading partitions",
        )
        found: dict[str, int | None] = {}
        for line in text.splitlines():
            name, _, limit = line.strip().partition(" ")
            name = name.rstrip("*")
            if name and name not in found:
                found[name] = slurm_minutes(limit)
        return found

    def add_key(self, public_key: str, comment: str) -> None:
        line = f"{public_key.strip().split(' ')[0]} {public_key.strip().split(' ')[1]} {comment}"
        script = (
            "umask 077 && mkdir -p ~/.ssh && touch ~/.ssh/authorized_keys && "
            f"(grep -v -F -- {shlex.quote(comment)} ~/.ssh/authorized_keys || true) "
            "> ~/.ssh/authorized_keys.clio-relay && "
            f"printf '%s\\n' {shlex.quote(line)} >> ~/.ssh/authorized_keys.clio-relay && "
            "mv ~/.ssh/authorized_keys.clio-relay ~/.ssh/authorized_keys"
        )
        self.run(script, "installing relay's key")

    def remove_key(self, comment: str) -> None:
        script = (
            "[ -f ~/.ssh/authorized_keys ] || exit 0; umask 077 && "
            f"(grep -v -F -- {shlex.quote(comment)} ~/.ssh/authorized_keys || true) "
            "> ~/.ssh/authorized_keys.clio-relay && "
            "mv ~/.ssh/authorized_keys.clio-relay ~/.ssh/authorized_keys"
        )
        self.run(script, "removing relay's key")

    def has_key(self, comment: str) -> bool:
        text = self.run(
            f"grep -c -F -- {shlex.quote(comment)} ~/.ssh/authorized_keys 2>/dev/null || true",
            "reading authorized_keys",
        )
        return text.strip() not in ("", "0")


def key_comment(system_id: str) -> str:
    return f"clio-relay-tapis:{system_id}"


@dataclass
class KeyPair:
    """A key pair relay made in a private temporary directory."""

    directory: Path
    private_key: str
    public_key: str

    def destroy(self) -> None:
        shutil.rmtree(self.directory, ignore_errors=True)

    @property
    def gone(self) -> bool:
        return not self.directory.exists()


def make_key_pair(comment: str) -> KeyPair:
    """Make an RSA key in PEM form, with no passphrase, in a fresh 0700 directory."""
    directory = Path(tempfile.mkdtemp(prefix="clio-relay-tapis-key-"))
    path = directory / "id"
    argv = [
        "ssh-keygen", "-q", "-t", "rsa", "-b", str(KEY_BITS), "-m", "PEM",
        "-N", "", "-C", comment, "-f", str(path),
    ]  # fmt: skip
    done = subprocess.run(argv, capture_output=True, text=True, check=False)
    if done.returncode != 0:
        shutil.rmtree(directory, ignore_errors=True)
        raise Refusal(
            RefusalReason.ADAPTER_UNAVAILABLE,
            f"ssh-keygen could not make relay's key: {done.stderr.strip()}",
        )
    return KeyPair(
        directory=directory,
        private_key=path.read_text(encoding="utf-8"),
        public_key=path.with_suffix(".pub").read_text(encoding="utf-8").strip(),
    )


def slurm_minutes(limit: str) -> int | None:
    """A Slurm time limit (``2-00:00:00``, ``1:00:00``, ``30:00``) in whole minutes.

    ``infinite``, ``UNLIMITED`` or anything unreadable is None: no limit is recorded.
    """
    text = limit.strip()
    days = 0
    if "-" in text:
        head, _, text = text.partition("-")
        if not head.isdigit():
            return None
        days = int(head)
    parts = text.split(":") if text else []
    if not parts or not all(part.isdigit() for part in parts) or len(parts) > 3:
        return None
    numbers = [int(part) for part in parts]
    if len(parts) == 3:
        hours, minutes, seconds = numbers
    elif len(parts) == 2:
        hours, (minutes, seconds) = 0, numbers
    else:
        hours, minutes, seconds = 0, numbers[0], 0
    total = days * 1440 + hours * 60 + minutes + (1 if seconds else 0)
    return total if total > 0 else None


def ssh_for_site(site: SiteEntry) -> SshHost | None:
    """The site's ``ssh`` way in, whether it is the adapter in use or an alternative."""
    config = site.config if site.adapter == "ssh" else site.alternatives.get("ssh")
    return SshHost.from_alternative(config) if config else None


@dataclass
class Registration:
    """What a registration did, for the ledger and the compute tab. No secrets."""

    system_id: str
    created_system: bool
    authn: str
    login_user: str | None = None
    host: str | None = None
    queues: list[str] = field(default_factory=list[str])
    key_installed: bool = False
    private_key_deleted: bool = False
    tapis_message: str | None = None


def system_definition(
    system_id: str,
    target: SshTarget,
    *,
    home: str,
    queues: list[str],
    authn: str,
    queue_limits: dict[str, int | None] | None = None,
) -> dict[str, Any]:
    """The body of ``systems.createSystem`` for a person's host.

    A partition's Slurm time limit becomes its queue's ``maxMinutes``, so a job
    with no walltime asks for that limit and not for more than Slurm allows.
    """
    body: dict[str, Any] = {
        "id": system_id,
        "description": "Registered by clio-relay for its own jobs",
        "systemType": "LINUX",
        "host": target.host,
        "port": target.port,
        "effectiveUserId": "${apiUserId}",
        "defaultAuthnMethod": "TMS_KEYS" if authn == "tms" else "PKI_KEYS",
        "rootDir": "/",
        "canExec": True,
        # Lets a batch job exec the launcher, so a cancel's TERM reaches it.
        "enableCmdPrefix": True,
        "jobRuntimes": [{"runtimeType": "ZIP"}],
        "jobWorkingDir": f"{home.rstrip('/')}/{WORK_DIR_NAME}",
        "tags": ["clio-relay"],
    }
    if queues:
        body["canRunBatch"] = True
        body["batchScheduler"] = "SLURM"
        body["batchDefaultLogicalQueue"] = queues[0]
        body["batchLogicalQueues"] = [
            {"name": name, "hpcQueueName": name, "maxJobs": -1, "maxJobsPerUser": -1}
            | ({"maxMinutes": limit} if (limit := (queue_limits or {}).get(name)) else {})
            for name in queues
        ]
    return body


async def register_host(
    adapter: TapisAdapter,
    ssh: SshHost | None,
    *,
    home: str | None = None,
    queues: list[str] | None = None,
    address: str | None = None,
) -> Registration:
    """Register the site's system and the person's credential on it.

    ``address`` is the site's host. A TACC host with no ``system_id`` set is
    refused with the Tapis systems on that host the person can see, so the
    person can pick one.
    """
    username = await adapter.username()
    system_id = await adapter.system_id()
    authn = adapter.config.authn
    if authn == "pki" and ssh is None:
        raise Refusal(
            RefusalReason.INVALID_CONFIG,
            "a person's own host is registered through the site's ssh alternative",
        )
    target = await asyncio.to_thread(ssh.target) if ssh is not None else None
    created = False
    existing = await _get_system(adapter, system_id)
    if existing is None:
        if target is None:
            raise await _no_system(adapter, system_id, address)
        assert ssh is not None
        home = home or await asyncio.to_thread(ssh.home)
        limits: dict[str, int | None] = {}
        if queues is None:
            limits = await asyncio.to_thread(ssh.partition_limits)
            queues = list(limits)
        body = system_definition(
            system_id, target, home=home, queues=queues, authn=authn, queue_limits=limits
        )
        try:
            await adapter.tapis_call("systems", "createSystem", skipCredentialCheck=True, **body)
        except TapisCallError as exc:
            raise exc.refusal(RefusalReason.SUBMIT_FAILED, system=system_id) from None
        created = True
        existing = body
    adapter.forget_system()
    result = Registration(
        system_id=system_id,
        created_system=created,
        authn=authn,
        host=str(existing.get("host") or "") or None,
        queues=[
            str(item.get("name"))
            for item in cast(list[dict[str, Any]], existing.get("batchLogicalQueues") or [])
        ],
        login_user=target.user if target is not None else None,
    )
    if authn == "tms":
        result.tapis_message = await _credential(
            adapter, system_id, username, {"loginUser": target.user} if target else {}, tms=True
        )
        return result
    assert ssh is not None and target is not None
    comment = key_comment(system_id)
    pair = await asyncio.to_thread(make_key_pair, comment)
    try:
        await asyncio.to_thread(ssh.add_key, pair.public_key, comment)
        result.key_installed = True
        body = {
            "loginUser": target.user,
            "privateKey": pair.private_key,
            "publicKey": pair.public_key,
        }
        result.tapis_message = await _credential(adapter, system_id, username, body, tms=False)
    except Refusal:
        if result.key_installed:
            await asyncio.to_thread(ssh.remove_key, comment)
            result.key_installed = False
        raise
    finally:
        pair.destroy()
        result.private_key_deleted = pair.gone
    return result


async def _get_system(adapter: TapisAdapter, system_id: str) -> dict[str, Any] | None:
    try:
        return await adapter.tapis_call("systems", "getSystem", systemId=system_id)
    except TapisCallError as exc:
        if exc.not_found:
            return None
        raise exc.refusal(system=system_id) from None


async def systems_on_host(adapter: TapisAdapter, host: str) -> list[str]:
    """The ids of the Tapis systems on ``host`` that the person can see."""
    try:
        found = await adapter.tapis_call(
            "systems",
            "getSystems",
            search=f"(host.eq={host})",
            listType="ALL",
            select="id,host",
            limit=100,
        )
    except TapisCallError as exc:
        raise exc.refusal(host=host) from None
    items = cast(list[dict[str, Any]], found or [])
    return sorted(str(item["id"]) for item in items if item.get("id"))


async def _no_system(adapter: TapisAdapter, system_id: str, address: str | None) -> Refusal:
    """The refusal for a site whose system is missing and cannot be made from here."""
    if adapter.config.authn != "tms" or not address:
        return Refusal(
            RefusalReason.INVALID_CONFIG,
            f"Tapis has no system {system_id}, and the site has no ssh alternative "
            "to register one from",
            system=system_id,
        )
    candidates = await systems_on_host(adapter, address)
    shown = f": {', '.join(candidates)}" if candidates else "; Tapis shows this person none"
    return Refusal(
        RefusalReason.INVALID_CONFIG,
        f"Tapis has no system {system_id}. For {address}, set system_id in the site's "
        f"config to a Tapis system on that host{shown}",
        system=system_id,
        host=address,
        candidates=candidates,
    )


async def _credential(
    adapter: TapisAdapter, system_id: str, username: str, body: dict[str, Any], *, tms: bool
) -> str:
    try:
        await adapter.tapis_call(
            "systems",
            "createUserCredential",
            systemId=system_id,
            userName=username,
            createTmsKeys=tms,
            **body,
        )
    except TapisCallError as exc:
        # Tapis checks the login from TACC. Its reason is what the person sees.
        raise Refusal(
            RefusalReason.SUBMIT_FAILED,
            f"Tapis did not accept the credential for {system_id}: {exc.message}",
            system=system_id,
            status=exc.status,
            tapis_message=exc.message,
        ) from None
    return "credential registered"


async def unregister_host(adapter: TapisAdapter, ssh: SshHost | None) -> dict[str, Any]:
    """Remove the credential, the system relay made, and the ``authorized_keys`` line."""
    username = await adapter.username()
    system_id = await adapter.system_id()
    removed: dict[str, Any] = {"system_id": system_id}
    try:
        await adapter.tapis_call(
            "systems", "removeUserCredential", systemId=system_id, userName=username
        )
        removed["credential"] = True
    except TapisCallError as exc:
        if not exc.not_found:
            raise exc.refusal(system=system_id) from None
        removed["credential"] = False
    if adapter.config.system_id is None:
        try:
            await adapter.tapis_call("systems", "deleteSystem", systemId=system_id)
            removed["system"] = True
        except TapisCallError as exc:
            if not exc.not_found:
                raise exc.refusal(system=system_id) from None
            removed["system"] = False
    if ssh is not None and adapter.config.authn == "pki":
        await asyncio.to_thread(ssh.remove_key, key_comment(system_id))
        removed["key_line"] = not await asyncio.to_thread(ssh.has_key, key_comment(system_id))
    adapter.forget_system()
    return removed
