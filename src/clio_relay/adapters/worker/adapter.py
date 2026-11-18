"""The ``worker`` adapter (RL11): relay's own worker, placed on a site once.

The person signs in to the site once. In that one ssh pass relay installs
itself in a private directory on the site, writes the worker's identity and
starts ``clio-relay worker site-serve`` detached. That process serves the
owned-session API on loopback and holds an frpc ``stcp`` proxy to the
rendezvous server. Relay then reaches it over the one held link
(``brokered_tcp``), and nothing uses SSH after that.

On the site the verbs run through the ``local`` adapter, so scheduler coverage
is psij's (RL18). Every verb crosses the link as one POST to the worker's
``/adapter`` routes; a refusal on the site comes back as the same typed refusal
here.

What relay keeps between restarts is ``worker-link.json`` in the adapter's
state directory (0600): ids, the API token, the stcp secret and the port. A
restarted relay reconnects from it over frp, again without SSH.
"""

from __future__ import annotations

import asyncio
import atexit
import base64
import contextlib
import hashlib
import importlib
import json
import os
import re
import secrets
import shlex
import shutil
import threading
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from clio_relay.adapters.base import BaseAdapter, SignInNeed, SiteInfo, Verb
from clio_relay.adapters.local import SchedulerChoice
from clio_relay.jobs.handles import (
    Endpoint,
    ExecResult,
    FileAccess,
    FileEntry,
    JobHandle,
    JobStatus,
    StallReport,
)
from clio_relay.jobs.records import new_id
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, ServiceSpec

WORKER_LINK_FILE = "worker-link.json"
ADAPTER_ROUTE_PREFIX = "/adapter"
READY_MARKER = "CLIO_RELAY_WORKER_READY "
# The worker's API answers at most 8 MiB per response; base64 adds a third.
_READ_CHUNK_BYTES = 4 * 1024 * 1024
_MAX_EXEC_OUTPUT_BYTES = 2 * 1024 * 1024
_CONNECT_ATTEMPTS = 20
_CALL_S = 120.0
# How long one exec poll waits on the site for output news before answering.
_EXEC_POLL_WAIT_S = 20.0
_REMOTE_DIR_PATTERN = re.compile(r"^(~/)?[A-Za-z0-9_./-]+$")


class Rendezvous(BaseModel):
    """The frps server the worker dials out to, and relay dials too."""

    model_config = ConfigDict(extra="forbid")

    address: str = Field(min_length=1, description="frps address as relay reaches it")
    port: int = Field(ge=1, le=65_535, description="frps port as relay reaches it")
    protocol: Literal["tcp", "websocket", "wss"] = "wss"
    site_address: str | None = Field(
        default=None, description="frps address as the site reaches it, when it differs"
    )
    site_port: int | None = Field(default=None, ge=1, le=65_535)
    token_env: str | None = Field(
        default=None, description="Environment variable holding the frps token"
    )
    token_file: str | None = Field(default=None, description="File holding the frps token")


class WorkerConfig(BaseModel):
    """Settings for the ``worker`` adapter."""

    model_config = ConfigDict(extra="forbid")

    ssh_profile: str = Field(
        min_length=1, description="The Host in ~/.ssh/config that signs in to the site"
    )
    ssh_config_file: str | None = Field(
        default=None, description="An ssh config file to use instead of ~/.ssh/config"
    )
    scheduler: SchedulerChoice = Field(
        default="auto",
        description="auto picks the scheduler whose submit command is on the site's PATH",
    )
    work_dir: str | None = Field(
        default=None, description="Where job directories go on the site"
    )
    rendezvous: Rendezvous | None = Field(
        default=None,
        description="Defaults to relay's CLIO_RELAY_FRPS_ADDR and CLIO_RELAY_FRP_TOKEN",
    )
    remote_dir: str | None = Field(
        default=None,
        description="The worker's directory on the site. Defaults to "
        "~/.local/share/clio-relay/worker/<site>",
    )
    relay_wheel: str | None = Field(
        default=None, description="A local clio-relay wheel to install on the site"
    )
    relay_requirement: str | None = Field(
        default=None,
        description="What uv installs on the site instead, such as clio-relay==1.7.0",
    )
    site_frpc: str | None = Field(
        default=None,
        description="frpc on the site. When unset relay uploads its own frpc",
    )
    python: str = Field(default="3.12", pattern=r"^3\.\d+$")
    submit_timeout_s: float = Field(default=60, gt=0)

    @field_validator("remote_dir")
    @classmethod
    def _plain_remote_dir(cls, value: str | None) -> str | None:
        if value is not None and _REMOTE_DIR_PATTERN.fullmatch(value) is None:
            raise ValueError("remote_dir must be a plain path, optionally starting with ~/")
        return value

    @field_validator("ssh_profile")
    @classmethod
    def _plain_profile(cls, value: str) -> str:
        if value.startswith("-") or any(ch.isspace() for ch in value):
            raise ValueError("ssh_profile must be one ssh_config Host")
        return value


class WorkerLink(BaseModel):
    """What relay keeps to reach a placed worker without SSH."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    session_id: str
    generation_id: str
    api_token: str
    stcp_secret: str
    proxy_name: str
    port: int
    remote_dir: str


# --- the askpass seam (W6) -------------------------------------------------


def _askpass() -> Any | None:
    try:
        return importlib.import_module("clio_relay.asks.askpass")
    except ModuleNotFoundError as exc:
        if exc.name not in ("clio_relay.asks", "clio_relay.asks.askpass"):
            raise
        return None


def ssh_sign_in_environment(
    site_id: str, *, operation_id: str | None, title: str
) -> dict[str, str]:
    """Return the askpass environment for the sign-in ssh, or nothing without asks."""
    module = _askpass()
    if module is None:
        return {}
    env: Mapping[str, str] = module.environment(
        site=site_id, operation_id=operation_id, title=title
    )
    return dict(env)


def register_ssh_sign_in(
    site_id: str, starter: Callable[[Mapping[str, str]], Awaitable[None]]
) -> bool:
    """Offer the sign-in to relay's sign-in page. Returns whether asks is there."""
    module = _askpass()
    if module is None:
        return False
    module.register_sign_in(site_id, starter)
    return True


# --- held connections, shared by every adapter instance in this process ------

_connections: dict[str, Any] = {}
_connections_lock = threading.Lock()


def _close_all_connections() -> None:
    with _connections_lock:
        held = list(_connections.values())
        _connections.clear()
    for connection in held:
        try:
            connection.close(at_exit=True)
        except Exception:  # noqa: BLE001, S112 - best effort at exit
            continue


atexit.register(_close_all_connections)


def _env_key(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12].upper()


class WorkerAdapter(BaseAdapter[WorkerConfig]):
    """A site reached through relay's own placed worker."""

    name: ClassVar[str] = "worker"
    Config: ClassVar[type[BaseModel]] = WorkerConfig
    implements: ClassVar[frozenset[Verb]] = frozenset(
        {"describe", "run", "exec", "files", "serve"}
    )

    # --- link state -----------------------------------------------------

    @property
    def link_path(self) -> Path:
        return self.context.state_dir / WORKER_LINK_FILE

    def load_link(self) -> WorkerLink | None:
        """Return the stored link, or ``None`` before the first sign-in."""
        try:
            raw = self.link_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        try:
            return WorkerLink.model_validate_json(raw)
        except ValidationError:
            return None

    def _store_link(self, link: WorkerLink) -> None:
        self.context.state_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.link_path.with_name(f".{WORKER_LINK_FILE}.tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(link.model_dump_json())
        os.replace(tmp, self.link_path)

    def _namespace(self) -> str:
        return hashlib.sha256(str(self.context.state_dir.resolve()).encode()).hexdigest()[:24]

    def _site_key(self) -> str:
        return _env_key(self.context.site_id).lower()

    def _remote_dir(self) -> str:
        return self.config.remote_dir or f"~/.local/share/clio-relay/worker/{self._site_key()}"

    def _rendezvous(self) -> tuple[Rendezvous, str]:
        rendezvous = self.config.rendezvous
        if rendezvous is None:
            address = os.environ.get("CLIO_RELAY_FRPS_ADDR", "")
            host, _, port = address.rpartition(":")
            if not host or not port.isdigit():
                raise Refusal(
                    RefusalReason.INVALID_CONFIG,
                    "the worker needs a rendezvous: set it in the site's config, or set "
                    "CLIO_RELAY_FRPS_ADDR to host:port",
                )
            rendezvous = Rendezvous(address=host, port=int(port))
        token: str | None = None
        if rendezvous.token_file:
            token = Path(rendezvous.token_file).expanduser().read_text(encoding="utf-8").strip()
        elif rendezvous.token_env:
            token = os.environ.get(rendezvous.token_env)
        else:
            token = os.environ.get("CLIO_RELAY_FRP_TOKEN")
        if not token:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                "the rendezvous token is not set (token_file, token_env or CLIO_RELAY_FRP_TOKEN)",
            )
        return rendezvous, token

    def _definition(self, link: WorkerLink, *, site_view: bool) -> Any:
        from clio_relay.cluster_config import ClusterDefinition, FrpTransportConfig

        rendezvous, _ = self._rendezvous()
        address = rendezvous.address
        port = rendezvous.port
        if site_view:
            address = rendezvous.site_address or address
            port = rendezvous.site_port or port
            token_env, secret_env = "CLIO_RELAY_FRP_TOKEN", "CLIO_RELAY_STCP_SECRET"
        else:
            key = _env_key(link.proxy_name)
            token_env = f"CLIO_RELAY_WORKER_{key}_FRP_TOKEN"
            secret_env = f"CLIO_RELAY_WORKER_{key}_STCP_SECRET"
        return ClusterDefinition(
            name=link.cluster,
            ssh_host=self.config.ssh_profile,
            frp_transport=FrpTransportConfig(
                protocol=rendezvous.protocol,
                server_addr=address,
                server_port=port,
                token_env=token_env,
                stcp_secret_env=secret_env,
                proxy_name=link.proxy_name,
                identity_anchor="preshared_link_secret",
            ),
        )

    # --- placement: the one ssh pass ------------------------------------

    def _payload(self, label: str, target: str, data: bytes, mode: str) -> str:
        encoded = base64.b64encode(data).decode("ascii")
        lines = [encoded[i : i + 76] for i in range(0, len(encoded), 76)]
        return (
            f'base64 -d > "{target}.new" <<\'CLIO_RELAY_{label}\'\n'
            + "\n".join(lines)
            + f'\nCLIO_RELAY_{label}\nchmod {mode} "{target}.new"\nmv -f "{target}.new" "{target}"\n'
        )

    def _text_file(self, label: str, target: str, text: str) -> str:
        return self._payload(label, target, text.encode("utf-8"), "600")

    def placement_script(self, link: WorkerLink, *, session_owner_token: str) -> str:
        """Return the bash script the one ssh pass runs on the site."""
        from clio_relay.cluster_config import ClusterRegistry
        from clio_relay.config import RelaySettings

        _, token = self._rendezvous()
        remote_dir = self._remote_dir()
        shell_dir = '"$HOME"/' + remote_dir[2:] if remote_dir.startswith("~/") else remote_dir
        parts = [
            "set -euo pipefail",
            "umask 077",
            f"D={shell_dir}",
            'mkdir -p "$D/run" "$D/bin" "$D/dist"',
            'D="$(cd "$D" && pwd -P)"',
            # Stop a worker this directory already ran, and its proxy.
            'if [ -f "$D/run/site.pid" ]; then',
            '  p="$(cat "$D/run/site.pid")"',
            '  if [ -r "/proc/$p/cmdline" ] && tr "\\0" " " < "/proc/$p/cmdline" | grep -q site-serve; then',
            '    kill "$p" 2>/dev/null || true',
            "    n=0",
            '    while kill -0 "$p" 2>/dev/null && [ "$n" -lt 100 ]; do sleep 0.2; n=$((n+1)); done',
            '    kill -9 "$p" 2>/dev/null || true',
            "  fi",
            "fi",
            'pkill -f -- "$D/run/frpc.toml" 2>/dev/null || true',
            'rm -f "$D/run/ready.json" "$D/run/site.pid"',
            'UV="$(command -v uv || true)"',
            '[ -n "$UV" ] || UV="$HOME/.local/bin/uv"',
            'if [ ! -x "$UV" ]; then',
            "  curl -LsSf https://astral.sh/uv/install.sh | env UV_NO_MODIFY_PATH=1 sh >&2",
            "fi",
        ]
        if self.config.relay_wheel:
            wheel = Path(self.config.relay_wheel).expanduser()
            name = wheel.name
            if not re.fullmatch(r"[A-Za-z0-9_.+-]+\.whl", name):
                raise Refusal(RefusalReason.INVALID_CONFIG, "relay_wheel must be a .whl file")
            parts.append(self._payload("WHEEL", f"$D/dist/{name}", wheel.read_bytes(), "600"))
            install = f'"$D/dist/{name}"'
        else:
            requirement = self.config.relay_requirement
            if requirement is None:
                from importlib.metadata import version

                requirement = f"clio-relay=={version('clio-relay')}"
            install = shlex.quote(requirement)
        parts.append(
            'UV_TOOL_DIR="$D/tools" UV_TOOL_BIN_DIR="$D/bin" "$UV" tool install --force '
            f"--python {self.config.python} {install} >&2"
        )
        if self.config.site_frpc:
            frpc = self.config.site_frpc
        else:
            local_frpc = shutil.which(RelaySettings.from_env().frpc_bin)
            if local_frpc is None:
                raise Refusal(
                    RefusalReason.INVALID_CONFIG,
                    "relay has no frpc to upload; set site_frpc or CLIO_RELAY_FRPC_BIN",
                )
            parts.append(self._payload("FRPC", "$D/bin/frpc", Path(local_frpc).read_bytes(), "700"))
            frpc = "$D/bin/frpc"
        registry = ClusterRegistry(
            clusters={link.cluster: self._definition(link, site_view=True)}
        )
        settings = {
            "cluster": link.cluster,
            "session_id": link.session_id,
            "generation_id": link.generation_id,
            "api_token": link.api_token,
            "session_owner_token": session_owner_token,
            "frpc": "__FRPC__",
        }
        parts += [
            self._text_file("REGISTRY", "$D/run/registry.json", registry.model_dump_json()),
            f'FRPC_PATH="{frpc}"',
            self._text_file("SETTINGS", "$D/run/settings.tmpl", json.dumps(settings)),
            'sed "s#__FRPC__#$FRPC_PATH#" "$D/run/settings.tmpl" > "$D/run/settings.json"',
            'rm -f "$D/run/settings.tmpl"',
            self._text_file(
                "FRPENV",
                "$D/run/frp.env",
                f"CLIO_RELAY_FRP_TOKEN={token}\nCLIO_RELAY_STCP_SECRET={link.stcp_secret}\n",
            ),
            'setsid nohup "$D/bin/clio-relay" worker site-serve --state-dir "$D" '
            '> "$D/run/site.log" 2>&1 < /dev/null &',
            "pid=$!",
            'while [ ! -s "$D/run/ready.json" ]; do',
            '  if ! kill -0 "$pid" 2>/dev/null; then tail -n 40 "$D/run/site.log" >&2; exit 3; fi',
            "  sleep 0.2",
            "done",
            f'printf "{READY_MARKER}%s %s\\n" "$D" "$(cat "$D/run/ready.json")"',
        ]
        return "\n".join(parts) + "\n"

    def ssh_argv(self) -> list[str]:
        """Return the sign-in ssh command. It asks through askpass, never a terminal."""
        argv = ["ssh", "-T", "-o", "BatchMode=no"]
        if self.config.ssh_config_file:
            argv += ["-F", str(Path(self.config.ssh_config_file).expanduser())]
        return [*argv, self.config.ssh_profile, "bash", "-s"]

    async def connect(self, env: Mapping[str, str] | None = None) -> WorkerLink:
        """Place the worker in one ssh pass, then hold the link to it over frp.

        ``env`` is the askpass environment the sign-in page hands over. The
        script goes on stdin, so the ssh's stdin carries it and nothing else;
        the prompts go to askpass, never to a terminal.
        """
        link = WorkerLink(
            cluster=f"worker-{self._site_key()}",
            session_id=new_id("session"),
            generation_id=new_id("generation"),
            api_token=secrets.token_urlsafe(32),
            stcp_secret=secrets.token_urlsafe(32),
            proxy_name=f"clio-worker-{secrets.token_hex(8)}",
            port=0,
            remote_dir=self._remote_dir(),
        )
        script = self.placement_script(link, session_owner_token=secrets.token_urlsafe(48))
        self._drop_connection()
        # No time limit: the person may be typing a code on the sign-in page.
        process = await asyncio.create_subprocess_exec(
            *self.ssh_argv(),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, **(env or {})},
            start_new_session=os.name == "posix",
        )
        out, err = await process.communicate(script.encode("utf-8"))
        stdout = out.decode("utf-8", "replace")
        ready = next(
            (line for line in stdout.splitlines() if line.startswith(READY_MARKER)), None
        )
        if process.returncode != 0 or ready is None:
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"placing the worker on {self.config.ssh_profile} failed "
                f"(exit {process.returncode}): {err.decode('utf-8', 'replace')[-2000:]}",
                site=self.context.site_id,
            )
        remote_dir, _, document = ready[len(READY_MARKER) :].partition(" ")
        report = json.loads(document)
        link = link.model_copy(update={"port": int(report["port"]), "remote_dir": remote_dir})
        self._store_link(link)
        await self._connection(attempts=_CONNECT_ATTEMPTS)
        return link

    # --- the held link ----------------------------------------------------

    @staticmethod
    def _connection_key(link: WorkerLink) -> str:
        return f"{link.cluster}/{link.generation_id}"

    def _drop_connection(self) -> None:
        link = self.load_link()
        if link is None:
            return
        with _connections_lock:
            held = _connections.pop(self._connection_key(link), None)
        if held is not None:
            held.close()

    async def _connection(self, *, attempts: int = 1) -> Any:
        from clio_relay.adapters.worker.remote_connection import RemoteConnection
        from clio_relay.config import RelaySettings

        link = self.load_link()
        if link is None:
            raise Refusal(
                RefusalReason.NEEDS_SIGN_IN,
                f"sign in to {self.config.ssh_profile} so relay can place its worker",
                site=self.context.site_id,
                provider="ssh",
            )
        key = self._connection_key(link)
        with _connections_lock:
            held = _connections.get(key)
        if held is not None and held.connected:
            return held
        if held is not None:
            self._drop_connection()
        _, token = self._rendezvous()
        local = self._definition(link, site_view=False)
        os.environ[local.frp_transport.token_env] = token
        os.environ[local.frp_transport.stcp_secret_env] = link.stcp_secret
        settings = RelaySettings.from_env().model_copy(
            update={
                "owner_session_id": link.session_id,
                "owner_session_generation_id": link.generation_id,
                "owner_session_cluster": link.cluster,
                "owner_session_api_port": link.port,
                "api_token": link.api_token,
                "remote_cluster": None,
                "remote_transport_mode": "brokered_tcp",
                "remote_transport_interactive": False,
            }
        )
        last: Exception | None = None
        for attempt in range(attempts):
            connection = RemoteConnection(
                definition=local,
                settings=settings,
                remote_api_port=link.port,
                transport_mode="brokered_tcp",
                allow_interactive_authorization=False,
            )
            try:
                await asyncio.to_thread(connection.connect)
            except Exception as exc:  # noqa: BLE001 - every failure is the same refusal
                last = exc
                await asyncio.to_thread(connection.close)
                if attempt + 1 < attempts:
                    await asyncio.sleep(1.0)
                continue
            with _connections_lock:
                _connections[key] = connection
            return connection
        raise Refusal(
            RefusalReason.UNREACHABLE,
            f"the worker on {self.config.ssh_profile} did not answer over the rendezvous: {last}",
            site=self.context.site_id,
        )

    def _site_config(self) -> dict[str, Any]:
        config: dict[str, Any] = {
            "scheduler": self.config.scheduler,
            "submit_timeout_s": self.config.submit_timeout_s,
        }
        if self.config.work_dir is not None:
            config["work_dir"] = self.config.work_dir
        return config

    async def call(self, path: str, body: dict[str, Any], *, timeout_s: float) -> Any:
        """Call one of the worker's adapter routes over the held link."""
        from clio_relay.control_channel import ChannelDropped

        connection = await self._connection()
        payload: dict[str, object] = {
            "config": self._site_config(),
            "namespace": self._namespace(),
            **body,
        }
        try:
            document = await asyncio.to_thread(
                connection.request_json,
                method="POST",
                path=f"{ADAPTER_ROUTE_PREFIX}{path}",
                body=payload,
                response_timeout_seconds=timeout_s,
            )
        except ChannelDropped as exc:
            self._drop_connection()
            raise Refusal(
                RefusalReason.UNREACHABLE, f"the link to the worker dropped: {exc}"
            ) from exc
        except Exception as exc:
            self._drop_connection()
            raise Refusal(
                RefusalReason.UNREACHABLE, f"the worker did not answer {path}: {exc}"
            ) from exc
        if not isinstance(document, dict):
            raise Refusal(RefusalReason.UNREACHABLE, f"the worker answered {path} with no object")
        answer: dict[str, Any] = document  # pyright: ignore[reportUnknownVariableType]
        refusal = answer.get("refusal")
        if isinstance(refusal, dict):
            refused: dict[str, Any] = refusal  # pyright: ignore[reportUnknownVariableType]
            try:
                reason = RefusalReason(refused.get("reason"))
            except ValueError:
                reason = RefusalReason.SUBMIT_FAILED
            data = refused.get("data")
            raise Refusal(
                reason, str(refused.get("message", "")), **(data if isinstance(data, dict) else {})
            )
        return answer.get("ok")

    # --- verbs --------------------------------------------------------------

    async def describe(self) -> SiteInfo:
        reported = await self.call("/describe", {}, timeout_s=120)
        info = SiteInfo.model_validate(reported)
        facts = dict(info.facts)
        facts["ssh_profile"] = self.config.ssh_profile
        return info.model_copy(
            update={
                "adapter": self.name,
                "address": self.config.ssh_profile,
                "facts": facts,
                "verbs": sorted(self.implements),
            }
        )

    def _wrap(self, remote: JobHandle) -> JobHandle:
        return JobHandle(
            adapter=self.name,
            native_id=remote.native_id,
            data={"remote": remote.model_dump(mode="json")},
        )

    def _unwrap(self, handle: JobHandle) -> dict[str, Any]:
        remote = handle.data.get("remote")
        if handle.adapter != self.name or not isinstance(remote, dict):
            raise Refusal(
                RefusalReason.INVALID_ARGUMENTS, "the handle was not made by the worker adapter"
            )
        return remote  # pyright: ignore[reportUnknownVariableType]

    async def run(self, spec: JobSpec, *, operation_id: str) -> JobHandle:
        for item in spec.inputs:
            if not await self.call("/files/has", {"digest": item.sha256}, timeout_s=120):
                if not self.context.content.has(item.sha256):
                    raise Refusal(
                        RefusalReason.INPUT_MISSING,
                        f"input {item.name} ({item.sha256}) is not staged",
                        name=item.name,
                        sha256=item.sha256,
                    )
                data = self.context.content.get(item.sha256)
                await self.call(
                    "/files/stage",
                    {"data": base64.b64encode(data).decode("ascii")},
                    timeout_s=600,
                )
        reported = await self.call(
            "/run",
            {"spec": spec.model_dump(mode="json"), "operation_id": operation_id},
            timeout_s=self.config.submit_timeout_s + 120,
        )
        return self._wrap(JobHandle.model_validate(reported))

    async def status(self, handle: JobHandle) -> JobStatus:
        reported = await self.call("/status", {"handle": self._unwrap(handle)}, timeout_s=120)
        return JobStatus.model_validate(reported)

    async def cancel(self, handle: JobHandle) -> JobStatus:
        reported = await self.call(
            "/cancel",
            {"handle": self._unwrap(handle)},
            timeout_s=self.config.submit_timeout_s + 120,
        )
        return JobStatus.model_validate(reported)

    async def find(self, operation_id: str) -> JobHandle | None:
        if self.load_link() is None:
            return None
        reported = await self.call("/find", {"operation_id": operation_id}, timeout_s=120)
        return None if reported is None else self._wrap(JobHandle.model_validate(reported))

    async def exec(
        self,
        command: list[str],
        *,
        timeout_s: float | None = None,
        max_output_bytes: int,
        on_stall: StallReport | None = None,
    ) -> ExecResult:
        started = await self.call(
            "/exec/start",
            {
                "command": command,
                "timeout_s": timeout_s,
                "max_output_bytes": min(max_output_bytes, _MAX_EXEC_OUTPUT_BYTES),
            },
            timeout_s=_CALL_S,
        )
        exec_id = str(started["exec_id"])
        seen = 0
        try:
            while True:
                reply = await self.call(
                    "/exec/poll",
                    {"exec_id": exec_id, "after": seen, "wait_s": _EXEC_POLL_WAIT_S},
                    timeout_s=_EXEC_POLL_WAIT_S + _CALL_S,
                )
                stalls: list[list[float]] = reply.get("stalls") or []
                seen += len(stalls)
                if on_stall is not None:
                    for idle_s, next_s in stalls:
                        await on_stall(float(idle_s), float(next_s))
                if reply.get("done"):
                    return ExecResult.model_validate(reply["result"])
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await asyncio.shield(
                    self.call("/exec/cancel", {"exec_id": exec_id}, timeout_s=_CALL_S)
                )
            raise

    async def files(self) -> FileAccess:
        await self._connection()
        return WorkerFileAccess(self)

    async def hop_exec(
        self,
        command: list[str],
        *,
        timeout_s: float | None = None,
        max_output_bytes: int,
        on_stall: StallReport | None = None,
    ) -> ExecResult:
        """Run a command on the worker's host. The serve hook uses it for its hop."""
        return await self.exec(
            command, timeout_s=timeout_s, max_output_bytes=max_output_bytes, on_stall=on_stall
        )

    async def serve(self, spec: ServiceSpec, *, operation_id: str) -> Endpoint:
        try:
            runtime: Any = importlib.import_module("clio_relay.serve")
        except ModuleNotFoundError as exc:
            if exc.name not in ("clio_relay.serve", "clio_relay"):
                raise
            raise Refusal(
                RefusalReason.VERB_NOT_IMPLEMENTED,
                "serve needs the service runtime, and it is not installed",
                verb="serve",
            ) from exc
        endpoint: Endpoint = await runtime.start_for(self, spec, operation_id=operation_id)
        return endpoint

    # --- sign-in ----------------------------------------------------------

    async def sign_in_needed(self) -> SignInNeed | None:
        if self.load_link() is not None:
            try:
                await self._connection()
            except Refusal:
                pass
            else:
                return None

        async def starter(env: Mapping[str, str]) -> None:
            await self.connect(env)

        register_ssh_sign_in(self.context.site_id, starter)
        return SignInNeed(
            provider="ssh",
            message=f"Sign in to {self.config.ssh_profile} so relay can place its worker there",
            url=None,
        )

    async def sign_in_done(self, answer: dict[str, Any]) -> None:
        await self._connection()


class WorkerFileAccess:
    """File access on the site, over the worker's link."""

    def __init__(self, adapter: WorkerAdapter) -> None:
        self._adapter = adapter

    async def _call(self, op: str, body: dict[str, Any], timeout_s: float = 300) -> Any:
        return await self._adapter.call(f"/files/{op}", body, timeout_s=timeout_s)

    async def list(self, path: str) -> list[FileEntry]:
        reported = await self._call("list", {"path": path})
        return [FileEntry.model_validate(entry) for entry in reported]

    async def stat(self, path: str) -> FileEntry:
        return FileEntry.model_validate(await self._call("stat", {"path": path}))

    async def read(self, path: str, *, offset: int = 0, limit: int | None = None) -> bytes:
        chunks: list[bytes] = []
        position = max(0, offset)
        remaining = limit
        while remaining is None or remaining > 0:
            size = _READ_CHUNK_BYTES if remaining is None else min(remaining, _READ_CHUNK_BYTES)
            encoded = await self._call("read", {"path": path, "offset": position, "limit": size})
            chunk = base64.b64decode(encoded)
            chunks.append(chunk)
            position += len(chunk)
            if remaining is not None:
                remaining -= len(chunk)
            if len(chunk) < size:
                break
        return b"".join(chunks)

    async def write(self, path: str, data: bytes, *, overwrite: bool = False) -> FileEntry:
        reported = await self._call(
            "write",
            {
                "path": path,
                "data": base64.b64encode(data).decode("ascii"),
                "overwrite": overwrite,
            },
        )
        return FileEntry.model_validate(reported)

    async def stage(self, data: bytes) -> str:
        digest = await self._call("stage", {"data": base64.b64encode(data).decode("ascii")})
        return str(digest)

    async def has(self, digest: str) -> bool:
        return bool(await self._call("has", {"digest": digest}))
