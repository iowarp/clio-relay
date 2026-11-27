"""The ``worker`` adapter (RL11): relay's own worker, placed on a site once.

The person signs in to the site once. In that one ssh pass relay installs
itself in a private directory on the site, writes the worker's identity and
starts ``clio-relay worker site-serve`` detached. That process serves the
worker's API on loopback and holds an frpc ``stcp`` proxy to the
rendezvous server. It runs on the login node, so it starts light and exits
after ``idle_exit_s`` with no request from relay; the next sign-in places it
again (:mod:`clio_relay.login_load`). Relay then reaches it over the one held link
(``brokered_tcp``), and nothing uses SSH after that.

On the site the verbs run through the ``local`` adapter, so scheduler coverage
is psij's (RL18). Every verb crosses the link as one POST to the worker's
``/adapter`` routes; a refusal on the site comes back as the same typed refusal
here.

What relay keeps between restarts is ``worker-link.json`` in the adapter's
state directory (0600): ids, the API token, the session owner token, the stcp
secret and the port. A restarted relay reconnects from it over frp, again
without SSH. A worker an older relay placed is refused with a sign-in, which
places a current one (:mod:`clio_relay.adapters.worker.link`).
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
import posixpath
import re
import secrets
import shlex
import threading
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, cast

from pydantic import BaseModel, ValidationError

from clio_relay import jarvis_submit, programs
from clio_relay.adapters.base import BaseAdapter, Reach, SignInNeed, SiteInfo, Verb
from clio_relay.adapters.ssh import hosts as ssh_hosts
from clio_relay.adapters.worker.batch_ssh import batch_ssh
from clio_relay.adapters.worker.config import Rendezvous, WorkerConfig, WorkerLink
from clio_relay.adapters.worker.files import WorkerFileAccess
from clio_relay.adapters.worker.own_wheel import RelayInstall, relay_install
from clio_relay.jobs.handles import (
    Endpoint,
    ExecResult,
    FileAccess,
    JobHandle,
    JobStatus,
    StallReport,
)
from clio_relay.jobs.records import new_id
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, ServiceSpec
from clio_relay.login_load import light_function
from clio_relay.serve.config import Rendezvous as ServeRendezvous
from clio_relay.serve.config import ServeLink
from clio_relay.site_prune import (
    jobs_script,
    parse_live,
    parse_report,
    prune_script,
    site_keeps,
)

if TYPE_CHECKING:
    from clio_relay.adapters.worker.link import WorkerConnection

WORKER_LINK_FILE = "worker-link.json"
ADAPTER_ROUTE_PREFIX = "/adapter"
READY_MARKER = "CLIO_RELAY_WORKER_READY "
# The worker's API answers at most 8 MiB per response; base64 adds a third.
_MAX_EXEC_OUTPUT_BYTES = 2 * 1024 * 1024
_CONNECT_ATTEMPTS = 20
_CALL_S = 120.0
# How long one exec poll waits on the site for output news before answering.
_EXEC_POLL_WAIT_S = 20.0
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


async def _in_daemon_thread[T](function: Callable[[], T]) -> T:
    """Run ``function`` in a daemon thread; a cancelled caller leaves it to finish alone."""
    loop = asyncio.get_running_loop()
    future: asyncio.Future[T] = loop.create_future()

    def settle(result: T | None, error: BaseException | None) -> None:
        if future.done():
            return
        if error is not None:
            future.set_exception(error)
        else:
            future.set_result(cast(T, result))

    def run() -> None:
        try:
            result = function()
        except BaseException as exc:  # noqa: BLE001 - handed to the caller
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(settle, None, exc)
            return
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(settle, result, None)

    threading.Thread(target=run, name="clio-relay-worker", daemon=True).start()
    return await future


def _env_key(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12].upper()


class WorkerAdapter(BaseAdapter[WorkerConfig]):
    """A site reached through relay's own placed worker."""

    name: ClassVar[str] = "worker"
    Config: ClassVar[type[BaseModel]] = WorkerConfig
    implements: ClassVar[frozenset[Verb]] = frozenset({"describe", "run", "exec", "files", "serve"})
    # The site's local adapter hands a package job to jarvis ppl submit.
    runs_packages: ClassVar[bool] = True
    # and reads the port the package bound from JARVIS, so relay forwards that one.
    reads_service_runtimes: ClassVar[bool] = True

    @classmethod
    def reaches(cls, config: BaseModel) -> Reach | None:
        assert isinstance(config, WorkerConfig)
        host, user = ssh_hosts.destination(config.ssh_profile, config.ssh_config_file)
        return Reach(host, user)

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

    def _link_rendezvous(self) -> Rendezvous:
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
        return rendezvous

    def _rendezvous(self) -> tuple[Rendezvous, str]:
        rendezvous = self._link_rendezvous()
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
                stun_server=rendezvous.stun_server,
            ),
        )

    # --- placement: the one ssh pass ------------------------------------

    def _payload(self, label: str, target: str, data: bytes, mode: str) -> str:
        encoded = base64.b64encode(data).decode("ascii")
        lines = [encoded[i : i + 76] for i in range(0, len(encoded), 76)]
        return (
            f"base64 -d > \"{target}.new\" <<'CLIO_RELAY_{label}'\n"
            + "\n".join(lines)
            + f"\nCLIO_RELAY_{label}\n"
            + f'chmod {mode} "{target}.new"\nmv -f "{target}.new" "{target}"\n'
        )

    def _text_file(self, label: str, target: str, text: str) -> str:
        return self._payload(label, target, text.encode("utf-8"), "600")

    def _relay_install(self) -> RelayInstall:
        """Return the relay the site installs: the settings' own, else this computer's."""
        if self.config.relay_wheel:
            return RelayInstall(wheel=Path(self.config.relay_wheel).expanduser())
        if self.config.relay_requirement:
            return RelayInstall(requirement=self.config.relay_requirement)
        return relay_install(self.context.state_dir / "relay-wheel")

    def placement_script(self, link: WorkerLink, *, session_owner_token: str) -> str:
        """Return the bash script the one ssh pass runs on the site."""
        from clio_relay.cluster_config import ClusterRegistry

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
            '  if [ -r "/proc/$p/cmdline" ] \\',
            '    && tr "\\0" " " < "/proc/$p/cmdline" | grep -q site-serve; then',
            '    kill "$p" 2>/dev/null || true',
            "    n=0",
            '    while kill -0 "$p" 2>/dev/null && [ "$n" -lt 100 ]; do',
            "      sleep 0.2; n=$((n+1))",
            "    done",
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
        relay = self._relay_install()
        if relay.wheel is not None:
            name = relay.wheel.name
            if not re.fullmatch(r"[A-Za-z0-9_.+-]+\.whl", name):
                raise Refusal(RefusalReason.INVALID_CONFIG, "relay_wheel must be a .whl file")
            parts.append(self._payload("WHEEL", f"$D/dist/{name}", relay.wheel.read_bytes(), "600"))
            install = f'"$D/dist/{name}"'
        else:
            assert relay.requirement is not None
            install = shlex.quote(relay.requirement)
        parts.append(
            'UV_TOOL_DIR="$D/tools" UV_TOOL_BIN_DIR="$D/bin" "$UV" tool install --force '
            f"--python {self.config.python} {install} >&2"
        )
        if self.config.site_frpc:
            frpc = self.config.site_frpc
        elif self.config.frpc_binary:
            try:
                binary = Path(os.path.expanduser(self.config.frpc_binary)).read_bytes()
            except OSError as exc:
                raise Refusal(
                    RefusalReason.INVALID_CONFIG, f"cannot read frpc_binary: {exc.strerror}"
                ) from exc
            parts.append(self._payload("FRPC", "$D/bin/frpc", binary, "700"))
            frpc = "$D/bin/frpc"
        else:
            from clio_relay.adapters.worker.frpc_fetch import fetch_script

            if (self.config.frpc_url is None) != (self.config.frpc_sha256 is None):
                raise Refusal(
                    RefusalReason.INVALID_CONFIG,
                    "frpc_url and frpc_sha256 go together: the site checks the download "
                    "against the sha256",
                )
            parts += fetch_script(
                "$D/bin/frpc", url=self.config.frpc_url, sha256=self.config.frpc_sha256
            )
            frpc = "$D/bin/frpc"
        registry = ClusterRegistry(clusters={link.cluster: self._definition(link, site_view=True)})
        settings = {
            "cluster": link.cluster,
            "session_id": link.session_id,
            "generation_id": link.generation_id,
            "api_token": link.api_token,
            "session_owner_token": session_owner_token,
            "frpc": "__FRPC__",
            "link_mode": link.link_mode,
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
            light_function(self.config.login_limits).rstrip("\n"),
            # In a subshell, so that clio_light's ulimit fallback limits only the worker.
            '( clio_light setsid nohup "$D/bin/clio-relay" worker site-serve --state-dir "$D" '
            f"--idle-exit-s {self.config.idle_exit_s} ) "
            '> "$D/run/site.log" 2>&1 < /dev/null &',
            "pid=$!",
            "alive() {",
            '  kill -0 "$pid" 2>/dev/null \\',
            '    || { [ -s "$D/run/site.pid" ] \\',
            '      && kill -0 "$(cat "$D/run/site.pid")" 2>/dev/null; }',
            "}",
            'while [ ! -s "$D/run/ready.json" ]; do',
            '  if ! alive; then tail -n 40 "$D/run/site.log" >&2; exit 3; fi',
            "  sleep 0.2",
            "done",
            f'printf "{READY_MARKER}%s %s\\n" "$D" "$(cat "$D/run/ready.json")"',
        ]
        return "\n".join(parts) + "\n"

    def sign_in_argv(self) -> list[str]:
        """Return the sign-in ssh command. It asks through askpass, never a terminal."""
        argv = ["ssh", "-T", "-o", "BatchMode=no"]
        if self.config.ssh_config_file:
            argv += ["-F", str(Path(self.config.ssh_config_file).expanduser())]
        return [*argv, self.config.ssh_profile, "bash", "-s"]

    def ssh_argv(self) -> list[str]:
        """Return the base ``ssh`` argv (program, options, host) for ``serve``.

        It never prompts; ``serve`` adds its own flags (``-N -L``) before the host.
        """
        argv = ["ssh", "-o", "BatchMode=yes"]
        if self.config.ssh_config_file:
            argv += ["-F", str(Path(self.config.ssh_config_file).expanduser())]
        return [*argv, self.config.ssh_profile]

    def serve_link(self) -> ServeLink | None:
        """Return the worker link's rendezvous and site frpc, for a service here.

        A service on this site then meets the desktop through the same frps the
        worker's link uses, with the frpc the sign-in placed, and tries the
        direct path when the link does (``udp_rendezvous``). Before the first
        sign-in, or with no rendezvous for the link, there is none.
        """
        link = self.load_link()
        if link is None:
            return None
        try:
            rendezvous = self._link_rendezvous()
        except Refusal:
            return None
        token_env = rendezvous.token_env
        if rendezvous.token_file is None and token_env is None:
            token_env = "CLIO_RELAY_FRP_TOKEN"
        return ServeLink(
            rendezvous=ServeRendezvous(
                address=rendezvous.address,
                port=rendezvous.port,
                protocol=rendezvous.protocol,
                token_file=rendezvous.token_file,
                token_env=token_env,
                direct=link.link_mode == "udp_rendezvous",
                stun_server=rendezvous.stun_server,
            ),
            frpc=self.config.site_frpc or f"{link.remote_dir.rstrip('/')}/bin/frpc",
            address=rendezvous.site_address,
            port=rendezvous.site_port,
        )

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
            session_owner_token=secrets.token_urlsafe(48),
            link_mode=self.config.link_mode,
        )
        assert link.session_owner_token is not None
        # It may fetch relay's own wheel, so off the event loop.
        script = await asyncio.to_thread(
            self.placement_script, link, session_owner_token=link.session_owner_token
        )
        self._drop_connection()
        env = {**os.environ, **(env or {})}
        argv = programs.resolved(self.sign_in_argv(), env)
        if argv is None:
            raise Refusal(
                RefusalReason.UNREACHABLE,
                programs.missing("ssh"),
                site=self.context.site_id,
                command="ssh",
            )
        # No time limit: the person may be typing a code on the sign-in page.
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            start_new_session=os.name == "posix",
        )
        out, err = await process.communicate(script.encode("utf-8"))
        stdout = out.decode("utf-8", "replace")
        ready = next((line for line in stdout.splitlines() if line.startswith(READY_MARKER)), None)
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

    async def _connection(self, *, attempts: int = 1) -> WorkerConnection:
        from clio_relay.adapters.worker.link import WorkerOutdated
        from clio_relay.frp_transport import TransportPunchFailed

        link = self.load_link()
        if link is None:
            raise Refusal(
                RefusalReason.NEEDS_SIGN_IN,
                f"sign in to {self.config.ssh_profile} so relay can place its worker",
                site=self.context.site_id,
                provider="ssh",
            )
        if link.session_owner_token is None:
            raise self._outdated("its link was stored by an older relay")
        if link.link_mode != self.config.link_mode:
            raise Refusal(
                RefusalReason.NEEDS_SIGN_IN,
                f"the worker on {self.config.ssh_profile} was placed for {link.link_mode}, and "
                f"the site now asks for {self.config.link_mode}; sign in again so relay can "
                "place it for the new link",
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
        last: Exception | None = None
        for attempt in range(attempts):
            connection = self._new_connection(link)
            try:
                await asyncio.to_thread(connection.connect)
            except WorkerOutdated as exc:
                raise self._outdated(str(exc)) from exc
            except Exception as exc:  # noqa: BLE001 - every failure is the same refusal
                last = exc
                if attempt + 1 < attempts:
                    await asyncio.sleep(1.0)
                continue
            with _connections_lock:
                _connections[key] = connection
            return connection
        if isinstance(last, TransportPunchFailed):
            raise Refusal(
                RefusalReason.PUNCH_FAILED,
                f"relay could not hole punch to the worker on {self.config.ssh_profile} "
                f"({last}). Relay does not fall back to the rendezvous on its own; set the "
                "site's link_mode to brokered_tcp to carry calls through it",
                site=self.context.site_id,
                link_mode=link.link_mode,
            )
        idle = (
            f" It exits after {self.config.idle_exit_s} s with no request from relay."
            if self.config.idle_exit_s
            else ""
        )
        raise Refusal(
            RefusalReason.NEEDS_SIGN_IN,
            f"the worker on {self.config.ssh_profile} did not answer over the rendezvous "
            f"({last}).{idle} Sign in again so relay can place it",
            site=self.context.site_id,
            provider="ssh",
        )

    def _outdated(self, why: str) -> Refusal:
        return Refusal(
            RefusalReason.NEEDS_SIGN_IN,
            f"the worker on {self.config.ssh_profile} is from an older relay ({why}); sign in "
            "again so relay can replace it",
            site=self.context.site_id,
            provider="ssh",
        )

    def _new_connection(self, link: WorkerLink) -> WorkerConnection:
        """Return an unconnected link to the placed worker, its secrets in the environment."""
        from clio_relay.adapters.worker.link import WorkerConnection

        assert link.session_owner_token is not None
        _, token = self._rendezvous()
        local = self._definition(link, site_view=False)
        os.environ[local.frp_transport.token_env] = token
        os.environ[local.frp_transport.stcp_secret_env] = link.stcp_secret
        return WorkerConnection(
            definition=local,
            session_id=link.session_id,
            generation_id=link.generation_id,
            remote_api_port=link.port,
            api_token=link.api_token,
            session_owner_token=link.session_owner_token,
            mode=link.link_mode,
        )

    async def shutdown(self) -> list[dict[str, Any]]:
        """Stop the placed worker and relay's link to it. Jobs keep running.

        Bring-up and the request run in daemon threads, so that a caller's timeout
        never leaves a thread that holds the interpreter open.
        """
        link = self.load_link()
        if link is None or link.session_owner_token is None:
            return []
        with _connections_lock:
            held = _connections.pop(self._connection_key(link), None)
        connection = held if held is not None and held.connected else None
        if connection is None:
            if held is not None:
                held.close()
            connection = self._new_connection(link)
            try:
                await _in_daemon_thread(connection.connect)
            except Exception as exc:
                connection.close()
                raise Refusal(
                    RefusalReason.UNREACHABLE,
                    f"the worker on {self.config.ssh_profile} did not answer ({exc}). It may "
                    "have exited idle already",
                    site=self.context.site_id,
                ) from exc
        try:
            answer = await _in_daemon_thread(
                lambda: connection.request_json(
                    method="POST", path="/shutdown", body={}, response_timeout_seconds=10.0
                )
            )
        except Exception as exc:
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"the worker on {self.config.ssh_profile} did not take the shutdown ({exc})",
                site=self.context.site_id,
            ) from exc
        finally:
            connection.close()
        # The worker is gone, so is its link: the next use signs in at once, and a
        # second shutdown (site clean runs one) has nothing to wait on.
        self.link_path.unlink(missing_ok=True)
        answered = cast(dict[str, Any], answer) if isinstance(answer, dict) else {}
        pid = answered.get("pid")
        stopped: dict[str, Any] = {
            "kind": "site_serve",
            "detail": f"worker site-serve in {link.remote_dir} and its frpc proxy",
        }
        if isinstance(pid, int):
            stopped["pid"] = pid
        if held is None:
            return [stopped]
        return [stopped, {"kind": "frp visitor", "detail": "relay's link visitor to the worker"}]

    def clean_targets(self) -> tuple[list[str], list[str]]:
        """The worker's directory and job homes; each home's Spack store and JARVIS answers stay,
        and so does a published resource graph."""
        link = self.load_link()
        remote_dir = link.remote_dir if link is not None and link.remote_dir else self._remote_dir()
        targets = [remote_dir]
        graph = self.config.jarvis_graph
        keep = site_keeps(posixpath.join(remote_dir, "adapter", "*"), graph)
        if self.config.work_dir is not None:
            work_dir = self.config.work_dir
            targets += [
                posixpath.join(work_dir, jarvis_submit.BOOTSTRAP_DIR),
                posixpath.join(work_dir, "jobs"),
                posixpath.join(work_dir, "psij"),
            ]
            keep += site_keeps(work_dir, graph)
        return targets, list(dict.fromkeys(keep))

    async def clean(self, targets: list[str], keep: list[str]) -> dict[str, Any]:
        """Prune over a plain ``ssh``: the worker is stopped by then, so its link is gone."""
        self._drop_connection()
        return parse_report(await self._plain_ssh(prune_script(targets, keep), "cleaning"))

    async def live_jobs(self, known: list[str], *roots: str) -> list[dict[str, str]]:
        """Ask the site's scheduler, over a plain ``ssh``, which of relay's jobs still run."""
        return parse_live(await self._plain_ssh(jobs_script(known, roots), "listing jobs on"))

    async def _plain_ssh(self, script: str, what: str) -> str:
        """Run ``script`` over a plain ``ssh`` that signs in without a prompt."""
        argv = self.sign_in_argv()
        argv[argv.index("BatchMode=no")] = "BatchMode=yes"
        return await batch_ssh(
            argv, script, f"{what} {self.config.ssh_profile}", self.context.site_id
        )

    def _site_config(self) -> dict[str, Any]:
        config: dict[str, Any] = {
            "scheduler": self.config.scheduler,
            "submit_timeout_s": self.config.submit_timeout_s,
            "jarvis": self.config.jarvis,
            "outputs_dir": self.config.outputs_dir,
        }
        if self.config.jarvis_python:
            config["jarvis_python"] = self.config.jarvis_python
        if self.config.jarvis_graph:
            config["jarvis_graph"] = self.config.jarvis_graph
        if self.config.install_walltime_s is not None:
            config["install_walltime_s"] = self.config.install_walltime_s
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

    def job_dir(self, handle: JobHandle) -> str | None:
        """The job's directory on the site, from the site's own handle."""
        remote = handle.data.get("remote")
        if not isinstance(remote, dict):
            return None
        data = cast(dict[str, Any], remote).get("data")
        found = cast(dict[str, Any], data).get("job_dir") if isinstance(data, dict) else None
        return found if isinstance(found, str) and found else None

    async def run(self, spec: JobSpec, *, operation_id: str) -> JobHandle:
        return self._wrap(JobHandle.model_validate(await self._submit("/run", spec, operation_id)))

    async def install_check(self, spec: JobSpec, *, operation_id: str) -> dict[str, Any]:
        return cast(dict[str, Any], await self._submit("/install-check", spec, operation_id))

    async def _submit(
        self, route: str, spec: JobSpec, operation_id: str, *, stage: bool = True, **extra: Any
    ) -> Any:
        if stage:
            if spec.steps():
                spec = jarvis_submit.with_archive(spec)  # stage the reviewed archives too
            await self._stage(spec)
        body = {"spec": spec.model_dump(mode="json"), "operation_id": operation_id, **extra}
        return await self.call(route, body, timeout_s=self.config.submit_timeout_s + 120)

    async def _stage(self, spec: JobSpec) -> None:
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

    async def stop(self, handle: JobHandle) -> JobStatus:
        reported = await self.call(
            "/stop",
            {"handle": self._unwrap(handle)},
            timeout_s=self.config.submit_timeout_s + 120,
        )
        return JobStatus.model_validate(reported)

    async def service_runtimes(self, handle: JobHandle) -> dict[str, Any] | None:
        """The site's ``jarvis execution service-runtimes +json`` for a JARVIS job."""
        reported = await self.call(
            "/service-runtimes", {"handle": self._unwrap(handle)}, timeout_s=120
        )
        return cast(dict[str, Any], reported) if isinstance(reported, dict) else None

    async def install(
        self, spec: JobSpec, *, operation_id: str, answers: dict[str, Any]
    ) -> JobHandle:
        reported = await self._submit("/install", spec, operation_id, stage=False, answers=answers)
        return self._wrap(JobHandle.model_validate(reported))

    async def install_jarvis(self) -> str:
        # Building a virtual environment and installing jarvis-cd takes minutes.
        return str(await self.call("/install-jarvis", {}, timeout_s=3600))

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
