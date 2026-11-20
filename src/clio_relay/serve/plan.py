"""Turning a service spec into the job relay submits, and the record it keeps (RL19).

The job relay submits is the person's job wrapped by ``serve/launch.sh``. A
package service is a JARVIS pipeline instead: JARVIS starts the service, and
relay's connector, ``serve/connect.sh``, runs beside the batch shell
(:func:`package_service_job`). The wrapper or the connector, its port list,
the frpc configuration and the secrets reach the job directory as ordinary
content-addressed inputs (RL16). Secrets travel only in
``serve/secrets.env``, which the wrapper deletes once read, so they are in no
argument list, environment block of a batch script, record or log. That input
is marked transient, so relay and the site drop their stored copies when the
job ends.
"""

from __future__ import annotations

import os
import re
import secrets
import socket
from datetime import datetime
from importlib import resources
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from clio_relay.jobs.handles import Endpoint, EndpointPort, JobHandle, utc_now
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import CustomValue, InputRef, JobSpec, PortProtocol, ServiceSpec
from clio_relay.serve import frp
from clio_relay.serve.config import Rendezvous, SiteServe

SERVE_DIR = "serve"
SECRETS_FILE = "secrets.env"
SECRET_ENV_PREFIX = "secret_env:"
_PLACEHOLDER = re.compile(r"\$\{(CLIO_RELAY_[A-Z0-9_]+)\}")


def site_file(name: str) -> bytes:
    """Return one of the files relay runs on the site."""
    return resources.files("clio_relay.serve").joinpath("_site", name).read_bytes()


def env_suffix(name: str) -> str:
    """Return a port name as it appears in an environment variable."""
    return re.sub(r"[^A-Z0-9]", "_", name.upper())


def service_name(spec: ServiceSpec) -> str:
    """Return the service's name: the job's name, or its first port's."""
    raw = (spec.job.name or spec.ports[0].name).lower()
    name = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
    return name or "service"


def free_loopback_port() -> int:
    """Return a loopback port nothing listens on now."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def remote_port_choice() -> int:
    """Return a port for a service that did not name one."""
    return 20000 + secrets.randbelow(40000)


class HopState(BaseModel):
    """The login-side CONNECT proxy relay started for a site with no way out."""

    pid: int
    host: str
    port: int
    token: str


class PortPlan(BaseModel):
    """One served port, on the site and on the desktop."""

    name: str
    protocol: PortProtocol
    remote_port: int
    local_port: int
    health_path: str | None = None
    protection: str | None = None
    secret_env: str | None = None

    def url(self) -> str | None:
        """Return the desktop URL, which never carries a secret."""
        path = self.health_path or ("/mcp" if self.protocol == "mcp" else "")
        if self.protocol in ("http", "mcp"):
            return f"http://127.0.0.1:{self.local_port}{path}"
        if self.protocol == "ws":
            return f"ws://127.0.0.1:{self.local_port}{path}"
        return None


class ServePlan(BaseModel):
    """What relay keeps about one service. Holds secrets, so the file is private."""

    version: Literal[1] = 1
    operation_id: str
    site: str
    adapter: str
    service: str
    mode: Literal["frp", "ssh"]
    ports: list[PortPlan]
    secret_key: str = ""
    secrets: dict[str, str] = Field(default_factory=dict[str, str])
    rendezvous: Rendezvous | None = None
    site_serve: SiteServe = Field(default_factory=SiteServe)
    hop: HopState | None = None
    handle: JobHandle | None = None
    job_dir: str | None = None
    bound_to: str | None = None
    idle_release_s: float | None = None
    ssh_argv: list[str] = Field(default_factory=list[str])
    login_host: str | None = None
    host: str | None = None
    ready_at: datetime | None = None
    stop_requested_at: datetime | None = None
    cancel_sent: bool = False
    forward_pid: int | None = None
    rebinds: int = 0
    package: bool = Field(
        default=False,
        description="A JARVIS pipeline: jarvis execution stop ends it, and serve/idle asks to",
    )
    final: dict[str, Any] | None = None
    created_at: datetime = Field(default_factory=utc_now)

    def proxy_names(self) -> list[str]:
        """Return the frp proxy names, one per port."""
        return [frp.proxy_name(self.operation_id, port.name) for port in self.ports]

    def endpoint(self, *, note: str | None, connect_command: str | None) -> Endpoint:
        """Return the endpoint as any client may see it: no secret in it."""
        return Endpoint(
            service=self.service,
            ports=[
                EndpointPort(
                    name=port.name,
                    protocol=port.protocol,
                    host="127.0.0.1",
                    port=port.local_port,
                    url=port.url(),
                )
                for port in self.ports
            ],
            durable=self.mode == "frp" or os.name != "nt",
            connect_command=connect_command,
            note=note,
        )

    def service_env(self) -> dict[str, str]:
        """Return the non-secret environment the service gets (RL22)."""
        env: dict[str, str] = {}
        for port in self.ports:
            suffix = env_suffix(port.name)
            env[f"CLIO_RELAY_PORT_{suffix}"] = str(port.remote_port)
            env[f"CLIO_RELAY_LOCAL_PORT_{suffix}"] = str(port.local_port)
            url = port.url()
            if url:
                env[f"CLIO_RELAY_URL_{suffix}"] = url
        return env


def plan_ports(spec: ServiceSpec) -> list[PortPlan]:
    """Choose the ports and the per-port secrets of a new service."""
    ports: list[PortPlan] = []
    seen: set[str] = set()
    for port in spec.ports:
        if port.name in seen:
            raise Refusal(RefusalReason.INVALID_SPEC, f"port {port.name!r} is named twice")
        seen.add(port.name)
        secret_env: str | None = None
        if port.protection and port.protection.startswith(SECRET_ENV_PREFIX):
            secret_env = port.protection[len(SECRET_ENV_PREFIX) :].strip()
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", secret_env):
                raise Refusal(
                    RefusalReason.INVALID_SPEC,
                    f"port {port.name!r}: {port.protection!r} names no environment variable",
                )
        ports.append(
            PortPlan(
                name=port.name,
                protocol=port.protocol,
                remote_port=port.port or remote_port_choice(),
                local_port=free_loopback_port(),
                health_path=port.health_path,
                protection=port.protection,
                secret_env=secret_env,
            )
        )
    return ports


def check_wrappable(spec: ServiceSpec) -> None:
    """Refuse a service relay cannot put its wrapper around."""
    job = spec.job
    for item in job.inputs:
        if item.name == SERVE_DIR or item.name.startswith(f"{SERVE_DIR}/"):
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                f"input {item.name!r} is inside {SERVE_DIR}/, which relay uses",
            )
    if job.steps():
        for port in spec.ports:
            if port.protection and port.protection.startswith(SECRET_ENV_PREFIX):
                raise Refusal(
                    RefusalReason.INVALID_SPEC,
                    f"port {port.name!r}: a package service cannot take a secret, because "
                    "secrets reach only relay's wrapper and JARVIS starts the package",
                    field="protection",
                )
        return
    if job.launcher not in (None, "single"):
        raise Refusal(
            RefusalReason.INVALID_SPEC,
            "a service runs one wrapper process; start parallel ranks from the program itself",
            launcher=job.launcher,
        )


def substitute(values: list[str], env: dict[str, str]) -> list[str]:
    """Fill ``${CLIO_RELAY_...}`` in arguments from the service's environment."""
    return [
        _PLACEHOLDER.sub(lambda match: env.get(match.group(1), match.group(0)), value)
        for value in values
    ]


def ports_file(plan: ServePlan) -> bytes:
    """Return ``serve/ports`` for the wrapper."""
    lines = [
        f"{port.name} {port.remote_port} {port.protocol} {port.health_path or '-'}"
        for port in plan.ports
    ]
    return ("\n".join(lines) + "\n").encode()


def secrets_file(values: dict[str, str]) -> bytes:
    """Return ``serve/secrets.env``. Values are URL-safe tokens or the frp token."""
    lines: list[str] = []
    for name, value in sorted(values.items()):
        if "'" in value or "\n" in value:
            raise Refusal(
                RefusalReason.INVALID_CONFIG, f"secret {name} has a character it cannot carry"
            )
        lines.append(f"export {name}='{value}'")
    return ("\n".join(lines) + "\n").encode()


def site_frpc_config(plan: ServePlan) -> bytes:
    """Return the frpc configuration that runs beside the service."""
    rendezvous = plan.rendezvous
    if rendezvous is None:
        raise Refusal(RefusalReason.INVALID_CONFIG, "no rendezvous host is set")
    site = plan.site_serve
    proxy_url: str | None = None
    if site.hop == "login" and plan.hop is not None:
        proxy_url = (
            "http://relay:{{ .Envs."
            + frp.ENV_HOP_TOKEN
            + " }}@"
            + f"{plan.hop.host}:{plan.hop.port}"
        )
    elif site.hop == "http_proxy":
        proxy_url = site.proxy_url
    common = frp.Common(
        address=site.address or rendezvous.address,
        port=site.port or rendezvous.port,
        protocol=rendezvous.protocol,
        ca_file="{{ .Envs.CLIO_RELAY_JOB_DIR }}/serve/ca.pem"
        if rendezvous.protocol == "wss"
        else None,
        server_name=rendezvous.tls_name(),
        proxy_url=proxy_url,
        stun_server=(site.stun_server or rendezvous.stun_server) if rendezvous.direct else None,
    )
    ports = [(port.name, port.remote_port) for port in plan.ports]
    return frp.render_site(common, ports, plan.operation_id, direct=rendezvous.direct).encode()


def site_secrets(plan: ServePlan) -> dict[str, str]:
    """Return everything ``serve/secrets.env`` carries."""
    values = dict(plan.secrets)
    if plan.mode == "frp":
        if plan.rendezvous is None:
            raise Refusal(RefusalReason.INVALID_CONFIG, "no rendezvous host is set")
        values[frp.ENV_TOKEN] = plan.rendezvous.token()
        values[frp.ENV_SECRET_KEY] = plan.secret_key
        if plan.hop is not None:
            values[frp.ENV_HOP_TOKEN] = plan.hop.token
    return values


def ca_bytes(rendezvous: Rendezvous) -> bytes:
    """Return the CA bundle the site's frpc verifies a wss server with."""
    path = rendezvous.ca_file or frp.default_ca_file()
    if not path:
        raise Refusal(RefusalReason.INVALID_CONFIG, "a wss rendezvous needs ca_file")
    try:
        return Path(os.path.expanduser(path)).read_bytes()
    except OSError as exc:
        raise Refusal(
            RefusalReason.INVALID_CONFIG, f"cannot read the CA file: {exc.strerror}"
        ) from exc


def site_files(plan: ServePlan) -> dict[str, bytes]:
    """Return the files relay places in the job's ``serve/`` directory."""
    wrapper = "connect.sh" if plan.package else "launch.sh"
    files: dict[str, bytes] = {
        wrapper: site_file(wrapper),
        "ports": ports_file(plan),
        SECRETS_FILE: secrets_file(site_secrets(plan)),
    }
    if plan.mode == "frp":
        files["frpc.toml"] = site_frpc_config(plan)
        site = plan.site_serve
        if not site.frpc:
            if not site.frpc_binary:
                raise Refusal(
                    RefusalReason.INVALID_CONFIG,
                    f"site {plan.site!r} has no frp client: set its serve frpc (a site path), "
                    "or frpc_binary (a local frpc built for the site, which relay copies in)",
                    site=plan.site,
                )
            try:
                files["frpc"] = Path(os.path.expanduser(site.frpc_binary)).read_bytes()
            except OSError as exc:
                raise Refusal(
                    RefusalReason.INVALID_CONFIG, f"cannot read frpc_binary: {exc.strerror}"
                ) from exc
        if plan.rendezvous is not None and plan.rendezvous.protocol == "wss":
            files["ca.pem"] = ca_bytes(plan.rendezvous)
    return files


def wrapped_job(spec: ServiceSpec, plan: ServePlan, staged: dict[str, str]) -> JobSpec:
    """Return the job relay submits: the person's program under ``serve/launch.sh``."""
    job = spec.job
    if job.executable is None:
        raise Refusal(RefusalReason.INVALID_SPEC, "a service needs an executable")
    service_env = plan.service_env()
    env = {**job.environment, **service_env}
    env["CLIO_RELAY_STOP_GRACE"] = str(int(plan.site_serve.stop_grace_s))
    env["CLIO_RELAY_IDLE_S"] = str(int(plan.idle_release_s or 0))
    env["CLIO_RELAY_OPERATION"] = plan.operation_id
    if plan.mode == "frp":
        env["CLIO_RELAY_FRPC"] = plan.site_serve.frpc or f"{SERVE_DIR}/frpc"
    if job.directory:
        env["CLIO_RELAY_SERVICE_DIR"] = job.directory
    inputs = list(job.inputs) + [
        InputRef(
            name=f"{SERVE_DIR}/{name}",
            sha256=digest,
            transient=name == SECRETS_FILE,
        )
        for name, digest in staged.items()
    ]
    return job.model_copy(
        update={
            "executable": "/bin/sh",
            "arguments": [
                f"{SERVE_DIR}/launch.sh",
                "--",
                job.executable,
                *substitute(job.arguments, env),
            ],
            "directory": None,
            "environment": env,
            "inputs": inputs,
            "launcher": None,
            "name": job.name or f"relay-{plan.service}",
        }
    )


def _filled(value: CustomValue, env: dict[str, str]) -> CustomValue:
    if isinstance(value, str):
        return substitute([value], env)[0]
    return value


def package_service_job(spec: ServiceSpec, plan: ServePlan, staged: dict[str, str]) -> JobSpec:
    """Return the JARVIS pipeline relay submits for a package service.

    Every step runs as a service. ``${CLIO_RELAY_...}`` in a step's parameters
    is filled from the service's environment, so a package's port parameter can
    name the port relay chose (``${CLIO_RELAY_PORT_<NAME>}``). The connector's
    settings travel in the job's environment, where
    :func:`clio_relay.jarvis_submit.connector_command` reads them.
    """
    job = spec.job
    env = {**job.environment, **plan.service_env()}
    env["CLIO_RELAY_IDLE_S"] = str(int(plan.idle_release_s or 0))
    env["CLIO_RELAY_OPERATION"] = plan.operation_id
    if plan.mode == "frp":
        env["CLIO_RELAY_FRPC"] = plan.site_serve.frpc or f"{SERVE_DIR}/frpc"
    steps = [
        step.model_copy(
            update={
                "service": True,
                "parameters": {key: _filled(value, env) for key, value in step.parameters.items()},
            }
        )
        for step in job.steps()
    ]
    inputs = list(job.inputs) + [
        InputRef(name=f"{SERVE_DIR}/{name}", sha256=digest, transient=name == SECRETS_FILE)
        for name, digest in staged.items()
    ]
    return job.with_steps(steps).model_copy(
        update={
            "environment": env,
            "inputs": inputs,
            "name": job.name or f"relay-{plan.service}",
        }
    )


def new_secret() -> str:
    """Return a fresh URL-safe secret."""
    return secrets.token_urlsafe(24)
