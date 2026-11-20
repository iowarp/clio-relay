"""The rendezvous setting and the per-site serve settings (RL20).

The rendezvous host is one setting: address, protocol and token. A site may
see the rendezvous host at a different address than the desktop does, and its
compute nodes may need a hop to reach it. Both live in :class:`SiteServe`.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.login_load import LoginLimits

FrpProtocol = Literal["tcp", "kcp", "quic", "websocket", "wss"]
Hop = Literal["none", "login", "http_proxy"]

SETTINGS_FILE = "rendezvous.json"
# Nextcloud's public STUN server. frp's own default, stun.easyvoip.com, timed out on 3 of
# 10 tries from Ares; this one answered 10 of 10 and supports RFC 5780, which frp needs.
DEFAULT_STUN_SERVER = "stun.nextcloud.com:3478"


class Rendezvous(BaseModel):
    """Where frp clients meet: the person's own ``relay-host`` or a hosted one."""

    model_config = ConfigDict(extra="forbid")

    address: str = Field(min_length=1, description="Host name or IP of the frp server")
    port: int = Field(ge=1, le=65535)
    protocol: FrpProtocol = "tcp"
    token_file: str | None = Field(default=None, description="File holding the frp token")
    token_env: str | None = Field(default=None, description="Environment variable with the token")
    ca_file: str | None = Field(
        default=None,
        description="CA bundle for a wss server. Without it the system roots verify it",
    )
    server_name: str | None = Field(
        default=None, description="Name the wss certificate must carry. Defaults to address"
    )
    frpc: str = Field(default="frpc", description="frpc on this machine")
    direct: bool = Field(default=False, description="Try XTCP first, with STCP as the fallback")
    stun_server: str = Field(
        default=DEFAULT_STUN_SERVER,
        min_length=1,
        description=(
            "STUN server both frpc ends ask for their public address when direct. It must "
            "support RFC 5780 (it answers from a second address)"
        ),
    )
    direct_wait_s: float = Field(
        default=10,
        gt=0,
        le=19,
        description=(
            "How long a connection waits for the direct path before it goes through the "
            "rendezvous host. frp gives up on its own at 20 s"
        ),
    )

    @model_validator(mode="after")
    def _token_source(self) -> Rendezvous:
        if self.token_file is None and self.token_env is None:
            raise ValueError("give token_file or token_env")
        return self

    def token(self) -> str:
        """Read the token. It is never stored in a record or a log."""
        if self.token_env:
            value = os.environ.get(self.token_env)
            if value:
                return value.strip()
        if self.token_file:
            path = Path(os.path.expanduser(self.token_file))
            try:
                return path.read_text(encoding="utf-8").strip()
            except OSError as exc:
                raise Refusal(
                    RefusalReason.INVALID_CONFIG,
                    f"cannot read the rendezvous token file: {exc.strerror}",
                ) from exc
        raise Refusal(RefusalReason.INVALID_CONFIG, "the rendezvous token is not set")

    def tls_name(self) -> str:
        """Return the name a wss certificate must carry."""
        return self.server_name or self.address


class SiteServe(BaseModel):
    """How one site's services reach the rendezvous host."""

    model_config = ConfigDict(extra="forbid")

    path: Literal["auto", "frp", "ssh"] = Field(
        default="auto",
        description=(
            "auto: frp when a rendezvous host is set, otherwise an ssh -L forward "
            "on an adapter that gives ssh_argv()"
        ),
    )
    address: str | None = Field(
        default=None, description="The rendezvous address as the site sees it"
    )
    port: int | None = Field(default=None, ge=1, le=65535)
    hop: Hop = Field(
        default="none",
        description=(
            "none: the job dials out. login: relay starts a CONNECT proxy on the login side, "
            "through the site's worker or exec. http_proxy: the site's own HTTP proxy"
        ),
    )
    proxy_url: str | None = Field(default=None, description="For hop http_proxy")
    hop_bind: str = Field(default="0.0.0.0", description="Where the login hop listens")
    hop_advertise: str | None = Field(
        default=None, description="The login hop's address as compute nodes see it"
    )
    frpc: str | None = Field(
        default=None, description="frpc on the site. Otherwise relay stages it"
    )
    frpc_binary: str | None = Field(
        default=None, description="A local frpc built for the site, to stage"
    )
    python: str = Field(default="python3", description="Python on the login side, for the hop")
    hop_dir: str = Field(
        default="~/.clio-relay/hop",
        min_length=1,
        description="Where the login hop keeps its lock, leases and token, one directory per site",
    )
    login_limits: LoginLimits = Field(
        default_factory=LoginLimits, description="What the login hop may use on the login node"
    )
    stun_server: str | None = Field(
        default=None, description="STUN server for this site's frpc. Defaults to the rendezvous one"
    )
    stop_grace_s: float = Field(default=30, gt=0)

    @model_validator(mode="after")
    def _proxy(self) -> SiteServe:
        if self.hop == "http_proxy" and not self.proxy_url:
            raise ValueError("hop http_proxy needs proxy_url")
        return self


class ServeSettings(BaseModel):
    """The whole settings file."""

    model_config = ConfigDict(extra="forbid")

    rendezvous: Rendezvous | None = None
    sites: dict[str, SiteServe] = Field(default_factory=dict[str, SiteServe])


def settings_path(state_dir: Path, settings: dict[str, Any] | None = None) -> Path:
    """Return the settings file.

    In order: ``settings["rendezvous_file"]`` from the hook context,
    ``CLIO_RELAY_RENDEZVOUS_FILE``, then ``<state>/serve/rendezvous.json``.
    """
    configured = (settings or {}).get("rendezvous_file")
    if configured:
        return Path(os.path.expanduser(str(configured)))
    env = os.environ.get("CLIO_RELAY_RENDEZVOUS_FILE")
    if env:
        return Path(os.path.expanduser(env))
    return state_dir / "serve" / SETTINGS_FILE


def load(path: Path) -> ServeSettings:
    """Read the settings, or empty settings when the file is absent."""
    try:
        raw: Any = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return ServeSettings()
    except (OSError, json.JSONDecodeError) as exc:
        raise Refusal(
            RefusalReason.INVALID_CONFIG, f"cannot read the serve settings {path}: {exc}"
        ) from exc
    try:
        return ServeSettings.model_validate(raw)
    except ValidationError as exc:
        raise Refusal(
            RefusalReason.INVALID_CONFIG, f"the serve settings {path} are not valid: {exc}"
        ) from exc


def save(path: Path, value: ServeSettings) -> None:
    """Write the settings. They name a token file or variable, never the token."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".tmp")
    temp.write_text(value.model_dump_json(indent=2, exclude_none=True), encoding="utf-8")
    temp.replace(path)


def site_serve(value: ServeSettings, site_id: str, entry_serve: Any = None) -> SiteServe:
    """Return a site's serve settings: the site entry's own, then the file's."""
    if isinstance(entry_serve, dict) and entry_serve:
        try:
            return SiteServe.model_validate(entry_serve)
        except ValidationError as exc:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                f"site {site_id!r} has invalid serve settings: {exc}",
                site=site_id,
            ) from exc
    return value.sites.get(site_id, SiteServe())


def show(path: Path) -> dict[str, Any]:
    """Return the settings as the CLI shows them."""
    value = load(path)
    return {"file": str(path), **value.model_dump(mode="json", exclude_none=True)}


def set_rendezvous(path: Path, rendezvous: dict[str, Any]) -> dict[str, Any]:
    """Validate and store the rendezvous setting, keeping the site settings."""
    value = load(path)
    try:
        value.rendezvous = Rendezvous.model_validate(rendezvous)
    except ValidationError as exc:
        raise Refusal(RefusalReason.INVALID_CONFIG, f"invalid rendezvous: {exc}") from exc
    save(path, value)
    return show(path)


def set_site(path: Path, site_id: str, site: dict[str, Any]) -> dict[str, Any]:
    """Validate and store one site's serve settings."""
    value = load(path)
    try:
        value.sites[site_id] = SiteServe.model_validate(site)
    except ValidationError as exc:
        raise Refusal(RefusalReason.INVALID_CONFIG, f"invalid site serve settings: {exc}") from exc
    save(path, value)
    return show(path)
