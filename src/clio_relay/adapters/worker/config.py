"""The worker adapter's settings and the link record relay keeps for it."""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from clio_relay import jarvis_submit, site_outputs
from clio_relay.adapters.local import SchedulerChoice
from clio_relay.adapters.worker.site_server import IDLE_EXIT_S, LinkMode
from clio_relay.login_load import LoginLimits
from clio_relay.serve.config import DEFAULT_STUN_SERVER

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
    stun_server: str = Field(
        default=DEFAULT_STUN_SERVER,
        min_length=1,
        description=(
            "The STUN server both ends ask in udp_rendezvous. It must support RFC 5780, "
            "as frp needs"
        ),
    )


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
    work_dir: str | None = Field(default=None, description="Where job directories go on the site")
    outputs_dir: str = Field(default=site_outputs.OUTPUTS_DIR, description=site_outputs.HELP)
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
        default=None, description="What uv installs on the site instead, such as clio-relay==1.7.0"
    )
    site_frpc: str | None = Field(
        default=None,
        description="frpc on the site. When unset the sign-in fetches a checked frpc on the site",
    )
    frpc_url: str | None = Field(
        default=None,
        description="Where the site fetches frpc instead of relay's pinned frp release "
        "(a .tar.gz, .tgz or the binary); needs frpc_sha256",
    )
    frpc_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
        description="The sha256 the file at frpc_url must have",
    )
    frpc_binary: str | None = Field(
        default=None,
        description="A local frpc built for the site, copied in instead of fetching one",
    )
    python: str = Field(default="3.12", pattern=r"^3\.\d+$")
    submit_timeout_s: float = Field(default=60, gt=0)
    jarvis: str = Field(default="", description=jarvis_submit.JARVIS_SETTING_HELP)
    jarvis_graph: str = Field(default="", description=jarvis_submit.GRAPH_SETTING_HELP)
    jarvis_python: str = Field(default="", description=jarvis_submit.PYTHON_SETTING_HELP)
    install_walltime_s: int | None = Field(
        default=None,
        ge=60,
        description=jarvis_submit.INSTALL_WALLTIME_HELP,
    )
    idle_exit_s: int = Field(
        default=IDLE_EXIT_S,
        ge=0,
        description=(
            "The worker on the site exits after this many seconds with no request from relay, "
            "and the next sign-in places it again. 0 keeps it until it is stopped"
        ),
    )
    login_limits: LoginLimits = Field(
        default_factory=LoginLimits,
        description="What the worker may use on the site's login node",
    )
    link_mode: LinkMode = Field(
        default="brokered_tcp",
        description=(
            "How relay reaches the worker. brokered_tcp carries every call through the "
            "rendezvous (frp stcp). udp_rendezvous punches a UDP hole to the site (frp xtcp); "
            "a failed punch is refused, and relay never switches to brokered_tcp on its own"
        ),
    )

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

    @field_validator("jarvis")
    @classmethod
    def _jarvis_path(cls, value: str) -> str:
        return jarvis_submit.explicit_jarvis(value)

    @field_validator("jarvis_graph")
    @classmethod
    def _jarvis_graph(cls, value: str) -> str:
        return jarvis_submit.published_graph(value)

    @field_validator("jarvis_python")
    @classmethod
    def _jarvis_python(cls, value: str) -> str:
        return jarvis_submit.explicit_python(value)


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
    # Links stored by an older relay lack it; their worker is placed again.
    session_owner_token: str | None = None
    # The worker's proxy type follows it, so a changed link_mode places it again.
    link_mode: LinkMode = "brokered_tcp"
