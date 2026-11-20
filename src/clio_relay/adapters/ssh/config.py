"""Settings for the ``ssh`` adapter."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

SchedulerName = Literal["slurm", "pbs", "lsf", "cobalt"]
SchedulerChoice = Literal["auto", "none", "slurm", "pbs", "lsf", "cobalt"]


class SshConfig(BaseModel):
    """How relay reaches a site with the system ``ssh`` and ``scp``.

    The profile is a host from the person's own ssh configuration, so their
    agent, keys, jump hosts and options apply.
    """

    model_config = ConfigDict(extra="forbid")

    ssh_profile: str = Field(
        min_length=1,
        description="The host to connect to: a Host from ~/.ssh/config, or user@host",
    )
    ssh_config: str | None = Field(
        default=None,
        description="An ssh_config file to use instead of ~/.ssh/config",
    )
    ssh_options: list[str] = Field(
        default_factory=list[str],
        description="Extra ssh options as Key=Value, passed with -o",
    )
    remote_dir: str = Field(
        default=".clio-relay",
        description="Relay's directory on the site, relative to the home directory or absolute",
    )
    scheduler: SchedulerChoice = Field(
        default="auto",
        description="auto picks the scheduler whose submit command the site has",
    )
    jarvis: str = Field(
        default="jarvis",
        min_length=1,
        description=(
            "The jarvis command on the site, which runs package jobs. A leading ~/ "
            "means the home directory"
        ),
    )
    interactive_sign_in: bool = Field(
        default=False,
        description=(
            "The site asks for a code or password on every connection. Relay signs in "
            "once through an ask and reuses that connection"
        ),
    )
    askpass: str | None = Field(
        default=None,
        description="The program ssh asks for codes and passwords. Defaults to relay's own",
    )
    ssh_command: str = Field(default="ssh", description="The ssh program")
    scp_command: str = Field(default="scp", description="The scp program")
    connect_timeout_s: int = Field(default=30, ge=1, description="ssh ConnectTimeout")
    command_timeout_s: float = Field(
        default=300,
        gt=0,
        description=(
            "How long one describe, status or cancel call may go without finishing before "
            "relay calls it stalled. The next poll tries again. Jobs and transfers have no limit"
        ),
    )
    retries: int = Field(
        default=5,
        ge=0,
        description=(
            "How many times relay repeats a call or copy whose connection failed or dropped, "
            "waiting 2, 4, 8, 16, then 30 seconds. exec is repeated only when the "
            "connection never opened"
        ),
    )
