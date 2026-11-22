"""Settings for the ``ssh`` adapter."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay import jarvis_submit, site_outputs

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
        description=(
            "Relay's own directory on the site, relative to the home directory or absolute. "
            "Site clean deletes it, so no run's outputs belong there"
        ),
    )
    outputs_dir: str = Field(default=site_outputs.OUTPUTS_DIR, description=site_outputs.HELP)
    scheduler: SchedulerChoice = Field(
        default="auto",
        description=(
            "auto picks the scheduler whose submit command the site has, and refuses to run "
            "jobs on the login host when it finds none but the host shows a batch system. "
            "none runs them on that host"
        ),
    )
    jarvis: str = Field(default="", description=jarvis_submit.JARVIS_SETTING_HELP)
    jarvis_python: str = Field(default="", description=jarvis_submit.PYTHON_SETTING_HELP)
    install_walltime_s: int = Field(
        default=jarvis_submit.INSTALL_WALLTIME_S,
        ge=60,
        description=jarvis_submit.INSTALL_WALLTIME_HELP,
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

    @field_validator("jarvis")
    @classmethod
    def _jarvis_path(cls, value: str) -> str:
        return jarvis_submit.explicit_jarvis(value)

    @field_validator("jarvis_python")
    @classmethod
    def _jarvis_python(cls, value: str) -> str:
        return jarvis_submit.explicit_python(value)

    @model_validator(mode="after")
    def _outputs_outside(self) -> SshConfig:
        site_outputs.check_outputs_dir(self.outputs_dir, self.remote_dir)
        return self
