"""Settings for the ``tapis`` adapter."""

from __future__ import annotations

from typing import Literal, cast

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TapisConfig(BaseModel):
    """One Tapis system, reached through one tenant."""

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def _no_launcher(cls, data: object) -> object:
        if isinstance(data, dict) and "launcher" in cast(dict[str, object], data):
            raise ValueError(
                "launcher is no longer a Tapis setting: every job runs a reviewed JARVIS "
                "package from the site's JARVIS root. Remove launcher from the site's config"
            )
        return cast(object, data)

    base_url: str = Field(
        default="https://tacc.tapis.io", description="The Tapis tenant's base address"
    )
    tenant_id: str = Field(default="tacc", description="The tenant id")
    authn: Literal["pki", "tms"] = Field(
        default="pki",
        description=(
            "pki: relay makes a key pair for a host the person can reach over SSH. "
            "tms: Tapis's trust manager makes and holds the key, for hosts with a second factor"
        ),
    )
    system_id: str | None = Field(
        default=None,
        description=(
            "The Tapis system. Defaults to clio-relay.<Tapis username>.<site id>, "
            "the system relay registers for the person"
        ),
    )
    work_dir: str | None = Field(
        default=None,
        description=(
            "Relay's directory on the system, as a Tapis path. Defaults to the system's "
            "job working directory"
        ),
    )
    client_id: str | None = Field(
        default=None, description="Relay's OAuth client in the tenant, when not the shipped one"
    )
