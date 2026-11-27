"""Per-site guard settings, read from ``<config_dir>/guard.yaml``.

The file is optional. Each key under ``sites`` is a site id::

    sites:
      polaris:
        jarvis: ~/venvs/jarvis/bin/jarvis
        read_only: [myquota]
        spack: [.clio-relay/spack/bin/spack, /opt/spack/bin/spack]
        warm_idle_s: 600

The file is read on every check, so an edit takes effect without a restart.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from clio_relay.jobs.refusals import Refusal, RefusalReason

WARM_IDLE_S = 600
"""A warm allocation nobody uses for this long releases itself (RL26)."""
CLIO_SPACK = (".clio-relay/spack/bin/spack",)
"""The CLIO Spack (``jarvis spack prepare``), relative to the home directory."""


class SiteGuard(BaseModel):
    """The guard's settings for one site."""

    model_config = ConfigDict(extra="forbid")

    jarvis: str = Field(
        default="jarvis",
        description=(
            "The jarvis command on the site, whose read-only commands exec allows. The "
            "adapter's own jarvis setting is the one that submits package jobs"
        ),
    )
    read_only: list[str] = Field(
        default_factory=list[str],
        description="Programs added to the site's read-only exec list, by exact argv[0]",
    )
    spack: list[str] = Field(
        default_factory=lambda: list(CLIO_SPACK),
        description=(
            "Spack executables, by exact argv[0], whose find, info and spec exec allows "
            "besides a bare spack. Relative paths are from the home directory"
        ),
    )
    warm_idle_s: int = Field(default=WARM_IDLE_S, ge=60)


class GuardConfig(BaseModel):
    """The whole ``guard.yaml``."""

    model_config = ConfigDict(extra="forbid")

    sites: dict[str, SiteGuard] = Field(default_factory=dict[str, SiteGuard])

    def site(self, site_id: str) -> SiteGuard:
        """Return ``site_id``'s settings, or the defaults."""
        return self.sites.get(site_id) or SiteGuard()


def load_config(config_dir: Path | None) -> GuardConfig:
    """Read ``guard.yaml``. A missing file means the defaults everywhere."""
    if config_dir is None:
        return GuardConfig()
    path = config_dir / "guard.yaml"
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return GuardConfig()
    try:
        return GuardConfig.model_validate(yaml.safe_load(text) or {})
    except (yaml.YAMLError, ValidationError) as exc:
        raise Refusal(
            RefusalReason.INVALID_CONFIG, f"{path} is not a valid guard file: {exc}", path=str(path)
        ) from exc
