"""Site entries (RL4)."""

from __future__ import annotations

from datetime import timedelta
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

SITE_ID_PATTERN = r"^[a-z0-9][a-z0-9_-]{0,62}$"

SitePolicy = Literal["open", "guarded"]
SpendAsk = Literal["always", "never"] | Annotated[float, Field(gt=0)]


class SpendPolicy(BaseModel):
    """When the spend gate asks (RL25). A number is a threshold in node-hours."""

    model_config = ConfigDict(extra="forbid")

    ask: SpendAsk = "always"

    def needs_approval(self, node_hours: float) -> bool:
        """Return whether a job of ``node_hours`` needs a yes."""
        if self.ask == "always":
            return True
        if self.ask == "never":
            return False
        return node_hours > self.ask


class SiteDefaults(BaseModel):
    """Values a job spec gets when it leaves them out."""

    model_config = ConfigDict(extra="forbid")

    account: str | None = None
    queue: str | None = None
    duration: timedelta | None = None


class SiteEntry(BaseModel):
    """One site in ``sites.yaml``."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(pattern=SITE_ID_PATTERN)
    name: str | None = None
    address: str | None = None
    adapter: str = Field(min_length=1, description="The adapter in use")
    identity: str | None = None
    config: dict[str, Any] = Field(default_factory=dict[str, Any])
    alternatives: dict[str, dict[str, Any]] = Field(
        default_factory=dict[str, dict[str, Any]],
        description="Other ways in: adapter name to its config, in routing order",
    )
    policy: SitePolicy = Field(
        default="guarded",
        description="open runs what the agent writes; guarded runs reviewed packages only",
    )
    spend: SpendPolicy = Field(default_factory=SpendPolicy)
    defaults: SiteDefaults = Field(default_factory=SiteDefaults)
    skill: str | None = None

    @property
    def title(self) -> str:
        """Return the name to show a person."""
        return self.name or self.id


class CatalogEntry(SiteEntry):
    """A site relay ships. ``source`` is the facility page its facts came from."""

    source: str | None = None
    description: str | None = None

    def to_site(self, site_id: str | None = None) -> SiteEntry:
        """Return a copy for ``sites.yaml``."""
        data = self.model_dump(exclude={"source", "description"})
        if site_id is not None:
            data["id"] = site_id
        return SiteEntry.model_validate(data)
