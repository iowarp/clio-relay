"""Local cluster registry for relay targets."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from clio_relay.errors import ConfigurationError


class ClusterDefinition(BaseModel):
    """A locally configured cluster target."""

    model_config = ConfigDict(extra="forbid")

    name: str
    ssh_host: str
    bootstrap_profile: str = "linux-user"
    core_dir: str = "$HOME/.local/share/clio-relay/core"
    spool_dir: str = "$HOME/.local/share/clio-relay/spool"
    agent_npm_package: str = "@openai/codex"
    agent_npm_bin: str = "codex"


class ClusterRegistry(BaseModel):
    """Configured cluster targets."""

    model_config = ConfigDict(extra="forbid")

    clusters: dict[str, ClusterDefinition] = Field(default_factory=dict)

    @classmethod
    def default(cls) -> ClusterRegistry:
        """Return default local cluster definitions."""
        return cls(
            clusters={
                "ares": ClusterDefinition(name="ares", ssh_host="ares"),
                "homelab": ClusterDefinition(name="homelab", ssh_host="homelab"),
            }
        )

    @classmethod
    def load(cls, path: Path) -> ClusterRegistry:
        """Load a registry from disk, creating defaults if the file is absent."""
        if not path.exists():
            registry = cls.default()
            registry.save(path)
            return registry
        return cls.model_validate_json(path.read_text(encoding="utf-8"))

    def save(self, path: Path) -> None:
        """Persist the registry to disk."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.model_dump(), indent=2), encoding="utf-8")

    def require(self, name: str) -> ClusterDefinition:
        """Return a configured cluster or raise a configuration error."""
        try:
            return self.clusters[name]
        except KeyError as exc:
            raise ConfigurationError(f"cluster is not configured: {name}") from exc


def default_registry_path() -> Path:
    """Return the default local cluster registry path."""
    return Path(".clio-relay/clusters.json")
