"""Local cluster registry for relay targets."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator

from clio_relay.errors import ConfigurationError


class DirectTransportConfig(BaseModel):
    """Optional NAT-punching transport optimization settings."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    mode: str = "xtcp"
    fallback_order: list[str] = Field(default_factory=lambda: ["frp_stcp", "queue"])
    probe_timeout_seconds: float = 10.0

    @field_validator("mode")
    @classmethod
    def _validate_mode(cls, value: str) -> str:
        if value not in {"xtcp"}:
            raise ValueError("direct transport mode must be xtcp")
        return value

    @field_validator("fallback_order")
    @classmethod
    def _validate_fallback_order(cls, value: list[str]) -> list[str]:
        allowed = {"xtcp", "frp_stcp", "queue"}
        if not value:
            raise ValueError("direct transport fallback_order must not be empty")
        invalid = [entry for entry in value if entry not in allowed]
        if invalid:
            raise ValueError(f"unsupported direct transport fallback entries: {invalid}")
        if value[-1] != "queue":
            raise ValueError("direct transport fallback_order must end with queue")
        return value

    @field_validator("probe_timeout_seconds")
    @classmethod
    def _validate_probe_timeout_seconds(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("direct transport probe_timeout_seconds must be positive")
        return value


class FrpTransportConfig(BaseModel):
    """Transport settings for frpc-to-frps connections."""

    model_config = ConfigDict(extra="forbid")

    protocol: str = "wss"
    server_addr: str = ""
    server_port: int = 443
    token_env: str = "CLIO_RELAY_FRP_TOKEN"
    stcp_secret_env: str = "CLIO_RELAY_STCP_SECRET"
    direct: DirectTransportConfig = Field(default_factory=DirectTransportConfig)


class LiveTestConfig(BaseModel):
    """Configured live acceptance inputs for a cluster."""

    model_config = ConfigDict(extra="forbid")

    jarvis_yaml: str | None = None
    monitor_pattern: str | None = None
    progress_pattern: str | None = None
    progress_action_payload: dict[str, object] = Field(default_factory=dict)
    verify_transport: bool = False
    verify_direct_transport: bool = False
    allow_direct_transport_fallback: bool = False
    transport_local_bind_port: int = 18765
    transport_remote_api_port: int | None = None
    transport_proxy_name: str | None = None
    agent_prompt: str | None = None
    agent_child_jarvis_yaml: str | None = None
    agent_mcp_config: str | None = None


class ClusterDefinition(BaseModel):
    """A locally configured cluster target."""

    model_config = ConfigDict(extra="forbid")

    name: str
    ssh_host: str
    bootstrap_profile: str = "linux-user"
    core_dir: str = "$HOME/.local/share/clio-relay/core"
    spool_dir: str = "$HOME/.local/share/clio-relay/spool"
    jarvis_bin: str | None = None
    frpc_bin: str | None = None
    agent_bin: str | None = None
    agent_adapter: str = "exec"
    agent_npm_package: str | None = None
    agent_npm_bin: str | None = None
    agent_args: list[str] = Field(default_factory=list)
    frp_transport: FrpTransportConfig = Field(default_factory=FrpTransportConfig)
    live_test: LiveTestConfig = Field(default_factory=LiveTestConfig)


class ClusterRegistry(BaseModel):
    """Configured cluster targets."""

    model_config = ConfigDict(extra="forbid")

    clusters: dict[str, ClusterDefinition] = Field(default_factory=dict)

    @classmethod
    def default(cls) -> ClusterRegistry:
        """Return an empty registry for explicit local cluster definitions."""
        return cls()

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
