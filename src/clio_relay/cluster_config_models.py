"""Pydantic schema for one locally configured cluster target.

Split out of `cluster_config.py` (iowarp/clio-relay#231): the transport,
live-acceptance, target-identity, and remote-MCP-registration models that
compose into `ClusterDefinition`. Pure schema/validation -- no filesystem or
Windows-ACL machinery, so nothing here needs a qualified cross-module call
for `tests/test_cluster_config.py`'s benefit (these classes are exercised as
ordinary Pydantic models, never monkeypatched).
"""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Literal, cast

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from clio_relay.errors import ConfigurationError
from clio_relay.models import JobKind, validate_mcp_env_from
from clio_relay.remote_values import validate_remote_path

MAX_REMOTE_MCP_SERVERS_PER_CLUSTER = 256
MAX_REMOTE_MCP_ARGS = 256
MAX_REMOTE_MCP_ENV_REFS = 256
MAX_REMOTE_MCP_ALLOW_TOOLS = 2_048
MAX_REMOTE_MCP_ARGUMENT_BYTES = 4_096
MAX_REMOTE_MCP_SCHEMA_CACHE_TTL_SECONDS = 31_536_000


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


# The identity anchor `frp_transport.py` (#231 R5) requires a cluster to opt into
# before `brokered_tcp`/`udp_rendezvous` may be used -- see
# `relay-architecture-2026-08.md` §8.3. `None` means "not opted in": modes (a)/(b)
# have no ssh-authenticated act to carry the bring-up identity document over, so a
# cluster that has not explicitly accepted the weaker preshared-link anchor does not
# get to use those modes by falling through to it unannounced.
IdentityAnchor = Literal["preshared_link_secret"]


class FrpTransportConfig(BaseModel):
    """Transport settings for frpc-to-frps connections."""

    model_config = ConfigDict(extra="forbid")

    protocol: str = "wss"
    server_addr: str = ""
    server_port: int = 443
    token_env: str = "CLIO_RELAY_FRP_TOKEN"
    stcp_secret_env: str = "CLIO_RELAY_STCP_SECRET"
    # The stcp/xtcp proxy+visitor name pair this cluster's remote relay already
    # registered at the relay point for the owned-session control channel (modes
    # (a)/(b)). `None` resolves to a per-cluster default at transport build time
    # (`frp_transport.py`); set this explicitly when the remote registration uses a
    # different name.
    proxy_name: str | None = None
    identity_anchor: IdentityAnchor | None = None
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


class ClusterTargetIdentity(BaseModel):
    """Operator-pinned physical identity for a cluster reached through an SSH alias."""

    model_config = ConfigDict(extra="forbid")

    hostnames: list[str] = Field(min_length=1, max_length=64)
    ssh_host_key_sha256: list[str] = Field(min_length=1, max_length=64)
    scheduler_cluster_name: str | None = None
    site_marker_sha256: str | None = None

    @field_validator("hostnames", "ssh_host_key_sha256")
    @classmethod
    def _identity_values_must_be_unique_and_nonempty(cls, value: list[str]) -> list[str]:
        if any(not item.strip() for item in value):
            raise ValueError("cluster target identity values must not be blank")
        if len(value) != len(set(value)):
            raise ValueError("cluster target identity values must be unique")
        return value


RemoteMcpProfile = Literal["user", "admin", "operator"]
RemoteMcpContract = Literal[
    "clio-kit-jarvis-user-v3.7.1",
    "clio-kit-jarvis-user-v3.6",
    "clio-kit-spack-user-v2.3",
    "clio-kit-spack-user-v2.1",
    "clio-kit-spack-user-v2",
    "clio-kit-scientific-catalog-user-v1.1",
    "clio-kit-scientific-catalog-user-v1",
]


def _validated_cluster_label(value: str, *, field: str) -> str:
    """Return a visible logical cluster label without changing its identity."""
    if value != value.strip() or any(
        ord(character) < 32 or ord(character) == 127 for character in value
    ):
        raise ValueError(f"{field} must not contain surrounding whitespace or controls")
    return value


class RemoteMcpServerConfig(BaseModel):
    """A remote stdio MCP server registered for one cluster.

    Registration is intentionally deny-by-default: discovery may cache every
    schema returned by the server, but no virtual tool is exposed until its
    remote name is present in ``allow_tools`` (or the operator explicitly uses
    ``["*"]``). Profiles control which local MCP surfaces may expose the
    allowlisted tools.
    """

    model_config = ConfigDict(extra="forbid")

    command: str = Field(max_length=MAX_REMOTE_MCP_ARGUMENT_BYTES)
    args: list[str] = Field(default_factory=list, max_length=MAX_REMOTE_MCP_ARGS)
    env_from: dict[str, str] = Field(default_factory=dict, max_length=MAX_REMOTE_MCP_ENV_REFS)
    namespace: str | None = Field(default=None, max_length=256)
    contract: RemoteMcpContract | None = None
    allow_tools: list[str] = Field(default_factory=list, max_length=MAX_REMOTE_MCP_ALLOW_TOOLS)
    profiles: list[RemoteMcpProfile] = Field(default_factory=lambda: ["admin"], max_length=3)
    schema_cache_ttl_seconds: int = Field(
        default=86_400,
        ge=1,
        le=MAX_REMOTE_MCP_SCHEMA_CACHE_TTL_SECONDS,
    )
    call_timeout_seconds: int = Field(default=300, ge=1, le=86_400)
    allow_mutable_artifact: bool = False
    enabled: bool = True

    @field_validator("command")
    @classmethod
    def _command_must_not_be_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("remote MCP command must not be blank")
        return value

    @field_validator("namespace")
    @classmethod
    def _namespace_must_not_be_blank(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("remote MCP namespace must not be blank")
        return value

    @field_validator("args")
    @classmethod
    def _args_must_not_embed_secrets(cls, value: list[str]) -> list[str]:
        for item in value:
            if len(item.encode("utf-8")) > MAX_REMOTE_MCP_ARGUMENT_BYTES:
                raise ValueError("remote MCP args entries exceed the byte limit")
            option = item.split("=", 1)[0].lower().replace("_", "-")
            sensitive = any(name in option for name in ("token", "secret", "password", "api-key"))
            environment_reference = "env" in option
            if sensitive and not environment_reference:
                raise ValueError(
                    "remote MCP args must not persist secret values; use env_from references"
                )
        return value

    @field_validator("env_from")
    @classmethod
    def _validate_env_from(cls, value: dict[str, str]) -> dict[str, str]:
        return validate_mcp_env_from(value)

    @field_validator("allow_tools")
    @classmethod
    def _validate_allow_tools(cls, value: list[str]) -> list[str]:
        if any(not item.strip() for item in value):
            raise ValueError("remote MCP allow_tools entries must not be blank")
        if any("*" in item and item != "*" for item in value):
            raise ValueError("remote MCP allow_tools supports exact names or '*' only")
        if len(value) != len(set(value)):
            raise ValueError("remote MCP allow_tools entries must be unique")
        return value

    @field_validator("profiles")
    @classmethod
    def _validate_profiles(cls, value: list[RemoteMcpProfile]) -> list[RemoteMcpProfile]:
        if not value:
            raise ValueError("remote MCP profiles must not be empty")
        if len(value) != len(set(value)):
            raise ValueError("remote MCP profiles must be unique")
        return value

    def allows_tool(self, tool_name: str) -> bool:
        """Return whether an operator explicitly allowlisted a remote tool."""
        return "*" in self.allow_tools or tool_name in self.allow_tools

    @model_validator(mode="after")
    def _mutable_artifact_must_not_reach_user_profile(self) -> RemoteMcpServerConfig:
        if self.allow_mutable_artifact and "user" in self.profiles:
            raise ValueError(
                "mutable remote MCP artifacts cannot be exposed through the user profile"
            )
        return self


class WorkerCapacityPolicy(BaseModel):
    """Persisted capacity policy for one managed cluster worker service.

    ``concurrency`` is the total number of worker slots. The control-query
    capacity is carved out of that total so a long-lived workload cannot make
    its own live status and binding queries impossible.
    """

    model_config = ConfigDict(extra="forbid")

    concurrency: int = Field(default=3, ge=2, strict=True)
    control_query_concurrency: int = Field(default=1, ge=1, strict=True)
    kind_concurrency: dict[JobKind, int] = Field(
        default_factory=dict[JobKind, int],
        max_length=len(JobKind),
    )

    @field_validator("kind_concurrency", mode="before")
    @classmethod
    def _validate_kind_concurrency(cls, value: object) -> object:
        if not isinstance(value, dict):
            raise ValueError("worker kind concurrency must be an object")
        normalized: dict[JobKind, int] = {}
        for raw_kind, raw_limit in cast(dict[object, object], value).items():
            if not isinstance(raw_kind, str):
                raise ValueError("worker job kind keys must be strings")
            try:
                kind = JobKind(raw_kind)
            except ValueError as exc:
                expected = ", ".join(kind.value for kind in JobKind)
                raise ValueError(
                    f"unknown worker job kind {raw_kind!r}; expected one of {expected}"
                ) from exc
            if type(raw_limit) is not int or raw_limit < 1:
                raise ValueError(
                    f"worker concurrency limit for {kind.value} must be an integer at least 1"
                )
            normalized[kind] = raw_limit
        return normalized

    @model_validator(mode="after")
    def _reserve_a_workload_slot(self) -> WorkerCapacityPolicy:
        if self.control_query_concurrency >= self.concurrency:
            raise ValueError("worker control_query_concurrency must be less than total concurrency")
        return self


class ClusterDefinition(BaseModel):
    """A locally configured cluster target."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=256)
    ssh_host: str = Field(min_length=1, max_length=1_024)
    bootstrap_profile: str = "linux-user"
    core_dir: str = "$HOME/.local/share/clio-relay/core"
    spool_dir: str = "$HOME/.local/share/clio-relay/spool"
    relay_executable: str = "$HOME/.local/bin/clio-relay"
    relay_install_receipt: str | None = None
    # clio-relay#211: downgrades this cluster's install/identity/receipt/sha
    # verification chain to typed warnings instead of raising. CLIO_RELAY_DEV_MODE=1
    # (environment) is equivalent; either is sufficient. See dev_mode.py --
    # checks protecting live state (writer proof, worker-lifetime lock, storage
    # admission, teardown scoping) and physical target identity never consult
    # this flag and stay hard regardless.
    dev_mode: bool = Field(default=False, strict=True)
    jarvis_bin: str | None = None
    jarvis_resource_graph_profile: str | None = None
    allow_jarvis_resource_graph_build: bool = Field(default=False, strict=True)
    spack_executable: str | None = None
    frpc_bin: str | None = None
    agent_bin: str | None = None
    agent_adapter: str = "exec"
    agent_npm_package: str | None = None
    agent_npm_bin: str | None = None
    agent_args: list[str] = Field(default_factory=list)
    scheduler_provider: str = "external"
    worker_capacity: WorkerCapacityPolicy = Field(default_factory=WorkerCapacityPolicy)
    remote_mcp_servers: dict[str, RemoteMcpServerConfig] = Field(
        default_factory=dict,
        max_length=MAX_REMOTE_MCP_SERVERS_PER_CLUSTER,
    )
    frp_transport: FrpTransportConfig = Field(default_factory=FrpTransportConfig)
    live_test: LiveTestConfig = Field(default_factory=LiveTestConfig)
    target_identity: ClusterTargetIdentity | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        return _validated_cluster_label(value, field="cluster name")

    @field_validator("ssh_host")
    @classmethod
    def _validate_ssh_host(cls, value: str) -> str:
        if (
            value != value.strip()
            or value.startswith("-")
            or any(
                character.isspace() or ord(character) < 32 or ord(character) == 127
                for character in value
            )
        ):
            raise ValueError(
                "ssh_host must be one non-option SSH destination without whitespace or controls"
            )
        return value

    @field_validator("core_dir", "spool_dir")
    @classmethod
    def _validate_remote_data_path(cls, value: str, info: ValidationInfo) -> str:
        try:
            validate_remote_path(value, field=info.field_name or "remote data path")
        except ConfigurationError as error:
            raise ValueError(str(error)) from error
        return value

    @field_validator("remote_mcp_servers")
    @classmethod
    def _remote_mcp_names_must_not_be_blank(
        cls, value: dict[str, RemoteMcpServerConfig]
    ) -> dict[str, RemoteMcpServerConfig]:
        if any(not name.strip() for name in value):
            raise ValueError("remote MCP server registration names must not be blank")
        if any(len(name) > 256 for name in value):
            raise ValueError("remote MCP server registration names must not exceed 256 characters")
        return value

    @field_validator("scheduler_provider")
    @classmethod
    def _validate_scheduler_provider(cls, value: str) -> str:
        normalized = value.strip().lower().replace("_", "-")
        if normalized in {"none", "unmanaged"}:
            return "external"
        if (
            not normalized
            or not normalized[0].isalpha()
            or not all(item.isalnum() or item == "-" for item in normalized)
        ):
            raise ValueError("scheduler_provider must be a lowercase provider identifier")
        return normalized

    @field_validator("jarvis_resource_graph_profile")
    @classmethod
    def _validate_jarvis_resource_graph_profile(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if (
            not value
            or value != value.strip()
            or len(value) > 256
            or value in {".", ".."}
            or "/" in value
            or "\\" in value
            or any(ord(character) < 32 or ord(character) == 127 for character in value)
        ):
            raise ValueError(
                "jarvis_resource_graph_profile must be one safe exact JARVIS profile name"
            )
        return value

    @field_validator("spack_executable")
    @classmethod
    def _validate_spack_executable(cls, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            validate_remote_path(value, field="spack_executable")
        except ConfigurationError as error:
            raise ValueError(
                "spack_executable must be one absolute remote path or start with $HOME/"
            ) from error
        if value != value.strip() or ".." in PurePosixPath(value).parts:
            raise ValueError(
                "spack_executable must be one absolute remote path or start with $HOME/"
            )
        return value

    @field_validator("relay_executable", "relay_install_receipt")
    @classmethod
    def _validate_relay_installation_path(cls, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            validate_remote_path(value, field="relay installation path")
        except ConfigurationError as error:
            raise ValueError(
                "relay installation paths must be absolute or start with $HOME/"
            ) from error
        if value != value.strip() or ".." in PurePosixPath(value).parts:
            raise ValueError("relay installation paths must be absolute or start with $HOME/")
        return value

    @model_validator(mode="after")
    def _remote_mcp_must_not_reference_transport_credentials(self) -> ClusterDefinition:
        if self.allow_jarvis_resource_graph_build and self.jarvis_resource_graph_profile is None:
            raise ValueError(
                "allow_jarvis_resource_graph_build requires jarvis_resource_graph_profile"
            )
        forbidden = {
            self.frp_transport.token_env,
            self.frp_transport.stcp_secret_env,
        }
        for server_name, registration in self.remote_mcp_servers.items():
            referenced = forbidden.intersection(
                {*registration.env_from.keys(), *registration.env_from.values()}
            )
            if referenced:
                credential = sorted(referenced)[0]
                raise ValueError(
                    f"remote MCP server {server_name} cannot expose relay transport "
                    f"credential {credential}"
                )
        return self
