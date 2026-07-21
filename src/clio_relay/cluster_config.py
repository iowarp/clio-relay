"""Local cluster registry for relay targets."""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import stat
import time
from collections.abc import Callable
from importlib import import_module
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Literal, cast
from uuid import uuid4

from filelock import FileLock
from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.models import JobKind, validate_mcp_env_from
from clio_relay.remote_values import validate_remote_path

CLUSTER_REGISTRY_ENV = "CLIO_RELAY_CLUSTER_REGISTRY"
MAX_CLUSTER_REGISTRY_BYTES = 4 * 1024 * 1024
MAX_CONFIG_READ_ATTEMPTS = 25
CONFIG_READ_RETRY_SECONDS = 0.02
MAX_CONFIGURED_CLUSTERS = 512
MAX_REMOTE_MCP_SERVERS_PER_CLUSTER = 256
MAX_REMOTE_MCP_REGISTRATIONS = 1_024
MAX_REMOTE_MCP_ARGS = 256
MAX_REMOTE_MCP_ENV_REFS = 256
MAX_REMOTE_MCP_ALLOW_TOOLS = 2_048
MAX_REMOTE_MCP_ARGUMENT_BYTES = 4_096
MAX_REMOTE_MCP_SCHEMA_CACHE_TTL_SECONDS = 31_536_000
CONFIG_REPLACE_ATTEMPTS = 25

_WINDOWS_READ_CONTROL = 0x00020000
_WINDOWS_WRITE_DAC = 0x00040000
_WINDOWS_WRITE_OWNER = 0x00080000
_WINDOWS_GENERIC_READ = 0x80000000
_WINDOWS_GENERIC_WRITE = 0x40000000
_WINDOWS_DELETE = 0x00010000
_WINDOWS_FILE_SHARE_READ = 0x00000001
_WINDOWS_CREATE_NEW = 1
_WINDOWS_OPEN_EXISTING = 3
_WINDOWS_FILE_ATTRIBUTE_NORMAL = 0x00000080
_WINDOWS_FILE_ATTRIBUTE_DIRECTORY = 0x00000010
_WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
_WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
_WINDOWS_FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
_WINDOWS_FILE_FLAG_DELETE_ON_CLOSE = 0x04000000
_WINDOWS_SE_FILE_OBJECT = 1
_WINDOWS_OWNER_SECURITY_INFORMATION = 0x00000001
_WINDOWS_DACL_SECURITY_INFORMATION = 0x00000004
_WINDOWS_PROTECTED_DACL_SECURITY_INFORMATION = 0x80000000
_WINDOWS_SE_DACL_PROTECTED = 0x1000
_WINDOWS_ACL_SIZE_INFORMATION = 2
_WINDOWS_ACCESS_ALLOWED_ACE_TYPE = 0
_WINDOWS_FILE_ALL_ACCESS = 0x001F01FF
_WINDOWS_OBJECT_INHERIT_ACE = 0x01
_WINDOWS_CONTAINER_INHERIT_ACE = 0x02
_WINDOWS_PRIVATE_SIDS = {"S-1-3-4", "S-1-5-18", "S-1-5-32-544"}
_WINDOWS_TOKEN_QUERY = 0x0008
_WINDOWS_TOKEN_USER = 1
_WINDOWS_TOKEN_OWNER = 4
_WINDOWS_ERROR_ALREADY_EXISTS = 183


class _WindowsFileTime(ctypes.Structure):
    _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]


class _WindowsFileInformation(ctypes.Structure):
    _fields_ = [
        ("attributes", ctypes.c_uint32),
        ("creation_time", _WindowsFileTime),
        ("last_access_time", _WindowsFileTime),
        ("last_write_time", _WindowsFileTime),
        ("volume_serial_number", ctypes.c_uint32),
        ("file_size_high", ctypes.c_uint32),
        ("file_size_low", ctypes.c_uint32),
        ("number_of_links", ctypes.c_uint32),
        ("file_index_high", ctypes.c_uint32),
        ("file_index_low", ctypes.c_uint32),
    ]


class _WindowsAclSizeInformation(ctypes.Structure):
    _fields_ = [
        ("ace_count", ctypes.c_uint32),
        ("acl_bytes_in_use", ctypes.c_uint32),
        ("acl_bytes_free", ctypes.c_uint32),
    ]


class _WindowsAceHeader(ctypes.Structure):
    _fields_ = [
        ("ace_type", ctypes.c_ubyte),
        ("ace_flags", ctypes.c_ubyte),
        ("ace_size", ctypes.c_uint16),
    ]


class _WindowsAccessAllowedAce(ctypes.Structure):
    _fields_ = [
        ("header", _WindowsAceHeader),
        ("mask", ctypes.c_uint32),
        ("sid_start", ctypes.c_uint32),
    ]


class _WindowsSidAndAttributes(ctypes.Structure):
    _fields_ = [("sid", ctypes.c_void_p), ("attributes", ctypes.c_uint32)]


class _WindowsTokenUser(ctypes.Structure):
    _fields_ = [("user", _WindowsSidAndAttributes)]


class _WindowsTokenOwner(ctypes.Structure):
    _fields_ = [("owner", ctypes.c_void_p)]


class _WindowsSecurityAttributes(ctypes.Structure):
    _fields_ = [
        ("length", ctypes.c_uint32),
        ("security_descriptor", ctypes.c_void_p),
        ("inherit_handle", ctypes.c_int),
    ]


def _load_windows_library(name: str) -> Any:
    """Load a Win32 library without exposing platform-specific ctypes stubs."""
    factory = cast(Callable[..., Any], vars(ctypes)["WinDLL"])
    return factory(name, use_last_error=True)


def _windows_last_error() -> int:
    """Return the calling thread's Win32 last-error value."""
    get_last_error = cast(Callable[[], int], vars(ctypes)["get_last_error"])
    return get_last_error()


def _windows_error(error: int) -> OSError:
    """Build the native Python exception for a Win32 error code."""
    factory = cast(Callable[[int], OSError], vars(ctypes)["WinError"])
    return factory(error)


def _windows_os_file_handle(descriptor: int) -> int:
    """Return the Win32 handle owned by a CRT file descriptor."""
    module = import_module("msvcrt")
    get_osfhandle = cast(Callable[[int], int], vars(module)["get_osfhandle"])
    return get_osfhandle(descriptor)


def _open_windows_os_file_handle(handle: int, flags: int) -> int:
    """Transfer ownership of a Win32 handle to a CRT file descriptor."""
    module = import_module("msvcrt")
    open_osfhandle = cast(Callable[[int, int], int], vars(module)["open_osfhandle"])
    return open_osfhandle(handle, flags)


class _ConfigurationChangedError(ConfigurationError):
    """Transient configuration identity/version change during a stable read."""


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


def cluster_route_revision(definition: ClusterDefinition) -> str:
    """Return a stable digest for fields that determine durable queue routing.

    Remote MCP registrations and worker scheduling capacity can change without
    changing the SSH destination or queue location of an existing job handle.
    """
    payload = definition.model_dump(
        mode="json",
        exclude={"remote_mcp_servers", "worker_capacity"},
    )
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


class ClusterRegistry(BaseModel):
    """Configured cluster targets."""

    model_config = ConfigDict(extra="forbid")

    clusters: dict[str, ClusterDefinition] = Field(
        default_factory=dict,
        max_length=MAX_CONFIGURED_CLUSTERS,
    )

    @field_validator("clusters")
    @classmethod
    def _cluster_keys_match_bounded_definitions(
        cls,
        value: dict[str, ClusterDefinition],
    ) -> dict[str, ClusterDefinition]:
        for name in value:
            _validated_cluster_label(name, field="cluster registry key")
            if len(name) > 256:
                raise ValueError("cluster registry keys must not exceed 256 characters")
        mismatches = sorted(name for name, definition in value.items() if definition.name != name)
        if mismatches:
            raise ValueError(
                "cluster registry keys must match ClusterDefinition.name: " + ", ".join(mismatches)
            )
        return value

    @model_validator(mode="after")
    def _remote_mcp_registration_count_is_bounded(self) -> ClusterRegistry:
        registration_count = sum(
            len(cluster.remote_mcp_servers) for cluster in self.clusters.values()
        )
        if registration_count > MAX_REMOTE_MCP_REGISTRATIONS:
            raise ValueError(
                "cluster registry contains more than "
                f"{MAX_REMOTE_MCP_REGISTRATIONS} remote MCP registrations"
            )
        return self

    @classmethod
    def default(cls) -> ClusterRegistry:
        """Return an empty registry for explicit local cluster definitions."""
        return cls()

    @classmethod
    def load(cls, path: Path) -> ClusterRegistry:
        """Load a registry from disk, creating defaults if the file is absent."""
        ensure_private_configuration_directory(path.parent)
        if not path.exists():
            with FileLock(f"{path}.lock"):
                if not path.exists():
                    cls.default()._write_atomic_unlocked(path)
        return cls.model_validate_json(
            read_bounded_configuration_bytes(path, max_bytes=MAX_CLUSTER_REGISTRY_BYTES)
        )

    def save(self, path: Path) -> None:
        """Persist the registry with locking, atomic replacement, and fsync."""
        ensure_private_configuration_directory(path.parent)
        with FileLock(f"{path}.lock"):
            validated = type(self).model_validate(self.model_dump(mode="python"))
            validated._write_atomic_unlocked(path)

    @classmethod
    def mutate(
        cls,
        path: Path,
        mutation: Callable[[ClusterRegistry], None],
    ) -> ClusterRegistry:
        """Apply a read-modify-write operation under one registry lock."""
        ensure_private_configuration_directory(path.parent)
        with FileLock(f"{path}.lock"):
            registry = (
                cls.model_validate_json(
                    read_bounded_configuration_bytes(
                        path,
                        max_bytes=MAX_CLUSTER_REGISTRY_BYTES,
                    )
                )
                if path.exists()
                else cls.default()
            )
            mutation(registry)
            validated = cls.model_validate(registry.model_dump(mode="python"))
            validated._write_atomic_unlocked(path)
            return validated

    def _write_atomic_unlocked(self, path: Path) -> None:
        """Write an atomic registry replacement while the caller holds the lock."""
        payload = (json.dumps(self.model_dump(), indent=2) + "\n").encode("utf-8")
        if len(payload) > MAX_CLUSTER_REGISTRY_BYTES:
            raise ConfigurationError(f"cluster registry exceeds {MAX_CLUSTER_REGISTRY_BYTES} bytes")
        temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid4().hex}.tmp")
        try:
            with open_private_atomic_file(temporary) as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            for attempt in range(CONFIG_REPLACE_ATTEMPTS):
                try:
                    os.replace(temporary, path)
                    break
                except PermissionError:
                    if attempt + 1 >= CONFIG_REPLACE_ATTEMPTS:
                        raise
                    time.sleep(CONFIG_READ_RETRY_SECONDS)
            _fsync_directory(path.parent)
        finally:
            temporary.unlink(missing_ok=True)

    def require(self, name: str) -> ClusterDefinition:
        """Return a configured cluster or raise a configuration error."""
        try:
            return self.clusters[name]
        except KeyError as exc:
            raise ConfigurationError(f"cluster is not configured: {name}") from exc


def _fsync_directory(path: Path) -> None:
    """Best-effort fsync of a directory after an atomic replacement."""
    if os.name == "nt":
        return
    directory_fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def read_bounded_configuration_bytes(path: Path, *, max_bytes: int) -> bytes:
    """Read one stable regular configuration file without following links."""
    if max_bytes < 1:
        raise ValueError("configuration byte limit must be positive")
    ensure_private_configuration_path(path.parent, directory=True)
    initial = os.lstat(path)
    _require_safe_configuration_stat(path, initial, max_bytes=max_bytes)
    ensure_private_configuration_path(path, directory=False)
    last_error: OSError | _ConfigurationChangedError | None = None
    for attempt in range(MAX_CONFIG_READ_ATTEMPTS):
        try:
            before = os.lstat(path)
            _require_safe_configuration_stat(path, before, max_bytes=max_bytes)
            flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(path, flags)
            try:
                opened = os.fstat(descriptor)
                _require_safe_configuration_stat(path, opened, max_bytes=max_bytes)
                if _stat_version(before) != _stat_version(opened):
                    raise _ConfigurationChangedError(
                        f"configuration identity changed during open: {path}"
                    )
                with os.fdopen(descriptor, "rb", closefd=False) as stream:
                    payload = stream.read(max_bytes + 1)
                    stream.seek(0)
                    confirmed_payload = stream.read(max_bytes + 1)
                if len(payload) > max_bytes:
                    raise ConfigurationError(
                        f"configuration file exceeds {max_bytes} bytes: {path}"
                    )
                final = os.fstat(descriptor)
                after = os.lstat(path)
                if (
                    payload != confirmed_payload
                    or _stat_version(opened) != _stat_version(final)
                    or _stat_version(final) != _stat_version(after)
                    or final.st_size != len(payload)
                ):
                    raise _ConfigurationChangedError(f"configuration changed during read: {path}")
                return payload.removeprefix(b"\xef\xbb\xbf")
            finally:
                os.close(descriptor)
        except (OSError, _ConfigurationChangedError) as exc:
            last_error = exc
            if attempt + 1 >= MAX_CONFIG_READ_ATTEMPTS:
                break
            time.sleep(CONFIG_READ_RETRY_SECONDS)
    if last_error is not None:
        raise ConfigurationError(
            f"cannot read configuration file {path}: {last_error}"
        ) from last_error
    raise ConfigurationError(f"cannot read configuration file: {path}")


def _require_safe_configuration_stat(path: Path, value: os.stat_result, *, max_bytes: int) -> None:
    if not stat.S_ISREG(value.st_mode) or _is_reparse_stat(value):
        raise ConfigurationError(f"configuration path is not a regular owned file: {path}")
    if os.name != "nt" and hasattr(os, "getuid"):
        if value.st_uid != os.getuid():
            raise ConfigurationError(f"configuration path is not owned by this user: {path}")
        if stat.S_IMODE(value.st_mode) & 0o022:
            raise ConfigurationError(
                f"configuration path is writable by group or other users: {path}"
            )
    if value.st_size > max_bytes:
        raise ConfigurationError(f"configuration file exceeds {max_bytes} bytes: {path}")


def _is_reparse_stat(value: os.stat_result) -> bool:
    attributes = getattr(value, "st_file_attributes", 0)
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def _stat_version(value: os.stat_result) -> tuple[int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
    )


def open_private_atomic_file(path: Path) -> BinaryIO:
    """Create a new private regular file for an eventual atomic replacement."""
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    descriptor = (
        _create_private_windows_atomic_descriptor(path)
        if os.name == "nt"
        else os.open(path, flags, 0o600)
    )
    try:
        if os.name == "nt":
            _set_private_windows_acl(
                path,
                directory=False,
                existing_handle=ctypes.c_void_p(_windows_os_file_handle(descriptor)),
            )
        else:
            ensure_private_configuration_path(path, directory=False)
        return os.fdopen(descriptor, "wb")
    except BaseException:
        os.close(descriptor)
        raise


def _create_private_windows_atomic_descriptor(path: Path) -> int:
    kernel32 = _load_windows_library("kernel32")
    advapi32 = _load_windows_library("advapi32")
    owner_sid = _current_windows_user_sid(
        advapi32=advapi32,
        kernel32=kernel32,
        path=path,
    )
    security_descriptor = _build_private_windows_security_descriptor(
        directory=False,
        advapi32=advapi32,
        owner_sid=owner_sid,
        path=path,
    )
    security_attributes = _WindowsSecurityAttributes(
        length=ctypes.sizeof(_WindowsSecurityAttributes),
        security_descriptor=security_descriptor,
        inherit_handle=0,
    )
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.POINTER(_WindowsSecurityAttributes),
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    ]
    create_file.restype = ctypes.c_void_p
    create_error = 0
    try:
        raw_handle = create_file(
            str(internal_filesystem_path(path, force_extended=True)),
            _WINDOWS_GENERIC_WRITE
            | _WINDOWS_READ_CONTROL
            | _WINDOWS_WRITE_DAC
            | _WINDOWS_WRITE_OWNER,
            0,
            ctypes.byref(security_attributes),
            _WINDOWS_CREATE_NEW,
            _WINDOWS_FILE_ATTRIBUTE_NORMAL,
            None,
        )
        if raw_handle in (None, ctypes.c_void_p(-1).value):
            create_error = _windows_last_error()
    finally:
        _free_windows_local(security_descriptor, kernel32=kernel32)
    if raw_handle in (None, ctypes.c_void_p(-1).value):
        raise _windows_error(create_error)
    try:
        descriptor_flags = os.O_WRONLY | getattr(os, "O_BINARY", 0)
        return _open_windows_os_file_handle(cast(int, raw_handle), descriptor_flags)
    except BaseException:
        _close_windows_handle(ctypes.c_void_p(raw_handle), kernel32=kernel32)
        raise


def _build_private_windows_security_descriptor(
    *,
    directory: bool,
    advapi32: Any,
    owner_sid: str,
    path: Path,
) -> ctypes.c_void_p:
    sddl = (
        f"O:{owner_sid}D:P(A;OICI;FA;;;OW)(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)"
        if directory
        else f"O:{owner_sid}D:P(A;;FA;;;OW)(A;;FA;;;SY)(A;;FA;;;BA)"
    )
    descriptor = ctypes.c_void_p()
    convert = advapi32.ConvertStringSecurityDescriptorToSecurityDescriptorW
    convert.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(ctypes.c_uint32),
    ]
    convert.restype = ctypes.c_int
    if not convert(sddl, 1, ctypes.byref(descriptor), None):
        error = _windows_last_error()
        raise ConfigurationError(f"could not build private Windows ACL ({error}): {path}")
    return descriptor


def _free_windows_local(pointer: ctypes.c_void_p, *, kernel32: Any) -> None:
    local_free = kernel32.LocalFree
    local_free.argtypes = [ctypes.c_void_p]
    local_free.restype = ctypes.c_void_p
    local_free(pointer)


def ensure_private_configuration_directory(path: Path) -> None:
    """Create a configuration directory privately, then verify its exact protections."""
    if os.name != "nt":
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        ensure_private_configuration_path(path, directory=True)
        return
    missing: list[Path] = []
    current = path
    while not current.exists():
        missing.append(current)
        parent = current.parent
        if parent == current:
            raise ConfigurationError(f"configuration directory has no existing parent: {path}")
        current = parent
    if not missing:
        ensure_private_configuration_path(path, directory=True)
        return

    kernel32 = _load_windows_library("kernel32")
    held_handles: list[ctypes.c_void_p] = []
    try:
        for directory in reversed(missing):
            _create_private_windows_directory(directory)
            handle = _open_windows_configuration_handle(
                directory,
                directory=True,
                kernel32=kernel32,
                write_owner=True,
            )
            try:
                _set_private_windows_acl(
                    directory,
                    directory=True,
                    existing_handle=handle,
                )
            except BaseException:
                _close_windows_handle(handle, kernel32=kernel32)
                raise
            held_handles.append(handle)
    finally:
        for handle in reversed(held_handles):
            _close_windows_handle(handle, kernel32=kernel32)


def create_private_configuration_directory(path: Path) -> None:
    """Create exactly one owner-private directory without accepting an existing path."""
    if os.name != "nt":
        os.mkdir(path, 0o700)
        ensure_private_configuration_path(path, directory=True)
        return
    _create_private_windows_directory(path, exist_ok=False)
    ensure_private_configuration_path(path, directory=True)


def _create_private_windows_directory(path: Path, *, exist_ok: bool = True) -> None:
    kernel32 = _load_windows_library("kernel32")
    advapi32 = _load_windows_library("advapi32")
    owner_sid = _current_windows_user_sid(
        advapi32=advapi32,
        kernel32=kernel32,
        path=path,
    )
    security_descriptor = _build_private_windows_security_descriptor(
        directory=True,
        advapi32=advapi32,
        owner_sid=owner_sid,
        path=path,
    )
    security_attributes = _WindowsSecurityAttributes(
        length=ctypes.sizeof(_WindowsSecurityAttributes),
        security_descriptor=security_descriptor,
        inherit_handle=0,
    )
    create_directory = kernel32.CreateDirectoryW
    create_directory.argtypes = [
        ctypes.c_wchar_p,
        ctypes.POINTER(_WindowsSecurityAttributes),
    ]
    create_directory.restype = ctypes.c_int
    try:
        storage_path = internal_filesystem_path(path, force_extended=True)
        created = create_directory(str(storage_path), ctypes.byref(security_attributes))
        if not created:
            error = _windows_last_error()
            if error != _WINDOWS_ERROR_ALREADY_EXISTS or not exist_ok:
                raise ConfigurationError(
                    f"could not create private Windows configuration directory ({error}): {path}"
                )
    finally:
        _free_windows_local(security_descriptor, kernel32=kernel32)


def _open_windows_configuration_handle(
    path: Path,
    *,
    directory: bool,
    kernel32: Any,
    write_owner: bool = False,
) -> ctypes.c_void_p:
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    ]
    create_file.restype = ctypes.c_void_p
    flags = _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT
    if directory:
        flags |= _WINDOWS_FILE_FLAG_BACKUP_SEMANTICS
    desired_access = _WINDOWS_GENERIC_READ | _WINDOWS_READ_CONTROL | _WINDOWS_WRITE_DAC
    if write_owner:
        desired_access |= _WINDOWS_WRITE_OWNER
    raw_handle = create_file(
        str(internal_filesystem_path(path, force_extended=True)),
        desired_access,
        _WINDOWS_FILE_SHARE_READ,
        None,
        _WINDOWS_OPEN_EXISTING,
        flags,
        None,
    )
    if raw_handle in (None, ctypes.c_void_p(-1).value):
        error = _windows_last_error()
        raise ConfigurationError(f"could not open Windows configuration path ({error}): {path}")
    handle = ctypes.c_void_p(raw_handle)
    try:
        _validate_windows_configuration_handle(
            handle,
            directory=directory,
            kernel32=kernel32,
            path=path,
        )
    except BaseException:
        _close_windows_handle(handle, kernel32=kernel32)
        raise
    return handle


def _validate_windows_configuration_handle(
    handle: ctypes.c_void_p,
    *,
    directory: bool,
    kernel32: Any,
    path: Path,
) -> None:
    get_information = kernel32.GetFileInformationByHandle
    get_information.argtypes = [ctypes.c_void_p, ctypes.POINTER(_WindowsFileInformation)]
    get_information.restype = ctypes.c_int
    information = _WindowsFileInformation()
    if not get_information(handle, ctypes.byref(information)):
        error = _windows_last_error()
        raise ConfigurationError(f"could not inspect Windows configuration path ({error}): {path}")
    is_directory = bool(information.attributes & _WINDOWS_FILE_ATTRIBUTE_DIRECTORY)
    is_reparse_point = bool(information.attributes & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT)
    if is_directory != directory or is_reparse_point:
        kind = "directory" if directory else "file"
        raise ConfigurationError(f"configuration path is not a regular {kind}: {path}")


def _close_windows_handle(handle: ctypes.c_void_p, *, kernel32: Any) -> None:
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [ctypes.c_void_p]
    close_handle.restype = ctypes.c_int
    close_handle(handle)


def _windows_sid_text(
    sid_pointer: ctypes.c_void_p,
    *,
    advapi32: Any,
    kernel32: Any,
    path: Path,
    context: str,
) -> str:
    sid_to_text = advapi32.ConvertSidToStringSidW
    sid_to_text.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
    sid_to_text.restype = ctypes.c_int
    sid_text_pointer = ctypes.c_void_p()
    if not sid_to_text(sid_pointer, ctypes.byref(sid_text_pointer)):
        error = _windows_last_error()
        raise ConfigurationError(f"could not inspect Windows {context} SID ({error}): {path}")
    try:
        return ctypes.wstring_at(sid_text_pointer)
    finally:
        local_free = kernel32.LocalFree
        local_free.argtypes = [ctypes.c_void_p]
        local_free.restype = ctypes.c_void_p
        local_free(sid_text_pointer)


def _current_windows_token_sid(
    *,
    information_class: int,
    minimum_size: int,
    context: str,
    advapi32: Any,
    kernel32: Any,
    path: Path,
) -> str:
    get_current_process = kernel32.GetCurrentProcess
    get_current_process.argtypes = []
    get_current_process.restype = ctypes.c_void_p
    open_process_token = advapi32.OpenProcessToken
    open_process_token.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    open_process_token.restype = ctypes.c_int
    token = ctypes.c_void_p()
    if not open_process_token(
        get_current_process(),
        _WINDOWS_TOKEN_QUERY,
        ctypes.byref(token),
    ):
        error = _windows_last_error()
        raise ConfigurationError(f"could not inspect current Windows user ({error}): {path}")
    try:
        get_token_information = advapi32.GetTokenInformation
        get_token_information.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.POINTER(ctypes.c_uint32),
        ]
        get_token_information.restype = ctypes.c_int
        required = ctypes.c_uint32()
        get_token_information(
            token,
            information_class,
            None,
            0,
            ctypes.byref(required),
        )
        if required.value < minimum_size:
            error = _windows_last_error()
            raise ConfigurationError(
                f"could not size current Windows {context} identity ({error}): {path}"
            )
        buffer = ctypes.create_string_buffer(required.value)
        if not get_token_information(
            token,
            information_class,
            buffer,
            required.value,
            ctypes.byref(required),
        ):
            error = _windows_last_error()
            raise ConfigurationError(f"could not read current Windows {context} ({error}): {path}")
        sid = ctypes.cast(buffer, ctypes.POINTER(ctypes.c_void_p)).contents
        if sid.value is None:
            raise ConfigurationError(f"current Windows {context} has no SID: {path}")
        return _windows_sid_text(
            sid,
            advapi32=advapi32,
            kernel32=kernel32,
            path=path,
            context=f"current {context}",
        )
    finally:
        _close_windows_handle(token, kernel32=kernel32)


def _current_windows_user_sid(*, advapi32: Any, kernel32: Any, path: Path) -> str:
    return _current_windows_token_sid(
        information_class=_WINDOWS_TOKEN_USER,
        minimum_size=ctypes.sizeof(_WindowsTokenUser),
        context="user",
        advapi32=advapi32,
        kernel32=kernel32,
        path=path,
    )


def _current_windows_default_owner_sid(*, advapi32: Any, kernel32: Any, path: Path) -> str:
    return _current_windows_token_sid(
        information_class=_WINDOWS_TOKEN_OWNER,
        minimum_size=ctypes.sizeof(_WindowsTokenOwner),
        context="default owner",
        advapi32=advapi32,
        kernel32=kernel32,
        path=path,
    )


def _windows_object_owner_sid(
    handle: ctypes.c_void_p,
    *,
    advapi32: Any,
    kernel32: Any,
    path: Path,
) -> str:
    get_security = advapi32.GetSecurityInfo
    get_security.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    get_security.restype = ctypes.c_uint32
    owner = ctypes.c_void_p()
    descriptor = ctypes.c_void_p()
    result = get_security(
        handle,
        _WINDOWS_SE_FILE_OBJECT,
        _WINDOWS_OWNER_SECURITY_INFORMATION,
        ctypes.byref(owner),
        None,
        None,
        None,
        ctypes.byref(descriptor),
    )
    if result != 0:
        raise ConfigurationError(
            f"could not inspect Windows configuration owner ({result}): {path}"
        )
    try:
        if owner.value is None:
            raise ConfigurationError(f"Windows configuration path has no owner: {path}")
        return _windows_sid_text(
            owner,
            advapi32=advapi32,
            kernel32=kernel32,
            path=path,
            context="configuration owner",
        )
    finally:
        local_free = kernel32.LocalFree
        local_free.argtypes = [ctypes.c_void_p]
        local_free.restype = ctypes.c_void_p
        local_free(descriptor)


def _require_current_windows_owner(
    *,
    owner_sid: str,
    user_sid: str,
    default_owner_sid: str | None = None,
    path: Path,
) -> None:
    permitted_owner_sids = {user_sid}
    if default_owner_sid is not None:
        permitted_owner_sids.add(default_owner_sid)
    if owner_sid not in permitted_owner_sids:
        raise ConfigurationError(f"configuration path is not owned by this user: {path}")


def _windows_acl_entries(
    dacl: ctypes.c_void_p,
    *,
    advapi32: Any,
    kernel32: Any,
    path: Path,
) -> list[tuple[str, int, int]]:
    get_acl_information = advapi32.GetAclInformation
    get_acl_information.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(_WindowsAclSizeInformation),
        ctypes.c_uint32,
        ctypes.c_uint32,
    ]
    get_acl_information.restype = ctypes.c_int
    information = _WindowsAclSizeInformation()
    if not get_acl_information(
        dacl,
        ctypes.byref(information),
        ctypes.sizeof(information),
        _WINDOWS_ACL_SIZE_INFORMATION,
    ):
        error = _windows_last_error()
        raise ConfigurationError(f"could not inspect Windows configuration ACL ({error}): {path}")
    get_ace = advapi32.GetAce
    get_ace.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.POINTER(ctypes.c_void_p)]
    get_ace.restype = ctypes.c_int
    entries: list[tuple[str, int, int]] = []
    for index in range(information.ace_count):
        ace_pointer = ctypes.c_void_p()
        if not get_ace(dacl, index, ctypes.byref(ace_pointer)):
            error = _windows_last_error()
            raise ConfigurationError(
                f"could not inspect Windows configuration ACE ({error}): {path}"
            )
        ace = ctypes.cast(ace_pointer, ctypes.POINTER(_WindowsAccessAllowedAce)).contents
        if (
            ace.header.ace_type != _WINDOWS_ACCESS_ALLOWED_ACE_TYPE
            or ace.header.ace_size < ctypes.sizeof(_WindowsAccessAllowedAce)
        ):
            raise ConfigurationError(f"Windows configuration ACL has an unexpected ACE: {path}")
        sid_address = cast(int, ace_pointer.value) + _WindowsAccessAllowedAce.sid_start.offset
        sid = _windows_sid_text(
            ctypes.c_void_p(sid_address),
            advapi32=advapi32,
            kernel32=kernel32,
            path=path,
            context="configuration ACE",
        )
        entries.append((sid, ace.mask, ace.header.ace_flags))
    return entries


def _verify_private_windows_acl(
    handle: ctypes.c_void_p,
    *,
    directory: bool,
    expected_owner_sid: str,
    advapi32: Any,
    kernel32: Any,
    path: Path,
) -> None:
    get_security = advapi32.GetSecurityInfo
    get_security.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    get_security.restype = ctypes.c_uint32
    owner = ctypes.c_void_p()
    dacl = ctypes.c_void_p()
    descriptor = ctypes.c_void_p()
    result = get_security(
        handle,
        _WINDOWS_SE_FILE_OBJECT,
        _WINDOWS_OWNER_SECURITY_INFORMATION | _WINDOWS_DACL_SECURITY_INFORMATION,
        ctypes.byref(owner),
        None,
        ctypes.byref(dacl),
        None,
        ctypes.byref(descriptor),
    )
    if result != 0:
        raise ConfigurationError(f"could not read back private Windows ACL ({result}): {path}")
    try:
        if owner.value is None:
            raise ConfigurationError(f"Windows configuration path has no owner: {path}")
        owner_sid = _windows_sid_text(
            owner,
            advapi32=advapi32,
            kernel32=kernel32,
            path=path,
            context="configuration owner",
        )
        _require_current_windows_owner(
            owner_sid=owner_sid,
            user_sid=expected_owner_sid,
            path=path,
        )
        if dacl.value is None:
            raise ConfigurationError(f"Windows configuration path has no private DACL: {path}")
        get_control = advapi32.GetSecurityDescriptorControl
        get_control.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_uint16),
            ctypes.POINTER(ctypes.c_uint32),
        ]
        get_control.restype = ctypes.c_int
        control = ctypes.c_uint16()
        revision = ctypes.c_uint32()
        if not get_control(descriptor, ctypes.byref(control), ctypes.byref(revision)):
            error = _windows_last_error()
            raise ConfigurationError(
                f"could not verify Windows configuration ACL control ({error}): {path}"
            )
        if not control.value & _WINDOWS_SE_DACL_PROTECTED:
            raise ConfigurationError(f"Windows configuration ACL remains inherited: {path}")
        expected_flags = (
            _WINDOWS_OBJECT_INHERIT_ACE | _WINDOWS_CONTAINER_INHERIT_ACE if directory else 0
        )
        entries = _windows_acl_entries(
            dacl,
            advapi32=advapi32,
            kernel32=kernel32,
            path=path,
        )
        if (
            len(entries) != len(_WINDOWS_PRIVATE_SIDS)
            or {sid for sid, _mask, _flags in entries} != _WINDOWS_PRIVATE_SIDS
        ):
            raise ConfigurationError(f"Windows configuration ACL is not owner-private: {path}")
        if any(
            mask != _WINDOWS_FILE_ALL_ACCESS or flags != expected_flags
            for _sid, mask, flags in entries
        ):
            raise ConfigurationError(f"Windows configuration ACL grants unexpected access: {path}")
    finally:
        local_free = kernel32.LocalFree
        local_free.argtypes = [ctypes.c_void_p]
        local_free.restype = ctypes.c_void_p
        local_free(descriptor)


def _set_private_windows_acl(
    path: Path,
    *,
    directory: bool,
    existing_handle: ctypes.c_void_p | None = None,
) -> None:
    advapi32 = _load_windows_library("advapi32")
    kernel32 = _load_windows_library("kernel32")
    user_sid = _current_windows_user_sid(
        advapi32=advapi32,
        kernel32=kernel32,
        path=path,
    )
    default_owner_sid = _current_windows_default_owner_sid(
        advapi32=advapi32,
        kernel32=kernel32,
        path=path,
    )
    descriptor = _build_private_windows_security_descriptor(
        directory=directory,
        advapi32=advapi32,
        owner_sid=user_sid,
        path=path,
    )
    handle = existing_handle
    owns_handle = existing_handle is None
    try:
        dacl = ctypes.c_void_p()
        dacl_present = ctypes.c_int()
        dacl_defaulted = ctypes.c_int()
        get_dacl = advapi32.GetSecurityDescriptorDacl
        get_dacl.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_int),
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(ctypes.c_int),
        ]
        get_dacl.restype = ctypes.c_int
        if not get_dacl(
            descriptor,
            ctypes.byref(dacl_present),
            ctypes.byref(dacl),
            ctypes.byref(dacl_defaulted),
        ):
            error = _windows_last_error()
            raise ConfigurationError(f"could not read private Windows ACL ({error}): {path}")
        if not dacl_present.value or dacl.value is None:
            raise ConfigurationError(f"private Windows ACL has no DACL: {path}")
        if handle is None:
            handle = _open_windows_configuration_handle(
                path,
                directory=directory,
                kernel32=kernel32,
            )
        else:
            _validate_windows_configuration_handle(
                handle,
                directory=directory,
                kernel32=kernel32,
                path=path,
            )
        owner_sid = _windows_object_owner_sid(
            handle,
            advapi32=advapi32,
            kernel32=kernel32,
            path=path,
        )
        _require_current_windows_owner(
            owner_sid=owner_sid,
            user_sid=user_sid,
            default_owner_sid=default_owner_sid,
            path=path,
        )
        # Elevated tokens can assign their TokenOwner SID (commonly the local
        # Administrators group) to objects this process creates.  Accept only
        # that token-proven default, then normalize it to TokenUser below.
        descriptor_owner = ctypes.c_void_p()
        owner_defaulted = ctypes.c_int()
        get_owner = advapi32.GetSecurityDescriptorOwner
        get_owner.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(ctypes.c_int),
        ]
        get_owner.restype = ctypes.c_int
        if not get_owner(
            descriptor,
            ctypes.byref(descriptor_owner),
            ctypes.byref(owner_defaulted),
        ):
            error = _windows_last_error()
            raise ConfigurationError(
                f"could not read private Windows configuration owner ({error}): {path}"
            )
        if descriptor_owner.value is None:
            raise ConfigurationError(f"private Windows configuration owner has no SID: {path}")
        normalize_owner = owner_sid != user_sid
        if normalize_owner and owns_handle:
            _close_windows_handle(handle, kernel32=kernel32)
            handle = None
            handle = _open_windows_configuration_handle(
                path,
                directory=directory,
                kernel32=kernel32,
                write_owner=True,
            )
            owner_sid = _windows_object_owner_sid(
                handle,
                advapi32=advapi32,
                kernel32=kernel32,
                path=path,
            )
            _require_current_windows_owner(
                owner_sid=owner_sid,
                user_sid=user_sid,
                default_owner_sid=default_owner_sid,
                path=path,
            )
            normalize_owner = owner_sid != user_sid
        set_security = advapi32.SetSecurityInfo
        set_security.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_void_p,
        ]
        set_security.restype = ctypes.c_uint32
        result = set_security(
            handle,
            _WINDOWS_SE_FILE_OBJECT,
            _WINDOWS_DACL_SECURITY_INFORMATION
            | _WINDOWS_PROTECTED_DACL_SECURITY_INFORMATION
            | (_WINDOWS_OWNER_SECURITY_INFORMATION if normalize_owner else 0),
            descriptor_owner if normalize_owner else None,
            None,
            dacl,
            None,
        )
        if result != 0:
            raise ConfigurationError(
                f"could not protect Windows configuration ACL ({result}): {path}"
            )
        _verify_private_windows_acl(
            handle,
            directory=directory,
            expected_owner_sid=user_sid,
            advapi32=advapi32,
            kernel32=kernel32,
            path=path,
        )
    finally:
        if owns_handle and handle is not None:
            _close_windows_handle(handle, kernel32=kernel32)
        _free_windows_local(descriptor, kernel32=kernel32)


def ensure_private_configuration_path(path: Path, *, directory: bool) -> None:
    """Enforce private ownership on a configuration file or state directory."""
    if os.name != "nt":
        value = os.lstat(path)
        expected_type = stat.S_ISDIR(value.st_mode) if directory else stat.S_ISREG(value.st_mode)
        if not expected_type or _is_reparse_stat(value):
            kind = "directory" if directory else "file"
            raise ConfigurationError(f"configuration path is not a regular {kind}: {path}")
        if hasattr(os, "getuid") and value.st_uid != os.getuid():
            raise ConfigurationError(f"configuration path is not owned by this user: {path}")
        if stat.S_IMODE(value.st_mode) & 0o022:
            raise ConfigurationError(
                f"configuration path is writable by group or other users: {path}"
            )
        return
    _set_private_windows_acl(path, directory=directory)


def ensure_private_configuration_windows_handle(
    path: Path,
    *,
    handle: ctypes.c_void_p,
    directory: bool,
) -> None:
    """Enforce and verify a private ACL through an exact open Windows handle."""
    if os.name != "nt":  # pragma: no cover - explicit platform contract
        raise ConfigurationError("Windows handle ACL enforcement is unavailable")
    _set_private_windows_acl(
        path,
        directory=directory,
        existing_handle=handle,
    )


def acquire_private_configuration_windows_parent_guard(
    parent: Path,
) -> tuple[Path, ctypes.c_void_p]:
    """Create an auto-deleting private child that prevents parent rename on Windows."""
    if os.name != "nt":  # pragma: no cover - explicit platform contract
        raise ConfigurationError("Windows parent guarding is unavailable")
    guard_path = parent / f".clio-parent-guard-{os.getpid()}-{uuid4().hex}.pending"
    storage_path = internal_filesystem_path(guard_path, force_extended=True)
    kernel32 = _load_windows_library("kernel32")
    advapi32 = _load_windows_library("advapi32")
    owner_sid = _current_windows_user_sid(
        advapi32=advapi32,
        kernel32=kernel32,
        path=storage_path,
    )
    security_descriptor = _build_private_windows_security_descriptor(
        directory=False,
        advapi32=advapi32,
        owner_sid=owner_sid,
        path=storage_path,
    )
    security_attributes = _WindowsSecurityAttributes(
        length=ctypes.sizeof(_WindowsSecurityAttributes),
        security_descriptor=security_descriptor,
        inherit_handle=0,
    )
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.POINTER(_WindowsSecurityAttributes),
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    ]
    create_file.restype = ctypes.c_void_p
    raw_handle: int | None = None
    try:
        raw_value = create_file(
            str(storage_path),
            _WINDOWS_GENERIC_READ
            | _WINDOWS_GENERIC_WRITE
            | _WINDOWS_DELETE
            | _WINDOWS_READ_CONTROL
            | _WINDOWS_WRITE_DAC
            | _WINDOWS_WRITE_OWNER,
            0,
            ctypes.byref(security_attributes),
            _WINDOWS_CREATE_NEW,
            _WINDOWS_FILE_ATTRIBUTE_NORMAL
            | _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT
            | _WINDOWS_FILE_FLAG_DELETE_ON_CLOSE,
            None,
        )
        if raw_value in (None, ctypes.c_void_p(-1).value):
            raise ConfigurationError(
                f"could not create private Windows parent guard ({_windows_last_error()}): {parent}"
            )
        raw_handle = cast(int, raw_value)
        handle = ctypes.c_void_p(raw_handle)
        _validate_windows_configuration_handle(
            handle,
            directory=False,
            kernel32=kernel32,
            path=storage_path,
        )
        ensure_private_configuration_windows_handle(
            storage_path,
            handle=handle,
            directory=False,
        )
        return guard_path, handle
    except BaseException:
        if raw_handle is not None:
            _close_windows_handle(ctypes.c_void_p(raw_handle), kernel32=kernel32)
        raise
    finally:
        _free_windows_local(security_descriptor, kernel32=kernel32)


def release_private_configuration_windows_parent_guard(
    guard: tuple[Path, ctypes.c_void_p] | None,
) -> None:
    """Close one auto-deleting Windows parent guard."""
    if guard is None:
        return
    if os.name != "nt":  # pragma: no cover - explicit platform contract
        raise ConfigurationError("Windows parent guarding is unavailable")
    _path, handle = guard
    _close_windows_handle(handle, kernel32=_load_windows_library("kernel32"))


def open_private_configuration_windows_descriptor(
    path: Path,
    *,
    exclusive: bool = False,
    expected_nlink: int = 1,
) -> int:
    """Open one exact Windows file and enforce its private ACL in place."""
    if os.name != "nt":  # pragma: no cover - explicit platform contract
        raise ConfigurationError("Windows private descriptor opening is unavailable")
    storage_path = internal_filesystem_path(path, force_extended=True)
    before = os.lstat(storage_path)
    if not (
        stat.S_ISREG(before.st_mode)
        and not _is_reparse_stat(before)
        and before.st_nlink == expected_nlink
    ):
        raise ConfigurationError(f"configuration path is not one regular owned file: {path}")
    kernel32 = _load_windows_library("kernel32")
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    ]
    create_file.restype = ctypes.c_void_p
    raw_handle = create_file(
        str(storage_path),
        _WINDOWS_GENERIC_READ | _WINDOWS_READ_CONTROL | _WINDOWS_WRITE_DAC | _WINDOWS_WRITE_OWNER,
        0 if exclusive else _WINDOWS_FILE_SHARE_READ,
        None,
        _WINDOWS_OPEN_EXISTING,
        _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT,
        None,
    )
    if raw_handle in (None, ctypes.c_void_p(-1).value):
        error = _windows_last_error()
        raise ConfigurationError(f"could not open private Windows file ({error}): {path}")
    handle = ctypes.c_void_p(raw_handle)
    try:
        get_information = kernel32.GetFileInformationByHandle
        get_information.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(_WindowsFileInformation),
        ]
        get_information.restype = ctypes.c_int
        information = _WindowsFileInformation()
        if not get_information(handle, ctypes.byref(information)):
            error = _windows_last_error()
            raise ConfigurationError(f"could not inspect private Windows file ({error}): {path}")
        file_index = (int(information.file_index_high) << 32) | int(information.file_index_low)
        after = os.lstat(storage_path)
        if not (
            not information.attributes & _WINDOWS_FILE_ATTRIBUTE_DIRECTORY
            and not information.attributes & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT
            and information.number_of_links == expected_nlink
            and before.st_ino == file_index
            and os.path.samestat(before, after)
            and after.st_nlink == expected_nlink
        ):
            raise ConfigurationError(f"private Windows file changed while opening: {path}")
        ensure_private_configuration_windows_handle(
            storage_path,
            handle=handle,
            directory=False,
        )
        confirmed = os.lstat(storage_path)
        if not os.path.samestat(after, confirmed) or confirmed.st_nlink != expected_nlink:
            raise ConfigurationError(f"private Windows file changed while securing: {path}")
        descriptor_flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        descriptor = _open_windows_os_file_handle(cast(int, raw_handle), descriptor_flags)
        raw_handle = None
        return descriptor
    finally:
        if raw_handle not in (None, ctypes.c_void_p(-1).value):
            _close_windows_handle(handle, kernel32=kernel32)


def default_registry_path() -> Path:
    """Return the default local cluster registry path."""
    configured = os.getenv(CLUSTER_REGISTRY_ENV)
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(".clio-relay/clusters.json").resolve()
