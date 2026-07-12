"""Local cluster registry for relay targets."""

from __future__ import annotations

import ctypes
import json
import os
import stat
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, BinaryIO, Literal, cast
from uuid import uuid4

from filelock import FileLock
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay.errors import ConfigurationError
from clio_relay.models import validate_mcp_env_from

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
RemoteMcpContract = Literal["clio-kit-spack-user-v3"]


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


class ClusterDefinition(BaseModel):
    """A locally configured cluster target."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=256)
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
    scheduler_provider: str = "external"
    remote_mcp_servers: dict[str, RemoteMcpServerConfig] = Field(
        default_factory=dict,
        max_length=MAX_REMOTE_MCP_SERVERS_PER_CLUSTER,
    )
    frp_transport: FrpTransportConfig = Field(default_factory=FrpTransportConfig)
    live_test: LiveTestConfig = Field(default_factory=LiveTestConfig)
    target_identity: ClusterTargetIdentity | None = None

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

    @model_validator(mode="after")
    def _remote_mcp_must_not_reference_transport_credentials(self) -> ClusterDefinition:
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
        if any(not name.strip() for name in value):
            raise ValueError("cluster registry keys must not be blank")
        if any(len(name) > 256 for name in value):
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
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        ensure_private_configuration_path(path.parent, directory=True)
        if not path.exists():
            with FileLock(f"{path}.lock"):
                if not path.exists():
                    cls.default()._write_atomic_unlocked(path)
        return cls.model_validate_json(
            read_bounded_configuration_bytes(path, max_bytes=MAX_CLUSTER_REGISTRY_BYTES)
        )

    def save(self, path: Path) -> None:
        """Persist the registry with locking, atomic replacement, and fsync."""
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        ensure_private_configuration_path(path.parent, directory=True)
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
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        ensure_private_configuration_path(path.parent, directory=True)
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
    descriptor = os.open(path, flags, 0o600)
    try:
        ensure_private_configuration_path(path, directory=False)
        return os.fdopen(descriptor, "wb")
    except BaseException:
        os.close(descriptor)
        raise


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
    sddl = (
        "D:P(A;OICI;FA;;;OW)(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)"
        if directory
        else "D:P(A;;FA;;;OW)(A;;FA;;;SY)(A;;FA;;;BA)"
    )
    win_dll = cast(Any, ctypes.WinDLL)
    advapi32 = win_dll("advapi32", use_last_error=True)
    kernel32 = win_dll("kernel32", use_last_error=True)
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
        error = ctypes.get_last_error()
        raise ConfigurationError(f"could not build private Windows ACL ({error}): {path}")
    try:
        set_security = advapi32.SetFileSecurityW
        set_security.argtypes = [ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_void_p]
        set_security.restype = ctypes.c_int
        security_information = 0x00000004 | 0x80000000
        if not set_security(str(path), security_information, descriptor):
            error = ctypes.get_last_error()
            raise ConfigurationError(
                f"could not protect Windows configuration ACL ({error}): {path}"
            )
    finally:
        local_free = kernel32.LocalFree
        local_free.argtypes = [ctypes.c_void_p]
        local_free.restype = ctypes.c_void_p
        local_free(descriptor)


def default_registry_path() -> Path:
    """Return the default local cluster registry path."""
    configured = os.getenv(CLUSTER_REGISTRY_ENV)
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(".clio-relay/clusters.json").resolve()
