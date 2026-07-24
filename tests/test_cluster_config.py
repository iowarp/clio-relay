from __future__ import annotations

import ctypes
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pytest import MonkeyPatch

from clio_relay import cluster_config
from clio_relay.cluster_config import (
    MAX_CLUSTER_REGISTRY_BYTES,
    MAX_CONFIGURED_CLUSTERS,
    MAX_REMOTE_MCP_ARGS,
    ClusterDefinition,
    ClusterRegistry,
    ClusterTargetIdentity,
    RemoteMcpServerConfig,
    WorkerCapacityPolicy,
    cluster_route_revision,
    default_registry_path,
    ensure_private_configuration_directory,
    ensure_private_configuration_path,
    open_private_atomic_file,
    read_bounded_configuration_bytes,
)
from clio_relay.errors import ConfigurationError
from clio_relay.remote_mcp import default_remote_mcp_cache_path


@pytest.mark.parametrize("profile", ["", " ares", "ares ", ".", "..", "a/res", "a\\res"])
def test_cluster_definition_rejects_unsafe_jarvis_graph_profile(profile: str) -> None:
    """A profile is one explicit JARVIS catalog key, never a relay-owned path."""
    with pytest.raises(ValueError, match="safe exact JARVIS profile"):
        ClusterDefinition(
            name="cluster-a",
            ssh_host="cluster-a",
            jarvis_resource_graph_profile=profile,
        )


def test_cluster_definition_requires_profile_for_graph_build_fallback() -> None:
    """An operator cannot enable an unscoped hardware-discovery fallback."""
    with pytest.raises(ValueError, match="requires jarvis_resource_graph_profile"):
        ClusterDefinition(
            name="cluster-a",
            ssh_host="cluster-a",
            allow_jarvis_resource_graph_build=True,
        )


def test_registry_and_schema_cache_defaults_are_absolute_and_stable(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.delenv("CLIO_RELAY_CLUSTER_REGISTRY", raising=False)
    monkeypatch.delenv("CLIO_RELAY_REMOTE_MCP_CACHE", raising=False)
    monkeypatch.chdir(tmp_path)

    registry_path = default_registry_path()
    cache_path = default_remote_mcp_cache_path(registry_path=registry_path)

    assert registry_path == (tmp_path / ".clio-relay" / "clusters.json").resolve()
    assert cache_path == (tmp_path / ".clio-relay" / "remote-mcp-cache.json").resolve()
    assert registry_path.is_absolute()
    assert cache_path.is_absolute()


def test_registry_and_schema_cache_environment_paths_are_resolved(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", "state/clusters.json")
    monkeypatch.setenv("CLIO_RELAY_REMOTE_MCP_CACHE", "state/cache.json")

    assert default_registry_path() == (tmp_path / "state" / "clusters.json").resolve()
    assert default_remote_mcp_cache_path() == (tmp_path / "state" / "cache.json").resolve()


def test_cluster_registry_save_uses_atomic_replace_and_fsync(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    path = tmp_path / ".clio-relay" / "clusters.json"
    fsynced: list[int] = []
    replaced: list[tuple[Path, Path]] = []
    original_fsync = os.fsync
    original_replace = os.replace

    def recording_fsync(file_descriptor: int) -> None:
        fsynced.append(file_descriptor)
        original_fsync(file_descriptor)

    def recording_replace(source: str | os.PathLike[str], target: str | os.PathLike[str]) -> None:
        replaced.append((Path(source), Path(target)))
        original_replace(source, target)

    monkeypatch.setattr(cluster_config.os, "fsync", recording_fsync)
    monkeypatch.setattr(cluster_config.os, "replace", recording_replace)
    registry = _registry("alpha")

    registry.save(path)

    assert ClusterRegistry.load(path) == registry
    assert fsynced
    assert len(replaced) == 1
    assert replaced[0][1] == path
    assert replaced[0][0] != path
    assert list(path.parent.glob(f".{path.name}.*.tmp")) == []


def test_cluster_registry_save_retries_windows_sharing_violation(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    path = tmp_path / "clusters.json"
    attempts = 0
    original_replace = os.replace

    def sharing_once(
        source: str | os.PathLike[str],
        target: str | os.PathLike[str],
    ) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise PermissionError("simulated sharing violation")
        original_replace(source, target)

    def no_sleep(_seconds: float) -> None:
        return

    monkeypatch.setattr(cluster_config.os, "replace", sharing_once)
    monkeypatch.setattr(cluster_config.time, "sleep", no_sleep)

    registry = _registry("alpha")
    registry.save(path)

    assert attempts == 2
    assert ClusterRegistry.load(path) == registry
    assert list(path.parent.glob(f".{path.name}.*.tmp")) == []


def test_private_atomic_file_requests_owner_only_posix_mode(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    path = tmp_path / "private.tmp"
    modes: list[int] = []
    original_open = cluster_config.os.open

    def recording_open(
        target: str | os.PathLike[str],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        modes.append(mode)
        return original_open(target, flags, mode)

    monkeypatch.setattr(cluster_config.os, "open", recording_open)

    with open_private_atomic_file(path) as stream:
        stream.write(b"{}")
        stream.flush()
        if os.name == "nt":
            with pytest.raises(PermissionError):
                path.read_bytes()

    assert modes == ([] if os.name == "nt" else [0o600])
    assert path.read_bytes() == b"{}"


def test_windows_atomic_create_captures_error_before_freeing_descriptor(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    events: list[str] = []

    class FailingCreateFile:
        argtypes: list[object]
        restype: object

        def __call__(self, *_args: object) -> int:
            events.append("create")
            return int(ctypes.c_void_p(-1).value or -1)

    kernel32 = SimpleNamespace(CreateFileW=FailingCreateFile())

    def load_library(name: str) -> object:
        return kernel32 if name == "kernel32" else object()

    def last_error() -> int:
        events.append("last-error")
        return 5

    def free_descriptor(_pointer: ctypes.c_void_p, *, kernel32: object) -> None:
        assert kernel32 is not None
        events.append("free")

    def windows_error(error: int) -> OSError:
        events.append("error")
        return PermissionError(error, "simulated access denial")

    def build_descriptor(**_kwargs: object) -> ctypes.c_void_p:
        return ctypes.c_void_p(1)

    def current_user_sid(*, advapi32: object, kernel32: object, path: Path) -> str:
        assert advapi32 is not None
        assert kernel32 is not None
        assert path.name == "private.tmp"
        return "S-1-5-21-current"

    def identity_internal_path(path: Path, *, force_extended: bool) -> Path:
        del force_extended
        return path

    monkeypatch.setattr(cluster_config.os, "name", "nt")
    monkeypatch.setattr(cluster_config, "_load_windows_library", load_library)
    monkeypatch.setattr(
        cluster_config,
        "_build_private_windows_security_descriptor",
        build_descriptor,
    )
    monkeypatch.setattr(cluster_config, "_current_windows_user_sid", current_user_sid)
    monkeypatch.setattr(
        cluster_config,
        "internal_filesystem_path",
        identity_internal_path,
    )
    monkeypatch.setattr(cluster_config, "_windows_last_error", last_error)
    monkeypatch.setattr(cluster_config, "_free_windows_local", free_descriptor)
    monkeypatch.setattr(cluster_config, "_windows_error", windows_error)

    with pytest.raises(PermissionError, match="simulated access denial"):
        open_private_atomic_file(tmp_path / "private.tmp")

    assert events == ["create", "last-error", "free", "error"]


def test_configuration_directory_is_created_with_private_protection(tmp_path: Path) -> None:
    path = tmp_path / "new-state" / "private"

    ensure_private_configuration_directory(path)

    assert path.is_dir()
    ensure_private_configuration_path(path, directory=True)


def test_windows_nested_configuration_creation_retains_hardened_handles(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    path = tmp_path / "new-state" / "private"
    events: list[tuple[str, Path | int]] = []
    open_handles: set[int] = set()
    next_handle = 100
    kernel32 = object()

    def create(directory: Path) -> None:
        directory.mkdir()
        events.append(("create", directory))

    def open_handle(
        target: Path,
        *,
        directory: bool,
        kernel32: object,
        write_owner: bool,
    ) -> Any:
        nonlocal next_handle
        assert directory
        assert write_owner
        assert kernel32 is not None
        next_handle += 1
        handle = SimpleNamespace(value=next_handle)
        open_handles.add(next_handle)
        events.append(("open", target))
        return handle

    def harden(
        directory_path: Path,
        *,
        directory: bool,
        existing_handle: Any,
    ) -> None:
        assert directory
        assert existing_handle.value in open_handles
        events.append(("harden", directory_path))

    def close(handle: Any, *, kernel32: object) -> None:
        assert kernel32 is not None
        assert handle.value in open_handles
        open_handles.remove(handle.value)
        events.append(("close", handle.value))

    def load_kernel32(_name: str) -> object:
        return kernel32

    monkeypatch.setattr(cluster_config.os, "name", "nt")
    monkeypatch.setattr(cluster_config, "_load_windows_library", load_kernel32)
    monkeypatch.setattr(cluster_config, "_create_private_windows_directory", create)
    monkeypatch.setattr(cluster_config, "_open_windows_configuration_handle", open_handle)
    monkeypatch.setattr(cluster_config, "_set_private_windows_acl", harden)
    monkeypatch.setattr(cluster_config, "_close_windows_handle", close)

    ensure_private_configuration_directory(path)

    assert events[:6] == [
        ("create", path.parent),
        ("open", path.parent),
        ("harden", path.parent),
        ("create", path),
        ("open", path),
        ("harden", path),
    ]
    assert [event for event in events if event[0] == "close"] == [
        ("close", 102),
        ("close", 101),
    ]
    assert open_handles == set()


def test_windows_configuration_open_only_requests_owner_change_when_needed(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    requested_access: list[int] = []
    requested_share_modes: list[int] = []

    class RecordingCreateFile:
        argtypes: list[object]
        restype: object

        def __call__(
            self,
            _path: str,
            desired_access: int,
            share_mode: int,
            *_args: object,
        ) -> int:
            requested_access.append(desired_access)
            requested_share_modes.append(share_mode)
            return 100 + len(requested_access)

    kernel32 = SimpleNamespace(CreateFileW=RecordingCreateFile())

    def accept_handle(
        _handle: ctypes.c_void_p,
        *,
        directory: bool,
        kernel32: object,
        path: Path,
    ) -> None:
        del directory, kernel32, path

    monkeypatch.setattr(
        cluster_config,
        "_validate_windows_configuration_handle",
        accept_handle,
    )

    cluster_config._open_windows_configuration_handle(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        tmp_path,
        directory=True,
        kernel32=kernel32,
    )
    cluster_config._open_windows_configuration_handle(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        tmp_path,
        directory=True,
        kernel32=kernel32,
        write_owner=True,
    )

    assert requested_access[0] & cluster_config._WINDOWS_WRITE_DAC  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert not requested_access[0] & cluster_config._WINDOWS_WRITE_OWNER  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert requested_access[1] & cluster_config._WINDOWS_WRITE_OWNER  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert (
        requested_share_modes
        == [
            cluster_config._WINDOWS_FILE_SHARE_READ  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            | cluster_config._WINDOWS_FILE_SHARE_WRITE,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        ]
        * 2
    )


def test_configuration_read_rejects_foreign_or_group_writable_posix_file(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    path = tmp_path / "configuration.json"
    path.write_text("{}", encoding="utf-8")
    owner = os.stat(path).st_uid
    monkeypatch.setattr(cluster_config.os, "name", "posix")
    monkeypatch.setattr(cluster_config.os, "getuid", lambda: owner, raising=False)
    path.chmod(0o666)

    with pytest.raises(ConfigurationError, match="writable by group or other"):
        read_bounded_configuration_bytes(path, max_bytes=1024)

    path.chmod(0o600)
    monkeypatch.setattr(cluster_config.os, "getuid", lambda: owner + 1)
    with pytest.raises(ConfigurationError, match="not owned by this user"):
        read_bounded_configuration_bytes(path, max_bytes=1024)

    directory = tmp_path / "insecure-state"
    directory.mkdir()
    directory.chmod(0o777)
    monkeypatch.setattr(cluster_config.os, "getuid", lambda: os.stat(directory).st_uid)
    with pytest.raises(ConfigurationError, match="writable by group or other"):
        ensure_private_configuration_path(directory, directory=True)


def test_windows_configuration_directory_removes_broad_inherited_acl(tmp_path: Path) -> None:
    if os.name != "nt":
        return
    path = tmp_path / "private-state"
    path.mkdir()

    ensure_private_configuration_path(path, directory=True)
    # The implementation reads the ACL back through Win32, verifies that
    # inheritance is protected, and rejects any SID/mask/flag outside its exact
    # private ACE set before returning.  Reapplying also verifies idempotence.
    ensure_private_configuration_path(path, directory=True)


def test_windows_configuration_hardening_allows_an_existing_writer(tmp_path: Path) -> None:
    if os.name != "nt":
        return
    path = tmp_path / "configuration.json"
    path.write_text("original", encoding="utf-8")

    with path.open("r+b") as writer:
        ensure_private_configuration_path(path, directory=False)
        writer.seek(0)
        assert writer.read() == b"original"

    ensure_private_configuration_path(path, directory=False)


def test_windows_configuration_owner_must_match_current_user(tmp_path: Path) -> None:
    path = tmp_path / "configuration.json"

    cluster_config._require_current_windows_owner(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        owner_sid="S-1-5-21-current",
        user_sid="S-1-5-21-current",
        path=path,
    )
    cluster_config._require_current_windows_owner(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        owner_sid="S-1-5-32-544",
        user_sid="S-1-5-21-current",
        default_owner_sid="S-1-5-32-544",
        path=path,
    )
    with pytest.raises(ConfigurationError, match="not owned by this user"):
        cluster_config._require_current_windows_owner(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            owner_sid="S-1-5-21-foreign",
            user_sid="S-1-5-21-current",
            default_owner_sid="S-1-5-32-544",
            path=path,
        )


def test_cluster_registry_save_preserves_previous_file_when_replace_fails(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    path = tmp_path / "clusters.json"
    original = _registry("original")
    original.save(path)

    def fail_replace(source: object, target: object) -> None:
        del source, target
        raise OSError("simulated replacement failure")

    monkeypatch.setattr(cluster_config.os, "replace", fail_replace)

    with pytest.raises(OSError, match="simulated replacement failure"):
        _registry("replacement").save(path)

    assert ClusterRegistry.load(path) == original
    assert list(path.parent.glob(f".{path.name}.*.tmp")) == []


def test_cluster_registry_concurrent_saves_never_publish_partial_json(tmp_path: Path) -> None:
    path = tmp_path / "clusters.json"
    registries = [_registry(f"cluster-{index}") for index in range(16)]

    def save_registry(registry: ClusterRegistry) -> None:
        registry.save(path)

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(save_registry, registries))

    loaded = ClusterRegistry.load(path)
    assert loaded in registries
    assert list(path.parent.glob(f".{path.name}.*.tmp")) == []


def test_cluster_registry_concurrent_mutations_preserve_all_updates(tmp_path: Path) -> None:
    path = tmp_path / "clusters.json"
    ClusterRegistry.default().save(path)

    def add_cluster(index: int) -> None:
        name = f"cluster-{index}"

        def mutation(registry: ClusterRegistry) -> None:
            registry.clusters[name] = ClusterDefinition(
                name=name,
                ssh_host=f"{name}.example.invalid",
            )

        ClusterRegistry.mutate(path, mutation)

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(add_cluster, range(16)))

    loaded = ClusterRegistry.load(path)
    assert set(loaded.clusters) == {f"cluster-{index}" for index in range(16)}


def test_cluster_registry_mutation_revalidates_registration_limits(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    path = tmp_path / "clusters.json"
    registration = RemoteMcpServerConfig(command="science-mcp")
    original = ClusterRegistry(
        clusters={
            "alpha": ClusterDefinition(
                name="alpha",
                ssh_host="localhost",
                remote_mcp_servers={"first": registration},
            )
        }
    )
    original.save(path)
    monkeypatch.setattr(cluster_config, "MAX_REMOTE_MCP_REGISTRATIONS", 1)

    def exceed_limit(registry: ClusterRegistry) -> None:
        registry.clusters["alpha"].remote_mcp_servers["second"] = registration

    with pytest.raises(ValueError, match="more than 1 remote MCP registrations"):
        ClusterRegistry.mutate(path, exceed_limit)

    assert ClusterRegistry.load(path) == original


def test_cluster_registry_save_revalidates_in_place_model_mutation(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    path = tmp_path / "clusters.json"
    registry = _registry("alpha")
    monkeypatch.setattr(cluster_config, "MAX_REMOTE_MCP_REGISTRATIONS", 0)
    registry.clusters["alpha"].remote_mcp_servers["science"] = RemoteMcpServerConfig(
        command="science-mcp"
    )

    with pytest.raises(ValueError, match="more than 0 remote MCP registrations"):
        registry.save(path)

    assert not path.exists()


def test_stable_configuration_read_retries_version_change(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    path = tmp_path / "clusters.json"
    registry = _registry("alpha")
    registry.save(path)
    calls = 0

    def one_changed_version(value: os.stat_result) -> tuple[int, int, int, int]:
        nonlocal calls
        calls += 1
        version = (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)
        if calls == 3:
            return (*version[:-1], version[-1] + 1)
        return version

    def no_sleep(_seconds: float) -> None:
        return

    monkeypatch.setattr(cluster_config, "_stat_version", one_changed_version)
    monkeypatch.setattr(cluster_config.time, "sleep", no_sleep)

    assert ClusterRegistry.load(path) == registry
    assert calls >= 6


def test_cluster_registry_rejects_oversized_or_non_regular_files(tmp_path: Path) -> None:
    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(b" " * (MAX_CLUSTER_REGISTRY_BYTES + 1))
    non_regular = tmp_path / "directory.json"
    non_regular.mkdir()

    with pytest.raises(ConfigurationError, match="exceeds"):
        ClusterRegistry.load(oversized)
    with pytest.raises(ConfigurationError, match="regular owned file"):
        ClusterRegistry.load(non_regular)


def test_cluster_registry_and_remote_mcp_cardinality_are_bounded(
    monkeypatch: MonkeyPatch,
) -> None:
    clusters = {
        f"cluster-{index}": ClusterDefinition(name=f"cluster-{index}", ssh_host="localhost")
        for index in range(MAX_CONFIGURED_CLUSTERS + 1)
    }

    with pytest.raises(ValueError, match="at most"):
        ClusterRegistry(clusters=clusters)
    with pytest.raises(ValueError, match="at most"):
        RemoteMcpServerConfig(
            command="science-mcp",
            args=[f"argument-{index}" for index in range(MAX_REMOTE_MCP_ARGS + 1)],
        )

    monkeypatch.setattr(cluster_config, "MAX_REMOTE_MCP_REGISTRATIONS", 1)
    registration = RemoteMcpServerConfig(command="science-mcp")
    with pytest.raises(ValueError, match="more than 1 remote MCP registrations"):
        ClusterRegistry(
            clusters={
                "alpha": ClusterDefinition(
                    name="alpha",
                    ssh_host="localhost",
                    remote_mcp_servers={"first": registration},
                ),
                "beta": ClusterDefinition(
                    name="beta",
                    ssh_host="localhost",
                    remote_mcp_servers={"second": registration},
                ),
            }
        )

    with pytest.raises(ValueError, match="must not exceed 256 characters"):
        ClusterDefinition(
            name="alpha",
            ssh_host="localhost",
            remote_mcp_servers={"x" * 257: registration},
        )
    with pytest.raises(ValueError, match="must match ClusterDefinition.name"):
        ClusterRegistry(
            clusters={"alias": ClusterDefinition(name="canonical", ssh_host="localhost")}
        )


@pytest.mark.parametrize("name", [" cluster", "cluster ", "cluster\nExecStart=x", "cluster\x00x"])
def test_cluster_names_reject_controls_and_ambiguous_whitespace(name: str) -> None:
    """Logical labels may be rich text but cannot become line-oriented injection."""
    with pytest.raises(ValueError, match="cluster name"):
        ClusterDefinition(name=name, ssh_host="localhost")


@pytest.mark.parametrize("ssh_host", ["-oProxyCommand=evil", "host name", "host\nnext"])
def test_cluster_ssh_host_rejects_option_and_control_injection(ssh_host: str) -> None:
    """Configured SSH destinations cannot be interpreted as client options."""
    with pytest.raises(ValueError, match="ssh_host"):
        ClusterDefinition(name="cluster", ssh_host=ssh_host)


def test_cluster_target_identity_round_trips_operator_pins(tmp_path: Path) -> None:
    path = tmp_path / "clusters.json"
    registry = ClusterRegistry(
        clusters={
            "ares": ClusterDefinition(
                name="ares",
                ssh_host="ares",
                scheduler_provider="slurm",
                target_identity=ClusterTargetIdentity(
                    hostnames=["ares-login-1", "ares-login-1.example.edu"],
                    ssh_host_key_sha256=["SHA256:operator-pinned-fingerprint"],
                    scheduler_cluster_name="ares",
                    site_marker_sha256="a" * 64,
                ),
            )
        }
    )

    registry.save(path)

    assert ClusterRegistry.load(path) == registry


def test_cluster_route_revision_excludes_worker_capacity_across_upgrade_and_edits() -> None:
    """Scheduling capacity changes must not invalidate durable queue-route handles."""
    legacy_definition = ClusterDefinition.model_validate(
        {
            "name": "ares",
            "ssh_host": "ares-login",
            "scheduler_provider": "slurm",
        }
    )
    legacy_payload = legacy_definition.model_dump(
        mode="json",
        exclude={"remote_mcp_servers", "worker_capacity"},
    )
    pre_upgrade_revision = hashlib.sha256(
        json.dumps(legacy_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    tuned_definition = legacy_definition.model_copy(
        update={
            "worker_capacity": WorkerCapacityPolicy(
                concurrency=8,
                control_query_concurrency=2,
            )
        }
    )

    assert cluster_route_revision(legacy_definition) == pre_upgrade_revision
    assert cluster_route_revision(tuned_definition) == pre_upgrade_revision


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("hostnames", ["ares", "ares"], "must be unique"),
        ("ssh_host_key_sha256", [""], "must not be blank"),
    ],
)
def test_cluster_target_identity_rejects_ambiguous_pins(
    field: str,
    value: list[str],
    message: str,
) -> None:
    values: dict[str, object] = {
        "hostnames": ["ares"],
        "ssh_host_key_sha256": ["SHA256:operator-pinned-fingerprint"],
    }
    values[field] = value

    with pytest.raises(ValueError, match=message):
        ClusterTargetIdentity.model_validate(values)


def test_cluster_spack_executable_is_explicit_site_configuration() -> None:
    definition = ClusterDefinition(
        name="site-cluster",
        ssh_host="site-login",
        spack_executable="/srv/site/spack/bin/spack",
    )

    assert definition.spack_executable == "/srv/site/spack/bin/spack"
    with pytest.raises(ValueError, match="absolute remote path"):
        ClusterDefinition(
            name="site-cluster",
            ssh_host="site-login",
            spack_executable="spack",
        )


def _registry(name: str) -> ClusterRegistry:
    return ClusterRegistry(
        clusters={name: ClusterDefinition(name=name, ssh_host=f"{name}.example.invalid")}
    )
