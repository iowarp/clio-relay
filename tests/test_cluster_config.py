from __future__ import annotations

import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

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
    default_registry_path,
    ensure_private_configuration_path,
    open_private_atomic_file,
    read_bounded_configuration_bytes,
)
from clio_relay.errors import ConfigurationError
from clio_relay.remote_mcp import default_remote_mcp_cache_path


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

    assert modes == [0o600]


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

    powershell = (
        Path(os.environ["SYSTEMROOT"])
        / "System32"
        / "WindowsPowerShell"
        / "v1.0"
        / "powershell.exe"
    )
    script = (
        "$acl=Get-Acl -LiteralPath $env:CLIO_RELAY_ACL_TEST_PATH;"
        "$broad=@('S-1-1-0','S-1-5-11','S-1-5-32-545');"
        "$bad=@($acl.Access | ForEach-Object {"
        "try {$sid=$_.IdentityReference.Translate("
        "[System.Security.Principal.SecurityIdentifier]).Value} catch {$sid='unresolved'};"
        "if ($broad -contains $sid -and $_.AccessControlType -eq 'Allow') {$sid}});"
        "[pscustomobject]@{protected=$acl.AreAccessRulesProtected;bad=$bad.Count}"
        "| ConvertTo-Json -Compress"
    )
    result = subprocess.run(
        [str(powershell), "-NoProfile", "-NonInteractive", "-Command", script],
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
        env={**os.environ, "CLIO_RELAY_ACL_TEST_PATH": str(path)},
    )

    assert result.returncode == 0, result.stderr
    acl = json.loads(result.stdout)
    assert acl == {"protected": True, "bad": 0}


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


def _registry(name: str) -> ClusterRegistry:
    return ClusterRegistry(
        clusters={name: ClusterDefinition(name=name, ssh_host=f"{name}.example.invalid")}
    )
