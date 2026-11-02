"""iowarp/clio-relay#203: a host-local teardown acts only on its cluster's registered core."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

import clio_relay.cli as cli
from clio_relay.cli import app
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry
from clio_relay.config import RelaySettings
from clio_relay.errors import ConfigurationError, TeardownCoreScopeError
from clio_relay.teardown_core_scope import require_registered_local_core


class _QueueOpened(Exception):
    """Raised in place of opening a real queue, proving the guard let teardown through."""


def _local_definition(core_dir: str, spool_dir: str) -> ClusterDefinition:
    return ClusterDefinition(
        name="shared-host",
        ssh_host="localhost",
        core_dir=core_dir,
        spool_dir=spool_dir,
    )


def _settings(core_dir: Path, spool_dir: Path) -> RelaySettings:
    return RelaySettings(core_dir=core_dir, spool_dir=spool_dir)


def test_matching_paths_pass(tmp_path: Path) -> None:
    definition = _local_definition(str(tmp_path / "core"), str(tmp_path / "spool"))

    require_registered_local_core(definition, _settings(tmp_path / "core", tmp_path / "spool"))


def test_registered_home_token_expands_against_this_host(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    definition = _local_definition("$HOME/deploy/core", "$HOME/deploy/spool")

    require_registered_local_core(
        definition,
        _settings(tmp_path / "deploy" / "core", tmp_path / "deploy" / "spool"),
    )


def test_symlinked_core_resolves_to_the_registered_core(tmp_path: Path) -> None:
    real_core = tmp_path / "real-core"
    real_core.mkdir()
    (tmp_path / "core-link").symlink_to(real_core, target_is_directory=True)
    definition = _local_definition(str(real_core), str(tmp_path / "spool"))

    require_registered_local_core(
        definition,
        _settings(tmp_path / "core-link", tmp_path / "spool"),
    )


@pytest.mark.parametrize(
    ("field", "core", "spool"),
    [
        ("core_dir", "other-core", "spool"),
        ("spool_dir", "core", "other-spool"),
    ],
)
def test_a_different_core_or_spool_is_refused_naming_both_paths(
    tmp_path: Path,
    field: str,
    core: str,
    spool: str,
) -> None:
    definition = _local_definition(str(tmp_path / "core"), str(tmp_path / "spool"))

    with pytest.raises(TeardownCoreScopeError) as raised:
        require_registered_local_core(definition, _settings(tmp_path / core, tmp_path / spool))

    error = raised.value
    assert isinstance(error, ConfigurationError)
    assert error.reason == "teardown_core_scope_mismatch"
    assert error.cluster == "shared-host"
    assert error.field == field
    registered = tmp_path / ("core" if field == "core_dir" else "spool")
    resolved = tmp_path / (core if field == "core_dir" else spool)
    assert error.registered == str(registered.resolve())
    assert error.resolved == str(resolved.resolve())
    assert error.registered in str(error)
    assert error.resolved in str(error)


def _register_shared_host(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Register a host-local cluster whose core is NOT the default core."""
    monkeypatch.chdir(tmp_path)
    home = tmp_path / "home"
    # Another deployment's live default core on the same host.
    (home / ".local" / "share" / "clio-relay" / "core").mkdir(parents=True)
    (home / ".local" / "share" / "clio-relay" / "spool").mkdir(parents=True)
    monkeypatch.setenv("HOME", str(home))
    for name in (
        "CLIO_RELAY_CORE_DIR",
        "CLIO_RELAY_SPOOL_DIR",
        "CLIO_RELAY_CLI_MODE",
        "CLIO_RELAY_REMOTE_CLUSTER",
    ):
        monkeypatch.delenv(name, raising=False)
    deployment = tmp_path / "deployment"
    ClusterRegistry(
        clusters={
            "shared-host": _local_definition(
                str(deployment / "core"),
                str(deployment / "spool"),
            )
        }
    ).save(tmp_path / ".clio-relay" / "clusters.json")
    return deployment


def _teardown(tmp_path: Path) -> list[str]:
    return [
        "session",
        "teardown",
        "--cluster",
        "shared-host",
        "--session-id",
        "owned-session",
        "--validation-report",
        str(tmp_path / "teardown-report.json"),
    ]


def test_teardown_without_core_environment_refuses_instead_of_using_the_default_core(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = _register_shared_host(tmp_path, monkeypatch)
    opened: list[bool] = []

    def record_queue_open() -> object:
        opened.append(True)
        raise _QueueOpened

    monkeypatch.setattr(cli, "_managed_queue_from_env", record_queue_open)

    result = CliRunner().invoke(app, _teardown(tmp_path))

    assert result.exit_code != 0
    assert isinstance(result.exception, TeardownCoreScopeError)
    assert result.exception.registered == str((deployment / "core").resolve())
    default_core = tmp_path / "home" / ".local" / "share" / "clio-relay" / "core"
    assert result.exception.resolved == str(default_core.resolve())
    assert opened == []


def test_teardown_with_the_registered_core_environment_proceeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = _register_shared_host(tmp_path, monkeypatch)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(deployment / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(deployment / "spool"))

    def stop_at_queue_open() -> object:
        raise _QueueOpened

    monkeypatch.setattr(cli, "_managed_queue_from_env", stop_at_queue_open)

    result = CliRunner().invoke(app, _teardown(tmp_path))

    assert isinstance(result.exception, _QueueOpened)
