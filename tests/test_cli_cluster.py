"""Tests for the ``cluster`` registry command group (iowarp/clio-relay#231).

These moved out of ``tests/test_cli.py`` unchanged (beyond imports) alongside
the four registry-CRUD ``cluster_app`` commands' (``list``/``add``/
``pin-target``/``pin-runtime``) extraction into
``src/clio_relay/cli_cluster.py``, per ground rule 3 (SS2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through this command group moves with the logic it exercises.
``test_mint_receipt_then_pin_runtime_passes_verify_remote_worker_info_matrix``
stays in ``tests/test_cli.py`` -- it exercises ``cluster pin-runtime``
together with ``endpoint worker-info`` as one cross-group flow, not this
group alone (the same reasoning ``tests/test_cli_endpoint.py``'s own
docstring already applied to that test).

The six **deployment** ``cluster_app`` commands (``probe``/``bootstrap``/
``install-app``/``install-endpoint-service``/``restart-endpoint-service``/
``endpoint-service-status``) are a separate, later slice's real seam
(``cli_cluster.py``'s own docstring) -- their tests stay in
``tests/test_cli.py`` until that slice lands.

**Autouse-fixture parity.** ``test_cli.py`` defines its own module-scoped
``autouse=True`` ``_default_cli_mode`` fixture (env-var defaults every CLI
invocation there relies on). It is reproduced here (the env-var half only,
the same precedent ``tests/test_cli_relay_host.py``'s own
``_default_cli_mode`` established) -- the trap
``tests/test_cli_worker.py``'s docstring documents hitting for real.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

from clio_relay.cli import app
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry, ClusterTargetIdentity
from clio_relay.errors import ConfigurationError


@pytest.fixture(autouse=True)
def _default_cli_mode(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    """Mirror ``test_cli.py``'s own autouse fixture's env-var half only."""
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv(
        "CLIO_RELAY_INSTALL_RECEIPT",
        str(tmp_path / "relay-state" / "install-receipt.json"),
    )


def test_cluster_add_persists_explicit_jarvis_graph_policy(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The operator-selected profile and build policy cross the CLI boundary unchanged."""
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "add",
            "--name",
            "ares",
            "--ssh-host",
            "ares-login",
            "--jarvis-resource-graph-profile",
            "ares",
            "--allow-jarvis-resource-graph-build",
        ],
    )

    assert result.exit_code == 0, result.output
    definition = ClusterRegistry.load(tmp_path / ".clio-relay/clusters.json").clusters["ares"]
    assert definition.jarvis_resource_graph_profile == "ares"
    assert definition.allow_jarvis_resource_graph_build is True


def test_cluster_add_dev_mode_flag_persists_on_the_registry_entry(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """clio-relay#211: cluster add --dev-mode is the CLI path to pin a cluster's dev mode."""
    monkeypatch.chdir(tmp_path)
    registry_path = tmp_path / ".clio-relay" / "clusters.json"

    result = CliRunner().invoke(
        app,
        ["cluster", "add", "--name", "ares-p5run2", "--ssh-host", "ares-login", "--dev-mode"],
    )
    assert result.exit_code == 0, result.output
    definition = ClusterRegistry.load(registry_path).require("ares-p5run2")
    assert definition.dev_mode is True

    result = CliRunner().invoke(
        app,
        ["cluster", "add", "--name", "ares", "--ssh-host", "ares-login"],
    )
    assert result.exit_code == 0, result.output
    assert ClusterRegistry.load(registry_path).require("ares").dev_mode is False


def test_cli_cluster_add_writes_explicit_definition(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "add",
            "--name",
            "delta",
            "--ssh-host",
            "delta-login",
            "--scheduler-provider",
            "slurm",
            "--spack-executable",
            "/opt/site/spack/bin/spack",
            "--target-hostname",
            "delta-login-1",
            "--target-hostname",
            "delta-login-1.example.edu",
            "--ssh-host-key-sha256",
            "SHA256:operator-pinned-fingerprint",
            "--scheduler-cluster-name",
            "delta",
            "--site-marker-sha256",
            "a" * 64,
            "--agent-adapter",
            "exec",
            "--agent-npm-package",
            "",
            "--agent-npm-bin",
            "clio",
            "--frp-server-addr",
            "relay.example.edu",
            "--frp-protocol",
            "tcp",
            "--frp-server-port",
            "7000",
        ],
    )

    assert result.exit_code == 0
    registry = ClusterRegistry.load(tmp_path / ".clio-relay" / "clusters.json")
    definition = registry.require("delta")
    assert definition.ssh_host == "delta-login"
    assert definition.scheduler_provider == "slurm"
    assert definition.spack_executable == "/opt/site/spack/bin/spack"
    assert definition.target_identity == ClusterTargetIdentity(
        hostnames=["delta-login-1", "delta-login-1.example.edu"],
        ssh_host_key_sha256=["SHA256:operator-pinned-fingerprint"],
        scheduler_cluster_name="delta",
        site_marker_sha256="a" * 64,
    )
    assert definition.agent_adapter == "exec"
    assert definition.agent_npm_package is None
    assert definition.agent_npm_bin == "clio"
    assert definition.frp_transport.server_addr == "relay.example.edu"
    assert definition.frp_transport.protocol == "tcp"
    assert definition.frp_transport.server_port == 7000
    assert definition.frp_transport.direct.enabled is False
    assert definition.frp_transport.direct.fallback_order == ["frp_stcp", "queue"]


def test_cli_cluster_add_requires_hostname_and_host_key_pins_together(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "add",
            "--name",
            "delta",
            "--ssh-host",
            "delta-login",
            "--target-hostname",
            "delta-login-1",
        ],
        terminal_width=200,
    )

    assert result.exit_code == 2
    assert not (tmp_path / ".clio-relay" / "clusters.json").exists()


def test_cli_cluster_pin_target_preserves_every_unrelated_cluster_setting(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    registry_path = tmp_path / ".clio-relay" / "clusters.json"
    definition = ClusterDefinition.model_validate(
        {
            "name": "delta",
            "ssh_host": "delta-login",
            "bootstrap_profile": "site-profile",
            "core_dir": "/srv/clio/core",
            "spool_dir": "/scratch/clio/spool",
            "jarvis_bin": "/opt/jarvis/bin/jarvis",
            "spack_executable": "/opt/site/spack/bin/spack",
            "frpc_bin": "/opt/frp/frpc",
            "agent_bin": "/opt/agent/bin/agent",
            "agent_adapter": "exec",
            "agent_npm_package": "@example/agent",
            "agent_npm_bin": "example-agent",
            "agent_args": ["--profile", "science"],
            "scheduler_provider": "slurm",
            "remote_mcp_servers": {
                "spack": {
                    "command": "uvx",
                    "args": [
                        "--from",
                        "/opt/clio/clio_kit-2.3.1-py3-none-any.whl",
                        "clio-kit",
                        "mcp-server",
                        "spack",
                    ],
                    "namespace": "software",
                    "allow_tools": ["spack_find", "spack_install"],
                    "profiles": ["user"],
                }
            },
            "frp_transport": {
                "protocol": "tcp",
                "server_addr": "relay.example.edu",
                "server_port": 7000,
                "token_env": "SITE_FRP_TOKEN",
                "stcp_secret_env": "SITE_STCP_SECRET",
                "direct": {
                    "enabled": True,
                    "mode": "xtcp",
                    "fallback_order": ["xtcp", "frp_stcp", "queue"],
                    "probe_timeout_seconds": 14,
                },
            },
            "live_test": {
                "jarvis_yaml": "site/pipeline.yaml",
                "monitor_pattern": "iteration",
                "progress_pattern": "progress",
                "verify_transport": True,
                "transport_local_bind_port": 19001,
                "agent_prompt": "validate the site",
            },
            "target_identity": {
                "hostnames": ["old-login.example.edu"],
                "ssh_host_key_sha256": ["SHA256:old-key"],
            },
        }
    )
    ClusterRegistry(clusters={"delta": definition}).save(registry_path)
    expected_unrelated = definition.model_dump(mode="json")
    expected_unrelated.pop("target_identity")

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "pin-target",
            "--cluster",
            "delta",
            "--target-hostname",
            "delta-login-1",
            "--target-hostname",
            "delta-login-1.example.edu",
            "--ssh-host-key-sha256",
            "SHA256:new-key-a",
            "--ssh-host-key-sha256",
            "SHA256:new-key-b",
            "--scheduler-cluster-name",
            "delta-production",
            "--site-marker-sha256",
            "b" * 64,
        ],
    )

    assert result.exit_code == 0, result.output
    updated = ClusterRegistry.load(registry_path).require("delta")
    actual_unrelated = updated.model_dump(mode="json")
    actual_unrelated.pop("target_identity")
    assert actual_unrelated == expected_unrelated
    assert updated.target_identity == ClusterTargetIdentity(
        hostnames=["delta-login-1", "delta-login-1.example.edu"],
        ssh_host_key_sha256=["SHA256:new-key-a", "SHA256:new-key-b"],
        scheduler_cluster_name="delta-production",
        site_marker_sha256="b" * 64,
    )


def test_cli_cluster_pin_target_clear_is_exclusive_and_preserves_cluster_config(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    registry_path = tmp_path / ".clio-relay" / "clusters.json"
    definition = ClusterDefinition(
        name="frontier",
        ssh_host="frontier-login",
        agent_args=["--keep-this"],
        target_identity=ClusterTargetIdentity(
            hostnames=["frontier-login.example.edu"],
            ssh_host_key_sha256=["SHA256:old-key"],
        ),
    )
    ClusterRegistry(clusters={"frontier": definition}).save(registry_path)

    rejected = CliRunner().invoke(
        app,
        [
            "cluster",
            "pin-target",
            "--cluster",
            "frontier",
            "--clear",
            "--target-hostname",
            "unexpected.example.edu",
        ],
    )
    assert rejected.exit_code == 2
    assert ClusterRegistry.load(registry_path).require("frontier") == definition

    cleared = CliRunner().invoke(
        app,
        ["cluster", "pin-target", "--cluster", "frontier", "--clear"],
    )
    assert cleared.exit_code == 0, cleared.output
    updated = ClusterRegistry.load(registry_path).require("frontier")
    assert updated.target_identity is None
    assert updated.model_copy(update={"target_identity": definition.target_identity}) == definition


def test_cli_cluster_pin_runtime_preserves_every_unrelated_cluster_setting(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """clio-relay#205 follow-up: pin-runtime is a partial update, not a wholesale replace."""
    monkeypatch.chdir(tmp_path)
    registry_path = tmp_path / ".clio-relay" / "clusters.json"
    definition = ClusterDefinition.model_validate(
        {
            "name": "ares-p5run2",
            "ssh_host": "ares-login",
            "bootstrap_profile": "site-profile",
            "core_dir": "/srv/clio/core",
            "spool_dir": "/scratch/clio/spool",
            "jarvis_bin": "/opt/jarvis/bin/jarvis",
            "spack_executable": "/opt/site/spack/bin/spack",
            "frpc_bin": "/opt/frp/frpc",
            "agent_bin": "/opt/agent/bin/agent",
            "agent_adapter": "exec",
            "agent_npm_package": "@example/agent",
            "agent_npm_bin": "example-agent",
            "agent_args": ["--profile", "science"],
            "scheduler_provider": "slurm",
            "remote_mcp_servers": {
                "spack": {
                    "command": "uvx",
                    "args": [
                        "--from",
                        "/opt/clio/clio_kit-2.3.1-py3-none-any.whl",
                        "clio-kit",
                        "mcp-server",
                        "spack",
                    ],
                    "namespace": "software",
                    "allow_tools": ["spack_find", "spack_install"],
                    "profiles": ["user"],
                }
            },
            "frp_transport": {
                "protocol": "tcp",
                "server_addr": "relay.example.edu",
                "server_port": 7000,
                "token_env": "SITE_FRP_TOKEN",
                "stcp_secret_env": "SITE_STCP_SECRET",
                "direct": {
                    "enabled": True,
                    "mode": "xtcp",
                    "fallback_order": ["xtcp", "frp_stcp", "queue"],
                    "probe_timeout_seconds": 14,
                },
            },
            "target_identity": {
                "hostnames": ["ares-login.example.edu"],
                "ssh_host_key_sha256": ["SHA256:pinned-key"],
                "scheduler_cluster_name": "ares",
                "site_marker_sha256": "a" * 64,
            },
        }
    )
    ClusterRegistry(clusters={"ares-p5run2": definition}).save(registry_path)
    expected_unrelated = definition.model_dump(mode="json")
    expected_unrelated.pop("relay_executable")
    expected_unrelated.pop("relay_install_receipt")

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "pin-runtime",
            "--cluster",
            "ares-p5run2",
            "--relay-executable",
            "$HOME/.local/share/clio-relay/generations/g1/bin/clio-relay",
            "--install-receipt",
            "$HOME/.local/share/clio-relay/generations/g1/install-receipt.json",
        ],
    )

    assert result.exit_code == 0, result.output
    updated = ClusterRegistry.load(registry_path).require("ares-p5run2")
    actual_unrelated = updated.model_dump(mode="json")
    actual_unrelated.pop("relay_executable")
    actual_unrelated.pop("relay_install_receipt")
    assert actual_unrelated == expected_unrelated
    assert updated.relay_executable == "$HOME/.local/share/clio-relay/generations/g1/bin/clio-relay"
    assert (
        updated.relay_install_receipt
        == "$HOME/.local/share/clio-relay/generations/g1/install-receipt.json"
    )


def test_cli_cluster_pin_runtime_warns_when_route_revision_changes(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """#216: an edit that changes cluster_route_revision must warn loudly at edit
    time -- cached MCP discovery evidence for the cluster silently strands
    otherwise, and every call through it fails typed only later, per call.
    """
    monkeypatch.chdir(tmp_path)
    registry_path = tmp_path / ".clio-relay" / "clusters.json"
    definition = ClusterDefinition(name="ares-p5run2", ssh_host="ares-login")
    ClusterRegistry(clusters={"ares-p5run2": definition}).save(registry_path)

    changed = CliRunner().invoke(
        app,
        [
            "cluster",
            "pin-runtime",
            "--cluster",
            "ares-p5run2",
            "--install-receipt",
            "$HOME/.local/share/clio-relay/generations/g1/install-receipt.json",
        ],
    )
    assert changed.exit_code == 0, changed.output
    assert "route revision changed" in changed.output
    assert "stale" in changed.output
    assert "remote-mcp refresh" in changed.output

    # Re-pinning the identical value changes nothing: no repeated warning noise.
    unchanged = CliRunner().invoke(
        app,
        [
            "cluster",
            "pin-runtime",
            "--cluster",
            "ares-p5run2",
            "--install-receipt",
            "$HOME/.local/share/clio-relay/generations/g1/install-receipt.json",
        ],
    )
    assert unchanged.exit_code == 0, unchanged.output
    assert "route revision changed" not in unchanged.output


def test_cli_cluster_add_warns_when_route_revision_changes(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """#216 rework: `cluster add` is one of the three sanctioned edit commands
    the fix names (add / pin-target / pin-runtime) but only pin-runtime's
    warning path was pinned by a test above -- this fills that gap for
    `add`. Re-adding an EXISTING cluster with a changed routing-relevant
    field (ssh_host) must warn loudly that cached MCP discovery evidence is
    now stale, exactly like pin-runtime.
    """
    monkeypatch.chdir(tmp_path)

    created = CliRunner().invoke(
        app,
        ["cluster", "add", "--name", "ares-p5run2", "--ssh-host", "ares-login-1"],
    )
    assert created.exit_code == 0, created.output
    # First registration of a NEW cluster has no prior route revision to
    # compare against (before=None) -- never a warning.
    assert "route revision changed" not in created.output

    changed = CliRunner().invoke(
        app,
        ["cluster", "add", "--name", "ares-p5run2", "--ssh-host", "ares-login-2"],
    )
    assert changed.exit_code == 0, changed.output
    assert "route revision changed" in changed.output
    assert "stale" in changed.output
    assert "remote-mcp refresh" in changed.output

    # Re-adding with the identical routing-relevant fields changes nothing:
    # no repeated warning noise.
    unchanged = CliRunner().invoke(
        app,
        ["cluster", "add", "--name", "ares-p5run2", "--ssh-host", "ares-login-2"],
    )
    assert unchanged.exit_code == 0, unchanged.output
    assert "route revision changed" not in unchanged.output


def test_cli_cluster_pin_target_warns_when_route_revision_changes(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """#216 rework: `cluster pin-target` is one of the three sanctioned edit
    commands the fix names (add / pin-target / pin-runtime) but only
    pin-runtime's warning path was pinned by a test above -- this fills
    that gap for `pin-target`. target_identity is included in
    cluster_route_revision()'s digest (it excludes only
    remote_mcp_servers/worker_capacity), so pinning a physical target
    identity strands cached MCP discovery evidence exactly like pin-runtime
    and must warn the same way.
    """
    monkeypatch.chdir(tmp_path)
    registry_path = tmp_path / ".clio-relay" / "clusters.json"
    definition = ClusterDefinition(name="ares-p5run2", ssh_host="ares-login")
    ClusterRegistry(clusters={"ares-p5run2": definition}).save(registry_path)

    changed = CliRunner().invoke(
        app,
        [
            "cluster",
            "pin-target",
            "--cluster",
            "ares-p5run2",
            "--target-hostname",
            "ares-login-1.example.edu",
            "--ssh-host-key-sha256",
            "SHA256:operator-pinned-fingerprint",
        ],
    )
    assert changed.exit_code == 0, changed.output
    assert "route revision changed" in changed.output
    assert "stale" in changed.output
    assert "remote-mcp refresh" in changed.output

    # Re-pinning the identical target identity changes nothing: no repeated
    # warning noise.
    unchanged = CliRunner().invoke(
        app,
        [
            "cluster",
            "pin-target",
            "--cluster",
            "ares-p5run2",
            "--target-hostname",
            "ares-login-1.example.edu",
            "--ssh-host-key-sha256",
            "SHA256:operator-pinned-fingerprint",
        ],
    )
    assert unchanged.exit_code == 0, unchanged.output
    assert "route revision changed" not in unchanged.output

    # --clear also changes target_identity (back to None) and must warn too.
    cleared = CliRunner().invoke(
        app,
        ["cluster", "pin-target", "--cluster", "ares-p5run2", "--clear"],
    )
    assert cleared.exit_code == 0, cleared.output
    assert "route revision changed" in cleared.output


def test_cli_cluster_pin_runtime_clear_is_exclusive_and_preserves_cluster_config(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    registry_path = tmp_path / ".clio-relay" / "clusters.json"
    definition = ClusterDefinition(
        name="frontier",
        ssh_host="frontier-login",
        agent_args=["--keep-this"],
        relay_executable="$HOME/.local/share/clio-relay/generations/g1/bin/clio-relay",
        relay_install_receipt="$HOME/.local/share/clio-relay/generations/g1/install-receipt.json",
    )
    ClusterRegistry(clusters={"frontier": definition}).save(registry_path)

    rejected = CliRunner().invoke(
        app,
        [
            "cluster",
            "pin-runtime",
            "--cluster",
            "frontier",
            "--clear",
            "--install-receipt",
            "$HOME/unexpected/install-receipt.json",
        ],
    )
    assert rejected.exit_code == 2
    assert ClusterRegistry.load(registry_path).require("frontier") == definition

    cleared = CliRunner().invoke(
        app,
        ["cluster", "pin-runtime", "--cluster", "frontier", "--clear"],
    )
    assert cleared.exit_code == 0, cleared.output
    updated = ClusterRegistry.load(registry_path).require("frontier")
    assert updated.relay_install_receipt is None
    assert updated.relay_executable == ClusterDefinition.model_fields["relay_executable"].default
    assert (
        updated.model_copy(
            update={
                "relay_executable": definition.relay_executable,
                "relay_install_receipt": definition.relay_install_receipt,
            }
        )
        == definition
    )


def test_cli_cluster_pin_runtime_rejects_an_unconfigured_cluster(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """clio-relay#205 follow-up: a typed error, not a silently-created entry."""
    monkeypatch.chdir(tmp_path)
    registry_path = tmp_path / ".clio-relay" / "clusters.json"
    ClusterRegistry(clusters={}).save(registry_path)

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "pin-runtime",
            "--cluster",
            "nonexistent",
            "--install-receipt",
            "$HOME/.local/share/clio-relay/install-receipt.json",
        ],
    )

    assert result.exit_code != 0
    assert result.exception is not None
    assert isinstance(result.exception, ConfigurationError)
    assert "nonexistent" in str(result.exception)
    assert ClusterRegistry.load(registry_path).clusters == {}


def test_cli_cluster_add_persists_direct_transport_optimization(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "add",
            "--name",
            "homelab",
            "--ssh-host",
            "homelab",
            "--direct-transport",
            "--direct-transport-mode",
            "xtcp",
            "--direct-transport-fallback",
            "xtcp,frp_stcp,queue",
        ],
    )

    assert result.exit_code == 0
    definition = ClusterRegistry.load(tmp_path / ".clio-relay" / "clusters.json").require("homelab")
    assert definition.frp_transport.direct.enabled is True
    assert definition.frp_transport.direct.mode == "xtcp"
    assert definition.frp_transport.direct.fallback_order == ["xtcp", "frp_stcp", "queue"]
    assert definition.frp_transport.server_addr == ""


def test_cli_cluster_add_rejects_direct_transport_without_queue_fallback(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "add",
            "--name",
            "homelab",
            "--ssh-host",
            "homelab",
            "--direct-transport",
            "--direct-transport-fallback",
            "xtcp,frp_stcp",
        ],
    )

    assert result.exit_code != 0
    assert "fallback_order must end with queue" in result.output
