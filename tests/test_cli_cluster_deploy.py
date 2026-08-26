"""Tests for the ``cluster`` deployment command group (iowarp/clio-relay#231).

These moved out of ``tests/test_cli.py`` alongside the six deployment
``cluster_app`` commands' (``probe``/``bootstrap``/``install-app``/
``install-endpoint-service``/``restart-endpoint-service``/
``endpoint-service-status``) extraction into
``src/clio_relay/cli_cluster_deploy.py``, per ground rule 3 (SS2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through this command group moves with the logic it exercises.

**Patch-target changes.** Most of these tests patch a collaborator module
directly (``bootstrap``/``bootstrap_acceptance``/``application_profiles``/
``deployment``/``cluster_probe``), or ``cli.RemoteMcpSchemaCache`` (a
class-attribute patch -- robust regardless of which module holds a
reference to the same class object), so the move needed no patch-target
changes there. Two things DID need updating because ``pinned_runtime_
present`` and ``_invalidate_remote_mcp_cache_after_bootstrap`` moved with
``cluster_bootstrap``, their sole caller: ``_bootstrap_cli_fakes`` (and
``test_dead_pin_repair_loop_probe_bootstrap_probe``, which also patches it
directly) now target ``cli_cluster_deploy.pinned_runtime_present`` instead
of ``cli.pinned_runtime_present`` (cli.py no longer bare-imports that name
at all), and ``test_bootstrap_same_generation_preserves_remote_mcp_cache``
now calls ``cli_cluster_deploy._invalidate_remote_mcp_cache_after_
bootstrap`` directly instead of ``cli._invalidate_remote_mcp_cache_after_
bootstrap``.

**Autouse-fixture parity.** ``test_cli.py`` defines its own module-scoped
``autouse=True`` ``_default_cli_mode`` fixture (env-var defaults every CLI
invocation there relies on). It is reproduced here (the env-var half only,
the same precedent ``tests/test_cli_gateway.py``'s own ``_default_cli_mode``
established) -- none of the tests in this file exercise the session-teardown
collaborator half of that fixture.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import cast

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

import clio_relay.application_profiles as application_profiles
import clio_relay.bootstrap as bootstrap
import clio_relay.bootstrap_acceptance as bootstrap_acceptance
import clio_relay.cli as cli
import clio_relay.cli_cluster_deploy as cli_cluster_deploy
import clio_relay.cli_remote_worker_probe as cli_remote_worker_probe
import clio_relay.cluster_probe as cluster_probe
import clio_relay.deployment as deployment
from clio_relay.bootstrap_pin import (
    BOOTSTRAP_PRODUCED_INSTALL_RECEIPT,
    BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE,
)
from clio_relay.cli import app
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry, ClusterTargetIdentity
from tests.test_cli import (
    _write_test_cluster,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)


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


def test_cli_cluster_install_app_uses_explicit_app_installer(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, name="delta")
    calls: list[tuple[str, str]] = []

    def fake_install_cluster_app_over_ssh(*, ssh_host: str, app_name: str) -> list[str]:
        calls.append((ssh_host, app_name))
        return ["site_stack=ready"]

    monkeypatch.setattr(
        application_profiles, "install_cluster_app_over_ssh", fake_install_cluster_app_over_ssh
    )

    result = CliRunner().invoke(
        app,
        ["cluster", "install-app", "--cluster", "delta", "--app", "site-stack"],
    )

    assert result.exit_code == 0
    assert calls == [("delta", "site-stack")]
    assert "site_stack=ready" in result.output


def test_cli_endpoint_service_requires_persistence_unless_explicitly_opted_out(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The operator-facing install defaults to persistent and names the diagnostic escape."""
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, name="delta")
    persistence_requests: list[bool] = []

    def fake_install_endpoint_user_service_over_ssh(
        *,
        cluster: str,
        ssh_host: str,
        service_text: str,
        start: bool,
        enable: bool,
        require_persistent: bool,
        timeout_seconds: float = 120.0,
    ) -> list[str]:
        del service_text, timeout_seconds
        assert cluster == "delta"
        assert ssh_host == "delta"
        assert start is True
        assert enable is True
        persistence_requests.append(require_persistent)
        return [
            "endpoint_service.persistence="
            + ("systemd-user-linger" if require_persistent else "login-scoped")
        ]

    monkeypatch.setattr(
        deployment,
        "install_endpoint_user_service_over_ssh",
        fake_install_endpoint_user_service_over_ssh,
    )

    persistent = CliRunner().invoke(
        app,
        ["cluster", "install-endpoint-service", "--cluster", "delta"],
    )
    login_scoped = CliRunner().invoke(
        app,
        [
            "cluster",
            "install-endpoint-service",
            "--cluster",
            "delta",
            "--allow-login-scoped",
        ],
    )

    assert persistent.exit_code == 0, persistent.output
    assert login_scoped.exit_code == 0, login_scoped.output
    assert persistence_requests == [True, False]
    assert "endpoint_service.persistence=systemd-user-linger" in persistent.output
    assert "endpoint_service.persistence=login-scoped" in login_scoped.output


def test_cli_cluster_install_app_rejects_option_like_ssh_override(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, name="delta")

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "install-app",
            "--cluster",
            "delta",
            "--app",
            "site-stack",
            "--ssh-host=-oProxyCommand=malicious-command",
        ],
    )

    assert result.exit_code == 1
    assert "ssh host must be one non-option destination" in result.output


def _bootstrap_cli_fakes(
    monkeypatch: MonkeyPatch,
    *,
    outcome: str = "noop_verified",
    pin_present: bool = False,
) -> None:
    """Install the standard cluster-bootstrap CLI fakes used by the pin tests.

    ``pin_present`` stands in for the remote presence probe: False models a pin
    proven absent on the host (repairable), True a custom pin that resolves.
    """

    def fake_pinned_runtime_present(_definition: ClusterDefinition) -> bool:
        return pin_present

    monkeypatch.setattr(cli_cluster_deploy, "pinned_runtime_present", fake_pinned_runtime_present)

    def fake_bootstrap_cluster_over_ssh(**_kwargs: object) -> list[str]:
        receipt = {
            "schema_version": "clio-relay.bootstrap-receipt.v2",
            "outcome": outcome,
            "invocation_id": "bootstrap_test",
            "bootstrap_profile": "linux-user",
            "relay_install_spec": "clio-relay==1.0.0",
            "install_receipt_sha256": "a" * 64,
            "completed_at": "2026-07-11T00:00:00Z",
        }
        return [
            "bootstrapped",
            "bootstrap_receipt=/home/test/.local/share/clio-relay/bootstrap-receipt.json",
            "bootstrap_invocation_id=bootstrap_test",
            "bootstrap_install_receipt_sha256=" + "a" * 64,
            "bootstrap_receipt_json=" + json.dumps(receipt, sort_keys=True),
        ]

    def fake_remote_target_identity(definition: ClusterDefinition) -> dict[str, object]:
        return {
            "schema_version": "clio-relay.cluster-target-info.v1",
            "hostname": definition.ssh_host,
            "fqdn": "ares.example.test",
            "scheduler_provider": "external",
            "scheduler_cluster_name": None,
            "site_marker_sha256": "b" * 64,
            "ssh_host": definition.ssh_host,
            "ssh_host_key_sha256": ["SHA256:test"],
            "expected_hostnames": ["ares.example.test"],
            "expected_ssh_host_key_sha256": ["SHA256:test"],
            "expected_scheduler_cluster_name": None,
            "expected_site_marker_sha256": "b" * 64,
            "verified": True,
        }

    def fake_bootstrap_reuse_acceptance_evidence(
        receipt: dict[str, object],
        *,
        elapsed_seconds: float | int,
    ) -> dict[str, object]:
        return {
            "schema_version": "clio-relay.bootstrap-reuse-acceptance.v1",
            "outcome": receipt["outcome"],
            "elapsed_seconds": float(elapsed_seconds),
            "maximum_seconds": 30.0,
            "payload_free": True,
            "scheduler_untouched": True,
            "jarvis_preserved": True,
            "component_actions": {},
            "service_operations": {},
        }

    monkeypatch.setattr(bootstrap, "bootstrap_cluster_over_ssh", fake_bootstrap_cluster_over_ssh)
    monkeypatch.setattr(
        cli_remote_worker_probe, "_remote_target_identity", fake_remote_target_identity
    )
    monkeypatch.setattr(
        bootstrap_acceptance,
        "bootstrap_reuse_acceptance_evidence",
        fake_bootstrap_reuse_acceptance_evidence,
    )


def test_cluster_bootstrap_repoints_a_stale_relay_executable_pin(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Bootstrap must leave the registry pointing at what it actually produced.

    clio-relay#158: bootstrap installs the relay at its canonical stable
    launcher, but the registry pin was never reconciled. A cluster pinned to a
    generation path that has since been garbage-collected therefore stayed
    pinned to the dead path -- bootstrap reported success (even
    ``noop_verified``, because its preflight probes the canonical path, not the
    pin) while every session command kept failing on the stale pointer. An
    install that leaves a dead pointer behind is a silent half-deploy.
    """
    monkeypatch.chdir(tmp_path)
    ClusterRegistry(
        clusters={
            "ares": ClusterDefinition(
                name="ares",
                ssh_host="ares",
                relay_executable="/srv/generations/gone/bin/clio-relay",
                relay_install_receipt="/srv/generations/gone/install-receipt.json",
            )
        }
    ).save(tmp_path / ".clio-relay" / "clusters.json")
    _bootstrap_cli_fakes(monkeypatch)

    result = CliRunner().invoke(
        app,
        ["cluster", "bootstrap", "--cluster", "ares"],
    )

    assert result.exit_code == 0
    saved = ClusterRegistry.load(tmp_path / ".clio-relay" / "clusters.json").require("ares")
    assert saved.relay_executable == BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE
    assert saved.relay_install_receipt == BOOTSTRAP_PRODUCED_INSTALL_RECEIPT
    # The repoint must be announced, never silent.
    assert "relay_executable_repointed" in result.output


def test_cluster_bootstrap_pins_a_freshly_observed_target_identity(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """clio-relay#209: a cold one-pass bootstrap's observation pins the identity.

    Closes the ``cluster pin-target`` manual-entry gap: an unpinned cluster
    that just went through the one-pass cold install gets its physical
    identity pinned from that SAME session's observation -- never a further
    ssh dial (``_remote_target_identity`` is poisoned here to prove it).
    """
    monkeypatch.chdir(tmp_path)
    ClusterRegistry(
        clusters={"ares": ClusterDefinition(name="ares", ssh_host="ares")}
    ).save(tmp_path / ".clio-relay" / "clusters.json")

    def fake_bootstrap_cluster_over_ssh(**_kwargs: object) -> list[str]:
        receipt = {
            "schema_version": "clio-relay.bootstrap-receipt.v2",
            "outcome": "installed",
            "invocation_id": "bootstrap_test",
            "bootstrap_profile": "linux-user",
            "relay_install_spec": "clio-relay==1.0.0",
            "install_receipt_sha256": "a" * 64,
            "completed_at": "2026-08-26T00:00:00Z",
        }
        identity = {
            "schema_version": "clio-relay.bootstrap-one-pass-target-identity.v1",
            "hostnames": ["ares-login-1", "ares-login-1.example.test"],
            "site_marker_sha256": "c" * 64,
        }
        return [
            "bootstrap_receipt=/home/test/.local/share/clio-relay/bootstrap-receipt.json",
            "bootstrap_receipt_json=" + json.dumps(receipt, sort_keys=True),
            "bootstrap_persistent_receipt_json=" + json.dumps(receipt, sort_keys=True),
            "bootstrap_target_identity_json=" + json.dumps(identity, sort_keys=True),
        ]

    def fake_ssh_host_key_fingerprints(ssh_host: str, **_kwargs: object) -> list[str]:
        assert ssh_host == "ares"
        return ["SHA256:fresh-fingerprint"]

    def poisoned_remote_target_identity(_definition: ClusterDefinition) -> dict[str, object]:
        raise AssertionError(
            "clio-relay#209: a fresh one-pass observation must never fall through "
            "to the separate re-verification dial"
        )

    def fake_bootstrap_reuse_acceptance_evidence(
        receipt: dict[str, object],
        *,
        elapsed_seconds: float | int,
    ) -> dict[str, object]:
        return {
            "schema_version": "clio-relay.bootstrap-reuse-acceptance.v1",
            "outcome": receipt["outcome"],
            "elapsed_seconds": float(elapsed_seconds),
            "maximum_seconds": 30.0,
            "payload_free": True,
            "scheduler_untouched": True,
            "jarvis_preserved": True,
            "component_actions": {},
            "service_operations": {},
        }

    monkeypatch.setattr(bootstrap, "bootstrap_cluster_over_ssh", fake_bootstrap_cluster_over_ssh)
    monkeypatch.setattr(
        cli_remote_worker_probe, "_ssh_host_key_fingerprints", fake_ssh_host_key_fingerprints
    )
    monkeypatch.setattr(
        cli_remote_worker_probe, "_remote_target_identity", poisoned_remote_target_identity
    )
    monkeypatch.setattr(
        bootstrap_acceptance,
        "bootstrap_reuse_acceptance_evidence",
        fake_bootstrap_reuse_acceptance_evidence,
    )

    result = CliRunner().invoke(app, ["cluster", "bootstrap", "--cluster", "ares"])

    assert result.exit_code == 0, result.output
    saved = ClusterRegistry.load(tmp_path / ".clio-relay" / "clusters.json").require("ares")
    assert saved.target_identity is not None
    assert saved.target_identity.hostnames == ["ares-login-1", "ares-login-1.example.test"]
    assert saved.target_identity.ssh_host_key_sha256 == ["SHA256:fresh-fingerprint"]
    assert saved.target_identity.site_marker_sha256 == "c" * 64


def test_cluster_bootstrap_never_overwrites_an_existing_target_identity_pin(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """An operator-pinned identity is never silently replaced by an observation.

    Falls through to the existing verify-only dial instead -- the same
    proven-before-repaired discipline ``reconcile_cluster_runtime_pin``
    already applies to the runtime pin.
    """
    monkeypatch.chdir(tmp_path)
    ClusterRegistry(
        clusters={
            "ares": ClusterDefinition(
                name="ares",
                ssh_host="ares",
                target_identity=ClusterTargetIdentity(
                    hostnames=["operator-pinned-hostname"],
                    ssh_host_key_sha256=["SHA256:operator-pinned"],
                ),
            )
        }
    ).save(tmp_path / ".clio-relay" / "clusters.json")

    def fake_bootstrap_cluster_over_ssh(**_kwargs: object) -> list[str]:
        receipt = {
            "schema_version": "clio-relay.bootstrap-receipt.v2",
            "outcome": "installed",
            "invocation_id": "bootstrap_test",
            "bootstrap_profile": "linux-user",
            "relay_install_spec": "clio-relay==1.0.0",
            "install_receipt_sha256": "a" * 64,
            "completed_at": "2026-08-26T00:00:00Z",
        }
        identity = {
            "schema_version": "clio-relay.bootstrap-one-pass-target-identity.v1",
            "hostnames": ["ares-login-1"],
            "site_marker_sha256": None,
        }
        return [
            "bootstrap_receipt=/home/test/.local/share/clio-relay/bootstrap-receipt.json",
            "bootstrap_receipt_json=" + json.dumps(receipt, sort_keys=True),
            "bootstrap_persistent_receipt_json=" + json.dumps(receipt, sort_keys=True),
            "bootstrap_target_identity_json=" + json.dumps(identity, sort_keys=True),
        ]

    verify_calls: list[ClusterDefinition] = []

    def fake_remote_target_identity(definition: ClusterDefinition) -> dict[str, object]:
        verify_calls.append(definition)
        return {"verified": True}

    def fake_bootstrap_reuse_acceptance_evidence(
        receipt: dict[str, object],
        *,
        elapsed_seconds: float | int,
    ) -> dict[str, object]:
        return {
            "schema_version": "clio-relay.bootstrap-reuse-acceptance.v1",
            "outcome": receipt["outcome"],
            "elapsed_seconds": float(elapsed_seconds),
            "maximum_seconds": 30.0,
            "payload_free": True,
            "scheduler_untouched": True,
            "jarvis_preserved": True,
            "component_actions": {},
            "service_operations": {},
        }

    monkeypatch.setattr(bootstrap, "bootstrap_cluster_over_ssh", fake_bootstrap_cluster_over_ssh)
    monkeypatch.setattr(
        cli_remote_worker_probe, "_remote_target_identity", fake_remote_target_identity
    )
    monkeypatch.setattr(
        bootstrap_acceptance,
        "bootstrap_reuse_acceptance_evidence",
        fake_bootstrap_reuse_acceptance_evidence,
    )

    result = CliRunner().invoke(app, ["cluster", "bootstrap", "--cluster", "ares"])

    assert result.exit_code == 0, result.output
    assert len(verify_calls) == 1
    saved = ClusterRegistry.load(tmp_path / ".clio-relay" / "clusters.json").require("ares")
    assert saved.target_identity is not None
    assert saved.target_identity.hostnames == ["operator-pinned-hostname"]


def test_cluster_bootstrap_preserves_a_valid_custom_pin(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Review F1, at the CLI boundary: a resolving custom pin survives bootstrap.

    The operator pinned this cluster at a shared-filesystem relay on purpose.
    Bootstrap must not relocate it onto the canonical launcher just because it
    installed one there.
    """
    monkeypatch.chdir(tmp_path)
    ClusterRegistry(
        clusters={
            "ares": ClusterDefinition(
                name="ares",
                ssh_host="ares",
                relay_executable="/mnt/common/tenant-b/bin/clio-relay",
                relay_install_receipt="/mnt/common/tenant-b/install-receipt.json",
            )
        }
    ).save(tmp_path / ".clio-relay" / "clusters.json")
    _bootstrap_cli_fakes(monkeypatch, pin_present=True)

    result = CliRunner().invoke(app, ["cluster", "bootstrap", "--cluster", "ares"])

    assert result.exit_code == 0, result.output
    saved = ClusterRegistry.load(tmp_path / ".clio-relay" / "clusters.json").require("ares")
    assert saved.relay_executable == "/mnt/common/tenant-b/bin/clio-relay"
    assert saved.relay_install_receipt == "/mnt/common/tenant-b/install-receipt.json"
    assert "relay_executable_pin_preserved=/mnt/common/tenant-b/bin/clio-relay" in result.output
    assert "relay_executable_repointed" not in result.output


def test_cluster_probe_command_reports_a_typed_state(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Review F5(a): the probe must be reachable and typed through the CLI."""
    monkeypatch.chdir(tmp_path)
    ClusterRegistry(
        clusters={
            "ares": ClusterDefinition(
                name="ares",
                ssh_host="ares",
                relay_executable="/srv/generations/gone/bin/clio-relay",
            )
        }
    ).save(tmp_path / ".clio-relay" / "clusters.json")

    def fake_run_remote_shell(_definition: ClusterDefinition, _script: str) -> str:
        return (
            "pinned_executable_present=false\n"
            "pinned_receipt_present=false\n"
            "produced_executable_present=true\n"
            "produced_receipt_present=true\n"
        )

    monkeypatch.setattr(cluster_probe, "run_remote_shell", fake_run_remote_shell)

    result = CliRunner().invoke(app, ["cluster", "probe", "--cluster", "ares"])

    assert result.exit_code == 0, result.output
    report = json.loads(result.output)
    assert report["schema_version"] == "clio-relay.cluster-probe.v1"
    assert report["state"] == "relay_executable_missing"
    assert report["repair"] == "clio-relay cluster bootstrap --cluster ares"


def test_dead_pin_repair_loop_probe_bootstrap_probe(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Review F5(b): the whole repair loop as ONE durable in-repo flow.

    probe(dead) -> bootstrap(repoint) -> probe(ready), over a fake SSH layer
    whose answers follow the registry, so the flow proves the pieces compose --
    not merely that each behaves in isolation.
    """
    monkeypatch.chdir(tmp_path)
    registry_path = tmp_path / ".clio-relay" / "clusters.json"
    ClusterRegistry(
        clusters={
            "ares": ClusterDefinition(
                name="ares",
                ssh_host="ares",
                relay_executable="/srv/generations/gone/bin/clio-relay",
                relay_install_receipt="/srv/generations/gone/install-receipt.json",
            )
        }
    ).save(registry_path)

    # The host carries ONLY the canonical launcher; the generation is gone.
    present_on_host = {
        BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE,
        BOOTSTRAP_PRODUCED_INSTALL_RECEIPT,
    }

    def fake_run_remote_shell(definition: ClusterDefinition, _script: str) -> str:
        def flag(path: str | None) -> str:
            return "true" if path in present_on_host else "false"

        return (
            f"pinned_executable_present={flag(definition.relay_executable)}\n"
            f"pinned_receipt_present={flag(definition.relay_install_receipt)}\n"
            f"produced_executable_present={flag(BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE)}\n"
            f"produced_receipt_present={flag(BOOTSTRAP_PRODUCED_INSTALL_RECEIPT)}\n"
        )

    monkeypatch.setattr(cluster_probe, "run_remote_shell", fake_run_remote_shell)

    first = CliRunner().invoke(app, ["cluster", "probe", "--cluster", "ares"])
    assert first.exit_code == 0, first.output
    assert json.loads(first.output)["state"] == "relay_executable_missing"

    _bootstrap_cli_fakes(monkeypatch)
    monkeypatch.setattr(
        cli_cluster_deploy, "pinned_runtime_present", cluster_probe.pinned_runtime_present
    )
    bootstrapped = CliRunner().invoke(app, ["cluster", "bootstrap", "--cluster", "ares"])
    assert bootstrapped.exit_code == 0, bootstrapped.output
    assert "relay_executable_repointed" in bootstrapped.output

    healed = CliRunner().invoke(app, ["cluster", "probe", "--cluster", "ares"])
    assert healed.exit_code == 0, healed.output
    healed_report = json.loads(healed.output)
    assert healed_report["state"] == "ready"
    assert healed_report["repair"] is None


def test_cluster_bootstrap_leaves_an_already_correct_pin_untouched(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Reconciling the pin is idempotent and stays quiet when nothing changed."""
    monkeypatch.chdir(tmp_path)
    ClusterRegistry(
        clusters={
            "ares": ClusterDefinition(
                name="ares",
                ssh_host="ares",
                relay_executable=BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE,
                relay_install_receipt=BOOTSTRAP_PRODUCED_INSTALL_RECEIPT,
            )
        }
    ).save(tmp_path / ".clio-relay" / "clusters.json")
    _bootstrap_cli_fakes(monkeypatch)

    result = CliRunner().invoke(
        app,
        ["cluster", "bootstrap", "--cluster", "ares"],
    )

    assert result.exit_code == 0
    saved = ClusterRegistry.load(tmp_path / ".clio-relay" / "clusters.json").require("ares")
    assert saved.relay_executable == BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE
    assert "relay_executable_repointed" not in result.output


def test_cli_cluster_bootstrap_uses_package_source_root(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(
        tmp_path,
        jarvis_resource_graph_profile="ares",
        allow_jarvis_resource_graph_build=True,
    )
    package_root = tmp_path / "package-root"
    wheel = tmp_path / "clio_relay-0.0.0-py3-none-any.whl"
    wheel.write_bytes(b"wheel")
    captured: dict[str, object] = {}

    def fake_package_source_root() -> Path:
        return package_root

    def fake_bootstrap_cluster_over_ssh(**kwargs: object) -> list[str]:
        captured.update(kwargs)
        receipt = {
            "schema_version": "clio-relay.bootstrap-receipt.v2",
            "outcome": "noop_verified",
            "invocation_id": "bootstrap_test",
            "bootstrap_profile": "linux-user",
            "relay_install_spec": "clio-relay==1.0.0",
            "install_receipt_sha256": "a" * 64,
            "completed_at": "2026-07-11T00:00:00Z",
        }
        return [
            "bootstrapped",
            "bootstrap_receipt=/home/test/.local/share/clio-relay/bootstrap-receipt.json",
            "bootstrap_invocation_id=bootstrap_test",
            "bootstrap_install_receipt_sha256=" + "a" * 64,
            "bootstrap_receipt_json=" + json.dumps(receipt, sort_keys=True),
        ]

    def fake_remote_target_identity(
        definition: ClusterDefinition,
    ) -> dict[str, object]:
        assert definition.name == "ares"
        assert definition.ssh_host == "ares"
        return {
            "schema_version": "clio-relay.cluster-target-info.v1",
            "hostname": "ares",
            "fqdn": "ares.example.test",
            "scheduler_provider": "external",
            "scheduler_cluster_name": None,
            "site_marker_sha256": "b" * 64,
            "ssh_host": "ares",
            "ssh_host_key_sha256": ["SHA256:test"],
            "expected_hostnames": ["ares.example.test"],
            "expected_ssh_host_key_sha256": ["SHA256:test"],
            "expected_scheduler_cluster_name": None,
            "expected_site_marker_sha256": "b" * 64,
            "verified": True,
        }

    def fake_bootstrap_reuse_acceptance_evidence(
        receipt: dict[str, object],
        *,
        elapsed_seconds: float | int,
    ) -> dict[str, object]:
        assert receipt["outcome"] == "noop_verified"
        assert 0 <= elapsed_seconds < 30
        return {
            "schema_version": "clio-relay.bootstrap-reuse-acceptance.v1",
            "outcome": "noop_verified",
            "elapsed_seconds": float(elapsed_seconds),
            "maximum_seconds": 30.0,
            "payload_free": True,
            "scheduler_untouched": True,
            "jarvis_preserved": True,
            "component_actions": {},
            "service_operations": {},
        }

    monkeypatch.setattr(bootstrap, "package_source_root", fake_package_source_root)
    monkeypatch.setattr(bootstrap, "bootstrap_cluster_over_ssh", fake_bootstrap_cluster_over_ssh)
    monkeypatch.setattr(
        cli_remote_worker_probe, "_remote_target_identity", fake_remote_target_identity
    )
    monkeypatch.setattr(
        bootstrap_acceptance,
        "bootstrap_reuse_acceptance_evidence",
        fake_bootstrap_reuse_acceptance_evidence,
    )

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "bootstrap",
            "--cluster",
            "ares",
            "--relay-wheel",
            str(wheel),
            "--relay-artifact-sha256",
            hashlib.sha256(b"wheel").hexdigest(),
        ],
    )

    assert result.exit_code == 0
    output_lines = result.output.splitlines()
    assert output_lines[:-1] == [
        "bootstrapped",
        "bootstrap_receipt=/home/test/.local/share/clio-relay/bootstrap-receipt.json",
        "bootstrap_invocation_id=bootstrap_test",
        "bootstrap_install_receipt_sha256=" + "a" * 64,
        "bootstrap_receipt_json="
        + json.dumps(
            {
                "schema_version": "clio-relay.bootstrap-receipt.v2",
                "outcome": "noop_verified",
                "invocation_id": "bootstrap_test",
                "bootstrap_profile": "linux-user",
                "relay_install_spec": "clio-relay==1.0.0",
                "install_receipt_sha256": "a" * 64,
                "completed_at": "2026-07-11T00:00:00Z",
            },
            sort_keys=True,
        ),
    ]
    assert output_lines[-1].startswith("validation.report=")
    report_path = Path(output_lines[-1].partition("=")[2])
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "passed"
    assert report["checks"][0]["check_id"] == "cluster.bootstrap"
    assert report["checks"][1]["check_id"] == "worker.target-identity"
    assert report["checks"][1]["evidence"][0]["kind"] == "cluster_target"
    # The runtime pin is reconciled only after the physical target verifies.
    assert report["checks"][2]["check_id"] == "cluster.bootstrap.runtime-pin"
    assert report["checks"][2]["evidence"][0]["kind"] == "bootstrap_runtime_pin"
    assert report["checks"][3]["check_id"] == "cluster.bootstrap.reuse-slo"
    reuse_evidence = report["checks"][3]["evidence"][0]
    assert reuse_evidence["kind"] == "bootstrap_reuse_acceptance"
    assert reuse_evidence["reference"] == "bootstrap-reuse:bootstrap_test"
    assert reuse_evidence["metadata"]["payload_free"] is True
    cluster_target = next(
        resource for resource in report["resources"] if resource["kind"] == "cluster_target"
    )
    assert cluster_target["resource_id"] == "target:ares"
    assert cluster_target["role"] == "physical_cluster_target"
    assert cluster_target["metadata"]["verified"] is True
    assert captured["ssh_host"] == "ares"
    assert captured["source_root"] == package_root
    assert captured["source_root"] != tmp_path
    assert captured["relay_artifact_sha256"] == hashlib.sha256(b"wheel").hexdigest()
    assert captured["relay_wheel"] == wheel
    assert captured["jarvis_resource_graph_profile"] == "ares"
    assert captured["allow_jarvis_resource_graph_build"] is True


def test_bootstrap_same_generation_preserves_remote_mcp_cache(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    def reject_invalidation(
        _cls: type[object],
        _path: Path,
        _cluster: str,
    ) -> tuple[object, tuple[str, ...]]:
        raise AssertionError("same-generation bootstrap must preserve cached MCP schemas")

    monkeypatch.setattr(
        cli.RemoteMcpSchemaCache,
        "invalidate_cluster_entries",
        classmethod(reject_invalidation),
    )

    evidence = cli_cluster_deploy._invalidate_remote_mcp_cache_after_bootstrap(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cluster="ares",
        receipt={
            "generation": {
                "previous": "a" * 64,
                "active": "a" * 64,
            }
        },
        registry_path=tmp_path / "clusters.json",
    )

    assert evidence == {
        "schema_version": "clio-relay.remote-mcp-cache-invalidation.v1",
        "cluster": "ares",
        "previous_generation": "a" * 64,
        "active_generation": "a" * 64,
        "generation_changed": False,
        "action": "preserved",
        "removed_server_count": 0,
        "removed_server_names": [],
    }


def test_cluster_bootstrap_invalidates_cache_before_target_validation_and_records_evidence(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    report_path = tmp_path / "bootstrap-report.json"
    events: list[str] = []

    def fake_bootstrap_cluster_over_ssh(**_kwargs: object) -> list[str]:
        receipt = {
            "schema_version": "clio-relay.bootstrap-receipt.v2",
            "outcome": "reconciled",
            "invocation_id": "bootstrap_generation_changed",
            "generation": {
                "previous": "a" * 64,
                "active": "b" * 64,
            },
        }
        return [
            "bootstrap_receipt=/home/test/.local/share/clio-relay/bootstrap-receipt.json",
            "bootstrap_receipt_json=" + json.dumps(receipt, sort_keys=True),
        ]

    def invalidate_cluster_entries(
        cls: type[cli.RemoteMcpSchemaCache],
        path: Path,
        cluster: str,
    ) -> tuple[cli.RemoteMcpSchemaCache, tuple[str, ...]]:
        events.append("cache-invalidated")
        assert path == (tmp_path / ".clio-relay" / "remote-mcp-cache.json").resolve()
        assert cluster == "ares"
        return cls(), ("__builtin_jarvis__", "jarvis-demo")

    def fake_remote_target_identity(
        _definition: ClusterDefinition,
    ) -> dict[str, object]:
        assert events == ["cache-invalidated"]
        events.append("target-validated")
        return {"verified": True}

    monkeypatch.setattr(bootstrap, "package_source_root", lambda: tmp_path / "package")
    monkeypatch.setattr(bootstrap, "bootstrap_cluster_over_ssh", fake_bootstrap_cluster_over_ssh)
    monkeypatch.setattr(
        cli_remote_worker_probe, "_remote_target_identity", fake_remote_target_identity
    )
    monkeypatch.setattr(
        cli.RemoteMcpSchemaCache,
        "invalidate_cluster_entries",
        classmethod(invalidate_cluster_entries),
    )

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "bootstrap",
            "--cluster",
            "ares",
            "--report",
            str(report_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert events == ["cache-invalidated", "target-validated"]
    report = json.loads(report_path.read_text(encoding="utf-8"))
    bootstrap_check = cast(dict[str, object], report["checks"][0])
    evidence = cast(list[dict[str, object]], bootstrap_check["evidence"])
    cache_evidence = next(
        item for item in evidence if item["kind"] == "remote_mcp_cache_invalidation"
    )
    assert cache_evidence["metadata"] == {
        "schema_version": "clio-relay.remote-mcp-cache-invalidation.v1",
        "cluster": "ares",
        "previous_generation": "a" * 64,
        "active_generation": "b" * 64,
        "generation_changed": True,
        "action": "invalidated",
        "removed_server_count": 2,
        "removed_server_names": ["__builtin_jarvis__", "jarvis-demo"],
    }
    bootstrap_resource = next(
        resource for resource in report["resources"] if resource["kind"] == "bootstrap_invocation"
    )
    assert (
        bootstrap_resource["metadata"]["remote_mcp_cache_invalidation"]
        == cache_evidence["metadata"]
    )
