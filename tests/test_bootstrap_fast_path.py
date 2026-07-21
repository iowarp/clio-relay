"""Focused acceptance contracts for payload-free repeated cluster bootstrap."""

from __future__ import annotations

import base64
import copy
import subprocess
import tarfile
from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path
from typing import NoReturn, cast

import pytest
from typer.testing import CliRunner

import clio_relay.bootstrap as bootstrap
import clio_relay.cli as cli
from clio_relay import __version__
from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
    BootstrapInspection,
    BootstrapReadinessEvidence,
    BootstrapTransactionJournal,
    BootstrapTransactionState,
    JarvisStateEvidence,
    make_bootstrap_receipt,
)
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry
from clio_relay.errors import ConfigurationError, RelayError


def _verify_persistent_receipt(**_kwargs: object) -> None:
    """Model a successfully re-read persistent receipt."""


def _which(executable: str) -> str:
    """Return a deterministic executable resolution for bootstrap tests."""

    return executable


def test_exact_remote_bootstrap_never_reads_or_builds_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A released-wheel no-op ends after remote evidence and receipt verification."""
    digest = "a" * 64
    wheel = tmp_path / f"clio_relay-{__version__}-py3-none-any.whl"
    source_root = tmp_path / "poison-source"
    identity = bootstrap.bootstrap_relay_identity(
        source_root=source_root,
        relay_wheel=wheel,
        relay_artifact_sha256=digest,
    )
    desired = bootstrap._bootstrap_desired_state(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        identity=identity,
        cluster="ares",
        core_dir=bootstrap.DEFAULT_REMOTE_CORE_DIR,
        spool_dir=bootstrap.DEFAULT_REMOTE_SPOOL_DIR,
        frp_version=bootstrap.FRP_VERSION,
        clio_kit_install_spec=bootstrap.CLIO_KIT_JARVIS_MCP_WHEEL_URL,
        clio_kit_artifact_sha256=bootstrap.CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
        agent_adapter="exec",
        agent_npm_package=None,
        agent_npm_bin=None,
        agent_args=[],
        jarvis_resource_graph_profile="ares",
    )
    jarvis_state = JarvisStateEvidence(
        initialized=True,
        root="/home/operator/.ppi-jarvis",
        roots={
            "config_dir": "/operator/jarvis/config",
            "private_dir": "/operator/jarvis/private",
            "shared_dir": "/operator/jarvis/shared",
        },
        config_sha256="b" * 64,
        repos_sha256="c" * 64,
        resource_graph_sha256="d" * 64,
        managed_repo_registered=True,
    )
    inspection = BootstrapInspection(
        exact_match=True,
        desired_fingerprint=desired.fingerprint,
        install_receipt_sha256="e" * 64,
        active_generation=desired.fingerprint,
        current_generation_target=f"/home/operator/generations/{desired.fingerprint}",
        jarvis_state=jarvis_state,
        readiness=BootstrapReadinessEvidence(
            service_name=desired.worker_service,
            service_was_active=True,
            service_was_enabled=True,
            queue_ready=True,
            queue={
                "schema_version": "clio-relay.queue-readiness.v1",
                "complete": True,
                "sealed": True,
                "repair_required": False,
            },
            worker_ready=True,
            worker={"running": True},
        ),
    )
    receipt = make_bootstrap_receipt(
        invocation_id="bootstrap_test",
        desired=desired,
        outcome="noop_verified",
        inspection=inspection,
        started_at=datetime.now(UTC),
        transaction=None,
        previous_generation=desired.fingerprint,
        active_generation=desired.fingerprint,
    )
    observed_desired: list[str] = []

    def preflight(**kwargs: object) -> bootstrap.BootstrapPreflightResult:
        requested = kwargs["desired"]
        assert hasattr(requested, "fingerprint")
        observed_desired.append(requested.fingerprint)  # type: ignore[attr-defined]
        return bootstrap.BootstrapPreflightResult(
            action="exact",
            receipt=receipt,
            lines=["bootstrap_preflight_json={}"],
        )

    def poison(*_args: object, **_kwargs: object) -> NoReturn:
        raise AssertionError("the exact no-op touched bootstrap payload code")

    monkeypatch.setattr(bootstrap, "_bootstrap_preflight_over_ssh", preflight)
    monkeypatch.setattr(
        bootstrap,
        "_verify_persistent_bootstrap_receipt",
        _verify_persistent_receipt,
    )
    monkeypatch.setattr(bootstrap, "create_bootstrap_archive", poison)
    monkeypatch.setattr(bootstrap, "_validate_relay_bootstrap_wheel", poison)
    monkeypatch.setattr(bootstrap.shutil, "which", _which)
    monkeypatch.setattr(bootstrap, "uuid4", lambda: type("Uuid", (), {"hex": "test"})())

    lines = bootstrap.bootstrap_cluster_over_ssh(
        bootstrap_profile="linux-user",
        ssh_host="ares",
        source_root=source_root,
        cluster="ares",
        relay_wheel=wheel,
        relay_artifact_sha256=digest,
        jarvis_resource_graph_profile="ares",
    )

    assert observed_desired == [desired.fingerprint]
    assert any(line.startswith("bootstrap_receipt_json=") for line in lines)
    assert receipt["jarvis_commands"] == {"count": 0, "argv": []}
    operations = receipt["operations"]
    assert isinstance(operations, dict)
    assert operations["payload_transfer_count"] == 0
    assert operations["payload_transfer_bytes"] == 0


def test_public_cluster_bootstrap_noop_never_touches_nonexistent_wheel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real public command reaches exact evidence before touching payload bytes."""
    monkeypatch.chdir(tmp_path)
    ClusterRegistry(
        clusters={
            "cluster-a": ClusterDefinition(
                name="cluster-a",
                ssh_host="cluster-a.example.test",
            )
        }
    ).save(tmp_path / ".clio-relay/clusters.json")
    digest = "a" * 64
    wheel = tmp_path / f"clio_relay-{__version__}-py3-none-any.whl"

    def preflight(**kwargs: object) -> bootstrap.BootstrapPreflightResult:
        desired = kwargs["desired"]
        invocation_id = kwargs["invocation_id"]
        assert isinstance(desired, BootstrapDesiredState)
        assert isinstance(invocation_id, str)
        jarvis_state = JarvisStateEvidence(
            initialized=True,
            root="/home/operator/.ppi-jarvis",
            roots={
                "config_dir": "/operator/jarvis/config",
                "private_dir": "/operator/jarvis/private",
                "shared_dir": "/operator/jarvis/shared",
            },
            config_sha256="b" * 64,
            repos_sha256="c" * 64,
            resource_graph_sha256="d" * 64,
            managed_repo_registered=True,
        )
        inspection = BootstrapInspection(
            exact_match=True,
            desired_fingerprint=desired.fingerprint,
            install_receipt_sha256="e" * 64,
            active_generation=desired.fingerprint,
            current_generation_target=f"/home/operator/generations/{desired.fingerprint}",
            jarvis_state=jarvis_state,
            readiness=BootstrapReadinessEvidence(
                service_name=desired.worker_service,
                service_was_active=True,
                service_was_enabled=True,
                queue_ready=True,
                queue={
                    "schema_version": "clio-relay.queue-readiness.v1",
                    "complete": True,
                    "sealed": True,
                    "repair_required": False,
                },
                worker_ready=True,
                worker={"running": True},
            ),
        )
        receipt = make_bootstrap_receipt(
            invocation_id=invocation_id,
            desired=desired,
            outcome="noop_verified",
            inspection=inspection,
            started_at=datetime.now(UTC),
            transaction=None,
            previous_generation=desired.fingerprint,
            active_generation=desired.fingerprint,
        )
        return bootstrap.BootstrapPreflightResult(
            action="exact",
            receipt=receipt,
            lines=["bootstrap_preflight_json={}"],
        )

    def poison(*_args: object, **_kwargs: object) -> NoReturn:
        raise AssertionError("the public exact no-op touched bootstrap payload code")

    monkeypatch.setattr(bootstrap, "_bootstrap_preflight_over_ssh", preflight)
    monkeypatch.setattr(
        bootstrap,
        "_verify_persistent_bootstrap_receipt",
        _verify_persistent_receipt,
    )
    monkeypatch.setattr(bootstrap, "create_bootstrap_archive", poison)
    monkeypatch.setattr(bootstrap, "_validate_relay_bootstrap_wheel", poison)
    monkeypatch.setattr(bootstrap.shutil, "which", _which)
    monkeypatch.setattr(bootstrap, "uuid4", lambda: type("Uuid", (), {"hex": "cli_test"})())
    monkeypatch.setattr(cli, "package_source_root", lambda: tmp_path / "missing-source")

    def remote_target_identity(_definition: ClusterDefinition) -> dict[str, object]:
        return {"verified": True}

    monkeypatch.setattr(cli, "_remote_target_identity", remote_target_identity)

    result = CliRunner().invoke(
        cli.app,
        [
            "cluster",
            "bootstrap",
            "--cluster",
            "cluster-a",
            "--relay-wheel",
            str(wheel),
            "--relay-artifact-sha256",
            digest,
        ],
    )

    assert result.exit_code == 0, result.output
    assert not wheel.exists()


def test_payload_reconcile_requires_profile_before_building_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only exact reuse may omit a profile; fresh state needs operator graph policy."""

    def preflight(**_kwargs: object) -> bootstrap.BootstrapPreflightResult:
        return bootstrap.BootstrapPreflightResult(
            action="payload_required",
            receipt=None,
            lines=["bootstrap_preflight_json={}"],
        )

    monkeypatch.setattr(bootstrap, "_bootstrap_preflight_over_ssh", preflight)
    monkeypatch.setattr(bootstrap.shutil, "which", _which)

    def poison(*_args: object, **_kwargs: object) -> NoReturn:
        raise AssertionError("missing profile reached payload construction")

    monkeypatch.setattr(bootstrap, "create_bootstrap_archive", poison)

    with pytest.raises(ConfigurationError, match="operator-selected"):
        bootstrap.bootstrap_cluster_over_ssh(
            bootstrap_profile="linux-user",
            ssh_host="cluster-a",
            source_root=tmp_path / "not-a-checkout",
            cluster="cluster-a",
            relay_artifact_sha256="a" * 64,
        )


def test_public_release_bootstrap_requires_artifact_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A release invocation cannot collapse rebuilt wheels into one identity."""
    monkeypatch.chdir(tmp_path)
    ClusterRegistry(
        clusters={
            "cluster-a": ClusterDefinition(
                name="cluster-a",
                ssh_host="cluster-a.example.test",
            )
        }
    ).save(tmp_path / ".clio-relay/clusters.json")
    monkeypatch.setattr(cli, "package_source_root", lambda: tmp_path / "installed-package")
    monkeypatch.setattr(bootstrap.shutil, "which", _which)

    result = CliRunner().invoke(
        cli.app,
        ["cluster", "bootstrap", "--cluster", "cluster-a"],
    )

    assert result.exit_code != 0
    assert "--relay-artifact-sha256" in result.output


def test_payload_free_inspector_fails_closed_after_repair_does_not_converge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A supported inspector never asks the desktop for payload after mutation."""
    desired = bootstrap._bootstrap_desired_state(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        identity=bootstrap.BootstrapRelayIdentity(
            install_spec=f"clio-relay=={__version__}",
            transport_install_spec=f"clio-relay=={__version__}",
            source_identity=f"release:clio-relay=={__version__}:sha256:{'a' * 64}",
            deployment_artifact_sha256="a" * 64,
        ),
        cluster="cluster-a",
        core_dir=bootstrap.DEFAULT_REMOTE_CORE_DIR,
        spool_dir=bootstrap.DEFAULT_REMOTE_SPOOL_DIR,
        frp_version=bootstrap.FRP_VERSION,
        clio_kit_install_spec=bootstrap.CLIO_KIT_JARVIS_MCP_WHEEL_URL,
        clio_kit_artifact_sha256=bootstrap.CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
        agent_adapter="exec",
        agent_npm_package=None,
        agent_npm_bin=None,
        agent_args=[],
        jarvis_resource_graph_profile="ares",
    )
    monkeypatch.setenv(
        "CLIO_RELAY_BOOTSTRAP_DESIRED_STATE_BASE64",
        base64.b64encode(desired.model_dump_json().encode()).decode(),
    )
    state = JarvisStateEvidence(
        initialized=True,
        root=str(tmp_path / ".ppi-jarvis"),
        config_sha256="b" * 64,
        repos_sha256="c" * 64,
        resource_graph_sha256="d" * 64,
        managed_repo_registered=True,
    )
    initial = BootstrapInspection(
        exact_match=False,
        desired_fingerprint=desired.fingerprint,
        reasons=["managed endpoint service is inactive"],
        jarvis_state=state,
        readiness=BootstrapReadinessEvidence(
            service_name=desired.worker_service,
            service_was_active=False,
            service_was_enabled=True,
            queue_ready=True,
            worker_ready=False,
        ),
    )
    failed_repair = initial.model_copy(
        update={
            "reasons": ["active endpoint worker readiness did not verify"],
            "readiness": initial.readiness.model_copy(
                update={"service_was_active": True, "worker_ready": False}
            ),
        }
    )
    inspections = iter((initial, failed_repair))

    class ReadyQueue:
        def __init__(self, _root: Path) -> None:
            pass

        def readiness_info(self) -> dict[str, object]:
            return {
                "schema_version": "clio-relay.queue-readiness.v1",
                "complete": True,
                "sealed": True,
                "repair_required": False,
            }

    def systemctl(
        command: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        if "is-active" in command:
            return subprocess.CompletedProcess(command, 3, "", "")
        stdout = "loaded\n" if "show" in command else ""
        return subprocess.CompletedProcess(command, 0, stdout, "")

    def installation_info() -> dict[str, object]:
        return {}

    def inspect(*_args: object, **_kwargs: object) -> BootstrapInspection:
        return next(inspections)

    def worker_info(**_kwargs: object) -> dict[str, object]:
        return {"running": True}

    def invocation_lock(**_kwargs: object) -> nullcontext[Path]:
        return nullcontext(tmp_path / "bootstrap.lock")

    monkeypatch.setattr(cli, "installation_info", installation_info)
    monkeypatch.setattr(cli, "ClioCoreQueue", ReadyQueue)
    monkeypatch.setattr(cli, "inspect_exact_bootstrap_noop", inspect)
    monkeypatch.setattr(cli, "run_bounded_process", systemctl)
    monkeypatch.setattr(cli, "worker_runtime_info", worker_info)
    monkeypatch.setattr(cli, "bootstrap_invocation_lock", invocation_lock)

    result = CliRunner().invoke(
        cli.app,
        ["bootstrap-inspect", "--invocation-id", "bootstrap_fail_closed", "--repair"],
    )

    assert result.exit_code != 0
    assert "repair did not converge" in result.output
    assert "bootstrap_preflight_json=" not in result.output


def test_bootstrap_inspection_deadlines_match_acceptance_contract() -> None:
    assert 0 < cli.BOOTSTRAP_EXACT_INSPECTION_DEADLINE_SECONDS < 30
    assert (
        cli.BOOTSTRAP_EXACT_INSPECTION_DEADLINE_SECONDS < cli.BOOTSTRAP_REPAIR_DEADLINE_SECONDS < 60
    )


def test_component_upgrade_receipt_accepts_only_bound_managed_repo_registration() -> None:
    """A fenced upgrade may register relay's exact repository without changing other state."""
    desired = bootstrap._bootstrap_desired_state(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        identity=bootstrap.BootstrapRelayIdentity(
            install_spec=f"clio-relay=={__version__}",
            transport_install_spec=f"clio-relay=={__version__}",
            source_identity=f"release:clio-relay=={__version__}:sha256:{'a' * 64}",
            deployment_artifact_sha256="a" * 64,
        ),
        cluster="ares",
        core_dir=bootstrap.DEFAULT_REMOTE_CORE_DIR,
        spool_dir=bootstrap.DEFAULT_REMOTE_SPOOL_DIR,
        frp_version=bootstrap.FRP_VERSION,
        clio_kit_install_spec=bootstrap.CLIO_KIT_JARVIS_MCP_WHEEL_URL,
        clio_kit_artifact_sha256=bootstrap.CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
        agent_adapter="exec",
        agent_npm_package=None,
        agent_npm_bin=None,
        agent_args=[],
        jarvis_resource_graph_profile="ares",
    )
    before = JarvisStateEvidence(
        initialized=True,
        root="/home/operator/.ppi-jarvis",
        roots={
            "config_dir": "/operator/jarvis/config",
            "private_dir": "/operator/jarvis/private",
            "shared_dir": "/operator/jarvis/shared",
        },
        config_sha256="b" * 64,
        repos_sha256="c" * 64,
        resource_graph_sha256="d" * 64,
        managed_repo_registered=False,
    )
    after = before.model_copy(update={"repos_sha256": "e" * 64, "managed_repo_registered": True})
    inspection = BootstrapInspection(
        exact_match=True,
        desired_fingerprint=desired.fingerprint,
        install_receipt_sha256="f" * 64,
        active_generation=desired.fingerprint,
        current_generation_target=f"/home/operator/generations/{desired.fingerprint}",
        jarvis_state=after,
        readiness=BootstrapReadinessEvidence(
            service_name=desired.worker_service,
            service_was_active=True,
            service_was_enabled=True,
            queue_ready=True,
            queue={
                "schema_version": "clio-relay.queue-readiness.v1",
                "complete": True,
                "sealed": True,
                "repair_required": False,
            },
            worker_ready=True,
        ),
    )
    transaction = BootstrapTransactionJournal(
        invocation_id="bootstrap_component_upgrade",
        desired_fingerprint=desired.fingerprint,
        mode="component-upgrade",
        state=BootstrapTransactionState.COMMITTED,
        previous_generation="legacy",
        prepared_generation=desired.fingerprint,
        service_name=desired.worker_service,
        service_was_active=True,
        service_was_enabled=True,
        irreversible_boundary=True,
    )
    actions = {
        "clio-relay": "replaced",
        "clio-kit": "replaced",
        "jarvis-cd": "replaced",
        "jarvis-util": "reused",
        "frp": "reused",
        "uv": "reused",
    }
    components: dict[str, dict[str, object]] = {
        name: {
            "action": action,
            "observed_identity": {},
            "duration_seconds": 1.0,
        }
        for name, action in actions.items()
    }
    managed_repo = "/home/operator/.local/share/clio-relay/managed-jarvis-repo"
    repository_update: dict[str, object] = {
        "link_action": "reused",
        "link": managed_repo,
        "target": (
            "/home/operator/.local/share/clio-relay/current/source/jarvis-packages/clio_relay"
        ),
        "repositories": {
            "action": "updated",
            "managed_repo": managed_repo,
            "added_managed_repos": [managed_repo],
            "removed_previous_managed_repos": [
                "/home/operator/.local/src/clio-relay/jarvis-packages/clio_relay"
            ],
            "before_sha256": before.repos_sha256,
            "after_sha256": after.repos_sha256,
        },
    }
    receipt = make_bootstrap_receipt(
        invocation_id=transaction.invocation_id,
        desired=desired,
        outcome="reconciled",
        inspection=inspection,
        started_at=datetime.now(UTC),
        transaction=transaction,
        previous_generation="legacy",
        active_generation=desired.fingerprint,
        components=components,
        duration_seconds=1.0,
        jarvis_state_before=before,
        jarvis_repo_reconciliation=repository_update,
        payload_transfer_count=2,
        payload_transfer_bytes=1,
    )

    bootstrap._validate_bootstrap_receipt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        receipt,
        bootstrap_profile="linux-user",
        relay_install_spec=desired.relay_install_spec,
        desired_fingerprint=desired.fingerprint,
        expected_jarvis_resource_graph_profile="ares",
        expected_allow_jarvis_resource_graph_build=False,
        expected_worker_service=desired.worker_service,
    )

    tampered = copy.deepcopy(receipt)
    preservation = cast(dict[str, object], tampered["jarvis_preservation"])
    binding = cast(dict[str, object], preservation["repositories"])
    update = cast(dict[str, object], binding["repositories"])
    update["after_sha256"] = "0" * 64
    with pytest.raises(RelayError, match="hashes do not bind"):
        bootstrap._validate_bootstrap_receipt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            tampered,
            bootstrap_profile="linux-user",
            relay_install_spec=desired.relay_install_spec,
            desired_fingerprint=desired.fingerprint,
            expected_jarvis_resource_graph_profile="ares",
            expected_allow_jarvis_resource_graph_build=False,
            expected_worker_service=desired.worker_service,
        )

    unauthorized_removal = copy.deepcopy(receipt)
    preservation = cast(dict[str, object], unauthorized_removal["jarvis_preservation"])
    binding = cast(dict[str, object], preservation["repositories"])
    update = cast(dict[str, object], binding["repositories"])
    update["removed_previous_managed_repos"] = ["/operator/unrelated-repository"]
    with pytest.raises(RelayError, match="repository migration is invalid"):
        bootstrap._validate_bootstrap_receipt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            unauthorized_removal,
            bootstrap_profile="linux-user",
            relay_install_spec=desired.relay_install_spec,
            desired_fingerprint=desired.fingerprint,
            expected_jarvis_resource_graph_profile="ares",
            expected_allow_jarvis_resource_graph_build=False,
            expected_worker_service=desired.worker_service,
        )

    replaced_link = copy.deepcopy(receipt)
    preservation = cast(dict[str, object], replaced_link["jarvis_preservation"])
    binding = cast(dict[str, object], preservation["repositories"])
    binding["link_action"] = "retargeted"
    with pytest.raises(RelayError, match="did not preserve existing JARVIS state"):
        bootstrap._validate_bootstrap_receipt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            replaced_link,
            bootstrap_profile="linux-user",
            relay_install_spec=desired.relay_install_spec,
            desired_fingerprint=desired.fingerprint,
            expected_jarvis_resource_graph_profile="ares",
            expected_allow_jarvis_resource_graph_build=False,
            expected_worker_service=desired.worker_service,
        )


def test_fresh_bootstrap_receipt_allows_explicit_pending_service_install(
    tmp_path: Path,
) -> None:
    """Unit creation remains the documented explicit command after fresh bootstrap."""
    desired = bootstrap._bootstrap_desired_state(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        identity=bootstrap.BootstrapRelayIdentity(
            install_spec=f"clio-relay=={__version__}",
            transport_install_spec=f"clio-relay=={__version__}",
            source_identity=f"release:clio-relay=={__version__}:sha256:{'a' * 64}",
            deployment_artifact_sha256="a" * 64,
        ),
        cluster="fresh-cluster",
        core_dir=bootstrap.DEFAULT_REMOTE_CORE_DIR,
        spool_dir=bootstrap.DEFAULT_REMOTE_SPOOL_DIR,
        frp_version=bootstrap.FRP_VERSION,
        clio_kit_install_spec=bootstrap.CLIO_KIT_JARVIS_MCP_WHEEL_URL,
        clio_kit_artifact_sha256=bootstrap.CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
        agent_adapter="exec",
        agent_npm_package=None,
        agent_npm_bin=None,
        agent_args=[],
        jarvis_resource_graph_profile="ares",
    )
    jarvis_state = JarvisStateEvidence(
        initialized=True,
        root=str(tmp_path / ".ppi-jarvis"),
        config_sha256="b" * 64,
        repos_sha256="c" * 64,
        resource_graph_sha256="d" * 64,
        managed_repo_registered=True,
    )
    inspection = BootstrapInspection(
        exact_match=False,
        desired_fingerprint=desired.fingerprint,
        reasons=[
            "managed endpoint service is inactive",
            "managed endpoint service is disabled",
        ],
        install_receipt_sha256="e" * 64,
        active_generation=desired.fingerprint,
        current_generation_target=f"/home/operator/generations/{desired.fingerprint}",
        jarvis_state=jarvis_state,
        readiness=BootstrapReadinessEvidence(
            service_name=desired.worker_service,
            service_was_active=False,
            service_was_enabled=False,
            queue_ready=True,
            queue={
                "schema_version": "clio-relay.queue-readiness.v1",
                "complete": True,
                "sealed": True,
                "repair_required": False,
            },
            worker_ready=False,
        ),
    )
    components: dict[str, dict[str, object]] = {
        name: {
            "action": "prepared",
            "observed_identity": {},
            "duration_seconds": 1.0,
        }
        for name in ("clio-relay", "clio-kit", "jarvis-cd", "jarvis-util", "frp", "uv")
    }
    loaded_builtin_result: dict[str, object] = {
        "schema_version": "jarvis.resource-graph-builtin.v1",
        "profile": "ares",
        "action": "loaded",
        "available": True,
        "source": "/opt/jarvis/resource_graphs/ares.yaml",
        "source_sha256": "d" * 64,
        "catalog": ["ares"],
    }
    receipt = make_bootstrap_receipt(
        invocation_id="bootstrap_fresh",
        desired=desired,
        outcome="full",
        inspection=inspection,
        started_at=datetime.now(UTC),
        transaction=None,
        previous_generation=None,
        active_generation=desired.fingerprint,
        components=components,
        jarvis_init_action="initialized",
        jarvis_init_duration_seconds=1.0,
        jarvis_graph_action="loaded",
        jarvis_graph_duration_seconds=1.0,
        jarvis_builtin_result=loaded_builtin_result,
        jarvis_commands=[
            ["jarvis", "init", "/config", "/private", "/shared"],
            ["jarvis", "rg", "load-builtin", "ares", "+json"],
        ],
        jarvis_state_before=JarvisStateEvidence(
            initialized=False,
            root=jarvis_state.root,
        ),
        service_pending_install=True,
        service_active_after=False,
        service_enabled_after=False,
        payload_transfer_count=2,
        payload_transfer_bytes=1,
    )

    bootstrap._validate_bootstrap_receipt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        receipt,
        bootstrap_profile="linux-user",
        relay_install_spec=desired.relay_install_spec,
        desired_fingerprint=desired.fingerprint,
        expected_jarvis_resource_graph_profile="ares",
        expected_allow_jarvis_resource_graph_build=False,
        expected_worker_service=desired.worker_service,
    )
    service = receipt["service"]
    assert isinstance(service, dict)
    assert service["pending_install"] is True

    with pytest.raises(ValueError, match="packaged source digest"):
        make_bootstrap_receipt(
            invocation_id="bootstrap_mismatched_builtin",
            desired=desired,
            outcome="full",
            inspection=inspection,
            started_at=datetime.now(UTC),
            transaction=None,
            previous_generation=None,
            active_generation=desired.fingerprint,
            components=components,
            jarvis_init_action="initialized",
            jarvis_init_duration_seconds=1.0,
            jarvis_graph_action="loaded",
            jarvis_graph_duration_seconds=1.0,
            jarvis_builtin_result={
                **loaded_builtin_result,
                "source_sha256": "e" * 64,
            },
        )

    unavailable: dict[str, object] = {
        "schema_version": "jarvis.resource-graph-builtin.v1",
        "profile": "ares",
        "action": "unavailable",
        "available": False,
        "source": None,
        "source_sha256": None,
        "catalog": [],
    }
    with pytest.raises(ValueError, match="build was not enabled"):
        make_bootstrap_receipt(
            invocation_id="bootstrap_unauthorized_build",
            desired=desired,
            outcome="full",
            inspection=inspection,
            started_at=datetime.now(UTC),
            transaction=None,
            previous_generation=None,
            active_generation=desired.fingerprint,
            components=components,
            jarvis_init_action="initialized",
            jarvis_init_duration_seconds=1.0,
            jarvis_graph_action="built",
            jarvis_graph_duration_seconds=1.0,
            jarvis_builtin_result=unavailable,
        )

    allowed = desired.model_copy(update={"allow_jarvis_resource_graph_build": True})
    built = make_bootstrap_receipt(
        invocation_id="bootstrap_allowed_build",
        desired=allowed,
        outcome="full",
        inspection=inspection,
        started_at=datetime.now(UTC),
        transaction=None,
        previous_generation=None,
        active_generation=allowed.fingerprint,
        components=components,
        jarvis_init_action="initialized",
        jarvis_init_duration_seconds=1.0,
        jarvis_graph_action="built",
        jarvis_graph_duration_seconds=1.0,
        jarvis_builtin_result=unavailable,
        jarvis_commands=[
            ["jarvis", "init", "/config", "/private", "/shared"],
            ["jarvis", "rg", "load-builtin", "ares", "+json"],
            ["jarvis", "rg", "build", "+no_benchmark"],
        ],
    )
    graph = built["jarvis_resource_graph"]
    assert isinstance(graph, dict)
    assert graph["action"] == "built"
    assert graph["builtin_result"] == unavailable


def test_release_identity_is_canonical_across_wheel_and_pypi_transport(
    tmp_path: Path,
) -> None:
    digest = "f" * 64
    wheel = tmp_path / f"clio_relay-{__version__}-py3-none-any.whl"

    wheel_identity = bootstrap.bootstrap_relay_identity(
        source_root=tmp_path / "not-a-checkout",
        relay_wheel=wheel,
        relay_artifact_sha256=digest,
    )
    pypi_identity = bootstrap.bootstrap_relay_identity(
        source_root=tmp_path / "not-a-checkout",
        relay_wheel=None,
        relay_artifact_sha256=digest,
    )
    assert wheel_identity.install_spec == pypi_identity.install_spec
    assert wheel_identity.source_identity == pypi_identity.source_identity
    assert wheel_identity.transport_install_spec.endswith(wheel.name)
    assert pypi_identity.transport_install_spec == f"clio-relay=={__version__}"


def test_future_release_identity_does_not_require_network_or_a_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bootstrap, "__version__", "1.4.17")

    identity = bootstrap.bootstrap_relay_identity(
        source_root=tmp_path / "not-a-checkout",
        relay_wheel=None,
        relay_artifact_sha256="1" * 64,
    )

    assert identity.install_spec == "clio-relay==1.4.17"
    assert identity.source_identity == f"release:clio-relay==1.4.17:sha256:{'1' * 64}"
    assert identity.deployment_artifact_sha256 == "1" * 64


def test_release_identity_requires_exact_artifact_digest(tmp_path: Path) -> None:
    with pytest.raises(bootstrap.ConfigurationError, match="--relay-artifact-sha256"):
        bootstrap.bootstrap_relay_identity(
            source_root=tmp_path / "not-a-checkout",
            relay_wheel=None,
            relay_artifact_sha256=None,
        )


def test_same_release_version_with_different_wheels_has_different_identity(
    tmp_path: Path,
) -> None:
    first = bootstrap.bootstrap_relay_identity(
        source_root=tmp_path / "not-a-checkout",
        relay_wheel=None,
        relay_artifact_sha256="1" * 64,
    )
    second = bootstrap.bootstrap_relay_identity(
        source_root=tmp_path / "not-a-checkout",
        relay_wheel=None,
        relay_artifact_sha256="2" * 64,
    )

    assert first.source_identity != second.source_identity
    assert first.deployment_artifact_sha256 != second.deployment_artifact_sha256


def test_payload_script_uses_digest_bound_safe_extractor() -> None:
    """Payload bootstrap never dispatches the source archive through raw tar."""
    script = bootstrap.render_linux_user_bootstrap_script(cluster="cluster-a")

    assert 'tar -xf "$SOURCE_ARCHIVE"' not in script
    assert "stream.extractall(destination" not in script
    assert "from clio_relay.safe_archive import safe_extract_tar" in script
    assert "candidate_safe_archive" not in script
    assert "BOOTSTRAP_CANDIDATE_PACKAGE/safe_archive.py" in script


def test_fresh_jarvis_hardware_graph_commands_are_exact_and_ordered() -> None:
    lines = bootstrap.render_linux_user_bootstrap_script(cluster="cluster-a").splitlines()
    init_argv = [
        '"$JARVIS_VENV/bin/jarvis"',
        "init",
        '"$HOME/.local/share/clio-relay/jarvis-config"',
        '"$HOME/.local/share/clio-relay/jarvis-private"',
        '"$HOME/.local/share/clio-relay/jarvis-shared"',
    ]
    graph_argv = ['"$JARVIS_VENV/bin/jarvis"', "rg", "build", "+no_benchmark"]
    init_indexes = [index for index, line in enumerate(lines) if line.split() == init_argv]
    graph_indexes = [index for index, line in enumerate(lines) if line.split() == graph_argv]

    assert len(init_indexes) == 1
    assert len(graph_indexes) == 1
    assert init_indexes[0] < graph_indexes[0]


def test_fresh_journal_precedes_every_mutation_boundary() -> None:
    """First-install state is owned durably before components or JARVIS can mutate."""
    script = bootstrap.render_linux_user_bootstrap_script(cluster="cluster-a")
    journal = script.index("bootstrap_journal_action create")
    boundaries = (
        'curl -L --fail --retry 3 -o "$ARCHIVE"',
        "uv python install 3.12",
        'uv venv --python 3.12 --seed "$JARVIS_VENV"',
        '"$JARVIS_VENV/bin/jarvis" init',
        '"$JARVIS_VENV/bin/jarvis" rg build +no_benchmark',
        'managed_repo "$MANAGED_JARVIS_REPO_TARGET"',
        'mkdir -m 0700 "$BOOTSTRAP_GENERATION/bin"',
        'current "$BOOTSTRAP_GENERATION"',
        'bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" migration_started',
    )

    assert all(journal < script.index(boundary, journal) for boundary in boundaries)
    assert 'rm -rf "$DEST"' not in script
    assert '--clear "$JARVIS_VENV"' not in script
    assert 'bootstrap_journal_action discard-full "$BOOTSTRAP_TRANSACTION_JOURNAL"' in script
    assert 'mkdir -m 0700 -p "$BOOTSTRAP_TRANSACTION_ROOT/downloads"' not in script
    assert 'bootstrap_journal_action own "$BOOTSTRAP_TRANSACTION_JOURNAL"' not in script
    for owned_action in ("mkdir-owned", "copy-owned", "symlink-owned"):
        assert f"bootstrap_journal_action {owned_action}" in script[journal:]
    for phase in (
        "ownership_manifest",
        "components_prepared",
        "jarvis_initialized",
        "resource_graph_$JARVIS_GRAPH_ACTION",
        "managed_repository_reconciled",
        "generation_prepared",
        "generation_activated",
        "queue_migrated",
        "service_verified",
        "final_inspection",
    ):
        assert phase in script[journal:]


def test_packaged_payload_archive_has_only_safe_canonical_modes(tmp_path: Path) -> None:
    """Windows host modes cannot leak group/world write bits into the wire tar."""
    deployment = bootstrap.create_bootstrap_archive(
        source_root=tmp_path / "not-a-checkout",
        archive=tmp_path / "bootstrap.tar",
    )

    with tarfile.open(deployment.archive, "r:") as archive:
        members = archive.getmembers()
    assert members
    assert all(member.isdir() or member.isreg() for member in members)
    assert all(member.mode == (0o755 if member.isdir() else 0o644) for member in members)
