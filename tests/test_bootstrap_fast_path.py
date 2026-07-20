"""Focused acceptance contracts for payload-free repeated cluster bootstrap."""

from __future__ import annotations

import tarfile
from datetime import UTC, datetime
from pathlib import Path
from typing import NoReturn

import pytest

import clio_relay.bootstrap as bootstrap
from clio_relay import __version__
from clio_relay.bootstrap_reconcile import (
    BootstrapInspection,
    BootstrapReadinessEvidence,
    JarvisStateEvidence,
    make_bootstrap_receipt,
)


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

    def preflight(**kwargs: object) -> tuple[dict[str, object], list[str]]:
        requested = kwargs["desired"]
        assert hasattr(requested, "fingerprint")
        observed_desired.append(requested.fingerprint)  # type: ignore[attr-defined]
        return receipt, ["bootstrap_preflight_json={}"]

    def poison(*_args: object, **_kwargs: object) -> NoReturn:
        raise AssertionError("the exact no-op touched bootstrap payload code")

    monkeypatch.setattr(bootstrap, "_bootstrap_preflight_over_ssh", preflight)
    monkeypatch.setattr(bootstrap, "_verify_persistent_bootstrap_receipt", lambda **_kwargs: None)
    monkeypatch.setattr(bootstrap, "create_bootstrap_archive", poison)
    monkeypatch.setattr(bootstrap, "_validate_relay_bootstrap_wheel", poison)
    monkeypatch.setattr(bootstrap.shutil, "which", lambda executable: executable)
    monkeypatch.setattr(bootstrap, "uuid4", lambda: type("Uuid", (), {"hex": "test"})())

    lines = bootstrap.bootstrap_cluster_over_ssh(
        bootstrap_profile="linux-user",
        ssh_host="ares",
        source_root=source_root,
        cluster="ares",
        relay_wheel=wheel,
        relay_artifact_sha256=digest,
    )

    assert observed_desired == [desired.fingerprint]
    assert any(line.startswith("bootstrap_receipt_json=") for line in lines)
    assert receipt["jarvis_commands"] == {"count": 0, "argv": []}
    operations = receipt["operations"]
    assert isinstance(operations, dict)
    assert operations["payload_transfer_count"] == 0
    assert operations["payload_transfer_bytes"] == 0


def test_release_identity_is_canonical_across_wheel_and_pypi_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    digest = "f" * 64
    wheel = tmp_path / f"clio_relay-{__version__}-py3-none-any.whl"
    monkeypatch.setattr(bootstrap, "_pypi_release_wheel_sha256", lambda _version: digest)

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
    pypi_without_optional_evidence = bootstrap.bootstrap_relay_identity(
        source_root=tmp_path / "not-a-checkout",
        relay_wheel=None,
        relay_artifact_sha256=None,
    )

    assert wheel_identity.install_spec == pypi_identity.install_spec
    assert wheel_identity.source_identity == pypi_identity.source_identity
    assert pypi_identity == pypi_without_optional_evidence
    assert wheel_identity.transport_install_spec.endswith(wheel.name)
    assert pypi_identity.transport_install_spec == f"clio-relay=={__version__}"


def test_release_identity_rejects_validation_digest_different_from_pypi(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bootstrap, "_pypi_release_wheel_sha256", lambda _version: "f" * 64)

    with pytest.raises(bootstrap.ConfigurationError, match="official PyPI release wheel"):
        bootstrap.bootstrap_relay_identity(
            source_root=tmp_path / "not-a-checkout",
            relay_wheel=None,
            relay_artifact_sha256="e" * 64,
        )


def test_payload_script_uses_digest_bound_safe_extractor() -> None:
    """Payload bootstrap never dispatches the source archive through raw tar."""
    script = bootstrap.render_linux_user_bootstrap_script(cluster="cluster-a")

    assert 'tar -xf "$SOURCE_ARCHIVE"' not in script
    assert "stream.extractall(destination" not in script
    assert "from clio_relay.safe_archive import safe_extract_tar" in script
    assert "candidate_safe_archive" not in script
    assert "BOOTSTRAP_CANDIDATE_PACKAGE/safe_archive.py" in script


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
