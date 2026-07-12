"""Tests for stable validation evidence and the release gate."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import subprocess
import sys
import zipfile
from collections.abc import Callable
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import cast

import pytest
from pydantic import ValidationError
from pytest import MonkeyPatch

from clio_relay import validation_report as validation_report_module
from clio_relay.errors import ConfigurationError
from clio_relay.models import GatewaySession, GatewaySessionState
from clio_relay.remote_mcp import RemoteMcpAcceptanceCheck, RemoteMcpAcceptanceReport
from clio_relay.service_runtime import ServiceRuntimeStartResult, ServiceRuntimeStopResult
from clio_relay.session_lifecycle import (
    CleanupResource,
    RemoteSessionStateEvidence,
    SessionLifecycleReport,
)
from clio_relay.validation_report import (
    EvidenceOrigin,
    EvidenceReference,
    EvidenceTrust,
    InstallSource,
    InstallSourceKind,
    LiveValidationReport,
    ReleaseGatePolicy,
    ReleaseGateRequirement,
    ReleaseGateResult,
    ReleaseResourceRequirement,
    ReleaseTargetIdentity,
    SoftwareIdentity,
    TransportCleanupResourceEvidence,
    TransportProbeEvidence,
    ValidationCheck,
    ValidationRecorder,
    ValidationResource,
    ValidationStatus,
    default_report_path,
    evaluate_release_gate,
    load_release_gate_policy,
    load_validation_report,
    new_live_validation_report,
    render_validation_markdown,
    transport_probe_evidence_line,
    write_validation_report,
)

_verify_running_artifact_identity = cast(
    Callable[..., bool],
    validation_report_module._verify_running_artifact_identity,  # pyright: ignore[reportPrivateUsage]
)
_uv_executable_identity = cast(
    Callable[[str | None], tuple[bool, str | None, str | None]],
    validation_report_module._uv_executable_identity,  # pyright: ignore[reportPrivateUsage]
)


def _timestamp() -> datetime:
    return datetime(2026, 7, 10, 18, 0, tzinfo=UTC)


def test_install_source_records_unverified_uvx_without_crashing_report_creation(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    class FakeDistribution:
        version = "1.0.0"

        def read_text(self, _filename: str) -> str | None:
            return None

    def fake_distribution(_name: str) -> object:
        return FakeDistribution()

    def fake_files(_name: str) -> Path:
        return tmp_path

    def verified_artifact(*_args: object, **_kwargs: object) -> bool:
        return True

    def unverified_launcher(
        _launcher: str,
        **_kwargs: object,
    ) -> tuple[bool, dict[str, object]]:
        return False, {"verified": False}

    monkeypatch.setattr(validation_report_module.metadata, "distribution", fake_distribution)
    monkeypatch.setattr(validation_report_module.resources, "files", fake_files)
    monkeypatch.setattr(
        validation_report_module,
        "_verify_running_artifact_identity",
        verified_artifact,
    )
    monkeypatch.setattr(
        validation_report_module,
        "_detect_launcher_receipt",
        unverified_launcher,
    )

    source = validation_report_module.detect_install_source(
        launcher="uvx",
        artifact_sha256="a" * 64,
    )

    assert source.artifact_identity_verified is True
    assert source.launcher_verified is False
    assert source.released_artifact is False


class _FakeWheelDistribution:
    def __init__(self, root: Path) -> None:
        self.root = root

    def locate_file(self, path: str) -> Path:
        return self.root / path


def _test_wheel(tmp_path: Path) -> tuple[Path, Path, str, metadata.Distribution]:
    installed_root = tmp_path / "installed"
    installed_file = installed_root / "clio_relay" / "probe.py"
    installed_file.parent.mkdir(parents=True)
    content = b"PROBE = 'wheel-record-bound'\n"
    installed_file.write_bytes(content)
    encoded = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).decode().rstrip("=")
    record_path = "clio_relay-1.0.0.dist-info/RECORD"
    record = f"clio_relay/probe.py,sha256={encoded},{len(content)}\n{record_path},,\n"
    wheel = tmp_path / "clio_relay-1.0.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("clio_relay/probe.py", content)
        archive.writestr(record_path, record)
    digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
    distribution = cast(metadata.Distribution, _FakeWheelDistribution(installed_root))
    return wheel, installed_file, digest, distribution


def test_local_wheel_identity_hashes_archive_and_verifies_installed_record(
    tmp_path: Path,
) -> None:
    wheel, installed_file, digest, distribution = _test_wheel(tmp_path)
    direct_url = {
        "url": wheel.as_uri(),
        "archive_info": {"hash": f"sha256={digest}"},
    }

    assert _verify_running_artifact_identity(
        distribution,
        detected_kind=InstallSourceKind.WHEEL,
        direct_url=direct_url,
        artifact_sha256=digest,
        launcher="uvx",
    )

    installed_file.write_text("PROBE = 'tampered'\n", encoding="utf-8")
    assert not _verify_running_artifact_identity(
        distribution,
        detected_kind=InstallSourceKind.WHEEL,
        direct_url=direct_url,
        artifact_sha256=digest,
        launcher="uvx",
    )


def test_local_wheel_identity_rejects_direct_url_hash_without_archive(tmp_path: Path) -> None:
    wheel, _, digest, distribution = _test_wheel(tmp_path)
    wheel.unlink()

    assert not _verify_running_artifact_identity(
        distribution,
        detected_kind=InstallSourceKind.WHEEL,
        direct_url={
            "url": wheel.as_uri(),
            "archive_info": {"hash": f"sha256={digest}"},
        },
        artifact_sha256=digest,
        launcher="uvx",
    )


def test_local_wheel_identity_rejects_forged_direct_url_hash(tmp_path: Path) -> None:
    wheel, _, _, distribution = _test_wheel(tmp_path)
    forged_digest = "f" * 64

    assert not _verify_running_artifact_identity(
        distribution,
        detected_kind=InstallSourceKind.WHEEL,
        direct_url={
            "url": wheel.as_uri(),
            "archive_info": {"hash": f"sha256={forged_digest}"},
        },
        artifact_sha256=forged_digest,
        launcher="uvx",
    )


def test_uv_launcher_identity_hashes_exact_regular_executable(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    executable = tmp_path / "uv.exe"
    executable.write_bytes(b"trusted uv executable")

    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess([str(executable), "--version"], 0, "uv 0.11.28\n", "")

    monkeypatch.setattr(validation_report_module.subprocess, "run", fake_run)

    verified, version, digest = _uv_executable_identity(str(executable))

    assert verified is True
    assert version == "0.11.28"
    assert digest == hashlib.sha256(executable.read_bytes()).hexdigest()


def test_uv_launcher_identity_rejects_executable_replaced_during_probe(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    executable = tmp_path / "uv.exe"
    executable.write_bytes(b"trusted uv executable")

    def replacing_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        executable.write_bytes(b"substituted executable")
        return subprocess.CompletedProcess([str(executable), "--version"], 0, "uv 0.11.28\n", "")

    monkeypatch.setattr(validation_report_module.subprocess, "run", replacing_run)

    assert _uv_executable_identity(str(executable)) == (
        False,
        None,
        None,
    )


@pytest.fixture(scope="module")
def real_uvx_relay_wheel(tmp_path_factory: pytest.TempPathFactory) -> tuple[str, Path]:
    """Build one real wheel for cache-backed and no-cache uvx receipt probes."""
    uv = shutil.which("uv")
    uvx = shutil.which("uvx")
    assert uv is not None and uvx is not None, "the production test suite requires uv and uvx"
    uv_version = subprocess.run(
        [uv, "--version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    uvx_version = subprocess.run(
        [uvx, "--version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    assert uv_version[:2] == ["uv", "0.11.28"]
    assert uvx_version[:2] == ["uvx", "0.11.28"]
    tmp_path = tmp_path_factory.mktemp("real-uvx-receipt")
    artifact_dir = tmp_path / "dist"
    project_root = Path(__file__).parents[1]
    subprocess.run(
        [uv, "build", "--wheel", "--out-dir", str(artifact_dir), str(project_root)],
        check=True,
        capture_output=True,
        text=True,
    )
    wheels = list(artifact_dir.glob("clio_relay-*.whl"))
    assert len(wheels) == 1
    return uvx, wheels[0].resolve()


def test_real_uvx_0_11_28_produces_verified_os_bound_launcher_receipt(
    tmp_path: Path,
    real_uvx_relay_wheel: tuple[str, Path],
) -> None:
    uvx, wheel = real_uvx_relay_wheel
    report = tmp_path / "uvx-receipt.json"
    environment = os.environ.copy()
    environment["CLIO_RELAY_VALIDATION_INVOCATION_ID"] = "real-uvx-receipt-test"
    completed = subprocess.run(
        [
            uvx,
            "--refresh",
            "--no-config",
            "--python",
            f"{sys.version_info.major}.{sys.version_info.minor}",
            "--from",
            str(wheel),
            "clio-relay",
            "live-test",
            "--cluster",
            "__missing_real_uvx_receipt_cluster__",
            "--validation-launcher",
            "uvx",
            "--validation-install-source",
            f"wheel:{wheel}",
            "--validation-artifact",
            str(wheel),
            "--report",
            str(report),
        ],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=180,
    )

    assert completed.returncode != 0
    assert report.is_file(), completed.stderr
    source = json.loads(report.read_text(encoding="utf-8"))["install_source"]
    receipt = source["launcher_receipt"]
    assert receipt["schema_version"] == "clio-relay.launcher-receipt.v2"
    assert receipt["verified"] is True
    assert receipt["uv_executable_verified"] is True
    assert receipt["uv_executable_stable"] is True
    assert receipt["uv_version"] == "0.11.28"
    assert (
        receipt["uv_executable_sha256"]
        == hashlib.sha256(Path(receipt["uv_executable"]).read_bytes()).hexdigest()
    )
    assert receipt["uv_cache_contains_environment"] is True
    assert Path(receipt["process_prefix"]).is_relative_to(Path(receipt["uv_cache_directory"]))
    assert receipt["pyvenv_matches_uv"] is True
    assert receipt["package_in_process_environment"] is True
    assert receipt["executable_in_process_environment"] is True
    assert receipt["executable_target_bound"] is True
    assert receipt["uv_process_ancestor_verified"] is True
    assert receipt["uv_process_ancestor"]["depth"] <= (
        validation_report_module.MAX_LAUNCHER_PROCESS_ANCESTORS
    )
    assert receipt["invocation_id"] == "real-uvx-receipt-test"
    assert "uv_run_recursion_depth" not in receipt
    assert "virtual_environment" not in receipt


def test_real_uvx_no_cache_temporary_environment_is_not_a_verified_receipt(
    tmp_path: Path,
    real_uvx_relay_wheel: tuple[str, Path],
) -> None:
    uvx, wheel = real_uvx_relay_wheel
    report = tmp_path / "uvx-no-cache-receipt.json"
    environment = os.environ.copy()
    environment["CLIO_RELAY_VALIDATION_INVOCATION_ID"] = "real-uvx-no-cache-test"
    completed = subprocess.run(
        [
            uvx,
            "--refresh",
            "--no-cache",
            "--no-config",
            "--python",
            f"{sys.version_info.major}.{sys.version_info.minor}",
            "--from",
            str(wheel),
            "clio-relay",
            "live-test",
            "--cluster",
            "__missing_real_uvx_no_cache_cluster__",
            "--validation-launcher",
            "uvx",
            "--validation-install-source",
            f"wheel:{wheel}",
            "--validation-artifact",
            str(wheel),
            "--report",
            str(report),
        ],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=180,
    )

    assert completed.returncode != 0
    assert report.is_file(), completed.stderr
    source = json.loads(report.read_text(encoding="utf-8"))["install_source"]
    receipt = source["launcher_receipt"]
    assert receipt["schema_version"] == "clio-relay.launcher-receipt.v2"
    assert receipt["verified"] is False
    assert receipt["uv_executable_verified"] is True
    assert receipt["uv_cache_contains_environment"] is False
    assert not Path(receipt["process_prefix"]).is_relative_to(Path(receipt["uv_cache_directory"]))
    assert source["launcher_verified"] is False
    assert source["released_artifact"] is False
    assert receipt["invocation_id"] == "real-uvx-no-cache-test"


def _report(
    *,
    kind: InstallSourceKind = InstallSourceKind.PYPI,
    launcher: str = "uvx",
    released_artifact: bool = True,
    artifact_identity_verified: bool = True,
) -> LiveValidationReport:
    now = _timestamp()
    return LiveValidationReport(
        report_id="validation_test",
        scenario="remote-mcp",
        cluster="primary",
        started_at=now,
        completed_at=now,
        status=ValidationStatus.PASSED,
        evidence_trust=EvidenceTrust(
            producer_github_login="release-operator",
            producer_github_id=123456,
            invocation_id="run-20260710-0001",
        ),
        software=SoftwareIdentity(
            version="1.0.0",
            commit="a" * 40,
            tag="v1.0.0",
            dirty=False,
        ),
        install_source=InstallSource(
            kind=kind,
            detected_kind=kind,
            reference="clio-relay==1.0.0",
            launcher=launcher,
            package_path="/tmp/uv/archive/clio_relay",
            distribution_version="1.0.0",
            artifact_sha256="b" * 64,
            artifact_identity_verified=artifact_identity_verified,
            released_artifact=released_artifact,
            launcher_verified=launcher == "uvx",
            launcher_receipt={
                "verified": launcher == "uvx",
                "uv_executable_verified": launcher == "uvx",
                "uv_version": "0.11.28",
                "uv_executable_sha256": "e" * 64,
                "invocation_id": "run-20260710-0001",
            },
        ),
        checks=[
            ValidationCheck(
                check_id="remote-mcp.call",
                summary="call a virtualized remote MCP tool",
                status=ValidationStatus.PASSED,
                started_at=now,
                completed_at=now,
                evidence=[EvidenceReference(kind="test", excerpt="test evidence")],
            )
        ],
        resources=[
            ValidationResource(
                kind="relay_job",
                resource_id="job_123",
                cluster="primary",
                state="succeeded",
            )
        ],
    )


def _policy() -> ReleaseGatePolicy:
    return ReleaseGatePolicy(
        release_version="1.0.0",
        required_uv_version="0.11.28",
        requirements=[
            ReleaseGateRequirement(
                requirement_id="remote-mcp-primary",
                description="non-JARVIS virtual MCP on the primary cluster",
                cluster="primary",
                scenarios=["remote-mcp"],
                required_checks=["remote-mcp.call"],
                required_resource_kinds=["relay_job"],
            )
        ],
    )


def _evaluate(
    policy: ReleaseGatePolicy,
    reports: list[LiveValidationReport],
) -> ReleaseGateResult:
    return evaluate_release_gate(
        policy,
        reports,
        expected_artifact_sha256="b" * 64,
    )


def _target_policy(
    *,
    hostnames: list[str] | None = None,
    fingerprints: list[str] | None = None,
    site_marker_sha256: str = "d" * 64,
    scheduler_provider: str = "slurm",
    scheduler_cluster_name: str | None = "primary-scheduler",
) -> ReleaseTargetIdentity:
    normalized_hostnames = sorted(
        item.strip().rstrip(".").lower() for item in (hostnames or ["login.primary.example"])
    )
    normalized_fingerprints = sorted(fingerprints or ["SHA256:primary-host-key"])
    canonical = {
        "schema_version": "clio-relay.cluster-target-identity.v1",
        "observed_hostnames": normalized_hostnames,
        "observed_ssh_host_key_sha256": normalized_fingerprints,
        "scheduler_cluster_name": scheduler_cluster_name,
        "site_marker_sha256": site_marker_sha256,
        "scheduler_provider": scheduler_provider,
    }
    digest = hashlib.sha256(
        json.dumps(canonical, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()
    return ReleaseTargetIdentity(
        hostnames=normalized_hostnames,
        ssh_host_key_sha256=normalized_fingerprints,
        scheduler_provider=scheduler_provider,
        scheduler_cluster_name=scheduler_cluster_name,
        site_marker_sha256=site_marker_sha256,
        identity_sha256=digest,
    )


def _attach_verified_target_identity(
    report: LiveValidationReport,
    *,
    hostname: str = "login.primary.example",
    fingerprint: str = "SHA256:primary-host-key",
    expected_hostnames: list[str] | None = None,
    expected_fingerprints: list[str] | None = None,
    site_marker_sha256: str = "d" * 64,
    expected_site_marker_sha256: str | None = "d" * 64,
) -> None:
    now = _timestamp()
    report.checks.append(
        ValidationCheck(
            check_id="worker.target-identity",
            summary="worker physical target identity",
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt="target identity verified")],
        )
    )
    report.resources.append(
        ValidationResource(
            kind="cluster_target",
            resource_id=f"target:{report.cluster}",
            role="physical_cluster_target",
            cluster=report.cluster,
            state="verified",
            provider="slurm",
            metadata={
                "schema_version": "clio-relay.cluster-target-info.v1",
                "hostname": hostname,
                "fqdn": hostname,
                "scheduler_provider": "slurm",
                "scheduler_cluster_name": "primary-scheduler",
                "site_marker_sha256": site_marker_sha256,
                "ssh_host_key_sha256": [fingerprint],
                "expected_hostnames": (
                    expected_hostnames if expected_hostnames is not None else [hostname]
                ),
                "expected_ssh_host_key_sha256": (
                    expected_fingerprints if expected_fingerprints is not None else [fingerprint]
                ),
                "expected_scheduler_cluster_name": "primary-scheduler",
                "expected_site_marker_sha256": expected_site_marker_sha256,
                "verified": True,
            },
        )
    )


def test_report_json_round_trip_is_stable(tmp_path: Path) -> None:
    path = tmp_path / "report.json"

    write_validation_report(_report(), path)
    loaded = load_validation_report(path)

    assert loaded.report_id == "validation_test"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "1.0"
    assert payload["evidence_trust"]["producer_github_id"] == 123456
    assert payload["evidence_trust"]["invocation_id"] == "run-20260710-0001"
    assert path.read_text(encoding="utf-8").endswith("\n")
    assert not list(tmp_path.glob("*.tmp"))


def test_report_writer_redacts_nested_capability_values(tmp_path: Path) -> None:
    report = _report()
    owner_token = "owned-connector-capability-123"
    report.resources[0].metadata = {
        "owner_token": owner_token,
        "command": f"connector-wrapper {owner_token} --serve",
        "token_env": "CLIO_RELAY_FRP_TOKEN",
        "nested": {"API-TOKEN": "remote-api-capability-456"},
    }
    report.checks[0].evidence[0].metadata = {
        "diagnostic": f"process environment contained {owner_token}"
    }
    path = tmp_path / "redacted-report.json"

    write_validation_report(report, path)

    rendered = path.read_text(encoding="utf-8")
    payload = json.loads(rendered)
    metadata = payload["resources"][0]["metadata"]
    assert owner_token not in rendered
    assert "remote-api-capability-456" not in rendered
    assert metadata["owner_token"] == "<redacted>"
    assert metadata["command"] == "connector-wrapper <redacted> --serve"
    assert metadata["token_env"] == "CLIO_RELAY_FRP_TOKEN"
    assert metadata["nested"]["API-TOKEN"] == "<redacted>"


def test_report_rejects_unknown_schema_or_unevidenced_success() -> None:
    payload = _report().model_dump(mode="python")
    payload["schema_version"] = "2.0"
    with pytest.raises(ValidationError, match="schema_version"):
        LiveValidationReport.model_validate(payload)

    payload = _report().model_dump(mode="python")
    payload["checks"][0]["evidence"] = []
    with pytest.raises(ValidationError, match="require evidence"):
        LiveValidationReport.model_validate(payload)


def test_report_cannot_self_claim_independent_producer_execution_proof() -> None:
    report = _report()
    payload = report.model_dump(mode="python")
    payload["evidence_trust"]["producer_execution_verified"] = True

    with pytest.raises(ValidationError, match="producer_execution_verified"):
        LiveValidationReport.model_validate(payload)

    local = new_live_validation_report(scenario="local-release", cluster="local")
    remote = new_live_validation_report(scenario="live-test", cluster="site-a")
    assert local.evidence_trust.origin is EvidenceOrigin.LOCAL_PROCESS
    assert remote.evidence_trust.origin is EvidenceOrigin.OPERATOR_GENERATED
    assert remote.evidence_trust.producer_execution_verified is False


def test_new_report_records_only_explicit_complete_producer_identity(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_LOGIN", "release-operator")
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_ID", "123456")
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_INVOCATION_ID", "run-20260710-0001")

    report = new_live_validation_report(scenario="live-test", cluster="site-a")

    assert report.evidence_trust.producer_github_login == "release-operator"
    assert report.evidence_trust.producer_github_id == 123456
    assert report.evidence_trust.invocation_id == "run-20260710-0001"


def test_new_report_allows_partial_but_rejects_invalid_producer_identity(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_LOGIN", "release-operator")
    partial = new_live_validation_report(scenario="live-test", cluster="site-a")
    assert partial.evidence_trust.producer_github_login == "release-operator"
    assert partial.evidence_trust.producer_github_id is None

    monkeypatch.setenv("CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_ID", "not-numeric")
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_INVOCATION_ID", "run-20260710-0001")
    with pytest.raises(ConfigurationError, match="positive integer"):
        new_live_validation_report(scenario="live-test", cluster="site-a")


def test_release_gate_rejects_missing_or_mismatched_producer_invocation() -> None:
    missing = _report()
    missing.evidence_trust = EvidenceTrust()
    missing_result = _evaluate(_policy(), [missing])

    mismatched = _report()
    mismatched.install_source.launcher_receipt["invocation_id"] = "run-20260710-other"
    mismatched_result = _evaluate(_policy(), [mismatched])

    assert any(
        "omits authenticated producer" in reason
        for reason in missing_result.unsatisfied_requirements["remote-mcp-primary"]
    )
    assert any(
        "invocation id does not match" in reason
        for reason in mismatched_result.unsatisfied_requirements["remote-mcp-primary"]
    )


def test_release_gate_rejects_unpinned_launcher_binary_identity() -> None:
    report = _report()
    report.install_source.launcher_receipt["uv_version"] = "0.11.27"
    report.install_source.launcher_receipt["uv_executable_sha256"] = "E" * 64

    result = _evaluate(_policy(), [report])

    reasons = result.unsatisfied_requirements["remote-mcp-primary"]
    assert "launcher receipt uv version must be 0.11.28, got 0.11.27" in reasons
    assert "launcher receipt omits a lowercase uv executable SHA-256" in reasons


def test_recorder_converts_lines_to_resources_and_artifacts() -> None:
    report = _report()
    report.checks.clear()
    report.resources.clear()
    recorder = ValidationRecorder(report)

    recorder.observe_line("acceptance.job_id=job_abc")
    recorder.observe_line("acceptance.job_state=succeeded")
    recorder.observe_line("acceptance.stdout_bytes=42")
    recorder.observe_line("acceptance.artifacts=stdout,provenance")
    recorder.observe_line("scheduler.pending=observed")
    recorder.observe_line("cluster.bootstrap=verified")
    recorder.observe_line("worker.running=passed")
    recorder.observe_line("worker.artifact-version=1.0.0")
    recorder.observe_line('worker.components={"clio-kit":"2.2.6"}')
    recorder.observe_line("worker.execute=passed")
    recorder.observe_line("live acceptance passed")
    recorder.finish()

    job = next(resource for resource in report.resources if resource.kind == "relay_job")
    assert job.resource_id == "job_abc"
    assert job.state == "succeeded"
    assert {item.kind for item in report.artifacts} == {"log", "stdout", "provenance"}
    assert any(check.check_id == "scheduler.pending" for check in report.checks)
    assert {
        "cluster.bootstrap",
        "worker.running",
        "worker.artifact-version",
        "worker.execute",
    }.issubset({check.check_id for check in report.checks})
    assert report.status is ValidationStatus.PASSED
    worker = next(resource for resource in report.resources if resource.kind == "relay_worker")
    assert worker.metadata["components"] == {"clio-kit": "2.2.6"}


def test_recorder_does_not_turn_failure_or_informational_lines_into_passes() -> None:
    report = _report()
    report.checks.clear()
    recorder = ValidationRecorder(report)

    recorder.observe_line("transport.remote_cleanup=not_started")
    recorder.observe_line("transport.cleanup=detached")
    recorder.observe_line("direct_transport.result=frp_stcp")
    recorder.observe_line("scheduler.pending=unknown")
    recorder.observe_line("worker.artifact-sha256=none")
    recorder.observe_line("transport.server=relay.example:7000")
    recorder.observe_line("acceptance.unrecognized=looks-good")

    check_ids = {check.check_id for check in report.checks}
    assert "transport.remote_cleanup" not in check_ids
    assert "transport.cleanup" not in check_ids
    assert "direct_transport.result" not in check_ids
    assert "transport.direct" not in check_ids
    assert "scheduler.pending" not in check_ids
    assert "worker.artifact-sha256" not in check_ids
    assert "transport.server" not in check_ids
    assert "acceptance.unrecognized" not in check_ids


def test_recorder_preserves_exact_structured_transport_cleanup_resources() -> None:
    report = _report()
    report.checks.clear()
    report.resources.clear()
    report.cleanup.actions.clear()
    recorder = ValidationRecorder(report)
    recorder.observe_line(
        transport_probe_evidence_line(
            TransportProbeEvidence(
                probe_id="ssh-probe:session-a:generation-a",
                cluster=report.cluster,
                cleanup_mode="transport_probe_teardown",
                resources=[
                    TransportCleanupResourceEvidence(
                        kind="relay_session",
                        resource_id="session-a:generation-a",
                        role="remote_transport_session",
                        location="cluster-host",
                        action="stop",
                        ownership_verified=True,
                        outcome="stopped",
                        verified_after_operation=True,
                        observed_state="stopped",
                        residual=False,
                        detail="owned generation stopped",
                        metadata={"session_generation_id": "generation-a"},
                    ),
                    TransportCleanupResourceEvidence(
                        kind="connector",
                        resource_id="connector-4271",
                        role="remote_frpc_connector",
                        location="cluster-host",
                        action="stop",
                        ownership_verified=True,
                        outcome="stopped",
                        verified_after_operation=True,
                        observed_state="stopped",
                        residual=False,
                        detail=None,
                    ),
                    TransportCleanupResourceEvidence(
                        kind="gateway_session",
                        resource_id="gateway-91",
                        role="gateway_record:close",
                        location="cluster-host",
                        action="close",
                        ownership_verified=True,
                        outcome="closed",
                        verified_after_operation=True,
                        observed_state="closed",
                        residual=False,
                        detail=None,
                    ),
                ],
            )
        )
    )

    assert recorder.transport_probe_count == 1
    assert {(item.kind, item.resource_id) for item in report.resources} == {
        ("relay_session", "session-a:generation-a"),
        ("connector", "connector-4271"),
        ("gateway_session", "gateway-91"),
    }
    gateway = next(item for item in report.resources if item.kind == "gateway_session")
    assert gateway.metadata["ownership_verified"] is True
    assert gateway.metadata["observed_state"] == "closed"
    assert gateway.metadata["detail"] is None
    assert report.cleanup.remaining_resources == []
    assert all(
        action["probe_id"] == "ssh-probe:session-a:generation-a"
        for action in report.cleanup.actions
    )
    assert not any(action.get("kind") == "transport_probe" for action in report.cleanup.actions)


def test_recorder_preserves_partial_transport_cleanup_as_remaining_resource() -> None:
    report = _report()
    report.checks.clear()
    report.resources.clear()
    report.cleanup.actions.clear()
    recorder = ValidationRecorder(report)
    recorder.observe_line(
        transport_probe_evidence_line(
            TransportProbeEvidence(
                probe_id="frp-probe-partial",
                cluster=report.cluster,
                cleanup_mode="transport_probe_teardown",
                resources=[
                    TransportCleanupResourceEvidence(
                        kind="connector",
                        resource_id="remote-frpc-pid-811",
                        role="remote_frpc_connector",
                        location="cluster-host",
                        action="stop",
                        ownership_verified=True,
                        outcome="failed",
                        verified_after_operation=False,
                        observed_state="running",
                        residual=True,
                        detail="process remained after TERM and KILL",
                        metadata={"pid": 811},
                    )
                ],
            )
        )
    )

    remaining = report.cleanup.remaining_resources
    assert [(item.kind, item.resource_id) for item in remaining] == [
        ("connector", "remote-frpc-pid-811")
    ]
    assert remaining[0].metadata["ownership_verified"] is True
    assert remaining[0].metadata["observed_state"] == "running"
    assert remaining[0].metadata["residual"] is True
    assert remaining[0].metadata["detail"] == "process remained after TERM and KILL"
    assert report.cleanup.actions[0]["outcome"] == "failed"


@pytest.mark.parametrize(
    "payload",
    [
        '{"schema_version":"clio-relay.transport-probe-evidence.v1","extra":true}',
        '{"schema_version":"clio-relay.transport-probe-evidence.v1","probe_id":"p",'
        '"cluster":"test-cluster","cleanup_mode":"teardown","resources":[],"n":NaN}',
        "x" * (256 * 1024 + 1),
    ],
    ids=["extra-field", "non-finite", "oversized"],
)
def test_recorder_rejects_unbounded_or_non_strict_transport_evidence(payload: str) -> None:
    recorder = ValidationRecorder(_report())

    with pytest.raises(ConfigurationError, match="transport probe evidence"):
        recorder.observe_line(f"transport.probe_evidence={payload}")


def test_recorder_records_failed_check_and_report() -> None:
    report = _report()
    report.checks.clear()
    recorder = ValidationRecorder(report)
    error = RuntimeError("live failure")

    recorder.record_failure("transport.cleanup", "clean owned connector", error)
    recorder.finish(error)

    assert report.status is ValidationStatus.FAILED
    assert report.error == "RuntimeError: live failure"
    assert report.checks[0].status is ValidationStatus.FAILED


def test_release_gate_accepts_only_complete_released_uvx_evidence() -> None:
    result = _evaluate(_policy(), [_report()])

    assert result.passed is True
    assert result.satisfied_requirements == ["remote-mcp-primary"]
    assert result.report_ids == ["validation_test"]


def test_release_gate_requires_verified_identity_for_every_nonlocal_report() -> None:
    policy = _policy()
    policy.require_target_identity = True

    result = _evaluate(policy, [_report()])

    assert result.passed is False
    assert any(
        "must identify exactly one cluster_target resource" in reason
        for reason in result.unsatisfied_requirements["target-identity"]
    )


def test_release_gate_rejects_same_cluster_label_for_different_physical_targets() -> None:
    policy = _policy()
    policy.require_target_identity = True
    first = _report()
    _attach_verified_target_identity(first)
    second = _report()
    second.report_id = "validation_same_label_different_target"
    _attach_verified_target_identity(
        second,
        hostname="other-login.example",
        fingerprint="SHA256:other-host-key",
    )

    policy.targets = {"primary": _target_policy()}
    result = _evaluate(policy, [first, second])

    assert result.passed is False
    assert any(
        "reports identify different physical target identities" in reason
        for reason in result.unsatisfied_requirements["target-identity"]
    )


def test_release_gate_does_not_confuse_broad_pins_with_observed_identity() -> None:
    policy = _policy()
    policy.require_target_identity = True
    expected_hostnames = ["login-a.example", "login-b.example"]
    expected_fingerprints = ["SHA256:host-a", "SHA256:host-b"]
    first = _report()
    _attach_verified_target_identity(
        first,
        hostname="login-a.example",
        fingerprint="SHA256:host-a",
        expected_hostnames=expected_hostnames,
        expected_fingerprints=expected_fingerprints,
        site_marker_sha256="a" * 64,
        expected_site_marker_sha256=None,
    )
    second = _report()
    second.report_id = "validation_broad_pins_other_target"
    _attach_verified_target_identity(
        second,
        hostname="login-b.example",
        fingerprint="SHA256:host-b",
        expected_hostnames=expected_hostnames,
        expected_fingerprints=expected_fingerprints,
        site_marker_sha256="b" * 64,
        expected_site_marker_sha256=None,
    )

    policy.targets = {
        "primary": _target_policy(
            hostnames=["login-a.example"],
            fingerprints=["SHA256:host-a"],
            site_marker_sha256="a" * 64,
        )
    }
    result = _evaluate(policy, [first, second])

    assert result.passed is False
    assert any(
        "reports identify different physical target identities" in reason
        for reason in result.unsatisfied_requirements["target-identity"]
    )


def test_release_gate_records_one_stable_target_digest_per_cluster() -> None:
    policy = _policy()
    policy.require_target_identity = True
    first = _report()
    _attach_verified_target_identity(first)
    second = _report()
    second.report_id = "validation_same_physical_target"
    _attach_verified_target_identity(second)

    policy.targets = {"primary": _target_policy()}
    result = _evaluate(policy, [first, second])

    assert result.passed is True
    assert set(result.target_identity_sha256) == {"primary"}
    assert len(result.target_identity_sha256["primary"]) == 64
    assert result.policy_target_identity_sha256 == result.target_identity_sha256


def test_target_digest_normalizes_hostname_and_host_key_set_order() -> None:
    policy = _policy()
    policy.require_target_identity = True
    first = _report()
    _attach_verified_target_identity(first)
    first_target = next(
        resource for resource in first.resources if resource.kind == "cluster_target"
    )
    first_target.metadata.update(
        {
            "hostname": "LOGIN.PRIMARY.EXAMPLE.",
            "fqdn": "worker.primary.example",
            "ssh_host_key_sha256": ["SHA256:key-b", "SHA256:primary-host-key"],
            "expected_hostnames": ["login.primary.example", "worker.primary.example"],
            "expected_ssh_host_key_sha256": [
                "SHA256:primary-host-key",
                "SHA256:key-b",
            ],
        }
    )
    second = _report()
    second.report_id = "validation_same_target_reordered_observations"
    _attach_verified_target_identity(second)
    second_target = next(
        resource for resource in second.resources if resource.kind == "cluster_target"
    )
    second_target.metadata.update(
        {
            "hostname": "worker.primary.example",
            "fqdn": "login.primary.example",
            "ssh_host_key_sha256": ["SHA256:primary-host-key", "SHA256:key-b"],
            "expected_hostnames": ["worker.primary.example", "login.primary.example"],
            "expected_ssh_host_key_sha256": [
                "SHA256:key-b",
                "SHA256:primary-host-key",
            ],
        }
    )

    policy.targets = {
        "primary": _target_policy(
            hostnames=["login.primary.example", "worker.primary.example"],
            fingerprints=["SHA256:primary-host-key", "SHA256:key-b"],
        )
    }
    result = _evaluate(policy, [first, second])

    assert result.passed is True
    assert set(result.target_identity_sha256) == {"primary"}


def test_release_gate_requires_exact_policy_target_coverage() -> None:
    policy = _policy()
    policy.require_target_identity = True
    policy.targets = {
        "primary": _target_policy(),
        "unreported-site": _target_policy(hostnames=["unreported.example"]),
    }
    report = _report()
    _attach_verified_target_identity(report)

    result = _evaluate(policy, [report])

    assert (
        "policy targets lack report coverage: ['unreported-site']"
        in (result.unsatisfied_requirements["target-identity"])
    )


def test_release_gate_ignores_self_reported_expected_target_fields() -> None:
    policy = _policy()
    policy.require_target_identity = True
    policy.targets = {"primary": _target_policy()}
    report = _report()
    _attach_verified_target_identity(
        report,
        hostname="attacker.example",
        fingerprint="SHA256:attacker-key",
        expected_hostnames=["attacker.example"],
        expected_fingerprints=["SHA256:attacker-key"],
    )

    result = _evaluate(policy, [report])

    assert result.passed is False
    assert any(
        "does not match policy-pinned fields" in reason
        for reason in result.unsatisfied_requirements["target-identity"]
    )


def test_release_gate_rejects_policy_digest_not_bound_to_pinned_fields() -> None:
    policy = _policy()
    policy.require_target_identity = True
    target = _target_policy()
    target.identity_sha256 = "e" * 64
    policy.targets = {"primary": target}
    report = _report()
    _attach_verified_target_identity(report)

    result = _evaluate(policy, [report])

    assert any(
        "identity_sha256 does not match its pinned fields" in reason
        for reason in result.unsatisfied_requirements["target-identity"]
    )


def test_release_gate_fails_closed_for_declared_external_blockers() -> None:
    policy = _policy()
    policy.release_blockers = ["JARVIS-CD structured scheduler submission has no released artifact"]

    result = _evaluate(policy, [_report()])

    assert result.passed is False
    assert result.satisfied_requirements == ["remote-mcp-primary"]
    assert result.unsatisfied_requirements["declared-release-blockers"] == (policy.release_blockers)


def test_release_gate_rejects_checkout_or_local_wheel_claim() -> None:
    report = _report(
        kind=InstallSourceKind.WHEEL,
        launcher="uvx",
        released_artifact=False,
    )

    result = _evaluate(_policy(), [report])

    assert result.passed is False
    reasons = result.unsatisfied_requirements["remote-mcp-primary"]
    assert "install source wheel is not release-approved" in reasons
    assert "report does not prove a released artifact" in reasons


def test_release_gate_rejects_dirty_or_untagged_build() -> None:
    report = _report()
    report.software.dirty = True
    report.software.tag = None

    result = _evaluate(_policy(), [report])

    reasons = result.unsatisfied_requirements["remote-mcp-primary"]
    assert "report does not prove a clean build" in reasons
    assert "report source tag must be v1.0.0, got None" in reasons


def test_release_gate_binds_candidate_wheel_reports_to_independent_digest() -> None:
    policy = _policy()
    policy.artifact_stage = "immutable_candidate"
    policy.require_released_artifact = False
    policy.allowed_install_sources = [InstallSourceKind.WHEEL]
    report = _report(
        kind=InstallSourceKind.WHEEL,
        launcher="uvx",
        released_artifact=False,
    )

    accepted = evaluate_release_gate(
        policy,
        [report],
        expected_artifact_sha256="B" * 64,
    )
    rejected = evaluate_release_gate(
        policy,
        [report],
        expected_artifact_sha256="c" * 64,
    )

    assert accepted.passed is True
    assert accepted.artifact_sha256 == "b" * 64
    assert rejected.passed is False
    assert (
        "tested artifact SHA-256 does not match the immutable candidate: " + "b" * 64
        in rejected.unsatisfied_requirements["remote-mcp-primary"]
    )


def test_candidate_gate_rejects_unbound_running_distribution() -> None:
    policy = _policy()
    policy.artifact_stage = "immutable_candidate"
    policy.require_released_artifact = False
    policy.allowed_install_sources = [InstallSourceKind.WHEEL]
    report = _report(
        kind=InstallSourceKind.WHEEL,
        released_artifact=False,
        artifact_identity_verified=False,
    )

    result = evaluate_release_gate(
        policy,
        [report],
        expected_artifact_sha256="b" * 64,
    )

    assert result.passed is False
    assert (
        "running distribution is not bound to the expected wheel bytes"
        in (result.unsatisfied_requirements["remote-mcp-primary"])
    )


def test_release_gate_rejects_invalid_expected_artifact_digest() -> None:
    with pytest.raises(ConfigurationError, match="64 hexadecimal"):
        evaluate_release_gate(_policy(), [_report()], expected_artifact_sha256="not-a-digest")


def test_candidate_gate_requires_independently_computed_digest() -> None:
    policy = _policy()
    policy.artifact_stage = "immutable_candidate"
    policy.require_released_artifact = False
    policy.allowed_install_sources = [InstallSourceKind.WHEEL]

    with pytest.raises(ConfigurationError, match="independently computed"):
        evaluate_release_gate(policy, [])


def test_published_gate_requires_independently_computed_digest() -> None:
    with pytest.raises(ConfigurationError, match="published gates.*independently computed"):
        evaluate_release_gate(_policy(), [_report()])


def test_release_gate_enforces_resource_state_role_and_metadata() -> None:
    policy = _policy()
    policy.requirements[0].required_resources = [
        ReleaseResourceRequirement(
            kind="relay_job",
            roles=["virtual_remote_mcp_call"],
            states=["succeeded"],
            metadata_equals={"ownership_verified": True},
        )
    ]
    report = _report()
    report.resources[0].role = "virtual_remote_mcp_call"
    report.resources[0].metadata["ownership_verified"] = True

    accepted = _evaluate(policy, [report])
    report.resources[0].state = "failed"
    rejected = _evaluate(policy, [report])

    assert accepted.passed is True
    assert rejected.passed is False
    assert any(
        "requires 1 matching relay_job" in reason
        for reason in rejected.unsatisfied_requirements["remote-mcp-primary"]
    )


def test_release_gate_can_combine_checks_from_multiple_live_reports() -> None:
    now = _timestamp()
    policy = _policy()
    policy.requirements[0].required_checks = ["cleanup.detach", "cleanup.teardown"]
    policy.requirements[0].required_resource_kinds = ["relay_job", "connector"]
    detached = _report()
    detached.report_id = "validation_detach"
    detached.checks = [
        ValidationCheck(
            check_id="cleanup.detach",
            summary="detach",
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt="test evidence")],
        )
    ]
    teardown = _report()
    teardown.report_id = "validation_teardown"
    teardown.checks = [
        ValidationCheck(
            check_id="cleanup.teardown",
            summary="teardown",
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt="test evidence")],
        )
    ]
    teardown.resources = [
        ValidationResource(kind="connector", resource_id="connector_1", cluster="primary")
    ]

    result = _evaluate(policy, [detached, teardown])

    assert result.passed is True
    assert result.report_ids == ["validation_detach", "validation_teardown"]


def test_release_gate_does_not_combine_reports_outside_expected_artifact_hash() -> None:
    now = _timestamp()
    policy = _policy()
    policy.requirements[0].required_checks = ["cleanup.detach", "cleanup.teardown"]
    detached = _report()
    detached.checks = [
        ValidationCheck(
            check_id="cleanup.detach",
            summary="detach",
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt="test evidence")],
        )
    ]
    teardown = _report()
    teardown.report_id = "validation_other_artifact"
    teardown.install_source.artifact_sha256 = "c" * 64
    teardown.checks = [
        ValidationCheck(
            check_id="cleanup.teardown",
            summary="teardown",
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt="test evidence")],
        )
    ]

    result = _evaluate(policy, [detached, teardown])

    assert result.passed is False
    assert any(
        "tested artifact SHA-256 does not match the immutable candidate" in reason
        for reason in result.unsatisfied_requirements["remote-mcp-primary"]
    )


def test_release_gate_does_not_combine_unrelated_session_evidence() -> None:
    now = _timestamp()
    policy = _policy()
    requirement = policy.requirements[0]
    requirement.required_checks = ["cleanup.detach", "cleanup.teardown"]
    requirement.required_resource_kinds = ["relay_session"]
    requirement.evidence_group_resource_kind = "relay_session"
    detached = _report()
    detached.checks = [
        ValidationCheck(
            check_id="cleanup.detach",
            summary="detach",
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt="test evidence")],
        )
    ]
    detached.resources = [
        ValidationResource(kind="relay_session", resource_id="session-a", cluster="primary")
    ]
    teardown = _report()
    teardown.report_id = "validation_other_session"
    teardown.checks = [
        ValidationCheck(
            check_id="cleanup.teardown",
            summary="teardown",
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt="test evidence")],
        )
    ]
    teardown.resources = [
        ValidationResource(kind="relay_session", resource_id="session-b", cluster="primary")
    ]

    result = _evaluate(policy, [detached, teardown])

    assert result.passed is False
    assert any(
        "missing passed checks across reports" in reason
        for reason in result.unsatisfied_requirements["remote-mcp-primary"]
    )


def test_release_gate_rejects_one_report_with_multiple_group_resource_ids() -> None:
    policy = _policy()
    requirement = policy.requirements[0]
    requirement.evidence_group_resource_kind = "relay_session"
    requirement.required_resource_kinds = ["relay_session"]
    report = _report()
    report.resources = [
        ValidationResource(kind="relay_session", resource_id="session-a", cluster="primary"),
        ValidationResource(kind="relay_session", resource_id="session-b", cluster="primary"),
    ]

    result = _evaluate(policy, [report])

    assert result.passed is False
    assert any(
        "exactly one evidence-group resource" in reason
        or "no reports share required evidence group" in reason
        for reason in result.unsatisfied_requirements["remote-mcp-primary"]
    )


def test_markdown_is_a_view_of_canonical_report() -> None:
    rendered = render_validation_markdown(_report())

    assert "# live validation validation_test" in rendered
    assert "`passed` `remote-mcp.call`" in rendered
    assert "`relay_job` `job_123`" in rendered


def test_default_report_path_sanitizes_cluster_name(tmp_path: Path) -> None:
    path = default_report_path("site/cluster one", root=tmp_path)

    assert path.parent == tmp_path
    assert path.name.startswith("validation-")
    assert "site-cluster-one" in path.name
    assert path.suffix == ".json"


def test_repository_release_policy_is_machine_readable() -> None:
    policy = load_release_gate_policy(Path("docs/release-gate-1.0.yaml"))

    assert policy.release_version == "1.0.0"
    assert set(policy.targets) == {"ares", "homelab"}
    assert policy.targets["ares"].identity_sha256 == (
        "f114f70f952dad7eccb5c5cfb340feb653e8879080238e5a8465205ff84afa6b"
    )
    requirement_ids = {item.requirement_id for item in policy.requirements}
    assert "ares-non-jarvis-virtual-mcp" in requirement_ids
    assert "ares-lammps-application-boundary" in requirement_ids
    assert "homelab-owned-cleanup" in requirement_ids


def test_repository_policy_target_digests_match_canonical_live_pins() -> None:
    policy = load_release_gate_policy(Path("docs/release-gate-1.0.yaml"))
    ares = _report()
    ares.cluster = "ares"
    _attach_verified_target_identity(
        ares,
        hostname="ares.ares.local",
        fingerprint="SHA256:bRLzWxtUOvr7HWapionskS2S74r21SlOArTukSkKrcw",
        site_marker_sha256=("2162bf2c726235b2d856e1adeaded38c626766245b6e36ef933a0f83cb9d54ea"),
    )
    ares_target = next(resource for resource in ares.resources if resource.kind == "cluster_target")
    ares_target.metadata.update(
        {
            "scheduler_cluster_name": "linux",
            "ssh_host_key_sha256": [
                "SHA256:bRLzWxtUOvr7HWapionskS2S74r21SlOArTukSkKrcw",
                "SHA256:U5VnrrdDfBluAMQpDQ+PSHU9XyaVc5JeJ+u1oSW3zk4",
                "SHA256:rlGpdsBvZOaqcY5d6fKgVCUrAwZhLRGK70v3MNQ/LKA",
            ],
        }
    )

    homelab = _report()
    homelab.report_id = "validation_homelab"
    homelab.cluster = "homelab"
    _attach_verified_target_identity(
        homelab,
        hostname="Server",
        fingerprint="SHA256:Icn/LmcEIhEg//DW9ClFNxUrP3lawXtiQM7PzuCHUpY",
        site_marker_sha256=("4a966c6fb25b0aef845a064fdb5243269d749c81370ba015172cefde5fd1f32f"),
    )
    homelab_target = next(
        resource for resource in homelab.resources if resource.kind == "cluster_target"
    )
    homelab_target.provider = "external"
    homelab_target.metadata.update(
        {
            "scheduler_provider": "external",
            "scheduler_cluster_name": None,
            "ssh_host_key_sha256": [
                "SHA256:Icn/LmcEIhEg//DW9ClFNxUrP3lawXtiQM7PzuCHUpY",
                "SHA256:wq6LnyUAsrEEe/mK+jHaOlpCF/CLoWa9xxgefvBR1JY",
            ],
        }
    )

    result = _evaluate(policy, [ares, homelab])

    assert "target-identity" not in result.unsatisfied_requirements
    assert result.policy_target_identity_sha256 == result.target_identity_sha256


def test_lammps_application_boundary_gate_rejects_and_accepts_provider_attestation() -> None:
    repository_policy = load_release_gate_policy(Path("docs/release-gate-1.0.yaml"))
    requirement = next(
        item
        for item in repository_policy.requirements
        if item.requirement_id == "ares-lammps-application-boundary"
    )
    policy = repository_policy.model_copy(
        update={
            "release_blockers": [],
            "requirements": [requirement],
            "targets": {
                "ares": _target_policy(hostnames=["ares-login.example"]),
            },
        }
    )
    report = _report()
    report.scenario = "live-test"
    report.cluster = "ares"
    now = _timestamp()
    report.checks = [
        ValidationCheck(
            check_id=check_id,
            summary=check_id,
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt=f"{check_id}=passed")],
        )
        for check_id in requirement.required_checks
    ]
    provider_metadata = {
        "adapter": "lammps",
        "package_name": "builtin.lammps",
        "package_version": "builtin",
        "provider_entry_point": "lammps",
        "provider_entry_point_value": "jarvis_cd.progress.lammps:adapter_from_package",
        "provider_distribution": "jarvis_cd",
        "provider_distribution_version": "2.0.0",
        "provider_source_authority": "package_log",
        "provider_validated": True,
        "acceptance_validated": False,
    }
    jarvis_component = {
        "distribution": "jarvis_cd",
        "distribution_version": "2.0.0",
        "install_spec": (
            "https://github.com/grc-iit/jarvis-cd/releases/download/"
            "v2.0.0/jarvis_cd-2.0.0-py3-none-any.whl"
        ),
        "requested_source": "github_release",
        "artifact_filename": "jarvis_cd-2.0.0-py3-none-any.whl",
        "artifact_sha256": "PENDING_JARVIS_CD_2_0_0_RELEASE_WHEEL_SHA256",
        "entry_points": ["clio_relay.package_progress_adapters:lammps"],
    }
    report.resources = [
        ValidationResource(
            kind="relay_job",
            resource_id="job_lammps",
            cluster="ares",
            state="succeeded",
        ),
        ValidationResource(
            kind="relay_worker",
            resource_id="worker:ares",
            role="cluster_worker",
            cluster="ares",
            state="running",
            metadata={
                "components": {"jarvis-cd": "2.0.0"},
                "component_artifacts": {"jarvis-cd": jarvis_component},
                "component_runtime": {
                    "jarvis-cd": {
                        "verified": True,
                        "artifact_sha256_verified": True,
                        "provider_interpreter_verified": True,
                        "execution_interpreter_verified": True,
                        "execution_distribution_identity_verified": True,
                        "execution_entry_points_visible": True,
                        "execution_source_verified": True,
                        "jarvis_executable_verified": True,
                        "extra_runtime_evidence": "preserved",
                    }
                },
            },
        ),
        ValidationResource(
            kind="package_progress_provider",
            resource_id="jarvis_cd:2.0.0:lammps:lammps",
            role="jarvis_package_progress",
            cluster="ares",
            state="verified",
            provider="jarvis_cd",
            metadata=provider_metadata,
        ),
    ]
    _attach_verified_target_identity(report, hostname="ares-login.example")

    rejected = _evaluate(policy, [report])
    provider_metadata["acceptance_validated"] = True
    progress_provider = next(
        resource for resource in report.resources if resource.kind == "package_progress_provider"
    )
    progress_provider.metadata = provider_metadata
    accepted = _evaluate(policy, [report])

    assert rejected.passed is False
    assert accepted.passed is True


def test_report_invocation_redacts_cli_secrets(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(
        "sys.orig_argv",
        ["clio-relay", "live-test", "--transport-token", "secret-value"],
    )

    report = new_live_validation_report(
        scenario="live-test",
        cluster="primary",
        install_source="checkout:file:///checkout",
    )

    assert "secret-value" not in report.invocation
    assert report.invocation[-1] == "<redacted>"


def test_remote_mcp_acceptance_converts_to_canonical_report() -> None:
    domain = RemoteMcpAcceptanceReport(
        generated_at=_timestamp(),
        cluster="primary",
        server_name="science",
        remote_tool_name="inspect",
        virtual_alias="remote_science_inspect",
        profile="user",
        passed=True,
        checks=[
            RemoteMcpAcceptanceCheck(
                name="remote-mcp.call",
                passed=True,
                message="durable virtual call succeeded",
                evidence={"job_id": "job_call"},
            )
        ],
        discovery={
            "provenance": {
                "discovery_job_id": "job_discovery",
                "artifact_id": "artifact_schema",
            }
        },
        call_job={"job_id": "job_call", "state": "succeeded"},
        artifacts=[
            {
                "artifact_id": "artifact_result",
                "kind": "mcp_result",
                "uri": "file:///result.json",
                "sha256": "a" * 64,
            }
        ],
    )

    canonical = domain.to_live_validation_report(
        launcher="uvx",
        install_source="pypi:clio-relay==1.0.0",
        artifact_sha256="b" * 64,
    )

    assert canonical.scenario == "remote-mcp"
    assert canonical.status is ValidationStatus.PASSED
    assert canonical.checks[0].check_id == "remote-mcp.call"
    assert {resource.resource_id for resource in canonical.resources} == {
        "job_call",
        "job_discovery",
        "artifact_schema",
        "artifact_result",
    }


def test_session_cleanup_converts_to_canonical_report_with_safe_job_default() -> None:
    lifecycle = SessionLifecycleReport(
        cluster="primary",
        session_id="desktop-session",
        session_generation_id="generation-123",
        mode="teardown",
        prior_session_status=RemoteSessionStateEvidence(
            api_pid=123,
            session_generation_id="generation-123",
            process_start_marker="start-123",
            running=True,
            ownership_verified=True,
            observed_at=datetime.now(UTC),
            started_at=datetime.now(UTC),
        ),
        post_session_status=RemoteSessionStateEvidence(
            api_pid=123,
            session_generation_id="generation-123",
            process_start_marker="start-123",
            running=False,
            ownership_verified=True,
            observed_at=datetime.now(UTC),
            started_at=datetime.now(UTC),
        ),
        resources=[
            CleanupResource(
                kind="remote_relay_api",
                resource_id="123",
                location="primary-login",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
            ),
            CleanupResource(
                kind="scheduler_job",
                resource_id="slurm-123",
                location="primary-login",
                action="retain",
                ownership_verified=True,
                outcome="retained",
                provider="slurm",
                verified_after_operation=True,
            ),
        ],
    )

    canonical = lifecycle.to_live_validation_report(cancel_jobs=False)

    assert canonical.scenario == "cleanup"
    assert canonical.status is ValidationStatus.PASSED
    assert {check.check_id for check in canonical.checks} == {
        "cleanup.relay-session",
        "cleanup.jobs-preserved-default",
        "cleanup.no-owned-resources",
    }
    assert canonical.cleanup.cancel_scheduler_jobs is False
    assert canonical.resources[0].kind == "relay_session"


def test_nonexistent_session_teardown_is_not_passing_acceptance_evidence() -> None:
    lifecycle = SessionLifecycleReport(
        cluster="primary",
        session_id="never-started",
        mode="teardown",
        prior_session_status=RemoteSessionStateEvidence(
            running=False,
            ownership_verified=False,
            observed_at=datetime.now(UTC),
        ),
        post_session_status=RemoteSessionStateEvidence(
            running=False,
            ownership_verified=False,
            observed_at=datetime.now(UTC),
        ),
        resources=[
            CleanupResource(
                kind="remote_relay_api",
                resource_id="never-started",
                location="primary-login",
                action="stop",
                ownership_verified=False,
                outcome="missing",
            )
        ],
    )

    canonical = lifecycle.to_live_validation_report(cancel_jobs=False)

    checks = {check.check_id: check.status for check in canonical.checks}
    assert canonical.status is ValidationStatus.FAILED
    assert checks["cleanup.relay-session"] is ValidationStatus.FAILED
    assert checks["cleanup.no-owned-resources"] is ValidationStatus.FAILED


def test_gateway_start_and_stop_reports_combine_for_release_gate() -> None:
    ready = GatewaySession(
        session_id="gateway_1",
        cluster="primary",
        name="service",
        state=GatewaySessionState.READY,
        scheduler="slurm",
        scheduler_job_id="123",
        queue_state="running",
        node="compute-1",
        gateway={
            "transport": {
                "remote_connector": {"pid": 11, "config_path": "/tmp/remote.toml"},
                "desktop_connector": {"pid": 12, "config_path": "/tmp/local.toml"},
            }
        },
    )
    started = ServiceRuntimeStartResult(
        session=ready,
        connect_url="http://127.0.0.1:18080",
        health_url="http://127.0.0.1:18080/healthz",
        stream_url="http://127.0.0.1:18080/stream",
        compatibility_urls={},
        events_url=None,
    ).to_live_validation_report()
    stopped_session = ready.model_copy(update={"state": GatewaySessionState.CLOSED})
    stopped = ServiceRuntimeStopResult(
        session=stopped_session,
        mode="teardown",
        stopped_local_pid=12,
        stopped_remote_pid=11,
        canceled_scheduler_job=None,
        resources=[
            CleanupResource(
                kind="desktop_connector",
                resource_id="12",
                location="desktop",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
            ),
            CleanupResource(
                kind="remote_connector",
                resource_id="11",
                location="primary",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
            ),
            CleanupResource(
                kind="scheduler_job",
                resource_id="123",
                location="primary",
                provider="slurm",
                action="retain",
                ownership_verified=True,
                outcome="retained",
                verified_after_operation=True,
                observed_state="running",
            ),
        ],
        errors=[],
    ).to_live_validation_report()

    assert started.status is ValidationStatus.PASSED
    assert stopped.status is ValidationStatus.PASSED
    assert {check.check_id for check in started.checks} == {
        "gateway.submit",
        "gateway.allocated",
        "gateway.ready",
        "gateway.connect",
    }
    assert {check.check_id for check in stopped.checks} == {
        "gateway.stop-connectors",
        "gateway.jobs-preserved-default",
        "gateway.closed-record",
    }
