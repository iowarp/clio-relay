from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path
from typing import cast

import yaml

from clio_relay.models import ServiceRuntimeSpec

ROOT = Path(__file__).parents[1]
FIXTURES = ROOT / "examples" / "release-gate"
RUNBOOK = ROOT / "docs" / "release-acceptance-1.0.md"
POLICY = ROOT / "docs" / "release-gate-1.0.yaml"


def _render(path: Path, replacements: dict[str, str]) -> str:
    text = path.read_text(encoding="utf-8")
    for name, value in replacements.items():
        text = text.replace(f"__{name}__", value)
    assert "__" not in text
    return text


def _matrix_reports() -> list[dict[str, object]]:
    document = cast(
        dict[str, object],
        json.loads((FIXTURES / "report-matrix-1.0.json").read_text(encoding="utf-8")),
    )
    assert document["report_count_per_stage"] == 17
    return cast(list[dict[str, object]], document["reports"])


def test_release_identity_is_consistent_across_package_policy_matrix_and_runbook() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    policy = cast(dict[str, object], yaml.safe_load(POLICY.read_text(encoding="utf-8")))
    matrix = cast(
        dict[str, object],
        json.loads((FIXTURES / "report-matrix-1.0.json").read_text(encoding="utf-8")),
    )
    version = cast(dict[str, object], project["project"])["version"]

    assert policy["release_version"] == version
    assert matrix["release_version"] == version
    assert f'$Version = "{version}"' in RUNBOOK.read_text(encoding="utf-8")


def test_candidate_manifest_parser_accepts_the_exact_gnu_checksum_separator() -> None:
    digest = "a" * 64
    wheel_name = "clio_relay-1.0.2-py3-none-any.whl"
    selector = re.compile(rf"^[0-9A-Fa-f]{{64}} [ *]{re.escape(wheel_name)}$")
    parser = re.compile(r"^([0-9A-Fa-f]{64}) [ *](.+)$")

    for mode in ("*", " "):
        line = f"{digest} {mode}{wheel_name}"
        assert selector.fullmatch(line) is not None
        match = parser.fullmatch(line)
        assert match is not None
        assert match.groups() == (digest, wheel_name)

    assert selector.fullmatch(f"{digest}*{wheel_name}") is None


def test_release_helper_scripts_bind_direct_gray_scott_and_private_spack_state() -> None:
    gray = (FIXTURES / "gray-scott-direct-build.sh").read_text(encoding="utf-8")
    fresh_spack = (FIXTURES / "spack-fresh-store.sh").read_text(encoding="utf-8")

    assert "https://github.com/iowarp/clio-core.git" in gray
    assert "rev-parse HEAD:external/iowarp-gray-scott" in gray
    assert 'build-env "/$adios_hash"' in gray
    assert '[[ "$value" == *//* ]]' in gray
    assert '[[ "$value" == *"/./"* ]]' in gray
    assert "coeus" not in gray.lower()
    assert "hermes" not in gray.lower()

    assert "concretizer:\n  reuse: false" in fresh_spack
    assert "mirrors:: {}" in fresh_spack
    assert "upstreams:: {}" in fresh_spack
    assert "SPACK_USER_CONFIG_PATH" in fresh_spack
    assert "acceptance-manifest.sha256" in fresh_spack
    assert "sha256sum --check --strict" in fresh_spack
    assert '[[ "$value" == *//* ]]' in fresh_spack
    assert '[[ "$value" == *"/./"* ]]' in fresh_spack


def test_release_acceptance_matrix_is_complete_ordered_and_unique() -> None:
    reports = _matrix_reports()

    assert [report["ordinal"] for report in reports] == list(range(1, 18))
    assert len({cast(str, report["id"]) for report in reports}) == 17
    assert reports[2]["id"] == "ares-queue-management"
    assert all(
        cast(int, report["ordinal"]) > 3
        for report in reports
        if report["scenario"] in {"cleanup", "transport", "gateway-runtime"}
    )
    jarvis_reports = [
        report for report in reports if cast(str, report["id"]).startswith("ares-jarvis-")
    ]
    assert [report.get("package") for report in jarvis_reports] == [
        "builtin.gray_scott",
        "builtin.lammps",
    ]
    assert [report.get("remote_tool") for report in reports if "remote_tool" in report] == [
        "spack_find",
        "spack_locate",
        "spack_install",
    ]


def test_release_acceptance_upload_checks_derive_matrix_cardinality() -> None:
    runbook = RUNBOOK.read_text(encoding="utf-8")

    assert "-ne 17" not in runbook
    assert runbook.count("-ne $Matrix.report_count_per_stage") >= 3


def test_release_acceptance_matrix_preserves_evidence_groups() -> None:
    reports = _matrix_reports()
    grouped: dict[str, list[str]] = {}
    for report in reports:
        group = report.get("evidence_group")
        if isinstance(group, str):
            grouped.setdefault(group, []).append(cast(str, report["id"]))

    assert grouped["ares-default-cleanup-session"] == [
        "ares-cleanup-detach",
        "ares-cleanup-teardown",
    ]
    assert grouped["homelab-default-cleanup-session"] == [
        "homelab-cleanup-detach",
        "homelab-cleanup-teardown",
    ]
    assert grouped["ares-dedicated-gateway"] == [
        "ares-gateway-start",
        "ares-gateway-stop",
    ]


def test_release_policy_groups_bootstrap_and_worker_proof_by_physical_target() -> None:
    document = cast(dict[str, object], yaml.safe_load(POLICY.read_text(encoding="utf-8")))
    requirements = cast(list[dict[str, object]], document["requirements"])
    bootstrap = next(
        requirement
        for requirement in requirements
        if requirement["requirement_id"] == "ares-released-bootstrap"
    )

    assert bootstrap["evidence_group_resource_kind"] == "cluster_target"


def test_release_policy_requires_existing_lammps_and_fresh_install_transition() -> None:
    document = cast(dict[str, object], yaml.safe_load(POLICY.read_text(encoding="utf-8")))
    requirements = cast(list[dict[str, object]], document["requirements"])
    requirement = next(
        item for item in requirements if item["requirement_id"] == "ares-spack-virtual-mcp"
    )
    checks = cast(list[str], requirement["required_checks"])
    assert "remote-mcp.structured-result" in checks

    resources = cast(list[dict[str, object]], requirement["required_resources"])
    calls = {
        cast(
            str, cast(dict[str, object], resource["metadata_equals"])["remote_mcp_tool_name"]
        ): cast(dict[str, object], resource["metadata_equals"])
        for resource in resources
        if resource["kind"] == "relay_job"
    }
    expected_hash = "p5gjmq4rseitqanua7mdd2zdnag4v3u2"
    assert set(calls) == {"spack_find", "spack_locate"}
    for tool, metadata in calls.items():
        assertion = cast(dict[str, object], metadata["structured_result_assertion"])
        expected = cast(dict[str, object], assertion["expected"])
        observed = cast(dict[str, object], assertion["observed"])
        assert expected["tool"] == tool
        assert expected["dag_hash"] == expected_hash
        assert observed["structured_content_present"] is True
        assert assertion["failures"] == []
    locate = cast(
        dict[str, object],
        calls["spack_locate"]["structured_result_assertion"],
    )
    locate_observed = cast(dict[str, object], locate["observed"])
    assert locate_observed["load_spec"] == f"/{expected_hash}"
    expected_prefix = (
        "/mnt/common/jcernudagarcia/spack/opt/spack/"
        "linux-ubuntu22.04-skylake_avx512/gcc-11.4.0/"
        f"lammps-20240829.1-{expected_hash}"
    )
    assert cast(dict[str, object], locate["expected"])["prefix"] == expected_prefix
    assert locate_observed["prefix"] == expected_prefix
    assert locate_observed["prefix_is_canonical_absolute"] is True
    assert locate_observed["prefix_matches_expected"] is True
    fresh = next(
        item for item in requirements if item["requirement_id"] == "ares-spack-fresh-install"
    )
    assert cast(list[str], fresh["required_checks"]) == [
        "remote-mcp.spack-preinstall-absent",
        "remote-mcp.spack-fresh-install",
        "remote-mcp.spack-postinstall-locate",
        "remote-mcp.spack-disposable-store",
        "remote-mcp.spack-transition-identity",
        "remote-mcp.spack-transition-durable-evidence",
        "remote-mcp.spack-fresh-configuration",
    ]
    transition = cast(dict[str, object], fresh["spack_fresh_install_transition"])
    assert transition == {
        "schema_version": "clio-relay.release-spack-fresh-install.v1",
        "server_name": "spack-fresh",
        "profile": "user",
        "package_name": "libsigsegv",
        "requested_spec": "libsigsegv@2.14",
        "reuse": False,
    }
    fresh_resources = cast(list[dict[str, object]], fresh["required_resources"])
    phase_roles = {
        cast(list[str], resource["roles"])[0]
        for resource in fresh_resources
        if resource["kind"] == "relay_job"
    }
    assert phase_roles == {
        "spack_preinstall_find",
        "spack_fresh_install",
        "spack_postinstall_locate",
    }
    assert any(resource["kind"] == "configuration_manifest" for resource in fresh_resources)


def test_release_acceptance_yaml_templates_are_real_bounded_pipelines() -> None:
    replacements = {
        "RUN_ID": "released-1-0-acceptance",
        "REMOTE_ROOT": "/tmp/clio-relay-release-acceptance",
    }
    for name in (
        "ares-bootstrap-echo.yaml.tmpl",
        "homelab-transport-echo.yaml.tmpl",
        "owned-cleanup-ares.yaml.tmpl",
        "owned-cleanup-homelab.yaml.tmpl",
        "owned-cancel-ares.yaml.tmpl",
    ):
        document = cast(dict[str, object], yaml.safe_load(_render(FIXTURES / name, replacements)))
        packages = cast(list[dict[str, object]], document["pkgs"])
        assert packages
        assert all(isinstance(package.get("pkg_type"), str) for package in packages)


def test_release_acceptance_gateway_templates_match_runtime_contract() -> None:
    replacements = {
        "RUN_ID": "released-1-0-gateway",
        "REMOTE_FIXTURE_ROOT": "/tmp/clio-relay-release-fixtures",
        "REMOTE_STATE_ROOT": "/tmp/clio-relay-release-state",
        "SERVICE_PORT": "19080",
        "DESKTOP_PORT": "29080",
        "HEALTH_NONCE": "a" * 64,
    }
    ares = ServiceRuntimeSpec.model_validate_json(
        _render(FIXTURES / "gateway" / "ares-runtime.json.tmpl", replacements)
    )
    homelab = ServiceRuntimeSpec.model_validate_json(
        _render(FIXTURES / "gateway" / "homelab-runtime.json.tmpl", replacements)
    )

    assert ares.scheduler == "slurm"
    assert ares.deployment_driver == "scheduler"
    assert ares.status_command is not None and "{scheduler_job_id}" in ares.status_command
    assert ares.health_expected_body == "a" * 64
    assert homelab.scheduler == "external"
    assert homelab.deployment_driver == "custom"
    assert homelab.cancel_command is not None and "{scheduler_job_id}" in homelab.cancel_command
    assert homelab.health_expected_body == "a" * 64


def test_release_acceptance_fixture_lifetimes_dominate_operation_budgets() -> None:
    ares_submit = (FIXTURES / "gateway" / "slurm_submit.sh").read_text(encoding="utf-8")
    homelab_runtime = (FIXTURES / "gateway" / "homelab-runtime.json.tmpl").read_text(
        encoding="utf-8"
    )
    ares_cleanup = (FIXTURES / "owned-cleanup-ares.yaml.tmpl").read_text(encoding="utf-8")
    ares_cancel = (FIXTURES / "owned-cancel-ares.yaml.tmpl").read_text(encoding="utf-8")
    homelab_cleanup = (FIXTURES / "owned-cleanup-homelab.yaml.tmpl").read_text(encoding="utf-8")

    assert "--lifetime-seconds 900" in ares_submit
    assert "--time 00:20:00" in ares_submit
    assert '"600"' in homelab_runtime
    assert 'time: "00:20:00"' in ares_cleanup and "sleep 900" in ares_cleanup
    assert 'time: "00:30:00"' in ares_cancel and "sleep 1200" in ares_cancel
    assert "sleep 900" in homelab_cleanup and "timeout_seconds: 960" in homelab_cleanup


def test_release_acceptance_runbook_binds_production_specifics_without_secrets() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "Windows PowerShell 5.1 or newer" in text
    assert "uv tool install --force --python 3.12 --no-config" in text
    assert "uv tool install --force --refresh --no-config" in text
    assert "CLIO_RELAY_VALIDATION_INVOCATION_ID" in text
    assert '--validation-scenario", "cluster-bootstrap' in text
    assert "--verify-cluster-deployment" in text
    assert "--require-structured-runtime-metadata" in text
    assert '$GrayExecutable = "$GrayInstallRoot/bin/gray-scott"' in text
    assert "examples/release-gate/gray-scott-direct-build.sh" in text
    assert '$ExpectedCoreCommit = "e2fedd8847f8deb71f041f692e405023a712ca44"' in text
    assert '$ExpectedGrayTree = "072d6eab3df3bde92e48ae2f4823305af831535e"' in text
    assert "$ExpectedAdiosHash = [string]$env:CLIO_RELAY_ACCEPTANCE_ADIOS_HASH" in text
    assert "find --json '/$ExpectedAdiosHash'" in text
    assert "find --json adios2" not in text
    assert 'spack_specs = @("/$ExpectedAdiosHash")' in text
    assert 'spack_specs = @("/$ExpectedLammpsHash")' in text
    assert 'spack_specs = @("adios2-coeus")' not in text
    assert "find --format '{hash}' adios2-coeus" not in text
    slurm_submit = (FIXTURES / "gateway" / "slurm_submit.sh").read_text(encoding="utf-8")
    assert '[[ ! "$job_id" =~ ^[0-9]+$ ]]' in slurm_submit
    assert "examples/release-gate/spack-fresh-store.sh" in text
    assert "spec --format '{hash}' '$FreshSpackSpec'" in text
    assert "fresh_install_store_root = $FreshSpackStore" in text
    assert "fresh_install_configuration_manifest_path = $FreshSpackConfigurationManifest" in text
    assert "fresh_install_configuration_sha256 = $ExpectedFreshSpackConfigurationSha256" in text
    assert '"--name", "spack-fresh"' in text
    assert '$ExpectedLammpsHash = "p5gjmq4rseitqanua7mdd2zdnag4v3u2"' in text
    assert "$LammpsHashes.Count -ne 1" in text
    assert "$ExpectedLammpsPrefix" in text
    assert "expected exact LAMMPS prefix is unavailable or non-canonical" in text
    assert "--contract clio-kit-spack-user-v2" in text
    assert "--allow-tool spack_find --allow-tool spack_locate --allow-tool spack_install" in text
    assert '"--result-expectation-json-file", $Expectation' in text
    assert '--keep-jobs", "--keep-scheduler-jobs' in text
    assert '--cancel-jobs", "--cancel-scheduler-jobs' in text
    assert "--preserve-scheduler-job-id" in text
    assert '{ "validation" } else { "released-validation" }' in text
    assert '"--transport-local-bind-port"' in text
    assert '"--transport-remote-api-port"' in text
    assert '"--ssh-transport-local-bind-port"' in text
    assert '"--ssh-transport-remote-api-port"' in text
    assert '"--no-verify-direct-transport"' in text
    assert '"--no-allow-direct-transport-fallback"' in text
    assert "cluster registry contains a blank secret environment-variable name" in text
    assert '"--transport-local-port"' not in text
    assert '"--transport-remote-port"' not in text
    assert '"--ssh-transport-local-port"' not in text
    assert '"--ssh-transport-remote-port"' not in text
    assert "not product allowlists" in text
    assert "REPLACE_WITH_VERIFIED_CANDIDATE_MANIFEST_DIGEST" not in text
    assert '$GitHubRepo = "github.com/iowarp/clio-relay"' in text
    assert "gh release download $Tag --repo $GitHubRepo" in text
    assert "gh api --hostname github.com user" in text
    assert "$ManifestLines.Count -ne 1" in text
    assert "^[0-9A-Fa-f]{64} [ *]$([Regex]::Escape($WheelName))$" in text
    assert "^([0-9A-Fa-f]{64}) [ *](.+)$" in text
    assert "$Observed -ne $ExpectedWheelSha256" in text
    assert "gh attestation verify $Wheel --hostname github.com --repo iowarp/clio-relay" in text
    assert "$CheckoutCommit -ne $TagCommit" in text
    assert "acceptance tag checkout is not clean" in text
    assert "$AresSshHost = [string]$AresDefinition.ssh_host" in text
    assert "$AresHome = Get-RemoteHome $AresSshHost" in text
    assert "$AresSpack = [string]$AresDefinition.spack_executable" in text
    assert '$AresFreshBaseSpack = "$AresHome/spack/bin/spack"' in text
    assert "'$FreshSpackRoot' '$AresFreshBaseSpack'" in text
    assert "$Promotion.version -cne $Version" in text
    assert "promotion wheel digest is missing or malformed" in text
    index_cleanup = text.index("$AllowedUvLocationEnvironment = @(")
    candidate_install = text.index("uv tool install --force --python 3.12 --no-config")
    assert index_cleanup < candidate_install
    assert '$_.Name -like "UV_*"' in text
    assert '@("UV_CACHE_DIR", "UV_TOOL_DIR", "UV_TOOL_BIN_DIR")' in text
    assert "--default-index https://pypi.org/simple $Wheel" in text
    assert "$AdiosLibraries.Count -eq 0" in text
    assert "$MpiLibraries.Count -ne 1" in text
    assert "health_expected_body" not in text
    assert "HEALTH_NONCE = $AresDefaultHealthNonce" in text
    assert "desktop acceptance port is already occupied" in text
    assert '"ExitOnForwardFailure=yes"' in text
    assert "$Forward.HasExited" in text
    assert "$AresDefaultLocalPort, $CancelLocalPort, $HomelabDefaultLocalPort" in text
    assert "$HomelabTransportLocalPort, $HomelabSshTransportLocalPort" in text
    assert "$SentinelStatus.phase" in text
    assert "CLIO_RELAY_FRP_TOKEN=" not in text
    assert "CLIO_RELAY_STCP_SECRET=" not in text
    assert "$_.evidence_trust.invocation_id" in text
    assert "$_.producer.invocation_id" not in text
    assert "$ExpectedNames" in text
    assert "$ActualNames" in text
    assert "policy report file set or order does not match the release matrix" in text
    assert "$Observed.cluster -ne $Expected.cluster" in text
    assert "$Observed.scenario -ne $Expected.scenario" in text
    assert "Invoke-EmergencySessionCleanup" in text
    assert "Invoke-EmergencyGatewayCleanup" in text
    assert "\"emergency-session-$([guid]::NewGuid().ToString('N'))\"" in text
    assert "\"emergency-gateway-$([guid]::NewGuid().ToString('N'))\"" in text
    assert "RELEASED-VALIDATION-BINDING.json" in text
    assert "refusing to modify a sealed evidence stage" in text
    assert "gh release delete-asset $Tag $AssetName --repo $GitHubRepo --yes" in text
    assert "-ConfirmDiscardEntireIncompleteStage" in text
    assert "Do not run this section during a normal acceptance pass" in text
    assert "--clobber" in text


def test_release_acceptance_creates_remote_output_roots_before_first_pipeline() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    root_creation = text.index("remote acceptance root creation failed")
    first_pipeline = text.index('Invoke-RelayReport -Id "ares-cluster-bootstrap-live-test"')
    assert root_creation < first_pipeline


def test_release_runbooks_require_main_to_remain_frozen_through_finalization() -> None:
    release = (ROOT / "docs" / "release.md").read_text(encoding="utf-8")
    acceptance = RUNBOOK.read_text(encoding="utf-8")

    assert "keep `main` frozen" in release
    assert "advancing `main` after PyPI publication" in release
    assert "confirm that maintainers have frozen" in acceptance
    assert "candidate tag commit through finalization" in acceptance
    assert "moving the protected tag" in acceptance
