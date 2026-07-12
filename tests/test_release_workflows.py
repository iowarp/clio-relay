"""Guard the irreversible ordering and provenance checks in release workflows."""

from __future__ import annotations

import re
import textwrap
from pathlib import Path
from typing import Any, cast

import yaml

ROOT = Path(__file__).parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"


def _workflow(name: str) -> dict[str, Any]:
    document = yaml.load(
        (WORKFLOWS / name).read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )
    return cast(dict[str, Any], document)


def test_tag_workflow_only_builds_an_unprivileged_actions_payload() -> None:
    workflow = _workflow("release.yml")
    jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
    text = (WORKFLOWS / "release.yml").read_text(encoding="utf-8")

    assert "gh-action-pypi-publish" not in text
    assert "actions/attest@" not in text
    assert "gh release create" not in text
    assert "gh release upload" not in text
    assert "environment:" not in text
    assert "--clobber" not in text
    assert set(jobs) == {"build"}
    assert jobs["build"]["permissions"] == {}
    assert "actions/checkout@" not in text
    assert "CI-STATUS.json" not in text
    assert "REPOSITORY-GOVERNANCE.json" not in text
    assert "https://github.com/$GITHUB_REPOSITORY.git" in text
    assert "actions/upload-artifact@" in text


def test_protected_main_staging_attests_and_creates_the_explicit_target_draft() -> None:
    workflow = _workflow("stage-candidate.yml")
    jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
    stage = jobs["stage"]
    text = (WORKFLOWS / "stage-candidate.yml").read_text(encoding="utf-8")

    assert stage["needs"] == "authorize"
    assert cast(dict[str, str], stage["environment"])["name"] == "live-validation"
    assert cast(dict[str, str], stage["permissions"])["contents"] == "write"
    assert "actions/attest@" in text
    assert "gh release create" in text
    assert '--target "$SOURCE_COMMIT"' in text
    assert '--signer-workflow "$REPOSITORY/.github/workflows/stage-candidate.yml"' in text
    assert '--source-ref "refs/heads/main"' in text
    assert "ci_validation.py mutation-authority" in text
    assert "--release-state absent" in text
    assert "--release-state present" in text
    assert "ci_validation.py actions-artifact-manifest" in text
    assert "ci_validation.py extract-actions-artifact" in text


def test_final_policy_requires_published_artifacts_and_retains_external_blockers() -> None:
    policy = cast(
        dict[str, Any],
        yaml.safe_load((ROOT / "docs" / "release-gate-1.0.yaml").read_text(encoding="utf-8")),
    )

    assert policy["artifact_stage"] == "published"
    assert policy["evidence_trust_model"] == "reviewer_sealed_operator_evidence"
    assert policy["require_released_artifact"] is True
    assert policy["require_target_identity"] is True
    assert policy["allowed_install_sources"] == ["pypi"]
    assert policy["allowed_launchers"] == ["uvx"]
    blockers = cast(list[str], policy["release_blockers"])
    assert len(blockers) == 5
    assert any("JARVIS-CD" in blocker for blocker in blockers)
    assert any("clio-kit" in blocker for blocker in blockers)
    assert any("owned-process containment" in blocker for blocker in blockers)
    assert any("retention" in blocker for blocker in blockers)
    assert any("Python 3.14" in blocker for blocker in blockers)
    assert not any("nested" in blocker for blocker in blockers)
    requirement_ids = {
        item["requirement_id"] for item in cast(list[dict[str, Any]], policy["requirements"])
    }
    assert "ares-spack-virtual-mcp" in requirement_ids
    local = next(
        item
        for item in cast(list[dict[str, Any]], policy["requirements"])
        if item["requirement_id"] == "local-release-gate"
    )
    assert {
        "local.dependency-lock-export",
        "local.dependency-audit",
        "local.build-backend",
        "local.runtime-lock-export",
        "local.wheel-smoke",
        "local.sdist-smoke",
        "local.containment-hard-crash",
        "local.sidecar-reclamation",
        "local.retention-storage-pagination",
    } <= set(cast(list[str], local["required_checks"]))


def test_ci_uses_read_only_permissions_and_nonpersistent_checkout_credentials() -> None:
    workflow = _workflow("ci.yml")
    permissions = cast(dict[str, str], workflow["permissions"])
    jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
    steps = cast(list[dict[str, Any]], jobs["test"]["steps"])
    checkout = next(step for step in steps if "actions/checkout@" in str(step.get("uses")))
    strategy = cast(dict[str, Any], jobs["test"]["strategy"])
    matrix = cast(dict[str, list[str]], strategy["matrix"])

    assert permissions == {"contents": "read"}
    assert cast(dict[str, str], checkout["with"])["persist-credentials"] == "false"
    assert matrix["os"] == ["ubuntu-latest", "windows-latest"]
    assert matrix["python-version"] == ["3.12", "3.13", "3.14"]
    workflow_lint = str(jobs["workflow-lint"])
    assert "actionlint_1.7.12_linux_amd64.tar.gz" in workflow_lint
    assert "8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8" in (workflow_lint)
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'requires-python = ">=3.12,<3.15"' in pyproject
    assert 'requires = ["hatchling==1.31.0"]' in pyproject


def test_jarvis_release_requirement_enforces_remote_contract_and_spack_reload() -> None:
    policy = cast(
        dict[str, Any],
        yaml.safe_load((ROOT / "docs" / "release-gate-1.0.yaml").read_text(encoding="utf-8")),
    )
    requirements = {
        item["requirement_id"]: item for item in cast(list[dict[str, Any]], policy["requirements"])
    }
    jarvis = requirements["ares-jarvis-virtual-mcp"]

    assert "remote-mcp.jarvis-remote-contract" in jarvis["required_checks"]
    assert "jarvis.spack-runtime-environment" in jarvis["required_checks"]
    server = next(
        resource
        for resource in cast(list[dict[str, Any]], jarvis["required_resources"])
        if resource["kind"] == "mcp_server"
    )
    assert server["metadata_equals"]["server_process_artifact_verified"] is True
    worker = next(
        resource
        for resource in cast(list[dict[str, Any]], jarvis["required_resources"])
        if resource["kind"] == "relay_worker"
    )
    jarvis_component = worker["metadata_equals"]["component_artifacts"]["jarvis-cd"]
    assert jarvis_component["distribution_version"] == "2.0.0"
    assert jarvis_component["requested_source"] == "github_release"
    assert jarvis_component["install_spec"].endswith("/v2.0.0/jarvis_cd-2.0.0-py3-none-any.whl")
    assert jarvis_component["artifact_sha256"] == "PENDING_JARVIS_CD_2_0_0_RELEASE_WHEEL_SHA256"
    runtime = worker["metadata_equals"]["component_runtime"]["jarvis-cd"]
    assert runtime["provider_interpreter_verified"] is True
    assert runtime["execution_interpreter_verified"] is True
    assert runtime["jarvis_executable_verified"] is True


def test_spack_release_requirement_enforces_exact_user_contract() -> None:
    policy = cast(
        dict[str, Any],
        yaml.safe_load((ROOT / "docs" / "release-gate-1.0.yaml").read_text(encoding="utf-8")),
    )
    requirements = {
        item["requirement_id"]: item for item in cast(list[dict[str, Any]], policy["requirements"])
    }
    spack = requirements["ares-spack-virtual-mcp"]

    assert "remote-mcp.spack-user-contract" in spack["required_checks"]
    server = next(
        resource
        for resource in cast(list[dict[str, Any]], spack["required_resources"])
        if resource["kind"] == "mcp_server"
    )
    assert server["metadata_equals"]["remote_tool_names"] == [
        "spack_find",
        "spack_install",
        "spack_locate",
    ]
    assert server["metadata_equals"]["allowlisted_tool_names"] == [
        "spack_find",
        "spack_install",
        "spack_locate",
    ]
    calls = {
        resource["metadata_equals"]["remote_mcp_tool_name"]: resource["metadata_equals"]["spec"][
            "arguments"
        ]
        for resource in cast(list[dict[str, Any]], spack["required_resources"])
        if resource["kind"] == "relay_job"
    }
    assert calls == {
        "spack_find": {"query": "lammps"},
        "spack_install": {"spec": "lammps"},
        "spack_locate": {"spec": "lammps"},
    }


def test_candidate_gate_publishes_only_to_pypi_and_keeps_github_draft() -> None:
    workflow = _workflow("release-gate.yml")
    jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])

    assert jobs["publish-pypi"]["needs"] == "gate"
    assert "publish-github-release" not in jobs
    gate_text = str(jobs["gate"])
    assert "uv run --no-sync clio-relay release gate" in gate_text
    assert '--from "$wheel"' not in gate_text
    assert '--expected-artifact-sha256 "$WHEEL_SHA256"' in gate_text
    assert "gh attestation verify" in gate_text
    assert '--source-digest "$SOURCE_COMMIT"' in gate_text
    assert "LIVE-VALIDATION-BINDING.json" in gate_text
    assert ".github/workflows/live-validation-attest.yml" in gate_text
    text = (WORKFLOWS / "release-gate.yml").read_text(encoding="utf-8")
    promotion_source = (ROOT / "src" / "clio_relay" / "promotion_record.py").read_text(
        encoding="utf-8"
    )
    assert "candidate-policy.yaml" in text
    assert "PYPI-PROMOTION.json" in text
    assert '"artifact_stage": "published"' in promotion_source
    assert 'test "$(gh release view "$TAG_NAME"' in text
    assert "DISPATCH_REF: ${{ github.ref }}" in text
    assert "DISPATCH_SHA: ${{ github.sha }}" in text
    assert 'test "$DISPATCH_REF" = "refs/heads/main"' in text
    assert 'test "$source_commit" = "$DISPATCH_SHA"' in text
    assert "--draft=false" not in text


def test_release_stages_require_the_exact_reviewed_origin_main_commit() -> None:
    release = (WORKFLOWS / "release.yml").read_text(encoding="utf-8")
    gate = (WORKFLOWS / "release-gate.yml").read_text(encoding="utf-8")
    finalization = (WORKFLOWS / "finalize-release.yml").read_text(encoding="utf-8")
    staging = (WORKFLOWS / "stage-candidate.yml").read_text(encoding="utf-8")

    fetch = "git fetch --force --no-tags origin refs/heads/main:refs/remotes/origin/main"
    assert fetch in release
    assert fetch in gate
    assert fetch in finalization
    assert 'test "$GITHUB_SHA" = "$(git rev-parse refs/remotes/origin/main)"' in release
    assert 'test "$source_commit" = "$REVIEWED_MAIN_SHA"' in gate
    assert 'test "$source_commit" = "$(git rev-parse refs/remotes/origin/main)"' in gate
    assert 'test "$DISPATCH_SHA" = "$REVIEWED_MAIN_SHA"' in finalization
    assert 'test "$DISPATCH_SHA" = "$(git rev-parse refs/remotes/origin/main)"' in finalization
    assert 'test "$source_commit" = "$WORKFLOW_SHA"' in staging
    assert 'test "$source_commit" = "$REVIEWED_MAIN_SHA"' in staging

    for workflow_name in ("stage-candidate.yml", "release-gate.yml", "finalize-release.yml"):
        workflow = _workflow(workflow_name)
        dispatch = cast(dict[str, Any], cast(dict[str, Any], workflow["on"])["workflow_dispatch"])
        inputs = cast(dict[str, dict[str, str]], dispatch["inputs"])
        assert inputs["reviewed_main_sha"]["required"] == "true"


def test_release_stages_reject_assets_outside_the_exact_allowlist() -> None:
    staging = (WORKFLOWS / "stage-candidate.yml").read_text(encoding="utf-8")
    gate = (WORKFLOWS / "release-gate.yml").read_text(encoding="utf-8")
    finalization = (WORKFLOWS / "finalize-release.yml").read_text(encoding="utf-8")

    assert "--json assets" in gate
    assert "--jq '.assets[].name'" in gate
    assert "diff --unified expected-assets.txt observed-assets.txt" in gate
    assert "/assets?per_page=100" in finalization
    assert "ci_validation.py staged-assets" in staging
    assert "candidate directory file set does not match" not in staging
    assert "draft release contains assets outside the verified prepublication set" in gate
    assert "candidate-release-gate-1.0.json PYPI-PROMOTION.json" in gate
    assert "ci_validation.py exact-release-assets" in finalization
    assert "EXACT-FINAL-ASSETS.json" in finalization
    assert "RELEASE-CLAIMS.json" in finalization


def test_live_reports_are_sealed_by_authorized_candidate_tag_workflow() -> None:
    workflow = _workflow("live-validation-attest.yml")
    jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
    seal = jobs["seal"]
    environment = cast(dict[str, str], seal["environment"])
    permissions = cast(dict[str, str], seal["permissions"])
    text = (WORKFLOWS / "live-validation-attest.yml").read_text(encoding="utf-8")

    assert environment["name"] == "live-validation"
    assert permissions["id-token"] == "write"
    assert permissions["attestations"] == "write"
    assert 'test "$DISPATCH_REF" = "refs/heads/main"' in text
    assert 'software.get("commit") == commit' in text
    assert 'source.get("artifact_sha256") == wheel_sha256' in text
    assert 'source.get("artifact_identity_verified") is True' in text
    assert 'source.get("launcher_verified") is True' in text
    assert 'source.get("launcher_receipt", {}).get("verified") is True' in text
    assert 'trust.get("origin") == "operator_generated"' in text
    assert 'trust.get("producer_execution_verified") is False' in text
    assert '"trust_model": "reviewer_sealed_operator_evidence"' in text
    assert "sealing dispatcher is not a write-capable maintainer" in text
    assert "ci_validation.py reviewer-exclusions" in text
    assert 'test "$DISPATCH_ACTOR" != "$uploader"' in text
    assert '"review_method": "independent_maintainer_workflow_dispatch"' in text
    assert '"reviewer_login": reviewer_login' in text
    assert '"source_commit_author_login": source_author_login' in text
    assert 'source.get("released_artifact") is False' in text
    assert 'source.get("detected_kind") == "wheel"' in text
    assert "LIVE-VALIDATION-BINDING.json" in text
    assert "actions/attest@" in text
    assert "uv run --no-sync clio-relay release gate" in text
    assert '--from "$wheel"' not in text


def test_released_reports_require_actual_pypi_uvx_source_before_final_gate() -> None:
    workflow = _workflow("released-validation-attest.yml")
    jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
    seal = jobs["seal"]
    environment = cast(dict[str, str], seal["environment"])
    text = (WORKFLOWS / "released-validation-attest.yml").read_text(encoding="utf-8")

    assert environment["name"] == "released-validation"
    assert 'source.get("kind") == "pypi"' in text
    assert 'source.get("detected_kind") == "pypi"' in text
    assert 'source.get("released_artifact") is True' in text
    assert 'source.get("launcher_verified") is True' in text
    assert 'source.get("launcher_receipt", {}).get("verified") is True' in text
    assert 'trust.get("origin") == "operator_generated"' in text
    assert 'trust.get("producer_execution_verified") is False' in text
    assert '"trust_model": "reviewer_sealed_operator_evidence"' in text
    assert "sealing dispatcher is not a write-capable maintainer" in text
    assert "ci_validation.py reviewer-exclusions" in text
    assert 'test "$DISPATCH_ACTOR" != "$uploader"' in text
    assert '"review_method": "independent_maintainer_workflow_dispatch"' in text
    assert '"reviewer_login": reviewer_login' in text
    assert '"source_commit_author_login": source_author_login' in text
    assert "uv run --no-sync clio-relay release gate" in text
    assert 'uvx --from "clio-relay==$VERSION"' not in text
    assert "--policy docs/release-gate-1.0.yaml" in text
    assert "PYPI-PROMOTION.json" in text
    assert "RELEASED-VALIDATION-BINDING.json" in text
    assert "released-release-gate-1.0.json" in text


def test_released_validation_commands_preserve_uv_cache_for_launcher_receipts() -> None:
    release_document = (ROOT / "docs" / "release.md").read_text(encoding="utf-8")

    assert release_document.count("uvx --refresh --no-config `") == 3
    assert "uvx --refresh --no-cache" not in release_document
    assert release_document.count("--default-index https://pypi.org/simple `") == 3
    assert release_document.count('--from "clio-relay==$Version" `') == 3


def test_github_release_finalization_depends_on_sealed_released_evidence() -> None:
    workflow = _workflow("finalize-release.yml")
    jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
    finalize = jobs["finalize"]
    environment = cast(dict[str, str], finalize["environment"])
    text = (WORKFLOWS / "finalize-release.yml").read_text(encoding="utf-8")

    assert environment["name"] == "release-finalization"
    assert "PYPI-PROMOTION.json" in text
    assert "RELEASED-VALIDATION-BINDING.json" in text
    assert "released-release-gate-1.0.json" in text
    assert ".github/workflows/released-validation-attest.yml" in text
    assert "RELEASE-CLAIMS.json" in text
    assert "released_artifact_requirements" in text
    assert "local_quality_requirements" in text
    assert "released_reports" in text
    assert '"review_method": binding.get("review_method")' in text
    assert 'candidate_binding["reviewer_login"]' in text
    assert 'released_binding["reviewer_login"]' in text
    assert '"independent_reviewer": binding.get("reviewer_login")' in text
    assert '!= binding.get("source_commit_author_login")' in text
    assert "--draft=false" in text


def test_only_finalization_can_publish_the_github_release() -> None:
    for workflow_name in (
        "release.yml",
        "stage-candidate.yml",
        "live-validation-attest.yml",
        "release-gate.yml",
        "released-validation-attest.yml",
    ):
        text = (WORKFLOWS / workflow_name).read_text(encoding="utf-8")
        assert "--draft=false" not in text

    finalization = (WORKFLOWS / "finalize-release.yml").read_text(encoding="utf-8")
    assert finalization.count("--draft=false") == 1


def test_pypi_promotion_recovers_only_from_exact_existing_bytes() -> None:
    workflow = _workflow("release-gate.yml")
    jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
    steps = cast(list[dict[str, Any]], jobs["publish-pypi"]["steps"])
    state = next(step for step in steps if step.get("id") == "pypi_state")
    publish = next(step for step in steps if "gh-action-pypi-publish" in str(step.get("uses")))
    script = str(state["run"])

    assert "set(observed) - set(expected)" in script
    assert "set(observed) & set(expected)" in script
    assert "set(expected) - set(observed)" in script
    assert "if unexpected or mismatched" in script
    assert "'true' if missing else 'false'" in script
    assert publish["if"] == "steps.pypi_state.outputs.publish_required == 'true'"
    assert cast(dict[str, str], publish["with"])["skip-existing"] == "true"


def test_existing_release_recovery_assets_are_verified_before_pypi() -> None:
    text = (WORKFLOWS / "release-gate.yml").read_text(encoding="utf-8")
    promotion_source = (ROOT / "src" / "clio_relay" / "promotion_record.py").read_text(
        encoding="utf-8"
    )

    assert "verify any existing recovery assets before publication" in text
    assert "reverify recovery assets immediately before PyPI" in text
    assert text.count("cmp --silent candidate/evidence/candidate-release-gate-1.0.json") == 1
    assert text.count("cmp --silent promotion/evidence/candidate-release-gate-1.0.json") == 1
    assert text.count('--verify-existing "$recovery/PYPI-PROMOTION.json"') == 2
    assert text.count('--signer-workflow "$REPOSITORY/.github/workflows/release-gate.yml"') >= 4
    assert text.count("python src/clio_relay/promotion_record.py") == 3
    assert "args.verify_existing.read_bytes() != encoded" in promotion_source
    assert "if observed != expected:" in promotion_source


def test_candidate_staging_binds_live_exact_sha_ci_and_governance() -> None:
    workflow = _workflow("stage-candidate.yml")
    jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
    stage = jobs["stage"]
    permissions = cast(dict[str, str], stage["permissions"])
    text = (WORKFLOWS / "stage-candidate.yml").read_text(encoding="utf-8")

    assert permissions["actions"] == "read"
    assert "actions/workflows/ci.yml/runs?head_sha=$SOURCE_COMMIT" in text
    assert "event=push&status=completed&per_page=100" in text
    assert "actions/runs/$ci_run_id/attempts/$ci_run_attempt/jobs?per_page=100" in text
    assert "ci_validation.py select-ci-run" in text
    assert "ci_validation.py build-ci-status" in text
    assert "repos/$REPOSITORY/rules/branches/main?per_page=100" in text
    assert "repos/$REPOSITORY/branches/main/protection" not in text
    assert "branches?protected=true&per_page=100" in text
    assert '--protected-branches "$inputs/protected-branches.json"' in text
    assert "repos/$REPOSITORY/rulesets/$ruleset_id" in text
    for environment in (
        "live-validation",
        "pypi",
        "release-finalization",
        "released-validation",
    ):
        assert environment in text
    assert "deployment-branch-policies" not in text
    assert "ci_validation.py build-governance" in text
    assert "ci_validation.py candidate-manifest" in text


def test_ci_and_governance_receipts_survive_every_release_stage() -> None:
    workflow_names = (
        "stage-candidate.yml",
        "live-validation-attest.yml",
        "release-gate.yml",
        "released-validation-attest.yml",
        "finalize-release.yml",
    )
    for workflow_name in workflow_names:
        text = (WORKFLOWS / workflow_name).read_text(encoding="utf-8")
        assert "CI-STATUS.json" in text, workflow_name
        assert "REPOSITORY-GOVERNANCE.json" in text, workflow_name

    for workflow_name in (
        "live-validation-attest.yml",
        "release-gate.yml",
        "released-validation-attest.yml",
        "finalize-release.yml",
    ):
        text = (WORKFLOWS / workflow_name).read_text(encoding="utf-8")
        assert "ci_validation.py verify-ci-status" in text, workflow_name
        assert "ci_validation.py verify-live-governance" in text, workflow_name

    staging = (WORKFLOWS / "stage-candidate.yml").read_text(encoding="utf-8")
    assert "ci_validation.py build-ci-status" in staging
    assert "ci_validation.py build-governance" in staging

    finalization = (WORKFLOWS / "finalize-release.yml").read_text(encoding="utf-8")
    assert '"ci_status": {' in finalization
    assert '"repository_governance": {' in finalization
    assert '"environment_reviewers_available"' in finalization


def test_live_seals_bind_numeric_producer_uploader_reviewer_and_invocation() -> None:
    for workflow_name in (
        "live-validation-attest.yml",
        "released-validation-attest.yml",
    ):
        text = (WORKFLOWS / workflow_name).read_text(encoding="utf-8")
        assert 'dispatcher_id="$(gh api "users/$DISPATCH_ACTOR" --jq .id)"' in text
        assert "source_author_id" in text
        assert "source_committer_id" in text
        assert "source_pr_authors" in text
        assert "source_contributor_ids" in text
        assert "ci_validation.py reviewer-exclusions" in text
        assert '--dispatcher-login "$DISPATCH_ACTOR"' in text
        assert '--dispatcher-id "$dispatcher_id"' in text
        assert "report_uploader_ids" in text
        assert 'test "$dispatcher_id" != "$uploader_id"' in text
        assert "source_commit_committer_id" in text
        assert 'trust.get("producer_github_login")' in text
        assert 'trust.get("producer_github_id")' in text
        assert 'trust.get("invocation_id")' in text
        assert 'launcher_receipt.get("invocation_id")' in text
        assert 'launcher_receipt.get("uv_version")' in text
        assert 'launcher_receipt.get("uv_executable_sha256")' in text
        assert '"producer_github_id": producer_id' in text
        assert '"uploader_github_id": uploader.get("id")' in text
        assert '"reviewer_id": reviewer_id' in text
        assert '"uv_version": launcher_receipt.get("uv_version")' in text
        assert '"uv_executable_sha256": launcher_receipt.get(' in text
        assert '"environment_reviewers_available": False' in text


def test_privileged_dispatches_run_only_from_protected_main() -> None:
    dependencies = {
        "stage-candidate.yml": {"stage": "authorize"},
        "live-validation-attest.yml": {"seal": "authorize"},
        "released-validation-attest.yml": {"seal": "authorize"},
        "release-gate.yml": {"gate": "authorize", "publish-pypi": "gate"},
        "finalize-release.yml": {"finalize": "authorize"},
    }
    checkout_refs = {
        "stage": "${{ needs.authorize.outputs.workflow_sha }}",
        "seal": "${{ needs.authorize.outputs.workflow_sha }}",
        "gate": "${{ needs.authorize.outputs.workflow_sha }}",
        "publish-pypi": "${{ needs.gate.outputs.workflow_sha }}",
        "finalize": "${{ needs.authorize.outputs.workflow_sha }}",
    }
    for workflow_name, sensitive_jobs in dependencies.items():
        workflow = _workflow(workflow_name)
        jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
        authorize = jobs["authorize"]
        assert authorize["permissions"] == {}
        assert 'test "$DISPATCH_REF" = "refs/heads/main"' in str(authorize["steps"])
        for job_name, dependency in sensitive_jobs.items():
            job = jobs[job_name]
            assert job["needs"] == dependency
            checkout = next(
                step
                for step in cast(list[dict[str, Any]], job["steps"])
                if "actions/checkout@" in str(step.get("uses"))
            )
            assert cast(dict[str, str], checkout["with"])["ref"] == checkout_refs[job_name]
            assert cast(dict[str, str], checkout["with"])["ref"] != "${{ inputs.tag }}"
    candidate = (WORKFLOWS / "release.yml").read_text(encoding="utf-8")
    assert "environment:" not in candidate
    assert "gh-action-pypi-publish" not in candidate


def test_release_workflows_preflight_bounded_report_assets() -> None:
    expectations = {
        "live-validation-attest.yml": ("--kind candidate",),
        "released-validation-attest.yml": ("--kind released",),
        "release-gate.yml": ("--kind candidate",),
        "finalize-release.yml": ("--kind candidate", "--kind released"),
    }
    for workflow_name, kinds in expectations.items():
        text = (WORKFLOWS / workflow_name).read_text(encoding="utf-8")
        assert "ci_validation.py report-assets" in text, workflow_name
        assert "report-assets-before.json" in text, workflow_name
        assert "report-assets-after.json" in text, workflow_name
        assert "cmp --silent" in text, workflow_name
        assert "/assets?per_page=100" in text, workflow_name
        assert text.index("preflight bounded") < text.index("gh release download"), workflow_name
        assert text.index("report-assets-after.json") < text.index("json.loads"), workflow_name
        for kind in kinds:
            assert kind in text, workflow_name


def test_actions_artifacts_are_preflighted_and_downloaded_by_exact_api_id() -> None:
    stage = (WORKFLOWS / "stage-candidate.yml").read_text(encoding="utf-8")
    publish = (WORKFLOWS / "release-gate.yml").read_text(encoding="utf-8")

    assert "--artifact-kind tag-payload" in stage
    assert "actions/runs/$RUN_ID/attempts/$RUN_ATTEMPT" in stage
    assert "actions/artifacts/$ARTIFACT_ID/zip" in stage
    assert "--artifact-kind promotion" in publish
    assert "actions/runs/$RUN_ID/attempts/$RUN_ATTEMPT" in publish
    assert "actions/artifacts/$ARTIFACT_ID/zip" in publish
    assert "actions/download-artifact@" not in stage
    assert "actions/download-artifact@" not in publish


def test_every_protected_environment_reader_has_actions_metadata_permission() -> None:
    for workflow_name in (
        "stage-candidate.yml",
        "live-validation-attest.yml",
        "released-validation-attest.yml",
        "release-gate.yml",
        "finalize-release.yml",
    ):
        workflow = _workflow(workflow_name)
        jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
        for job_name, job in jobs.items():
            if "environment" not in job:
                continue
            permissions = cast(dict[str, str], job["permissions"])
            assert permissions.get("actions") == "read", (workflow_name, job_name)


def test_final_binding_publish_steps_receive_the_exact_source_commit() -> None:
    for workflow_name in (
        "live-validation-attest.yml",
        "released-validation-attest.yml",
    ):
        workflow = _workflow(workflow_name)
        jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
        steps = cast(list[dict[str, Any]], jobs["seal"]["steps"])
        publish = next(step for step in steps if "gh release upload" in str(step.get("run", "")))
        assert "SOURCE_COMMIT" in cast(dict[str, str], publish["env"]), workflow_name


def test_every_release_mutation_rechecks_live_identity_immediately_before_write() -> None:
    for workflow_name in (
        "stage-candidate.yml",
        "live-validation-attest.yml",
        "released-validation-attest.yml",
        "release-gate.yml",
        "finalize-release.yml",
    ):
        workflow = _workflow(workflow_name)
        jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
        for job in jobs.values():
            steps = cast(list[dict[str, Any]], job["steps"])
            for index, step in enumerate(steps):
                script = str(step.get("run", ""))
                for line_index, line in enumerate(script.splitlines()):
                    mutations = (
                        "gh release create",
                        "gh release upload",
                        "gh release edit",
                    )
                    if any(mutation in line for mutation in mutations):
                        preceding = script.splitlines()[max(0, line_index - 120) : line_index]
                        window = "\n".join(preceding)
                        assert "mutation-authority" in window or '"${authority[@]}"' in window, (
                            workflow_name,
                            step["name"],
                            line,
                        )
                action = str(step.get("uses", ""))
                if action.startswith(("actions/attest@", "actions/upload-artifact@")):
                    assert index > 0
                    predecessor = str(steps[index - 1].get("run", ""))
                    assert "mutation-authority" in predecessor, (workflow_name, step["name"])

    gate = _workflow("release-gate.yml")
    publish_steps = cast(
        list[dict[str, Any]],
        cast(dict[str, dict[str, Any]], gate["jobs"])["publish-pypi"]["steps"],
    )
    publish_index = next(
        index
        for index, step in enumerate(publish_steps)
        if "gh-action-pypi-publish" in str(step.get("uses"))
    )
    assert "mutation-authority" in str(publish_steps[publish_index - 1].get("run"))


def test_final_publication_rechecks_exact_assets_before_and_after_immutability() -> None:
    workflow = _workflow("finalize-release.yml")
    jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
    steps = cast(list[dict[str, Any]], jobs["finalize"]["steps"])
    publication = next(step for step in steps if "gh release edit" in str(step.get("run", "")))
    script = str(publication["run"])

    authority = script.index("ci_validation.py mutation-authority")
    preflight = script.index("ci_validation.py exact-release-assets", authority)
    receipt = script.index('--output "$RUNNER_TEMP/EXACT-FINAL-ASSETS.json"', preflight)
    publish = script.index("gh release edit", receipt)
    postflight = script.index("ci_validation.py exact-release-assets", publish)
    immutable_receipt = script.index(
        '--verify-existing "$RUNNER_TEMP/EXACT-FINAL-ASSETS.json"',
        postflight,
    )
    immutable_check = script.index("--json isImmutable", immutable_receipt)

    assert authority < preflight < receipt < publish
    assert publish < postflight < immutable_receipt < immutable_check
    assert '"${asset_args[@]}"' in script
    assert script.count("/assets?per_page=100&page=1") == 1
    assert script.count("/assets?per_page=100&page=2") == 1
    assert script.count("--next-assets-page") == 2
    assert script.count("--page-size 100") == 2


def test_final_claims_attestation_is_verified_unconditionally_on_retry() -> None:
    workflow = _workflow("finalize-release.yml")
    jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
    steps = cast(list[dict[str, Any]], jobs["finalize"]["steps"])
    verification = next(
        step
        for step in steps
        if step.get("name") == "verify final claims attestation on first run or retry"
    )
    script = str(verification["run"])

    assert "if" not in verification
    assert "gh attestation verify finalization/evidence/RELEASE-CLAIMS.json" in script
    assert '--signer-workflow "$REPOSITORY/.github/workflows/finalize-release.yml"' in script
    assert '--source-ref "refs/heads/main"' in script
    assert '--source-digest "$SOURCE_COMMIT"' in script
    assert "--deny-self-hosted-runners" in script


def test_distribution_archives_are_parsed_but_never_executed_in_privileged_jobs() -> None:
    gate = (WORKFLOWS / "release-gate.yml").read_text(encoding="utf-8")
    release = (WORKFLOWS / "release.yml").read_text(encoding="utf-8")

    assert gate.count("ci_validation.py distribution-archives") == 2
    assert "DISTRIBUTION-ARCHIVES.json" in gate
    assert "local.sdist-smoke" not in gate
    assert "release validate-local" in release


def test_privileged_release_jobs_never_execute_candidate_or_pypi_code() -> None:
    for workflow_name in (
        "stage-candidate.yml",
        "live-validation-attest.yml",
        "released-validation-attest.yml",
        "release-gate.yml",
        "finalize-release.yml",
    ):
        workflow = _workflow(workflow_name)
        jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
        for job_name, job in jobs.items():
            permissions = cast(dict[str, str], job.get("permissions", {}))
            if not any(value == "write" for value in permissions.values()):
                continue
            scripts = [
                str(step.get("run", "")) for step in cast(list[dict[str, Any]], job["steps"])
            ]
            command_lines = [line.strip() for script in scripts for line in script.splitlines()]
            assert not any(line.startswith("uvx ") for line in command_lines), (
                workflow_name,
                job_name,
            )
            assert not any('--from "$wheel"' in line for line in command_lines), (
                workflow_name,
                job_name,
            )
            for script in scripts:
                if "clio-relay release gate" in script:
                    assert "uv run --no-sync clio-relay release gate" in script


def test_release_workflows_do_not_claim_unavailable_immutable_setting_access() -> None:
    for workflow_name in (
        "stage-candidate.yml",
        "live-validation-attest.yml",
        "released-validation-attest.yml",
        "release-gate.yml",
        "finalize-release.yml",
    ):
        text = (WORKFLOWS / workflow_name).read_text(encoding="utf-8")
        assert "/immutable-releases" not in text


def test_final_claims_resolve_all_decision_reports_and_bind_target_policy() -> None:
    text = (WORKFLOWS / "finalize-release.yml").read_text(encoding="utf-8")

    assert "unknown_report_ids" in text
    assert "final decision contains unbound report ids" in text
    assert "every non-local final decision report must resolve" in text
    assert '"target_identity_sha256": decision["target_identity_sha256"]' in text
    assert '"policy_target_identity_sha256": decision[' in text
    assert 'decision.get("policy_target_identity_sha256")' in text
    assert '== decision.get("target_identity_sha256")' in text
    assert '"producer_github_id": document.get("evidence_trust", {}).get(' in text
    assert '"invocation_id": document.get("evidence_trust", {}).get(' in text


def test_release_workflow_actions_are_commit_pinned() -> None:
    for workflow_name in (
        "ci.yml",
        "release.yml",
        "stage-candidate.yml",
        "release-gate.yml",
        "live-validation-attest.yml",
        "released-validation-attest.yml",
        "finalize-release.yml",
    ):
        workflow = _workflow(workflow_name)
        jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
        for job in jobs.values():
            for step in cast(list[dict[str, Any]], job["steps"]):
                action = step.get("uses")
                if action is not None:
                    assert re.fullmatch(r"[^@]+@[0-9a-f]{40}", str(action)), action


def test_every_setup_uv_action_installs_the_tested_uv_version() -> None:
    setup_steps: list[tuple[str, dict[str, Any]]] = []
    for workflow_path in sorted(WORKFLOWS.glob("*.yml")):
        workflow = _workflow(workflow_path.name)
        jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
        for job in jobs.values():
            for step in cast(list[dict[str, Any]], job["steps"]):
                if str(step.get("uses", "")).startswith("astral-sh/setup-uv@"):
                    setup_steps.append((workflow_path.name, step))

    assert setup_steps
    for workflow_name, step in setup_steps:
        options = cast(dict[str, str], step.get("with", {}))
        assert options.get("version") == "0.11.28", workflow_name


def test_embedded_workflow_python_compiles() -> None:
    marker = re.compile(r"<<'PY'\n(?P<body>.*?)(?=\n[ ]+PY(?:\n|$))", re.DOTALL)
    workflow_names = (
        "release.yml",
        "stage-candidate.yml",
        "release-gate.yml",
        "live-validation-attest.yml",
        "released-validation-attest.yml",
        "finalize-release.yml",
    )

    for workflow_name in workflow_names:
        text = (WORKFLOWS / workflow_name).read_text(encoding="utf-8")
        for index, match in enumerate(marker.finditer(text), start=1):
            source = textwrap.dedent(match.group("body"))
            compile(source, f"{workflow_name}:embedded-python-{index}", "exec")
