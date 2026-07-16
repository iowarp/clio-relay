"""Tests for exact-SHA CI and live repository-governance receipts."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import stat
import tarfile
import zipfile
from copy import deepcopy
from pathlib import Path
from typing import cast

import pytest

from clio_relay import ci_validation
from clio_relay.ci_validation import (
    GITHUB_ACTIONS_APP_ID,
    MAIN_REVIEW_POLICY,
    MAX_ACTIONS_ARTIFACT_ARCHIVE_BYTES,
    MAX_DISTRIBUTION_BYTES,
    MAX_DISTRIBUTION_MEMBERS,
    MAX_FIXED_JSON_BYTES,
    MAX_JSON_DOCUMENT_BYTES,
    MAX_MANIFEST_BYTES,
    MAX_RELEASE_ASSET_AGGREGATE_BYTES,
    MAX_RELEASE_ASSET_BYTES,
    MAX_RELEASE_ASSET_METADATA_RECORDS,
    MAX_VALIDATION_REPORT_AGGREGATE_BYTES,
    MAX_VALIDATION_REPORT_ASSETS,
    MAX_VALIDATION_REPORT_BYTES,
    REQUIRE_LAST_PUSH_APPROVAL,
    REQUIRED_APPROVING_REVIEW_COUNT,
    REQUIRED_CI_JOBS,
    REQUIRED_ENVIRONMENTS,
    REQUIRED_MATRIX_JOBS,
    REQUIRED_MERGE_QUEUE_PARAMETERS,
    ProvenanceError,
    build_actions_artifact_manifest,
    build_candidate_build_receipt,
    build_ci_status,
    build_distribution_archive_receipt,
    build_exact_release_asset_inventory,
    build_repository_governance,
    build_staged_release_asset_plan,
    build_tag_binding,
    build_validation_report_asset_manifest,
    fetch_live_repository_governance,
    resolve_live_release,
    select_ci_run,
    validate_release_acceptance_matrix,
    verify_actions_artifact_archive,
    verify_ci_status,
    verify_downloaded_validation_report_assets,
    verify_exact_release_asset_inventory,
    verify_live_mutation_authority,
    verify_live_release_identity,
    verify_live_repository_governance,
    verify_release_identity,
    verify_repository_governance,
    write_candidate_checksum_manifest,
)

REPOSITORY = "iowarp/clio-relay"
COMMIT = "a" * 40
TESTED_COMMIT = "b" * 40
TREE = "c" * 40
TAG = "v1.0.0"


def _runs() -> dict[str, object]:
    return {
        "total_count": 1,
        "workflow_runs": [
            {
                "id": 1001,
                "run_attempt": 2,
                "run_number": 75,
                "workflow_id": 99,
                "html_url": "https://github.com/iowarp/clio-relay/actions/runs/1001",
                "head_sha": TESTED_COMMIT,
                "head_branch": "gh-readonly-queue/main/pr-23-deadbeef",
                "event": "merge_group",
                "status": "completed",
                "conclusion": "success",
                "path": ".github/workflows/ci.yml",
            }
        ],
    }


def _jobs() -> dict[str, object]:
    return {
        "total_count": len(REQUIRED_CI_JOBS),
        "jobs": [
            {
                "id": 2000 + index,
                "name": name,
                "status": "completed",
                "conclusion": "success",
                "html_url": f"https://github.com/iowarp/clio-relay/actions/jobs/{2000 + index}",
            }
            for index, name in enumerate(REQUIRED_CI_JOBS, start=1)
        ],
    }


def _candidate_build() -> dict[str, object]:
    distributions = {
        "clio_relay-1.0.0-py3-none-any.whl": "d" * 64,
        "clio_relay-1.0.0.tar.gz": "e" * 64,
    }
    return {
        "schema_version": "clio-relay.candidate-build.v1",
        "repository": REPOSITORY,
        "workflow": ".github/workflows/ci.yml",
        "event": "merge_group",
        "tested_commit": TESTED_COMMIT,
        "source_tree": TREE,
        "base_ref": "refs/heads/main",
        "head_ref": "refs/heads/gh-readonly-queue/main/pr-23-deadbeef",
        "pull_request_number": 23,
        "run_id": 1001,
        "run_attempt": 2,
        "distribution_sha256": distributions,
        "matrix_reports": [
            {
                "job": job,
                "filename": (
                    f"validation-local-{job.split(' / python ')[0]}-{job.rsplit(' ', 1)[1]}.json"
                ),
                "sha256": f"{index:x}" * 64,
                "mode": "build" if index == 1 else "prebuilt",
                "artifact_sha256": distributions,
            }
            for index, job in enumerate(REQUIRED_MATRIX_JOBS, start=1)
        ],
    }


def _candidate_artifact() -> dict[str, object]:
    return {
        "schema_version": "1.1",
        "repository": REPOSITORY,
        "source_commit": TESTED_COMMIT,
        "source_tree": TREE,
        "tag": TAG,
        "artifact_kind": "candidate",
        "head_branch": "gh-readonly-queue/main/pr-23-deadbeef",
        "run_id": 1001,
        "run_attempt": 2,
        "artifact": {
            "id": 6001,
            "name": f"release-candidate-{TREE}",
            "size_in_bytes": 100,
            "digest": f"sha256:{'f' * 64}",
            "archive_download_url": (
                f"https://api.github.com/repos/{REPOSITORY}/actions/artifacts/6001/zip"
            ),
            "expired": False,
            "created_at": "2026-07-11T10:05:00+00:00",
        },
    }


def _canonical_digest(document: object) -> str:
    return hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _tag_binding() -> dict[str, object]:
    artifact = _candidate_artifact()
    return {
        "schema_version": "clio-relay.tag-candidate-binding.v1",
        "repository": REPOSITORY,
        "tag": TAG,
        "source_commit": COMMIT,
        "source_tree": TREE,
        "merge_group_anchor_pull_request_number": 23,
        "pull_request": {
            "number": 23,
            "merge_commit_sha": COMMIT,
            "url": "https://github.com/iowarp/clio-relay/pull/23",
        },
        "tested_commit": TESTED_COMMIT,
        "candidate_build_sha256": _canonical_digest(_candidate_build()),
        "candidate_run": {
            "id": 1001,
            "attempt": 2,
            "head_sha": TESTED_COMMIT,
            "head_branch": "gh-readonly-queue/main/pr-23-deadbeef",
        },
        "candidate_artifact": artifact["artifact"],
    }


MAIN_RULESET_ID = 18816105
TAG_RULESET_ID = 18813108


def _main_effective_rules() -> list[dict[str, object]]:
    return [
        {"type": "deletion", "ruleset_id": MAIN_RULESET_ID},
        {"type": "non_fast_forward", "ruleset_id": MAIN_RULESET_ID},
        {
            "type": "merge_queue",
            "ruleset_id": MAIN_RULESET_ID,
            "parameters": deepcopy(REQUIRED_MERGE_QUEUE_PARAMETERS),
        },
        {
            "type": "pull_request",
            "ruleset_id": MAIN_RULESET_ID,
            "parameters": {
                "dismiss_stale_reviews_on_push": True,
                "require_last_push_approval": REQUIRE_LAST_PUSH_APPROVAL,
                "required_approving_review_count": REQUIRED_APPROVING_REVIEW_COUNT,
                "required_review_thread_resolution": True,
            },
        },
        {
            "type": "required_status_checks",
            "ruleset_id": MAIN_RULESET_ID,
            "parameters": {
                "strict_required_status_checks_policy": True,
                "required_status_checks": [
                    {"context": name, "integration_id": GITHUB_ACTIONS_APP_ID}
                    for name in REQUIRED_CI_JOBS
                ],
            },
        },
    ]


def _branch_rulesets() -> list[dict[str, object]]:
    return [
        {
            "id": MAIN_RULESET_ID,
            "name": "protect-main-release-source",
            "target": "branch",
            "enforcement": "active",
            "conditions": {
                "ref_name": {"include": ["refs/heads/main"], "exclude": []},
            },
            # A visible global actor is recorded, not misrepresented as absent. The
            # release decision rests on the current workflow token being unable to bypass.
            "bypass_actors": [{"actor_id": 1, "actor_type": "OrganizationAdmin"}],
            "current_user_can_bypass": "never",
        }
    ]


def _tag_rulesets() -> list[dict[str, object]]:
    return [
        {
            "id": TAG_RULESET_ID,
            "name": "protect-release-tags",
            "target": "tag",
            "enforcement": "active",
            "conditions": {
                "ref_name": {"include": ["refs/tags/v*"], "exclude": []},
            },
            "rules": [{"type": "update"}, {"type": "deletion"}],
            "bypass_actors": [{"actor_id": 1, "actor_type": "OrganizationAdmin"}],
            "current_user_can_bypass": "never",
        }
    ]


def _protected_branches() -> list[dict[str, object]]:
    return [{"name": "main", "protected": True}]


def _environments() -> dict[str, object]:
    return {
        name: {
            "name": name,
            "can_admins_bypass": False,
            "protection_rules": [{"id": index, "type": "branch_policy"}],
            "deployment_branch_policy": {
                "protected_branches": True,
                "custom_branch_policies": False,
            },
        }
        for index, name in enumerate(REQUIRED_ENVIRONMENTS, start=1)
    }


def _immutable_releases() -> dict[str, object]:
    return {"enabled": True, "enforced_by_owner": True}


def _governance_routes() -> dict[str, object]:
    routes: dict[str, object] = {
        f"repos/{REPOSITORY}/rules/branches/main?per_page=100": _main_effective_rules(),
        f"repos/{REPOSITORY}/branches?protected=true&per_page=100": _protected_branches(),
        f"repos/{REPOSITORY}/rulesets?includes_parents=true&per_page=100": [
            {"id": MAIN_RULESET_ID, "target": "branch"},
            {"id": TAG_RULESET_ID, "target": "tag"},
        ],
        f"repos/{REPOSITORY}/rulesets/{MAIN_RULESET_ID}": _branch_rulesets()[0],
        f"repos/{REPOSITORY}/rulesets/{TAG_RULESET_ID}": _tag_rulesets()[0],
        f"repos/{REPOSITORY}/immutable-releases": _immutable_releases(),
    }
    for name, environment in _environments().items():
        routes[f"repos/{REPOSITORY}/environments/{name}"] = environment
    return routes


def _release_assets(names_and_sizes: list[tuple[str, int]]) -> dict[str, object]:
    return {
        "assets": [
            {
                "id": 1000 + index,
                "name": name,
                "size": size,
                "digest": f"sha256:{hashlib.sha256(b'x' * size).hexdigest()}",
                "uploader": {"login": "release-operator", "id": 123456},
            }
            for index, (name, size) in enumerate(names_and_sizes)
        ]
    }


def test_ci_status_requires_exact_successful_push_run_and_job_set() -> None:
    selected = select_ci_run(_runs(), repository=REPOSITORY, source_commit=TESTED_COMMIT)
    receipt = build_ci_status(
        _runs(),
        _jobs(),
        _candidate_build(),
        _candidate_artifact(),
        _tag_binding(),
        repository=REPOSITORY,
        source_commit=COMMIT,
    )

    assert selected == {
        "run_id": 1001,
        "run_attempt": 2,
        "run_number": 75,
        "workflow_id": 99,
        "url": "https://github.com/iowarp/clio-relay/actions/runs/1001",
        "event": "merge_group",
        "head_branch": "gh-readonly-queue/main/pr-23-deadbeef",
    }
    assert receipt["required_jobs"] == list(REQUIRED_CI_JOBS)
    assert [job["name"] for job in cast(list[dict[str, object]], receipt["jobs"])] == list(
        REQUIRED_CI_JOBS
    )
    verify_ci_status(receipt, repository=REPOSITORY, source_commit=COMMIT)


@pytest.mark.parametrize("mutation", ["failed", "missing", "duplicate", "truncated"])
def test_ci_status_rejects_incomplete_or_nonpassing_jobs(mutation: str) -> None:
    jobs = deepcopy(_jobs())
    typed_jobs = cast(list[dict[str, object]], jobs["jobs"])
    if mutation == "failed":
        typed_jobs[-1]["conclusion"] = "failure"
    elif mutation == "missing":
        typed_jobs.pop()
        jobs["total_count"] = len(typed_jobs)
    elif mutation == "duplicate":
        typed_jobs[-1]["name"] = typed_jobs[0]["name"]
    else:
        jobs["total_count"] = len(typed_jobs) + 1

    with pytest.raises(ProvenanceError):
        build_ci_status(
            _runs(),
            jobs,
            _candidate_build(),
            _candidate_artifact(),
            _tag_binding(),
            repository=REPOSITORY,
            source_commit=COMMIT,
        )


def test_ci_status_rejects_a_caller_supplied_non_push_or_wrong_sha_run() -> None:
    runs = _runs()
    run = cast(list[dict[str, object]], runs["workflow_runs"])[0]
    run["event"] = "workflow_dispatch"

    with pytest.raises(ProvenanceError, match="exactly one"):
        select_ci_run(runs, repository=REPOSITORY, source_commit=TESTED_COMMIT)

    run["event"] = "merge_group"
    run["head_sha"] = "d" * 40
    with pytest.raises(ProvenanceError, match="exactly one"):
        select_ci_run(runs, repository=REPOSITORY, source_commit=TESTED_COMMIT)


def test_candidate_build_receipt_proves_one_build_and_five_exact_reuses(
    tmp_path: Path,
) -> None:
    candidate = tmp_path / "candidate"
    reports = tmp_path / "reports"
    candidate.mkdir()
    reports.mkdir()
    wheel = candidate / "clio_relay-1.0.0-py3-none-any.whl"
    sdist = candidate / "clio_relay-1.0.0.tar.gz"
    wheel.write_bytes(b"wheel")
    sdist.write_bytes(b"sdist")
    distribution_digests = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in (wheel, sdist)
    }
    (candidate / "SHA256SUMS").write_text(
        "".join(
            f"{distribution_digests[path.name]} *{path.name}\n"
            for path in sorted((wheel, sdist), key=lambda item: item.name)
        ),
        encoding="ascii",
        newline="\n",
    )
    for index, job in enumerate(REQUIRED_MATRIX_JOBS):
        filename = f"validation-local-{job.split(' / python ')[0]}-{job.rsplit(' ', 1)[1]}.json"
        checks = (
            ["local.build", "local.sdist-smoke"] if index == 0 else ["local.prebuilt-artifacts"]
        )
        document = {
            "status": "passed",
            "scenario": "local-release",
            "cluster": "local",
            "software": {"commit": TESTED_COMMIT, "dirty": False},
            "checks": [{"check_id": check_id} for check_id in checks],
            "resources": [
                {
                    "resource_id": name,
                    "metadata": {"sha256": digest},
                }
                for name, digest in distribution_digests.items()
            ],
        }
        (reports / filename).write_text(json.dumps(document), encoding="utf-8")

    receipt = build_candidate_build_receipt(
        candidate,
        reports,
        repository=REPOSITORY,
        source_commit=TESTED_COMMIT,
        source_tree=TREE,
        event="merge_group",
        run_id=1001,
        run_attempt=2,
        head_ref="refs/heads/gh-readonly-queue/main/pr-23-deadbeef",
        base_ref="refs/heads/main",
    )

    matrix = cast(list[dict[str, object]], receipt["matrix_reports"])
    assert [item["mode"] for item in matrix] == ["build", *(["prebuilt"] * 5)]
    assert all(item["artifact_sha256"] == distribution_digests for item in matrix)

    changed = json.loads((reports / cast(str, matrix[-1]["filename"])).read_text())
    cast(list[dict[str, object]], changed["resources"])[0]["metadata"] = {"sha256": "0" * 64}
    (reports / cast(str, matrix[-1]["filename"])).write_text(json.dumps(changed), encoding="utf-8")
    with pytest.raises(ProvenanceError, match="artifact digests differ"):
        build_candidate_build_receipt(
            candidate,
            reports,
            repository=REPOSITORY,
            source_commit=TESTED_COMMIT,
            source_tree=TREE,
            event="merge_group",
            run_id=1001,
            run_attempt=2,
            head_ref="refs/heads/gh-readonly-queue/main/pr-23-deadbeef",
            base_ref="refs/heads/main",
        )


def test_tag_binding_accepts_a_multi_pr_group_final_commit_and_requires_tested_tree() -> None:
    pulls = [
        {
            "number": 47,
            "state": "closed",
            "merged_at": "2026-07-14T01:00:00Z",
            "merge_commit_sha": COMMIT,
            "html_url": "https://github.com/iowarp/clio-relay/pull/47",
            "base": {"ref": "main"},
        }
    ]
    binding = build_tag_binding(
        _candidate_build(),
        _candidate_artifact(),
        pulls,
        repository=REPOSITORY,
        source_commit=COMMIT,
        source_tree=TREE,
        tag=TAG,
    )

    assert binding["candidate_build_sha256"] == _canonical_digest(_candidate_build())
    assert binding["merge_group_anchor_pull_request_number"] == 23
    assert cast(dict[str, object], binding["pull_request"])["number"] == 47
    with pytest.raises(ProvenanceError, match="tree differs"):
        build_tag_binding(
            _candidate_build(),
            _candidate_artifact(),
            pulls,
            repository=REPOSITORY,
            source_commit=COMMIT,
            source_tree="9" * 40,
            tag=TAG,
        )


def _artifact_run(*, kind: str = "tag-payload") -> dict[str, object]:
    if kind == "candidate":
        return {
            "id": 1001,
            "run_attempt": 2,
            "head_branch": "gh-readonly-queue/main/pr-23-deadbeef",
            "head_sha": TESTED_COMMIT,
            "event": "merge_group",
            "status": "completed",
            "conclusion": "success",
            "run_started_at": "2026-07-11T10:00:00Z",
            "path": ".github/workflows/ci.yml",
            "repository": {"id": 100},
            "head_repository": {"id": 100},
        }
    is_tag = kind == "tag-payload"
    return {
        "id": 5001,
        "run_attempt": 3,
        "head_branch": TAG if is_tag else "main",
        "head_sha": COMMIT,
        "event": "push" if is_tag else "workflow_dispatch",
        "status": "completed" if is_tag else "in_progress",
        "conclusion": "success" if is_tag else None,
        "run_started_at": "2026-07-11T10:00:00Z",
        "path": (
            ".github/workflows/release.yml" if is_tag else ".github/workflows/release-gate.yml"
        ),
        "repository": {"id": 100},
        "head_repository": {"id": 100},
    }


def _artifact_listing(*, size: int, digest: str, kind: str = "tag-payload") -> dict[str, object]:
    if kind == "candidate":
        name = f"release-candidate-{TREE}"
        head_sha = TESTED_COMMIT
        head_branch = "gh-readonly-queue/main/pr-23-deadbeef"
    elif kind == "tag-payload":
        name = f"release-candidate-{TAG}"
        head_sha = COMMIT
        head_branch = TAG
    else:
        name = f"verified-release-{TAG}"
        head_sha = COMMIT
        head_branch = "main"
    return {
        "total_count": 1,
        "artifacts": [
            {
                "id": 6001,
                "name": name,
                "size_in_bytes": size,
                "digest": f"sha256:{digest}",
                "expired": False,
                "created_at": "2026-07-11T10:05:00Z",
                "archive_download_url": (
                    f"https://api.github.com/repos/{REPOSITORY}/actions/artifacts/6001/zip"
                ),
                "workflow_run": {
                    "id": 1001 if kind == "candidate" else 5001,
                    "head_sha": head_sha,
                    "head_branch": head_branch,
                    "repository_id": 100,
                    "head_repository_id": 100,
                },
            }
        ],
    }


def _write_tag_payload_zip(path: Path, *, extra_name: str | None = None) -> None:
    files = {
        "clio_relay-1.0.0-py3-none-any.whl": b"wheel",
        "clio_relay-1.0.0.tar.gz": b"sdist",
        "validation-local.json": b"{}",
    }
    checksum = "".join(
        f"{hashlib.sha256(payload).hexdigest()} *{name}\n"
        for name, payload in sorted(files.items())
    ).encode()
    files["SHA256SUMS"] = checksum
    if extra_name is not None:
        files[extra_name] = b"extra"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in files.items():
            archive.writestr(name, payload)


def _write_candidate_zip(path: Path, *, inconsistent_receipt: bool = False) -> None:
    wheel_name = "clio_relay-1.0.0-py3-none-any.whl"
    sdist_name = "clio_relay-1.0.0.tar.gz"
    wheel = b"one tested wheel"
    sdist = b"one tested sdist"
    distribution_digests = {
        wheel_name: hashlib.sha256(wheel).hexdigest(),
        sdist_name: hashlib.sha256(sdist).hexdigest(),
    }
    report = {
        "status": "passed",
        "scenario": "local-release",
        "cluster": "local",
        "software": {"commit": TESTED_COMMIT, "dirty": False},
        "resources": [
            {
                "resource_id": name,
                "metadata": {"sha256": digest},
            }
            for name, digest in distribution_digests.items()
        ],
        "checks": [
            {"check_id": "local.build"},
            {"check_id": "local.sdist-smoke"},
        ],
    }
    report_bytes = json.dumps(report, sort_keys=True, separators=(",", ":")).encode()
    build = _candidate_build()
    build["distribution_sha256"] = dict(distribution_digests)
    matrix = cast(list[dict[str, object]], build["matrix_reports"])
    for item in matrix:
        item["artifact_sha256"] = dict(distribution_digests)
    matrix[0]["sha256"] = hashlib.sha256(report_bytes).hexdigest()
    if inconsistent_receipt:
        build["distribution_sha256"][wheel_name] = "0" * 64
        for item in matrix:
            cast(dict[str, str], item["artifact_sha256"])[wheel_name] = "0" * 64
    files = {
        wheel_name: wheel,
        sdist_name: sdist,
        "validation-local.json": report_bytes,
        "CANDIDATE-BUILD.json": json.dumps(
            build,
            sort_keys=True,
            separators=(",", ":"),
        ).encode(),
    }
    files["SHA256SUMS"] = "".join(
        f"{hashlib.sha256(payload).hexdigest()} *{name}\n"
        for name, payload in sorted(files.items())
    ).encode()
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in files.items():
            archive.writestr(name, payload)


def _write_promotion_zip(path: Path) -> None:
    files = {
        "packages/clio_relay-1.0.0-py3-none-any.whl": b"wheel",
        "packages/clio_relay-1.0.0.tar.gz": b"sdist",
        "evidence/SHA256SUMS": b"manifest",
        "evidence/validation-local.json": b"{}",
        "evidence/CI-STATUS.json": b"{}",
        "evidence/REPOSITORY-GOVERNANCE.json": b"{}",
        "evidence/DISTRIBUTION-ARCHIVES.json": b"{}",
        "evidence/LIVE-VALIDATION-BINDING.json": b"{}",
        "evidence/candidate-release-gate-1.0.json": b"{}",
        "evidence/VALIDATION-SHA256SUMS": b"manifest",
        "evidence/live/validation-ares.json": b"{}",
        "evidence/recovery/candidate-release-gate-1.0.json": b"{}",
    }
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in files.items():
            archive.writestr(name, payload)


def _write_distribution_archives(
    directory: Path,
    *,
    metadata_version: str = "1.0.0",
    sdist_extra: tuple[tarfile.TarInfo, bytes | None] | None = None,
) -> tuple[Path, Path]:
    wheel = directory / "clio_relay-1.0.0-py3-none-any.whl"
    sdist = directory / "clio_relay-1.0.0.tar.gz"
    metadata = (
        f"Metadata-Version: 2.4\nName: clio-relay\nVersion: {metadata_version}\n\n"
    ).encode()
    with zipfile.ZipFile(wheel, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("clio_relay/__init__.py", b"__version__ = '1.0.0'\n")
        archive.writestr("clio_relay-1.0.0.dist-info/METADATA", metadata)
        archive.writestr(
            "clio_relay-1.0.0.dist-info/WHEEL",
            b"Wheel-Version: 1.0\nGenerator: tests\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        )
        archive.writestr("clio_relay-1.0.0.dist-info/RECORD", b"")
    with tarfile.open(sdist, "w:gz") as archive:
        root = tarfile.TarInfo("clio_relay-1.0.0")
        root.type = tarfile.DIRTYPE
        archive.addfile(root)
        entries = {
            "clio_relay-1.0.0/PKG-INFO": metadata,
            "clio_relay-1.0.0/pyproject.toml": b"[build-system]\nrequires=[]\n",
            "clio_relay-1.0.0/src/clio_relay/__init__.py": b"__version__='1.0.0'\n",
        }
        for name, content in entries.items():
            member = tarfile.TarInfo(name)
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))
        if sdist_extra is not None:
            member, content = sdist_extra
            if content is not None:
                member.size = len(content)
                archive.addfile(member, io.BytesIO(content))
            else:
                archive.addfile(member)
    return wheel, sdist


def test_actions_artifact_manifest_and_archive_bind_exact_api_identity(tmp_path: Path) -> None:
    archive = tmp_path / "candidate.zip"
    _write_tag_payload_zip(archive)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    manifest = build_actions_artifact_manifest(
        _artifact_run(),
        _artifact_listing(size=archive.stat().st_size, digest=digest),
        repository=REPOSITORY,
        source_commit=COMMIT,
        tag=TAG,
        run_id=5001,
        run_attempt=3,
        artifact_name=f"release-candidate-{TAG}",
        artifact_kind="tag-payload",
    )
    output = tmp_path / "candidate"

    verify_actions_artifact_archive(manifest, archive, output)

    assert {item.name for item in output.iterdir()} == {
        "clio_relay-1.0.0-py3-none-any.whl",
        "clio_relay-1.0.0.tar.gz",
        "validation-local.json",
        "SHA256SUMS",
    }


@pytest.mark.parametrize("inconsistent_receipt", [False, True])
def test_candidate_archive_binds_extracted_distributions_to_build_receipt(
    tmp_path: Path,
    inconsistent_receipt: bool,
) -> None:
    archive = tmp_path / "candidate.zip"
    _write_candidate_zip(archive, inconsistent_receipt=inconsistent_receipt)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    manifest = build_actions_artifact_manifest(
        _artifact_run(kind="candidate"),
        _artifact_listing(
            size=archive.stat().st_size,
            digest=digest,
            kind="candidate",
        ),
        repository=REPOSITORY,
        source_commit=TESTED_COMMIT,
        source_tree=TREE,
        tag=TAG,
        run_id=1001,
        run_attempt=2,
        artifact_name=f"release-candidate-{TREE}",
        artifact_kind="candidate",
    )

    if inconsistent_receipt:
        with pytest.raises(ProvenanceError, match="distributions differ"):
            verify_actions_artifact_archive(manifest, archive, tmp_path / "candidate")
    else:
        verify_actions_artifact_archive(manifest, archive, tmp_path / "candidate")


@pytest.mark.parametrize(
    "mutation",
    [
        "attempt",
        "head",
        "name",
        "count",
        "expired",
        "digest",
        "size",
        "foreign_repo",
        "prior_attempt",
    ],
)
def test_actions_artifact_manifest_rejects_replay_or_unbounded_metadata(
    mutation: str,
) -> None:
    run = _artifact_run()
    artifacts = _artifact_listing(size=100, digest="1" * 64)
    artifact = cast(list[dict[str, object]], artifacts["artifacts"])[0]
    if mutation == "attempt":
        run["run_attempt"] = 2
    elif mutation == "head":
        run["head_sha"] = "b" * 40
    elif mutation == "name":
        artifact["name"] = "release-candidate-v1.0.3"
    elif mutation == "count":
        artifacts["total_count"] = 2
    elif mutation == "expired":
        artifact["expired"] = True
    elif mutation == "digest":
        artifact["digest"] = "sha256:not-a-digest"
    elif mutation == "size":
        artifact["size_in_bytes"] = MAX_ACTIONS_ARTIFACT_ARCHIVE_BYTES + 1
    elif mutation == "foreign_repo":
        cast(dict[str, object], run["head_repository"])["id"] = 101
    else:
        artifact["created_at"] = "2026-07-11T09:59:59Z"

    with pytest.raises(ProvenanceError):
        build_actions_artifact_manifest(
            run,
            artifacts,
            repository=REPOSITORY,
            source_commit=COMMIT,
            tag=TAG,
            run_id=5001,
            run_attempt=3,
            artifact_name=f"release-candidate-{TAG}",
            artifact_kind="tag-payload",
        )


@pytest.mark.parametrize("mutation", ["digest", "size", "extra", "traversal"])
def test_actions_artifact_archive_rejects_tampering_and_extra_paths(
    tmp_path: Path,
    mutation: str,
) -> None:
    archive = tmp_path / "candidate.zip"
    _write_tag_payload_zip(
        archive,
        extra_name="../escape"
        if mutation == "traversal"
        else "extra.txt"
        if mutation == "extra"
        else None,
    )
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    manifest = build_actions_artifact_manifest(
        _artifact_run(),
        _artifact_listing(size=archive.stat().st_size, digest=digest),
        repository=REPOSITORY,
        source_commit=COMMIT,
        tag=TAG,
        run_id=5001,
        run_attempt=3,
        artifact_name=f"release-candidate-{TAG}",
        artifact_kind="tag-payload",
    )
    artifact = cast(dict[str, object], manifest["artifact"])
    if mutation == "digest":
        artifact["digest"] = f"sha256:{'2' * 64}"
    elif mutation == "size":
        artifact["size_in_bytes"] = cast(int, artifact["size_in_bytes"]) + 1

    with pytest.raises(ProvenanceError):
        verify_actions_artifact_archive(manifest, archive, tmp_path / "output")


def test_in_progress_promotion_artifact_is_bound_to_the_same_dispatch_run() -> None:
    manifest = build_actions_artifact_manifest(
        _artifact_run(kind="promotion"),
        _artifact_listing(size=100, digest="1" * 64, kind="promotion"),
        repository=REPOSITORY,
        source_commit=COMMIT,
        tag=TAG,
        run_id=5001,
        run_attempt=3,
        artifact_name=f"verified-release-{TAG}",
        artifact_kind="promotion",
    )

    assert manifest["artifact_kind"] == "promotion"


def test_promotion_artifact_extracts_only_the_bounded_canonical_tree(tmp_path: Path) -> None:
    archive = tmp_path / "promotion.zip"
    _write_promotion_zip(archive)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    manifest = build_actions_artifact_manifest(
        _artifact_run(kind="promotion"),
        _artifact_listing(
            size=archive.stat().st_size,
            digest=digest,
            kind="promotion",
        ),
        repository=REPOSITORY,
        source_commit=COMMIT,
        tag=TAG,
        run_id=5001,
        run_attempt=3,
        artifact_name=f"verified-release-{TAG}",
        artifact_kind="promotion",
    )

    verify_actions_artifact_archive(manifest, archive, tmp_path / "promotion")

    assert (tmp_path / "promotion/evidence/recovery/candidate-release-gate-1.0.json").is_file()


def test_distribution_archives_are_fully_bounded_and_identity_checked(tmp_path: Path) -> None:
    wheel, sdist = _write_distribution_archives(tmp_path)

    receipt = build_distribution_archive_receipt(
        wheel,
        sdist,
        project="clio-relay",
        version="1.0.0",
    )

    assert receipt["schema_version"] == "clio-relay.distribution-archives.v1"
    assert cast(dict[str, object], receipt["wheel"])["member_count"] == 4
    assert cast(dict[str, object], receipt["sdist"])["top_level_directory"] == ("clio_relay-1.0.0")
    assert cast(dict[str, object], receipt["limits"])["maximum_members"] == (
        MAX_DISTRIBUTION_MEMBERS
    )


@pytest.mark.parametrize(
    "mutation",
    ["traversal", "sdist_traversal", "symlink", "metadata", "member_limit", "member_size"],
)
def test_distribution_archive_preflight_rejects_adversarial_members(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    if mutation == "metadata":
        wheel, sdist = _write_distribution_archives(tmp_path, metadata_version="9.9.9")
    elif mutation in {"sdist_traversal", "symlink"}:
        link = tarfile.TarInfo("clio_relay-1.0.0/escape")
        content: bytes | None = None
        if mutation == "symlink":
            link.type = tarfile.SYMTYPE
            link.linkname = "../../escape"
        else:
            link.name = "../escape"
            content = b"escape"
        wheel, sdist = _write_distribution_archives(
            tmp_path,
            sdist_extra=(link, content),
        )
    else:
        wheel, sdist = _write_distribution_archives(tmp_path)
    if mutation == "traversal":
        with zipfile.ZipFile(wheel, "a", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("../escape", b"escape")
    elif mutation == "member_limit":
        monkeypatch.setattr(ci_validation, "MAX_DISTRIBUTION_MEMBERS", 2)
    elif mutation == "member_size":
        monkeypatch.setattr(ci_validation, "MAX_DISTRIBUTION_MEMBER_BYTES", 8)

    with pytest.raises(ProvenanceError):
        build_distribution_archive_receipt(
            wheel,
            sdist,
            project="clio-relay",
            version="1.0.0",
        )


def test_distribution_archive_preflight_rejects_zip_symlinks_and_expansion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wheel, sdist = _write_distribution_archives(tmp_path)
    link = zipfile.ZipInfo("clio_relay/link")
    link.external_attr = (stat.S_IFLNK | 0o777) << 16
    with zipfile.ZipFile(wheel, "a") as archive:
        archive.writestr(link, b"../../escape")
    with pytest.raises(ProvenanceError, match="not regular"):
        build_distribution_archive_receipt(
            wheel,
            sdist,
            project="clio-relay",
            version="1.0.0",
        )


def test_distribution_archive_rejects_raw_gzip_expansion_before_tar_parsing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wheel, sdist = _write_distribution_archives(tmp_path)
    monkeypatch.setattr(ci_validation, "MAX_DISTRIBUTION_TAR_BYTES", 1024)
    sdist.write_bytes(gzip.compress(b"x" * 1025))

    with pytest.raises(ProvenanceError, match="tar stream exceeds"):
        build_distribution_archive_receipt(
            wheel,
            sdist,
            project="clio-relay",
            version="1.0.0",
        )

    wheel, sdist = _write_distribution_archives(tmp_path)
    monkeypatch.setattr(ci_validation, "MAX_DISTRIBUTION_UNCOMPRESSED_BYTES", 32)
    with pytest.raises(ProvenanceError, match="aggregate"):
        build_distribution_archive_receipt(
            wheel,
            sdist,
            project="clio-relay",
            version="1.0.0",
        )


def test_exact_release_assets_bind_ids_names_sizes_and_digests(tmp_path: Path) -> None:
    wheel = tmp_path / "clio_relay-1.0.0-py3-none-any.whl"
    claims = tmp_path / "RELEASE-CLAIMS.json"
    wheel.write_bytes(b"xxx")
    claims.write_bytes(b"xx")
    release = _release_assets([(wheel.name, 3), (claims.name, 2)])

    receipt = build_exact_release_asset_inventory(
        release,
        [wheel, claims],
        next_page_document=[],
        page_size=100,
    )
    verify_exact_release_asset_inventory(
        receipt,
        release,
        [wheel, claims],
        next_page_document=[],
        page_size=100,
    )

    assert receipt["release_asset_count"] == 2
    assert receipt["release_asset_aggregate_bytes"] == 5
    assert receipt["api_pagination"] == {
        "page_size": 100,
        "pages_requested": [1, 2],
        "first_page_count": 2,
        "next_page_count": 0,
        "maximum_asset_count": MAX_RELEASE_ASSET_METADATA_RECORDS,
    }
    assert [
        item["name"] for item in cast(list[dict[str, object]], receipt["release_assets"])
    ] == sorted([wheel.name, claims.name])


@pytest.mark.parametrize("mutation", ["missing", "extra", "digest", "size", "unsafe"])
def test_exact_release_assets_reject_any_live_inventory_difference(
    tmp_path: Path,
    mutation: str,
) -> None:
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    first.write_bytes(b"xx")
    second.write_bytes(b"xxx")
    release = _release_assets([(first.name, 2), (second.name, 3)])
    assets = cast(list[dict[str, object]], release["assets"])
    if mutation == "missing":
        assets.pop()
    elif mutation == "extra":
        assets.append(
            {
                "id": 9999,
                "name": "extra.json",
                "size": 1,
                "digest": f"sha256:{hashlib.sha256(b'x').hexdigest()}",
            }
        )
    elif mutation == "digest":
        assets[0]["digest"] = f"sha256:{'f' * 64}"
    elif mutation == "size":
        assets[0]["size"] = 1
    else:
        assets[0]["name"] = "../escape"

    with pytest.raises(ProvenanceError):
        build_exact_release_asset_inventory(
            release,
            [first, second],
            next_page_document=[],
            page_size=100,
        )


def test_exact_release_assets_require_empty_bounded_next_page(tmp_path: Path) -> None:
    subject = tmp_path / "RELEASE-CLAIMS.json"
    subject.write_bytes(b"xx")
    release = _release_assets([(subject.name, 2)])

    with pytest.raises(ProvenanceError, match="non-empty next page"):
        build_exact_release_asset_inventory(
            release,
            [subject],
            next_page_document=[{"id": 9999}],
            page_size=100,
        )

    oversized = deepcopy(release)
    assets = cast(list[dict[str, object]], oversized["assets"])
    assets.extend(deepcopy(assets[0]) for _ in range(MAX_RELEASE_ASSET_METADATA_RECORDS))
    with pytest.raises(ProvenanceError, match="count must be"):
        build_exact_release_asset_inventory(
            oversized,
            [subject],
            next_page_document=[],
            page_size=100,
        )


def test_exact_release_assets_reject_postpublication_id_replacement(tmp_path: Path) -> None:
    subject = tmp_path / "RELEASE-CLAIMS.json"
    subject.write_bytes(b"xx")
    release = _release_assets([(subject.name, 2)])
    receipt = build_exact_release_asset_inventory(
        release,
        [subject],
        next_page_document=[],
        page_size=100,
    )
    replaced = deepcopy(release)
    cast(list[dict[str, object]], replaced["assets"])[0]["id"] = 9999

    with pytest.raises(ProvenanceError, match="differs from the prepublication receipt"):
        verify_exact_release_asset_inventory(
            receipt,
            replaced,
            [subject],
            next_page_document=[],
            page_size=100,
        )


def test_protected_main_rewrites_checksums_and_plans_idempotent_staging(
    tmp_path: Path,
) -> None:
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    subjects = {
        "clio_relay-1.0.0-py3-none-any.whl": b"wheel",
        "clio_relay-1.0.0.tar.gz": b"sdist",
        "validation-local.json": b"{}",
        "CI-STATUS.json": b"{}",
        "REPOSITORY-GOVERNANCE.json": b"{}",
        "SHA256SUMS": b"untrusted tag manifest",
    }
    for name, payload in subjects.items():
        (candidate / name).write_bytes(payload)

    write_candidate_checksum_manifest(candidate)
    missing_plan = build_staged_release_asset_plan({"assets": []}, candidate)
    missing = cast(list[dict[str, object]], missing_plan["missing"])
    assert {cast(str, item["name"]) for item in missing} == set(subjects)

    one = missing[0]
    existing_plan = build_staged_release_asset_plan(
        {
            "assets": [
                {
                    "name": one["name"],
                    "size": one["size"],
                    "digest": one["digest"],
                }
            ]
        },
        candidate,
    )
    assert cast(list[dict[str, object]], existing_plan["existing"]) == [one]
    assert len(cast(list[object], existing_plan["missing"])) == 5


def test_repository_governance_receipt_requires_effective_current_token_controls() -> None:
    receipt = build_repository_governance(
        _main_effective_rules(),
        _protected_branches(),
        _branch_rulesets(),
        _tag_rulesets(),
        _environments(),
        _immutable_releases(),
        repository=REPOSITORY,
        source_commit=COMMIT,
        tag=TAG,
    )

    assert receipt["environment_reviewers_available"] is False
    main = cast(dict[str, object], receipt["main_protection"])
    assert main["source"] == "effective_rulesets"
    assert main["ruleset_ids"] == [MAIN_RULESET_ID]
    assert main["current_workflow_token_can_bypass"] is False
    assert main["review_policy"] == MAIN_REVIEW_POLICY
    assert main["required_approving_review_count"] == REQUIRED_APPROVING_REVIEW_COUNT
    assert main["require_last_push_approval"] is REQUIRE_LAST_PUSH_APPROVAL
    main_ruleset = cast(list[dict[str, object]], main["rulesets"])[0]
    assert main_ruleset["global_bypass_actors_visible"] is True
    assert main_ruleset["configured_bypass_actor_count"] == 1
    tag_protection = cast(list[dict[str, object]], receipt["tag_protections"])[0]
    assert tag_protection["current_workflow_token_can_bypass"] is False
    assert tag_protection["configured_bypass_actor_count"] == 1
    verify_repository_governance(
        receipt,
        repository=REPOSITORY,
        source_commit=COMMIT,
        tag=TAG,
    )


def test_live_governance_requery_must_equal_the_carried_receipt() -> None:
    routes = _governance_routes()

    def fetch(path: str) -> object:
        return deepcopy(routes[path])

    receipt = fetch_live_repository_governance(
        repository=REPOSITORY,
        source_commit=COMMIT,
        tag=TAG,
        fetch_json=fetch,
        fetch_admin_json=fetch,
    )
    verify_live_repository_governance(
        receipt,
        repository=REPOSITORY,
        source_commit=COMMIT,
        tag=TAG,
        fetch_json=fetch,
        fetch_admin_json=fetch,
    )

    branch = cast(dict[str, object], routes[f"repos/{REPOSITORY}/rulesets/{MAIN_RULESET_ID}"])
    branch["current_user_can_bypass"] = "always"
    with pytest.raises(ProvenanceError):
        verify_live_repository_governance(
            receipt,
            repository=REPOSITORY,
            source_commit=COMMIT,
            tag=TAG,
            fetch_json=fetch,
            fetch_admin_json=fetch,
        )


@pytest.mark.parametrize(
    ("surface", "expected_message"),
    [
        ("status_app", "integration id"),
        ("main_bypass", "current workflow token can bypass"),
        ("missing_main_rule", "effective main branch rules are incomplete"),
        ("merge_queue", "merge queue parameters"),
        ("review_count", "required_approving_review_count"),
        ("last_push_approval", "require_last_push_approval"),
        ("tag_bypass", "current_workflow_token_cannot_bypass"),
        ("tag_update", "no active workflow-token-protected tag ruleset"),
        ("environment", "protected-branch deployment policy"),
        ("environment_policy", "protected-branch deployment policy"),
        ("environment_admin_bypass", "administrator bypass"),
        ("immutable_releases", "immutable releases must be enabled"),
    ],
)
def test_repository_governance_rejects_weakened_controls(
    surface: str,
    expected_message: str,
) -> None:
    effective = deepcopy(_main_effective_rules())
    branch_rulesets = deepcopy(_branch_rulesets())
    tag_rulesets = deepcopy(_tag_rulesets())
    environments = deepcopy(_environments())
    immutable_releases = deepcopy(_immutable_releases())
    if surface == "status_app":
        status = cast(dict[str, object], effective[-1]["parameters"])
        cast(list[dict[str, object]], status["required_status_checks"])[0]["integration_id"] = None
    elif surface == "main_bypass":
        branch_rulesets[0]["current_user_can_bypass"] = "always"
    elif surface == "missing_main_rule":
        effective.pop(0)
    elif surface == "merge_queue":
        queue = cast(dict[str, object], effective[2]["parameters"])
        queue["max_entries_to_build"] = 6
    elif surface == "review_count":
        reviews = cast(dict[str, object], effective[3]["parameters"])
        reviews["required_approving_review_count"] = 1
    elif surface == "last_push_approval":
        reviews = cast(dict[str, object], effective[3]["parameters"])
        reviews["require_last_push_approval"] = True
    elif surface == "tag_bypass":
        tag_rulesets[0]["current_user_can_bypass"] = "always"
    elif surface == "tag_update":
        tag_rulesets[0]["rules"] = [{"type": "deletion"}]
    elif surface == "environment":
        environment = cast(dict[str, object], environments["live-validation"])
        environment["protection_rules"] = []
    elif surface == "environment_policy":
        environment = cast(dict[str, object], environments["live-validation"])
        environment["deployment_branch_policy"] = {
            "protected_branches": False,
            "custom_branch_policies": True,
        }
    elif surface == "environment_admin_bypass":
        environment = cast(dict[str, object], environments["live-validation"])
        environment["can_admins_bypass"] = True
    else:
        immutable_releases["enabled"] = False

    with pytest.raises(ProvenanceError, match=expected_message):
        build_repository_governance(
            effective,
            _protected_branches(),
            branch_rulesets,
            tag_rulesets,
            environments,
            immutable_releases,
            repository=REPOSITORY,
            source_commit=COMMIT,
            tag=TAG,
        )


def test_repository_governance_rejects_another_protected_branch() -> None:
    protected_branches = [*_protected_branches(), {"name": "release-work", "protected": True}]

    with pytest.raises(ProvenanceError, match="sole protected branch"):
        build_repository_governance(
            _main_effective_rules(),
            protected_branches,
            _branch_rulesets(),
            _tag_rulesets(),
            _environments(),
            _immutable_releases(),
            repository=REPOSITORY,
            source_commit=COMMIT,
            tag=TAG,
        )


def _release_identity() -> dict[str, object]:
    return {
        "id": 8001,
        "tag_name": TAG,
        "target_commitish": COMMIT,
        "draft": True,
        "prerelease": False,
        "immutable": False,
    }


def _release_routes(*, release: dict[str, object] | None = None) -> dict[str, object]:
    exact = _release_identity() if release is None else release
    return {
        f"repos/{REPOSITORY}/releases?per_page=100&page=1": [exact],
        f"repos/{REPOSITORY}/releases?per_page=100&page=2": [],
        f"repos/{REPOSITORY}/releases/{exact['id']}": exact,
    }


def test_release_identity_binds_tag_target_and_state_to_source_commit() -> None:
    verify_release_identity(
        _release_identity(),
        tag=TAG,
        source_commit=COMMIT,
        resolved_tag_commit=COMMIT,
        resolved_target_commit=COMMIT,
        expect_draft=True,
        expect_prerelease=False,
    )


@pytest.mark.parametrize(
    "mutation",
    ["tag_name", "tag_resolution", "target_resolution", "draft"],
)
def test_release_identity_rejects_mutable_or_mismatched_identity(mutation: str) -> None:
    release = _release_identity()
    tag_commit = COMMIT
    target_commit = COMMIT
    if mutation == "tag_name":
        release["tag_name"] = "v1.0.3"
    elif mutation == "tag_resolution":
        tag_commit = "b" * 40
    elif mutation == "target_resolution":
        target_commit = "b" * 40
    else:
        release["draft"] = False

    with pytest.raises(ProvenanceError):
        verify_release_identity(
            release,
            tag=TAG,
            source_commit=COMMIT,
            resolved_tag_commit=tag_commit,
            resolved_target_commit=target_commit,
            expect_draft=True,
            expect_prerelease=False,
        )


def test_release_identity_accepts_existing_tag_target_spelling_when_it_resolves_exactly() -> None:
    release = _release_identity()
    release["target_commitish"] = "main"

    verify_release_identity(
        release,
        tag=TAG,
        source_commit=COMMIT,
        resolved_tag_commit=COMMIT,
        resolved_target_commit=COMMIT,
        expect_draft=True,
        expect_prerelease=False,
    )


def test_live_release_identity_resolves_both_tag_and_explicit_target() -> None:
    routes: dict[str, object] = {
        **_release_routes(),
        f"repos/{REPOSITORY}/commits/{TAG}": {"sha": COMMIT},
        f"repos/{REPOSITORY}/commits/{COMMIT}": {"sha": COMMIT},
    }

    verify_live_release_identity(
        repository=REPOSITORY,
        tag=TAG,
        source_commit=COMMIT,
        expect_draft=True,
        expect_prerelease=False,
        fetch_json=lambda path: routes[path],
    )


def test_live_release_resolver_uses_bounded_numeric_identity() -> None:
    paths: list[str] = []

    def fetch(path: str) -> object:
        paths.append(path)
        return deepcopy(_release_routes()[path])

    release = resolve_live_release(
        repository=REPOSITORY,
        tag=TAG,
        expect_draft=True,
        fetch_json=fetch,
    )

    assert release == _release_identity()
    assert paths == [
        f"repos/{REPOSITORY}/releases?per_page=100&page=1",
        f"repos/{REPOSITORY}/releases?per_page=100&page=2",
        f"repos/{REPOSITORY}/releases/8001",
    ]
    assert all("/releases/tags/" not in path for path in paths)


def test_live_release_resolver_paginates_complete_history_before_numeric_resolution() -> None:
    routes = _release_routes()
    routes[f"repos/{REPOSITORY}/releases?per_page=100&page=1"] = [
        {**_release_identity(), "id": 7999, "tag_name": "v0.9.22"}
    ]
    routes[f"repos/{REPOSITORY}/releases?per_page=100&page=2"] = [_release_identity()]
    routes[f"repos/{REPOSITORY}/releases?per_page=100&page=3"] = []

    release = resolve_live_release(
        repository=REPOSITORY,
        tag=TAG,
        expect_draft=True,
        fetch_json=lambda path: deepcopy(routes[path]),
    )

    assert release == _release_identity()


def test_live_release_resolver_allows_proven_absence() -> None:
    routes = _release_routes()
    routes[f"repos/{REPOSITORY}/releases?per_page=100&page=1"] = []

    assert (
        resolve_live_release(
            repository=REPOSITORY,
            tag=TAG,
            expect_draft=None,
            fetch_json=lambda path: deepcopy(routes[path]),
            allow_absent=True,
        )
        is None
    )


@pytest.mark.parametrize(
    "mutation",
    ["duplicate", "duplicate_later_page", "repeated_id", "oversized_page", "numeric_drift"],
)
def test_live_release_resolver_fails_closed_on_unbounded_or_racing_state(mutation: str) -> None:
    routes = _release_routes()
    if mutation == "duplicate":
        routes[f"repos/{REPOSITORY}/releases?per_page=100&page=1"] = [
            _release_identity(),
            {**_release_identity(), "id": 8002},
        ]
    elif mutation == "duplicate_later_page":
        routes[f"repos/{REPOSITORY}/releases?per_page=100&page=2"] = [
            {**_release_identity(), "id": 8002}
        ]
        routes[f"repos/{REPOSITORY}/releases?per_page=100&page=3"] = []
    elif mutation == "repeated_id":
        routes[f"repos/{REPOSITORY}/releases?per_page=100&page=2"] = [
            {**_release_identity(), "tag_name": "v0.9.22"}
        ]
        routes[f"repos/{REPOSITORY}/releases?per_page=100&page=3"] = []
    elif mutation == "oversized_page":
        routes[f"repos/{REPOSITORY}/releases?per_page=100&page=1"] = [
            {**_release_identity(), "id": index + 1, "tag_name": f"v0.0.{index}"}
            for index in range(101)
        ]
    else:
        routes[f"repos/{REPOSITORY}/releases/8001"] = {
            **_release_identity(),
            "draft": False,
        }

    with pytest.raises(ProvenanceError):
        resolve_live_release(
            repository=REPOSITORY,
            tag=TAG,
            expect_draft=True,
            fetch_json=lambda path: deepcopy(routes[path]),
        )


def test_live_release_resolver_rejects_an_unterminated_paginated_history() -> None:
    paths: list[str] = []

    def fetch(path: str) -> object:
        paths.append(path)
        page = int(path.rpartition("=")[2])
        return [{**_release_identity(), "id": page, "tag_name": f"v0.0.{page}"}]

    with pytest.raises(ProvenanceError, match="bounded pagination window"):
        resolve_live_release(
            repository=REPOSITORY,
            tag=TAG,
            expect_draft=None,
            fetch_json=fetch,
            allow_absent=True,
        )

    assert len(paths) == 100


def _governance_receipt() -> dict[str, object]:
    return build_repository_governance(
        _main_effective_rules(),
        _protected_branches(),
        _branch_rulesets(),
        _tag_rulesets(),
        _environments(),
        _immutable_releases(),
        repository=REPOSITORY,
        source_commit=COMMIT,
        tag=TAG,
    )


def _mutation_routes() -> dict[str, object]:
    return {
        **_governance_routes(),
        **_release_routes(),
        f"repos/{REPOSITORY}/commits/main": {"sha": COMMIT},
        f"repos/{REPOSITORY}/commits/{TAG}": {"sha": COMMIT},
        f"repos/{REPOSITORY}/commits/{COMMIT}": {"sha": COMMIT},
    }


def test_mutation_authority_revalidates_exact_main_tag_governance_and_draft() -> None:
    routes = _mutation_routes()

    verify_live_mutation_authority(
        _governance_receipt(),
        repository=REPOSITORY,
        source_commit=COMMIT,
        tag=TAG,
        workflow_ref="refs/heads/main",
        workflow_sha=COMMIT,
        release_state="present",
        expect_draft=True,
        fetch_json=lambda path: deepcopy(routes[path]),
        fetch_admin_json=lambda path: deepcopy(routes[path]),
    )


def test_mutation_authority_proves_release_absence_before_create() -> None:
    routes = _mutation_routes()
    routes[f"repos/{REPOSITORY}/releases?per_page=100&page=1"] = []

    verify_live_mutation_authority(
        _governance_receipt(),
        repository=REPOSITORY,
        source_commit=COMMIT,
        tag=TAG,
        workflow_ref="refs/heads/main",
        workflow_sha=COMMIT,
        release_state="absent",
        expect_draft=True,
        fetch_json=lambda path: deepcopy(routes[path]),
        fetch_admin_json=lambda path: deepcopy(routes[path]),
    )


def test_mutation_authority_rejects_an_unspecified_present_draft_state() -> None:
    with pytest.raises(ProvenanceError, match="only while the release is a draft"):
        verify_live_mutation_authority(
            _governance_receipt(),
            repository=REPOSITORY,
            source_commit=COMMIT,
            tag=TAG,
            workflow_ref="refs/heads/main",
            workflow_sha=COMMIT,
            release_state="present",
            expect_draft=None,
            fetch_json=lambda path: deepcopy(_mutation_routes()[path]),
            fetch_admin_json=lambda path: deepcopy(_mutation_routes()[path]),
        )


@pytest.mark.parametrize("mutation", ["workflow", "main", "tag", "draft", "governance"])
def test_mutation_authority_rejects_stale_or_bypassable_live_state(mutation: str) -> None:
    routes = _mutation_routes()
    workflow_ref = "refs/heads/main"
    if mutation == "workflow":
        workflow_ref = f"refs/tags/{TAG}"
    elif mutation == "main":
        routes[f"repos/{REPOSITORY}/commits/main"] = {"sha": "b" * 40}
    elif mutation == "tag":
        routes[f"repos/{REPOSITORY}/commits/{TAG}"] = {"sha": "b" * 40}
    elif mutation == "draft":
        cast(dict[str, object], routes[f"repos/{REPOSITORY}/releases/8001"])["draft"] = False
    else:
        cast(dict[str, object], routes[f"repos/{REPOSITORY}/rulesets/{MAIN_RULESET_ID}"])[
            "current_user_can_bypass"
        ] = "always"

    with pytest.raises(ProvenanceError):
        verify_live_mutation_authority(
            _governance_receipt(),
            repository=REPOSITORY,
            source_commit=COMMIT,
            tag=TAG,
            workflow_ref=workflow_ref,
            workflow_sha=COMMIT,
            release_state="present",
            expect_draft=True,
            fetch_json=lambda path: deepcopy(routes[path]),
            fetch_admin_json=lambda path: deepcopy(routes[path]),
        )


def test_validation_report_asset_manifest_is_bounded_and_matches_downloads(
    tmp_path: Path,
) -> None:
    release = _release_assets([("validation-local.json", 2), ("validation-ares-cleanup.json", 3)])
    manifest = build_validation_report_asset_manifest(release, kind="candidate")
    report_dir = tmp_path / "reports"
    report_dir.mkdir()
    (report_dir / "validation-local.json").write_bytes(b"xx")
    (report_dir / "validation-ares-cleanup.json").write_bytes(b"xxx")

    verify_downloaded_validation_report_assets(manifest, report_dir)
    assert manifest["aggregate_bytes"] == 5
    assert manifest["report_count"] == 2
    assert cast(dict[str, int], manifest["limits"]) == {
        "maximum_release_asset_metadata_records": MAX_RELEASE_ASSET_METADATA_RECORDS,
        "maximum_release_asset_bytes": MAX_RELEASE_ASSET_BYTES,
        "maximum_release_asset_aggregate_bytes": MAX_RELEASE_ASSET_AGGREGATE_BYTES,
        "maximum_distribution_bytes": MAX_DISTRIBUTION_BYTES,
        "maximum_fixed_json_bytes": MAX_FIXED_JSON_BYTES,
        "maximum_manifest_bytes": MAX_MANIFEST_BYTES,
        "maximum_assets": MAX_VALIDATION_REPORT_ASSETS,
        "maximum_asset_bytes": MAX_VALIDATION_REPORT_BYTES,
        "maximum_aggregate_bytes": MAX_VALIDATION_REPORT_AGGREGATE_BYTES,
    }


def test_release_report_asset_manifest_enforces_exact_ordered_matrix() -> None:
    matrix_path = (
        Path(__file__).parents[1] / "examples" / "release-gate" / ("report-matrix-1.0.json")
    )
    raw_matrix = json.loads(matrix_path.read_text(encoding="utf-8"))
    raw_reports = cast(list[dict[str, object]], raw_matrix["reports"])
    assert [item["id"] for item in raw_reports[5:8]] == [
        "ares-spack-find",
        "ares-spack-locate",
        "ares-spack-install",
    ]
    assert [item["evidence_group"] for item in raw_reports[5:8]] == [
        "spack",
        "spack",
        "spack-fresh",
    ]
    assert [item["package"] for item in raw_reports[5:8]] == [
        "lammps",
        "lammps",
        "libsigsegv@2.14",
    ]
    matrix = validate_release_acceptance_matrix(raw_matrix)
    assert matrix["matrix_sha256"] == (
        "1b5e5b4ab864ad37920bde06d4435d9eb007f88301f6b34f875dea4c7d89aec4"
    )
    reports = cast(list[dict[str, object]], matrix["reports"])
    report_ids = [cast(str, item["id"]) for item in reports]
    assets = [
        ("validation-local.json", 2),
        *[(f"validation-{report_id}.json", 3) for report_id in reversed(report_ids)],
    ]

    manifest = build_validation_report_asset_manifest(
        _release_assets(assets),
        kind="candidate",
        acceptance_matrix=raw_matrix,
    )

    binding = cast(dict[str, object], manifest["acceptance_matrix"])
    assert binding["sha256"] == matrix["matrix_sha256"]
    assert binding["report_count"] == 17
    manifest_assets = cast(list[dict[str, object]], manifest["assets"])
    assert [cast(str, item["name"]) for item in manifest_assets] == [
        "validation-local.json",
        *[f"validation-{report_id}.json" for report_id in report_ids],
    ]

    with pytest.raises(ProvenanceError, match="do not exactly match"):
        build_validation_report_asset_manifest(
            _release_assets(assets[:-1]),
            kind="candidate",
            acceptance_matrix=raw_matrix,
        )


@pytest.mark.parametrize(
    ("kind", "prefix", "include_local"),
    [
        ("candidate", "validation", True),
        ("released", "released-validation", False),
    ],
)
def test_release_report_asset_manifest_derives_configurable_matrix_cardinality(
    kind: str,
    prefix: str,
    include_local: bool,
) -> None:
    raw_matrix: dict[str, object] = {
        "schema_version": "clio-relay.release-acceptance-matrix.v1",
        "release_version": "1.0.0",
        "matrix_sha256": "",
        "report_count_per_stage": 2,
        "target_labels_are_policy_evidence_instances": True,
        "stages": [
            {
                "name": "candidate",
                "artifact_stage": "immutable_candidate",
                "filename_prefix": "validation",
            },
            {
                "name": "released",
                "artifact_stage": "published",
                "filename_prefix": "released-validation",
            },
        ],
        "reports": [
            {
                "ordinal": ordinal,
                "id": report_id,
                "cluster": cluster,
                "scenario": "cleanup",
                "command": ["clio-relay", "session", "cleanup"],
                "report_option": "--report",
            }
            for ordinal, report_id, cluster in (
                (1, "site-alpha-cleanup", "site-alpha"),
                (2, "site-beta-cleanup", "site-beta"),
            )
        ],
    }
    canonical = dict(raw_matrix)
    del canonical["matrix_sha256"]
    raw_matrix["matrix_sha256"] = hashlib.sha256(
        json.dumps(
            canonical,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    report_assets = [
        (f"{prefix}-site-alpha-cleanup.json", 3),
        (f"{prefix}-site-beta-cleanup.json", 4),
    ]
    assets = ([("validation-local.json", 2)] if include_local else []) + report_assets

    manifest = build_validation_report_asset_manifest(
        _release_assets(assets),
        kind=kind,
        acceptance_matrix=raw_matrix,
    )

    binding = cast(dict[str, object], manifest["acceptance_matrix"])
    assert binding["report_count"] == 2
    assert [
        cast(str, report["id"]) for report in cast(list[dict[str, object]], binding["reports"])
    ] == ["site-alpha-cleanup", "site-beta-cleanup"]

    with pytest.raises(ProvenanceError, match="do not exactly match"):
        build_validation_report_asset_manifest(
            _release_assets(assets[:-1]),
            kind=kind,
            acceptance_matrix=raw_matrix,
        )
    with pytest.raises(ProvenanceError, match="do not exactly match"):
        build_validation_report_asset_manifest(
            _release_assets([*assets, (f"{prefix}-site-gamma-cleanup.json", 5)]),
            kind=kind,
            acceptance_matrix=raw_matrix,
        )


@pytest.mark.parametrize("mutation", ["count", "single_size", "aggregate", "unsafe", "missing"])
def test_validation_report_asset_manifest_rejects_unbounded_or_ambiguous_inputs(
    mutation: str,
) -> None:
    assets = [("validation-local.json", 2), ("validation-ares.json", 3)]
    if mutation == "count":
        assets = [
            ("validation-local.json", 1),
            *[
                (f"validation-site-{index}.json", 1)
                for index in range(MAX_VALIDATION_REPORT_ASSETS)
            ],
        ]
    elif mutation == "single_size":
        assets[-1] = (assets[-1][0], MAX_VALIDATION_REPORT_BYTES + 1)
    elif mutation == "aggregate":
        assets = [
            ("validation-local.json", 1),
            *[(f"validation-site-{index}.json", MAX_VALIDATION_REPORT_BYTES) for index in range(9)],
        ]
    elif mutation == "unsafe":
        assets[-1] = ("validation-site name.json", 3)
    else:
        assets = [("validation-local.json", 2)]

    with pytest.raises(ProvenanceError):
        build_validation_report_asset_manifest(_release_assets(assets), kind="candidate")


def test_downloaded_validation_report_manifest_rejects_size_or_file_set_changes(
    tmp_path: Path,
) -> None:
    manifest = build_validation_report_asset_manifest(
        _release_assets(
            [("released-validation-ares.json", 2), ("released-validation-homelab.json", 2)]
        ),
        kind="released",
    )
    report_dir = tmp_path / "reports"
    report_dir.mkdir()
    (report_dir / "released-validation-ares.json").write_bytes(b"xx")
    (report_dir / "released-validation-homelab.json").write_bytes(b"bad")

    with pytest.raises(ProvenanceError, match="differ from the preflight manifest"):
        verify_downloaded_validation_report_assets(manifest, report_dir)


def test_validation_report_preflight_rejects_an_unbounded_release_asset_listing() -> None:
    assets = _release_assets(
        [(f"unrelated-{index}.txt", 1) for index in range(MAX_RELEASE_ASSET_METADATA_RECORDS + 1)]
    )["assets"]

    with pytest.raises(ProvenanceError, match="metadata count exceeds"):
        build_validation_report_asset_manifest(assets, kind="candidate")


@pytest.mark.parametrize("field", ["report_count", "aggregate_bytes", "limits"])
def test_downloaded_report_verification_rejects_tampered_preflight_manifest(
    tmp_path: Path,
    field: str,
) -> None:
    manifest = build_validation_report_asset_manifest(
        _release_assets([("released-validation-ares.json", 2)]),
        kind="released",
    )
    if field == "report_count":
        manifest[field] = 2
    elif field == "aggregate_bytes":
        manifest[field] = 3
    else:
        cast(dict[str, int], manifest[field])["maximum_assets"] += 1
    report_dir = tmp_path / "reports"
    report_dir.mkdir()
    (report_dir / "released-validation-ares.json").write_bytes(b"xx")

    with pytest.raises(ProvenanceError):
        verify_downloaded_validation_report_assets(manifest, report_dir)


def test_json_loader_rejects_oversized_input_before_decoding(tmp_path: Path) -> None:
    document = tmp_path / "oversized.json"
    with document.open("wb") as stream:
        stream.truncate(MAX_JSON_DOCUMENT_BYTES + 1)

    with pytest.raises(ProvenanceError, match="exceeds"):
        ci_validation._load_json(document)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
