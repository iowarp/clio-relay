"""Build and verify release receipts from live GitHub repository state."""

from __future__ import annotations

import argparse
import email.parser
import email.policy
import gzip
import hashlib
import json
import os
import re
import stat
import struct
import tarfile
import tempfile
import urllib.error
import urllib.request
import zipfile
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import BinaryIO, NoReturn, Protocol, cast
from uuid import uuid4

REQUIRED_MATRIX_JOBS: tuple[str, ...] = (
    "ubuntu-latest / python 3.12",
    "ubuntu-latest / python 3.13",
    "ubuntu-latest / python 3.14",
    "windows-latest / python 3.12",
    "windows-latest / python 3.13",
    "windows-latest / python 3.14",
)
REQUIRED_CI_JOBS: tuple[str, ...] = (
    "GitHub Actions syntax and semantics",
    *REQUIRED_MATRIX_JOBS,
    "seal tested release candidate",
)
GITHUB_ACTIONS_APP_ID = 15368
MAIN_REVIEW_POLICY = "single-maintainer"
REQUIRED_APPROVING_REVIEW_COUNT = 0
REQUIRE_LAST_PUSH_APPROVAL = False
REQUIRED_ENVIRONMENTS: tuple[str, ...] = (
    "live-validation",
    "pypi",
    "release-finalization",
    "released-validation",
)
CI_WORKFLOW_PATH = ".github/workflows/ci.yml"
RELEASE_WORKFLOW_PATH = ".github/workflows/release.yml"
IMMUTABLE_RELEASES_API_VERSION = "2026-03-10"
REQUIRED_MERGE_QUEUE_PARAMETERS: dict[str, object] = {
    "check_response_timeout_minutes": 60,
    "grouping_strategy": "ALLGREEN",
    "max_entries_to_build": 5,
    "max_entries_to_merge": 5,
    "merge_method": "SQUASH",
    "min_entries_to_merge": 1,
    "min_entries_to_merge_wait_minutes": 0,
}
RELEASE_TAG_PATTERN = "refs/tags/v*"
MAX_RELEASE_ASSET_METADATA_RECORDS = 96
MAX_RELEASE_HISTORY_PAGES = 100
MAX_VALIDATION_REPORT_ASSETS = 64
MAX_VALIDATION_REPORT_BYTES = 8 * 1024 * 1024
MAX_VALIDATION_REPORT_AGGREGATE_BYTES = 64 * 1024 * 1024
MAX_JSON_DOCUMENT_BYTES = 16 * 1024 * 1024
MAX_ACTIONS_ARTIFACT_ARCHIVE_BYTES = 128 * 1024 * 1024
MAX_DISTRIBUTION_BYTES = 64 * 1024 * 1024
MAX_DISTRIBUTION_MEMBERS = 4096
MAX_DISTRIBUTION_MEMBER_BYTES = 16 * 1024 * 1024
MAX_DISTRIBUTION_UNCOMPRESSED_BYTES = 128 * 1024 * 1024
MAX_DISTRIBUTION_TAR_BYTES = MAX_DISTRIBUTION_UNCOMPRESSED_BYTES + (
    (MAX_DISTRIBUTION_MEMBERS + 4) * 2048
)
MAX_DISTRIBUTION_METADATA_BYTES = 1024 * 1024
MAX_DISTRIBUTION_PATH_LENGTH = 512
MAX_FIXED_JSON_BYTES = 8 * 1024 * 1024
MAX_MANIFEST_BYTES = 1024 * 1024
MAX_RELEASE_ASSET_BYTES = 128 * 1024 * 1024
MAX_RELEASE_ASSET_AGGREGATE_BYTES = 512 * 1024 * 1024
TAG_PAYLOAD_FIXED_FILES = frozenset({"SHA256SUMS", "validation-local.json"})
CANDIDATE_PAYLOAD_FIXED_FILES = frozenset(
    {"SHA256SUMS", "validation-local.json", "CANDIDATE-BUILD.json"}
)
RELEASE_ACCEPTANCE_MATRIX_SCHEMA = "clio-relay.release-acceptance-matrix.v1"
RELEASE_ACCEPTANCE_MATRIX_STAGES = ("candidate", "released")


class ProvenanceError(ValueError):
    """Raised when live GitHub state cannot prove a release prerequisite."""


class GitHubNotFound(ProvenanceError):
    """Raised when an authenticated GitHub resource is conclusively absent."""


class GitHubJsonFetcher(Protocol):
    """Fetch one bounded JSON document from an authenticated GitHub API path."""

    def __call__(self, path: str) -> object:
        """Return the decoded JSON response for a repository API path."""
        ...


class _BinaryReader(Protocol):
    def read(self, size: int = -1, /) -> bytes:
        """Read at most ``size`` bytes from an archive member."""
        ...


def select_ci_run(
    document: object,
    *,
    repository: str,
    source_commit: str,
) -> dict[str, object]:
    """Select the sole successful merge-queue CI run for an exact tested commit."""
    _validate_repository(repository)
    _validate_commit(source_commit)
    payload = _mapping(document, "workflow-runs document")
    runs = _list(payload.get("workflow_runs"), "workflow_runs")
    total_count = _integer(payload.get("total_count"), "workflow-runs total_count")
    if total_count != len(runs) or total_count > 100:
        raise ProvenanceError(
            "workflow-runs query must be complete and bounded to at most 100 records"
        )
    matches: list[dict[str, object]] = []
    for raw in runs:
        run = _mapping(raw, "workflow run")
        if (
            run.get("head_sha") == source_commit
            and run.get("event") == "merge_group"
            and run.get("status") == "completed"
            and run.get("conclusion") == "success"
            and run.get("path") == CI_WORKFLOW_PATH
        ):
            head_branch = _nonempty_string(run.get("head_branch"), "workflow head branch")
            if not head_branch.startswith("gh-readonly-queue/main/"):
                continue
            matches.append(
                {
                    "run_id": _positive_integer(run.get("id"), "workflow run id"),
                    "run_attempt": _positive_integer(
                        run.get("run_attempt"), "workflow run attempt"
                    ),
                    "run_number": _positive_integer(run.get("run_number"), "workflow run number"),
                    "workflow_id": _positive_integer(run.get("workflow_id"), "workflow id"),
                    "url": _https_url(run.get("html_url"), "workflow run URL"),
                    "event": "merge_group",
                    "head_branch": head_branch,
                }
            )
    if len(matches) != 1:
        raise ProvenanceError(
            "tested merge-group commit must have exactly one successful completed run of ci.yml; "
            f"found {len(matches)}"
        )
    return matches[0]


def build_ci_status(
    runs_document: object,
    jobs_document: object,
    candidate_build: object,
    candidate_artifact: object,
    tag_binding: object,
    *,
    repository: str,
    source_commit: str,
) -> dict[str, object]:
    """Build a receipt binding release main to its one tested merge-queue artifact."""
    binding = _mapping(tag_binding, "tag binding")
    verify_tag_binding(binding, repository=repository, source_commit=source_commit)
    build = _mapping(candidate_build, "candidate build receipt")
    verify_candidate_build_receipt(build, repository=repository)
    artifact_manifest = _mapping(candidate_artifact, "candidate artifact manifest")
    _verify_candidate_artifact_manifest(
        artifact_manifest,
        candidate_build=build,
        repository=repository,
    )
    if binding.get("candidate_build_sha256") != _canonical_json_sha256(build):
        raise ProvenanceError("tag binding does not identify the candidate build receipt")
    if binding.get("merge_group_anchor_pull_request_number") != build.get("pull_request_number"):
        raise ProvenanceError("tag binding does not identify the merge-group anchor pull request")
    if binding.get("candidate_artifact") != artifact_manifest.get("artifact"):
        raise ProvenanceError("tag binding does not identify the selected candidate artifact")
    tested_commit = _nonempty_string(build.get("tested_commit"), "tested merge-group commit")
    selected = select_ci_run(
        runs_document,
        repository=repository,
        source_commit=tested_commit,
    )
    if selected["run_id"] != build.get("run_id") or selected["run_attempt"] != build.get(
        "run_attempt"
    ):
        raise ProvenanceError("candidate build receipt does not identify the selected CI run")
    payload = _mapping(jobs_document, "workflow-jobs document")
    jobs = _list(payload.get("jobs"), "jobs")
    total_count = _integer(payload.get("total_count"), "workflow-jobs total_count")
    if total_count != len(jobs) or total_count > 100:
        raise ProvenanceError(
            "workflow-jobs query must be complete and bounded to at most 100 records"
        )
    observed: dict[str, dict[str, object]] = {}
    for raw in jobs:
        job = _mapping(raw, "workflow job")
        name = _nonempty_string(job.get("name"), "workflow job name")
        if name in observed:
            raise ProvenanceError(f"duplicate workflow job name: {name}")
        observed[name] = {
            "id": _positive_integer(job.get("id"), f"workflow job {name} id"),
            "name": name,
            "status": _nonempty_string(job.get("status"), f"workflow job {name} status"),
            "conclusion": _nonempty_string(
                job.get("conclusion"), f"workflow job {name} conclusion"
            ),
            "url": _https_url(job.get("html_url"), f"workflow job {name} URL"),
        }
    required = set(REQUIRED_CI_JOBS)
    if set(observed) != required:
        raise ProvenanceError(
            "CI job set does not exactly match the release requirement: "
            f"expected={sorted(required)}, observed={sorted(observed)}"
        )
    nonpassing = [
        name
        for name, job in observed.items()
        if job["status"] != "completed" or job["conclusion"] != "success"
    ]
    if nonpassing:
        raise ProvenanceError(f"CI jobs did not succeed: {sorted(nonpassing)}")
    receipt: dict[str, object] = {
        "schema_version": "1.1",
        "repository": repository,
        "source_commit": source_commit,
        "source_tree": build["source_tree"],
        "workflow": CI_WORKFLOW_PATH,
        "event": "merge_group",
        "head_branch": selected["head_branch"],
        "status": "completed",
        "conclusion": "success",
        **selected,
        "required_jobs": list(REQUIRED_CI_JOBS),
        "jobs": [observed[name] for name in REQUIRED_CI_JOBS],
        "tested_merge_group": {
            "commit": tested_commit,
            "tree": build["source_tree"],
            "base_ref": build["base_ref"],
            "head_ref": build["head_ref"],
            "pull_request_number": build["pull_request_number"],
        },
        "candidate_artifact": artifact_manifest["artifact"],
        "candidate_build_sha256": _canonical_json_sha256(build),
        "tag_binding_sha256": _canonical_json_sha256(binding),
    }
    verify_ci_status(receipt, repository=repository, source_commit=source_commit)
    return receipt


def verify_ci_status(
    receipt: object,
    *,
    repository: str,
    source_commit: str,
) -> None:
    """Fail unless a CI receipt proves the exact reviewed source commit."""
    _validate_repository(repository)
    _validate_commit(source_commit)
    document = _mapping(receipt, "CI status receipt")
    expected_scalars = {
        "schema_version": "1.1",
        "repository": repository,
        "source_commit": source_commit,
        "workflow": CI_WORKFLOW_PATH,
        "event": "merge_group",
        "status": "completed",
        "conclusion": "success",
    }
    mismatches = [key for key, value in expected_scalars.items() if document.get(key) != value]
    if mismatches:
        raise ProvenanceError(f"CI receipt identity mismatch: {sorted(mismatches)}")
    _validate_git_tree(_nonempty_string(document.get("source_tree"), "CI source tree"))
    head_branch = _nonempty_string(document.get("head_branch"), "CI head branch")
    if not head_branch.startswith("gh-readonly-queue/main/"):
        raise ProvenanceError("CI receipt is not from the main merge queue")
    for key in ("run_id", "run_attempt", "run_number", "workflow_id"):
        _positive_integer(document.get(key), f"CI receipt {key}")
    _https_url(document.get("url"), "CI receipt URL")
    required_jobs = _list(document.get("required_jobs"), "CI receipt required_jobs")
    if required_jobs != list(REQUIRED_CI_JOBS):
        raise ProvenanceError("CI receipt required job list does not match the release contract")
    jobs = _list(document.get("jobs"), "CI receipt jobs")
    if len(jobs) != len(REQUIRED_CI_JOBS):
        raise ProvenanceError("CI receipt job count does not match the release contract")
    names: list[str] = []
    for raw in jobs:
        job = _mapping(raw, "CI receipt job")
        name = _nonempty_string(job.get("name"), "CI receipt job name")
        names.append(name)
        _positive_integer(job.get("id"), f"CI receipt job {name} id")
        _https_url(job.get("url"), f"CI receipt job {name} URL")
        if job.get("status") != "completed" or job.get("conclusion") != "success":
            raise ProvenanceError(f"CI receipt contains a nonpassing job: {name}")
    if names != list(REQUIRED_CI_JOBS) or len(set(names)) != len(names):
        raise ProvenanceError("CI receipt jobs are missing, duplicated, or out of canonical order")
    tested = _mapping(document.get("tested_merge_group"), "tested merge group")
    _validate_commit(_nonempty_string(tested.get("commit"), "tested merge-group commit"))
    tree = _nonempty_string(tested.get("tree"), "tested merge-group tree")
    _validate_git_tree(tree)
    if tree != document.get("source_tree"):
        raise ProvenanceError("CI receipt tree identities differ")
    if tested.get("base_ref") != "refs/heads/main":
        raise ProvenanceError("CI receipt merge group does not target main")
    pull_number = _positive_integer(
        tested.get("pull_request_number"), "CI receipt pull request number"
    )
    head_ref = _nonempty_string(tested.get("head_ref"), "CI receipt merge-group head ref")
    if (
        re.fullmatch(
            rf"refs/heads/gh-readonly-queue/main/pr-{pull_number}-[A-Za-z0-9._/-]+",
            head_ref,
        )
        is None
    ):
        raise ProvenanceError("CI receipt merge-group head ref does not bind its pull request")
    artifact = _mapping(document.get("candidate_artifact"), "CI candidate artifact")
    _positive_integer(artifact.get("id"), "CI candidate artifact id")
    _positive_integer(artifact.get("size_in_bytes"), "CI candidate artifact size")
    digest = _nonempty_string(artifact.get("digest"), "CI candidate artifact digest")
    if re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
        raise ProvenanceError("CI candidate artifact digest is invalid")
    for field in ("candidate_build_sha256", "tag_binding_sha256"):
        value = _nonempty_string(document.get(field), f"CI receipt {field}")
        if re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise ProvenanceError(f"CI receipt {field} is invalid")


def _matrix_pytest_platform_partition(
    report: dict[str, object],
    *,
    path_name: str,
    expected_platform: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return one report's exact native-platform marked-test partition."""
    pytest_checks = [
        check
        for raw in _list(report.get("checks"), f"matrix report checks {path_name}")
        if (check := _mapping(raw, f"matrix report check {path_name}")).get("check_id")
        == "local.pytest"
    ]
    if len(pytest_checks) != 1:
        raise ProvenanceError(
            f"matrix report must contain exactly one local.pytest check: {path_name}"
        )
    command_evidence = [
        evidence
        for raw in _list(
            pytest_checks[0].get("evidence"),
            f"matrix report local.pytest evidence {path_name}",
        )
        if (evidence := _mapping(raw, f"matrix report local.pytest evidence {path_name}")).get(
            "kind"
        )
        == "command"
    ]
    if len(command_evidence) != 1:
        raise ProvenanceError(
            "matrix report local.pytest must contain exactly one command evidence entry: "
            f"{path_name}"
        )
    metadata = _mapping(
        command_evidence[0].get("metadata"),
        f"matrix report local.pytest command metadata {path_name}",
    )
    platform = _nonempty_string(
        metadata.get("pytest_platform"),
        f"matrix report pytest platform {path_name}",
    )
    if platform != expected_platform:
        raise ProvenanceError(f"matrix report pytest platform differs from its job: {path_name}")
    if metadata.get("platform_test_ids_truncated") is not False:
        raise ProvenanceError(f"matrix report pytest platform partition is truncated: {path_name}")
    selected = _canonical_string_list(
        metadata.get("platform_selected_test_ids"),
        f"matrix report selected platform tests {path_name}",
    )
    excluded = _canonical_string_list(
        metadata.get("platform_excluded_test_ids"),
        f"matrix report excluded platform tests {path_name}",
    )
    if set(selected).intersection(excluded):
        raise ProvenanceError(f"matrix report pytest platform partition overlaps: {path_name}")
    return tuple(selected), tuple(excluded)


def _validate_matrix_pytest_platform_partitions(
    *,
    posix_platform_partitions: list[tuple[str, tuple[tuple[str, ...], tuple[str, ...]]]],
    windows_platform_partitions: list[tuple[str, tuple[tuple[str, ...], tuple[str, ...]]]],
) -> None:
    """Require all six jobs to prove one complementary marked-test partition."""
    if len(posix_platform_partitions) != 3 or len(windows_platform_partitions) != 3:
        raise ProvenanceError("matrix reports do not prove all release-platform partitions")
    expected_posix = posix_platform_partitions[0][1]
    expected_windows = windows_platform_partitions[0][1]
    if any(partition != expected_posix for _job, partition in posix_platform_partitions[1:]):
        raise ProvenanceError("POSIX matrix reports disagree on their platform-test partition")
    if any(partition != expected_windows for _job, partition in windows_platform_partitions[1:]):
        raise ProvenanceError("Windows matrix reports disagree on their platform-test partition")
    posix_selected, posix_excluded = expected_posix
    windows_selected, windows_excluded = expected_windows
    if posix_selected != windows_excluded or posix_excluded != windows_selected:
        raise ProvenanceError(
            "Windows and POSIX matrix reports do not prove complementary platform-test partitions"
        )
    if not posix_selected and not windows_selected:
        raise ProvenanceError("matrix reports prove no release-platform marked tests")


def _canonical_string_list(value: object, field: str) -> list[str]:
    """Return an exact canonical set from a duplicate-free JSON string list."""
    items = _string_list(value, field)
    if len(items) != len(set(items)):
        raise ProvenanceError(f"{field} must contain no duplicates")
    return sorted(items)


def build_candidate_build_receipt(
    candidate_dir: Path,
    reports_dir: Path,
    *,
    repository: str,
    source_commit: str,
    source_tree: str,
    event: str,
    run_id: int,
    run_attempt: int,
    head_ref: str,
    base_ref: str,
) -> dict[str, object]:
    """Seal one build and six matrix validations from a merge-queue commit."""
    _validate_repository(repository)
    _validate_commit(source_commit)
    _validate_git_tree(source_tree)
    if event != "merge_group" or base_ref != "refs/heads/main":
        raise ProvenanceError("candidate build must originate from the main merge queue")
    match = re.fullmatch(
        r"refs/heads/gh-readonly-queue/main/pr-([1-9][0-9]*)-[A-Za-z0-9._/-]+",
        head_ref,
    )
    if match is None:
        raise ProvenanceError("candidate build head ref does not identify one queued pull request")
    pull_request_number = int(match.group(1))
    observed_run_id = _positive_integer(run_id, "candidate build run id")
    observed_run_attempt = _positive_integer(run_attempt, "candidate build run attempt")
    try:
        candidate_paths = sorted(candidate_dir.iterdir(), key=lambda path: path.name)
    except OSError as exc:
        raise ProvenanceError(f"could not inspect build-once candidate directory: {exc}") from exc
    wheels = [path for path in candidate_paths if path.name.endswith(".whl")]
    sdists = [path for path in candidate_paths if path.name.endswith(".tar.gz")]
    expected_candidate_names = {
        *(path.name for path in wheels),
        *(path.name for path in sdists),
        "SHA256SUMS",
    }
    if (
        len(wheels) != 1
        or len(sdists) != 1
        or {path.name for path in candidate_paths} != expected_candidate_names
    ):
        raise ProvenanceError(
            "build-once candidate must contain exactly one wheel, one sdist, and SHA256SUMS"
        )
    _verify_checksum_manifest(
        candidate_dir,
        expected_names={wheels[0].name, sdists[0].name},
    )
    distribution_digests = {
        path.name: _sha256_bounded_file(path, maximum_bytes=MAX_DISTRIBUTION_BYTES)
        for path in (wheels[0], sdists[0])
    }
    expected_reports = {
        f"validation-local-{job.split(' / python ')[0]}-{job.rsplit(' ', 1)[1]}.json": job
        for job in REQUIRED_MATRIX_JOBS
    }
    try:
        report_paths = sorted(reports_dir.iterdir(), key=lambda path: path.name)
    except OSError as exc:
        raise ProvenanceError(f"could not inspect matrix report directory: {exc}") from exc
    if {path.name for path in report_paths} != set(expected_reports):
        raise ProvenanceError("matrix validation report set is missing, extra, or renamed")
    reports: list[dict[str, object]] = []
    posix_platform_partitions: list[tuple[str, tuple[tuple[str, ...], tuple[str, ...]]]] = []
    windows_platform_partitions: list[tuple[str, tuple[tuple[str, ...], tuple[str, ...]]]] = []
    for path in report_paths:
        report = _mapping(_load_json(path), f"matrix validation report {path.name}")
        if (
            report.get("status") != "passed"
            or report.get("scenario") != "local-release"
            or report.get("cluster") != "local"
        ):
            raise ProvenanceError(f"matrix validation report did not pass: {path.name}")
        software = _mapping(report.get("software"), f"matrix report software {path.name}")
        if software.get("commit") != source_commit or software.get("dirty") is not False:
            raise ProvenanceError(f"matrix validation report source identity differs: {path.name}")
        resources = _list(report.get("resources"), f"matrix report resources {path.name}")
        observed_digests: dict[str, str] = {}
        for raw in resources:
            resource = _mapping(raw, f"matrix report resource {path.name}")
            name = resource.get("resource_id")
            if name not in distribution_digests:
                continue
            metadata = _mapping(resource.get("metadata"), f"matrix report metadata {path.name}")
            digest = _nonempty_string(
                metadata.get("sha256"), f"matrix report artifact digest {path.name}"
            )
            if name in observed_digests:
                raise ProvenanceError(f"matrix report duplicates a release artifact: {path.name}")
            observed_digests[cast(str, name)] = digest
        if observed_digests != distribution_digests:
            raise ProvenanceError(f"matrix report artifact digests differ: {path.name}")
        job = expected_reports[path.name]
        expected_platform = "windows" if job.startswith("windows-latest") else "posix"
        platform_partition = _matrix_pytest_platform_partition(
            report,
            path_name=path.name,
            expected_platform=expected_platform,
        )
        if expected_platform == "windows":
            windows_platform_partitions.append((job, platform_partition))
        else:
            posix_platform_partitions.append((job, platform_partition))
        checks = {
            _mapping(raw, f"matrix report check {path.name}").get("check_id")
            for raw in _list(report.get("checks"), f"matrix report checks {path.name}")
        }
        primary = job == "ubuntu-latest / python 3.12"
        if primary:
            if "local.build" not in checks or "local.sdist-smoke" not in checks:
                raise ProvenanceError("primary matrix report does not prove the sole build")
            mode = "build"
        else:
            if (
                "local.prebuilt-artifacts" not in checks
                or "local.build" in checks
                or "local.sdist-smoke" in checks
            ):
                raise ProvenanceError(
                    f"matrix report did not use the build-once artifact path: {path.name}"
                )
            mode = "prebuilt"
        reports.append(
            {
                "job": job,
                "filename": path.name,
                "sha256": _sha256_bounded_file(path, maximum_bytes=MAX_FIXED_JSON_BYTES),
                "mode": mode,
                "artifact_sha256": distribution_digests,
            }
        )
    _validate_matrix_pytest_platform_partitions(
        posix_platform_partitions=posix_platform_partitions,
        windows_platform_partitions=windows_platform_partitions,
    )
    reports.sort(key=lambda item: REQUIRED_MATRIX_JOBS.index(cast(str, item["job"])))
    receipt: dict[str, object] = {
        "schema_version": "clio-relay.candidate-build.v1",
        "repository": repository,
        "workflow": CI_WORKFLOW_PATH,
        "event": "merge_group",
        "tested_commit": source_commit,
        "source_tree": source_tree,
        "base_ref": base_ref,
        "head_ref": head_ref,
        "pull_request_number": pull_request_number,
        "run_id": observed_run_id,
        "run_attempt": observed_run_attempt,
        "distribution_sha256": distribution_digests,
        "matrix_reports": reports,
    }
    verify_candidate_build_receipt(receipt, repository=repository)
    return receipt


def verify_candidate_build_receipt(receipt: object, *, repository: str) -> None:
    """Verify the structural identity of a sealed merge-queue candidate build."""
    _validate_repository(repository)
    document = _mapping(receipt, "candidate build receipt")
    expected = {
        "schema_version": "clio-relay.candidate-build.v1",
        "repository": repository,
        "workflow": CI_WORKFLOW_PATH,
        "event": "merge_group",
        "base_ref": "refs/heads/main",
    }
    mismatches = [key for key, value in expected.items() if document.get(key) != value]
    if mismatches:
        raise ProvenanceError(f"candidate build receipt identity mismatch: {mismatches}")
    _validate_commit(_nonempty_string(document.get("tested_commit"), "tested commit"))
    _validate_git_tree(_nonempty_string(document.get("source_tree"), "candidate source tree"))
    pull_number = _positive_integer(
        document.get("pull_request_number"), "candidate pull request number"
    )
    head_ref = _nonempty_string(document.get("head_ref"), "candidate head ref")
    if (
        re.fullmatch(
            rf"refs/heads/gh-readonly-queue/main/pr-{pull_number}-[A-Za-z0-9._/-]+",
            head_ref,
        )
        is None
    ):
        raise ProvenanceError("candidate head ref does not match its pull request number")
    _positive_integer(document.get("run_id"), "candidate run id")
    _positive_integer(document.get("run_attempt"), "candidate run attempt")
    digests = _mapping(document.get("distribution_sha256"), "candidate distribution digests")
    if len(digests) != 2:
        raise ProvenanceError("candidate receipt must identify exactly two distributions")
    for name, digest in digests.items():
        if (
            not (name.endswith(".whl") or name.endswith(".tar.gz"))
            or re.fullmatch(r"[0-9a-f]{64}", _nonempty_string(digest, f"candidate digest {name}"))
            is None
        ):
            raise ProvenanceError(f"candidate distribution digest is invalid: {name}")
    reports = _list(document.get("matrix_reports"), "candidate matrix reports")
    if len(reports) != len(REQUIRED_MATRIX_JOBS):
        raise ProvenanceError("candidate receipt matrix report count differs")
    observed_jobs: list[str] = []
    for raw in reports:
        report = _mapping(raw, "candidate matrix report")
        observed_jobs.append(_nonempty_string(report.get("job"), "candidate matrix job"))
        filename = _nonempty_string(report.get("filename"), "candidate matrix filename")
        if re.fullmatch(r"validation-local-[A-Za-z0-9._-]+\.json", filename) is None:
            raise ProvenanceError("candidate matrix report filename is invalid")
        digest = _nonempty_string(report.get("sha256"), "candidate matrix report digest")
        if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise ProvenanceError("candidate matrix report digest is invalid")
        expected_mode = "build" if report["job"] == REQUIRED_MATRIX_JOBS[0] else "prebuilt"
        if report.get("mode") != expected_mode or report.get("artifact_sha256") != digests:
            raise ProvenanceError("candidate matrix report build mode or artifact digest differs")
    if observed_jobs != list(REQUIRED_MATRIX_JOBS) or len(set(observed_jobs)) != len(observed_jobs):
        raise ProvenanceError("candidate matrix jobs are missing, duplicated, or out of order")


def build_tag_binding(
    candidate_build: object,
    candidate_artifact: object,
    pulls_document: object,
    *,
    repository: str,
    source_commit: str,
    source_tree: str,
    tag: str,
) -> dict[str, object]:
    """Bind a protected release tag to the already tested merge-queue tree."""
    _validate_repository(repository)
    _validate_commit(source_commit)
    _validate_git_tree(source_tree)
    _validate_tag(tag)
    build = _mapping(candidate_build, "candidate build receipt")
    verify_candidate_build_receipt(build, repository=repository)
    if build.get("source_tree") != source_tree:
        raise ProvenanceError("release tag tree differs from the tested merge-group tree")
    artifact = _mapping(candidate_artifact, "candidate artifact manifest")
    _verify_candidate_artifact_manifest(artifact, candidate_build=build, repository=repository)
    pulls = _list(pulls_document, "release commit pull requests")
    if len(pulls) >= 100:
        raise ProvenanceError("release commit pull-request query is not provably complete")
    merge_group_anchor = _positive_integer(
        build.get("pull_request_number"), "candidate merge-group anchor pull request"
    )
    matches: list[dict[str, object]] = []
    for raw in pulls:
        pull = _mapping(raw, "release commit pull request")
        base = _mapping(pull.get("base"), "release pull request base")
        if (
            pull.get("state") == "closed"
            and pull.get("merged_at") is not None
            and pull.get("merge_commit_sha") == source_commit
            and base.get("ref") == "main"
        ):
            matches.append(pull)
    if len(matches) != 1:
        raise ProvenanceError("release commit does not identify the tested merged pull request")
    pull = matches[0]
    merged_pull_number = _positive_integer(pull.get("number"), "release commit pull request number")
    binding: dict[str, object] = {
        "schema_version": "clio-relay.tag-candidate-binding.v1",
        "repository": repository,
        "tag": tag,
        "source_commit": source_commit,
        "source_tree": source_tree,
        "merge_group_anchor_pull_request_number": merge_group_anchor,
        "pull_request": {
            "number": merged_pull_number,
            "merge_commit_sha": source_commit,
            "url": _https_url(pull.get("html_url"), "release pull request URL"),
        },
        "tested_commit": build["tested_commit"],
        "candidate_build_sha256": _canonical_json_sha256(build),
        "candidate_run": {
            "id": artifact["run_id"],
            "attempt": artifact["run_attempt"],
            "head_sha": build["tested_commit"],
            "head_branch": artifact["head_branch"],
        },
        "candidate_artifact": artifact["artifact"],
    }
    verify_tag_binding(binding, repository=repository, source_commit=source_commit)
    return binding


def verify_tag_binding(
    binding: object,
    *,
    repository: str,
    source_commit: str,
) -> None:
    """Verify a release tag's tree, PR, and merge-queue candidate binding."""
    _validate_repository(repository)
    _validate_commit(source_commit)
    document = _mapping(binding, "tag binding")
    if document.get("schema_version") != "clio-relay.tag-candidate-binding.v1":
        raise ProvenanceError("tag binding schema does not match")
    if document.get("repository") != repository or document.get("source_commit") != source_commit:
        raise ProvenanceError("tag binding release identity differs")
    _validate_tag(_nonempty_string(document.get("tag"), "tag binding release tag"))
    _validate_git_tree(_nonempty_string(document.get("source_tree"), "tag binding source tree"))
    _validate_commit(_nonempty_string(document.get("tested_commit"), "tag binding tested commit"))
    merge_group_anchor = _positive_integer(
        document.get("merge_group_anchor_pull_request_number"),
        "tag binding merge-group anchor pull request number",
    )
    build_digest = _nonempty_string(
        document.get("candidate_build_sha256"), "tag binding candidate build digest"
    )
    if re.fullmatch(r"[0-9a-f]{64}", build_digest) is None:
        raise ProvenanceError("tag binding candidate build digest is invalid")
    pull = _mapping(document.get("pull_request"), "tag binding pull request")
    _positive_integer(pull.get("number"), "tag binding pull request number")
    if pull.get("merge_commit_sha") != source_commit:
        raise ProvenanceError("tag binding pull request does not identify the release commit")
    _https_url(pull.get("url"), "tag binding pull request URL")
    artifact = _mapping(document.get("candidate_artifact"), "tag binding candidate artifact")
    _positive_integer(artifact.get("id"), "tag binding candidate artifact id")
    digest = _nonempty_string(artifact.get("digest"), "tag binding candidate artifact digest")
    if re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
        raise ProvenanceError("tag binding candidate artifact digest is invalid")
    run = _mapping(document.get("candidate_run"), "tag binding candidate run")
    _positive_integer(run.get("id"), "tag binding candidate run id")
    _positive_integer(run.get("attempt"), "tag binding candidate run attempt")
    if run.get("head_sha") != document.get("tested_commit"):
        raise ProvenanceError("tag binding candidate run commit differs")
    head_branch = _nonempty_string(run.get("head_branch"), "tag binding candidate head branch")
    if not head_branch.startswith(f"gh-readonly-queue/main/pr-{merge_group_anchor}-"):
        raise ProvenanceError("tag binding candidate run is not from the main merge queue")


def _verify_candidate_artifact_manifest(
    manifest: Mapping[str, object],
    *,
    candidate_build: Mapping[str, object],
    repository: str,
) -> None:
    if (
        manifest.get("schema_version") != "1.1"
        or manifest.get("repository") != repository
        or manifest.get("artifact_kind") != "candidate"
        or manifest.get("source_commit") != candidate_build.get("tested_commit")
        or manifest.get("source_tree") != candidate_build.get("source_tree")
        or manifest.get("run_id") != candidate_build.get("run_id")
        or manifest.get("run_attempt") != candidate_build.get("run_attempt")
        or manifest.get("head_branch")
        != str(candidate_build.get("head_ref", "")).removeprefix("refs/heads/")
    ):
        raise ProvenanceError("candidate artifact manifest differs from its build receipt")
    artifact = _mapping(manifest.get("artifact"), "candidate artifact")
    expected_name = f"release-candidate-{candidate_build['source_tree']}"
    if artifact.get("name") != expected_name:
        raise ProvenanceError("candidate artifact name does not bind the tested tree")


def build_actions_artifact_manifest(
    run_document: object,
    artifacts_document: object,
    *,
    repository: str,
    source_commit: str,
    tag: str,
    run_id: int,
    run_attempt: int,
    artifact_name: str,
    artifact_kind: str,
    source_tree: str | None = None,
) -> dict[str, object]:
    """Bind one nonexpired Actions artifact to its exact trusted workflow run."""
    _validate_repository(repository)
    _validate_commit(source_commit)
    _validate_tag(tag)
    expected_run_id = _positive_integer(run_id, "workflow run id")
    expected_attempt = _positive_integer(run_attempt, "workflow run attempt")
    expected_name = _nonempty_string(artifact_name, "Actions artifact name")
    if artifact_kind not in {"candidate", "tag-binding", "tag-payload", "promotion"}:
        raise ProvenanceError("Actions artifact kind is invalid")
    if artifact_kind == "candidate":
        tree = _nonempty_string(source_tree, "candidate source tree")
        _validate_git_tree(tree)
        required_name = f"release-candidate-{tree}"
    elif artifact_kind == "tag-binding":
        tree = None
        required_name = f"release-binding-{tag}"
    elif artifact_kind == "tag-payload":
        tree = None
        required_name = f"release-candidate-{tag}"
    else:
        tree = None
        required_name = f"verified-release-{tag}"
    if expected_name != required_name:
        raise ProvenanceError("Actions artifact name does not match the release tag")
    run = _mapping(run_document, "workflow run attempt")
    if artifact_kind == "candidate":
        expected_head_branch: str | None = None
        expected_event = "merge_group"
        expected_status = "completed"
        expected_conclusion: str | None = "success"
        expected_path = CI_WORKFLOW_PATH
    elif artifact_kind in {"tag-binding", "tag-payload"}:
        expected_head_branch = tag
        expected_event = "push"
        expected_status = "completed"
        expected_conclusion = "success"
        expected_path = RELEASE_WORKFLOW_PATH
    else:
        expected_head_branch = "main"
        expected_event = "workflow_dispatch"
        expected_status = "in_progress"
        expected_conclusion = None
        expected_path = ".github/workflows/release-gate.yml"
    run_expected = {
        "id": expected_run_id,
        "run_attempt": expected_attempt,
        "head_sha": source_commit,
        "event": expected_event,
        "status": expected_status,
        "conclusion": expected_conclusion,
        "path": expected_path,
    }
    if expected_head_branch is not None:
        run_expected["head_branch"] = expected_head_branch
    run_mismatches = [key for key, value in run_expected.items() if run.get(key) != value]
    if run_mismatches:
        raise ProvenanceError(f"tag-build run identity mismatch: {sorted(run_mismatches)}")
    run_head_branch = _nonempty_string(run.get("head_branch"), "workflow run head branch")
    if artifact_kind == "candidate" and not run_head_branch.startswith("gh-readonly-queue/main/"):
        raise ProvenanceError("candidate artifact run is not a main merge-group run")
    run_started_at = _rfc3339_timestamp(run.get("run_started_at"), "workflow run attempt start")
    repository_id = _positive_integer(
        _mapping(run.get("repository"), "workflow run repository").get("id"),
        "workflow run repository id",
    )
    head_repository_id = _positive_integer(
        _mapping(run.get("head_repository"), "workflow run head repository").get("id"),
        "workflow run head repository id",
    )
    if repository_id != head_repository_id:
        raise ProvenanceError("tag-build run originates from a different repository")

    payload = _mapping(artifacts_document, "workflow run artifacts")
    artifacts = _list(payload.get("artifacts"), "workflow run artifacts")
    total_count = _integer(payload.get("total_count"), "workflow run artifact total_count")
    if total_count != 1 or len(artifacts) != 1:
        raise ProvenanceError(
            "tag-build run must expose exactly one current artifact; "
            f"total_count={total_count}, observed={len(artifacts)}"
        )
    artifact = _mapping(artifacts[0], "workflow run artifact")
    artifact_id = _positive_integer(artifact.get("id"), "Actions artifact id")
    size = _positive_integer(artifact.get("size_in_bytes"), "Actions artifact size")
    if size > MAX_ACTIONS_ARTIFACT_ARCHIVE_BYTES:
        raise ProvenanceError(
            "Actions artifact archive exceeds the byte limit: "
            f"{size} > {MAX_ACTIONS_ARTIFACT_ARCHIVE_BYTES}"
        )
    digest = _nonempty_string(artifact.get("digest"), "Actions artifact digest")
    if re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
        raise ProvenanceError("Actions artifact digest is not a SHA-256 identity")
    if artifact.get("name") != expected_name or artifact.get("expired") is not False:
        raise ProvenanceError("Actions artifact name or expiration state does not match")
    artifact_created_at = _rfc3339_timestamp(
        artifact.get("created_at"), "Actions artifact creation time"
    )
    if artifact_created_at < run_started_at:
        raise ProvenanceError("Actions artifact predates the selected workflow run attempt")
    workflow_run = _mapping(artifact.get("workflow_run"), "Actions artifact workflow run")
    artifact_run_expected = {
        "id": expected_run_id,
        "head_sha": source_commit,
        "head_branch": run_head_branch,
        "repository_id": repository_id,
        "head_repository_id": repository_id,
    }
    artifact_run_mismatches = [
        key for key, value in artifact_run_expected.items() if workflow_run.get(key) != value
    ]
    if artifact_run_mismatches:
        raise ProvenanceError(
            f"Actions artifact run identity mismatch: {sorted(artifact_run_mismatches)}"
        )
    archive_url = _https_url(artifact.get("archive_download_url"), "Actions artifact archive URL")
    expected_url = f"https://api.github.com/repos/{repository}/actions/artifacts/{artifact_id}/zip"
    if archive_url != expected_url:
        raise ProvenanceError("Actions artifact archive URL does not match its repository and id")
    return {
        "schema_version": "1.1" if artifact_kind in {"candidate", "tag-binding"} else "1.0",
        "repository": repository,
        "source_commit": source_commit,
        "source_tree": tree,
        "tag": tag,
        "artifact_kind": artifact_kind,
        "head_branch": run_head_branch,
        "run_id": expected_run_id,
        "run_attempt": expected_attempt,
        "run_started_at": run_started_at.isoformat(),
        "artifact": {
            "id": artifact_id,
            "name": expected_name,
            "size_in_bytes": size,
            "digest": digest,
            "archive_download_url": archive_url,
            "expired": False,
            "created_at": artifact_created_at.isoformat(),
        },
    }


def verify_actions_artifact_archive(
    manifest: object,
    archive_path: Path,
    output_dir: Path,
) -> None:
    """Verify and safely extract the exact inert tag-build payload archive."""
    document = _mapping(manifest, "Actions artifact manifest")
    artifact_kind = _nonempty_string(document.get("artifact_kind"), "Actions artifact kind")
    expected_schema = "1.1" if artifact_kind in {"candidate", "tag-binding"} else "1.0"
    if document.get("schema_version") != expected_schema:
        raise ProvenanceError("Actions artifact manifest schema does not match")
    _validate_repository(_nonempty_string(document.get("repository"), "artifact repository"))
    _validate_commit(_nonempty_string(document.get("source_commit"), "artifact source commit"))
    _validate_tag(_nonempty_string(document.get("tag"), "artifact release tag"))
    if artifact_kind not in {"candidate", "tag-binding", "tag-payload", "promotion"}:
        raise ProvenanceError("Actions artifact manifest kind is invalid")
    _positive_integer(document.get("run_id"), "artifact workflow run id")
    _positive_integer(document.get("run_attempt"), "artifact workflow run attempt")
    run_started_at = _rfc3339_timestamp(
        document.get("run_started_at"), "artifact workflow run attempt start"
    )
    artifact = _mapping(document.get("artifact"), "Actions artifact manifest entry")
    _positive_integer(artifact.get("id"), "Actions artifact id")
    expected_size = _positive_integer(artifact.get("size_in_bytes"), "Actions artifact size")
    if expected_size > MAX_ACTIONS_ARTIFACT_ARCHIVE_BYTES:
        raise ProvenanceError("Actions artifact manifest size exceeds the byte limit")
    digest = _nonempty_string(artifact.get("digest"), "Actions artifact digest")
    if re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
        raise ProvenanceError("Actions artifact manifest digest is invalid")
    artifact_created_at = _rfc3339_timestamp(
        artifact.get("created_at"), "Actions artifact creation time"
    )
    if artifact_created_at < run_started_at:
        raise ProvenanceError("Actions artifact manifest replays an earlier run attempt")
    try:
        details = archive_path.lstat()
    except OSError as exc:
        raise ProvenanceError(f"could not inspect Actions artifact archive: {exc}") from exc
    if archive_path.is_symlink() or not stat.S_ISREG(details.st_mode):
        raise ProvenanceError("Actions artifact archive is not a regular file")
    if details.st_size != expected_size:
        raise ProvenanceError(
            "Actions artifact archive size differs from API metadata: "
            f"expected={expected_size}, observed={details.st_size}"
        )
    observed_digest = _sha256_bounded_file(
        archive_path,
        maximum_bytes=MAX_ACTIONS_ARTIFACT_ARCHIVE_BYTES,
    )
    if f"sha256:{observed_digest}" != digest:
        raise ProvenanceError("Actions artifact archive digest differs from API metadata")
    try:
        existing = list(output_dir.iterdir()) if output_dir.exists() else []
    except OSError as exc:
        raise ProvenanceError(f"could not inspect artifact output directory: {exc}") from exc
    if existing:
        raise ProvenanceError("Actions artifact output directory must be empty")
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(archive_path) as archive:
            members = archive.infolist()
            names = [item.filename for item in members]
            if len(names) != len(set(names)):
                raise ProvenanceError("Actions artifact archive contains duplicate paths")
            if artifact_kind == "candidate":
                _validate_candidate_payload_names(names)
            elif artifact_kind == "tag-payload":
                _validate_tag_payload_names(names)
            elif artifact_kind == "tag-binding":
                _validate_tag_binding_payload_names(names)
            else:
                _validate_promotion_payload_names(names)
            aggregate_size = 0
            for member in members:
                maximum = (
                    _tag_payload_file_limit(member.filename)
                    if artifact_kind in {"candidate", "tag-payload"}
                    else _promotion_payload_file_limit(member.filename)
                )
                if artifact_kind == "tag-binding":
                    maximum = MAX_FIXED_JSON_BYTES
                if member.flag_bits & 0x1:
                    raise ProvenanceError("Actions artifact archive contains encrypted content")
                mode = member.external_attr >> 16
                file_type = stat.S_IFMT(mode)
                if file_type not in {0, stat.S_IFREG}:
                    raise ProvenanceError(
                        f"tag payload archive member is not regular: {member.filename}"
                    )
                if member.file_size < 1 or member.file_size > maximum:
                    raise ProvenanceError(
                        f"tag payload file size is invalid for {member.filename}: "
                        f"{member.file_size}"
                    )
                aggregate_size += member.file_size
                aggregate_limit = (
                    2 * MAX_DISTRIBUTION_BYTES + MAX_FIXED_JSON_BYTES + MAX_MANIFEST_BYTES
                    if artifact_kind == "tag-payload"
                    else MAX_RELEASE_ASSET_AGGREGATE_BYTES
                )
                if aggregate_size > aggregate_limit:
                    raise ProvenanceError("tag payload uncompressed aggregate exceeds the limit")
            for member in members:
                destination = output_dir / member.filename
                destination.parent.mkdir(parents=True, exist_ok=True)
                written = 0
                with archive.open(member, "r") as source, destination.open("xb") as target:
                    while True:
                        chunk = source.read(1024 * 1024)
                        if not chunk:
                            break
                        written += len(chunk)
                        if written > member.file_size:
                            raise ProvenanceError(
                                f"tag payload expanded past declared size: {member.filename}"
                            )
                        target.write(chunk)
                    target.flush()
                    os.fsync(target.fileno())
                if written != member.file_size:
                    raise ProvenanceError(
                        f"tag payload size changed during extraction: {member.filename}"
                    )
    except (OSError, zipfile.BadZipFile) as exc:
        raise ProvenanceError(f"could not verify Actions artifact archive: {exc}") from exc
    if artifact_kind in {"candidate", "tag-payload"}:
        _verify_checksum_manifest(
            output_dir,
            expected_names={
                item.name for item in output_dir.iterdir() if item.name != "SHA256SUMS"
            },
        )
    if artifact_kind == "candidate":
        _verify_extracted_candidate_payload(output_dir, document)


def _verify_extracted_candidate_payload(
    directory: Path,
    artifact_manifest: Mapping[str, object],
) -> None:
    """Bind extracted build-once bytes to their tested merge-group receipt."""
    repository = _nonempty_string(
        artifact_manifest.get("repository"), "candidate artifact repository"
    )
    candidate_build = _mapping(
        _load_json(directory / "CANDIDATE-BUILD.json"),
        "extracted candidate build receipt",
    )
    verify_candidate_build_receipt(candidate_build, repository=repository)
    expected_identity = {
        "tested_commit": artifact_manifest.get("source_commit"),
        "source_tree": artifact_manifest.get("source_tree"),
        "run_id": artifact_manifest.get("run_id"),
        "run_attempt": artifact_manifest.get("run_attempt"),
    }
    mismatches = [
        key for key, value in expected_identity.items() if candidate_build.get(key) != value
    ]
    if mismatches:
        raise ProvenanceError(
            "extracted candidate build identity differs from its Actions artifact: "
            f"{sorted(mismatches)}"
        )
    distributions = sorted(
        (
            path
            for path in directory.iterdir()
            if path.name.endswith(".whl") or path.name.endswith(".tar.gz")
        ),
        key=lambda path: path.name,
    )
    observed_digests = {
        path.name: _sha256_bounded_file(path, maximum_bytes=MAX_DISTRIBUTION_BYTES)
        for path in distributions
    }
    if observed_digests != candidate_build.get("distribution_sha256"):
        raise ProvenanceError("extracted candidate distributions differ from the build receipt")
    report_path = directory / "validation-local.json"
    primary_reports = [
        _mapping(raw, "candidate primary matrix report")
        for raw in _list(candidate_build.get("matrix_reports"), "candidate matrix reports")
        if _mapping(raw, "candidate matrix report").get("job") == REQUIRED_MATRIX_JOBS[0]
    ]
    if len(primary_reports) != 1:
        raise ProvenanceError("candidate build receipt does not identify one primary report")
    primary = primary_reports[0]
    if primary.get("sha256") != _sha256_bounded_file(
        report_path,
        maximum_bytes=MAX_FIXED_JSON_BYTES,
    ):
        raise ProvenanceError("extracted primary validation report differs from the build receipt")
    report = _mapping(_load_json(report_path), "extracted primary validation report")
    if (
        report.get("status") != "passed"
        or report.get("scenario") != "local-release"
        or report.get("cluster") != "local"
    ):
        raise ProvenanceError("extracted primary validation report did not pass")
    software = _mapping(report.get("software"), "extracted primary report software")
    if (
        software.get("commit") != candidate_build.get("tested_commit")
        or software.get("dirty") is not False
    ):
        raise ProvenanceError("extracted primary validation report source identity differs")
    report_digests: dict[str, str] = {}
    for raw in _list(report.get("resources"), "extracted primary report resources"):
        resource = _mapping(raw, "extracted primary report resource")
        name = resource.get("resource_id")
        if name not in observed_digests:
            continue
        metadata = _mapping(resource.get("metadata"), "extracted primary report metadata")
        if name in report_digests:
            raise ProvenanceError("extracted primary report duplicates a distribution")
        report_digests[cast(str, name)] = _nonempty_string(
            metadata.get("sha256"), "extracted primary report distribution digest"
        )
    if report_digests != observed_digests:
        raise ProvenanceError("extracted primary report distribution digests differ")
    checks = {
        _mapping(raw, "extracted primary report check").get("check_id")
        for raw in _list(report.get("checks"), "extracted primary report checks")
    }
    if "local.build" not in checks or "local.sdist-smoke" not in checks:
        raise ProvenanceError("extracted primary report does not prove the sole build")


def build_distribution_archive_receipt(
    wheel_path: Path,
    sdist_path: Path,
    *,
    project: str,
    version: str,
) -> dict[str, object]:
    """Safely inspect exact wheel and sdist bytes without executing package code."""
    canonical_project = _canonical_distribution_name(project)
    if canonical_project != "clio-relay":
        raise ProvenanceError("distribution project identity must be clio-relay")
    if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[.-][0-9A-Za-z.-]+)?", version) is None:
        raise ProvenanceError("distribution version is not a supported release version")
    wheel = _inspect_wheel_archive(
        wheel_path,
        expected_project=canonical_project,
        expected_version=version,
    )
    sdist = _inspect_sdist_archive(
        sdist_path,
        expected_project=canonical_project,
        expected_version=version,
    )
    return {
        "schema_version": "clio-relay.distribution-archives.v1",
        "project": canonical_project,
        "version": version,
        "limits": {
            "maximum_archive_bytes": MAX_DISTRIBUTION_BYTES,
            "maximum_members": MAX_DISTRIBUTION_MEMBERS,
            "maximum_member_bytes": MAX_DISTRIBUTION_MEMBER_BYTES,
            "maximum_uncompressed_bytes": MAX_DISTRIBUTION_UNCOMPRESSED_BYTES,
            "maximum_uncompressed_tar_bytes": MAX_DISTRIBUTION_TAR_BYTES,
            "maximum_metadata_bytes": MAX_DISTRIBUTION_METADATA_BYTES,
            "maximum_path_length": MAX_DISTRIBUTION_PATH_LENGTH,
        },
        "wheel": wheel,
        "sdist": sdist,
    }


def build_exact_release_asset_inventory(
    document: object,
    expected_paths: Sequence[Path],
    *,
    next_page_document: object,
    page_size: int,
) -> dict[str, object]:
    """Bind a complete live release asset inventory to exact local file bytes."""
    release = _mapping(document, "exact release asset document")
    raw_assets = _list(release.get("assets"), "exact release assets")
    next_page = _list(next_page_document, "exact release asset next page")
    if not MAX_RELEASE_ASSET_METADATA_RECORDS < page_size <= 100:
        raise ProvenanceError(
            "release asset API page size must exceed the configured asset count and be at most 100"
        )
    if len(raw_assets) > page_size:
        raise ProvenanceError("first release asset API page exceeds its requested size")
    if not raw_assets or len(raw_assets) > MAX_RELEASE_ASSET_METADATA_RECORDS:
        raise ProvenanceError(
            "exact release asset count must be between one and "
            f"{MAX_RELEASE_ASSET_METADATA_RECORDS}"
        )
    if next_page:
        raise ProvenanceError(
            "release asset API has a non-empty next page beyond the configured asset count"
        )

    expected: dict[str, dict[str, object]] = {}
    expected_total = 0
    for path in expected_paths:
        name = path.name
        _validate_release_asset_name(name)
        if name in expected:
            raise ProvenanceError(f"expected release asset path is duplicated: {name}")
        maximum = _release_asset_file_limit(name)
        try:
            details = path.lstat()
        except OSError as exc:
            raise ProvenanceError(
                f"could not inspect expected release asset {path}: {exc}"
            ) from exc
        if path.is_symlink() or not stat.S_ISREG(details.st_mode):
            raise ProvenanceError(f"expected release asset is not regular: {path}")
        if details.st_size < 1 or details.st_size > maximum:
            raise ProvenanceError(f"expected release asset size is invalid: {name}")
        expected_total += details.st_size
        if expected_total > MAX_RELEASE_ASSET_AGGREGATE_BYTES:
            raise ProvenanceError("expected release assets exceed the aggregate byte limit")
        expected[name] = {
            "name": name,
            "size": details.st_size,
            "digest": f"sha256:{_sha256_bounded_file(path, maximum_bytes=maximum)}",
        }
    if not expected:
        raise ProvenanceError("expected release asset path set is empty")

    observed: dict[str, dict[str, object]] = {}
    observed_ids: set[int] = set()
    observed_total = 0
    for raw in raw_assets:
        asset = _mapping(raw, "exact live release asset")
        name = _nonempty_string(asset.get("name"), "exact live release asset name")
        _validate_release_asset_name(name)
        if name in observed:
            raise ProvenanceError(f"live release asset is duplicated: {name}")
        maximum = _release_asset_file_limit(name)
        size = _positive_integer(asset.get("size"), f"live release asset {name} size")
        if size > maximum:
            raise ProvenanceError(f"live release asset exceeds its byte limit: {name}")
        digest = _nonempty_string(asset.get("digest"), f"live release asset {name} digest")
        if re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
            raise ProvenanceError(f"live release asset digest is invalid: {name}")
        observed_total += size
        if observed_total > MAX_RELEASE_ASSET_AGGREGATE_BYTES:
            raise ProvenanceError("live release assets exceed the aggregate byte limit")
        asset_id = _positive_integer(asset.get("id"), f"live release asset {name} id")
        if asset_id in observed_ids:
            raise ProvenanceError(f"live release asset id is duplicated: {asset_id}")
        observed_ids.add(asset_id)
        observed[name] = {
            "id": asset_id,
            "name": name,
            "size": size,
            "digest": digest,
        }

    comparable_observed = {
        name: {key: item[key] for key in ("name", "size", "digest")}
        for name, item in observed.items()
    }
    if comparable_observed != expected:
        raise ProvenanceError(
            "live release assets differ from exact local files: "
            f"expected={sorted(expected)}, observed={sorted(observed)}"
        )
    if observed_total != expected_total:
        raise ProvenanceError("live and expected release asset aggregate sizes differ")
    return {
        "schema_version": "clio-relay.exact-release-assets.v1",
        "api_pagination": {
            "page_size": page_size,
            "pages_requested": [1, 2],
            "first_page_count": len(raw_assets),
            "next_page_count": len(next_page),
            "maximum_asset_count": MAX_RELEASE_ASSET_METADATA_RECORDS,
        },
        "release_asset_count": len(observed),
        "release_asset_aggregate_bytes": observed_total,
        "release_assets": [observed[name] for name in sorted(observed)],
    }


def verify_exact_release_asset_inventory(
    receipt: object,
    document: object,
    expected_paths: Sequence[Path],
    *,
    next_page_document: object,
    page_size: int,
) -> None:
    """Require current live asset IDs and bytes to equal a prior exact inventory."""
    expected_receipt = _mapping(receipt, "exact release asset receipt")
    current = build_exact_release_asset_inventory(
        document,
        expected_paths,
        next_page_document=next_page_document,
        page_size=page_size,
    )
    if current != expected_receipt:
        raise ProvenanceError(
            "current release asset inventory differs from the prepublication receipt"
        )


def _inspect_wheel_archive(
    path: Path,
    *,
    expected_project: str,
    expected_version: str,
) -> dict[str, object]:
    subject = _distribution_subject(path, kind="wheel")
    seen: set[str] = set()
    files: set[str] = set()
    directories: set[str] = set()
    metadata_documents: dict[str, bytes] = {}
    aggregate = 0
    member_count = 0
    try:
        declared_members = _preflight_zip_member_count(path)
        with zipfile.ZipFile(path) as archive:
            for member in archive.infolist():
                member_count += 1
                if member_count > MAX_DISTRIBUTION_MEMBERS:
                    raise ProvenanceError("wheel member count exceeds the limit")
                is_directory = member.is_dir()
                normalized = _validate_distribution_member_path(
                    member.filename,
                    is_directory=is_directory,
                )
                folded = normalized.casefold()
                if folded in seen:
                    raise ProvenanceError(f"wheel contains a duplicate path: {normalized}")
                seen.add(folded)
                mode = member.external_attr >> 16
                file_type = stat.S_IFMT(mode)
                if is_directory:
                    if file_type not in {0, stat.S_IFDIR} or member.file_size != 0:
                        raise ProvenanceError(f"wheel directory member is invalid: {normalized}")
                    directories.add(normalized)
                    continue
                if file_type not in {0, stat.S_IFREG}:
                    raise ProvenanceError(f"wheel member is not regular: {normalized}")
                if member.flag_bits & 0x1:
                    raise ProvenanceError(f"wheel member is encrypted: {normalized}")
                if member.file_size < 0 or member.file_size > MAX_DISTRIBUTION_MEMBER_BYTES:
                    raise ProvenanceError(f"wheel member exceeds the byte limit: {normalized}")
                aggregate += member.file_size
                if aggregate > MAX_DISTRIBUTION_UNCOMPRESSED_BYTES:
                    raise ProvenanceError("wheel uncompressed aggregate exceeds the limit")
                files.add(normalized)
                capture = normalized.endswith(".dist-info/METADATA")
                with archive.open(member, "r") as stream:
                    content = _read_declared_archive_member(
                        stream,
                        declared_size=member.file_size,
                        label=f"wheel member {normalized}",
                        capture=capture,
                    )
                if capture:
                    metadata_documents[normalized] = content
    except ProvenanceError:
        raise
    except (OSError, RuntimeError, zipfile.BadZipFile, NotImplementedError) as exc:
        raise ProvenanceError(f"could not safely inspect wheel archive: {exc}") from exc
    if member_count == 0:
        raise ProvenanceError("wheel archive contains no members")
    if member_count != declared_members:
        raise ProvenanceError("wheel central-directory member count changed while reading")
    _verify_distribution_path_topology(files, directories, kind="wheel")
    if len(metadata_documents) != 1:
        raise ProvenanceError("wheel must contain exactly one dist-info/METADATA")
    metadata_path, metadata = next(iter(metadata_documents.items()))
    if metadata_path.count("/") != 1:
        raise ProvenanceError("wheel dist-info metadata must be at archive top level")
    if not 1 <= len(metadata) <= MAX_DISTRIBUTION_METADATA_BYTES:
        raise ProvenanceError("wheel metadata size is invalid")
    dist_info = metadata_path.rsplit("/", 1)[0]
    expected_dist_info = (
        f"{expected_project.replace('-', '_')}-{expected_version.replace('-', '_')}.dist-info"
    )
    if dist_info != expected_dist_info:
        raise ProvenanceError("wheel dist-info directory identity does not match the release")
    wheel_prefix = expected_dist_info.removesuffix(".dist-info") + "-"
    if not path.name.startswith(wheel_prefix) or not path.name.endswith(".whl"):
        raise ProvenanceError("wheel filename identity does not match its metadata")
    for required in (f"{dist_info}/WHEEL", f"{dist_info}/RECORD"):
        if required not in files:
            raise ProvenanceError(f"wheel is missing required metadata member: {required}")
    _verify_core_metadata(
        metadata,
        expected_project=expected_project,
        expected_version=expected_version,
        label="wheel",
    )
    return {
        **subject,
        "member_count": member_count,
        "uncompressed_bytes": aggregate,
        "metadata_path": metadata_path,
    }


def _inspect_sdist_archive(
    path: Path,
    *,
    expected_project: str,
    expected_version: str,
) -> dict[str, object]:
    subject = _distribution_subject(path, kind="sdist")
    seen: set[str] = set()
    files: set[str] = set()
    directories: set[str] = set()
    roots: set[str] = set()
    metadata_documents: dict[str, bytes] = {}
    aggregate = 0
    member_count = 0
    try:
        with tempfile.TemporaryFile(mode="w+b") as bounded_tar:
            bounded_stream = cast(BinaryIO, bounded_tar)
            _inflate_sdist_to_bounded_tar(path, bounded_stream)
            with tarfile.open(fileobj=bounded_stream, mode="r:") as archive:
                for member in archive:
                    member_count += 1
                    if member_count > MAX_DISTRIBUTION_MEMBERS:
                        raise ProvenanceError("sdist member count exceeds the limit")
                    is_directory = member.isdir()
                    normalized = _validate_distribution_member_path(
                        member.name,
                        is_directory=is_directory,
                    )
                    folded = normalized.casefold()
                    if folded in seen:
                        raise ProvenanceError(f"sdist contains a duplicate path: {normalized}")
                    seen.add(folded)
                    roots.add(normalized.split("/", 1)[0])
                    if is_directory:
                        directories.add(normalized)
                        continue
                    if not member.isreg():
                        raise ProvenanceError(f"sdist member is not regular: {normalized}")
                    if member.size < 0 or member.size > MAX_DISTRIBUTION_MEMBER_BYTES:
                        raise ProvenanceError(f"sdist member exceeds the byte limit: {normalized}")
                    aggregate += member.size
                    if aggregate > MAX_DISTRIBUTION_UNCOMPRESSED_BYTES:
                        raise ProvenanceError("sdist uncompressed aggregate exceeds the limit")
                    files.add(normalized)
                    stream = archive.extractfile(member)
                    if stream is None:
                        raise ProvenanceError(f"could not read regular sdist member: {normalized}")
                    capture = normalized.count("/") == 1 and normalized.endswith("/PKG-INFO")
                    with stream:
                        content = _read_declared_archive_member(
                            stream,
                            declared_size=member.size,
                            label=f"sdist member {normalized}",
                            capture=capture,
                        )
                    if capture:
                        metadata_documents[normalized] = content
    except ProvenanceError:
        raise
    except (OSError, EOFError, tarfile.TarError) as exc:
        raise ProvenanceError(f"could not safely inspect sdist archive: {exc}") from exc
    if member_count == 0:
        raise ProvenanceError("sdist archive contains no members")
    if len(roots) != 1:
        raise ProvenanceError("sdist must contain exactly one top-level directory")
    _verify_distribution_path_topology(files, directories, kind="sdist")
    if len(metadata_documents) != 1:
        raise ProvenanceError("sdist must contain exactly one top-level PKG-INFO")
    metadata_path, metadata = next(iter(metadata_documents.items()))
    if not 1 <= len(metadata) <= MAX_DISTRIBUTION_METADATA_BYTES:
        raise ProvenanceError("sdist metadata size is invalid")
    root = next(iter(roots))
    if root != f"{expected_project.replace('-', '_')}-{expected_version}":
        raise ProvenanceError("sdist top-level directory identity does not match the release")
    if path.name != f"{root}.tar.gz":
        raise ProvenanceError("sdist filename identity does not match its top-level directory")
    if f"{root}/pyproject.toml" not in files:
        raise ProvenanceError("sdist is missing its top-level pyproject.toml")
    _verify_core_metadata(
        metadata,
        expected_project=expected_project,
        expected_version=expected_version,
        label="sdist",
    )
    return {
        **subject,
        "member_count": member_count,
        "uncompressed_bytes": aggregate,
        "metadata_path": metadata_path,
        "top_level_directory": root,
    }


def _inflate_sdist_to_bounded_tar(path: Path, target: BinaryIO) -> None:
    """Inflate a gzip sdist into a private file without crossing the tar ceiling."""
    written = 0
    try:
        with gzip.open(path, "rb") as source:
            while True:
                remaining = MAX_DISTRIBUTION_TAR_BYTES - written
                chunk = source.read(min(1024 * 1024, remaining + 1))
                if not chunk:
                    break
                written += len(chunk)
                if written > MAX_DISTRIBUTION_TAR_BYTES:
                    raise ProvenanceError("sdist uncompressed tar stream exceeds the byte limit")
                if target.write(chunk) != len(chunk):
                    raise ProvenanceError("sdist temporary tar write was incomplete")
        if written < 1:
            raise ProvenanceError("sdist uncompressed tar stream is empty")
        target.flush()
        os.fsync(target.fileno())
        target.seek(0)
    except ProvenanceError:
        raise
    except (EOFError, gzip.BadGzipFile, OSError) as exc:
        raise ProvenanceError(f"could not safely inflate sdist archive: {exc}") from exc


def _distribution_subject(path: Path, *, kind: str) -> dict[str, object]:
    try:
        details = path.lstat()
    except OSError as exc:
        raise ProvenanceError(f"could not inspect {kind} archive: {exc}") from exc
    if path.is_symlink() or not stat.S_ISREG(details.st_mode):
        raise ProvenanceError(f"{kind} archive is not a regular file")
    if details.st_size < 1 or details.st_size > MAX_DISTRIBUTION_BYTES:
        raise ProvenanceError(f"{kind} archive compressed size is invalid")
    return {
        "filename": path.name,
        "size_bytes": details.st_size,
        "sha256": _sha256_bounded_file(path, maximum_bytes=MAX_DISTRIBUTION_BYTES),
    }


def _preflight_zip_member_count(path: Path) -> int:
    """Reject oversized or ZIP64 central directories before ``ZipFile`` allocates them."""
    size = path.stat().st_size
    tail_size = min(size, 65557)
    with path.open("rb") as stream:
        stream.seek(size - tail_size)
        tail = stream.read(tail_size)
    marker = tail.rfind(b"PK\x05\x06")
    if marker < 0 or len(tail) - marker < 22:
        raise ProvenanceError("wheel end-of-central-directory record is missing")
    (
        signature,
        disk_number,
        central_disk,
        disk_entries,
        total_entries,
        central_size,
        central_offset,
        comment_size,
    ) = struct.unpack_from("<4s4H2LH", tail, marker)
    if signature != b"PK\x05\x06" or marker + 22 + comment_size != len(tail):
        raise ProvenanceError("wheel end-of-central-directory record is invalid")
    if disk_number != 0 or central_disk != 0 or disk_entries != total_entries:
        raise ProvenanceError("wheel uses unsupported multi-disk ZIP topology")
    if total_entries in {0, 0xFFFF} or total_entries > MAX_DISTRIBUTION_MEMBERS:
        raise ProvenanceError("wheel central-directory member count exceeds the limit")
    if central_size == 0xFFFFFFFF or central_offset == 0xFFFFFFFF:
        raise ProvenanceError("wheel uses unsupported ZIP64 topology")
    if central_size > size or central_offset > size or central_offset + central_size > size:
        raise ProvenanceError("wheel central-directory bounds are invalid")
    return total_entries


def _validate_distribution_member_path(name: str, *, is_directory: bool) -> str:
    if not name or len(name) > MAX_DISTRIBUTION_PATH_LENGTH:
        raise ProvenanceError("distribution archive member path is empty or too long")
    has_control = any(ord(char) < 32 or ord(char) == 127 for char in name)
    if "\\" in name or name.startswith("/") or has_control:
        raise ProvenanceError(f"distribution archive member path is unsafe: {name!r}")
    normalized = name[:-1] if is_directory and name.endswith("/") else name
    if not normalized or (not is_directory and name.endswith("/")):
        raise ProvenanceError(f"distribution archive member path is invalid: {name!r}")
    parts = normalized.split("/")
    if any(part in {"", ".", ".."} for part in parts) or ":" in parts[0]:
        raise ProvenanceError(f"distribution archive member path is unsafe: {name!r}")
    return normalized


def _verify_distribution_path_topology(
    files: set[str],
    directories: set[str],
    *,
    kind: str,
) -> None:
    for path in files | directories:
        parts = path.split("/")
        for index in range(1, len(parts)):
            parent = "/".join(parts[:index])
            if parent in files:
                raise ProvenanceError(f"{kind} path traverses a regular-file parent: {path}")


def _read_declared_archive_member(
    stream: _BinaryReader,
    *,
    declared_size: int,
    label: str,
    capture: bool,
) -> bytes:
    if declared_size < 0:
        raise ProvenanceError(f"{label} has a negative declared size")
    content = bytearray()
    observed = 0
    while True:
        chunk = stream.read(min(1024 * 1024, declared_size + 1 - observed))
        if not chunk:
            break
        observed += len(chunk)
        if observed > declared_size:
            raise ProvenanceError(f"{label} expanded past its declared size")
        if capture:
            if observed > MAX_DISTRIBUTION_METADATA_BYTES:
                raise ProvenanceError(f"{label} metadata exceeds the byte limit")
            content.extend(chunk)
    if observed != declared_size:
        raise ProvenanceError(f"{label} size changed while reading")
    return bytes(content)


def _verify_core_metadata(
    content: bytes,
    *,
    expected_project: str,
    expected_version: str,
    label: str,
) -> None:
    metadata = email.parser.BytesParser(policy=email.policy.default).parsebytes(content)
    names = metadata.get_all("Name", [])
    versions = metadata.get_all("Version", [])
    if len(names) != 1 or len(versions) != 1:
        raise ProvenanceError(f"{label} core metadata identity is missing or duplicated")
    if (
        _canonical_distribution_name(str(names[0])) != expected_project
        or str(versions[0]) != expected_version
    ):
        raise ProvenanceError(f"{label} core metadata identity does not match the release")


def _canonical_distribution_name(value: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9._-]+", value) is None:
        raise ProvenanceError("distribution project name is invalid")
    return re.sub(r"[-_.]+", "-", value).lower()


def write_candidate_checksum_manifest(candidate_dir: Path) -> None:
    """Replace the inert payload manifest with canonical protected-main checksums."""
    try:
        paths = list(candidate_dir.iterdir())
    except OSError as exc:
        raise ProvenanceError(f"could not inspect candidate directory: {exc}") from exc
    names = {path.name for path in paths}
    wheels = sorted(name for name in names if name.endswith(".whl"))
    sdists = sorted(name for name in names if name.endswith(".tar.gz"))
    expected = {
        *wheels,
        *sdists,
        "validation-local.json",
        "CI-STATUS.json",
        "REPOSITORY-GOVERNANCE.json",
        "SHA256SUMS",
    }
    if len(wheels) != 1 or len(sdists) != 1 or names != expected:
        raise ProvenanceError("candidate directory file set does not match the release contract")
    limits = {
        wheels[0]: MAX_DISTRIBUTION_BYTES,
        sdists[0]: MAX_DISTRIBUTION_BYTES,
        "validation-local.json": MAX_FIXED_JSON_BYTES,
        "CI-STATUS.json": MAX_FIXED_JSON_BYTES,
        "REPOSITORY-GOVERNANCE.json": MAX_FIXED_JSON_BYTES,
    }
    lines: list[str] = []
    for name in sorted(limits):
        path = candidate_dir / name
        details = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(details.st_mode):
            raise ProvenanceError(f"candidate subject is not a regular file: {name}")
        if details.st_size < 1 or details.st_size > limits[name]:
            raise ProvenanceError(f"candidate subject size is invalid: {name}")
        lines.append(f"{_sha256_bounded_file(path, maximum_bytes=limits[name])} *{name}")
    encoded = "\n".join(lines) + "\n"
    if len(encoded.encode("utf-8")) > MAX_MANIFEST_BYTES:
        raise ProvenanceError("candidate checksum manifest exceeds the byte limit")
    manifest = candidate_dir / "SHA256SUMS"
    temporary = candidate_dir / f".SHA256SUMS.{uuid4().hex}.tmp"
    try:
        temporary.write_text(encoded, encoding="utf-8", newline="\n")
        os.replace(temporary, manifest)
    finally:
        temporary.unlink(missing_ok=True)


def build_repository_governance(
    main_effective_rules: object,
    protected_branches: object,
    branch_rulesets: object,
    tag_rulesets: object,
    environments: Mapping[str, object],
    immutable_releases: object,
    *,
    repository: str,
    source_commit: str,
    tag: str,
) -> dict[str, object]:
    """Build a deterministic receipt for enforced main/tag release governance."""
    _validate_repository(repository)
    _validate_commit(source_commit)
    _validate_tag(tag)
    branch = _main_ruleset_protection_receipt(main_effective_rules, branch_rulesets)
    protected_branch_names = _protected_branch_names(protected_branches)
    tags = _tag_protection_receipts(tag_rulesets)
    environment_receipts = _environment_receipts(environments)
    immutable_receipt = _immutable_releases_receipt(immutable_releases)
    receipt: dict[str, object] = {
        "schema_version": "1.1",
        "repository": repository,
        "source_commit": source_commit,
        "tag": tag,
        "main_branch": "main",
        "protected_branches": protected_branch_names,
        "main_protection": branch,
        "tag_pattern": RELEASE_TAG_PATTERN,
        "tag_protections": tags,
        "environment_reviewers_available": False,
        "environments": environment_receipts,
        "immutable_releases": immutable_receipt,
    }
    verify_repository_governance(
        receipt,
        repository=repository,
        source_commit=source_commit,
        tag=tag,
    )
    return receipt


def verify_repository_governance(
    receipt: object,
    *,
    repository: str,
    source_commit: str,
    tag: str,
) -> None:
    """Fail unless a governance receipt proves the required live controls."""
    _validate_repository(repository)
    _validate_commit(source_commit)
    _validate_tag(tag)
    document = _mapping(receipt, "repository governance receipt")
    expected = {
        "schema_version": "1.1",
        "repository": repository,
        "source_commit": source_commit,
        "tag": tag,
        "main_branch": "main",
        "protected_branches": ["main"],
        "tag_pattern": RELEASE_TAG_PATTERN,
        "environment_reviewers_available": False,
    }
    mismatches = [key for key, value in expected.items() if document.get(key) != value]
    if mismatches:
        raise ProvenanceError(
            f"repository governance receipt identity mismatch: {sorted(mismatches)}"
        )
    _verify_main_protection(_mapping(document.get("main_protection"), "main protection"))
    tag_protections = _list(document.get("tag_protections"), "tag protections")
    if not tag_protections:
        raise ProvenanceError("repository governance receipt has no enforced tag protection")
    for raw in tag_protections:
        _verify_tag_protection(_mapping(raw, "tag protection"))
    environment_receipts = _list(document.get("environments"), "environments")
    if [_mapping(item, "environment receipt").get("name") for item in environment_receipts] != list(
        REQUIRED_ENVIRONMENTS
    ):
        raise ProvenanceError("repository governance receipt environment set does not match")
    for raw in environment_receipts:
        environment = _mapping(raw, "environment receipt")
        if environment.get("protection_rules") != ["branch_policy"]:
            raise ProvenanceError(
                "environment receipt does not enforce protected-branch deployment policy"
            )
        if (
            environment.get("required_reviewers") != []
            or environment.get("required_reviewers_available") is not False
        ):
            raise ProvenanceError(
                "environment receipt must not claim unavailable reviewer protection"
            )
        if environment.get("can_admins_bypass") is not False:
            raise ProvenanceError("environment receipt permits administrator bypass")
        if environment.get("deployment_branch_policy") != {
            "protected_branches": True,
            "custom_branch_policies": False,
        }:
            raise ProvenanceError("environment receipt branch policy does not match")
    immutable = _mapping(document.get("immutable_releases"), "immutable releases receipt")
    if immutable != {
        "api_version": IMMUTABLE_RELEASES_API_VERSION,
        "enabled": True,
        "enforced_by_owner": True,
    }:
        raise ProvenanceError("repository governance does not enforce immutable releases")


def fetch_live_repository_governance(
    *,
    repository: str,
    source_commit: str,
    tag: str,
    fetch_json: GitHubJsonFetcher,
    fetch_admin_json: GitHubJsonFetcher,
) -> dict[str, object]:
    """Query and normalize the current GitHub controls for a release identity."""
    _validate_repository(repository)
    _validate_commit(source_commit)
    _validate_tag(tag)
    main_effective_rules = fetch_json(f"repos/{repository}/rules/branches/main?per_page=100")
    protected_branches = fetch_json(f"repos/{repository}/branches?protected=true&per_page=100")
    effective = _list(main_effective_rules, "effective main branch rules")
    if len(effective) >= 100:
        raise ProvenanceError("effective main branch rule query is not provably complete")
    branch_ruleset_ids = sorted(
        {
            _positive_integer(
                _mapping(raw, "effective main branch rule").get("ruleset_id"),
                "effective main branch ruleset id",
            )
            for raw in effective
        }
    )
    branch_rulesets = [
        fetch_json(f"repos/{repository}/rulesets/{ruleset_id}") for ruleset_id in branch_ruleset_ids
    ]
    summaries = _list(
        fetch_json(f"repos/{repository}/rulesets?includes_parents=true&per_page=100"),
        "repository rulesets",
    )
    if len(summaries) >= 100:
        raise ProvenanceError("repository ruleset query is not provably complete")
    tag_rulesets: list[object] = []
    for raw in summaries:
        summary = _mapping(raw, "repository ruleset summary")
        if summary.get("target") != "tag":
            continue
        ruleset_id = _positive_integer(summary.get("id"), "repository ruleset id")
        tag_rulesets.append(fetch_json(f"repos/{repository}/rulesets/{ruleset_id}"))
    environments: dict[str, object] = {}
    for name in REQUIRED_ENVIRONMENTS:
        environment = _mapping(
            fetch_json(f"repos/{repository}/environments/{name}"),
            f"environment {name}",
        )
        environments[name] = environment
    immutable_releases = fetch_admin_json(f"repos/{repository}/immutable-releases")
    return build_repository_governance(
        main_effective_rules,
        protected_branches,
        branch_rulesets,
        tag_rulesets,
        environments,
        immutable_releases,
        repository=repository,
        source_commit=source_commit,
        tag=tag,
    )


def verify_live_repository_governance(
    receipt: object,
    *,
    repository: str,
    source_commit: str,
    tag: str,
    fetch_json: GitHubJsonFetcher,
    fetch_admin_json: GitHubJsonFetcher,
) -> None:
    """Require the carried governance receipt to equal current GitHub state."""
    verify_repository_governance(
        receipt,
        repository=repository,
        source_commit=source_commit,
        tag=tag,
    )
    current = fetch_live_repository_governance(
        repository=repository,
        source_commit=source_commit,
        tag=tag,
        fetch_json=fetch_json,
        fetch_admin_json=fetch_admin_json,
    )
    if current != receipt:
        raise ProvenanceError(
            "carried repository governance receipt differs from current GitHub controls"
        )


def verify_release_identity(
    document: object,
    *,
    tag: str,
    source_commit: str,
    resolved_tag_commit: str,
    resolved_target_commit: str,
    expect_draft: bool | None,
    expect_prerelease: bool,
    expect_immutable: bool | None = None,
) -> None:
    """Require a GitHub release to identify one exact immutable source commit."""
    _validate_tag(tag)
    _validate_commit(source_commit)
    _validate_commit(resolved_tag_commit)
    _validate_commit(resolved_target_commit)
    release = _mapping(document, "GitHub release")
    _positive_integer(release.get("id"), "GitHub release id")
    expected: dict[str, object] = {
        "tag_name": tag,
        "prerelease": expect_prerelease,
    }
    if expect_draft is not None:
        expected["draft"] = expect_draft
    if expect_immutable is not None:
        expected["immutable"] = expect_immutable
    mismatches = [key for key, value in expected.items() if release.get(key) != value]
    if mismatches:
        raise ProvenanceError(f"GitHub release identity mismatch: {sorted(mismatches)}")
    if resolved_tag_commit != source_commit:
        raise ProvenanceError("live release tag does not resolve to the reviewed source commit")
    # GitHub ignores target_commitish when a release is created for an existing
    # tag.  The stored value may therefore be ``main`` even when the workflow
    # supplied the exact SHA.  Its live resolution, not its spelling, is what
    # must remain bound to the reviewed commit.
    if resolved_target_commit != source_commit:
        raise ProvenanceError("release target does not resolve to the reviewed source commit")


def resolve_live_release(
    *,
    repository: str,
    tag: str,
    expect_draft: bool | None,
    fetch_json: GitHubJsonFetcher,
    allow_absent: bool = False,
    expect_immutable: bool | None = None,
) -> dict[str, object] | None:
    """Resolve one exact release by tag through a bounded list and numeric ID.

    GitHub's tag-scoped release endpoint does not expose draft releases. This
    resolver therefore walks bounded 100-record pages to an explicit empty page,
    requires stable numeric identities and one unique tag match, and then reloads
    that match through the numeric release endpoint before returning it.
    """
    _validate_repository(repository)
    _validate_tag(tag)
    release_summaries: list[object] = []
    for page_number in range(1, MAX_RELEASE_HISTORY_PAGES + 1):
        page = _list(
            fetch_json(f"repos/{repository}/releases?per_page=100&page={page_number}"),
            f"GitHub releases page {page_number}",
        )
        if len(page) > 100:
            raise ProvenanceError(
                f"GitHub releases page {page_number} exceeds the requested page size"
            )
        if not page:
            break
        release_summaries.extend(page)
    else:
        raise ProvenanceError("repository release history exceeds the bounded pagination window")
    matches: list[dict[str, object]] = []
    seen_release_ids: set[int] = set()
    for item in release_summaries:
        summary = _mapping(item, "GitHub release summary")
        release_id = _positive_integer(summary.get("id"), "GitHub release id")
        if release_id in seen_release_ids:
            raise ProvenanceError(
                f"GitHub release history changed during pagination: duplicate id {release_id}"
            )
        seen_release_ids.add(release_id)
        if summary.get("tag_name") == tag:
            matches.append(summary)
    if not matches:
        if allow_absent:
            return None
        raise GitHubNotFound(f"GitHub release was not found for tag: {tag}")
    if len(matches) != 1:
        raise ProvenanceError(f"expected one GitHub release for {tag}; found {len(matches)}")
    summary = matches[0]
    release_id = _positive_integer(summary.get("id"), "GitHub release id")
    release = _mapping(
        fetch_json(f"repos/{repository}/releases/{release_id}"),
        "GitHub release",
    )
    compared_fields = ("id", "tag_name", "target_commitish", "draft", "prerelease", "immutable")
    mismatches = [field for field in compared_fields if release.get(field) != summary.get(field)]
    if mismatches:
        raise ProvenanceError(
            f"GitHub release changed during numeric resolution: {sorted(mismatches)}"
        )
    if release.get("tag_name") != tag:
        raise ProvenanceError("numeric GitHub release identity does not match the requested tag")
    if expect_draft is not None and release.get("draft") is not expect_draft:
        raise ProvenanceError("GitHub release draft state does not match the required state")
    if expect_immutable is not None and release.get("immutable") is not expect_immutable:
        raise ProvenanceError("GitHub release immutable state does not match the required state")
    if release.get("draft") is True and release.get("immutable") is not False:
        raise ProvenanceError("a draft GitHub release must remain mutable")
    return release


def verify_live_release_identity(
    *,
    repository: str,
    tag: str,
    source_commit: str,
    expect_draft: bool | None,
    expect_prerelease: bool,
    fetch_json: GitHubJsonFetcher,
    expect_immutable: bool | None = None,
) -> None:
    """Fetch and verify the live release, tag, and target identities."""
    _validate_repository(repository)
    _validate_tag(tag)
    _validate_commit(source_commit)
    release = resolve_live_release(
        repository=repository,
        tag=tag,
        expect_draft=expect_draft,
        fetch_json=fetch_json,
        expect_immutable=expect_immutable,
    )
    if release is None:  # pragma: no cover - allow_absent is false above.
        raise GitHubNotFound(f"GitHub release was not found for tag: {tag}")
    target = _nonempty_string(release.get("target_commitish"), "release target_commitish")
    if re.fullmatch(r"[A-Za-z0-9_./-]+", target) is None or ".." in target:
        raise ProvenanceError("release target_commitish is unsafe")
    tag_commit = _mapping(
        fetch_json(f"repos/{repository}/commits/{tag}"),
        "release tag commit",
    )
    target_commit = _mapping(
        fetch_json(f"repos/{repository}/commits/{target}"),
        "release target commit",
    )
    verify_release_identity(
        release,
        tag=tag,
        source_commit=source_commit,
        resolved_tag_commit=_nonempty_string(tag_commit.get("sha"), "release tag commit SHA"),
        resolved_target_commit=_nonempty_string(
            target_commit.get("sha"), "release target commit SHA"
        ),
        expect_draft=expect_draft,
        expect_prerelease=expect_prerelease,
        expect_immutable=expect_immutable,
    )


def verify_live_mutation_authority(
    governance_receipt: object,
    *,
    repository: str,
    source_commit: str,
    tag: str,
    workflow_ref: str,
    workflow_sha: str,
    release_state: str,
    expect_draft: bool | None,
    fetch_json: GitHubJsonFetcher,
    fetch_admin_json: GitHubJsonFetcher,
) -> None:
    """Revalidate protected main, tag, governance, and release before mutation."""
    _validate_repository(repository)
    _validate_commit(source_commit)
    _validate_tag(tag)
    if workflow_ref != "refs/heads/main" or workflow_sha != source_commit:
        raise ProvenanceError("persistent mutation is not executing from the exact main commit")
    if release_state == "present" and expect_draft is not True:
        raise ProvenanceError("release mutation is permitted only while the release is a draft")
    main_commit = _mapping(
        fetch_json(f"repos/{repository}/commits/main"),
        "live main commit",
    )
    tag_commit = _mapping(
        fetch_json(f"repos/{repository}/commits/{tag}"),
        "live release tag commit",
    )
    if main_commit.get("sha") != source_commit or tag_commit.get("sha") != source_commit:
        raise ProvenanceError("live main or release tag moved away from the reviewed commit")
    verify_live_repository_governance(
        governance_receipt,
        repository=repository,
        source_commit=source_commit,
        tag=tag,
        fetch_json=fetch_json,
        fetch_admin_json=fetch_admin_json,
    )
    if release_state not in {"absent", "present"}:
        raise ProvenanceError("release state expectation is invalid")
    if release_state == "present" and expect_draft is None:
        raise ProvenanceError("present release mutation requires an exact draft state")
    release = resolve_live_release(
        repository=repository,
        tag=tag,
        expect_draft=expect_draft if release_state == "present" else None,
        fetch_json=fetch_json,
        allow_absent=release_state == "absent",
    )
    if release is None:
        return
    if release_state == "absent":
        raise ProvenanceError("GitHub release appeared before create mutation")
    target = _nonempty_string(release.get("target_commitish"), "release target_commitish")
    if release.get("immutable") is not False:
        raise ProvenanceError("a mutable draft is required before every release mutation")
    if re.fullmatch(r"[A-Za-z0-9_./-]+", target) is None or ".." in target:
        raise ProvenanceError("release target_commitish is unsafe")
    target_commit = _mapping(
        fetch_json(f"repos/{repository}/commits/{target}"),
        "release target commit",
    )
    verify_release_identity(
        release,
        tag=tag,
        source_commit=source_commit,
        resolved_tag_commit=_nonempty_string(tag_commit.get("sha"), "release tag SHA"),
        resolved_target_commit=_nonempty_string(
            target_commit.get("sha"), "release target commit SHA"
        ),
        expect_draft=expect_draft,
        expect_prerelease=False,
        expect_immutable=False,
    )


def build_staged_release_asset_plan(
    document: object,
    candidate_dir: Path,
) -> dict[str, object]:
    """Verify existing draft assets by metadata and plan only missing uploads."""
    release = _mapping(document, "staged release")
    assets = _list(release.get("assets"), "staged release assets")
    if len(assets) > 6:
        raise ProvenanceError("staged release contains more than six candidate assets")
    try:
        local_paths = list(candidate_dir.iterdir())
    except OSError as exc:
        raise ProvenanceError(f"could not inspect staged candidate directory: {exc}") from exc
    local: dict[str, dict[str, object]] = {}
    for path in local_paths:
        name = path.name
        maximum = _release_asset_file_limit(name)
        details = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(details.st_mode):
            raise ProvenanceError(f"staged candidate subject is not regular: {name}")
        if details.st_size < 1 or details.st_size > maximum:
            raise ProvenanceError(f"staged candidate subject size is invalid: {name}")
        local[name] = {
            "name": name,
            "size": details.st_size,
            "digest": f"sha256:{_sha256_bounded_file(path, maximum_bytes=maximum)}",
        }
    wheels = [name for name in local if name.endswith(".whl")]
    sdists = [name for name in local if name.endswith(".tar.gz")]
    expected_names = {
        *wheels,
        *sdists,
        "validation-local.json",
        "CI-STATUS.json",
        "REPOSITORY-GOVERNANCE.json",
        "SHA256SUMS",
    }
    if len(wheels) != 1 or len(sdists) != 1 or set(local) != expected_names:
        raise ProvenanceError("staged candidate directory file set does not match")
    observed: dict[str, dict[str, object]] = {}
    for raw in assets:
        asset = _mapping(raw, "staged release asset")
        name = _nonempty_string(asset.get("name"), "staged release asset name")
        if name in observed or name not in local:
            raise ProvenanceError(f"staged release asset is duplicate or unexpected: {name}")
        digest = _nonempty_string(asset.get("digest"), f"staged release asset {name} digest")
        normalized: dict[str, object] = {
            "name": name,
            "size": _positive_integer(asset.get("size"), f"staged release asset {name} size"),
            "digest": digest,
        }
        if re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None or normalized != local[name]:
            raise ProvenanceError(f"staged release asset metadata differs from candidate: {name}")
        observed[name] = normalized
    return {
        "schema_version": "1.0",
        "existing": [observed[name] for name in sorted(observed)],
        "missing": [local[name] for name in sorted(set(local) - set(observed))],
    }


def validate_release_acceptance_matrix(
    document: object,
    *,
    expected_sha256: str | None = None,
    expected_release_version: str | None = None,
) -> dict[str, object]:
    """Validate and normalize the exact ordered live-acceptance report matrix."""
    matrix = _mapping(document, "release acceptance matrix")
    expected_top_level = {
        "schema_version",
        "release_version",
        "matrix_sha256",
        "report_count_per_stage",
        "target_labels_are_policy_evidence_instances",
        "stages",
        "reports",
    }
    if set(matrix) != expected_top_level:
        raise ProvenanceError(
            "release acceptance matrix fields do not exactly match: "
            f"missing={sorted(expected_top_level - set(matrix))}, "
            f"unexpected={sorted(set(matrix) - expected_top_level)}"
        )
    if matrix.get("schema_version") != RELEASE_ACCEPTANCE_MATRIX_SCHEMA:
        raise ProvenanceError("release acceptance matrix schema does not match")
    release_version = _nonempty_string(
        matrix.get("release_version"), "release acceptance matrix version"
    )
    if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[.-][0-9A-Za-z.-]+)?", release_version) is None:
        raise ProvenanceError("release acceptance matrix version is invalid")
    if expected_release_version is not None and release_version != expected_release_version:
        raise ProvenanceError(
            "release acceptance matrix version does not match policy: "
            f"{release_version} != {expected_release_version}"
        )
    if matrix.get("target_labels_are_policy_evidence_instances") is not True:
        raise ProvenanceError("release acceptance matrix target-label semantics are not explicit")
    count = _positive_integer(
        matrix.get("report_count_per_stage"), "release acceptance matrix report count"
    )
    if count > MAX_VALIDATION_REPORT_ASSETS:
        raise ProvenanceError("release acceptance matrix report count exceeds the asset limit")

    stages = _list(matrix.get("stages"), "release acceptance matrix stages")
    if len(stages) != len(RELEASE_ACCEPTANCE_MATRIX_STAGES):
        raise ProvenanceError("release acceptance matrix must define exactly two stages")
    normalized_stages: list[dict[str, object]] = []
    expected_artifact_stages = {
        "candidate": "immutable_candidate",
        "released": "published",
    }
    expected_prefixes = {
        "candidate": "validation",
        "released": "released-validation",
    }
    for index, raw in enumerate(stages):
        stage = _mapping(raw, "release acceptance matrix stage")
        if set(stage) != {"name", "artifact_stage", "filename_prefix"}:
            raise ProvenanceError("release acceptance matrix stage fields do not exactly match")
        name = _nonempty_string(stage.get("name"), "release acceptance matrix stage name")
        if name != RELEASE_ACCEPTANCE_MATRIX_STAGES[index]:
            raise ProvenanceError("release acceptance matrix stage order does not match")
        artifact_stage = _nonempty_string(
            stage.get("artifact_stage"), "release acceptance matrix artifact stage"
        )
        prefix = _nonempty_string(
            stage.get("filename_prefix"), "release acceptance matrix filename prefix"
        )
        if artifact_stage != expected_artifact_stages[name] or prefix != expected_prefixes[name]:
            raise ProvenanceError(f"release acceptance matrix stage semantics differ: {name}")
        normalized_stages.append(
            {"name": name, "artifact_stage": artifact_stage, "filename_prefix": prefix}
        )

    reports = _list(matrix.get("reports"), "release acceptance matrix reports")
    if len(reports) != count:
        raise ProvenanceError(
            "release acceptance matrix count does not equal its ordered report list"
        )
    required_report_fields = {
        "ordinal",
        "id",
        "cluster",
        "scenario",
        "command",
        "report_option",
    }
    optional_report_fields = {"package", "remote_tool", "arguments", "evidence_group"}
    normalized_reports: list[dict[str, object]] = []
    report_ids: set[str] = set()
    for ordinal, raw in enumerate(reports, start=1):
        report = _mapping(raw, "release acceptance matrix report")
        fields = set(report)
        if not required_report_fields.issubset(fields) or not fields.issubset(
            required_report_fields | optional_report_fields
        ):
            raise ProvenanceError(
                f"release acceptance matrix report {ordinal} fields do not exactly match"
            )
        if _positive_integer(report.get("ordinal"), "matrix report ordinal") != ordinal:
            raise ProvenanceError("release acceptance matrix report order is not contiguous")
        report_id = _nonempty_string(report.get("id"), "matrix report id")
        if re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", report_id) is None:
            raise ProvenanceError(f"release acceptance matrix report id is unsafe: {report_id}")
        if report_id in report_ids:
            raise ProvenanceError(f"duplicate release acceptance matrix report id: {report_id}")
        report_ids.add(report_id)
        cluster = _nonempty_string(report.get("cluster"), "matrix report cluster")
        scenario = _nonempty_string(report.get("scenario"), "matrix report scenario")
        if (
            re.fullmatch(r"[A-Za-z0-9._-]+", cluster) is None
            or re.fullmatch(r"[A-Za-z0-9._-]+", scenario) is None
        ):
            raise ProvenanceError(
                f"release acceptance matrix report identity is unsafe: {report_id}"
            )
        command = _list(report.get("command"), "matrix report command")
        if not command or any(not isinstance(item, str) or not item.strip() for item in command):
            raise ProvenanceError(f"release acceptance matrix command is invalid: {report_id}")
        report_option = _nonempty_string(report.get("report_option"), "matrix report option")
        if report_option not in {"--report", "--validation-report"}:
            raise ProvenanceError(
                f"release acceptance matrix report option is invalid: {report_id}"
            )
        if "arguments" in report:
            arguments = _mapping(report.get("arguments"), "matrix report arguments")
            if len(arguments) > 64 or any(
                not key or len(key) > 256 or re.fullmatch(r"[A-Za-z0-9_.-]+", key) is None
                for key in arguments
            ):
                raise ProvenanceError(
                    f"release acceptance matrix arguments are invalid: {report_id}"
                )
            try:
                encoded_arguments = json.dumps(
                    arguments,
                    allow_nan=False,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            except (TypeError, ValueError) as exc:
                raise ProvenanceError(
                    f"release acceptance matrix arguments are not finite JSON: {report_id}"
                ) from exc
            if len(encoded_arguments) > 64 * 1024:
                raise ProvenanceError(
                    f"release acceptance matrix arguments are too large: {report_id}"
                )
        normalized_reports.append(
            {
                "ordinal": ordinal,
                "id": report_id,
                "cluster": cluster,
                "scenario": scenario,
            }
        )

    claimed_sha256 = _nonempty_string(
        matrix.get("matrix_sha256"), "release acceptance matrix SHA-256"
    )
    if re.fullmatch(r"[0-9a-f]{64}", claimed_sha256) is None:
        raise ProvenanceError("release acceptance matrix SHA-256 is invalid")
    canonical = dict(matrix)
    del canonical["matrix_sha256"]
    actual_sha256 = hashlib.sha256(
        json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    if claimed_sha256 != actual_sha256:
        raise ProvenanceError("release acceptance matrix self-digest does not match")
    if expected_sha256 is not None and expected_sha256 != actual_sha256:
        raise ProvenanceError("release acceptance matrix digest does not match policy")
    return {
        "schema_version": RELEASE_ACCEPTANCE_MATRIX_SCHEMA,
        "release_version": release_version,
        "matrix_sha256": actual_sha256,
        "report_count_per_stage": count,
        "stages": normalized_stages,
        "reports": normalized_reports,
    }


def load_release_acceptance_matrix(
    path: Path,
    *,
    expected_sha256: str | None = None,
    expected_release_version: str | None = None,
) -> dict[str, object]:
    """Load an acceptance matrix with bounded JSON parsing and semantic digest checks."""
    return validate_release_acceptance_matrix(
        _load_json(path),
        expected_sha256=expected_sha256,
        expected_release_version=expected_release_version,
    )


def _release_acceptance_matrix_stage(
    matrix: Mapping[str, object],
    kind: str,
) -> dict[str, object]:
    stages = _list(matrix.get("stages"), "release acceptance matrix stages")
    for raw in stages:
        stage = _mapping(raw, "release acceptance matrix stage")
        if stage.get("name") == kind:
            return stage
    raise ProvenanceError(f"release acceptance matrix does not define stage: {kind}")


def build_validation_report_asset_manifest(
    document: object,
    *,
    kind: str,
    acceptance_matrix: object | None = None,
) -> dict[str, object]:
    """Validate and normalize the bounded release assets used as live reports."""
    if kind == "candidate":
        prefix = "validation-"
        pattern = re.compile(r"validation-[A-Za-z0-9._-]+\.json")
        local_name = "validation-local.json"
    elif kind == "released":
        prefix = "released-validation-"
        pattern = re.compile(r"released-validation-[A-Za-z0-9._-]+\.json")
        local_name = None
    else:
        raise ProvenanceError("validation report asset kind must be candidate or released")
    if isinstance(document, list):
        raw_assets = _list(cast(object, document), "release assets")
    else:
        release = _mapping(document, "release asset document")
        raw_assets = _list(release.get("assets"), "release assets")
    if len(raw_assets) > MAX_RELEASE_ASSET_METADATA_RECORDS:
        raise ProvenanceError(
            "release asset metadata count exceeds the bounded preflight limit: "
            f"{len(raw_assets)} > {MAX_RELEASE_ASSET_METADATA_RECORDS}"
        )
    observed_names: set[str] = set()
    normalized: list[dict[str, object]] = []
    normalized_release_assets: list[dict[str, object]] = []
    total_bytes = 0
    release_total_bytes = 0
    for raw in raw_assets:
        asset = _mapping(raw, "release asset")
        name = _nonempty_string(asset.get("name"), "release asset name")
        if name in observed_names:
            raise ProvenanceError(f"release contains a duplicate asset name: {name}")
        observed_names.add(name)
        if (
            name != Path(name).name
            or "/" in name
            or "\\" in name
            or re.fullmatch(r"[A-Za-z0-9_.+-]+", name) is None
        ):
            raise ProvenanceError(f"release asset name is unsafe: {name}")
        size = _positive_integer(asset.get("size"), f"release asset {name} size")
        digest = _nonempty_string(asset.get("digest"), f"release asset {name} digest")
        if re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
            raise ProvenanceError(f"release asset digest is invalid: {name}")
        maximum_size = _release_asset_file_limit(name)
        if size > maximum_size:
            raise ProvenanceError(f"release asset exceeds its {maximum_size}-byte limit: {name}")
        release_total_bytes += size
        if release_total_bytes > MAX_RELEASE_ASSET_AGGREGATE_BYTES:
            raise ProvenanceError("release assets exceed the aggregate byte limit")
        normalized_release_assets.append(
            {
                "id": _positive_integer(asset.get("id"), f"release asset {name} id"),
                "name": name,
                "size": size,
                "digest": digest,
            }
        )
        if not (name.startswith(prefix) and name.endswith(".json")):
            continue
        if pattern.fullmatch(name) is None:
            raise ProvenanceError(f"validation report asset name is unsafe: {name}")
        if size > MAX_VALIDATION_REPORT_BYTES:
            raise ProvenanceError(
                f"validation report asset exceeds {MAX_VALIDATION_REPORT_BYTES} bytes: {name}"
            )
        uploader = _mapping(asset.get("uploader"), f"release asset {name} uploader")
        normalized.append(
            {
                "id": _positive_integer(asset.get("id"), f"release asset {name} id"),
                "name": name,
                "size": size,
                "digest": digest,
                "uploader": {
                    "login": _nonempty_string(
                        uploader.get("login"), f"release asset {name} uploader login"
                    ),
                    "id": _positive_integer(
                        uploader.get("id"), f"release asset {name} uploader id"
                    ),
                },
            }
        )
        total_bytes += size
    normalized.sort(key=lambda item: cast(str, item["name"]))
    normalized_release_assets.sort(key=lambda item: cast(str, item["name"]))
    if len(normalized) > MAX_VALIDATION_REPORT_ASSETS:
        raise ProvenanceError(
            "validation report asset count exceeds "
            f"{MAX_VALIDATION_REPORT_ASSETS}: {len(normalized)}"
        )
    if total_bytes > MAX_VALIDATION_REPORT_AGGREGATE_BYTES:
        raise ProvenanceError(
            "validation report assets exceed aggregate byte limit: "
            f"{total_bytes} > {MAX_VALIDATION_REPORT_AGGREGATE_BYTES}"
        )
    names = [cast(str, item["name"]) for item in normalized]
    if local_name is not None:
        if names.count(local_name) != 1:
            raise ProvenanceError("candidate assets must contain exactly one validation-local.json")
        if len(names) < 2:
            raise ProvenanceError("candidate assets contain no non-local validation report")
    elif not names:
        raise ProvenanceError("release assets contain no released-artifact validation report")
    matrix_binding: dict[str, object] | None = None
    if acceptance_matrix is not None:
        matrix = validate_release_acceptance_matrix(acceptance_matrix)
        stage = _release_acceptance_matrix_stage(matrix, kind)
        matrix_reports = _list(matrix.get("reports"), "release acceptance matrix reports")
        expected_reports = [
            {
                "ordinal": _positive_integer(item.get("ordinal"), "matrix report ordinal"),
                "id": _nonempty_string(item.get("id"), "matrix report id"),
                "cluster": _nonempty_string(item.get("cluster"), "matrix report cluster"),
                "scenario": _nonempty_string(item.get("scenario"), "matrix report scenario"),
                "filename": (
                    f"{_nonempty_string(stage.get('filename_prefix'), 'matrix stage prefix')}-"
                    f"{_nonempty_string(item.get('id'), 'matrix report id')}.json"
                ),
            }
            for item in (
                _mapping(raw, "release acceptance matrix report") for raw in matrix_reports
            )
        ]
        expected_names = [cast(str, item["filename"]) for item in expected_reports]
        actual_names = [name for name in names if name != local_name]
        if len(actual_names) != len(expected_names) or set(actual_names) != set(expected_names):
            missing = sorted(set(expected_names) - set(actual_names))
            unexpected = sorted(set(actual_names) - set(expected_names))
            raise ProvenanceError(
                "validation report assets do not exactly match the release acceptance matrix: "
                f"missing={missing}, unexpected={unexpected}"
            )
        assets_by_name = {cast(str, item["name"]): item for item in normalized}
        ordered_assets = [assets_by_name[name] for name in expected_names]
        if local_name is not None:
            ordered_assets.insert(0, assets_by_name[local_name])
        normalized = ordered_assets
        matrix_binding = {
            "schema_version": matrix["schema_version"],
            "release_version": matrix["release_version"],
            "sha256": matrix["matrix_sha256"],
            "report_count": matrix["report_count_per_stage"],
            "stage": stage["name"],
            "artifact_stage": stage["artifact_stage"],
            "filename_prefix": stage["filename_prefix"],
            "reports": expected_reports,
        }
    manifest: dict[str, object] = {
        "schema_version": "1.0",
        "kind": kind,
        "release_asset_count": len(raw_assets),
        "release_asset_aggregate_bytes": release_total_bytes,
        "release_assets": normalized_release_assets,
        "report_count": len(normalized),
        "limits": {
            "maximum_release_asset_metadata_records": MAX_RELEASE_ASSET_METADATA_RECORDS,
            "maximum_release_asset_bytes": MAX_RELEASE_ASSET_BYTES,
            "maximum_release_asset_aggregate_bytes": MAX_RELEASE_ASSET_AGGREGATE_BYTES,
            "maximum_distribution_bytes": MAX_DISTRIBUTION_BYTES,
            "maximum_fixed_json_bytes": MAX_FIXED_JSON_BYTES,
            "maximum_manifest_bytes": MAX_MANIFEST_BYTES,
            "maximum_assets": MAX_VALIDATION_REPORT_ASSETS,
            "maximum_asset_bytes": MAX_VALIDATION_REPORT_BYTES,
            "maximum_aggregate_bytes": MAX_VALIDATION_REPORT_AGGREGATE_BYTES,
        },
        "aggregate_bytes": total_bytes,
        "assets": normalized,
    }
    if matrix_binding is not None:
        manifest["acceptance_matrix"] = matrix_binding
    return manifest


def verify_downloaded_validation_report_assets(
    manifest: object,
    report_dir: Path,
) -> None:
    """Require downloaded report files to exactly match a preflight asset manifest."""
    document = _mapping(manifest, "validation report asset manifest")
    if document.get("schema_version") != "1.0":
        raise ProvenanceError("validation report asset manifest schema does not match")
    limits = _mapping(document.get("limits"), "validation report asset manifest limits")
    if limits != {
        "maximum_release_asset_metadata_records": MAX_RELEASE_ASSET_METADATA_RECORDS,
        "maximum_release_asset_bytes": MAX_RELEASE_ASSET_BYTES,
        "maximum_release_asset_aggregate_bytes": MAX_RELEASE_ASSET_AGGREGATE_BYTES,
        "maximum_distribution_bytes": MAX_DISTRIBUTION_BYTES,
        "maximum_fixed_json_bytes": MAX_FIXED_JSON_BYTES,
        "maximum_manifest_bytes": MAX_MANIFEST_BYTES,
        "maximum_assets": MAX_VALIDATION_REPORT_ASSETS,
        "maximum_asset_bytes": MAX_VALIDATION_REPORT_BYTES,
        "maximum_aggregate_bytes": MAX_VALIDATION_REPORT_AGGREGATE_BYTES,
    }:
        raise ProvenanceError("validation report asset manifest limits do not match")
    release_asset_count = _positive_integer(
        document.get("release_asset_count"), "release asset manifest total count"
    )
    if release_asset_count > MAX_RELEASE_ASSET_METADATA_RECORDS:
        raise ProvenanceError("release asset manifest total count exceeds the limit")
    release_aggregate = _positive_integer(
        document.get("release_asset_aggregate_bytes"),
        "release asset manifest aggregate bytes",
    )
    if release_aggregate > MAX_RELEASE_ASSET_AGGREGATE_BYTES:
        raise ProvenanceError("release asset manifest aggregate exceeds the limit")
    release_assets = _list(document.get("release_assets"), "release asset manifest inventory")
    if len(release_assets) != release_asset_count:
        raise ProvenanceError("release asset manifest inventory count does not match")
    inventory_total = 0
    inventory_names: set[str] = set()
    for raw in release_assets:
        asset = _mapping(raw, "release asset manifest inventory entry")
        name = _nonempty_string(asset.get("name"), "release asset inventory name")
        if name in inventory_names:
            raise ProvenanceError(f"release asset inventory duplicates {name}")
        inventory_names.add(name)
        _positive_integer(asset.get("id"), f"release asset inventory {name} id")
        size = _positive_integer(asset.get("size"), f"release asset inventory {name} size")
        if size > _release_asset_file_limit(name):
            raise ProvenanceError(f"release asset inventory entry is too large: {name}")
        digest = _nonempty_string(asset.get("digest"), f"release asset inventory {name} digest")
        if re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
            raise ProvenanceError(f"release asset inventory digest is invalid: {name}")
        inventory_total += size
    if inventory_total != release_aggregate:
        raise ProvenanceError("release asset manifest inventory aggregate does not match")
    declared: dict[str, tuple[int, str]] = {}
    aggregate_bytes = 0
    for raw in _list(document.get("assets"), "validation report asset manifest assets"):
        asset = _mapping(raw, "validation report asset manifest entry")
        name = _nonempty_string(asset.get("name"), "validation report asset manifest name")
        if name in declared:
            raise ProvenanceError(f"validation report asset manifest duplicates {name}")
        size = _positive_integer(asset.get("size"), f"validation report asset manifest {name} size")
        if size > MAX_VALIDATION_REPORT_BYTES:
            raise ProvenanceError(f"validation report asset manifest entry is too large: {name}")
        digest = _nonempty_string(
            asset.get("digest"), f"validation report asset manifest {name} digest"
        )
        if re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
            raise ProvenanceError(f"validation report asset manifest digest is invalid: {name}")
        declared[name] = (size, digest)
        aggregate_bytes += size
    if len(declared) > MAX_VALIDATION_REPORT_ASSETS:
        raise ProvenanceError("validation report asset manifest contains too many reports")
    if aggregate_bytes > MAX_VALIDATION_REPORT_AGGREGATE_BYTES:
        raise ProvenanceError("validation report asset manifest aggregate is too large")
    if document.get("report_count") != len(declared):
        raise ProvenanceError("validation report asset manifest count does not match")
    if document.get("aggregate_bytes") != aggregate_bytes:
        raise ProvenanceError("validation report asset manifest aggregate does not match")
    try:
        entries = list(report_dir.iterdir())
    except OSError as exc:
        raise ProvenanceError(f"could not inspect downloaded validation reports: {exc}") from exc
    observed: dict[str, tuple[int, str]] = {}
    for path in entries:
        try:
            details = path.lstat()
        except OSError as exc:
            raise ProvenanceError(
                f"could not inspect downloaded report {path.name}: {exc}"
            ) from exc
        if path.is_symlink() or not path.is_file():
            raise ProvenanceError(
                f"downloaded validation report is not a regular file: {path.name}"
            )
        observed[path.name] = (
            details.st_size,
            "sha256:" + _sha256_bounded_file(path, maximum_bytes=MAX_VALIDATION_REPORT_BYTES),
        )
    if observed != declared:
        raise ProvenanceError(
            "downloaded validation reports differ from the preflight manifest: "
            f"declared={declared}, observed={observed}"
        )


def _main_ruleset_protection_receipt(
    effective_rules_document: object,
    rulesets_document: object,
) -> dict[str, object]:
    effective_rules = _list(effective_rules_document, "effective main branch rules")
    if len(effective_rules) >= 100:
        raise ProvenanceError("effective main branch rule query is not provably complete")
    rulesets = _list(rulesets_document, "main branch rulesets")
    normalized_rulesets: dict[int, dict[str, object]] = {}
    for raw in rulesets:
        ruleset = _mapping(raw, "main branch ruleset")
        ruleset_id = _positive_integer(ruleset.get("id"), "main branch ruleset id")
        if ruleset_id in normalized_rulesets:
            raise ProvenanceError(f"duplicate main branch ruleset id: {ruleset_id}")
        if ruleset.get("target") != "branch" or ruleset.get("enforcement") != "active":
            raise ProvenanceError(f"main branch ruleset {ruleset_id} is not active")
        conditions = _mapping(ruleset.get("conditions"), "main branch ruleset conditions")
        ref_name = _mapping(conditions.get("ref_name"), "main branch ruleset ref_name")
        includes = sorted(_string_list(ref_name.get("include"), "main ruleset include"))
        excludes = sorted(_string_list(ref_name.get("exclude", []), "main ruleset exclude"))
        if "refs/heads/main" not in includes and "~DEFAULT_BRANCH" not in includes:
            raise ProvenanceError(f"main branch ruleset {ruleset_id} does not cover main")
        if excludes:
            raise ProvenanceError(f"main branch ruleset {ruleset_id} has exclusions")
        if ruleset.get("current_user_can_bypass") != "never":
            raise ProvenanceError(
                f"current workflow token can bypass main branch ruleset {ruleset_id}"
            )
        bypass_visible = "bypass_actors" in ruleset
        bypass_count: int | None = None
        if bypass_visible:
            bypass_count = len(
                _list(ruleset.get("bypass_actors"), "main branch ruleset bypass actors")
            )
        normalized_rulesets[ruleset_id] = {
            "id": ruleset_id,
            "name": _nonempty_string(ruleset.get("name"), "main branch ruleset name"),
            "enforcement": "active",
            "current_workflow_token_can_bypass": False,
            "global_bypass_actors_visible": bypass_visible,
            "configured_bypass_actor_count": bypass_count,
        }
    if not normalized_rulesets:
        raise ProvenanceError("no active ruleset supplies effective main branch rules")

    by_type: dict[str, list[dict[str, object]]] = {}
    effective_ruleset_ids: set[int] = set()
    for raw in effective_rules:
        rule = _mapping(raw, "effective main branch rule")
        rule_type = _nonempty_string(rule.get("type"), "effective main branch rule type")
        ruleset_id = _positive_integer(rule.get("ruleset_id"), "effective rule ruleset id")
        if ruleset_id not in normalized_rulesets:
            raise ProvenanceError(
                f"effective main rule references unavailable ruleset {ruleset_id}"
            )
        effective_ruleset_ids.add(ruleset_id)
        by_type.setdefault(rule_type, []).append(rule)
    required_types = {
        "deletion",
        "merge_queue",
        "non_fast_forward",
        "pull_request",
        "required_status_checks",
    }
    missing = sorted(required_types - set(by_type))
    if missing:
        raise ProvenanceError(f"effective main branch rules are incomplete: {missing}")
    for rule_type in required_types:
        if len(by_type[rule_type]) != 1:
            raise ProvenanceError(f"effective main branch rule is ambiguous: {rule_type}")

    status = _mapping(
        by_type["required_status_checks"][0].get("parameters"),
        "effective required status checks parameters",
    )
    status_checks: list[dict[str, object]] = []
    for raw in _list(status.get("required_status_checks"), "effective required status checks"):
        check = _mapping(raw, "effective required status check")
        status_checks.append(
            {
                "context": _nonempty_string(check.get("context"), "effective status check context"),
                "app_id": _positive_integer(
                    check.get("integration_id"), "effective status check integration id"
                ),
            }
        )
    status_checks.sort(key=lambda item: cast(str, item["context"]))
    reviews = _mapping(
        by_type["pull_request"][0].get("parameters"),
        "effective pull request rule parameters",
    )
    merge_queue = _mapping(
        by_type["merge_queue"][0].get("parameters"),
        "effective merge queue rule parameters",
    )
    normalized_merge_queue = {key: merge_queue.get(key) for key in REQUIRED_MERGE_QUEUE_PARAMETERS}
    if normalized_merge_queue != REQUIRED_MERGE_QUEUE_PARAMETERS or set(merge_queue) != set(
        REQUIRED_MERGE_QUEUE_PARAMETERS
    ):
        raise ProvenanceError("effective merge queue parameters differ from the release contract")
    receipt: dict[str, object] = {
        "source": "effective_rulesets",
        "ruleset_ids": sorted(effective_ruleset_ids),
        "rulesets": [normalized_rulesets[item] for item in sorted(effective_ruleset_ids)],
        "strict_status_checks": status.get("strict_required_status_checks_policy") is True,
        "required_status_checks": status_checks,
        "review_policy": MAIN_REVIEW_POLICY,
        "required_approving_review_count": _integer(
            reviews.get("required_approving_review_count"), "required approval count"
        ),
        "dismiss_stale_reviews": reviews.get("dismiss_stale_reviews_on_push") is True,
        "require_last_push_approval": reviews.get("require_last_push_approval") is True,
        "required_conversation_resolution": reviews.get("required_review_thread_resolution")
        is True,
        "merge_queue": normalized_merge_queue,
        "prevents_force_pushes": True,
        "prevents_deletions": True,
        "current_workflow_token_can_bypass": False,
        "global_bypass_visibility_complete": all(
            cast(bool, item["global_bypass_actors_visible"])
            for item in normalized_rulesets.values()
        ),
    }
    _verify_main_protection(receipt)
    return receipt


def _protected_branch_names(document: object) -> list[str]:
    """Require main to be the repository's sole protected deployment branch."""
    branches = _list(document, "protected branches")
    if len(branches) >= 100:
        raise ProvenanceError("protected branch query is not provably complete")
    names: list[str] = []
    for raw in branches:
        branch = _mapping(raw, "protected branch")
        name = _nonempty_string(branch.get("name"), "protected branch name")
        if branch.get("protected") is not True:
            raise ProvenanceError(f"protected branch query returned an unprotected branch: {name}")
        names.append(name)
    if sorted(names) != ["main"] or len(set(names)) != len(names):
        raise ProvenanceError(
            "main must be the sole protected branch admitted by release environments"
        )
    return ["main"]


def _verify_main_protection(branch: Mapping[str, object]) -> None:
    checks = branch.get("required_status_checks")
    approval_count = branch.get("required_approving_review_count")
    last_push_approval = branch.get("require_last_push_approval")
    expected_checks = [
        {"context": context, "app_id": GITHUB_ACTIONS_APP_ID}
        for context in sorted(REQUIRED_CI_JOBS)
    ]
    failures = {
        "effective_ruleset_source": branch.get("source") == "effective_rulesets",
        "ruleset_ids": isinstance(branch.get("ruleset_ids"), list)
        and bool(branch.get("ruleset_ids")),
        "strict_status_checks": branch.get("strict_status_checks") is True,
        "required_status_checks": checks == expected_checks,
        "review_policy": branch.get("review_policy") == MAIN_REVIEW_POLICY,
        "required_approving_review_count": type(approval_count) is int
        and approval_count == REQUIRED_APPROVING_REVIEW_COUNT,
        "dismiss_stale_reviews": branch.get("dismiss_stale_reviews") is True,
        "require_last_push_approval": type(last_push_approval) is bool
        and last_push_approval == REQUIRE_LAST_PUSH_APPROVAL,
        "required_conversation_resolution": branch.get("required_conversation_resolution") is True,
        "merge_queue": branch.get("merge_queue") == REQUIRED_MERGE_QUEUE_PARAMETERS,
        "prevents_force_pushes": branch.get("prevents_force_pushes") is True,
        "prevents_deletions": branch.get("prevents_deletions") is True,
        "current_workflow_token_cannot_bypass": branch.get("current_workflow_token_can_bypass")
        is False,
    }
    rulesets = _list(branch.get("rulesets"), "main protection rulesets")
    ruleset_ids = branch.get("ruleset_ids")
    normalized_ids: list[int] = []
    for raw in rulesets:
        ruleset = _mapping(raw, "main protection ruleset")
        normalized_ids.append(_positive_integer(ruleset.get("id"), "main ruleset id"))
        if ruleset.get("enforcement") != "active":
            raise ProvenanceError("main protection receipt contains an inactive ruleset")
        if ruleset.get("current_workflow_token_can_bypass") is not False:
            raise ProvenanceError("main protection receipt permits workflow-token bypass")
        visible = ruleset.get("global_bypass_actors_visible")
        count = ruleset.get("configured_bypass_actor_count")
        if visible is True:
            _integer(count, "visible configured bypass actor count")
        elif visible is False:
            if count is not None:
                raise ProvenanceError(
                    "hidden global bypass actors must not be represented as empty"
                )
        else:
            raise ProvenanceError("main protection bypass visibility is invalid")
    if normalized_ids != ruleset_ids:
        failures["ruleset_receipts"] = False
    visibility = branch.get("global_bypass_visibility_complete")
    if visibility is not all(
        _mapping(raw, "main protection ruleset").get("global_bypass_actors_visible") is True
        for raw in rulesets
    ):
        failures["global_bypass_visibility"] = False
    rejected = sorted(name for name, passed in failures.items() if not passed)
    if rejected:
        raise ProvenanceError(f"main branch protection is incomplete: {rejected}")


def _tag_protection_receipts(document: object) -> list[dict[str, object]]:
    rulesets = _list(document, "tag rulesets")
    matching: list[dict[str, object]] = []
    for raw in rulesets:
        ruleset = _mapping(raw, "tag ruleset")
        if ruleset.get("target") != "tag" or ruleset.get("enforcement") != "active":
            continue
        conditions = _mapping(ruleset.get("conditions"), "tag ruleset conditions")
        ref_name = _mapping(conditions.get("ref_name"), "tag ruleset ref_name")
        includes = sorted(_string_list(ref_name.get("include"), "tag ruleset include"))
        excludes = sorted(_string_list(ref_name.get("exclude", []), "tag ruleset exclude"))
        if RELEASE_TAG_PATTERN not in includes and "~ALL" not in includes:
            continue
        if excludes:
            continue
        bypass_visible = "bypass_actors" in ruleset
        bypass_count: int | None = None
        if bypass_visible:
            bypass_count = len(_list(ruleset.get("bypass_actors"), "tag ruleset bypass actors"))
        rule_types = sorted(
            _nonempty_string(_mapping(item, "tag rule").get("type"), "tag rule type")
            for item in _list(ruleset.get("rules"), "tag rules")
        )
        if not {"deletion", "update"}.issubset(rule_types):
            continue
        receipt: dict[str, object] = {
            "id": _positive_integer(ruleset.get("id"), "tag ruleset id"),
            "name": _nonempty_string(ruleset.get("name"), "tag ruleset name"),
            "enforcement": "active",
            "include": includes,
            "exclude": excludes,
            "global_bypass_actors_visible": bypass_visible,
            "configured_bypass_actor_count": bypass_count,
            "current_workflow_token_can_bypass": ruleset.get("current_user_can_bypass") != "never",
            "rules": rule_types,
        }
        _verify_tag_protection(receipt)
        matching.append(receipt)
    if not matching:
        raise ProvenanceError(
            "no active workflow-token-protected tag ruleset prevents v* deletion and rewrites"
        )
    return sorted(matching, key=lambda item: cast(int, item["id"]))


def _verify_tag_protection(ruleset: Mapping[str, object]) -> None:
    _positive_integer(ruleset.get("id"), "tag protection id")
    _nonempty_string(ruleset.get("name"), "tag protection name")
    includes = ruleset.get("include")
    rules = ruleset.get("rules")
    checks = {
        "active": ruleset.get("enforcement") == "active",
        "covers_release_tags": isinstance(includes, list)
        and (RELEASE_TAG_PATTERN in includes or "~ALL" in includes),
        "no_exclusions": ruleset.get("exclude") == [],
        "current_workflow_token_cannot_bypass": ruleset.get("current_workflow_token_can_bypass")
        is False,
        "prevents_deletion": isinstance(rules, list) and "deletion" in rules,
        "prevents_rewrite": isinstance(rules, list) and "update" in rules,
    }
    visible = ruleset.get("global_bypass_actors_visible")
    count = ruleset.get("configured_bypass_actor_count")
    if visible is True:
        _integer(count, "visible configured tag bypass actor count")
    elif visible is False:
        if count is not None:
            checks["hidden_bypass_not_claimed_empty"] = False
    else:
        checks["bypass_visibility"] = False
    rejected = sorted(name for name, passed in checks.items() if not passed)
    if rejected:
        raise ProvenanceError(f"tag protection is incomplete: {rejected}")


def _environment_receipts(environments: Mapping[str, object]) -> list[dict[str, object]]:
    if set(environments) != set(REQUIRED_ENVIRONMENTS):
        raise ProvenanceError(
            "live environment set differs from the release contract: "
            f"expected={list(REQUIRED_ENVIRONMENTS)}, observed={sorted(environments)}"
        )
    receipts: list[dict[str, object]] = []
    for name in REQUIRED_ENVIRONMENTS:
        environment = _mapping(environments[name], f"environment {name}")
        if environment.get("name") != name:
            raise ProvenanceError(f"environment API identity mismatch for {name}")
        protection_rules = _list(
            environment.get("protection_rules", []), f"environment {name} protection rules"
        )
        protection_types = sorted(
            _nonempty_string(
                _mapping(item, f"environment {name} protection rule").get("type"),
                f"environment {name} protection rule type",
            )
            for item in protection_rules
        )
        if protection_types != ["branch_policy"]:
            raise ProvenanceError(
                f"environment {name} must enforce only protected-branch deployment policy"
            )
        branch_policy = _mapping(
            environment.get("deployment_branch_policy"),
            f"environment {name} deployment branch policy",
        )
        if (
            branch_policy.get("protected_branches") is not True
            or branch_policy.get("custom_branch_policies") is not False
        ):
            raise ProvenanceError(
                f"environment {name} does not require a protected-branch deployment policy"
            )
        if environment.get("can_admins_bypass") is not False:
            raise ProvenanceError(f"environment {name} must disable administrator bypass")
        receipts.append(
            {
                "name": name,
                "protection_rules": ["branch_policy"],
                "required_reviewers": [],
                "required_reviewers_available": False,
                "can_admins_bypass": False,
                "deployment_branch_policy": {
                    "protected_branches": True,
                    "custom_branch_policies": False,
                },
            }
        )
    return receipts


def _immutable_releases_receipt(document: object) -> dict[str, object]:
    """Normalize the administration-read immutable-release policy response."""
    policy = _mapping(document, "immutable releases policy")
    if set(policy) != {"enabled", "enforced_by_owner"}:
        raise ProvenanceError("immutable releases policy contains an unexpected schema")
    if policy.get("enabled") is not True or policy.get("enforced_by_owner") is not True:
        raise ProvenanceError(
            "immutable releases must be enabled and enforced by the repository owner"
        )
    return {
        "api_version": IMMUTABLE_RELEASES_API_VERSION,
        "enabled": True,
        "enforced_by_owner": True,
    }


def _load_json(path: Path) -> object:
    try:
        details = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(details.st_mode):
            raise ProvenanceError(f"JSON document is not a regular file: {path}")
        if details.st_size > MAX_JSON_DOCUMENT_BYTES:
            raise ProvenanceError(f"JSON document exceeds {MAX_JSON_DOCUMENT_BYTES} bytes: {path}")
        with path.open("rb") as stream:
            content = stream.read(MAX_JSON_DOCUMENT_BYTES + 1)
        if len(content) > MAX_JSON_DOCUMENT_BYTES:
            raise ProvenanceError(f"JSON document exceeds {MAX_JSON_DOCUMENT_BYTES} bytes: {path}")
        return cast(object, json.loads(content.decode("utf-8-sig")))
    except ProvenanceError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProvenanceError(f"could not read JSON document {path}: {exc}") from exc


def _github_fetcher(token: str) -> GitHubJsonFetcher:
    if not token:
        raise ProvenanceError("GH_TOKEN is required to verify live repository governance")

    def fetch(path: str) -> object:
        if not path.startswith("repos/") or "://" in path or ".." in path:
            raise ProvenanceError("GitHub API path is outside the repository allowlist")
        request = urllib.request.Request(  # noqa: S310 - fixed api.github.com origin.
            f"https://api.github.com/{path}",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "User-Agent": "clio-relay-release-governance",
                "X-GitHub-Api-Version": "2026-03-10",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
                content = response.read(4 * 1024 * 1024 + 1)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise GitHubNotFound(f"GitHub resource was not found: {path}") from exc
            raise ProvenanceError(
                f"GitHub governance query failed for {path}: HTTP {exc.code}"
            ) from exc
        except (OSError, urllib.error.URLError) as exc:
            raise ProvenanceError(f"GitHub governance query failed for {path}: {exc}") from exc
        if len(content) > 4 * 1024 * 1024:
            raise ProvenanceError(f"GitHub governance response is too large for {path}")
        try:
            return cast(object, json.loads(content))
        except json.JSONDecodeError as exc:
            raise ProvenanceError(f"GitHub governance response is invalid for {path}") from exc

    return fetch


def _write_json(path: Path, document: object) -> None:
    encoded = json.dumps(document, indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(encoded, encoding="utf-8", newline="\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ProvenanceError(f"{field} must be a JSON object with string keys")
    typed = cast(dict[object, object], value)
    if not all(isinstance(key, str) for key in typed):
        raise ProvenanceError(f"{field} must be a JSON object with string keys")
    return {cast(str, key): item for key, item in typed.items()}


def _list(value: object, field: str) -> list[object]:
    if not isinstance(value, list):
        raise ProvenanceError(f"{field} must be a JSON array")
    return list(cast(list[object], value))


def _string_list(value: object, field: str) -> list[str]:
    items = _list(value, field)
    if not all(isinstance(item, str) and item for item in items):
        raise ProvenanceError(f"{field} must contain only non-empty strings")
    return [cast(str, item) for item in items]


def _integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ProvenanceError(f"{field} must be an integer")
    return value


def _positive_integer(value: object, field: str) -> int:
    integer = _integer(value, field)
    if integer < 1:
        raise ProvenanceError(f"{field} must be positive")
    return integer


def _nonempty_string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProvenanceError(f"{field} must be a non-empty string")
    return value


def _https_url(value: object, field: str) -> str:
    url = _nonempty_string(value, field)
    if not url.startswith("https://"):
        raise ProvenanceError(f"{field} must be an HTTPS URL")
    return url


def _rfc3339_timestamp(value: object, field: str) -> datetime:
    timestamp = _nonempty_string(value, field)
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProvenanceError(f"{field} must be an RFC 3339 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ProvenanceError(f"{field} must include a timezone")
    return parsed


def _sha256_bounded_file(path: Path, *, maximum_bytes: int) -> str:
    digest = hashlib.sha256()
    observed = 0
    try:
        with path.open("rb") as stream:
            while True:
                chunk = stream.read(min(1024 * 1024, maximum_bytes + 1 - observed))
                if not chunk:
                    break
                observed += len(chunk)
                if observed > maximum_bytes:
                    raise ProvenanceError(f"file exceeds {maximum_bytes} bytes: {path}")
                digest.update(chunk)
    except ProvenanceError:
        raise
    except OSError as exc:
        raise ProvenanceError(f"could not hash bounded file {path}: {exc}") from exc
    return digest.hexdigest()


def _validate_tag_payload_names(names: Sequence[str]) -> None:
    if any(
        not name
        or name != Path(name).name
        or "/" in name
        or "\\" in name
        or re.fullmatch(r"[A-Za-z0-9_.+-]+", name) is None
        for name in names
    ):
        raise ProvenanceError("Actions artifact archive contains an unsafe path")
    wheels = [name for name in names if name.endswith(".whl")]
    sdists = [name for name in names if name.endswith(".tar.gz")]
    fixed = set(names) - set(wheels) - set(sdists)
    if len(wheels) != 1 or len(sdists) != 1 or fixed != set(TAG_PAYLOAD_FIXED_FILES):
        raise ProvenanceError(
            "Actions artifact archive file set does not match the inert tag payload contract"
        )


def _validate_candidate_payload_names(names: Sequence[str]) -> None:
    _validate_flat_artifact_names(names)
    wheels = [name for name in names if name.endswith(".whl")]
    sdists = [name for name in names if name.endswith(".tar.gz")]
    fixed = set(names) - set(wheels) - set(sdists)
    if len(wheels) != 1 or len(sdists) != 1 or fixed != set(CANDIDATE_PAYLOAD_FIXED_FILES):
        raise ProvenanceError(
            "Actions artifact archive file set does not match the sealed candidate contract"
        )


def _validate_tag_binding_payload_names(names: Sequence[str]) -> None:
    _validate_flat_artifact_names(names)
    if list(names) != ["TAG-BINDING.json"]:
        raise ProvenanceError("tag binding artifact must contain only TAG-BINDING.json")


def _validate_flat_artifact_names(names: Sequence[str]) -> None:
    if any(
        not name
        or name != Path(name).name
        or "/" in name
        or "\\" in name
        or re.fullmatch(r"[A-Za-z0-9_.+-]+", name) is None
        for name in names
    ):
        raise ProvenanceError("Actions artifact archive contains an unsafe path")


def _tag_payload_file_limit(name: str) -> int:
    if name.endswith((".whl", ".tar.gz")):
        return MAX_DISTRIBUTION_BYTES
    if name == "validation-local.json":
        return MAX_FIXED_JSON_BYTES
    if name == "CANDIDATE-BUILD.json":
        return MAX_FIXED_JSON_BYTES
    if name == "SHA256SUMS":
        return MAX_MANIFEST_BYTES
    raise ProvenanceError(f"unsupported tag payload file: {name}")


def _validate_promotion_payload_names(names: Sequence[str]) -> None:
    safe_names: set[str] = set()
    for name in names:
        if (
            not name
            or name.startswith("/")
            or "\\" in name
            or len(name) > 512
            or any(part in {"", ".", ".."} for part in name.split("/"))
            or re.fullmatch(r"[A-Za-z0-9_./+-]+", name) is None
        ):
            raise ProvenanceError("promotion artifact archive contains an unsafe path")
        safe_names.add(name)
    packages = {name for name in safe_names if name.startswith("packages/")}
    wheels = {name for name in packages if name.endswith(".whl")}
    sdists = {name for name in packages if name.endswith(".tar.gz")}
    fixed = {
        "evidence/SHA256SUMS",
        "evidence/validation-local.json",
        "evidence/CI-STATUS.json",
        "evidence/REPOSITORY-GOVERNANCE.json",
        "evidence/DISTRIBUTION-ARCHIVES.json",
        "evidence/LIVE-VALIDATION-BINDING.json",
        "evidence/candidate-release-gate-1.0.json",
        "evidence/VALIDATION-SHA256SUMS",
    }
    reports = {
        name
        for name in safe_names
        if re.fullmatch(r"evidence/live/validation-[A-Za-z0-9._-]+\.json", name)
    }
    recovery = {
        name
        for name in safe_names
        if name
        in {
            "evidence/recovery/candidate-release-gate-1.0.json",
            "evidence/recovery/PYPI-PROMOTION.json",
        }
    }
    if (
        len(wheels) != 1
        or len(sdists) != 1
        or packages != wheels | sdists
        or not reports
        or safe_names != packages | fixed | reports | recovery
    ):
        raise ProvenanceError("promotion artifact archive file set does not match")


def _promotion_payload_file_limit(name: str) -> int:
    if name.startswith("packages/") and name.endswith((".whl", ".tar.gz")):
        return MAX_DISTRIBUTION_BYTES
    if name.endswith(".json"):
        return MAX_FIXED_JSON_BYTES
    if name.endswith("SHA256SUMS"):
        return MAX_MANIFEST_BYTES
    raise ProvenanceError(f"unsupported promotion payload file: {name}")


def _release_asset_file_limit(name: str) -> int:
    if name.endswith((".whl", ".tar.gz")):
        return MAX_DISTRIBUTION_BYTES
    if name.endswith(".json"):
        return MAX_FIXED_JSON_BYTES
    if name == "SHA256SUMS":
        return MAX_MANIFEST_BYTES
    return MAX_RELEASE_ASSET_BYTES


def _validate_release_asset_name(name: str) -> None:
    if (
        name != Path(name).name
        or "/" in name
        or "\\" in name
        or re.fullmatch(r"[A-Za-z0-9_.+-]+", name) is None
    ):
        raise ProvenanceError(f"release asset name is unsafe: {name}")


def _verify_checksum_manifest(directory: Path, *, expected_names: set[str]) -> None:
    manifest = directory / "SHA256SUMS"
    try:
        details = manifest.lstat()
        if manifest.is_symlink() or not stat.S_ISREG(details.st_mode):
            raise ProvenanceError("checksum manifest is not a regular file")
        if details.st_size < 1 or details.st_size > MAX_MANIFEST_BYTES:
            raise ProvenanceError("checksum manifest size is invalid")
        content = manifest.read_text(encoding="utf-8")
    except ProvenanceError:
        raise
    except (OSError, UnicodeDecodeError) as exc:
        raise ProvenanceError(f"could not read checksum manifest: {exc}") from exc
    declared: dict[str, str] = {}
    for line in content.splitlines():
        match = re.fullmatch(r"([0-9a-f]{64}) [ *]([A-Za-z0-9_.+-]+)", line)
        if match is None or match.group(2) in declared:
            raise ProvenanceError("checksum manifest contains an invalid or duplicate entry")
        declared[match.group(2)] = match.group(1)
    if set(declared) != expected_names:
        raise ProvenanceError("checksum manifest subject set does not match the payload")
    for name, digest in declared.items():
        maximum = _tag_payload_file_limit(name)
        path = directory / name
        if _sha256_bounded_file(path, maximum_bytes=maximum) != digest:
            raise ProvenanceError(f"checksum manifest digest mismatch: {name}")


def _validate_repository(repository: str) -> None:
    if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) is None:
        raise ProvenanceError("repository must be an owner/name slug")


def _validate_commit(commit: str) -> None:
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise ProvenanceError("source commit must be a lowercase 40-character SHA")


def _validate_git_tree(tree: str) -> None:
    if re.fullmatch(r"[0-9a-f]{40}", tree) is None:
        raise ProvenanceError("source tree must be a lowercase 40-character Git object id")


def _canonical_json_sha256(document: object) -> str:
    encoded = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_tag(tag: str) -> None:
    if re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+(?:[.-][0-9A-Za-z.-]+)?", tag) is None:
        raise ProvenanceError("release tag is invalid")


def _error(message: str) -> NoReturn:
    raise SystemExit(message)


def main(argv: Sequence[str] | None = None) -> int:
    """Build or verify canonical release prerequisite receipts."""
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    select = subparsers.add_parser("select-ci-run")
    select.add_argument("--runs", type=Path, required=True)
    select.add_argument("--repository", required=True)
    select.add_argument("--source-commit", required=True)
    select.add_argument("--output", type=Path, required=True)

    build_ci = subparsers.add_parser("build-ci-status")
    build_ci.add_argument("--runs", type=Path, required=True)
    build_ci.add_argument("--jobs", type=Path, required=True)
    build_ci.add_argument("--candidate-build", type=Path, required=True)
    build_ci.add_argument("--candidate-artifact", type=Path, required=True)
    build_ci.add_argument("--tag-binding", type=Path, required=True)
    build_ci.add_argument("--repository", required=True)
    build_ci.add_argument("--source-commit", required=True)
    build_ci.add_argument("--output", type=Path, required=True)

    artifact_manifest = subparsers.add_parser("actions-artifact-manifest")
    artifact_manifest.add_argument("--run", type=Path, required=True)
    artifact_manifest.add_argument("--artifacts", type=Path, required=True)
    artifact_manifest.add_argument("--repository", required=True)
    artifact_manifest.add_argument("--source-commit", required=True)
    artifact_manifest.add_argument("--tag", required=True)
    artifact_manifest.add_argument("--run-id", type=int, required=True)
    artifact_manifest.add_argument("--run-attempt", type=int, required=True)
    artifact_manifest.add_argument("--artifact-name", required=True)
    artifact_manifest.add_argument(
        "--artifact-kind",
        choices=("candidate", "tag-binding", "tag-payload", "promotion"),
        required=True,
    )
    artifact_manifest.add_argument("--source-tree")
    artifact_manifest.add_argument("--output", type=Path, required=True)

    extract_artifact = subparsers.add_parser("extract-actions-artifact")
    extract_artifact.add_argument("--manifest", type=Path, required=True)
    extract_artifact.add_argument("--archive", type=Path, required=True)
    extract_artifact.add_argument("--output-dir", type=Path, required=True)

    candidate_manifest = subparsers.add_parser("candidate-manifest")
    candidate_manifest.add_argument("--candidate-dir", type=Path, required=True)

    staged_assets = subparsers.add_parser("staged-assets")
    staged_assets.add_argument("--release", type=Path, required=True)
    staged_assets.add_argument("--candidate-dir", type=Path, required=True)
    staged_assets.add_argument("--output", type=Path, required=True)

    verify_ci = subparsers.add_parser("verify-ci-status")
    verify_ci.add_argument("--receipt", type=Path, required=True)
    verify_ci.add_argument("--repository", required=True)
    verify_ci.add_argument("--source-commit", required=True)

    build_governance = subparsers.add_parser("build-governance")
    build_governance.add_argument("--main-effective-rules", type=Path, required=True)
    build_governance.add_argument("--protected-branches", type=Path, required=True)
    build_governance.add_argument("--branch-rulesets", type=Path, required=True)
    build_governance.add_argument("--tag-rulesets", type=Path, required=True)
    build_governance.add_argument("--environments-dir", type=Path, required=True)
    build_governance.add_argument("--immutable-releases", type=Path, required=True)
    build_governance.add_argument("--repository", required=True)
    build_governance.add_argument("--source-commit", required=True)
    build_governance.add_argument("--tag", required=True)
    build_governance.add_argument("--output", type=Path, required=True)

    verify_governance = subparsers.add_parser("verify-governance")
    verify_governance.add_argument("--receipt", type=Path, required=True)
    verify_governance.add_argument("--repository", required=True)
    verify_governance.add_argument("--source-commit", required=True)
    verify_governance.add_argument("--tag", required=True)

    verify_live_governance = subparsers.add_parser("verify-live-governance")
    verify_live_governance.add_argument("--receipt", type=Path, required=True)
    verify_live_governance.add_argument("--repository", required=True)
    verify_live_governance.add_argument("--source-commit", required=True)
    verify_live_governance.add_argument("--tag", required=True)

    verify_live_release = subparsers.add_parser("verify-live-release")
    verify_live_release.add_argument("--repository", required=True)
    verify_live_release.add_argument("--tag", required=True)
    verify_live_release.add_argument("--source-commit", required=True)
    verify_live_release.add_argument("--draft", choices=("true", "false", "any"), required=True)
    verify_live_release.add_argument("--prerelease", choices=("true", "false"), required=True)
    verify_live_release.add_argument("--immutable", choices=("true", "false", "any"), default="any")

    resolve_release = subparsers.add_parser("resolve-live-release")
    resolve_release.add_argument("--repository", required=True)
    resolve_release.add_argument("--tag", required=True)
    resolve_release.add_argument("--draft", choices=("true", "false", "any"), required=True)
    resolve_release.add_argument("--allow-absent", action="store_true")
    resolve_release.add_argument("--immutable", choices=("true", "false", "any"), default="any")
    resolve_release.add_argument("--output", type=Path, required=True)

    mutation_authority = subparsers.add_parser("mutation-authority")
    mutation_authority.add_argument("--governance-receipt", type=Path, required=True)
    mutation_authority.add_argument("--repository", required=True)
    mutation_authority.add_argument("--source-commit", required=True)
    mutation_authority.add_argument("--tag", required=True)
    mutation_authority.add_argument("--workflow-ref", required=True)
    mutation_authority.add_argument("--workflow-sha", required=True)
    mutation_authority.add_argument(
        "--release-state",
        choices=("absent", "present"),
        required=True,
    )
    mutation_authority.add_argument("--draft", choices=("true", "false", "any"), required=True)

    report_assets = subparsers.add_parser("report-assets")
    report_assets.add_argument("--release", type=Path, required=True)
    report_assets.add_argument("--kind", choices=("candidate", "released"), required=True)
    report_assets.add_argument("--matrix", type=Path, required=True)
    report_assets.add_argument("--report-dir", type=Path)
    report_assets.add_argument("--output", type=Path, required=True)

    distributions = subparsers.add_parser("distribution-archives")
    distributions.add_argument("--wheel", type=Path, required=True)
    distributions.add_argument("--sdist", type=Path, required=True)
    distributions.add_argument("--project", required=True)
    distributions.add_argument("--version", required=True)
    distributions.add_argument("--output", type=Path, required=True)

    exact_assets = subparsers.add_parser("exact-release-assets")
    exact_assets.add_argument("--release", type=Path, required=True)
    exact_assets.add_argument("--next-assets-page", type=Path, required=True)
    exact_assets.add_argument("--page-size", type=int, required=True)
    exact_assets.add_argument("--asset", type=Path, action="append", required=True)
    exact_destination = exact_assets.add_mutually_exclusive_group(required=True)
    exact_destination.add_argument("--output", type=Path)
    exact_destination.add_argument("--verify-existing", type=Path)

    candidate_build = subparsers.add_parser("candidate-build-receipt")
    candidate_build.add_argument("--candidate-dir", type=Path, required=True)
    candidate_build.add_argument("--reports-dir", type=Path, required=True)
    candidate_build.add_argument("--repository", required=True)
    candidate_build.add_argument("--source-commit", required=True)
    candidate_build.add_argument("--source-tree", required=True)
    candidate_build.add_argument("--event", required=True)
    candidate_build.add_argument("--run-id", type=int, required=True)
    candidate_build.add_argument("--run-attempt", type=int, required=True)
    candidate_build.add_argument("--head-ref", required=True)
    candidate_build.add_argument("--base-ref", required=True)
    candidate_build.add_argument("--output", type=Path, required=True)

    tag_binding = subparsers.add_parser("tag-binding")
    tag_binding.add_argument("--candidate-build", type=Path, required=True)
    tag_binding.add_argument("--candidate-artifact", type=Path, required=True)
    tag_binding.add_argument("--pulls", type=Path, required=True)
    tag_binding.add_argument("--repository", required=True)
    tag_binding.add_argument("--source-commit", required=True)
    tag_binding.add_argument("--source-tree", required=True)
    tag_binding.add_argument("--tag", required=True)
    tag_binding.add_argument("--output", type=Path, required=True)

    args = parser.parse_args(argv)
    try:
        if args.command == "select-ci-run":
            selected = select_ci_run(
                _load_json(args.runs),
                repository=args.repository,
                source_commit=args.source_commit,
            )
            _write_json(args.output, selected)
        elif args.command == "build-ci-status":
            receipt = build_ci_status(
                _load_json(args.runs),
                _load_json(args.jobs),
                _load_json(args.candidate_build),
                _load_json(args.candidate_artifact),
                _load_json(args.tag_binding),
                repository=args.repository,
                source_commit=args.source_commit,
            )
            _write_json(args.output, receipt)
        elif args.command == "actions-artifact-manifest":
            manifest = build_actions_artifact_manifest(
                _load_json(args.run),
                _load_json(args.artifacts),
                repository=args.repository,
                source_commit=args.source_commit,
                tag=args.tag,
                run_id=args.run_id,
                run_attempt=args.run_attempt,
                artifact_name=args.artifact_name,
                artifact_kind=args.artifact_kind,
                source_tree=args.source_tree,
            )
            _write_json(args.output, manifest)
        elif args.command == "extract-actions-artifact":
            verify_actions_artifact_archive(
                _load_json(args.manifest),
                args.archive,
                args.output_dir,
            )
        elif args.command == "candidate-manifest":
            write_candidate_checksum_manifest(args.candidate_dir)
        elif args.command == "staged-assets":
            plan = build_staged_release_asset_plan(
                _load_json(args.release),
                args.candidate_dir,
            )
            _write_json(args.output, plan)
        elif args.command == "verify-ci-status":
            verify_ci_status(
                _load_json(args.receipt),
                repository=args.repository,
                source_commit=args.source_commit,
            )
        elif args.command == "build-governance":
            environment_documents: dict[str, object] = {}
            for name in REQUIRED_ENVIRONMENTS:
                environment = _mapping(
                    _load_json(args.environments_dir / f"{name}.json"),
                    f"environment {name}",
                )
                environment_documents[name] = environment
            receipt = build_repository_governance(
                _load_json(args.main_effective_rules),
                _load_json(args.protected_branches),
                _load_json(args.branch_rulesets),
                _load_json(args.tag_rulesets),
                environment_documents,
                _load_json(args.immutable_releases),
                repository=args.repository,
                source_commit=args.source_commit,
                tag=args.tag,
            )
            _write_json(args.output, receipt)
        elif args.command == "verify-governance":
            verify_repository_governance(
                _load_json(args.receipt),
                repository=args.repository,
                source_commit=args.source_commit,
                tag=args.tag,
            )
        elif args.command == "verify-live-governance":
            verify_live_repository_governance(
                _load_json(args.receipt),
                repository=args.repository,
                source_commit=args.source_commit,
                tag=args.tag,
                fetch_json=_github_fetcher(os.environ.get("GH_TOKEN", "")),
                fetch_admin_json=_github_fetcher(os.environ.get("GH_ADMIN_READ_TOKEN", "")),
            )
        elif args.command == "verify-live-release":
            verify_live_release_identity(
                repository=args.repository,
                tag=args.tag,
                source_commit=args.source_commit,
                expect_draft=None if args.draft == "any" else args.draft == "true",
                expect_prerelease=args.prerelease == "true",
                expect_immutable=(None if args.immutable == "any" else args.immutable == "true"),
                fetch_json=_github_fetcher(os.environ.get("GH_TOKEN", "")),
            )
        elif args.command == "resolve-live-release":
            release = resolve_live_release(
                repository=args.repository,
                tag=args.tag,
                expect_draft=None if args.draft == "any" else args.draft == "true",
                fetch_json=_github_fetcher(os.environ.get("GH_TOKEN", "")),
                allow_absent=args.allow_absent,
                expect_immutable=(None if args.immutable == "any" else args.immutable == "true"),
            )
            _write_json(args.output, release)
        elif args.command == "mutation-authority":
            verify_live_mutation_authority(
                _load_json(args.governance_receipt),
                repository=args.repository,
                source_commit=args.source_commit,
                tag=args.tag,
                workflow_ref=args.workflow_ref,
                workflow_sha=args.workflow_sha,
                release_state=args.release_state,
                expect_draft=None if args.draft == "any" else args.draft == "true",
                fetch_json=_github_fetcher(os.environ.get("GH_TOKEN", "")),
                fetch_admin_json=_github_fetcher(os.environ.get("GH_ADMIN_READ_TOKEN", "")),
            )
        elif args.command == "report-assets":
            manifest = build_validation_report_asset_manifest(
                _load_json(args.release),
                kind=args.kind,
                acceptance_matrix=_load_json(args.matrix),
            )
            if args.report_dir is not None:
                verify_downloaded_validation_report_assets(manifest, args.report_dir)
            _write_json(args.output, manifest)
        elif args.command == "distribution-archives":
            receipt = build_distribution_archive_receipt(
                args.wheel,
                args.sdist,
                project=args.project,
                version=args.version,
            )
            _write_json(args.output, receipt)
        elif args.command == "exact-release-assets":
            release_document = _load_json(args.release)
            if args.verify_existing is not None:
                verify_exact_release_asset_inventory(
                    _load_json(args.verify_existing),
                    release_document,
                    args.asset,
                    next_page_document=_load_json(args.next_assets_page),
                    page_size=args.page_size,
                )
            else:
                inventory = build_exact_release_asset_inventory(
                    release_document,
                    args.asset,
                    next_page_document=_load_json(args.next_assets_page),
                    page_size=args.page_size,
                )
                _write_json(args.output, inventory)
        elif args.command == "candidate-build-receipt":
            receipt = build_candidate_build_receipt(
                args.candidate_dir,
                args.reports_dir,
                repository=args.repository,
                source_commit=args.source_commit,
                source_tree=args.source_tree,
                event=args.event,
                run_id=args.run_id,
                run_attempt=args.run_attempt,
                head_ref=args.head_ref,
                base_ref=args.base_ref,
            )
            _write_json(args.output, receipt)
        elif args.command == "tag-binding":
            binding = build_tag_binding(
                _load_json(args.candidate_build),
                _load_json(args.candidate_artifact),
                _load_json(args.pulls),
                repository=args.repository,
                source_commit=args.source_commit,
                source_tree=args.source_tree,
                tag=args.tag,
            )
            _write_json(args.output, binding)
        else:  # pragma: no cover - argparse owns command validation.
            _error(f"unsupported command: {args.command}")
    except ProvenanceError as exc:
        _error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
