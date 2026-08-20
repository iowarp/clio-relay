"""Build and verify release receipts from live GitHub repository state."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import zipfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import NoReturn, cast

from clio_relay.branch_protection import (
    build_repository_governance,
    verify_live_repository_governance,
    verify_repository_governance,
)
from clio_relay.distribution_archive import build_distribution_archive_receipt
from clio_relay.payload_policy import (
    _promotion_payload_file_limit,
    _release_asset_file_limit,
    _tag_payload_file_limit,
    _validate_candidate_payload_names,
    _validate_promotion_payload_names,
    _validate_release_asset_name,
    _validate_tag_binding_payload_names,
    _validate_tag_payload_names,
    _verify_checksum_manifest,
    write_candidate_checksum_manifest,
)
from clio_relay.provenance_primitives import (
    CI_WORKFLOW_PATH,
    MAX_ACTIONS_ARTIFACT_ARCHIVE_BYTES,
    MAX_DISTRIBUTION_BYTES,
    MAX_FIXED_JSON_BYTES,
    MAX_MANIFEST_BYTES,
    MAX_RELEASE_ASSET_AGGREGATE_BYTES,
    MAX_RELEASE_ASSET_BYTES,
    MAX_RELEASE_ASSET_METADATA_RECORDS,
    MAX_VALIDATION_REPORT_AGGREGATE_BYTES,
    MAX_VALIDATION_REPORT_ASSETS,
    MAX_VALIDATION_REPORT_BYTES,
    RELEASE_ACCEPTANCE_MATRIX_SCHEMA,
    RELEASE_ACCEPTANCE_MATRIX_STAGES,
    RELEASE_WORKFLOW_PATH,
    REQUIRED_CI_JOBS,
    REQUIRED_ENVIRONMENTS,
    REQUIRED_MATRIX_JOBS,
    ProvenanceError,
    _canonical_json_sha256,
    _github_fetcher,
    _https_url,
    _integer,
    _list,
    _load_json,
    _mapping,
    _nonempty_string,
    _positive_integer,
    _rfc3339_timestamp,
    _sha256_bounded_file,
    _string_list,
    _validate_commit,
    _validate_git_tree,
    _validate_repository,
    _validate_tag,
    _write_json,
)
from clio_relay.release_identity import (
    resolve_live_release,
    verify_live_mutation_authority,
    verify_live_release_identity,
)


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


def compute_release_acceptance_matrix_sha256(canonical: Mapping[str, object]) -> str:
    """Compute the release-acceptance-matrix self-digest over its canonical fields.

    ``canonical`` is the matrix mapping with its own ``matrix_sha256``/
    ``acceptance_matrix_sha256`` key already removed. This is the sole digest
    computation for the release-acceptance-matrix family -- both
    :func:`validate_release_acceptance_matrix` (below) and
    ``clio_relay.release_pins``'s bump command (clio-relay#198) call this
    rather than each recomputing the hash independently.
    """
    encoded = json.dumps(
        dict(canonical), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


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
    actual_sha256 = compute_release_acceptance_matrix_sha256(canonical)
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
