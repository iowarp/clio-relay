"""The pre-tag receipt chain: sealed candidate builds and their tag binding.

The owner for sealing a merge-queue candidate build (one build + six
matrix-report validations, including the complementary POSIX/Windows
platform-marked-test partition proof) and binding a protected release tag
to that already-tested tree via its merged pull request. Extracted from
``ci_validation.py`` per clio-relay#231
(docs/design/relay-architecture-2026-08.md).
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from clio_relay.payload_policy import _verify_checksum_manifest
from clio_relay.provenance_primitives import (
    CI_WORKFLOW_PATH,
    MAX_DISTRIBUTION_BYTES,
    MAX_FIXED_JSON_BYTES,
    REQUIRED_MATRIX_JOBS,
    ProvenanceError,
    _canonical_json_sha256,
    _https_url,
    _list,
    _load_json,
    _mapping,
    _nonempty_string,
    _positive_integer,
    _sha256_bounded_file,
    _string_list,
    _validate_commit,
    _validate_git_tree,
    _validate_repository,
    _validate_tag,
)


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
