"""CI run and job identity: bind release main to its one tested run.

The owner for "which merge-queue CI run tested this exact commit, and did
every required job pass" -- selecting the sole successful `ci.yml`
merge-group run for a commit, and building/verifying the CI status receipt
that binds it to the already-sealed candidate build and tag binding
(``candidate_provenance``). Extracted from ``ci_validation.py`` per
clio-relay#231 (docs/design/relay-architecture-2026-08.md).
"""

from __future__ import annotations

import re

from clio_relay.candidate_provenance import (
    _verify_candidate_artifact_manifest,
    verify_candidate_build_receipt,
    verify_tag_binding,
)
from clio_relay.provenance_primitives import (
    CI_WORKFLOW_PATH,
    REQUIRED_CI_JOBS,
    ProvenanceError,
    _canonical_json_sha256,
    _https_url,
    _integer,
    _list,
    _mapping,
    _nonempty_string,
    _positive_integer,
    _validate_commit,
    _validate_git_tree,
    _validate_repository,
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
