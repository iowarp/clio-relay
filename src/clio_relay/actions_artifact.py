"""Bind and safely extract a GitHub Actions artifact for a trusted workflow run.

The owner for "does this Actions artifact belong to the exact trusted
workflow run it claims" (candidate/tag-binding/tag-payload/promotion
kinds) and safe, policy-bounded extraction of its archive
(``payload_policy``'s name/size policy), including binding an extracted
candidate payload back to its sealed build receipt
(``candidate_provenance``). Extracted from ``ci_validation.py`` per
clio-relay#231 (docs/design/relay-architecture-2026-08.md).
"""

from __future__ import annotations

import os
import re
import stat
import zipfile
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from clio_relay.candidate_provenance import verify_candidate_build_receipt
from clio_relay.payload_policy import (
    _promotion_payload_file_limit,
    _tag_payload_file_limit,
    _validate_candidate_payload_names,
    _validate_promotion_payload_names,
    _validate_tag_binding_payload_names,
    _validate_tag_payload_names,
    _verify_checksum_manifest,
)
from clio_relay.provenance_primitives import (
    CI_WORKFLOW_PATH,
    MAX_ACTIONS_ARTIFACT_ARCHIVE_BYTES,
    MAX_DISTRIBUTION_BYTES,
    MAX_FIXED_JSON_BYTES,
    MAX_MANIFEST_BYTES,
    MAX_RELEASE_ASSET_AGGREGATE_BYTES,
    RELEASE_WORKFLOW_PATH,
    REQUIRED_MATRIX_JOBS,
    ProvenanceError,
    _https_url,
    _integer,
    _list,
    _load_json,
    _mapping,
    _nonempty_string,
    _positive_integer,
    _rfc3339_timestamp,
    _sha256_bounded_file,
    _validate_commit,
    _validate_git_tree,
    _validate_repository,
    _validate_tag,
)


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
