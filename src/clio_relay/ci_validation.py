"""Build and verify release receipts from live GitHub repository state."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import NoReturn, cast

from clio_relay.actions_artifact import (
    build_actions_artifact_manifest,
    verify_actions_artifact_archive,
)
from clio_relay.branch_protection import (
    build_repository_governance,
    verify_live_repository_governance,
    verify_repository_governance,
)
from clio_relay.candidate_provenance import (
    build_candidate_build_receipt,
    build_tag_binding,
)
from clio_relay.ci_run_status import build_ci_status, select_ci_run, verify_ci_status
from clio_relay.distribution_archive import build_distribution_archive_receipt
from clio_relay.payload_policy import (
    _release_asset_file_limit,
    _validate_release_asset_name,
    write_candidate_checksum_manifest,
)
from clio_relay.provenance_primitives import (
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
    REQUIRED_ENVIRONMENTS,
    ProvenanceError,
    _github_fetcher,
    _list,
    _load_json,
    _mapping,
    _nonempty_string,
    _positive_integer,
    _sha256_bounded_file,
    _write_json,
)
from clio_relay.release_identity import (
    resolve_live_release,
    verify_live_mutation_authority,
    verify_live_release_identity,
)


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
