"""Bound and bind the release assets used as live-acceptance validation reports.

The owner for the preflight validation-report asset manifest -- normalizing
the bounded release-asset listing into the validation-report subset,
optionally binding it exactly to the release-acceptance-matrix's ordered
report list (``release_assets``) -- and for requiring downloaded report
files to exactly match that preflight manifest. Extracted from
``ci_validation.py`` per clio-relay#231
(docs/design/relay-architecture-2026-08.md).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import cast

from clio_relay.payload_policy import _release_asset_file_limit
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
    ProvenanceError,
    _list,
    _mapping,
    _nonempty_string,
    _positive_integer,
    _sha256_bounded_file,
)
from clio_relay.release_assets import (
    _release_acceptance_matrix_stage,
    validate_release_acceptance_matrix,
)


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
