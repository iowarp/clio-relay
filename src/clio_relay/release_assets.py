"""Release asset accounting: exact inventories, staged plans, the acceptance matrix.

The owner for binding a live GitHub release's asset listing to exact
local files (the prepublication and postpublication exact-inventory
receipts), planning only the missing staged uploads for a draft release,
and the release-acceptance-matrix family -- its self-digest, and
validating/loading the ordered live-acceptance report matrix
``clio_relay.release_pins``'s bump command and
``validation_report_assets.py`` both depend on. Extracted from
``ci_validation.py`` per clio-relay#231
(docs/design/relay-architecture-2026-08.md).
"""

from __future__ import annotations

import hashlib
import json
import re
import stat
from collections.abc import Mapping, Sequence
from pathlib import Path

from clio_relay.payload_policy import _release_asset_file_limit, _validate_release_asset_name
from clio_relay.provenance_primitives import (
    MAX_RELEASE_ASSET_AGGREGATE_BYTES,
    MAX_RELEASE_ASSET_METADATA_RECORDS,
    MAX_VALIDATION_REPORT_ASSETS,
    RELEASE_ACCEPTANCE_MATRIX_SCHEMA,
    RELEASE_ACCEPTANCE_MATRIX_STAGES,
    ProvenanceError,
    _list,
    _load_json,
    _mapping,
    _nonempty_string,
    _positive_integer,
    _sha256_bounded_file,
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
