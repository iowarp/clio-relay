"""Archive-member filename/size policy and checksum-manifest read/write.

The owner for "what files, at what size limit, may a given release payload
archive contain" -- the tag/candidate/tag-binding/promotion payload name
validators, their matching per-file byte limits, and the SHA256SUMS
checksum-manifest read (``_verify_checksum_manifest``) + write
(``write_candidate_checksum_manifest``) pair that binds a directory's
contents to that policy. Extracted from ``ci_validation.py`` per
clio-relay#231 (docs/design/relay-architecture-2026-08.md).
"""

from __future__ import annotations

import os
import re
import stat
from collections.abc import Sequence
from pathlib import Path
from uuid import uuid4

from clio_relay.provenance_primitives import (
    CANDIDATE_PAYLOAD_FIXED_FILES,
    MAX_DISTRIBUTION_BYTES,
    MAX_FIXED_JSON_BYTES,
    MAX_MANIFEST_BYTES,
    MAX_RELEASE_ASSET_BYTES,
    TAG_PAYLOAD_FIXED_FILES,
    ProvenanceError,
    _sha256_bounded_file,
)


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
