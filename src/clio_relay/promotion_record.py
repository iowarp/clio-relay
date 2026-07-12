"""Create or verify the exact PyPI promotion record used by release recovery."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import urllib.request
from collections.abc import Sequence
from pathlib import Path
from typing import cast
from uuid import uuid4


def build_promotion_record(
    *,
    version: str,
    tag: str,
    source_commit: str,
    wheel_sha256: str,
    package_dir: Path,
    candidate_binding: Path,
    candidate_gate: Path,
    pypi_document: object,
) -> dict[str, object]:
    """Build a promotion record after matching PyPI to exact candidate bytes."""
    expected = _candidate_distributions(package_dir)
    wheel_names = [name for name in expected if name.endswith(".whl")]
    sdist_names = [name for name in expected if name.endswith(".tar.gz")]
    if len(wheel_names) != 1 or len(sdist_names) != 1:
        raise ValueError("promotion requires exactly one wheel and one source distribution")
    if expected[wheel_names[0]] != wheel_sha256:
        raise ValueError("promotion wheel digest does not match the verified candidate")
    published = _published_distributions(pypi_document)
    observed = {name: item["sha256"] for name, item in published.items()}
    if observed != expected:
        raise ValueError(
            f"PyPI files do not match candidate: expected={expected}, observed={observed}"
        )
    return {
        "schema_version": "1.0",
        "artifact_stage": "published",
        "tag": tag,
        "source_commit": source_commit,
        "version": version,
        "project": "clio-relay",
        "index": "https://pypi.org/",
        "wheel_sha256": wheel_sha256,
        "candidate_binding_sha256": _sha256_file(candidate_binding),
        "candidate_gate_sha256": _sha256_file(candidate_gate),
        "authorized_workflow": ".github/workflows/release-gate.yml",
        "distributions": [
            {
                "filename": name,
                "sha256": published[name]["sha256"],
                "url": published[name]["url"],
            }
            for name in sorted(published)
        ],
    }


def canonical_promotion_bytes(record: dict[str, object]) -> bytes:
    """Serialize a promotion record to its unique release-asset representation."""
    return (json.dumps(record, indent=2, sort_keys=True) + "\n").encode("utf-8")


def fetch_pypi_document(version: str) -> object:
    """Fetch authoritative release metadata for one clio-relay version."""
    url = f"https://pypi.org/pypi/clio-relay/{version}/json"
    with urllib.request.urlopen(url, timeout=30) as response:
        return cast(object, json.load(response))


def main(argv: Sequence[str] | None = None) -> int:
    """Write a canonical promotion record or verify an existing asset byte-for-byte."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--wheel-sha256", required=True)
    parser.add_argument("--package-dir", required=True, type=Path)
    parser.add_argument("--candidate-binding", required=True, type=Path)
    parser.add_argument("--candidate-gate", required=True, type=Path)
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument("--output", type=Path)
    destination.add_argument("--verify-existing", type=Path)
    args = parser.parse_args(argv)
    record = build_promotion_record(
        version=args.version,
        tag=args.tag,
        source_commit=args.source_commit,
        wheel_sha256=args.wheel_sha256,
        package_dir=args.package_dir,
        candidate_binding=args.candidate_binding,
        candidate_gate=args.candidate_gate,
        pypi_document=fetch_pypi_document(args.version),
    )
    encoded = canonical_promotion_bytes(record)
    if args.verify_existing is not None:
        if args.verify_existing.read_bytes() != encoded:
            raise SystemExit(
                "existing PyPI promotion asset differs from current PyPI and candidate bytes"
            )
        return 0
    _atomic_write(args.output, encoded)
    return 0


def _candidate_distributions(package_dir: Path) -> dict[str, str]:
    distributions = {
        path.name: _sha256_file(path)
        for path in package_dir.iterdir()
        if path.is_file() and path.name.endswith((".whl", ".tar.gz"))
    }
    if len(distributions) != 2:
        raise ValueError(
            f"expected exactly two candidate distribution files, got {sorted(distributions)}"
        )
    return distributions


def _published_distributions(document: object) -> dict[str, dict[str, str]]:
    if not isinstance(document, dict):
        raise ValueError("PyPI returned an invalid project document")
    typed_document = cast(dict[object, object], document)
    raw_urls = typed_document.get("urls")
    if not isinstance(raw_urls, list):
        raise ValueError("PyPI returned an invalid project document")
    published: dict[str, dict[str, str]] = {}
    for raw_item in cast(list[object], raw_urls):
        if not isinstance(raw_item, dict):
            raise ValueError("PyPI returned an invalid distribution entry")
        item = cast(dict[object, object], raw_item)
        filename = item.get("filename")
        digests = item.get("digests")
        url = item.get("url")
        sha256 = (
            cast(dict[object, object], digests).get("sha256") if isinstance(digests, dict) else None
        )
        if (
            not isinstance(filename, str)
            or not filename
            or filename in published
            or not isinstance(sha256, str)
            or not isinstance(url, str)
            or not url
        ):
            raise ValueError("PyPI returned invalid or duplicate distribution metadata")
        published[filename] = {"sha256": sha256, "url": url}
    return published


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write(path: Path | None, content: bytes) -> None:
    if path is None:
        raise ValueError("promotion output path is required")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_bytes(content)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
