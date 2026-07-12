"""Tests for deterministic, fail-closed PyPI promotion recovery records."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import cast

import pytest

from clio_relay.promotion_record import build_promotion_record, canonical_promotion_bytes


def _write_candidate(tmp_path: Path) -> tuple[Path, Path, Path, str, dict[str, object]]:
    package_dir = tmp_path / "packages"
    package_dir.mkdir()
    wheel = package_dir / "clio_relay-1.0.0-py3-none-any.whl"
    sdist = package_dir / "clio_relay-1.0.0.tar.gz"
    wheel.write_bytes(b"verified wheel")
    sdist.write_bytes(b"verified sdist")
    binding = tmp_path / "LIVE-VALIDATION-BINDING.json"
    gate = tmp_path / "candidate-release-gate-1.0.json"
    binding.write_bytes(b'{"binding":true}\n')
    gate.write_bytes(b'{"passed":true}\n')
    wheel_sha256 = hashlib.sha256(wheel.read_bytes()).hexdigest()
    document: dict[str, object] = {
        "urls": [
            {
                "filename": wheel.name,
                "digests": {"sha256": wheel_sha256},
                "url": f"https://files.example/{wheel.name}",
            },
            {
                "filename": sdist.name,
                "digests": {"sha256": hashlib.sha256(sdist.read_bytes()).hexdigest()},
                "url": f"https://files.example/{sdist.name}",
            },
        ]
    }
    return package_dir, binding, gate, wheel_sha256, document


def test_promotion_record_is_canonical_and_bound_to_current_pypi(tmp_path: Path) -> None:
    package_dir, binding, gate, wheel_sha256, document = _write_candidate(tmp_path)

    record = build_promotion_record(
        version="1.0.0",
        tag="v1.0.0",
        source_commit="a" * 40,
        wheel_sha256=wheel_sha256,
        package_dir=package_dir,
        candidate_binding=binding,
        candidate_gate=gate,
        pypi_document=document,
    )

    encoded = canonical_promotion_bytes(record)
    assert encoded.endswith(b"\n")
    assert (
        json.loads(encoded)["candidate_gate_sha256"]
        == hashlib.sha256(gate.read_bytes()).hexdigest()
    )
    distributions = cast(list[dict[str, object]], record["distributions"])
    assert [str(item["filename"]) for item in distributions] == sorted(
        path.name for path in package_dir.iterdir()
    )


def test_promotion_record_rejects_partial_or_different_pypi_state(tmp_path: Path) -> None:
    package_dir, binding, gate, wheel_sha256, document = _write_candidate(tmp_path)
    urls = document["urls"]
    assert isinstance(urls, list)
    urls.pop()

    with pytest.raises(ValueError, match="PyPI files do not match candidate"):
        build_promotion_record(
            version="1.0.0",
            tag="v1.0.0",
            source_commit="a" * 40,
            wheel_sha256=wheel_sha256,
            package_dir=package_dir,
            candidate_binding=binding,
            candidate_gate=gate,
            pypi_document=document,
        )
