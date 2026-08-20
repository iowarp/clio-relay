"""Tests for the wheel/sdist distribution-archive inspection owner (#231)."""

from __future__ import annotations

import gzip
import stat
import tarfile
import zipfile
from io import BytesIO
from pathlib import Path
from typing import cast

import pytest

from clio_relay import distribution_archive
from clio_relay.distribution_archive import (
    MAX_DISTRIBUTION_MEMBERS,
    build_distribution_archive_receipt,
)
from clio_relay.provenance_primitives import ProvenanceError


def _write_distribution_archives(
    directory: Path,
    *,
    metadata_version: str = "1.0.0",
    sdist_extra: tuple[tarfile.TarInfo, bytes | None] | None = None,
) -> tuple[Path, Path]:
    wheel = directory / "clio_relay-1.0.0-py3-none-any.whl"
    sdist = directory / "clio_relay-1.0.0.tar.gz"
    metadata = (
        f"Metadata-Version: 2.4\nName: clio-relay\nVersion: {metadata_version}\n\n"
    ).encode()
    with zipfile.ZipFile(wheel, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("clio_relay/__init__.py", b"__version__ = '1.0.0'\n")
        archive.writestr("clio_relay-1.0.0.dist-info/METADATA", metadata)
        archive.writestr(
            "clio_relay-1.0.0.dist-info/WHEEL",
            b"Wheel-Version: 1.0\nGenerator: tests\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        )
        archive.writestr("clio_relay-1.0.0.dist-info/RECORD", b"")
    with tarfile.open(sdist, "w:gz") as archive:
        root = tarfile.TarInfo("clio_relay-1.0.0")
        root.type = tarfile.DIRTYPE
        archive.addfile(root)
        entries = {
            "clio_relay-1.0.0/PKG-INFO": metadata,
            "clio_relay-1.0.0/pyproject.toml": b"[build-system]\nrequires=[]\n",
            "clio_relay-1.0.0/src/clio_relay/__init__.py": b"__version__='1.0.0'\n",
        }
        for name, content in entries.items():
            member = tarfile.TarInfo(name)
            member.size = len(content)
            archive.addfile(member, BytesIO(content))
        if sdist_extra is not None:
            member, content = sdist_extra
            if content is not None:
                member.size = len(content)
                archive.addfile(member, BytesIO(content))
            else:
                archive.addfile(member)
    return wheel, sdist


def test_distribution_archives_are_fully_bounded_and_identity_checked(tmp_path: Path) -> None:
    wheel, sdist = _write_distribution_archives(tmp_path)

    receipt = build_distribution_archive_receipt(
        wheel,
        sdist,
        project="clio-relay",
        version="1.0.0",
    )

    assert receipt["schema_version"] == "clio-relay.distribution-archives.v1"
    assert cast(dict[str, object], receipt["wheel"])["member_count"] == 4
    assert cast(dict[str, object], receipt["sdist"])["top_level_directory"] == ("clio_relay-1.0.0")
    assert cast(dict[str, object], receipt["limits"])["maximum_members"] == (
        MAX_DISTRIBUTION_MEMBERS
    )


@pytest.mark.parametrize(
    "mutation",
    ["traversal", "sdist_traversal", "symlink", "metadata", "member_limit", "member_size"],
)
def test_distribution_archive_preflight_rejects_adversarial_members(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    if mutation == "metadata":
        wheel, sdist = _write_distribution_archives(tmp_path, metadata_version="9.9.9")
    elif mutation in {"sdist_traversal", "symlink"}:
        link = tarfile.TarInfo("clio_relay-1.0.0/escape")
        content: bytes | None = None
        if mutation == "symlink":
            link.type = tarfile.SYMTYPE
            link.linkname = "../../escape"
        else:
            link.name = "../escape"
            content = b"escape"
        wheel, sdist = _write_distribution_archives(
            tmp_path,
            sdist_extra=(link, content),
        )
    else:
        wheel, sdist = _write_distribution_archives(tmp_path)
    if mutation == "traversal":
        with zipfile.ZipFile(wheel, "a", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("../escape", b"escape")
    elif mutation == "member_limit":
        monkeypatch.setattr(distribution_archive, "MAX_DISTRIBUTION_MEMBERS", 2)
    elif mutation == "member_size":
        monkeypatch.setattr(distribution_archive, "MAX_DISTRIBUTION_MEMBER_BYTES", 8)

    with pytest.raises(ProvenanceError):
        build_distribution_archive_receipt(
            wheel,
            sdist,
            project="clio-relay",
            version="1.0.0",
        )


def test_distribution_archive_preflight_rejects_zip_symlinks_and_expansion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wheel, sdist = _write_distribution_archives(tmp_path)
    link = zipfile.ZipInfo("clio_relay/link")
    link.external_attr = (stat.S_IFLNK | 0o777) << 16
    with zipfile.ZipFile(wheel, "a") as archive:
        archive.writestr(link, b"../../escape")
    with pytest.raises(ProvenanceError, match="not regular"):
        build_distribution_archive_receipt(
            wheel,
            sdist,
            project="clio-relay",
            version="1.0.0",
        )


def test_distribution_archive_rejects_raw_gzip_expansion_before_tar_parsing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wheel, sdist = _write_distribution_archives(tmp_path)
    monkeypatch.setattr(distribution_archive, "MAX_DISTRIBUTION_TAR_BYTES", 1024)
    sdist.write_bytes(gzip.compress(b"x" * 1025))

    with pytest.raises(ProvenanceError, match="tar stream exceeds"):
        build_distribution_archive_receipt(
            wheel,
            sdist,
            project="clio-relay",
            version="1.0.0",
        )

    wheel, sdist = _write_distribution_archives(tmp_path)
    monkeypatch.setattr(distribution_archive, "MAX_DISTRIBUTION_UNCOMPRESSED_BYTES", 32)
    with pytest.raises(ProvenanceError, match="aggregate"):
        build_distribution_archive_receipt(
            wheel,
            sdist,
            project="clio-relay",
            version="1.0.0",
        )
