from __future__ import annotations

import io
import os
import stat
import subprocess
import tarfile
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import pytest

from clio_relay import safe_archive
from clio_relay.safe_archive import (
    TarExtractionLimits,
    UnsafeArchiveError,
    safe_extract_tar,
)


@dataclass(frozen=True)
class _Member:
    name: str
    kind: bytes = tarfile.REGTYPE
    content: bytes = b"content"
    mode: int = 0o644
    linkname: str = ""


def _write_tar(path: Path, members: Iterable[_Member]) -> None:
    with tarfile.open(path, "w") as archive:
        for item in members:
            info = tarfile.TarInfo(item.name)
            info.type = item.kind
            info.mode = item.mode
            info.linkname = item.linkname
            if item.kind in {tarfile.REGTYPE, tarfile.AREGTYPE}:
                info.size = len(item.content)
                archive.addfile(info, io.BytesIO(item.content))
            else:
                info.size = 0
                archive.addfile(info)


def test_safe_extract_tar_extracts_only_regular_files_and_directories(tmp_path: Path) -> None:
    archive_path = tmp_path / "bootstrap.tar"
    _write_tar(
        archive_path,
        [
            _Member("tree", kind=tarfile.DIRTYPE, content=b"", mode=0o755),
            _Member("tree/config.json", content=b'{"ready":true}'),
            _Member("tree/run", content=b"#!/bin/sh\n", mode=0o755),
        ],
    )

    receipt = safe_extract_tar(archive_path, tmp_path / "extracted")

    assert (receipt.destination / "tree" / "config.json").read_bytes() == b'{"ready":true}'
    assert (receipt.destination / "tree" / "run").read_bytes() == b"#!/bin/sh\n"
    assert receipt.member_count == 3
    assert receipt.regular_file_count == 2
    assert receipt.directory_count == 1
    assert receipt.extracted_bytes == len(b'{"ready":true}') + len(b"#!/bin/sh\n")
    if os.name != "nt":
        assert stat.S_IMODE((receipt.destination / "tree" / "config.json").stat().st_mode) == 0o600
        assert stat.S_IMODE((receipt.destination / "tree" / "run").stat().st_mode) == 0o700


@pytest.mark.parametrize(
    "name",
    [
        "/etc/passwd",
        "C:/Windows/System32/file",
        "C:relative-file",
        "//server/share/file",
        r"\\server\share\file",
        r"folder\child",
        "../outside",
        "folder/../../outside",
    ],
)
def test_safe_extract_tar_rejects_absolute_ambiguous_and_traversal_paths(
    tmp_path: Path,
    name: str,
) -> None:
    archive_path = tmp_path / "bootstrap.tar"
    _write_tar(archive_path, [_Member(name)])
    destination = tmp_path / "extracted"

    with pytest.raises(UnsafeArchiveError):
        safe_extract_tar(archive_path, destination)

    assert not destination.exists()


@pytest.mark.parametrize(
    ("kind", "linkname"),
    [
        (tarfile.SYMTYPE, "target"),
        (tarfile.LNKTYPE, "target"),
        (tarfile.CHRTYPE, ""),
        (tarfile.BLKTYPE, ""),
        (tarfile.FIFOTYPE, ""),
    ],
)
def test_safe_extract_tar_rejects_links_devices_and_fifos(
    tmp_path: Path,
    kind: bytes,
    linkname: str,
) -> None:
    archive_path = tmp_path / "bootstrap.tar"
    _write_tar(archive_path, [_Member("unsafe", kind=kind, content=b"", linkname=linkname)])

    with pytest.raises(UnsafeArchiveError, match="not a regular file or directory"):
        safe_extract_tar(archive_path, tmp_path / "extracted")


def test_safe_extract_tar_rejects_duplicate_normalized_members(tmp_path: Path) -> None:
    archive_path = tmp_path / "bootstrap.tar"
    _write_tar(
        archive_path,
        [_Member("folder/./value"), _Member("folder//value", content=b"other")],
    )

    with pytest.raises(UnsafeArchiveError, match="duplicate normalized path"):
        safe_extract_tar(archive_path, tmp_path / "extracted")


def test_safe_extract_tar_rejects_case_collisions_when_target_is_case_insensitive(
    tmp_path: Path,
) -> None:
    archive_path = tmp_path / "bootstrap.tar"
    _write_tar(archive_path, [_Member("Tree/one"), _Member("tree/two")])

    with pytest.raises(UnsafeArchiveError, match="case-insensitive filesystem"):
        safe_extract_tar(
            archive_path,
            tmp_path / "extracted",
            case_sensitive=False,
        )


@pytest.mark.parametrize("mode", [0o666, 0o6755, 0o1755])
def test_safe_extract_tar_rejects_unsafe_modes(tmp_path: Path, mode: int) -> None:
    archive_path = tmp_path / "bootstrap.tar"
    _write_tar(archive_path, [_Member("unsafe", mode=mode)])

    with pytest.raises(UnsafeArchiveError, match="mode is unsafe"):
        safe_extract_tar(archive_path, tmp_path / "extracted")


def test_safe_extract_tar_enforces_member_count_before_creating_destination(
    tmp_path: Path,
) -> None:
    archive_path = tmp_path / "bootstrap.tar"
    _write_tar(archive_path, [_Member("one"), _Member("two")])
    destination = tmp_path / "extracted"

    with pytest.raises(UnsafeArchiveError, match="member count"):
        safe_extract_tar(
            archive_path,
            destination,
            limits=TarExtractionLimits(max_members=1),
        )

    assert not destination.exists()


def test_safe_extract_tar_enforces_member_and_total_declared_bytes(tmp_path: Path) -> None:
    member_archive = tmp_path / "member.tar"
    _write_tar(member_archive, [_Member("large", content=b"12345")])
    with pytest.raises(UnsafeArchiveError, match="member size"):
        safe_extract_tar(
            member_archive,
            tmp_path / "member-extracted",
            limits=TarExtractionLimits(max_member_bytes=4),
        )

    total_archive = tmp_path / "total.tar"
    _write_tar(
        total_archive,
        [_Member("one", content=b"123"), _Member("two", content=b"456")],
    )
    with pytest.raises(UnsafeArchiveError, match="declared bytes"):
        safe_extract_tar(
            total_archive,
            tmp_path / "total-extracted",
            limits=TarExtractionLimits(max_total_bytes=5),
        )


def test_safe_extract_tar_rejects_regular_file_parent_conflicts(tmp_path: Path) -> None:
    archive_path = tmp_path / "bootstrap.tar"
    _write_tar(archive_path, [_Member("parent"), _Member("parent/child")])

    with pytest.raises(UnsafeArchiveError, match="regular-file parent"):
        safe_extract_tar(archive_path, tmp_path / "extracted")


def test_safe_extract_tar_rejects_existing_destination(tmp_path: Path) -> None:
    archive_path = tmp_path / "bootstrap.tar"
    _write_tar(archive_path, [_Member("safe")])
    existing = tmp_path / "existing"
    existing.mkdir()
    with pytest.raises(UnsafeArchiveError, match="must be a new directory"):
        safe_extract_tar(archive_path, existing)


def test_safe_extract_tar_rejects_symlink_or_junction_destination(tmp_path: Path) -> None:
    archive_path = tmp_path / "bootstrap.tar"
    _write_tar(archive_path, [_Member("safe")])
    target = tmp_path / "target"
    target.mkdir()
    linked = tmp_path / "linked"
    if os.name == "nt":
        subprocess.run(
            ["cmd.exe", "/d", "/c", "mklink", "/J", str(linked), str(target)],
            check=True,
            capture_output=True,
            text=True,
        )
    else:
        linked.symlink_to(target, target_is_directory=True)

    with pytest.raises(UnsafeArchiveError, match="must be a new directory"):
        safe_extract_tar(archive_path, linked)

    assert not (target / "safe").exists()


def test_safe_extract_tar_does_not_use_tarfile_extractall(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive_path = tmp_path / "bootstrap.tar"
    _write_tar(archive_path, [_Member("safe")])

    def fail_extractall(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("extractall must not be used")

    monkeypatch.setattr(tarfile.TarFile, "extractall", fail_extractall)
    receipt = safe_extract_tar(archive_path, tmp_path / "extracted")

    assert (receipt.destination / "safe").read_bytes() == b"content"


def test_safe_extract_tar_completes_short_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive_path = tmp_path / "bootstrap.tar"
    content = b"short-write-proof" * 1024
    _write_tar(archive_path, [_Member("safe", content=content)])
    real_write = os.write

    def short_write(descriptor: int, data: bytes | memoryview) -> int:
        bounded = memoryview(data)[: max(1, len(data) // 3)]
        return real_write(descriptor, bounded)

    monkeypatch.setattr(safe_archive.os, "write", short_write)

    receipt = safe_extract_tar(archive_path, tmp_path / "extracted")

    assert (receipt.destination / "safe").read_bytes() == content


def test_safe_extract_tar_rejects_reparse_source_before_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive_path = tmp_path / "bootstrap.tar"
    _write_tar(archive_path, [_Member("safe")])
    original_lstat = Path.lstat

    if os.name == "nt":

        def reparse_lstat(path: Path) -> os.stat_result:
            details = original_lstat(path)
            values = list(details)
            return os.stat_result(values)

        def report_reparse(_details: os.stat_result) -> bool:
            return True

        monkeypatch.setattr(Path, "lstat", reparse_lstat)
        monkeypatch.setattr(safe_archive, "_details_are_reparse_point", report_reparse)
        source = archive_path
    else:
        source = tmp_path / "bootstrap-link.tar"
        source.symlink_to(archive_path)

    with pytest.raises(UnsafeArchiveError, match="non-reparse regular file"):
        safe_extract_tar(source, tmp_path / "extracted")


def test_safe_extract_tar_rejects_source_identity_change_during_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive_path = tmp_path / "bootstrap.tar"
    _write_tar(archive_path, [_Member("safe")])
    real_fstat = os.fstat

    def changed_identity(descriptor: int) -> os.stat_result:
        details = real_fstat(descriptor)
        values = list(details)
        values[1] += 1
        return os.stat_result(values)

    monkeypatch.setattr(safe_archive.os, "fstat", changed_identity)

    with pytest.raises(UnsafeArchiveError, match="identity changed"):
        safe_extract_tar(archive_path, tmp_path / "extracted")


def test_tar_extraction_limits_reject_boolean_and_nonpositive_values() -> None:
    with pytest.raises(ValueError, match="max_members"):
        TarExtractionLimits(max_members=0)
    with pytest.raises(ValueError, match="max_total_bytes"):
        TarExtractionLimits(max_total_bytes=True)
