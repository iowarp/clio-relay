"""Tests for platform-specific internal filesystem path handling."""

from __future__ import annotations

import os
from pathlib import Path

from clio_relay import filesystem_paths
from clio_relay.filesystem_paths import (
    WINDOWS_EXTENDED_PATH_PREFIX,
    WINDOWS_EXTENDED_UNC_PREFIX,
    internal_filesystem_path,
    logical_filesystem_path,
)


def test_windows_extended_path_encodes_local_and_unc_paths() -> None:
    """The pure Windows encoder must preserve local and UNC identities."""
    local = filesystem_paths._windows_extended_path(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        r"C:\operator\relay\core"
    )
    unc = filesystem_paths._windows_extended_path(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        r"\\storage.example\relay\spool"
    )

    assert local == rf"{WINDOWS_EXTENDED_PATH_PREFIX}C:\operator\relay\core"
    assert unc == rf"{WINDOWS_EXTENDED_UNC_PREFIX}storage.example\relay\spool"


def test_windows_path_normalization_accepts_exact_unc_share_roots() -> None:
    """A UNC server/share root is complete even without a trailing separator."""
    raw = filesystem_paths._normalized_windows_path(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        r"\\storage.example\relay"
    )
    extended = filesystem_paths._normalized_windows_path(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        r"\\?\uNc\storage.example\relay"
    )

    assert raw == r"\\storage.example\relay"
    assert extended == raw


def test_internal_and_logical_paths_round_trip_without_public_prefixes(
    tmp_path: Path,
) -> None:
    """Windows I/O paths round-trip while non-Windows paths remain identical."""
    logical = tmp_path / "operator-configured-root"
    internal = internal_filesystem_path(logical, force_extended=True)

    if os.name == "nt":
        assert os.fspath(internal).startswith(WINDOWS_EXTENDED_PATH_PREFIX)
        assert logical_filesystem_path(internal) == logical.absolute()
    else:
        assert internal is logical
        assert logical_filesystem_path(internal) is internal


def test_logical_path_removes_extended_unc_prefix_on_windows() -> None:
    """UNC provenance never retains the private extended-length marker."""
    extended = Path(rf"{WINDOWS_EXTENDED_UNC_PREFIX}storage.example\relay\artifact.bin")

    if os.name == "nt":
        assert logical_filesystem_path(extended) == Path(r"\\storage.example\relay\artifact.bin")
    else:
        assert logical_filesystem_path(extended) is extended


def test_windows_path_normalization_rejects_device_and_extended_traversal() -> None:
    """Private namespaces and pre-extended dot segments cannot bypass normalization."""
    for hostile in (
        r"\\.\PhysicalDrive0",
        r"\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy1",
        r"\\?\C:\operator\root\..\escape",
        r"\\?\UNC\server\share\root\..\escape",
        "//?/C:/operator/root/../escape",
        r"C:operator\relative",
    ):
        try:
            filesystem_paths._normalized_windows_path(hostile)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        except ValueError:
            continue
        raise AssertionError(f"hostile Windows path was accepted: {hostile}")


def test_logical_path_accepts_mixed_case_extended_unc_on_windows() -> None:
    """Windows namespace matching is case-insensitive."""
    extended = Path(r"\\?\unc\storage.example\relay\artifact.bin")

    if os.name == "nt":
        assert logical_filesystem_path(extended) == Path(r"\\storage.example\relay\artifact.bin")
    else:
        assert logical_filesystem_path(extended) is extended
