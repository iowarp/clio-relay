"""Platform-specific path forms used only at filesystem boundaries."""

from __future__ import annotations

import ntpath
import os
import re
from pathlib import Path

WINDOWS_EXTENDED_PATH_PREFIX = "\\\\?\\"
WINDOWS_EXTENDED_UNC_PREFIX = "\\\\?\\UNC\\"
WINDOWS_LEGACY_PATH_HEADROOM = 240
_WINDOWS_DEVICE_PATH_PREFIX = "\\\\.\\"
_WINDOWS_DRIVE_ABSOLUTE = re.compile(r"[A-Za-z]:\\")


def internal_filesystem_path(path: Path, *, force_extended: bool = False) -> Path:
    """Return a Windows extended-length path for internal filesystem access.

    Logical paths must remain unprefixed in configuration, reports, provenance,
    and artifact URIs. Queue and spool roots use ``force_extended=True`` so all
    descendants, including atomic temporary names, inherit one safe internal
    root. Leaf callers may omit it and receive the extended form only when the
    legacy Windows path boundary is near. Other platforms preserve the input.
    """
    if os.name != "nt":
        return path
    raw = os.fspath(path)
    was_extended = raw.casefold().startswith(WINDOWS_EXTENDED_PATH_PREFIX.casefold())
    absolute = _normalized_windows_path(raw)
    if not force_extended and not was_extended and len(absolute) < WINDOWS_LEGACY_PATH_HEADROOM:
        return path
    return Path(_windows_extended_path(absolute))


def logical_filesystem_path(path: Path) -> Path:
    """Remove an internal Windows extended-length prefix for external display."""
    if os.name != "nt":
        return path
    raw = os.fspath(path)
    folded = raw.casefold()
    if folded.startswith(WINDOWS_EXTENDED_UNC_PREFIX.casefold()):
        return Path(f"\\\\{raw[len(WINDOWS_EXTENDED_UNC_PREFIX) :]}")
    if folded.startswith(WINDOWS_EXTENDED_PATH_PREFIX.casefold()):
        logical = raw[len(WINDOWS_EXTENDED_PATH_PREFIX) :]
        if not _WINDOWS_DRIVE_ABSOLUTE.match(logical):
            raise ValueError(f"unsupported Windows extended path namespace: {path}")
        return Path(logical)
    return path


def logical_filesystem_text(value: str) -> str:
    """Remove internal Windows path namespace markers from diagnostic text."""
    if os.name != "nt":
        return value
    value = re.sub(
        re.escape(WINDOWS_EXTENDED_UNC_PREFIX),
        lambda _match: "\\\\",
        value,
        flags=re.IGNORECASE,
    )
    return re.sub(
        re.escape(WINDOWS_EXTENDED_PATH_PREFIX),
        "",
        value,
        flags=re.IGNORECASE,
    )


def _windows_extended_path(absolute: str) -> str:
    absolute = _normalized_windows_path(absolute)
    if absolute.startswith("\\\\"):
        return f"{WINDOWS_EXTENDED_UNC_PREFIX}{absolute[2:]}"
    return f"{WINDOWS_EXTENDED_PATH_PREFIX}{absolute}"


def _normalized_windows_path(raw: str) -> str:
    namespace_normalized = raw.replace("/", "\\")
    folded = namespace_normalized.casefold()
    extended_prefix = WINDOWS_EXTENDED_PATH_PREFIX.casefold()
    extended_unc_prefix = WINDOWS_EXTENDED_UNC_PREFIX.casefold()
    if folded.startswith(_WINDOWS_DEVICE_PATH_PREFIX.casefold()):
        raise ValueError(f"unsupported Windows device path namespace: {raw}")
    if folded.startswith(extended_unc_prefix):
        suffix = namespace_normalized[len(WINDOWS_EXTENDED_UNC_PREFIX) :]
        _reject_extended_dot_segments(suffix, raw=raw)
        candidate = f"\\\\{suffix}"
    elif folded.startswith(extended_prefix):
        candidate = namespace_normalized[len(WINDOWS_EXTENDED_PATH_PREFIX) :]
        _reject_extended_dot_segments(candidate, raw=raw)
        if not _WINDOWS_DRIVE_ABSOLUTE.match(candidate):
            raise ValueError(f"unsupported Windows extended path namespace: {raw}")
    else:
        candidate = namespace_normalized
    candidate_drive, candidate_tail = ntpath.splitdrive(candidate)
    if (
        candidate_drive
        and not candidate_drive.startswith("\\\\")
        and not candidate_tail.startswith("\\")
    ):
        raise ValueError(f"Windows drive-relative path is ambiguous: {raw}")
    normalized = ntpath.normpath(ntpath.abspath(candidate))
    drive, tail = ntpath.splitdrive(normalized)
    if drive.startswith("\\\\"):
        unc_parts = [part for part in drive[2:].split("\\") if part]
        if len(unc_parts) != 2 or tail not in {"", "\\"} and not tail.startswith("\\"):
            raise ValueError(f"Windows UNC path must include a server and share: {raw}")
        return normalized
    if not _WINDOWS_DRIVE_ABSOLUTE.match(normalized):
        raise ValueError(f"Windows path must resolve to a drive or UNC root: {raw}")
    return normalized


def _reject_extended_dot_segments(value: str, *, raw: str) -> None:
    components = value.replace("/", "\\").split("\\")
    if any(component in {".", ".."} for component in components):
        raise ValueError(f"Windows extended path contains a dot segment: {raw}")
