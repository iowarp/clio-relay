"""Owned-generation process identity and systemd scope inspection (#231 rework).

Extracted from ``session_lifecycle.py``: bounded procfs reads, the systemd-scope
membership scan, scope termination, and the "is this the owned API leader
command line" check. These are pure process-identity primitives with no
dependency on the owned-session transaction or any wire model.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from clio_relay.errors import RelayError

_MAX_PROC_RECORD_BYTES = 1024 * 1024


@dataclass(frozen=True)
class _OwnedGenerationProcess:
    """One live process carrying the exact complete owned-generation identity."""

    pid: int
    process_group_id: int
    start_ticks: str


def _read_bounded_proc_bytes(path: Path, *, maximum_bytes: int) -> bytes:
    """Read one proc pseudo-file without following links or allocating without bound."""
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
        )
        payload = bytearray()
        while len(payload) <= maximum_bytes:
            chunk = os.read(descriptor, min(64 * 1024, maximum_bytes + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        if len(payload) > maximum_bytes:
            raise RelayError(f"process identity file exceeded its byte limit: {path}")
        return bytes(payload)
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _read_proc_identity(*, proc_root: Path, pid: int) -> _OwnedGenerationProcess:
    """Read one bounded process-group and start identity from procfs."""
    try:
        stat_payload = _read_bounded_proc_bytes(
            proc_root / str(pid) / "stat",
            maximum_bytes=_MAX_PROC_RECORD_BYTES,
        ).decode("utf-8")
        fields = stat_payload.rsplit(")", 1)[1].split()
        return _OwnedGenerationProcess(
            pid=pid,
            process_group_id=int(fields[2]),
            start_ticks=fields[19],
        )
    except (FileNotFoundError, ProcessLookupError):
        raise
    except (IndexError, OSError, UnicodeDecodeError, ValueError) as exc:
        raise RelayError(f"process identity record is invalid for pid {pid}: {exc}") from exc


def _current_linux_cgroup_path(
    *,
    pid: int,
    proc_root: Path = Path("/proc"),
    cgroup_root: Path = Path("/sys/fs/cgroup"),
) -> Path:
    """Return the exact cgroup-v2 path containing one process."""
    try:
        payload = _read_bounded_proc_bytes(
            proc_root / str(pid) / "cgroup",
            maximum_bytes=_MAX_PROC_RECORD_BYTES,
        ).decode("ascii")
    except (OSError, UnicodeDecodeError) as exc:
        raise RelayError(f"cannot inspect process cgroup for pid {pid}: {exc}") from exc
    matches = [line[3:] for line in payload.splitlines() if line.startswith("0::/")]
    relative = matches[0].lstrip("/") if len(matches) == 1 else ""
    if not relative or any(part in {"", ".", ".."} for part in relative.split("/")):
        raise RelayError(f"process cgroup-v2 identity is invalid for pid {pid}")
    try:
        root = cgroup_root.resolve(strict=True)
        observed = (root / relative).resolve(strict=True)
    except OSError as exc:
        raise RelayError(f"process cgroup-v2 path is unavailable for pid {pid}: {exc}") from exc
    if observed == root or not observed.is_relative_to(root):
        raise RelayError(f"process cgroup-v2 identity escaped its root for pid {pid}")
    return observed


def _recorded_scope_processes(
    *,
    proc_root: Path,
    systemd_unit: str,
    systemd_cgroup_path: str,
    systemd_invocation_id: str,
    systemd_description: str,
) -> list[_OwnedGenerationProcess]:
    """Enumerate only members of one exact persistent systemd generation scope."""
    from clio_relay.process_containment import recorded_linux_systemd_scope_process_ids

    try:
        process_ids = recorded_linux_systemd_scope_process_ids(
            unit=systemd_unit,
            cgroup_path=systemd_cgroup_path,
            invocation_id=systemd_invocation_id,
            description=systemd_description,
        )
    except RuntimeError as exc:
        raise RelayError(f"owned session scope identity could not be verified: {exc}") from exc
    processes: list[_OwnedGenerationProcess] = []
    for pid in process_ids:
        try:
            processes.append(_read_proc_identity(proc_root=proc_root, pid=pid))
        except (FileNotFoundError, ProcessLookupError):
            continue
    return sorted(processes, key=lambda process: process.pid)


def _terminate_recorded_session_scope(
    *,
    systemd_unit: str,
    systemd_cgroup_path: str,
    systemd_invocation_id: str,
    systemd_description: str,
) -> None:
    """Terminate one exact persisted session cgroup after InvocationID verification."""
    from clio_relay.process_containment import terminate_recorded_linux_systemd_scope

    try:
        terminate_recorded_linux_systemd_scope(
            unit=systemd_unit,
            cgroup_path=systemd_cgroup_path,
            invocation_id=systemd_invocation_id,
            description=systemd_description,
        )
    except RuntimeError as exc:
        raise RelayError(f"owned session scope termination failed: {exc}") from exc


def _is_clio_relay_api_leader(*, proc_root: Path, pid: int) -> bool:
    """Return whether one bounded command line is the owned API leader command."""
    try:
        command = (
            _read_bounded_proc_bytes(
                proc_root / str(pid) / "cmdline",
                maximum_bytes=_MAX_PROC_RECORD_BYTES,
            )
            .replace(bytes([0]), b" ")
            .decode("utf-8", errors="replace")
        )
    except (FileNotFoundError, ProcessLookupError):
        return False
    except OSError as exc:
        raise RelayError(f"cannot inspect API leader command for pid {pid}: {exc}") from exc
    return "clio-relay" in command and " api " in f" {command} " and " start" in command
