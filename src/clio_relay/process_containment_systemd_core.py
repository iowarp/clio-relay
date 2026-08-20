"""Shared Linux systemd cgroup-path and property parsing primitives.

Owner module for `clio_relay.process_containment` (iowarp/clio-relay#231).
Depended on by both `process_containment_systemd_scope` and
`process_containment_systemd_query`, which never import each other -- this
module is their one shared, one-directional dependency, avoiding a cycle
between the two. `_validated_recorded_systemd_scope_path` reads `_CGROUP_ROOT`,
and `_wait_for_linux_cgroup_empty` calls `_linux_cgroup_process_ids`, through
the live facade module (`clio_relay.process_containment`) because the test
suite monkeypatches both names on the facade; every other name here is never
individually replaced by the test suite.
"""

from __future__ import annotations

import errno
import re
import time
from pathlib import Path

from clio_relay import process_containment as _pc


def _validated_systemd_cgroup_path(
    control_group: str,
    *,
    unit: str,
    cgroup_root: Path = Path("/sys/fs/cgroup"),
) -> Path:
    """Bind systemctl ControlGroup output to the exact newly-created delegated unit."""
    if (
        not control_group.startswith("/")
        or "\x00" in control_group
        or any(part in {"", ".", ".."} for part in control_group.split("/")[1:])
    ):
        raise RuntimeError("systemd scope returned an invalid ControlGroup path")
    try:
        root = cgroup_root.resolve(strict=True)
        candidate = (root / control_group.lstrip("/")).resolve(strict=True)
    except OSError as exc:
        raise RuntimeError("systemd scope ControlGroup path could not be resolved") from exc
    if candidate == root or not candidate.is_relative_to(root) or candidate.name != unit:
        raise RuntimeError("systemd scope ControlGroup did not match its exact unit")
    if not candidate.is_dir() or not (candidate / "cgroup.procs").is_file():
        raise RuntimeError("systemd scope did not expose its exact cgroup")
    return candidate


def _parse_systemd_properties(payload: str, *, expected: set[str]) -> dict[str, str]:
    """Parse one bounded duplicate-free systemctl show response."""
    properties: dict[str, str] = {}
    for line in payload.splitlines():
        name, separator, value = line.partition("=")
        if not separator or name not in expected or name in properties:
            raise RuntimeError("systemd scope returned invalid or duplicate properties")
        properties[name] = value
    if set(properties) != expected:
        raise RuntimeError("systemd scope omitted required identity properties")
    return properties


def _remaining_deadline_seconds(
    deadline: float | None,
    *,
    maximum: float,
) -> float:
    """Return one finite subprocess timeout capped by a shared absolute deadline."""
    if deadline is None:
        return maximum
    return max(0.001, min(maximum, deadline - time.monotonic()))


def _linux_cgroup_process_ids(cgroup_path: Path) -> list[int]:
    if not cgroup_path.exists():
        return []
    process_ids: set[int] = set()
    files = [cgroup_path / "cgroup.procs", *cgroup_path.glob("**/cgroup.procs")]
    if len(files) > 1024:
        raise RuntimeError(f"systemd scope exceeded cgroup traversal bound: {cgroup_path}")
    for path in files:
        try:
            lines = path.read_text(encoding="ascii").splitlines()
        except OSError as exc:
            if exc.errno in {errno.ENOENT, errno.ENODEV}:
                continue
            raise
        for line in lines:
            try:
                process_ids.add(int(line))
            except ValueError as exc:
                raise RuntimeError(f"invalid process id in {path}: {line!r}") from exc
    return sorted(process_ids)


def _wait_for_linux_cgroup_empty(
    cgroup_path: Path,
    *,
    timeout_seconds: float,
) -> list[int]:
    deadline = time.monotonic() + timeout_seconds
    residual = _pc._linux_cgroup_process_ids(cgroup_path)
    while residual and time.monotonic() < deadline:
        time.sleep(_pc.POLL_SECONDS)
        residual = _pc._linux_cgroup_process_ids(cgroup_path)
    return residual


def _validated_recorded_systemd_scope_path(cgroup_path: str, *, unit: str) -> Path:
    """Validate a recorded cgroup path even after systemd removed the leaf scope."""
    if "\x00" in cgroup_path or any(
        part in {".", ".."} for part in re.split(r"[\\/]", cgroup_path)
    ):
        raise RuntimeError("recorded systemd execution has invalid cgroup path")
    recorded = Path(cgroup_path)
    if not recorded.is_absolute() or recorded.name != unit:
        raise RuntimeError("recorded systemd execution has invalid cgroup path")
    try:
        root = _pc._CGROUP_ROOT.resolve(strict=True)
        if not root.is_dir():
            raise RuntimeError("configured cgroup v2 root is not a directory")
        candidate = recorded.resolve(strict=False)
        candidate.relative_to(root)
    except (OSError, RuntimeError, ValueError) as exc:
        raise RuntimeError("recorded cgroup is outside cgroup v2") from exc
    if candidate == root or candidate.name != unit:
        raise RuntimeError("recorded systemd execution has invalid cgroup path")
    return candidate
