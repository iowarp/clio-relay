"""POSIX process-tree discovery and signaling primitives.

Owner module for `clio_relay.process_containment` (iowarp/clio-relay#231).
Most of these names are never individually replaced by the test suite, so
callers in other owner modules import them directly rather than through the
facade. `_wait_for_exit` is the one exception: it reads `POLL_SECONDS`, which
tests do monkeypatch on the facade (e.g. to shrink a poll interval under a
short deadline), so it reads that name through the live facade module
(`clio_relay.process_containment`) instead of importing a frozen copy --
matching how the pre-split monolith resolved the same bare name at call time.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from collections.abc import Callable
from typing import cast

from clio_relay import process_containment as _pc
from clio_relay.process_containment_types import DISCOVERY_TIMEOUT_SECONDS


def _posix_process_snapshot(*, fields: tuple[str, ...]) -> str:
    command = ["ps", "-e"]
    for field in fields:
        command.extend(["-o", f"{field}="])
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=DISCOVERY_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"could not discover descendant processes: {exc}") from exc
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "ps could not discover descendant processes")
    return result.stdout


def _posix_descendant_process_ids(root_pid: int) -> list[int]:
    children: dict[int, list[int]] = {}
    for line in _posix_process_snapshot(fields=("pid", "ppid")).splitlines():
        fields = line.split()
        if len(fields) != 2:
            continue
        try:
            pid, parent_pid = (int(field) for field in fields)
        except ValueError:
            continue
        children.setdefault(parent_pid, []).append(pid)
    descendants: list[int] = []
    pending = list(children.get(root_pid, []))
    seen = {root_pid}
    while pending:
        candidate = pending.pop()
        if candidate in seen:
            continue
        seen.add(candidate)
        descendants.append(candidate)
        pending.extend(children.get(candidate, []))
    return descendants


def _posix_process_group_ids(process_group: int) -> list[int]:
    members: list[int] = []
    for line in _posix_process_snapshot(fields=("pid", "pgid", "stat")).splitlines():
        fields = line.split()
        if len(fields) < 3:
            continue
        try:
            process_id, group_id = int(fields[0]), int(fields[1])
        except ValueError:
            continue
        if group_id == process_group and not fields[2].startswith("Z"):
            members.append(process_id)
    return members


def _signal_posix_tree(
    process_ids: list[int],
    process_groups: list[int],
    requested_signal: signal.Signals,
) -> None:
    killpg = cast(Callable[[int, int], None], vars(os)["killpg"])
    for group in process_groups:
        try:
            killpg(group, requested_signal)
        except ProcessLookupError:
            continue
    for process_id in reversed(process_ids):
        try:
            os.kill(process_id, requested_signal)
        except ProcessLookupError:
            continue


def _wait_for_exit(
    *,
    process_ids: list[int],
    process_group: int | None,
    timeout_seconds: float,
) -> list[int]:
    deadline = time.monotonic() + timeout_seconds
    residual = _residual_process_ids(process_ids, process_group=process_group)
    while residual and time.monotonic() < deadline:
        time.sleep(_pc.POLL_SECONDS)
        residual = _residual_process_ids(residual, process_group=process_group)
    return residual


def _residual_process_ids(
    process_ids: list[int],
    *,
    process_group: int | None,
) -> list[int]:
    if process_group is not None:
        return _posix_process_group_ids(process_group)
    return [process_id for process_id in process_ids if _process_exists(process_id)]


def _process_exists(process_id: int) -> bool:
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    return True


def _current_posix_group() -> int:
    getpgrp = cast(Callable[[], int], vars(os)["getpgrp"])
    return getpgrp()
