"""Standalone live probe for kernel-enforced process containment."""

from __future__ import annotations

import importlib
import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Mapping
from typing import Any, Protocol, cast


class _ContainmentModule(Protocol):
    def containment_capability(self) -> dict[str, object]: ...

    def owner_environment(self, environment: Mapping[str, str] | None) -> dict[str, str]: ...

    def spawn_owned_process(
        self, command: list[str], **popen_kwargs: Any
    ) -> subprocess.Popen[str]: ...

    def owned_process_metadata(self, process_id: int) -> dict[str, object]: ...

    def process_start_identity(self, process_id: int) -> str | None: ...

    def terminate_owned_process(self, process: subprocess.Popen[str]) -> None: ...

    def release_owned_process(self, process: subprocess.Popen[str]) -> None: ...


def _load_containment_module() -> _ContainmentModule:
    try:
        module = importlib.import_module("process_containment")
    except ModuleNotFoundError:
        module = importlib.import_module("clio_relay.process_containment")
    return cast(_ContainmentModule, module)


process_containment = _load_containment_module()


def main() -> int:
    """Run a setsid escape attempt and emit machine-readable cleanup evidence."""
    capability = process_containment.containment_capability()
    if capability.get("enforceable") is not True:
        print(json.dumps({"capability": capability, "passed": False, "reason": "unavailable"}))
        return 2
    inner = """
import os
import subprocess
import sys
import time

child = subprocess.Popen(
    [sys.executable, "-c", "import time;time.sleep(60)"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    start_new_session=True,
)
print(child.pid, flush=True)
time.sleep(60)
"""
    process = process_containment.spawn_owned_process(
        [sys.executable, "-u", "-c", inner],
        env=process_containment.owner_environment(None),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    child_pid: int | None = None
    child_start_identity: str | None = None
    metadata: dict[str, Any] = process_containment.owned_process_metadata(process.pid)
    try:
        if process.stdout is None:
            raise RuntimeError("probe process did not expose stdout")
        child_pid = int(process.stdout.readline().strip())
        child_start_identity = process_containment.process_start_identity(child_pid)
        if child_start_identity is None:
            raise RuntimeError("probe could not capture the child process identity")
        process_containment.terminate_owned_process(process)
        deadline = time.monotonic() + 10
        while _same_process_exists(child_pid, child_start_identity) and time.monotonic() < deadline:
            time.sleep(0.05)
        residual = _same_process_exists(child_pid, child_start_identity)
        print(
            json.dumps(
                {
                    "capability": capability,
                    "ownership": metadata,
                    "outer_pid": process.pid,
                    "setsid_child_pid": child_pid,
                    "residual_process_count": 1 if residual else 0,
                    "passed": not residual,
                },
                sort_keys=True,
            )
        )
        return 1 if residual else 0
    finally:
        if process.poll() is None:
            process_containment.terminate_owned_process(process)
        process_containment.release_owned_process(process)
        if (
            child_pid is not None
            and child_start_identity is not None
            and _same_process_exists(child_pid, child_start_identity)
        ):
            os.kill(child_pid, signal.SIGTERM)


def _same_process_exists(process_id: int, expected_start_identity: str) -> bool:
    return process_containment.process_start_identity(process_id) == expected_start_identity


if __name__ == "__main__":
    raise SystemExit(main())
