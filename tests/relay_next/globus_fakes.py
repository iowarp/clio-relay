"""Stand-ins for Globus Compute and Globus Transfer that run on this machine.

The W11 tests use them. :class:`LocalComputeService` runs the real
:func:`relay_task` in a detached process for each task and keeps every task in
files under ``root``, so a new relay process observes it as it would a real
endpoint. :class:`LocalTransferService` maps each collection id to a directory.
Nothing here talks to Globus.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from clio_relay.adapters.globus.compute import EndpointInfo, TaskInfo
from clio_relay.adapters.globus.task import relay_task
from clio_relay.jobs.records import write_json_atomic
from clio_relay.jobs.refusals import Refusal, RefusalReason

POSIX = os.name == "posix"
#: Run as a script, this file runs one task; see :func:`main`.
WORKER = Path(__file__).resolve()


class LocalComputeService:
    """A stand-in endpoint: each task is a local process running :func:`relay_task`."""

    def __init__(self, root: Path, *, status: str = "online") -> None:
        self.root = root
        self.status = status

    def _dir(self, task_id: str) -> Path:
        return self.root / "tasks" / task_id

    async def submit(self, payload: dict[str, Any], user_config: dict[str, Any]) -> str:
        task_id = str(uuid.uuid4())
        task_dir = self._dir(task_id)
        task_dir.mkdir(parents=True)
        write_json_atomic(task_dir / "payload.json", payload)
        write_json_atomic(task_dir / "user_config.json", user_config)
        with open(task_dir / "worker.log", "ab") as log:
            subprocess.Popen(
                [sys.executable, str(WORKER), str(task_dir)],
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                start_new_session=POSIX,
            )
        return task_id

    async def task(self, task_id: str) -> TaskInfo:
        task_dir = self._dir(task_id)
        if not task_dir.is_dir():
            raise Refusal(RefusalReason.OPERATION_NOT_FOUND, f"no task {task_id}")
        result = task_dir / "result.json"
        if result.is_file():
            loaded: Any = json.loads(result.read_text(encoding="utf-8"))
            return TaskInfo(pending=False, status="success", result=cast(dict[str, Any], loaded))
        error = task_dir / "error.txt"
        if error.is_file():
            return TaskInfo(pending=False, status="failed", error=error.read_text(encoding="utf-8"))
        if (task_dir / "started").exists():
            return TaskInfo(pending=True, status="running")
        return TaskInfo(pending=True, status="waiting-for-nodes")

    async def endpoint(self) -> EndpointInfo:
        return EndpointInfo(status=self.status, facts={"user_config_keys": ["account", "queue"]})

    def submitted(self) -> list[dict[str, Any]]:
        """Return each task's payload and user configuration, oldest first."""
        found: list[dict[str, Any]] = []
        tasks = self.root / "tasks"
        if not tasks.is_dir():
            return found
        for task_dir in sorted(tasks.iterdir(), key=lambda path: path.stat().st_mtime_ns):
            found.append(
                {
                    "task_id": task_dir.name,
                    "payload": json.loads((task_dir / "payload.json").read_text("utf-8")),
                    "user_config": json.loads((task_dir / "user_config.json").read_text("utf-8")),
                }
            )
        return found


class LocalTransferService:
    """A stand-in for Globus Transfer: each collection is a local directory."""

    def __init__(self, collections: dict[str, Path]) -> None:
        self.collections = collections

    def resolve(self, collection: str, path: str) -> Path:
        """Return the local path for ``path`` on ``collection``."""
        root = self.collections.get(collection)
        if root is None:
            raise Refusal(RefusalReason.UNREACHABLE, f"no collection {collection}")
        parts = [part for part in path.split("/") if part not in ("", ".")]
        return root.joinpath(*["home" if part == "~" else part for part in parts])

    async def ls(self, collection: str, path: str) -> list[dict[str, Any]]:
        target = self.resolve(collection, path)
        if not target.is_dir():
            raise Refusal(RefusalReason.PATH_NOT_FOUND, f"{path} does not exist", path=path)
        items: list[dict[str, Any]] = []
        for child in sorted(target.iterdir(), key=lambda item: item.name):
            # Like Transfer: a symlink shows its target's type and size, with link_target set.
            link = os.readlink(child) if child.is_symlink() else None
            broken = link is not None and not child.exists()
            info = child.lstat() if broken else child.stat()
            items.append(
                {
                    "name": child.name,
                    "type": "invalid_symlink" if broken else "dir" if child.is_dir() else "file",
                    "link_target": link,
                    "size": info.st_size,
                    "last_modified": datetime.fromtimestamp(info.st_mtime, UTC).strftime(
                        "%Y-%m-%d %H:%M:%S+00:00"
                    ),
                }
            )
        return items

    async def copy(self, source: str, source_path: str, destination: str, dest_path: str) -> None:
        origin = self.resolve(source, source_path)
        target = self.resolve(destination, dest_path)
        if not origin.is_file():
            raise Refusal(RefusalReason.PATH_NOT_FOUND, f"{source_path} does not exist")
        await asyncio.to_thread(target.parent.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(shutil.copyfile, origin, target)

    async def delete(self, collection: str, path: str, *, recursive: bool) -> None:
        # Globus Transfer deletes a directory only with recursive set.
        target = self.resolve(collection, path)
        if target.is_symlink() or target.is_file():
            await asyncio.to_thread(target.unlink)
        elif target.is_dir():
            if not recursive:
                raise Refusal(RefusalReason.UNREACHABLE, f"{path} is a directory")
            await asyncio.to_thread(shutil.rmtree, target)
        else:
            raise Refusal(RefusalReason.UNREACHABLE, f"Globus Transfer delete: {path} not found")

    async def rename(self, collection: str, old: str, new: str) -> None:
        origin = self.resolve(collection, old)
        target = self.resolve(collection, new)
        if not (origin.exists() or origin.is_symlink()):
            raise Refusal(RefusalReason.PATH_NOT_FOUND, f"{old} does not exist", path=old)
        if target.exists() or not target.parent.is_dir():
            raise Refusal(RefusalReason.UNREACHABLE, f"Globus Transfer could not move to {new}")
        await asyncio.to_thread(origin.rename, target)

    async def mkdir(self, collection: str, path: str) -> None:
        target = self.resolve(collection, path)
        if not target.parent.is_dir():
            raise Refusal(RefusalReason.PATH_NOT_FOUND, f"{path} has no parent", path=path)
        await asyncio.to_thread(target.mkdir, exist_ok=True)


def main() -> None:
    """Run one stand-in task: ``python globus_fakes.py <task dir>``."""
    task_dir = Path(sys.argv[1])
    payload: Any = json.loads((task_dir / "payload.json").read_text(encoding="utf-8"))
    (task_dir / "started").write_text("", encoding="utf-8")
    try:
        result = relay_task(cast(dict[str, Any], payload))
    except Exception as exc:
        (task_dir / "error.txt").write_text(f"{type(exc).__name__}: {exc}", encoding="utf-8")
        raise SystemExit(1) from exc
    write_json_atomic(task_dir / "result.json", result)


if __name__ == "__main__":
    main()
