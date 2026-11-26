"""The files relay's ``files`` verb made on a site, so ``site clean`` can remove them.

A ``write`` or an ``upload`` puts a file where the agent names it, often outside
relay's own directories (the site's outputs directory, say). Relay records each
file it made, by the absolute path the site reports, in its local state for the
site. A chunked write's part file is recorded until its last chunk moves it in
place. A file that was there before, which an overwrite replaced, stays the
person's and is not recorded. A ``delete`` drops what it removed.

``site clean`` lists the recorded files with the runs' outputs: it keeps them
unless ``remove_outputs`` says otherwise, and forgets the ones it removed. A
directory a write made for its file is not recorded, so it stays, empty, once
the file goes. Staged content is in relay's own directory, which the clean
removes anyway.
"""

from __future__ import annotations

import json
import os
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from filelock import FileLock

from clio_relay.jobs.file_rules import part_path
from clio_relay.relay_state import under

SCHEMA = "relay.site_writes.v1"


def ledger_path(state_dir: Path, site_id: str) -> Path:
    """Where the site's record lives: beside, not in, its adapters' state, which a clean drops."""
    return state_dir / "sites" / site_id / "writes.json"


@contextmanager
def _locked(path: Path) -> Generator[dict[str, dict[str, Any]]]:
    """Hold the record's lock across processes, and save what the block leaves in it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(str(path) + ".lock", timeout=30):
        files = _load(path)
        before = json.dumps(files, sort_keys=True)
        yield files
        if json.dumps(files, sort_keys=True) == before:
            return
        if not files:
            path.unlink(missing_ok=True)
            return
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_text(
            json.dumps({"schema": SCHEMA, "files": files}, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(temporary, path)


def _load(path: Path) -> dict[str, dict[str, Any]]:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    files = cast(dict[str, Any], loaded).get("files") if isinstance(loaded, dict) else None
    if not isinstance(files, dict):
        return {}
    return {
        str(key): cast(dict[str, Any], value)
        for key, value in cast(dict[Any, Any], files).items()
        if isinstance(value, dict)
    }


def written(state_dir: Path, site_id: str) -> dict[str, dict[str, Any]]:
    """The site's recorded files, by absolute path, each with its ``size`` and ``written`` time."""
    return _load(ledger_path(state_dir, site_id))


def _row(size: int | None) -> dict[str, Any]:
    return {"size": size, "written": datetime.now(UTC).isoformat()}


def record(state_dir: Path, site_id: str, path: str, size: int | None) -> None:
    """Record a file relay made. A path the site did not give absolute is not recorded."""
    if not path.startswith("/"):
        return
    with _locked(ledger_path(state_dir, site_id)) as files:
        files[path] = _row(size)


def finished(
    state_dir: Path, site_id: str, path: str, size: int | None, *, record_it: bool
) -> None:
    """A write's last chunk moved its part file in place: forget the part, record the file."""
    if not path.startswith("/"):
        return
    with _locked(ledger_path(state_dir, site_id)) as files:
        files.pop(part_path(path), None)
        if record_it:
            files[path] = _row(size)


def forget(state_dir: Path, site_id: str, paths: Iterable[str]) -> None:
    """Drop each of ``paths``, and what is under it, from the site's record."""
    gone = [path for path in paths if path]
    if not gone or not ledger_path(state_dir, site_id).exists():
        return
    with _locked(ledger_path(state_dir, site_id)) as files:
        for path in [path for path in files if under(path, gone)]:
            del files[path]
