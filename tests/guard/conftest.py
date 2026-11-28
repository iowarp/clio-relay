from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest
import yaml

from clio_relay.adapters.hooks import Ask, HookContext
from clio_relay.guard import Guard, create_guard
from clio_relay.jobs.records import OperationRecord, OperationState, RecordStore
from clio_relay.sites.models import SiteEntry

PKG = """from jarvis_cd.core.pkg import Application


class Hello(Application):
    def _configure_menu(self):
        return [{"name": "out", "msg": "where", "type": str, "default": "out.txt"}]

    def start(self):
        pass
"""


def make_repo(
    root: Path, repo: str = "demo", packages: tuple[str, ...] = ("hello", "other")
) -> Path:
    """Write a small JARVIS repository and return the directory `jarvis repo add` takes."""
    repo_dir = root / repo
    inner = repo_dir / repo
    (inner / "_contract").mkdir(parents=True)
    (repo_dir / "README.md").write_text("demo repository\n")
    (inner / "__init__.py").write_text("")
    (inner / "_contract" / "__init__.py").write_text("")
    (inner / "_contract" / "base.py").write_text("SHARED = 1\n")
    for name in packages:
        (inner / name / "__pycache__").mkdir(parents=True)
        (inner / name / "__init__.py").write_text("")
        (inner / name / "pkg.py").write_text(PKG)
        (inner / name / "__pycache__" / "pkg.cpython-312.pyc").write_bytes(b"\0compiled")
    (root / "schemas").mkdir(exist_ok=True)
    (root / "schemas" / "package-contract.v1.json").write_text("{}\n")
    (root / "unrelated").mkdir(exist_ok=True)
    (root / "unrelated" / "big.bin").write_bytes(b"\0" * 16)
    return repo_dir


def site(policy: str = "guarded", site_id: str = "hpc", **extra: Any) -> SiteEntry:
    return SiteEntry.model_validate(
        {"id": site_id, "adapter": "fixture", "policy": policy, **extra}
    )


def context_for(tmp_path: Path) -> HookContext:
    config = tmp_path / "config"
    config.mkdir(exist_ok=True)
    return HookContext(
        state_dir=tmp_path / "state",
        asks=RecordStore(tmp_path / "state" / "asks", Ask),
        settings={"config_dir": str(config)},
    )


@pytest.fixture
def guard(tmp_path: Path) -> Guard:
    return create_guard(context_for(tmp_path))


def _reviewed(inner: Path, name: str) -> list[Path]:
    tops = [inner / name, *(path for path in inner.iterdir() if path.name.startswith("_"))]
    found = [top for top in tops if top.is_file()]
    found += [path for top in tops if top.is_dir() for path in top.rglob("*") if path.is_file()]
    return [
        path
        for path in found
        if not {"__pycache__", ".git"} & set(path.relative_to(inner).parts)
        and path.suffix not in (".pyc", ".pyo")
    ]


MENU = [{"name": "out", "msg": "where", "type": "str", "default": "out.txt"}]
"""``PKG``'s menu, as ``jarvis repo list +json`` reads it."""


def listed(
    repo_dir: Path,
    name: str,
    git: dict[str, str] | None = None,
    menu: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return what the site listing prints of package ``name`` of ``repo_dir``, digest included."""
    inner = repo_dir / repo_dir.name
    package = name.split(".", 1)[1]
    files = [
        {
            "path": path.relative_to(inner).as_posix(),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "size": path.stat().st_size,
            "executable": bool(path.stat().st_mode & 0o100),
        }
        for path in _reviewed(inner, package)
    ]
    lines = sorted(
        f"{item['sha256']} {'x' if item['executable'] else '-'} {item['path']}\n".encode()
        for item in files
    )
    contract = inner / package / "contract.yaml"
    return {
        "name": name,
        "digest": hashlib.sha256(b"".join(lines)).hexdigest(),
        "files": sorted(files, key=lambda item: item["path"]),
        "menu": MENU if menu is None else menu,
        "contract": yaml.safe_load(contract.read_text()) if contract.is_file() else None,
        "repo_dir": str(repo_dir),
        "git": git,
    }


def stage(
    guard: Guard,
    repo_dir: Path,
    name: str,
    git: dict[str, str] | None = None,
    site_id: str = "hpc",
) -> str:
    """List package ``name`` into the site's catalog, beside what it held, and return its digest."""
    item = listed(repo_dir, name, git)
    kept = [{"name": e.name, "digest": e.digest} for e in guard.packages.all(site_id)]
    guard.packages.store(site_id, [item, *kept])
    return item["digest"]


def answer(
    guard: Guard,
    ask_id: str,
    site_id: str,
    message: str,
    binding: dict[str, Any],
    *,
    state: str = "accepted",
    operation_id: str = "op-1",
) -> Ask:
    """Store an approve ask for ``binding`` the way relay's ask provider does, and answer it.

    The operation that asked is recorded too, waiting on the answer, unless it already is.
    """
    ask = Ask(
        ask_id=ask_id, kind="approve", site=site_id, operation_id=operation_id, message=message
    )
    ask.bind(binding)
    if state == "accepted":
        ask.resolve("accepted", {"action": "accept", "content": {"approve": True}}, "test")
    elif state == "declined":
        ask.resolve("declined", {"action": "decline"}, "test")
    guard.context.asks.put(ask_id, ask)
    if guard.operations.get(operation_id) is None:
        record = OperationRecord(
            operation_id=operation_id,
            kind="run",
            site=site_id,
            adapter="fixture",
            spec={},
            spec_digest="0" * 64,
            state=OperationState.AWAITING_INPUT,
            asks=[ask_id],
        )
        guard.operations.put(operation_id, record)
    return ask
