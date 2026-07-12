"""Hatch build hook that embeds immutable source identity in distributions."""

from __future__ import annotations

import json
import subprocess
import tomllib
from pathlib import Path
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    """Include commit, exact tag, and cleanliness in wheel and sdist artifacts."""

    PLUGIN_NAME = "custom"

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        """Generate build identity and force-include it in the package."""
        root = Path(self.root)
        generated = root / ".clio-relay" / "build" / "_build_info.json"
        generated.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, object] = {
            "version": _project_version(root),
            "commit": _git_output(root, "rev-parse", "HEAD"),
            "tag": _git_output(root, "describe", "--tags", "--exact-match", "HEAD"),
            "dirty": _git_dirty(root),
        }
        if payload["commit"] is None:
            payload.update(_source_archive_identity(root))
        generated.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        force_include = build_data.setdefault("force_include", {})
        if self.target_name == "sdist":
            force_include[str(generated)] = "src/clio_relay/_build_info.json"
        elif not (root / "src" / "clio_relay" / "_build_info.json").exists():
            force_include[str(generated)] = "clio_relay/_build_info.json"


def _git_output(root: Path, *args: str) -> str | None:
    completed = subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        check=False,
        text=True,
    )
    output = completed.stdout.strip()
    return output if completed.returncode == 0 and output else None


def _git_dirty(root: Path) -> bool | None:
    completed = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain"],
        capture_output=True,
        check=False,
        text=True,
    )
    if completed.returncode != 0:
        return None
    return bool(completed.stdout.strip())


def _source_archive_identity(root: Path) -> dict[str, object]:
    embedded = root / "src" / "clio_relay" / "_build_info.json"
    if not embedded.exists():
        return {"commit": None, "tag": None, "dirty": None}
    loaded = json.loads(embedded.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        return {"commit": None, "tag": None, "dirty": None}
    return {
        "commit": loaded.get("commit"),
        "tag": loaded.get("tag"),
        "dirty": loaded.get("dirty"),
    }


def _project_version(root: Path) -> str:
    with (root / "pyproject.toml").open("rb") as stream:
        document = tomllib.load(stream)
    project = document.get("project")
    if not isinstance(project, dict) or not isinstance(project.get("version"), str):
        raise RuntimeError("pyproject.toml does not define project.version")
    return project["version"]
