"""Focused tests for distribution source-identity embedding."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest

import hatch_build
from hatch_build import CustomBuildHook


def _hook(root: Path, *, target_name: str = "wheel") -> CustomBuildHook:
    hook = CustomBuildHook.__new__(CustomBuildHook)
    object.__setattr__(hook, "_BuildHookInterface__root", str(root))
    object.__setattr__(hook, "_BuildHookInterface__target_name", target_name)
    return hook


def _write_project(root: Path, *, version: str = "1.3.15") -> None:
    (root / "pyproject.toml").write_text(
        f'[project]\nname = "clio-relay"\nversion = "{version}"\n',
        encoding="utf-8",
    )


def _clean_git_output(_root: Path, *args: str) -> str | None:
    """Return a stable clean tagged identity for build-hook tests."""
    return "a" * 40 if args == ("rev-parse", "HEAD") else "v1.3.15"


def _untagged_git_output(_root: Path, *args: str) -> str | None:
    """Return a stable commit with no exact tag for build-hook tests."""
    return "a" * 40 if args == ("rev-parse", "HEAD") else None


def _clean_git_status(_root: Path) -> bool:
    """Report a clean synthetic source tree."""
    return False


def test_release_build_embeds_exact_clean_tagged_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_RELEASE_BUILD", "1")
    monkeypatch.setattr(
        hatch_build,
        "_git_output",
        _clean_git_output,
    )
    monkeypatch.setattr(hatch_build, "_git_dirty", _clean_git_status)
    build_data: dict[str, object] = {}

    _hook(tmp_path).initialize("1.3.15", build_data)

    embedded = json.loads(
        (tmp_path / ".clio-relay" / "build" / "_build_info.json").read_text(encoding="utf-8")
    )
    assert embedded == {
        "commit": "a" * 40,
        "dirty": False,
        "tag": "v1.3.15",
        "version": "1.3.15",
    }
    assert build_data["force_include"] == {
        str(tmp_path / ".clio-relay" / "build" / "_build_info.json"): (
            "clio_relay/_build_info.json"
        )
    }


def test_release_build_rejects_source_without_exact_version_tag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_RELEASE_BUILD", "1")
    monkeypatch.setattr(
        hatch_build,
        "_git_output",
        _untagged_git_output,
    )
    monkeypatch.setattr(hatch_build, "_git_dirty", _clean_git_status)

    with pytest.raises(
        RuntimeError,
        match=r"release build must run from the exact package-version tag: "
        r"expected v1\.3\.15, got None",
    ):
        _hook(tmp_path).initialize("1.3.15", {})


@pytest.mark.parametrize(
    ("identity", "message"),
    [
        (
            {"version": "1.3.15", "commit": None, "tag": "v1.3.15", "dirty": False},
            "release build identity has no source commit",
        ),
        (
            {"version": "1.3.15", "commit": "a" * 40, "tag": "v1.3.15", "dirty": True},
            "release build must run from a clean source tree",
        ),
    ],
)
def test_release_build_rejects_incomplete_or_dirty_identity(
    identity: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(RuntimeError, match=message):
        cast(Any, hatch_build)._validate_release_build_identity(identity, release_mode="1")


def test_non_release_candidate_build_may_remain_untagged() -> None:
    cast(Any, hatch_build)._validate_release_build_identity(
        {"version": "1.3.15", "commit": "a" * 40, "tag": None, "dirty": False},
        release_mode=None,
    )
