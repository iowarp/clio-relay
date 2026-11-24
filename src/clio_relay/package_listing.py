"""What relay's package sources hold, listed without running anything.

``packages list`` shows these under ``available``, staged or not, so an agent finds an
application's own package before writing one.
"""

from __future__ import annotations

import ast
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import yaml

from clio_relay.packages import source_dirs

_SUMMARY_CHARS = 240


def _first_paragraph(text: str) -> str | None:
    """Return the first prose paragraph of a README: not a heading, a fence or a list."""
    paragraph: list[str] = []
    fenced = False
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("```"):
            fenced = not fenced
            if paragraph:
                break
            continue
        if fenced:
            continue
        if not line:
            if paragraph:
                break
            continue
        if line.startswith(("#", "|", "-", "*", "<", "!", ">")) and not paragraph:
            continue
        paragraph.append(line)
    joined = " ".join(paragraph)
    return joined or None


def _docstring(source: str) -> str | None:
    """Return the first paragraph of the package class's docstring, or the module's."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return None
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            text = ast.get_docstring(node)
            if text:
                return " ".join(text.split("\n\n", 1)[0].split())
    text = ast.get_docstring(tree)
    return " ".join(text.split("\n\n", 1)[0].split()) if text else None


def _clip(text: str) -> str:
    text = " ".join(text.split())
    return text if len(text) <= _SUMMARY_CHARS else text[: _SUMMARY_CHARS - 1].rstrip() + "…"


def _read(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def _source_summary(package_dir: Path) -> tuple[str | None, bool]:
    """Return a package's summary, read without running it, and whether it has a contract."""
    contract_text = _read(package_dir / "contract.yaml")
    if contract_text is not None:
        try:
            loaded = yaml.safe_load(contract_text)
        except yaml.YAMLError:
            loaded = None
        if isinstance(loaded, dict) and cast(dict[str, Any], loaded).get("summary"):
            return _clip(str(cast(dict[str, Any], loaded)["summary"])), True
    for readme in ("README.md", "USE.md"):
        text = _read(package_dir / readme)
        found = _first_paragraph(text) if text else None
        if found:
            return _clip(found), contract_text is not None
    found = _docstring(_read(package_dir / "pkg.py") or "")
    return (_clip(found) if found else None), contract_text is not None


def source_packages(sources: Sequence[Path], match: str | None = None) -> list[dict[str, Any]]:
    """List the packages relay's package sources hold, staged or not, read without running.

    Each is ``{name, summary, contract}``: ``contract`` says whether it has a
    ``contract.yaml`` relay reads its outputs, progress and resources from. ``match``
    keeps those whose name or summary holds every one of its words, case-insensitive.
    """
    words = [word.lower() for word in (match or "").split()]
    found: dict[str, dict[str, Any]] = {}
    for source in sources:
        for candidate in source_dirs(source):
            for repo_dir in source_dirs(candidate):
                inner = repo_dir / repo_dir.name
                if not (inner / "__init__.py").is_file():
                    continue
                try:
                    children = sorted(child for child in inner.iterdir() if child.is_dir())
                except OSError:
                    continue
                for package_dir in children:
                    if package_dir.name.startswith("_") or not (package_dir / "pkg.py").is_file():
                        continue
                    name = f"{repo_dir.name}.{package_dir.name}"
                    if name in found:
                        continue
                    summary, contract = _source_summary(package_dir)
                    text = f"{name} {summary or ''}".lower()
                    if all(word in text for word in words):
                        found[name] = {"name": name, "summary": summary, "contract": contract}
    return sorted(found.values(), key=lambda item: str(item["name"]))


__all__ = ["source_packages"]
