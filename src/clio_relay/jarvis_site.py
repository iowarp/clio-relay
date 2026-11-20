"""JARVIS on a site: package archives, the package runner and clio-jarvis (RL24, RL42).

A package archive is a gzipped tar of one JARVIS repository trimmed to one
package. Its SHA-256 is the hash relay pins a review to (RL24). The layout is
``<repo>/<repo>/<package>/...``, so ``jarvis repo add <unpacked>/<repo>``
registers the repository under its own name and ``<repo>.<package>`` resolves.
Shared modules of the repository (``<repo>/<repo>/_*``), the repository's
top-level files, and a ``schemas/`` directory next to the repository ride
along, because packages import or read them (clio-jarvis's contract reads
``<repo dir>/../schemas``).

JARVIS runs the package: :mod:`clio_relay.jarvis_submit` renders the
pipeline and the site script that checks the archive's SHA-256 against the
reviewed one, unpacks it into a JARVIS root private to the job and hands the
pipeline to ``jarvis ppl submit`` or JARVIS's direct run.

RL42 adds clio-jarvis, at a tagged release, to a site's own JARVIS the first
time a package is needed there. :func:`clio_jarvis_script` is the script and
:func:`add_clio_jarvis` runs it through an adapter's ``exec``.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import re
import subprocess
import tarfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from clio_relay import programs
from clio_relay.jarvis_submit import ARCHIVE_DIR as ARCHIVE_DIR
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.refusals import Refusal, RefusalReason

SOURCE_FILE = ".clio-relay-source.json"
"""The archive member that says where the package came from."""

SHARED_SIBLINGS = ("schemas",)
"""Directories next to the repository directory that its packages read."""

_SKIP_DIRS = frozenset({"__pycache__", ".git", ".pytest_cache", ".mypy_cache", ".ruff_cache"})
_SKIP_SUFFIXES = (".pyc", ".pyo")
_MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
_NAME_PART = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def split_name(name: str) -> tuple[str, str]:
    """Split ``repo.package`` into its repository and package."""
    repo, dot, package = name.partition(".")
    if not dot or not _NAME_PART.fullmatch(repo) or not _NAME_PART.fullmatch(package):
        raise Refusal(
            RefusalReason.INVALID_SPEC,
            f"a JARVIS package is named <repository>.<package>, not {name!r}",
            package=name,
        )
    return repo, package


# Building an archive


def _files(directory: Path) -> Iterator[Path]:
    for path in sorted(directory.rglob("*")):
        relative = path.relative_to(directory)
        if any(part in _SKIP_DIRS for part in relative.parts):
            continue
        if path.is_symlink():
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                f"{path} is a symbolic link; a package archive holds regular files only",
            )
        if path.is_file() and not path.name.endswith(_SKIP_SUFFIXES):
            yield path


def package_files(repo_dir: Path, name: str) -> list[tuple[str, Path]]:
    """Return ``(archive path, file)`` for what the archive of ``name`` holds.

    ``repo_dir`` is the JARVIS repository: the directory ``jarvis repo add``
    takes, holding a Python package of the same name.
    """
    repo, package = split_name(name)
    if repo_dir.name != repo:
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS,
            f"{name} lives in a repository named {repo!r}, and {repo_dir} is not one",
        )
    inner = repo_dir / repo
    if not (inner / package / "pkg.py").is_file():
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS,
            f"{inner / package / 'pkg.py'} does not exist, so {name} is not in {repo_dir}",
        )
    chosen: list[Path] = [path for path in sorted(repo_dir.iterdir()) if path.is_file()]
    chosen += [path for path in sorted(inner.iterdir()) if path.is_file()]
    for path in sorted(inner.iterdir()):
        if path.is_dir() and (path.name == package or path.name.startswith("_")):
            chosen += list(_files(path))
    entries = [
        (str(PurePosixPath(repo, *path.relative_to(repo_dir).parts)), path)
        for path in chosen
        if path.name not in _SKIP_DIRS and not path.name.endswith(_SKIP_SUFFIXES)
    ]
    for sibling in SHARED_SIBLINGS:
        shared = repo_dir.parent / sibling
        if shared.is_dir() and not shared.is_symlink():
            entries += [
                (str(PurePosixPath(sibling, *path.relative_to(shared).parts)), path)
                for path in _files(shared)
            ]
    return sorted(entries)


def _member(name: str, size: int, mode: int) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name)
    info.size = size
    info.mode = mode
    info.mtime = 0
    info.uid = info.gid = 0
    info.uname = info.gname = ""
    return info


def archive_package(repo_dir: Path, name: str, *, source: dict[str, Any] | None = None) -> bytes:
    """Return the deterministic archive of package ``name`` from ``repo_dir``.

    The same files give the same bytes, so the same SHA-256, on any machine.
    """
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.PAX_FORMAT) as tar:
        if source is not None:
            data = (json.dumps(source, sort_keys=True, indent=2) + "\n").encode("utf-8")
            tar.addfile(_member(SOURCE_FILE, len(data), 0o644), io.BytesIO(data))
        for arcname, path in package_files(repo_dir, name):
            data = path.read_bytes()
            mode = 0o755 if path.stat().st_mode & 0o111 else 0o644
            tar.addfile(_member(arcname, len(data), mode), io.BytesIO(data))
    packed = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=packed, mtime=0) as handle:
        handle.write(raw.getvalue())
    return packed.getvalue()


def _git(checkout: Path, *args: str) -> str | None:
    git = programs.find("git")
    if git is None:
        return None
    try:
        done = subprocess.run(
            [git, "-C", str(checkout), *args], capture_output=True, text=True, check=False
        )
    except OSError:
        return None
    return done.stdout.strip() if done.returncode == 0 else None


def git_source(repo_dir: Path) -> dict[str, Any] | None:
    """Describe where ``repo_dir`` came from: remote, tag, commit and path, if it is in git."""
    top = _git(repo_dir, "rev-parse", "--show-toplevel")
    commit = _git(repo_dir, "rev-parse", "HEAD")
    if top is None or commit is None:
        return None
    source: dict[str, Any] = {
        "repository": _git(repo_dir, "config", "--get", "remote.origin.url") or top,
        "commit": commit,
        "path": repo_dir.resolve().relative_to(Path(top).resolve()).as_posix() or ".",
    }
    tag = _git(repo_dir, "describe", "--tags", "--exact-match", "HEAD")
    if tag:
        source["tag"] = tag
    if _git(repo_dir, "status", "--porcelain", "--", ".") not in ("", None):
        source["modified"] = True
    return source


# Reading an archive


@dataclass(frozen=True)
class ArchiveFile:
    """One file in a package archive."""

    path: str
    size: int
    sha256: str
    executable: bool


@dataclass
class ArchiveInfo:
    """What a package archive holds, for the review question."""

    sha256: str
    size: int
    files: list[ArchiveFile] = field(default_factory=list[ArchiveFile])
    source: dict[str, Any] | None = None


def inspect_archive(data: bytes, name: str) -> ArchiveInfo:
    """Read a package archive and check it holds package ``name`` and nothing odd.

    Refuses with ``invalid_spec`` for links, devices, absolute paths, ``..``,
    or an archive without ``<repo>/<repo>/<package>/pkg.py``.
    """
    repo, package = split_name(name)
    info = ArchiveInfo(sha256=hashlib.sha256(data).hexdigest(), size=len(data))
    if len(data) > _MAX_ARCHIVE_BYTES:
        raise Refusal(RefusalReason.INVALID_SPEC, "the package archive is larger than 64 MiB")
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
            for member in tar.getmembers():
                path = PurePosixPath(member.name)
                if path.is_absolute() or ".." in path.parts:
                    raise Refusal(
                        RefusalReason.INVALID_SPEC,
                        f"the package archive has the unsafe path {member.name!r}",
                    )
                if member.isdir():
                    continue
                if not member.isfile():
                    raise Refusal(
                        RefusalReason.INVALID_SPEC,
                        f"{member.name!r} in the package archive is not a regular file",
                    )
                extracted = tar.extractfile(member)
                body = extracted.read() if extracted is not None else b""
                if member.name == SOURCE_FILE:
                    try:
                        loaded = json.loads(body)
                    except ValueError:
                        loaded = None
                    info.source = loaded if isinstance(loaded, dict) else None
                    continue
                info.files.append(
                    ArchiveFile(
                        path=member.name,
                        size=member.size,
                        sha256=hashlib.sha256(body).hexdigest(),
                        executable=bool(member.mode & 0o111),
                    )
                )
    except (tarfile.TarError, EOFError, OSError) as exc:
        raise Refusal(
            RefusalReason.INVALID_SPEC, f"the package archive is not a gzipped tar: {exc}"
        ) from exc
    wanted = f"{repo}/{repo}/{package}/pkg.py"
    if not any(item.path == wanted for item in info.files):
        raise Refusal(
            RefusalReason.INVALID_SPEC,
            f"the package archive has no {wanted}, so it does not hold {name}",
            package=name,
        )
    return info


def read_archive(content: ContentStore, name: str, digest: str) -> ArchiveInfo:
    """Inspect the archive staged in relay's own content store."""
    if not content.has(digest):
        raise Refusal(
            RefusalReason.INPUT_MISSING,
            f"the archive of {name} ({digest}) is not in relay's content store, so nobody can "
            "review what would run. Stage it with the packages tool: "
            'packages(op="stage", package="<repo.package>"), from relay\'s package sources or '
            "from a directory on the relay host (dir), then run again",
            digest=digest,
            package=name,
        )
    return inspect_archive(content.get(digest), name)
