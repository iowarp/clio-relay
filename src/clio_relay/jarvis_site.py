"""JARVIS on a site: package archives, the package runner and clio-jarvis (RL24, RL42).

A package archive is a gzipped tar of one JARVIS repository trimmed to one
package. Its SHA-256 is the hash relay pins a review to (RL24). The layout is
``<repo>/<repo>/<package>/...``, so ``jarvis repo add <unpacked>/<repo>``
registers the repository under its own name and ``<repo>.<package>`` resolves.
Shared modules of the repository (``<repo>/<repo>/_*``), the repository's
top-level files, and a ``schemas/`` directory next to the repository ride
along, because packages import or read them (clio-jarvis's contract reads
``<repo dir>/../schemas``), and so do the Spack repositories a package's
``contract.yaml`` installs from (:func:`contract_repos`). The repository's Python
requirements ride along as ``<repo>/.clio-relay-requirements.txt``
(:func:`repository_requirements`).

JARVIS runs the package: :mod:`clio_relay.jarvis_submit` renders the
pipeline and the site script that checks the archive's SHA-256 against the
reviewed one, unpacks it into a JARVIS root private to the job and hands the
pipeline to ``jarvis ppl submit`` or JARVIS's direct run.

RL42 adds clio-jarvis, at a tagged release, to a site's own JARVIS the first
time a package is needed there. :func:`clio_jarvis_script` is the script and
:func:`add_clio_jarvis` runs it through an adapter's ``exec``.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import subprocess
import tarfile
import tomllib
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, cast

import yaml

from clio_relay import programs
from clio_relay.jarvis_submit import ARCHIVE_DIR as ARCHIVE_DIR
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.stable_gzip import stable_gzip

SOURCE_FILE = ".clio-relay-source.json"
"""The archive member that says where the package came from."""

SHARED_SIBLINGS = ("schemas",)
"""Directories next to the repository directory that its packages read."""

REQUIREMENTS_FILE = ".clio-relay-requirements.txt"
"""The archive member ``<repo>/<this>``: the repository's Python requirements, one a line.

They are the ``[project] dependencies`` of the repository's ``pyproject.toml``, or of
the checkout's beside it, without jarvis-cd, which relay pins itself. The site
script installs them into relay's jarvis-cd environment before JARVIS loads the
package.
"""

_PINNED_BY_RELAY = frozenset({"jarvis-cd"})
_REQUIREMENT_NAME = re.compile(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)")

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


JARVIS_BUILTIN = "builtin"
"""jarvis-cd's own repository, whose packages relay never stages, lists or runs.

They hardcode a case or a site's setup, so they are a reference for writing a
generic package, not packages to run. JARVIS's own steps relay asks for
(``builtin.resource_graph``) are JARVIS's machinery, not staged packages.
"""


def refuse_builtin(name: str) -> None:
    """Refuse ``name`` when it is one of jarvis-cd's builtin packages (:data:`JARVIS_BUILTIN`)."""
    if name.partition(".")[0] == JARVIS_BUILTIN:
        raise Refusal(
            RefusalReason.POLICY_REFUSED,
            f"{name} is one of jarvis-cd's builtin packages, which relay does not stage or "
            "run. Read it only as a reference, and use a package relay's sources hold for "
            "the application or write a generic one with the jarvis-package skill",
            package=name,
            next_step="write_package",
        )


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
    for sibling in (*SHARED_SIBLINGS, *contract_repos(repo_dir, inner / package)):
        shared = repo_dir.parent / sibling
        if shared.is_dir() and not shared.is_symlink():
            entries += [
                (str(PurePosixPath(sibling, *path.relative_to(shared).parts)), path)
                for path in _files(shared)
            ]
    return sorted(set(entries))


def contract_repos(repo_dir: Path, package_dir: Path) -> list[str]:
    """Return the Spack repositories the package's ``contract.yaml`` installs from.

    ``install.repos`` names directories of the checkout, beside ``repo_dir`` as
    ``schemas/`` is, so they ride along in the archive. One that is not there is
    refused here, on the relay host, rather than on the site.
    """
    contract = package_dir / "contract.yaml"
    if not contract.is_file():
        return []
    try:
        loaded: object = yaml.safe_load(contract.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError):
        return []
    install = cast(dict[str, object], loaded).get("install") if isinstance(loaded, dict) else None
    listed = cast(dict[str, object], install).get("repos") if isinstance(install, dict) else None
    repos: list[str] = []
    for item in cast(list[object], listed) if isinstance(listed, list) else []:
        relative = PurePosixPath(str(item))
        where = repo_dir.parent / str(item)
        if relative.is_absolute() or ".." in relative.parts or not relative.parts:
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                f"{contract} installs from {item!r}, which is not a directory of the checkout",
            )
        if not (where / "repo.yaml").is_file():
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                f"{contract} installs from the Spack repository {item!r}, and {where} is not "
                "one: copy it beside the repository, as in the package's checkout",
                path=str(where),
            )
        repos.append(relative.as_posix())
    return repos


def repository_requirements(repo_dir: Path) -> list[str]:
    """Return the Python requirements of the JARVIS repository at ``repo_dir``.

    They come from ``repo_dir/pyproject.toml``, else from the checkout's
    ``repo_dir/../pyproject.toml``, without jarvis-cd (:data:`REQUIREMENTS_FILE`).
    """
    for pyproject in (repo_dir / "pyproject.toml", repo_dir.parent / "pyproject.toml"):
        if not pyproject.is_file():
            continue
        try:
            loaded = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
            raise Refusal(
                RefusalReason.INVALID_SPEC, f"{pyproject} is not a readable pyproject.toml: {exc}"
            ) from exc
        project: object = loaded.get("project")
        listed: object = (
            cast(dict[str, object], project).get("dependencies")
            if isinstance(project, dict)
            else None
        )
        requirements: list[str] = []
        for item in cast(list[object], listed) if isinstance(listed, list) else []:
            text = str(item).strip()
            match = _REQUIREMENT_NAME.match(text)
            if not match or "\n" in text:
                raise Refusal(
                    RefusalReason.INVALID_SPEC,
                    f"{pyproject} lists {text!r}, which is not a Python requirement",
                )
            if re.sub(r"[-_.]+", "-", match.group(1)).lower() not in _PINNED_BY_RELAY:
                requirements.append(text)
        return requirements
    return []


def _member(name: str, size: int, mode: int) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name)
    info.size = size
    info.mode = mode
    info.mtime = 0
    info.uid = info.gid = 0
    info.uname = info.gname = ""
    return info


def _portable(data: bytes) -> tuple[bytes, int]:
    """Return a file's archive bytes and mode, the same from any checkout of it.

    A text file (UTF-8, no NUL byte) has its CRLF line ends made LF, as git keeps
    them, so a Windows checkout with ``core.autocrlf`` packs what a POSIX one packs.
    The mode is 0755 for a file that starts with ``#!`` and 0644 otherwise: a host's
    permission bits are not part of the package (Windows has none to give).
    """
    if b"\0" not in data:
        try:
            data.decode("utf-8")
        except UnicodeDecodeError:
            pass
        else:
            data = data.replace(b"\r\n", b"\n")
    return data, 0o755 if data.startswith(b"#!") else 0o644


def archive_package(repo_dir: Path, name: str, *, source: dict[str, Any] | None = None) -> bytes:
    """Return the deterministic archive of package ``name`` from ``repo_dir``.

    The same files give the same bytes, so the same SHA-256, on any machine, from
    any checkout of them (:func:`_portable`) and under any Python, whatever zlib it
    links (:func:`~clio_relay.stable_gzip.stable_gzip`).
    """
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.PAX_FORMAT) as tar:
        if source is not None:
            data = (json.dumps(source, sort_keys=True, indent=2) + "\n").encode("utf-8")
            tar.addfile(_member(SOURCE_FILE, len(data), 0o644), io.BytesIO(data))
        members = [
            (arcname, *_portable(path.read_bytes()))
            for arcname, path in package_files(repo_dir, name)
        ]
        requirements = repository_requirements(repo_dir)
        if requirements:
            listed = "".join(f"{line}\n" for line in requirements).encode("utf-8")
            members.append((f"{split_name(name)[0]}/{REQUIREMENTS_FILE}", listed, 0o644))
        for arcname, data, mode in sorted(members):
            tar.addfile(_member(arcname, len(data), mode), io.BytesIO(data))
    return stable_gzip(raw.getvalue())


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
    requirements: list[str] = field(default_factory=list[str])


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
                if member.name == f"{repo}/{REQUIREMENTS_FILE}":
                    info.requirements = [
                        line.strip()
                        for line in body.decode("utf-8", errors="replace").splitlines()
                        if line.strip()
                    ]
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
