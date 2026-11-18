"""JARVIS on a site: package archives, the package runner and clio-jarvis (RL24, RL42).

A package archive is a gzipped tar of one JARVIS repository trimmed to one
package. Its SHA-256 is the hash relay pins a review to (RL24). The layout is
``<repo>/<repo>/<package>/...``, so ``jarvis repo add <unpacked>/<repo>``
registers the repository under its own name and ``<repo>.<package>`` resolves.
Shared modules of the repository (``<repo>/<repo>/_*``), the repository's
top-level files, and a ``schemas/`` directory next to the repository ride
along, because packages import or read them (clio-jarvis's contract reads
``<repo dir>/../schemas``).

The package runner turns a package job into a plain bash job any batch adapter
can submit. On the site it checks the archive's SHA-256 against the reviewed
one, unpacks it, and runs it with JARVIS under a JARVIS root private to the
job, so the site's own pipelines are never touched.

RL42 adds clio-jarvis, at a tagged release, to a site's own JARVIS the first
time a package is needed there. :func:`clio_jarvis_script` is the script and
:func:`add_clio_jarvis` runs it through an adapter's ``exec``.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import re
import shlex
import subprocess
import sys
import tarfile
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from clio_relay.adapters.base import BaseAdapter
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import CustomValue, InputRef, JobSpec

SOURCE_FILE = ".clio-relay-source.json"
"""The archive member that says where the package came from."""

ARCHIVE_DIR = ".relay-package"
"""Where the runner finds the archive, relative to the job directory."""

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
    try:
        done = subprocess.run(
            ["git", "-C", str(checkout), *args], capture_output=True, text=True, check=False
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
            "review what would run. Build and stage it with `python -m clio_relay.jarvis_site "
            "archive <repository dir> <package> --stage`, or stage the file with the files "
            "verb on a local site, then run again",
            digest=digest,
            package=name,
        )
    return inspect_archive(content.get(digest), name)


# The package runner

RUNNER = r"""set -eu
digest=$1 name=$2 jarvis=$3 workdir=$4 mode=$5
shift 5
case $jarvis in "~/"*) jarvis="$HOME/${jarvis#??}" ;; esac
fail() { echo "clio-relay package runner: $2" >&2; exit "$1"; }
case $mode in run|service) ;; *) fail 64 "mode $mode is not run or service" ;; esac
printf '%s\n' "$name" | grep -Eq '^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$' \
  || fail 64 "package name $name is not <repo>.<package>"
archive="$PWD/.relay-package/$digest.tar.gz"
[ -f "$archive" ] || fail 66 "the package archive $archive is missing"
if command -v sha256sum >/dev/null 2>&1; then got=$(sha256sum "$archive" | cut -d' ' -f1)
else got=$(shasum -a 256 "$archive" | cut -d' ' -f1); fi
[ "$got" = "$digest" ] || fail 65 "the archive's SHA-256 is $got, not the reviewed $digest"
root="$PWD/.relay-jarvis"
repo=${name%%.*}
rm -rf "$root"
mkdir -p "$root/src"
tar -xzf "$archive" -C "$root/src"
[ -d "$root/src/$repo/$repo" ] || fail 66 "the archive has no $repo/$repo directory"
command -v "$jarvis" >/dev/null 2>&1 || fail 69 "$jarvis is not on PATH on this site"
export JARVIS_ROOT="$root"
"$jarvis" init "$root/config" "$root/private" "$root/shared" >/dev/null
"$jarvis" repo add "$root/src/$repo" >/dev/null
if [ -n "${SLURM_JOB_NODELIST:-}" ] && command -v scontrol >/dev/null 2>&1; then
  scontrol show hostnames "$SLURM_JOB_NODELIST" > "$root/hostfile"
elif [ -n "${PBS_NODEFILE:-}" ] && [ -f "$PBS_NODEFILE" ]; then
  sort -u "$PBS_NODEFILE" > "$root/hostfile"
fi
[ -s "$root/hostfile" ] && "$jarvis" hostfile set "$root/hostfile" >/dev/null
"$jarvis" ppl create "relay_$(printf %.12s "$digest")" >/dev/null
"$jarvis" ppl append "$name" pkg "$@"
[ -z "$workdir" ] || cd "$workdir"
[ "$mode" = run ] && exec "$jarvis" ppl run
trap 'trap - TERM INT HUP; kill $! 2>/dev/null || true; "$jarvis" ppl stop; exit 0' TERM INT HUP
"$jarvis" ppl start
while :; do sleep 60 & wait $!; done
"""


def _parameter(key: str, value: CustomValue) -> str:
    if isinstance(value, bool):
        return f"{key}={'true' if value else 'false'}"
    return f"{key}={value}"


def package_job(spec: JobSpec, *, jarvis: str = "jarvis", service: bool = False) -> JobSpec:
    """Lower a package job to the bash job that runs it with JARVIS.

    The archive becomes an input, so the adapter stages it by digest. The
    runner checks the hash again on the site before it unpacks anything.

    An application runs ``jarvis ppl run``, which is start then stop. A service
    (``service=True``) runs ``jarvis ppl start`` and stays up until the job is
    told to stop (TERM, INT or HUP), then runs ``jarvis ppl stop`` and exits 0.
    """
    package = spec.package
    if package is None:
        return spec
    split_name(package.name)
    archive = InputRef(name=f"{ARCHIVE_DIR}/{package.sha256}.tar.gz", sha256=package.sha256)
    data = spec.model_dump(mode="json", exclude={"package", "executable", "arguments"})
    data["inputs"] = [
        *[item for item in data["inputs"] if item["name"] != archive.name],
        archive.model_dump(mode="json"),
    ]
    data["directory"] = None
    data["executable"] = "/bin/bash"
    data["arguments"] = [
        "-c",
        RUNNER,
        "clio-relay-package",
        package.sha256,
        package.name,
        jarvis,
        spec.directory or "",
        "service" if service else "run",
        *[_parameter(key, value) for key, value in sorted(package.parameters.items())],
    ]
    data["name"] = spec.name or package.name.replace(".", "-")
    return JobSpec.model_validate(data)


# RL42: clio-jarvis on a site


DEFAULT_CLIO_JARVIS_BASE = "~/.clio-relay/clio-jarvis"


def _site_path(path: str) -> str:
    """Quote ``path`` for sh, letting a leading ``~/`` mean the home directory."""
    if path == "~":
        return '"$HOME"'
    if path.startswith("~/"):
        return '"$HOME"/' + shlex.quote(path[2:])
    return shlex.quote(path)


def clio_jarvis_script(
    *,
    url: str,
    tag: str,
    commit: str | None = None,
    base: str = DEFAULT_CLIO_JARVIS_BASE,
    jarvis: str = "jarvis",
) -> str:
    """The script that puts clio-jarvis ``tag`` in the site's JARVIS, once (RL42).

    It clones the tag into ``<base>/<tag>``, checks the tag's commit when one is
    given, and runs ``jarvis repo add`` unless that directory is already
    registered. It prints ``@@repo <dir>`` and ``@@added`` or ``@@present``.
    """
    q = shlex.quote
    expected = q(commit or "")
    return f"""set -eu
base={_site_path(base)}
dest="$base"/{q(tag)}
jarvis={_site_path(jarvis)}
command -v "$jarvis" >/dev/null 2>&1 || {{ echo "@@error $jarvis is not on PATH"; exit 69; }}
if [ ! -d "$dest/.git" ]; then
  mkdir -p "$base"
  rm -rf "$dest.partial"
  git -c advice.detachedHead=false clone --quiet --depth 1 --branch {q(tag)} {q(url)} \
    "$dest.partial"
  mv "$dest.partial" "$dest"
fi
got=$(git -C "$dest" rev-parse HEAD)
want={expected}
if [ -n "$want" ] && [ "$got" != "$want" ]; then
  echo "@@error tag {tag} is $got, not $want"; exit 65
fi
repo="$dest/clio_jarvis"
[ -d "$repo/clio_jarvis" ] || {{ echo "@@error $dest has no clio_jarvis/clio_jarvis"; exit 65; }}
echo "@@commit $got"
echo "@@repo $repo"
if "$jarvis" repo list 2>/dev/null | grep -F -- "($repo)" >/dev/null; then
  echo "@@present"
else
  "$jarvis" repo add "$repo" >/dev/null
  echo "@@added"
fi
"""


async def add_clio_jarvis(
    adapter: BaseAdapter[Any],
    *,
    url: str,
    tag: str,
    commit: str | None = None,
    base: str = DEFAULT_CLIO_JARVIS_BASE,
    jarvis: str = "jarvis",
) -> dict[str, Any]:
    """Add clio-jarvis ``tag`` to the JARVIS on ``adapter``'s site (RL42).

    Idempotent: a second call finds the repository registered and changes nothing.
    """
    script = clio_jarvis_script(url=url, tag=tag, commit=commit, base=base, jarvis=jarvis)
    result = await adapter.exec(["sh", "-c", script], max_output_bytes=65536)
    lines = result.stdout.splitlines()
    marks = {
        line[2:].split(" ", 1)[0]: line[2:].split(" ", 1)[1:] for line in lines if line[:2] == "@@"
    }
    if result.exit_code != 0 or "error" in marks:
        reason = " ".join(marks.get("error", [])) or result.stderr.strip() or "it failed"
        raise Refusal(
            RefusalReason.SUBMIT_FAILED,
            f"could not add clio-jarvis {tag} to the site's JARVIS: {reason}",
            exit_code=result.exit_code,
        )
    return {
        "tag": tag,
        "commit": (marks.get("commit") or [""])[0],
        "repo": (marks.get("repo") or [""])[0],
        "added": "added" in marks,
    }


# Command line


def _main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m clio_relay.jarvis_site",
        description="Build a reviewed-package archive for a guarded site.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("archive", help="archive one package of a JARVIS repository")
    build.add_argument("repo_dir", type=Path, help="the directory `jarvis repo add` takes")
    build.add_argument("package", help="<repository>.<package>")
    build.add_argument("--output", type=Path, help="also write the archive here")
    build.add_argument(
        "--stage",
        action="store_true",
        help="put it in relay's content store (CLIO_RELAY_STATE_DIR or the default)",
    )
    args = parser.parse_args(argv)
    try:
        data = archive_package(args.repo_dir, args.package, source=git_source(args.repo_dir))
        info = inspect_archive(data, args.package)
    except Refusal as refusal:
        print(refusal.message, file=sys.stderr)
        return 2
    if args.output is not None:
        args.output.write_bytes(data)
    if args.stage:
        from clio_relay.surface.settings import RelayPaths

        ContentStore(RelayPaths.from_env().content_dir).put(data)
    print(
        json.dumps(
            {
                "name": args.package,
                "sha256": info.sha256,
                "size": info.size,
                "files": len(info.files),
                "source": info.source,
                "staged": bool(args.stage),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
