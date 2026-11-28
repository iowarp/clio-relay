"""JARVIS on a site: its one JARVIS root, how relay sets it up, and its packages (RL24, RL42).

Every site has one JARVIS root of relay's, ``<CLIO site home>/jarvis``
(:data:`SITE_ROOT_DIR`), and every JARVIS call relay makes there exports it as
``JARVIS_ROOT``. The person's own ``~/.ppi-jarvis`` is never touched.
:data:`SETUP_SCRIPT` sets it up in three steps: jarvis-cd at :data:`JARVIS_PIN`
as a uv tool, the root (without jarvis-cd's builtin), and clio-jarvis at
:data:`CLIO_JARVIS_REF`. A person's own repository joins it with ``jarvis repo
add <path>`` (:data:`PACKAGES_SCRIPT`'s ``add``).

A package's review identity is its digest (:data:`DIGEST_FUNCTIONS`), computed
on the site from the package as it sits in the root, however it got there.
:data:`PACKAGES_SCRIPT` lists every package of the root with its digest and files,
and with its menu and contract from ``jarvis repo list +json``; the site script
recomputes the digest of each step before JARVIS runs it and refuses one that
changed since its review.
"""

from __future__ import annotations

import json
import re
from typing import Any, cast

from clio_relay.jobs.refusals import Refusal, RefusalReason

JARVIS_PIN = "0c0fe0830b1948acf5ee335eabb1f9b5198f51db"
"""The jarvis-cd commit relay installs on a site (branch ``relay_ready``)."""
JARVIS_SOURCE = f"git+https://github.com/grc-iit/jarvis-cd@{JARVIS_PIN}"
CLIO_JARVIS_URL = "https://github.com/iowarp/clio-jarvis"
CLIO_JARVIS_REF = "2fbfac499649883c847ab57aa8f8d3d532eee064"
"""The clio-jarvis ref relay registers in a site's root."""
CLIO_JARVIS = "clio_jarvis"
SITE_ROOT_DIR = "jarvis"
"""The site's JARVIS root, under the CLIO site home."""
SETUP_DIRS = (SITE_ROOT_DIR, "bin", "uv-tools", "uv-python")
"""What setup writes under the CLIO site home: the root, uv and jarvis, and uv's Python."""
PIN_FILE = "relay.pin"
"""In the root: what relay set it up with, ``<jarvis-cd pin or program> <clio-jarvis ref>``."""
GRAPH_REPO = "clio_relay_site"
"""Relay's own network-collection repository (:mod:`clio_relay.jarvis_graph`), never listed."""

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
"""jarvis-cd's own repository, which relay never registers, lists or runs.

They hardcode a case or a site's setup and are meant for a person to drive, so
they are a reference for writing a generic package, not packages to run. Setup
runs ``jarvis repo rm builtin`` in the root, and relay runs every JARVIS call
with ``JARVIS_NO_BUILTIN=1`` too (:data:`clio_relay.jarvis_graph.NO_BUILTIN_ENV`).
"""


def refuse_builtin(name: str) -> None:
    """Refuse ``name`` when it is one of jarvis-cd's builtin packages (:data:`JARVIS_BUILTIN`)."""
    if name.partition(".")[0] == JARVIS_BUILTIN:
        raise Refusal(
            RefusalReason.POLICY_REFUSED,
            f"{name} is one of jarvis-cd's builtin packages, which relay does not run. Read "
            "it only as a reference, and use a clio-jarvis package for the application or "
            "write a generic one with the jarvis-package skill",
            package=name,
            next_step="write_package",
        )


def site_pin(jarvis: str = "") -> str:
    """What the root's :data:`PIN_FILE` holds once set up, for the site's ``jarvis`` setting."""
    return f"{jarvis or JARVIS_PIN} {CLIO_JARVIS_REF}"


HOME_LINES = r"""case $home in
  "~") home=$HOME ;;
  "~/"*) home="$HOME/${home#??}" ;;
  /*) ;;
  *) home="$HOME/$home" ;;
esac
root="$home/jarvis"
"""
"""Shell lines that make ``$home`` absolute (relative is from the home directory) and set
``$root``, the site's JARVIS root."""

DIGEST_FUNCTIONS = r"""relay_hash() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum | cut -c1-64
  else shasum -a 256 | cut -c1-64; fi
}
relay_files() (
  cd "$1" || exit 66
  pkg=$2
  set -- "$pkg"
  for f in _*; do if [ -e "$f" ] || [ -L "$f" ]; then set -- "$@" "$f"; fi; done
  links=$(find "$@" \( -name __pycache__ -o -name .git \) -prune -o -type l -print)
  [ -z "$links" ] || { echo "$pkg has symbolic links: $links" >&2; exit 65; }
  find "$@" \( -name __pycache__ -o -name .git \) -prune -o -type f \
    ! -name '*.pyc' ! -name '*.pyo' -print | while IFS= read -r f; do
    case $(ls -ld -- "$f" | cut -c4) in x|s) m=x ;; *) m=- ;; esac
    printf '%s %s %s %s\n' "$(relay_hash < "$f")" "$m" "$(wc -c < "$f" | tr -d ' ')" "$f"
  done
)
relay_digest() {
  printf '%s\n' "$1" | sed 's/^\([^ ]* [^ ]*\) [0-9]* /\1 /' | LC_ALL=C sort | relay_hash
}
repo_paths() {
  sed -n 's/^ *- *//p' "$root/repos.yaml" 2>/dev/null | sed "s/^['\"]//; s/['\"]\$//"
}
root_shared() {
  sed -n 's/^shared_dir:[[:space:]]*//p' "$root/jarvis_config.yaml" 2>/dev/null \
    | sed "s/^['\"]//; s/['\"]\$//" | head -n 1
}
"""
"""The review digest of a package, one way for every package on every site.

``relay_files R PKG`` prints ``<sha256> <x|-> <size> <path>`` for every regular
file under ``R/PKG/``, every file at R's top level whose name starts with ``_``
and every file under such a top-level directory, skipping ``__pycache__``,
``.git``, ``*.pyc`` and ``*.pyo``. R is ``<repo dir>/<repo name>``, the path is
relative to it, and ``x`` is the owner's execute bit. A symbolic link refuses
(exit 65). ``relay_digest`` takes that listing and returns the SHA-256 of its
``<sha256> <x|-> <path>`` lines, sorted bytewise, each ending in a newline:
clio-jarvis's ``package_sha256`` computes the same value. ``repo_paths`` prints
the repositories the root's ``repos.yaml`` registers, and ``root_shared`` the
root's JARVIS shared directory.
"""

SETUP_SCRIPT = (
    r"""set -eu
home=$1 pin=$2 source=$3 url=$4 ref=$5 jarvis=${6:-} outputs=${7:-}
want="${jarvis:-$pin} $ref"
"""
    + HOME_LINES
    + DIGEST_FUNCTIONS
    + r"""case $jarvis in "~/"*) jarvis="$HOME/${jarvis#??}" ;; esac
# JARVIS's shared directory, where every pipeline writes: the site's outputs directory.
case $outputs in
  "") outputs="$root/shared" ;;
  "~/"*) outputs="$HOME/${outputs#??}" ;;
  /*) ;;
  *) outputs="$HOME/$outputs" ;;
esac
mkdir -p "$outputs"
outputs=$(cd "$outputs" && pwd -P)
dir="$home/jarvis-cd"
mkdir -p "$home/bin" "$dir"
# One setup at a time per site home, across relay instances and operations: the lock
# is held until this script exits. Whoever waited reuses the setup it waited for.
lock="$dir/.relay.lock"
if command -v flock >/dev/null 2>&1; then
  exec 9>>"$lock"
  flock 9
else
  waited=0
  until mkdir "$lock.d" 2>/dev/null; do
    waited=$((waited + 2))
    [ "$waited" -lt 3600 ] || {
      echo "another setup holds $lock.d; remove it if none is running" >&2
      exit 1
    }
    sleep 2
  done
  trap 'rmdir "$lock.d" 2>/dev/null || true' EXIT
fi
program=${jarvis:-$home/bin/jarvis}
log="$dir/setup.log"
said() {
  printf '{"jarvis":"%s","root":"%s","pin":"%s","log":"%s","from":"%s"}\n' \
    "$program" "$root" "$want" "$log" "$1"
}
if [ "$(cat "$root/relay.pin" 2>/dev/null)" = "$want" ] && [ -x "$program" ] \
  && [ "$(root_shared)" = "$outputs" ]; then
  said reused
  exit 0
fi
: > "$log"
fail() { tail -n 40 "$log" >&2; echo "clio-relay setup: $1" >&2; exit 1; }
step() { echo "+ $*" >>"$log"; "$@" >>"$log" 2>&1 </dev/null || fail "$* failed"; }
rm -f "$root/relay.pin"
# 1. jarvis-cd at the pin, as a uv tool with uv's own Python.
if [ -z "$jarvis" ]; then
  uv=$(command -v uv 2>/dev/null) || uv="$home/bin/uv"
  if [ ! -x "$uv" ]; then
    echo "+ install uv into $home/bin" >>"$log"
    curl -LsSf https://astral.sh/uv/install.sh 2>>"$log" \
      | env UV_INSTALL_DIR="$home/bin" UV_NO_MODIFY_PATH=1 sh >>"$log" 2>&1 \
      || fail "installing uv failed"
    uv="$home/bin/uv"
  fi
  step env UV_TOOL_DIR="$home/uv-tools" UV_TOOL_BIN_DIR="$home/bin" \
    UV_PYTHON_INSTALL_DIR="$home/uv-python" "$uv" tool install --force -p 3.12 "$source"
fi
[ -x "$program" ] || fail "there is no jarvis program at $program"
# 2. The site's root, without jarvis-cd's builtin.
export JARVIS_ROOT="$root" JARVIS_NO_BUILTIN=1
[ -f "$root/jarvis_config.yaml" ] && [ "$(root_shared)" = "$outputs" ] \
  || step "$program" init "$root/config" "$root/private" "$outputs"
[ "$(root_shared)" = "$outputs" ] || fail "jarvis init did not set up $root with $outputs"
step "$program" repo rm builtin
# 3. clio-jarvis at the ref. JARVIS may print an error and exit 0, so the clone is checked.
clone="$root/repos/clio_jarvis"
if [ -d "$clone/.git" ]; then
  step "$program" repo update clio_jarvis --ref "$ref"
else
  step "$program" repo add "$url" --ref "$ref"
fi
[ -d "$clone/clio_jarvis" ] || fail "jarvis did not clone clio-jarvis into $clone"
case $ref in
  *[!0-9a-f]*) ;;
  ????????????????????????????????????????)
    [ "$(git -C "$clone" rev-parse HEAD)" = "$ref" ] || fail "clio-jarvis is not at $ref" ;;
esac
repo_paths() { sed -n 's/^ *- *//p' "$root/repos.yaml" 2>/dev/null; }
repo_paths | grep -q "clio_jarvis" || fail "jarvis did not register clio-jarvis"
printf '%s\n' "$want" > "$root/relay.pin"
said installed
"""
)
"""Set up the site's JARVIS root. ``bash -c SETUP_SCRIPT <$0> <home> <pin> <source> <url>
<ref> [jarvis] [outputs]`` (:func:`clio_relay.jarvis_submit.setup_command`).

1. jarvis-cd: ``uv`` on ``PATH``, else ``<home>/bin/uv``, else uv's installer
   puts it there; then ``uv tool install`` of jarvis-cd at the pin, with uv's
   tools, Python and the ``jarvis`` program under the home
   (``<home>/bin/jarvis``). The site's ``jarvis`` setting names another
   program instead, and this step is skipped.
2. The root ``<home>/jarvis``: ``jarvis init`` with the site's outputs
   directory (``<outputs>``, else ``<home>/jarvis/shared``) as JARVIS's shared
   directory, when it has none or another one, then ``jarvis repo rm builtin``.
3. clio-jarvis: ``jarvis repo add <url> --ref <ref>``, or ``jarvis repo update
   clio_jarvis --ref <ref>`` when the clone exists.

The pin file is written only after all three, and a root already at the pin is
reused under the lock (``"from": "reused"``). It prints ``{"jarvis", "root",
"pin", "log", "from"}``, and exits 1 with the log's tail when a step fails.
"""

PACKAGES_SCRIPT = (
    r"""set -u
mode=$1 home=$2 want=$3 jarvis=${4:-} path=${5:-} force=${6:-}
"""
    + HOME_LINES
    + DIGEST_FUNCTIONS
    + r"""if [ "$(cat "$root/relay.pin" 2>/dev/null)" != "$want" ]; then echo "@@unset"; exit 0; fi
case $jarvis in "") jarvis="$home/bin/jarvis" ;; "~/"*) jarvis="$HOME/${jarvis#??}" ;; esac
if [ "$mode" = add ]; then
  case $path in "~/"*) path="$HOME/${path#??}" ;; /*) ;; *) path="$HOME/$path" ;; esac
  path=$(cd "$path" 2>/dev/null && pwd -P) || { echo "no directory $5 on the site" >&2; exit 66; }
  JARVIS_ROOT="$root" JARVIS_NO_BUILTIN=1 "$jarvis" repo add "$path" $force </dev/null >&2
  repo_paths | grep -qxF -- "$path" || { echo "jarvis did not register $path" >&2; exit 65; }
fi
listed=$(JARVIS_ROOT="$root" JARVIS_NO_BUILTIN=1 "$jarvis" repo list +json </dev/null) \
  || { echo "jarvis repo list +json failed" >&2; exit 1; }
echo "@@repos $(printf '%s\n' "$listed" | tail -n 1)"
repo_paths | while IFS= read -r dir; do
  name=${dir##*/}
  case $name in builtin|clio_relay_site) continue ;; esac
  for p in "$dir/$name"/*/pkg.py; do
    [ -f "$p" ] || continue
    pkg=${p%/pkg.py}; pkg=${pkg##*/}
    files=$(relay_files "$dir/$name" "$pkg") || { echo "@@refused $name.$pkg"; continue; }
    echo "@@package $name.$pkg $(relay_digest "$files")"
    printf '%s\n' "$files" | sed 's/^/@@file /'
  done
done
"""
)
"""List the packages of the site's root; with ``add``, register a repository first.

``sh -c PACKAGES_SCRIPT <$0> list|add <home> <pin> [jarvis] [path] [+force]``
(:func:`clio_relay.jarvis_submit.packages_command`). ``add`` runs ``jarvis repo add
<path>`` in the root. It prints ``@@unset`` when the root is not set up at the
pin. Else it prints ``@@repos`` and JARVIS's ``jarvis repo list +json`` document
(each package's menu, contract and git source, as JARVIS reads them), then each
package's review digest as ``@@package <name> <digest>`` and an ``@@file`` line
per reviewed file (:func:`parse_packages`).
"""


def parse_packages(text: str) -> list[dict[str, Any]] | None:
    """Return the packages :data:`PACKAGES_SCRIPT` printed, or ``None`` for ``@@unset``.

    Each is its digest and files, with ``repo_dir``, ``git``, ``menu`` and
    ``contract`` from JARVIS's ``jarvis.repos.v1`` document.
    """
    found: list[dict[str, Any]] = []
    described: dict[str, dict[str, Any]] | None = None
    item: dict[str, Any] | None = None
    for line in text.splitlines():
        tag, _space, rest = line.partition(" ")
        if tag == "@@unset":
            return None
        if tag == "@@repos":
            described = _described(rest)
        elif tag == "@@package":
            name, _space, digest = rest.partition(" ")
            item = None
            if re.fullmatch(r"[0-9a-f]{64}", digest) and _NAME_PART.fullmatch(
                name.partition(".")[2] or "-"
            ):
                item = {"name": name, "digest": digest, "files": []}
                found.append(item)
        elif item is None:
            continue
        elif tag == "@@file":
            parts = rest.split(" ", 3)
            if len(parts) == 4 and parts[2].isdigit():
                item["files"].append(
                    {
                        "path": parts[3],
                        "sha256": parts[0],
                        "size": int(parts[2]),
                        "executable": parts[1] == "x",
                    }
                )
    if described is None:
        raise Refusal(
            RefusalReason.UNREACHABLE,
            "the site's jarvis printed no jarvis.repos.v1 document for `jarvis repo list +json`; "
            "set the site up again so its jarvis-cd is at relay's pin",
        )
    for each in found:
        each.update(described.get(each["name"], {}))
    return found


def _described(text: str) -> dict[str, dict[str, Any]] | None:
    """Each package of a ``jarvis.repos.v1`` document, by ``repo.package``; None if invalid."""
    try:
        document = json.loads(text)
    except ValueError:
        return None
    if not isinstance(document, dict) or document.get("schema_version") != "jarvis.repos.v1":
        return None
    found: dict[str, dict[str, Any]] = {}
    for repo in cast(list[Any], document.get("repos") or []):
        if not isinstance(repo, dict):
            continue
        repo = cast(dict[str, Any], repo)
        for package in cast(list[Any], repo.get("packages") or []):
            if not isinstance(package, dict):
                continue
            package = cast(dict[str, Any], package)
            found[f"{repo.get('name')}.{package.get('name')}"] = {
                "repo_dir": str(repo.get("path") or ""),
                "git": repo.get("git") if isinstance(repo.get("git"), dict) else None,
                "menu": package.get("menu") if isinstance(package.get("menu"), list) else None,
                "contract": package.get("contract")
                if isinstance(package.get("contract"), dict)
                else None,
                "contract_error": str(package.get("contract_error") or ""),
            }
    return found
