"""What ``clio-relay site clean`` runs on a site: a bounded, home-safe prune.

Adapters that implement :class:`~clio_relay.adapters.base.CleansSite` send
:func:`prune_script` over their own connection and read it back with
:func:`parse_report`. Before anything goes, the site's scheduler is asked for the
person's live jobs that are relay's (:func:`jobs_script`): the site, not relay's
local records, says what still runs, so a job whose record ended early, or that
another relay started, still stops the clean.
"""

from __future__ import annotations

import posixpath
import shlex
from typing import Any

from clio_relay.jarvis_submit import (
    BOOTSTRAP_DIR,
    DECISIONS_FILE,
    SITE_DIR,
    SPACK_CONFIG_DIR,
    SPACK_DIR,
)

DECISIONS_TAIL = posixpath.join(BOOTSTRAP_DIR, SITE_DIR, DECISIONS_FILE)
"""Where JARVIS records the person's install answers, under a site home."""
MINIFORGE_DIR = "miniforge3"
"""The Miniforge JARVIS installs beside its site record (``conda_missing``)."""
JOB_PREFIX = "relay-"
"""The scheduler job names relay's adapters give an operation's job."""


def spack_keeps(home: str) -> list[str]:
    """The person's package stores under a site home, which clean keeps.

    The Spack store and config, and the Miniforge JARVIS installs for conda
    packages once the person confirms it (``<home>/jarvis-cd/site/miniforge3``).
    """
    base = home.rstrip("/") or "/"
    return [
        posixpath.join(base, SPACK_DIR),
        posixpath.join(base, SPACK_CONFIG_DIR),
        posixpath.join(base, BOOTSTRAP_DIR, SITE_DIR, MINIFORGE_DIR),
    ]


def site_keeps(home: str, graph: str = "") -> list[str]:
    """What clean keeps under a site home: the Spack store, and JARVIS's recorded answers.

    The answers (which MPI, which compiler) are facts about the cluster the
    person confirmed; without them the next install asks again. ``graph`` is the
    site's published resource graph (its ``jarvis_graph`` setting): a site fact
    too, kept wherever it is.
    """
    base = home.rstrip("/") or "/"
    keeps = [*spack_keeps(home), posixpath.join(base, DECISIONS_TAIL)]
    return [*keeps, graph] if graph else keeps


def is_decisions(path: str) -> bool:
    """Say whether a keep is JARVIS's decisions file rather than the Spack store."""
    return path.endswith("/" + DECISIONS_TAIL)


def is_spack(path: str) -> bool:
    """Say whether a keep is part of the Spack store (:func:`spack_keeps`)."""
    return path.endswith(("/" + SPACK_DIR, "/" + SPACK_CONFIG_DIR)) or path.endswith(
        "/" + posixpath.join(BOOTSTRAP_DIR, SITE_DIR, MINIFORGE_DIR)
    )


def kept(keep: list[str], *, spack: bool = True, decisions: bool = True) -> list[str]:
    """Return ``keep`` without the Spack store or the decisions, as asked.

    Any other keep, such as a published resource graph, always stays.
    """

    def stays(path: str) -> bool:
        if is_decisions(path):
            return decisions
        return spack if is_spack(path) else True

    return list(dict.fromkeys(path for path in keep if stays(path)))


def _jobs_bash(known: list[str]) -> str:
    """Bash that prints ``live<TAB>id|name|state`` for each of relay's live jobs.

    A job is relay's when its name starts with :data:`JOB_PREFIX` or its id is in
    ``known`` (jobs JARVIS named, such as an install). Slurm, LSF, Flux and PBS
    are asked, whichever the site has; a site with none has no jobs to wait for.
    """
    ids = " ".join(
        item for item in known if item and all(c.isalnum() or c in "._-[]" for c in item)
    )
    return f"""relay_known=' {ids} '
relay_user=$(id -un)
relay_listed() {{
  if command -v squeue >/dev/null 2>&1; then
    squeue -h -u "$relay_user" -o '%i|%j|%T' 2>/dev/null
  elif command -v bjobs >/dev/null 2>&1; then
    bjobs -noheader -o 'jobid job_name stat delimiter="|"' 2>/dev/null
  elif command -v flux >/dev/null 2>&1; then
    flux jobs -n -o '{{id}}|{{name}}|{{status}}' 2>/dev/null
  elif command -v qstat >/dev/null 2>&1; then
    qstat -w -u "$relay_user" 2>/dev/null | awk 'NF>=10 && $1 ~ /^[0-9]/ {{print $1"|"$4"|"$10}}'
  fi
}}
relay_live() {{
  while IFS='|' read -r id name state; do
    [ -n "$id" ] || continue
    case $name in
      {JOB_PREFIX}*) ;;
      *) case $relay_known in *" $id "*) ;; *) continue ;; esac ;;
    esac
    printf 'live\t%s|%s|%s\n' "$id" "$name" "$state"
  done
}}
relay_running=$(relay_listed | relay_live)
"""


def jobs_script(known: list[str] | None = None) -> str:
    """Return the shell command that lists relay's live jobs on a site; read it with
    :func:`parse_live`. ``known`` adds native job ids from relay's own records."""
    script = (
        _jobs_bash(list(known or []))
        + '[ -n "$relay_running" ] && printf \'%s\\n\' "$relay_running"\nexit 0\n'
    )
    return shlex.join(["bash", "-c", script, "clio-relay-jobs"])


def parse_live(text: str) -> list[dict[str, str]]:
    """Read :func:`jobs_script`'s (or a refused prune's) ``live`` lines."""
    found: list[dict[str, str]] = []
    for line in text.splitlines():
        word, sep, rest = line.partition("\t")
        if sep and word == "live":
            native_id, _, tail = rest.partition("|")
            name, _, state = tail.partition("|")
            found.append({"native_id": native_id, "name": name, "state": state})
    return found


def prune_script(targets: list[str], keep: list[str]) -> str:
    """Return the shell command that removes ``targets`` on a site, sparing ``keep``.

    A path is absolute, ``~`` or ``~/...``, or relative to the home directory.
    ``keep`` entries may hold ``*`` globs. It never removes the home directory, ``/``
    or an ancestor of the home, and prints ``removed``, ``kept``, ``absent`` or
    ``refused`` and a path on each line. While one of relay's jobs still runs on
    the site it removes nothing and prints a ``live`` line for each instead. The
    prune itself is bash, so the command is ``bash -c <prune>``: it runs the same
    under ``sh`` (dash) or a login shell.
    """
    return shlex.join(["bash", "-c", _prune_bash(targets, keep), "clio-relay-prune"])


def _prune_bash(targets: list[str], keep: list[str]) -> str:
    quoted_targets = " ".join(shlex.quote(item) for item in targets)
    quoted_keep = " ".join(shlex.quote(item) for item in keep)
    return (
        _jobs_bash([])
        + 'if [ -n "$relay_running" ]; then printf \'%s\\n\' "$relay_running"; exit 0; fi\n'
        + _prune_body(quoted_targets, quoted_keep)
    )


def _prune_body(quoted_targets: str, quoted_keep: str) -> str:
    return f"""set -u
shopt -s nullglob dotglob
IFS=$'\\n'
home=$(cd "$HOME" && pwd -P)
expand() {{
  case $1 in
    "~") printf '%s' "$HOME" ;;
    "~/"*) printf '%s/%s' "$HOME" "${{1#??}}" ;;
    /*) printf '%s' "$1" ;;
    *) printf '%s/%s' "$HOME" "$1" ;;
  esac
}}
physical() {{
  if [ -d "$1" ] && [ ! -L "$1" ]; then (cd "$1" && pwd -P); return; fi
  # A kept file resolves through its directory, so it matches a resolved target.
  parent=${{1%/*}}
  if [ -e "$1" ] && [ -n "$parent" ] && [ "$parent" != "$1" ] && [ -d "$parent" ]; then
    printf '%s/%s' "$(cd "$parent" && pwd -P)" "${{1##*/}}"
  else
    printf '%s' "${{1%/}}"
  fi
}}
keeps=()
for pattern in {quoted_keep}; do
  for match in $(expand "$pattern"); do
    [ -e "$match" ] && keeps+=("$(physical "$match")")
  done
done
is_kept() {{
  for k in "${{keeps[@]+"${{keeps[@]}}"}}"; do [ "$1" = "$k" ] && return 0; done
  return 1
}}
holds_kept() {{
  for k in "${{keeps[@]+"${{keeps[@]}}"}}"; do
    case $k in "$1"/*) return 0 ;; esac
  done
  return 1
}}
prune() {{
  if is_kept "$1"; then printf 'kept\\t%s\\n' "$1"; return; fi
  if [ -d "$1" ] && [ ! -L "$1" ] && holds_kept "$1"; then
    for child in "$1"/*; do prune "$child"; done
    return
  fi
  if rm -rf -- "$1" 2>/dev/null; then printf 'removed\\t%s\\n' "$1"; return; fi
  # Tools like JARVIS leave read-only directories; the person owns them, so
  # make the tree writable and try once more.
  [ -L "$1" ] || chmod -R u+w -- "$1" 2>/dev/null
  if rm -rf -- "$1"; then printf 'removed\\t%s\\n' "$1"; else printf 'refused\\t%s\\n' "$1"; fi
}}
for target in {quoted_targets}; do
  path=$(expand "$target")
  if [ ! -e "$path" ] && [ ! -L "$path" ]; then printf 'absent\\t%s\\n' "$path"; continue; fi
  path=$(physical "$path")
  case "$home/" in
    "$path"/*|"$path/") printf 'refused\\t%s\\n' "$path"; continue ;;
  esac
  if [ "$path" = / ] || [ "$path" = "$home" ]; then
    printf 'refused\\t%s\\n' "$path"
    continue
  fi
  prune "$path"
done
exit 0
"""


def parse_report(text: str) -> dict[str, Any]:
    """Read the script's lines into ``removed``, ``kept``, ``absent`` and ``refused``.

    A prune that found relay's jobs still running also has ``live``.
    """
    report: dict[str, Any] = {"removed": [], "kept": [], "absent": [], "refused": []}
    for line in text.splitlines():
        word, sep, path = line.partition("\t")
        if sep and word in report and path not in report[word]:
            report[word].append(path)
    live = parse_live(text)
    if live:
        report["live"] = live
    return report
