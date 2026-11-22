"""What ``clio-relay site clean`` runs on a site: a bounded, home-safe prune.

Adapters that implement :class:`~clio_relay.adapters.base.CleansSite` send
:func:`prune_script` over their own connection and read it back with
:func:`parse_report`.
"""

from __future__ import annotations

import posixpath
import shlex

from clio_relay.jarvis_submit import (
    BOOTSTRAP_DIR,
    DECISIONS_FILE,
    SITE_DIR,
    SPACK_CONFIG_DIR,
    SPACK_DIR,
)

DECISIONS_TAIL = posixpath.join(BOOTSTRAP_DIR, SITE_DIR, DECISIONS_FILE)
"""Where JARVIS records the person's install answers, under a site home."""


def spack_keeps(home: str) -> list[str]:
    """The person's Spack store and config under a site home, which clean keeps."""
    base = home.rstrip("/") or "/"
    return [posixpath.join(base, SPACK_DIR), posixpath.join(base, SPACK_CONFIG_DIR)]


def site_keeps(home: str) -> list[str]:
    """What clean keeps under a site home: the Spack store, and JARVIS's recorded answers.

    The answers (which MPI, which compiler) are facts about the cluster the
    person confirmed; without them the next install asks again.
    """
    base = home.rstrip("/") or "/"
    return [*spack_keeps(home), posixpath.join(base, DECISIONS_TAIL)]


def is_decisions(path: str) -> bool:
    """Say whether a keep is JARVIS's decisions file rather than the Spack store."""
    return path.endswith("/" + DECISIONS_TAIL)


def kept(keep: list[str], *, spack: bool = True, decisions: bool = True) -> list[str]:
    """Return ``keep`` without the Spack store or the decisions, as asked."""
    return list(
        dict.fromkeys(path for path in keep if (decisions if is_decisions(path) else spack))
    )


def prune_script(targets: list[str], keep: list[str]) -> str:
    """Return the shell command that removes ``targets`` on a site, sparing ``keep``.

    A path is absolute, ``~`` or ``~/...``, or relative to the home directory.
    ``keep`` entries may hold ``*`` globs. It never removes the home directory, ``/``
    or an ancestor of the home, and prints ``removed``, ``kept``, ``absent`` or
    ``refused`` and a path on each line. The prune itself is bash, so the command
    is ``bash -c <prune>``: it runs the same under ``sh`` (dash) or a login shell.
    """
    return shlex.join(["bash", "-c", _prune_bash(targets, keep), "clio-relay-prune"])


def _prune_bash(targets: list[str], keep: list[str]) -> str:
    quoted_targets = " ".join(shlex.quote(item) for item in targets)
    quoted_keep = " ".join(shlex.quote(item) for item in keep)
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


def parse_report(text: str) -> dict[str, list[str]]:
    """Read the script's lines into ``removed``, ``kept``, ``absent`` and ``refused``."""
    report: dict[str, list[str]] = {"removed": [], "kept": [], "absent": [], "refused": []}
    for line in text.splitlines():
        word, sep, path = line.partition("\t")
        if sep and word in report and path not in report[word]:
            report[word].append(path)
    return report
