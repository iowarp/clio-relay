"""What ``clio-relay site clean`` runs on a site: a bounded, home-safe prune.

Adapters that implement :class:`~clio_relay.adapters.base.CleansSite` send
:func:`prune_script` over their own connection and read it back with
:func:`parse_report`.
"""

from __future__ import annotations

import posixpath
import shlex

from clio_relay.jarvis_submit import SPACK_CONFIG_DIR, SPACK_DIR


def spack_keeps(home: str) -> list[str]:
    """The person's Spack store and config under a site home, which clean keeps."""
    base = home.rstrip("/") or "/"
    return [posixpath.join(base, SPACK_DIR), posixpath.join(base, SPACK_CONFIG_DIR)]


def prune_script(targets: list[str], keep: list[str]) -> str:
    """Return the bash that removes ``targets`` on a site, sparing ``keep``.

    A path is absolute, ``~`` or ``~/...``, or relative to the home directory.
    ``keep`` entries may hold ``*`` globs. It never removes the home directory, ``/``
    or an ancestor of the home, and prints ``removed``, ``kept``, ``absent`` or
    ``refused`` and a path on each line.
    """
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
  if [ -d "$1" ] && [ ! -L "$1" ]; then (cd "$1" && pwd -P); else printf '%s' "${{1%/}}"; fi
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
