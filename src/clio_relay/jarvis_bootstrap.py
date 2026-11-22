"""The script that installs relay's jarvis-cd on a site, kept apart from its callers."""

BOOTSTRAP_SCRIPT = r"""set -eu
home=$1 pin=$2 source=$3 want=${4:-}
case $home in
  "~") home=$HOME ;;
  "~/"*) home="$HOME/${home#??}" ;;
  /*) ;;
  *) home="$HOME/$home" ;;
esac
dir="$home/jarvis-cd"
mkdir -p "$dir"
# One install at a time per site home, across relay instances and operations: the lock
# is held until this script exits. Whoever waited reuses the install it waited for.
lock="$dir/.relay.lock"
if command -v flock >/dev/null 2>&1; then
  exec 9>>"$lock"
  flock 9
else
  waited=0
  until mkdir "$lock.d" 2>/dev/null; do
    waited=$((waited + 2))
    [ "$waited" -lt 3600 ] || {
      echo "another jarvis-cd install holds $lock.d; remove it if none is running" >&2
      exit 1
    }
    sleep 2
  done
  trap 'rmdir "$lock.d" 2>/dev/null || true' EXIT
fi
log="$dir/install.log"
if [ "$(tr -cd '0-9a-f' 2>/dev/null < "$dir/pin")" = "$pin" ] && [ -x "$dir/jarvis" ] \
  && [ -x "$dir/pip" ]; then
  python=$(cat "$dir/python" 2>/dev/null) || python=
  printf '{"jarvis":"%s","pin":"%s","log":"%s","python":"%s","from":"reused"}\n' \
    "$dir/jarvis" "$pin" "$log" "$python"
  exit 0
fi
: > "$log"

# Find a Python 3.11 or newer that can build a venv with pip over https.
check='import sys
v = "%d.%d.%d" % sys.version_info[:3]
if sys.version_info < (3, 11):
    print(v)
    sys.exit(1)
try:
    import ensurepip, ssl, venv
except ImportError as e:
    print(v + " without " + (e.name or "a module it needs"))
    sys.exit(1)
print(v)'
tried="" python="" from="" libs=""
note() { tried="${tried:+$tried; }$1"; }
try_program() {
  local name=$1 p v
  case $name in "~/"*) name="$HOME/${name#??}" ;; esac
  p=$(command -v "$name" 2>/dev/null) || { note "$1: not found"; return 1; }
  if v=$("$p" -c "$check" 2>/dev/null); then python=$p from=$1; return 0; fi
  note "$1: ${v:-does not run}"
  return 1
}
# A site's modules are set up by its login profile, so they are asked in a login shell.
login() { bash -lc "$1" clio-relay-python "${@:2}" </dev/null 2>/dev/null || true; }
try_module() {
  local out p lib v
  out=$(login 'module load "$1" >/dev/null 2>&1 || exit 0
p=$(command -v python3) || exit 0
printf "@@py %s\n@@lib %s\n" "$p" "${LD_LIBRARY_PATH:-}"' "$1")
  p=$(printf '%s\n' "$out" | sed -n 's/^@@py //p' | tail -n 1)
  lib=$(printf '%s\n' "$out" | sed -n 's/^@@lib //p' | tail -n 1)
  [ -n "$p" ] || { note "module $1: does not load a python3"; return 1; }
  if v=$("$p" -c "$check" 2>/dev/null); then python=$p from="module $1"; return 0; fi
  if v=$(env LD_LIBRARY_PATH="$lib" "$p" -c "$check" 2>/dev/null); then
    python=$p from="module $1" libs=$lib
    return 0
  fi
  note "module $1: ${v:-does not run}"
  return 1
}
python_modules() {
  login 'type module >/dev/null 2>&1 || exit 0; module -t avail 2>&1' \
    | tr -d '\r' | sed -E 's/\(.*\)//; s/[[:space:]]+$//' \
    | grep -E '(^|/)[Pp]ython[0-9.]*/[0-9][^/[:space:]]*$' \
    | awk -F/ '{ print $NF, length($0), $0 }' \
    | { sort -k1,1Vr -k2,2n 2>/dev/null || sort -r; } \
    | awk '!seen[$1]++ { print $3 }' || true
}
find_python() {
  local name m
  try_program python3 && return 0
  for name in $(compgen -c python3. | grep -E '^python3\.[0-9]+$' | sort -u -V -r); do
    try_program "$name" && return 0
  done
  m=$(python_modules)
  [ -n "$m" ] || { note "modules: none named python"; return 1; }
  for name in $m; do try_module "$name" && return 0; done
  return 1
}
how="Name one in the site's jarvis_python setting: a path, or module:<name>"
need="jarvis-cd needs Python 3.11 or newer"
case $want in
  "")
    find_python || {
      echo "$need, and relay found none on this site. Tried $tried. $how" >&2
      exit 69
    } ;;
  module:*)
    try_module "${want#module:}" || {
      echo "$need, and the site's jarvis_python setting ($want) gave none: $tried" >&2
      exit 69
    } ;;
  *)
    try_program "$want" || {
      echo "$need, and the site's jarvis_python setting ($want) gave none: $tried" >&2
      exit 69
    } ;;
esac
[ -z "$libs" ] || export LD_LIBRARY_PATH="$libs${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
echo "python: $python ($from)" >>"$log"

# A fresh environment of relay's own each time: a new pin never keeps an old jarvis-cd,
# and a failed install leaves no jarvis behind.
rm -rf "$dir/venv" "$dir/pin" "$dir/jarvis" "$dir/python" "$dir/pip"
target="$dir/venv/bin/jarvis"
# jarvis-cd takes its version from git (setuptools-scm), and an archive has no .git, so
# the build is told one that names the pin.
export SETUPTOOLS_SCM_PRETEND_VERSION_FOR_JARVIS_CD="0+g${pin:0:12}"
if ! { "$python" -m venv "$dir/venv" >>"$log" 2>&1 \
  && "$dir/venv/bin/python" -m pip install --upgrade pip >>"$log" 2>&1 \
  && "$dir/venv/bin/python" -m pip install "jarvis-cd @ $source" >>"$log" 2>&1; } \
  || [ ! -x "$target" ]; then
  tail -n 40 "$log" >&2
  exit 1
fi
if [ -n "$libs" ]; then
  # The interpreter runs only with its module's libraries. JARVIS's jobs run that same
  # interpreter (the venv's), and inherit them from the jarvis that submits them.
  wrapper='#!/bin/sh\nexport LD_LIBRARY_PATH=%s${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}\n'
  printf "$wrapper"'exec %s "$@"\n' "$(printf '%q' "$libs")" "$(printf '%q' "$target")" \
    > "$dir/jarvis"
  printf "$wrapper"'exec %s -m pip "$@"\n' "$(printf '%q' "$libs")" \
    "$(printf '%q' "$dir/venv/bin/python")" > "$dir/pip"
  chmod +x "$dir/jarvis" "$dir/pip"
else
  ln -sfn "$target" "$dir/jarvis"
  printf '#!/bin/sh\nexec %s -m pip "$@"\n' "$(printf '%q' "$dir/venv/bin/python")" > "$dir/pip"
  chmod +x "$dir/pip"
fi
printf '%s\n' "$python" > "$dir/python"
printf '%s\n' "$pin" > "$dir/pin"
printf '{"jarvis":"%s","pin":"%s","log":"%s","python":"%s","from":"%s"}\n' \
  "$dir/jarvis" "$pin" "$log" "$python" "$from"
"""
"""Install jarvis-cd under the CLIO site home. ``bash -c BOOTSTRAP_SCRIPT <$0> <home>
<pin> <source> [python]`` (:func:`bootstrap_arguments`).

It finds a Python 3.11 or newer with ``ssl``, ``venv`` and ``ensurepip``. With
the site's ``jarvis_python`` setting (``python``) it uses only what that names:
a program (a path, ``~/...``, or a name on ``PATH``), or ``module:<name>``, an
environment module to load. Otherwise it tries ``python3``, then each
``python3.<minor>`` on ``PATH``, newest first, then the site's modules named
``python``, newest first, loaded in a login shell because the site's profile
sets modules up.

It makes a fresh virtual environment from that interpreter and installs
jarvis-cd at the pinned commit's source archive into it, never into the user's
own site-packages. The archive has no git metadata, so the build gets the
version ``0+g<pin>``. JARVIS's jobs run the venv's interpreter, so they use the
same Python. A module's interpreter that runs only with the module's libraries
gets ``<home>/jarvis-cd/jarvis`` as a wrapper that exports them; otherwise that
is a link to the venv's ``jarvis`` (:func:`bootstrap_path`). Beside it,
``<home>/jarvis-cd/pip`` runs the venv's pip the same way, for the packages'
Python requirements. It writes the pin
and the interpreter beside it and prints ``{"jarvis", "pin", "log", "python",
"from"}``, by absolute path. A relative ``home`` is from the home directory.
The log is ``<home>/jarvis-cd/install.log``. It exits 69, naming what it
tried, when the site has no usable Python, and 1 when the install fails.

Installs on one site home take turns: the script holds
``<home>/jarvis-cd/.relay.lock`` (``flock``, or a ``.relay.lock.d`` directory
where there is no ``flock``) until it exits, so two operations, or two relay
instances, never rebuild the venv under each other. After the lock, a jarvis-cd
already at the pin (its ``pin``, ``jarvis`` and ``pip`` in place) is reused, and
the script prints the same line with ``"from": "reused"``. The site script takes
the same lock around installing a package's Python requirements into that venv.
"""
