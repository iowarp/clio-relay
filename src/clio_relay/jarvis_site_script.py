"""The site script: the bash program relay runs in a JARVIS job directory on a site.

Relay copies it to the site and runs it with :func:`clio_relay.jarvis_submit.site_arguments`.
"""

from __future__ import annotations

SITE_SCRIPT = r"""set -eu
mode=$1 jarvis=$2 execution=$3 steps=$4
shift 4
job=$PWD
trap 'rc=$?; printf "%s\n" "$rc" > "$job/relay.exitcode"' EXIT
fail() { echo "clio-relay jarvis: $2" >&2; exit "$1"; }
case $mode in submit|run|install|check|recheck) ;;
  *) fail 64 "mode $mode is not submit, run, install, check or recheck" ;;
esac
printf '%s\n' "$steps" | tr , '\n' \
  | grep -Evq '^[A-Za-z0-9_]+\.[A-Za-z0-9_]+@[0-9a-f]{64}$' \
  && fail 64 "the steps $steps are not <repo>.<package>@<sha256>,..."
[ -n "$steps" ] || fail 64 "a pipeline needs a package"
if [ -n "$execution" ]; then
  printf '%s\n' "$execution" | grep -Eq '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$' \
    || fail 64 "execution id $execution is not a JARVIS id"
elif [ "$mode" != run ]; then
  fail 64 "a $mode needs an execution id"
fi
pipeline="$job/relay.pipeline.yaml"
answers="$job/relay.answers.json"
if [ "$mode" = install ] || [ "$mode" = recheck ]; then
  [ $# -ge 1 ] || fail 64 "an $mode needs the answers"
  printf '%s\n' "$1" > "$answers.tmp" && mv -f "$answers.tmp" "$answers"
  shift
  [ $# -eq 0 ] || pipeline="$job/relay.install-pipeline.yaml"
fi
[ $# -eq 0 ] || printf '%s' "$1" > "$pipeline"
[ -f "$pipeline" ] || fail 66 "the pipeline $pipeline is missing"
sha() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  else shasum -a 256 "$1" | cut -d' ' -f1; fi
}
for step in $(printf '%s' "$steps" | tr , ' '); do
  digest=${step##*@}
  archive="$job/.relay-package/$digest.tar.gz"
  [ -f "$archive" ] || fail 66 "the package archive $archive is missing"
  got=$(sha "$archive")
  [ "$got" = "$digest" ] || fail 65 "the archive's SHA-256 is $got, not the reviewed $digest"
done
home() {
  case $1 in
    "") ;;
    "~") printf '%s' "$HOME" ;;
    "~/"*) printf '%s' "$HOME/${1#??}" ;;
    /*) printf '%s' "$1" ;;
    *) printf '%s' "$HOME/$1" ;;
  esac
}
fallback=$(home "${CLIO_RELAY_JARVIS_FALLBACK:-}")
pin=${CLIO_RELAY_JARVIS_PIN:-}
decisions=$(home "${CLIO_RELAY_JARVIS_DECISIONS:-}")
graph=$(home "${CLIO_RELAY_JARVIS_GRAPH:-}")
spack_env=$(home "${CLIO_RELAY_JARVIS_SPACK_ENV:-}")
spack_root=$(home "${CLIO_RELAY_SPACK_ROOT:-}")
unset CLIO_RELAY_JARVIS_FALLBACK CLIO_RELAY_JARVIS_PIN CLIO_RELAY_JARVIS_DECISIONS
unset CLIO_RELAY_JARVIS_GRAPH
unset CLIO_RELAY_JARVIS_SPACK_ENV CLIO_RELAY_SPACK_ROOT
# Builds run only in a job JARVIS submits (ppl install +submit), never on this host.
unset JARVIS_ALLOW_LOCAL_BUILD
# So relay, the caller, defaults JARVIS's build placement to a job instead of asking
# where builds run; a site's recorded answer still comes first. Where checks run stays
# the site's choice: JARVIS asks it (install_placement) and relay relays it.
export JARVIS_INSTALL_PLACEMENT_BUILD=compute_job
unset JARVIS_INSTALL_PLACEMENT_CHECK
# The site's JARVIS Spack (jarvis spack prepare), when there is one. Without it JARVIS
# looks for a Spack itself and asks spack_missing when it finds none.
if [ -n "$spack_root" ] && [ -x "$spack_root/bin/spack" ]; then
  export JARVIS_SPACK_ROOT="$spack_root"
fi
# The site's own JARVIS record, shared by every per-job root: the resource graph and the
# install decisions. JARVIS reads and writes both; relay keeps no copy.
if [ -n "$graph" ]; then
  mkdir -p "$(dirname "$graph")"
  export JARVIS_RESOURCE_GRAPH="$graph"
fi
if [ -n "$decisions" ]; then
  mkdir -p "$(dirname "$decisions")"
  export JARVIS_SPACK_DECISIONS="$decisions"
fi
if [ -n "$spack_env" ]; then
  mkdir -p "$spack_env"
  export JARVIS_SPACK_ENV_DIR="$spack_env"
fi
refuse() {
  tried=$(printf '%s' "$1" | sed 's/[\\"]/\\&/g')
  printf '{"missing":"jarvis","reason":"adapter_unavailable","tried":"%s"%s}\n' "$tried" "$2" \
    > "$job/relay.refusal.json"
  cat "$job/relay.refusal.json" >&2
  fail 69 "$3"
}
# Relay runs its pinned JARVIS, or the one the site's setting names by path, and never
# looks one up on PATH.
if [ -n "$jarvis" ]; then
  found=$(home "$jarvis")
  [ -f "$found" ] && [ -x "$found" ] \
    || refuse "$found" ',"explicit":true' "the site's jarvis setting $found is not a program"
else
  found=$fallback
  [ -n "$found" ] && [ -f "$found" ] && [ -x "$found" ] \
    || refuse "$found" '' "relay's jarvis-cd is not installed at ${found:-the site home}"
  got=$(tr -cd '0-9a-f' < "${found%/*}/pin" 2>/dev/null) || got=
  [ -z "$pin" ] || [ "$got" = "$pin" ] \
    || refuse "$found" ",\"pin\":\"$got\"" "relay's jarvis-cd is at ${got:-no pin}, not $pin"
fi
printf '%s\n' "$found" > "$job/relay.jarvis"
root="$job/.relay-jarvis"
export JARVIS_ROOT="$root"
# Set up once per set of archives: the root keeps the install records and executions.
# Archives of one repository unpack into one tree, which JARVIS registers once.
if [ "$(cat "$root/.relay-setup" 2>/dev/null)" != "$steps" ]; then
  rm -rf "$root"
  mkdir -p "$root/src"
  "$found" init "$root/config" "$root/private" "$root/shared" >/dev/null
  repos=" "
  for step in $(printf '%s' "$steps" | tr , ' '); do
    name=${step%@*} digest=${step##*@}
    repo=${name%%.*}
    tar -xzf "$job/.relay-package/$digest.tar.gz" -C "$root/src"
    [ -d "$root/src/$repo/$repo" ] || fail 66 "the archive of $name has no $repo/$repo directory"
    case $repos in *" $repo "*) ;; *) repos="$repos$repo " ;; esac
  done
  for repo in $repos; do
    requirements="$root/src/$repo/.clio-relay-requirements.txt"
    # Relay installs a repository's Python requirements only into its own jarvis-cd,
    # never into the environment of a JARVIS the site's setting names.
    if [ -z "$jarvis" ] && [ -s "$requirements" ]; then
      pip="${found%/*}/pip"
      [ -x "$pip" ] || pip="${found%/*}/venv/bin/pip"
      [ -x "$pip" ] || fail 69 "relay's jarvis-cd at ${found%/*} has no pip"
      # Under the bootstrap's lock: concurrent jobs must not install into one venv at once.
      locked() {
        if command -v flock >/dev/null 2>&1; then flock "${found%/*}/.relay.lock" "$@"
        else "$@"; fi
      }
      locked "$pip" install --disable-pip-version-check --no-input -r "$requirements" \
        >>"$job/relay.requirements.log" 2>&1 || {
        tail -n 20 "$job/relay.requirements.log" >&2
        listed_now=$(tr '\n' ' ' < "$requirements")
        fail 1 "installing the Python requirements of $repo failed: $listed_now"
      }
    fi
    "$found" repo add "$root/src/$repo" >/dev/null
  done
  printf '%s\n' "$steps" > "$root/.relay-setup"
fi
[ -f "$answers" ] && export JARVIS_INSTALL_ANSWERS="$answers"
keep_report() {
  printf '%s\n' "$1" | tail -n 1 > "$job/relay.install-report.json.tmp"
  mv -f "$job/relay.install-report.json.tmp" "$job/relay.install-report.json"
}
light() {
  rc=0
  out=$("$found" ppl install yaml "$pipeline" +check +json) || rc=$?
  keep_report "$out"
  [ "$mode" = check ] || [ "$rc" -ne 0 ] || return 0
  printf '%s\n' "$out"
  exit "$rc"
}
[ "$mode" != check ] && [ "$mode" != submit ] || light
deferred() {
  # Whether JARVIS's report only says a job must check (install_unchecked).
  py="${found%/*}/python"
  [ -x "$py" ] || py="${found%/*}/venv/bin/python"
  [ -x "$py" ] || return 0
  printf '%s\n' "$1" | tail -n 1 | "$py" -c 'import json, sys
try:
    issues = json.loads(sys.stdin.read()).get("issues") or []
except ValueError:
    issues = []
sys.exit(0 if issues and all(i.get("kind") == "install_unchecked" for i in issues) else 1)'
}
if [ "$mode" = recheck ]; then
  # JARVIS's install placement may run the check here, under its limits; it records
  # placement answers only on this host. It defers to a job with install_unchecked.
  rc=0
  out=$("$found" ppl install yaml "$pipeline" --answers "$answers" +check +json) || rc=$?
  if [ "$rc" -ne 3 ] || ! deferred "$out"; then
    [ "$rc" -eq 0 ] || [ "$rc" -eq 3 ] && keep_report "$out"
    rm -f "$job/relay.install.json"
    printf '%s\n' "$out"
    exit "$rc"
  fi
fi
if [ "$mode" = run ]; then
  "$found" ppl run yaml "$pipeline" ${execution:+--execution-id "$execution"} +json
  exit 0
fi
rc=0
if [ "$mode" = install ] || [ "$mode" = recheck ]; then
  # Each install round is its own execution, as JARVIS refuses to reuse one.
  base=$execution n=1
  while ls -d "$root"/shared/*/executions/"$execution" >/dev/null 2>&1; do
    n=$((n + 1)); execution=$base-$n
  done
  check=; [ "$mode" = install ] || check=+check
  out=$("$found" ppl install yaml "$pipeline" --answers "$answers" $check \
    +submit --execution-id "$execution" +json) || rc=$?
  saved=relay.install.json
else
  # JARVIS checks the pipeline's coupling and rules; relay keeps its answer, and the
  # connector starts on the first host of the service it names.
  crc=0
  "$found" ppl check yaml "$pipeline" +json > "$job/relay.check.json.tmp" || crc=$?
  mv -f "$job/relay.check.json.tmp" "$job/relay.check.json"
  if [ "$crc" -ne 0 ]; then
    cat "$job/relay.check.json"
    exit "$crc"
  fi
  py="${found%/*}/python"
  [ -x "$py" ] || py="${found%/*}/venv/bin/python"
  if [ -x "$py" ]; then
    "$py" -c 'import json, sys
d = json.load(open(sys.argv[1]))
s = (d.get("services") or [{}])[0]
f = (s.get("nodes") or {}).get("first", 0)
print(f if isinstance(f, int) and not isinstance(f, bool) else 0)' \
      "$job/relay.check.json" > "$job/relay.service-node" 2>/dev/null \
      || printf '0\n' > "$job/relay.service-node"
  else
    printf '0\n' > "$job/relay.service-node"
  fi
  out=$(JARVIS_INSTALL_ON_LOAD=check "$found" ppl submit "$pipeline" \
    --execution-id "$execution" +json) || rc=$?
  saved=relay.submission.json
fi
printf '%s\n' "$out"
# Exit 3 stopped JARVIS before sbatch: its stdout is the jarvis.install report.
if [ "$rc" -eq 3 ]; then keep_report "$out"; exit 3; fi
[ "$rc" -eq 0 ] || exit "$rc"
printf '%s\n' "$out" | tail -n 1 > "$job/$saved.tmp"
mv -f "$job/$saved.tmp" "$job/$saved"
"""
"""The site script. ``bash -c SITE_SCRIPT <$0> <mode> <jarvis> <execution> <steps>
[<answers JSON>] [<pipeline YAML>]``, run in the job directory. ``steps`` is
``<repo>.<package>@<sha256>,...`` (:func:`steps_argument`).

It checks each archive's SHA-256 against the reviewed one and picks JARVIS by
absolute path, never from ``PATH``: the path the site's setting names when it
names one, else ``$CLIO_RELAY_JARVIS_FALLBACK``, relay's pinned jarvis-cd,
whose ``pin`` file must say ``$CLIO_RELAY_JARVIS_PIN``. When that JARVIS is
missing or at another pin it writes ``relay.refusal.json`` (with ``explicit``
or ``pin`` when they apply) and exits 69. A relative path is from the home
directory. ``$CLIO_RELAY_JARVIS_GRAPH``, ``$CLIO_RELAY_JARVIS_DECISIONS`` and
``$CLIO_RELAY_JARVIS_SPACK_ENV`` become ``JARVIS_RESOURCE_GRAPH``,
``JARVIS_SPACK_DECISIONS`` and ``JARVIS_SPACK_ENV_DIR`` (:func:`site_environment`).
When the CLIO Spack is on the site,
``$CLIO_RELAY_SPACK_ROOT`` becomes ``JARVIS_SPACK_ROOT`` (only when its
``bin/spack`` is there). The script unsets
``JARVIS_ALLOW_LOCAL_BUILD``: relay never builds outside a job. It unpacks the
archives into the job's private
JARVIS root and registers each repository once, once per set of archives. Before
it registers one whose archive lists Python requirements
(:data:`~clio_relay.jarvis_site.REQUIREMENTS_FILE`), it installs them with relay's
jarvis-cd ``pip`` (only for relay's own JARVIS, logged in
``relay.requirements.log``, under the ``.relay.lock`` the bootstrap holds, so
two jobs never install into that venv at once), and fails with exit 1 when that
install fails.
Relay sets no MPI network: JARVIS's MPI launches apply the one the site's
resource graph selects. Then it hands the pipeline to JARVIS:

- ``submit`` first runs the light ``ppl install +check`` and stops as below
  unless it says the software is all there. Then it runs ``ppl check +json``
  and keeps its document as
  ``relay.check.json``; when JARVIS refuses the pipeline (exit 5, typed issues)
  it prints that document and exits 5. It writes the first allocation index of
  the first service JARVIS names to ``relay.service-node``, where the connector
  starts (:func:`connector_command`). Then it runs ``ppl submit`` under
  ``JARVIS_INSTALL_ON_LOAD=check`` (checked in the allocation) and keeps the
  handle line as ``relay.submission.json``;
- ``install`` (the answers JSON comes first) keeps the answers as
  ``relay.answers.json``, runs ``ppl install ... +submit`` and keeps the handle
  line as ``relay.install.json``. A pipeline YAML given to ``install`` is the
  install job's own (:func:`install_pipeline`), kept as
  ``relay.install-pipeline.yaml``, so the run's pipeline stays as it was;
  ``recheck`` is ``install`` with ``+check``: the job only checks. It first runs
  ``ppl install +check --answers`` on this host, the command whose answers JARVIS
  records for where checks run (``install_placement``): under the site's
  ``login_limited`` choice JARVIS checks here within its limits, and the script
  prints that report and exits with its code (0 when all is there, 3 with new
  questions) without a job. Only when JARVIS defers to a job (every issue
  ``install_unchecked``) does it submit the check job. Builds keep JARVIS's
  ``compute_job`` placement (``JARVIS_INSTALL_PLACEMENT_BUILD``), as relay builds
  only through ``ppl install +submit``;
- ``run`` runs ``ppl run`` in place;
- ``check`` runs only the light ``ppl install +check``, for an install ahead of
  any run: it exits 0 with the report when the software is all there, else 3.

The light check runs no Spack: JARVIS answers from the ``install-check.json``
record a job's check wrote, while its inputs match, else ``install_unchecked``.
When it exits 3 (software to install, a question, or no record), the script
writes its report to ``relay.install-report.json``, prints it and exits 3; a
site where JARVIS finds no Spack is one of those questions (``spack_missing``,
whose candidates are the site's Spacks JARVIS found). ``ppl submit``'s own
exit 3 prints the same report, kept the same way.
Answers already given reach every check through
``JARVIS_INSTALL_ANSWERS``. Exit 4 (JARVIS refused to build outside an
allocation) passes through unchanged; relay reports it with
:func:`build_refused` and never retries it. The exit code is also written to
``relay.exitcode``.
"""
