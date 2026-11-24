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
case $mode in submit|plan|run|install|check|recheck) ;;
  *) fail 64 "mode $mode is not submit, plan, run, install, check or recheck" ;;
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
outputs=$(home "${CLIO_RELAY_OUTPUTS:-}")
bind=${CLIO_RELAY_PLAN:-}
unset CLIO_RELAY_JARVIS_FALLBACK CLIO_RELAY_JARVIS_PIN CLIO_RELAY_JARVIS_DECISIONS
unset CLIO_RELAY_JARVIS_GRAPH CLIO_RELAY_OUTPUTS CLIO_RELAY_PLAN
unset CLIO_RELAY_JARVIS_SPACK_ENV CLIO_RELAY_SPACK_ROOT
unset JARVIS_ALLOW_LOCAL_BUILD
# Where checks and builds run is the site's choice: JARVIS asks it (install_placement),
# relay relays it and sets no placement and no limits of its own.
unset JARVIS_INSTALL_PLACEMENT_CHECK JARVIS_INSTALL_PLACEMENT_BUILD
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
# Only relay's pinned JARVIS is known to plan. An older one ignores an unknown +plan and
# would submit, so any other JARVIS makes no plan and submits unbound.
plans=; [ -n "$jarvis" ] || [ -z "$pin" ] || plans=1
printf '%s\n' "$found" > "$job/relay.jarvis"
root="$job/.relay-jarvis"
export JARVIS_ROOT="$root"
# What the pipeline writes (JARVIS's shared directory) lives in the site's outputs
# directory, outside relay's own: a site clean removes the link, never the outputs.
shared="$root/shared"
[ -z "$outputs" ] || shared="$outputs/${job##*/}"
# Set up once per set of archives: the root keeps the install records and executions.
# Archives of one repository unpack into one tree, which JARVIS registers once.
if [ "$(cat "$root/.relay-setup" 2>/dev/null)" != "$steps" ]; then
  rm -rf "$root"
  mkdir -p "$root/src" "$shared"
  [ "$shared" = "$root/shared" ] || ln -sfn "$shared" "$root/shared"
  "$found" init "$root/config" "$root/private" "$shared" >/dev/null
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
    # +force, in this job's own root only, so a staged archive always replaces any
    # repository of its name. builtin never arrives here: relay refuses to stage it.
    "$found" repo add "$root/src/$repo" +force >/dev/null
  done
  printf '%s\n' "$steps" > "$root/.relay-setup"
fi
# jarvis init lists jarvis-cd's builtin repository; the job's root never keeps it.
! grep -qs builtin "$root/repos.yaml" || "$found" repo remove builtin >/dev/null 2>&1 || :
# Where the outputs really are, so relay names them there, never through the link.
printf '%s\n' "$shared" > "$job/relay.shared"
printf '{"schema_version":"clio-relay.shared.v1","path":"%s"}\n' \
  "$(printf '%s' "$shared" | sed 's/[\\"]/\\&/g')"
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
route() {
  # Where JARVIS's report sends this install: "job" (a job checks or builds: every issue
  # install_unchecked, or for an install also not_installed with a compute_job build),
  # "login" (an install whose build JARVIS placed on this host, login_limited, with the
  # host free) or "here" (the report is the answer: all there, or a question).
  py="${found%/*}/python"
  [ -x "$py" ] || py="${found%/*}/venv/bin/python"
  [ "$2" -eq 3 ] || { echo here; return 0; }
  [ -x "$py" ] || { echo job; return 0; }
  printf '%s\n' "$1" | tail -n 1 | "$py" -c 'import json, sys
try:
    report = json.loads(sys.stdin.read())
except ValueError:
    report = {}
kinds = {i.get("kind") for i in report.get("issues") or [] if isinstance(i, dict)}
placement = report.get("placement") or {}
build = (placement.get("build") or {}).get("choice")
limits = (placement.get("evidence") or {}).get("limits") or {}
if kinds == {"install_unchecked"}:
    print("job")
elif sys.argv[1] != "install" or not kinds or not kinds <= {"install_unchecked", "not_installed"}:
    print("here")
elif kinds == {"not_installed"} and build == "login_limited" and not limits.get("defer"):
    print("login")
else:
    print("job")' "$mode"
}
login_build() {
  # JARVIS builds on this host under its own limits (ppl install --answers): relay starts
  # it detached and returns at once, sets no limits, and the person's cancel stops it.
  lb="$job/relay.login-build" report=$1
  pid=$(cat "$lb/pid" 2>/dev/null) || pid=
  if [ -z "$pid" ] || [ -s "$lb/exit_code" ] || ! kill -0 "$pid" 2>/dev/null; then
    rm -rf "$lb"
    mkdir -p "$lb"
    cat > "$lb/supervise.sh" <<'SUPERVISE'
cd "$1" || exit 1
shift
ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
st() {
  printf '{"state":"%s","exit_code":%s,"supervisor_pid":%s,"child_pid":%s}\n' \
    "$1" "$2" "$$" "${c:-null}" > state.json.tmp && mv -f state.json.tmp state.json
}
c=
echo $$ > pid.tmp && mv -f pid.tmp pid
st queued null
if [ -e cancel ]; then
  echo 143 > exit_code.tmp && mv -f exit_code.tmp exit_code
  st cancelled 143
  exit 0
fi
if command -v setsid >/dev/null 2>&1; then
  setsid "$@" </dev/null >report.json 2>progress.log &
else
  "$@" </dev/null >report.json 2>progress.log &
fi
c=$!
echo "$c" > child_pid
echo "$(ts) running pid $c" >> states
st running null
ec=0
wait "$c" || ec=$?
if [ -e cancel ] && [ "$ec" -ne 0 ]; then s=cancelled
elif [ "$ec" -eq 0 ]; then s=completed
else s=failed; fi
echo "$ec" > exit_code.tmp && mv -f exit_code.tmp exit_code
st "$s" "$ec"
SUPERVISE
    set -- sh "$lb/supervise.sh" "$lb" "$found" ppl install yaml "$pipeline" \
      --answers "$answers" +json
    if command -v setsid >/dev/null 2>&1; then
      (setsid "$@" </dev/null >"$lb/supervisor.log" 2>&1 &)
    else
      (nohup "$@" </dev/null >"$lb/supervisor.log" 2>&1 &)
    fi
    n=0
    while [ ! -s "$lb/pid" ] && [ $n -lt 300 ]; do
      sleep 0.1 2>/dev/null || sleep 1; n=$((n + 1))
    done
    pid=$(cat "$lb/pid" 2>/dev/null) || pid=
    [ -n "$pid" ] || fail 1 "JARVIS's build on this host did not start: $(cat "$lb/supervisor.log")"
  fi
  rm -f "$job/relay.install.json"
  py="${found%/*}/python"
  [ -x "$py" ] || py="${found%/*}/venv/bin/python"
  "$py" -c 'import json, socket, sys
try:
    log = json.loads(sys.argv[3]).get("log")
except (ValueError, AttributeError):
    log = None
log = log if isinstance(log, str) and log else sys.argv[1] + "/progress.log"
print(json.dumps({"schema_version": "clio-relay.login-build.v1", "dir": sys.argv[1],
                  "pid": int(sys.argv[2]), "host": socket.gethostname(), "log": log}))' \
    "$lb" "$pid" "$(printf '%s\n' "$report" | tail -n 1)"
  exit 0
}
if [ "$mode" = recheck ] || [ "$mode" = install ]; then
  # JARVIS records the answers (where checks and builds run among them) only on this
  # host, and under the site's login_limited choice checks here within its limits. Its
  # report says where the rest goes (route): a job, its build here, or nowhere.
  rc=0
  out=$("$found" ppl install yaml "$pipeline" --answers "$answers" +check +json) || rc=$?
  where=$(route "$out" "$rc")
  [ "$where" != login ] || login_build "$out"
  if [ "$where" != job ]; then
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
plan() {
  # JARVIS plans the submit without making it: no Spack, no job and no record. The
  # same install policy, execution id and directory as the submit, so the shas match.
  prc=0
  rm -f "$job/relay.plan.json"
  [ -n "$plans" ] || { prc=1; return 0; }
  JARVIS_INSTALL_ON_LOAD=check "$found" ppl submit "$pipeline" --execution-id "$execution" \
    +plan +json > "$job/relay.plan.json.tmp" || prc=$?
  if grep -qF '"schema_version":"jarvis.submit-plan.v1"' "$job/relay.plan.json.tmp"; then
    tail -n 1 "$job/relay.plan.json.tmp" > "$job/relay.plan.json"
  else
    rm -f "$job/relay.plan.json"
  fi
  rm -f "$job/relay.plan.json.tmp"
}
if [ "$mode" = plan ]; then
  plan
  if [ -f "$job/relay.plan.json" ]; then
    cat "$job/relay.plan.json"
    case $prc in 0|3|4) exit 10 ;; esac
  fi
  [ "$prc" -ne 0 ] || prc=1
  exit "$prc"
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
  # The submit is bound to a plan: the one the person said yes to, else the one JARVIS
  # makes now. A JARVIS that cannot plan submits unbound.
  sealed=
  case $bind in
    sha256:*) sealed=${bind#sha256:} ;;
    *)
      plan
      if [ "$prc" -eq 0 ] && [ -f "$job/relay.plan.json" ]; then
        sealed=$(sed -n 's/.*"plan_sha256":"\([0-9a-f]*\)".*/\1/p' "$job/relay.plan.json") \
          || sealed=
      fi
      ;;
  esac
  [ ! -f "$job/relay.plan.json" ] || cat "$job/relay.plan.json"
  out=$(JARVIS_INSTALL_ON_LOAD=check "$found" ppl submit "$pipeline" \
    --execution-id "$execution" ${sealed:+--plan_sha256 "$sealed"} +json) || rc=$?
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
``JARVIS_ALLOW_LOCAL_BUILD`` and JARVIS's placement variables: where checks and
builds run is the site's answer to JARVIS's ``install_placement`` question, and
relay sets no placement and no limits. It unpacks the
archives into the job's private
JARVIS root and registers each repository once, once per set of archives. Before
it registers one whose archive lists Python requirements
(:data:`~clio_relay.jarvis_site.REQUIREMENTS_FILE`), it installs them with relay's
jarvis-cd ``pip`` (only for relay's own JARVIS, logged in
``relay.requirements.log``, under the ``.relay.lock`` the bootstrap holds, so
two jobs never install into that venv at once), and fails with exit 1 when that
install fails. It writes where JARVIS's shared directory really is (the site's
outputs directory when it has one) to ``relay.shared`` and prints it as a
``clio-relay.shared.v1`` line, so relay names outputs there and never through
the job root's link (:func:`~clio_relay.jarvis_submit.shared_dir`).
Relay sets no MPI network: JARVIS's MPI launches apply the one the site's
resource graph selects. Then it hands the pipeline to JARVIS:

- ``submit`` first runs the light ``ppl install +check`` and stops as below
  unless it says the software is all there. Then it runs ``ppl check +json``
  and keeps its document as
  ``relay.check.json``; when JARVIS refuses the pipeline (exit 5, typed issues)
  it prints that document and exits 5. It writes the first allocation index of
  the first service JARVIS names to ``relay.service-node``, where the connector
  starts (:func:`connector_command`). Then it binds the submit to a plan:
  ``$CLIO_RELAY_PLAN`` (``sha256:<plan_sha256>``, the plan the person said yes
  to) when it is set, else the plan ``ppl submit +plan +json`` makes now, kept
  as ``relay.plan.json`` and printed before the submit (a JARVIS that cannot
  plan, and a JARVIS the site's setting names, which may be older, submits
  unbound). Then it runs ``ppl submit --plan_sha256`` under
  ``JARVIS_INSTALL_ON_LOAD=check`` (checked in the allocation) and keeps the
  handle line as ``relay.submission.json``; JARVIS refuses a submit whose
  script or input changed since the plan (exit 5, ``submission_plan_changed``);
- ``plan`` runs only ``ppl submit +plan +json``, under the same install policy
  and execution id as the submit, with no Spack, no job and no record. It keeps
  and prints the ``jarvis.submit-plan.v1`` document and exits 10 when JARVIS
  made one (its own exit 0, 3 or 4); otherwise it exits with JARVIS's code;
- ``install`` (the answers JSON comes first) keeps the answers as
  ``relay.answers.json``. A pipeline YAML given to ``install`` is the
  install job's own (:func:`install_pipeline`), kept as
  ``relay.install-pipeline.yaml``, so the run's pipeline stays as it was.
  It first runs ``ppl install +check --answers`` on this host, the command whose
  answers JARVIS records for where checks and builds run (``install_placement``);
  under the site's ``login_limited`` check JARVIS checks here within its limits.
  JARVIS's report then routes it: exit 0 (all there) or a new question prints
  that report and exits with its code, without a job. When the only issue is
  ``not_installed`` and JARVIS placed the build on this host (its ``placement``
  says ``build.choice`` ``login_limited`` and the host is not too busy), it
  starts JARVIS's own ``ppl install --answers +json`` detached in
  ``relay.login-build/`` (its report, ``progress.log``, ``pid``, ``child_pid``,
  ``exit_code`` and ``state.json``), prints a ``clio-relay.login-build.v1``
  document (``dir``, ``pid``, ``host`` and JARVIS's ``log``) and exits 0. JARVIS
  sets and applies the limits; relay sets none and no time limit, and a cancel
  stops it. Otherwise (a check JARVIS defers to a job, ``install_unchecked``, or a
  ``compute_job`` build) it runs ``ppl install ... +submit`` and keeps the handle
  line as ``relay.install.json``;
  ``recheck`` is ``install`` with ``+check``: the job only checks, and it never
  builds here;
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
``JARVIS_INSTALL_ANSWERS``. Exit 4 (JARVIS refused to build where it was asked,
outside an allocation or on a login host too busy for its limits) passes through
unchanged; relay reports it with :func:`build_refused` and never retries it. The
exit code is also written to ``relay.exitcode``.
"""
