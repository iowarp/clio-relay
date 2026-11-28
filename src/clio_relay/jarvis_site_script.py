"""The site script: the bash program relay runs in a JARVIS job directory on a site.

Relay copies it to the site and runs it with :func:`clio_relay.jarvis_submit.site_arguments`.
"""

from __future__ import annotations

from clio_relay.jarvis_site import DIGEST_FUNCTIONS

GRAPH_LOOKUP = (
    'if [ -n "$G" ]; then GS=setting; else\n'
    "  G=${JARVIS_RESOURCE_GRAPH:-}\n"
    '  if [ -z "$G" ] && command -v bash >/dev/null 2>&1; then\n'
    '    GT=; command -v timeout >/dev/null 2>&1 && GT="timeout 20"\n'
    '    G=$($GT bash -lc \'printf "@@graph=%s\\n" "${JARVIS_RESOURCE_GRAPH:-}"\' '
    "</dev/null 2>/dev/null | sed -n 's/^@@graph=//p' | tail -n 1) || G=\n"
    "  fi\n"
    '  case $G in "~") G=$HOME ;; "~/"*) G=$HOME/${G#??} ;; esac\n'
    '  if [ -n "$G" ] && [ -f "$G" ] && [ -r "$G" ]; then GS=site; else G=$GO; GS=relay; fi\n'
    "fi\n"
    'gdir() { d=${1%/*}; (cd "$d" 2>/dev/null && pwd -P) || printf \'%s\\n\' "$d"; }\n'
    "SH=; JG=$G\n"
    'if [ -n "$GO" ]; then\n'
    "  SH=${GO%/*}\n"
    '  if [ "$GS" != relay ] && [ "$(gdir "$G")" = "$(gdir "$GO")" ]; then G=$GO; GS=relay; fi\n'
    '  if [ "$GS" = relay ]; then JG=; fi\n'
    "fi\n"
)
"""Shell lines that set ``G`` to the site's JARVIS resource graph and ``GS`` to its source.

``G`` comes in as the site's ``jarvis_graph`` setting (empty when unset) and
``GO`` as the graph relay keeps under the CLIO site home. The setting wins
(``GS=setting``). Without it, the graph the site publishes as
``$JARVIS_RESOURCE_GRAPH`` in the person's login environment is the site's
(``GS=site``) when it names a readable file: the call's own environment first,
then a login shell's, for a site that sets it only there. Relay never overrides
it. Only with neither is it relay's own (``GS=relay``), which relay's network
collection fills.

JARVIS reads a named graph and never writes it; what it records goes to its
site home, which relay sets to ``SH``, the directory of relay's own graph, so
every write stays under the CLIO site home. ``JG`` is what relay exports as
``JARVIS_RESOURCE_GRAPH``: the site's graph, or empty with none, so JARVIS's own
graph in ``SH`` is relay's (``GO``; jarvis-cd 1.9.18 keeps its state in
``JARVIS_SITE_HOME`` without a site graph too). A site graph in ``SH`` itself is
relay's own: JARVIS refuses a site home that holds the site graph.
"""

SITE_SCRIPT = (
    r"""set -eu
mode=$1 jarvis=$2 execution=$3 steps=$4
shift 4
job=$PWD
trap 'rc=$?; printf "%s\n" "$rc" > "$job/relay.exitcode"' EXIT
fail() { echo "clio-relay jarvis: $2" >&2; exit "$1"; }
case $mode in submit|plan|install|check|recheck) ;;
  *) fail 64 "mode $mode is not submit, plan, install, check or recheck" ;;
esac
printf '%s\n' "$steps" | tr , '\n' \
  | grep -Evq '^[A-Za-z0-9_]+\.[A-Za-z0-9_]+@[0-9a-f]{64}$' \
  && fail 64 "the steps $steps are not <repo>.<package>@<digest>,..."
[ -n "$steps" ] || fail 64 "a pipeline needs a package"
[ -n "$execution" ] || fail 64 "a $mode needs an execution id"
printf '%s\n' "$execution" | grep -Eq '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$' \
  || fail 64 "execution id $execution is not a JARVIS id"
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
"""
    + DIGEST_FUNCTIONS
    + r"""home() {
  case $1 in
    "") ;;
    "~") printf '%s' "$HOME" ;;
    "~/"*) printf '%s' "$HOME/${1#??}" ;;
    /*) printf '%s' "$1" ;;
    *) printf '%s' "$HOME/$1" ;;
  esac
}
site=$(home "${CLIO_RELAY_JARVIS_HOME:-}")
pin=${CLIO_RELAY_JARVIS_PIN:-}
decisions=$(home "${CLIO_RELAY_JARVIS_DECISIONS:-}")
G=$(home "${CLIO_RELAY_JARVIS_GRAPH:-}")
GO=$(home "${CLIO_RELAY_JARVIS_GRAPH_OWN:-}")
"""
    + GRAPH_LOOKUP
    + r"""spack_env=$(home "${CLIO_RELAY_JARVIS_SPACK_ENV:-}")
install_env=$(home "${CLIO_RELAY_JARVIS_INSTALL_ENV:-}")
spack_root=$(home "${CLIO_RELAY_SPACK_ROOT:-}")
outputs=$(home "${CLIO_RELAY_OUTPUTS:-}")
bind=${CLIO_RELAY_PLAN:-}
unset CLIO_RELAY_JARVIS_HOME CLIO_RELAY_JARVIS_PIN CLIO_RELAY_JARVIS_DECISIONS
unset CLIO_RELAY_JARVIS_GRAPH CLIO_RELAY_JARVIS_GRAPH_OWN CLIO_RELAY_OUTPUTS CLIO_RELAY_PLAN
unset CLIO_RELAY_JARVIS_SPACK_ENV CLIO_RELAY_JARVIS_INSTALL_ENV CLIO_RELAY_SPACK_ROOT
unset JARVIS_ALLOW_LOCAL_BUILD
# Where checks and builds run is the site's choice: JARVIS asks it (install_placement),
# relay relays it and sets no placement and no limits of its own.
unset JARVIS_INSTALL_PLACEMENT_CHECK JARVIS_INSTALL_PLACEMENT_BUILD
# The site's JARVIS Spack (jarvis spack prepare), when there is one. Without it JARVIS
# looks for a Spack itself and asks spack_missing when it finds none.
if [ -n "$spack_root" ] && [ -x "$spack_root/bin/spack" ]; then
  export JARVIS_SPACK_ROOT="$spack_root"
fi
# The site's own JARVIS record: the resource graph and the install decisions. A graph
# the site publishes ($JARVIS_RESOURCE_GRAPH) is the site's:
# JARVIS only reads it, and keeps what it records in relay's site home ($SH).
if [ -n "$SH" ]; then mkdir -p "$SH"; export JARVIS_SITE_HOME="$SH"; fi
if [ -n "$JG" ]; then export JARVIS_RESOURCE_GRAPH="$JG"; else unset JARVIS_RESOURCE_GRAPH; fi
if [ -n "$decisions" ]; then
  mkdir -p "$(dirname "$decisions")"
  export JARVIS_SPACK_DECISIONS="$decisions"
fi
if [ -n "$spack_env" ]; then
  mkdir -p "$spack_env"
  export JARVIS_SPACK_ENV_DIR="$spack_env"
fi
# The pip venvs and conda envs JARVIS installs: shared by the install job and the runs,
# as the Spack env is.
if [ -n "$install_env" ]; then
  mkdir -p "$install_env"
  export JARVIS_INSTALL_ENV_DIR="$install_env"
fi
refuse() {
  tried=$(printf '%s' "$1" | sed 's/[\\"]/\\&/g')
  printf '{"missing":"jarvis","reason":"adapter_unavailable","tried":"%s"%s}\n' "$tried" "$2" \
    > "$job/relay.refusal.json"
  cat "$job/relay.refusal.json" >&2
  fail 69 "$3"
}
# Relay runs its pinned JARVIS, or the one the site's setting names by path, and never
# looks one up on PATH, always in the site's one JARVIS root that relay set up.
[ -n "$site" ] || refuse "" '' "relay has no CLIO site home here"
root="$site/jarvis"
if [ -n "$jarvis" ]; then
  found=$(home "$jarvis")
  [ -f "$found" ] && [ -x "$found" ] \
    || refuse "$found" ',"explicit":true' "the site's jarvis setting $found is not a program"
else
  found="$site/bin/jarvis"
  [ -f "$found" ] && [ -x "$found" ] || refuse "$found" '' "relay has not set up jarvis-cd at $site"
fi
got=$(cat "$root/relay.pin" 2>/dev/null) || got=
[ -z "$pin" ] || [ "$got" = "$pin" ] \
  || refuse "$found" ",\"pin\":\"$got\"" "the site's JARVIS root is at ${got:-no setup}, not $pin"
# Only relay's pinned JARVIS is known to plan. An older one ignores an unknown +plan and
# would submit, so any other JARVIS makes no plan and submits unbound.
plans=; [ -n "$jarvis" ] || [ -z "$pin" ] || plans=1
printf '%s\n' "$found" > "$job/relay.jarvis"
# The Python JARVIS runs with (its script's shebang), for relay's reads of JSON.
py=$(head -n 1 "$found" 2>/dev/null | sed -n 's/^#![[:space:]]*\([^[:space:]]*\).*/\1/p')
case ${py##*/} in python*) [ -x "$py" ] || py= ;; *) py= ;; esac
[ -n "$py" ] || py=$(command -v python3 2>/dev/null) || py=
# jarvis-cd's builtin is never registered or resolved: setup removed it from the root.
export JARVIS_ROOT="$root" JARVIS_NO_BUILTIN=1
# Each step runs only as reviewed: its digest in the root now is the one the person saw.
for step in $(printf '%s' "$steps" | tr , ' '); do
  name=${step%@*} digest=${step##*@}
  repo=${name%%.*} pkg=${name#*.}
  dir=$(repo_paths | grep -E "/$repo\$" | tail -n 1) || dir=
  [ -n "$dir" ] && [ -f "$dir/$repo/$pkg/pkg.py" ] \
    || fail 66 "$name is not in the site's JARVIS root $root"
  files=$(relay_files "$dir/$repo" "$pkg") || fail 65 "$name holds a symbolic link"
  now=$(relay_digest "$files")
  [ "$now" = "$digest" ] || fail 65 "$name's digest on the site is $now, not the reviewed $digest"
done
# What the pipeline writes is its directory in JARVIS's shared one, which setup put in
# the site's outputs directory when it has one: <outputs>/<operation id>.
pname=$(sed -n "s/^name:[[:space:]]*['\"]\{0,1\}\([A-Za-z0-9_.-]*\).*/\1/p" "$pipeline" \
  | head -n 1)
[ -n "$pname" ] || fail 66 "the pipeline $pipeline has no name"
shared=$(root_shared)
[ -n "$shared" ] || fail 66 "the site's JARVIS root $root names no shared directory"
if [ -n "$outputs" ]; then
  real=$(mkdir -p "$outputs" && cd "$outputs" && pwd -P) \
    || fail 66 "cannot reach the site's outputs directory $outputs"
  [ "$real" = "$shared" ] || fail 66 "the site's JARVIS root keeps outputs in $shared, \
not in this site's outputs_dir $real; call install with this site to set it up again"
fi
shared="$shared/$pname"
mkdir -p "$shared"
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
  [ "$2" -eq 3 ] || { echo here; return 0; }
  [ -n "$py" ] || { echo job; return 0; }
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
  "${py:-python3}" -c 'import json, socket, sys
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
  while [ -e "$shared/executions/$execution" ]; do
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
  if [ -n "$py" ]; then
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
)
"""The site script. ``bash -c SITE_SCRIPT <$0> <mode> <jarvis> <execution> <steps>
[<answers JSON>] [<pipeline YAML>]``, run in the job directory. ``steps`` is
``<repo>.<package>@<digest>,...`` (:func:`steps_argument`).

It picks JARVIS by absolute path, never from ``PATH``: the path the site's
setting names when it names one, else ``<$CLIO_RELAY_JARVIS_HOME>/bin/jarvis``,
relay's pinned jarvis-cd. It runs every JARVIS call in the site's one root,
``<$CLIO_RELAY_JARVIS_HOME>/jarvis``, whose ``relay.pin`` must say
``$CLIO_RELAY_JARVIS_PIN`` (:func:`clio_relay.jarvis_site.site_pin`). When that
JARVIS is missing or the root is not set up at the pin it writes
``relay.refusal.json`` (with ``explicit`` or ``pin`` when they apply) and exits
69. Then it recomputes each step's digest in the root
(:data:`clio_relay.jarvis_site.DIGEST_FUNCTIONS`) and exits 65 when one differs
from the reviewed digest, or holds a symbolic link; 66 when the root has no
such package. A relative path is from the home
directory. ``$CLIO_RELAY_JARVIS_GRAPH`` (the site's ``jarvis_graph`` setting) or,
without it, the graph the site publishes as ``$JARVIS_RESOURCE_GRAPH``, else
``$CLIO_RELAY_JARVIS_GRAPH_OWN`` (:data:`GRAPH_LOOKUP`), and
``$CLIO_RELAY_JARVIS_DECISIONS``,
``$CLIO_RELAY_JARVIS_SPACK_ENV`` and ``$CLIO_RELAY_JARVIS_INSTALL_ENV`` become
``JARVIS_RESOURCE_GRAPH``, ``JARVIS_SPACK_DECISIONS``, ``JARVIS_SPACK_ENV_DIR``
and ``JARVIS_INSTALL_ENV_DIR`` (:func:`site_environment`), and the directory of
``$CLIO_RELAY_JARVIS_GRAPH_OWN`` becomes ``JARVIS_SITE_HOME``, where JARVIS keeps
what it records over a named graph.
When the CLIO Spack is on the site,
``$CLIO_RELAY_SPACK_ROOT`` becomes ``JARVIS_SPACK_ROOT`` (only when its
``bin/spack`` is there). The script unsets
``JARVIS_ALLOW_LOCAL_BUILD`` and JARVIS's placement variables: where checks and
builds run is the site's answer to JARVIS's ``install_placement`` question, and
relay sets no placement and no limits.
It writes the pipeline's directory in JARVIS's shared one (which setup put in
the site's outputs directory, and which it refuses when the site's outputs
directory is now another) to ``relay.shared`` and prints it as a
``clio-relay.shared.v1`` line, so relay names outputs there
(:func:`~clio_relay.jarvis_submit.shared_dir`).
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
