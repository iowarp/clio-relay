"""A package job handed to JARVIS: the pipeline, the site script, the records (RL40, RL45).

JARVIS owns a package job from end to end. Relay renders one pipeline YAML
from the job, with the run's resources in its ``scheduler:`` block, and a site
script hands it to JARVIS:

- ``submit`` runs ``jarvis ppl submit <pipeline> --execution-id <op> +json`` on
  the login host. JARVIS seals the execution directory, writes the typed
  ``.jarvis-execution.json`` record and the allocation hostfile, binds the
  interpreter and ``JARVIS_ROOT``, writes ``submit.slurm`` and calls ``sbatch``.
  The ssh adapter runs it with its own ``ssh``; the Tapis adapter runs it in a
  FORK job.
- ``run`` runs ``jarvis ppl run yaml <pipeline> +json``, JARVIS's direct run,
  for an adapter that starts the job itself (no batch scheduler).
- ``install`` runs ``jarvis ppl install yaml <pipeline> --answers <file>
  +submit --execution-id <op>-install +json``: JARVIS installs what the
  pipeline's packages need, as a job of its own.

A submit loads the pipeline with ``JARVIS_INSTALL_ON_LOAD=check``. When a
package's software is missing or needs a decision, JARVIS exits 3 before it
calls ``sbatch``; the script then writes JARVIS's ``jarvis.install-report.v1``
to ``relay.install-report.json`` and exits 3 too. Relay asks the person to
approve the install and to answer each ambiguous question
(:func:`install_needed`), runs ``install`` with the answers, follows the
install job's log and progress, and then submits the run again.

Either way the job's JARVIS root is ``<job dir>/.relay-jarvis``, private to the
job, so the person's own pipelines are never read or changed. Status, outputs,
services, cancel and stop all go through JARVIS's ``execution`` commands and its
record, never through relay's own tracking of the batch job or the scheduler.

Relay always runs its own pinned JARVIS, by absolute path, never a ``jarvis``
found on the site's ``PATH``: an older one would reject the service reports and
skip JARVIS's refusal to build on the login node. :data:`BOOTSTRAP_SCRIPT` puts
jarvis-cd at :data:`JARVIS_PIN` under the CLIO site home, after the person
approves it, and the site script refuses when it is missing or at another pin.
A site that needs another JARVIS names its path in the adapter's ``jarvis``
setting (:func:`explicit_jarvis`).
"""

from __future__ import annotations

import hashlib
import json
import math
import posixpath
import re
import shlex
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, Literal, cast

import yaml

from clio_relay.jobs.handles import VIEW_METADATA, JobProgress, JobState, JobStatus, OutputRef
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import CustomValue, InputRef, JobSpec, PackageRef

ARCHIVE_DIR = ".relay-package"
"""Where the site script finds the package archive, relative to the job directory."""

JARVIS_DIR = ".relay-jarvis"
"""The job's private JARVIS root, relative to the job directory."""

PIPELINE_FILE = "relay.pipeline.yaml"
SCRIPT_FILE = "relay-jarvis.sh"
SUBMISSION_FILE = "relay.submission.json"
"""The ``jarvis.execution.handle.v1`` line ``ppl submit`` printed."""
REFUSAL_FILE = "relay.refusal.json"
"""Written when the site has no JARVIS, or no Spack for it: ``{"reason", "missing", "tried"}``."""
EXITCODE_FILE = "relay.exitcode"
JARVIS_FILE = "relay.jarvis"
"""The JARVIS program the site script found, for the ``execution`` commands later."""
INSTALL_REPORT_FILE = "relay.install-report.json"
"""The ``jarvis.install-report.v1`` of a submit that exited 3."""
ANSWERS_FILE = "relay.answers.json"
"""The person's answers to JARVIS's install questions, ``{issue_id: {constraint, ...}}``."""
INSTALL_FILE = "relay.install.json"
"""The ``jarvis.execution.handle.v1`` line ``ppl install +submit`` printed."""

SITE_FILES = (
    PIPELINE_FILE,
    SCRIPT_FILE,
    SUBMISSION_FILE,
    REFUSAL_FILE,
    EXITCODE_FILE,
    JARVIS_FILE,
    INSTALL_REPORT_FILE,
    ANSWERS_FILE,
    INSTALL_FILE,
)
"""Files the site script keeps in the job directory. They are not outputs."""

HANDLE_SCHEMA = "jarvis.execution.handle.v1"
RECORD_SCHEMA = "jarvis.execution.record.v1"
ARTIFACTS_SCHEMA = "jarvis.execution.artifacts.v1"
SERVICE_RUNTIMES_SCHEMA = "jarvis.execution.service-runtimes.v1"
INSTALL_REPORT_SCHEMA = "jarvis.install-report.v1"
RECORD_FILE = ".jarvis-execution.json"

JARVIS_PIN = "c95fa922a1b46aae8a2e3abcc335b0142a54d393"
"""The jarvis-cd commit relay installs under the CLIO site home and runs."""
JARVIS_SOURCE = f"https://github.com/grc-iit/jarvis-cd/archive/{JARVIS_PIN}.tar.gz"
BOOTSTRAP_DIR = "jarvis-cd"
"""Where relay installs jarvis-cd, relative to the CLIO site home."""
FALLBACK_ENV = "CLIO_RELAY_JARVIS_FALLBACK"
"""Relay's pinned JARVIS (:func:`bootstrap_path`), which the site script runs unless the
site's setting names another by path."""
PIN_ENV = "CLIO_RELAY_JARVIS_PIN"
"""The pin the site script expects in ``<home>/jarvis-cd/pin`` beside relay's JARVIS."""
JARVIS_SETTING_HELP = (
    "Leave empty: relay runs its own pinned jarvis-cd, installed under the CLIO site home. "
    "Only a site that needs another jarvis names its path here, absolute or from ~/. Relay "
    "never runs a jarvis found on the site's PATH"
)
"""The description of an adapter's ``jarvis`` setting (:func:`explicit_jarvis`)."""
PIPELINES_DIR = "pipelines"
"""Where the install answers and Spack env of each set of archives are kept, under
:data:`BOOTSTRAP_DIR`."""
DECISIONS_ENV = "CLIO_RELAY_JARVIS_DECISIONS"
"""The file the site script exports as ``JARVIS_SPACK_DECISIONS``."""
SPACK_ENV_ENV = "CLIO_RELAY_JARVIS_SPACK_ENV"
"""The directory the site script exports as ``JARVIS_SPACK_ENV_DIR``."""
SPACK_DIR = "spack"
"""The CLIO Spack checkout, relative to the CLIO site home (clio-kit's ``spack_prepare_site``)."""
SPACK_CONFIG_DIR = "spack-config"
"""The CLIO Spack's own user configuration, relative to the CLIO site home."""
SPACK_ROOT_ENV = "CLIO_RELAY_SPACK_ROOT"
"""The Spack the site script exports as ``JARVIS_SPACK_ROOT`` when it is there."""
SPACK_CONFIG_ENV = "CLIO_RELAY_SPACK_CONFIG"
"""The directory the site script exports as ``JARVIS_SPACK_CONFIG`` when it is there."""

EXIT_DECISION = 3
"""JARVIS needs to install software, or a decision, before the job can run."""
EXIT_BUILD_REFUSED = 4
"""JARVIS refused to build outside an allocation (``refused.kind: build_outside_allocation``).
Relay builds only through ``ppl install +submit``, so this is a refusal, never a retry."""
EXIT_USAGE = 64
"""A bad mode, package name or execution id."""
EXIT_DIGEST = 65
"""The archive's SHA-256 is not the reviewed one."""
EXIT_MISSING = 66
"""The archive, the pipeline or the archive's repository directory is missing."""
EXIT_NO_JARVIS = 69
"""The site has no JARVIS where the site's setting says."""

ExecutionVerb = Literal["get", "progress", "artifacts", "service-runtimes", "cancel", "stop"]
EXECUTION_VERBS: tuple[ExecutionVerb, ...] = (
    "get",
    "progress",
    "artifacts",
    "service-runtimes",
    "cancel",
    "stop",
)

NEEDS_INSTALL = "jarvis ppl install"

_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_TOKEN = re.compile(r"[^\s#]+")
_NAME = re.compile(r"[a-z][a-z0-9_]*")
_RESERVED_PARAMETERS = frozenset({"pkg_type", "pkg_name"})
_SLURM_PREFIX = "slurm."

# The ``scheduler:`` keys relay sets itself. A ``slurm.<key>`` custom attribute
# may not replace them.
_OWN_KEYS = frozenset(
    {
        "name",
        "job_name",
        "nodes",
        "ntasks",
        "ntasks_per_node",
        "cpus_per_task",
        "mem",
        "time",
        "partition",
        "account",
        "reservation",
        "exclusive",
        "gpus_per_node",
        "output",
        "error",
        "hostfile",
        "suffix",
        "sbatch_args",
        "pre_cmds",
        "post_cmds",
    }
)


# Names and paths


def pipeline_id(operation_id: str) -> str:
    """Return the JARVIS pipeline id of an operation's package job."""
    return f"relay_{operation_id}"


def install_execution_id(operation_id: str) -> str:
    """Return the execution id of the job that installs an operation's software.

    It lives in the operation's pipeline, ``pipeline_id(operation_id)``.
    """
    return f"{operation_id}-install"


def bootstrap_path(home: str) -> str:
    """Return where :data:`BOOTSTRAP_SCRIPT` links the JARVIS it installs under ``home``."""
    return posixpath.join(home, BOOTSTRAP_DIR, "jarvis")


def explicit_jarvis(value: str) -> str:
    """Check an adapter's ``jarvis`` setting; empty means relay's pinned JARVIS.

    A site that needs another JARVIS names it by path: absolute, or from the
    home directory with a leading ``~/``. A bare name would be looked up on the
    site's ``PATH``, which relay never does, so it is refused. ``jarvis``, the
    old default, means relay's pinned JARVIS.
    """
    value = value.strip()
    if value in ("", "jarvis"):
        return ""
    if not value.startswith(("/", "~/")):
        raise ValueError(
            f"jarvis must be a path (absolute or ~/...), not {value!r}: relay never runs a "
            "jarvis found on the site's PATH. Leave it empty to use relay's pinned jarvis-cd"
        )
    return value


def site_environment(spec: JobSpec, home: str | None) -> dict[str, str]:
    """Return the site script's settings for ``spec`` and the CLIO site home ``home``.

    Relay's pinned JARVIS there (:func:`bootstrap_path`) is the one the script
    runs, and :data:`JARVIS_PIN` is the pin it must carry. A relative ``home``
    is from the home directory, as ssh's ``remote_dir`` is.
    The person's install answers and the pipeline's Spack environment are kept
    at ``<home>/jarvis-cd/pipelines/<steps digest>/``, keyed by the set of
    archives (:func:`steps_digest`): the job's JARVIS root and pipeline are per
    operation, so JARVIS's own per-pipeline ``decisions.yaml`` and ``spack.lock``
    would be forgotten, and a repeat run would concretize again.

    The CLIO Spack, ``<home>/spack`` with its configuration in
    ``<home>/spack-config``, is offered too. The site script exports it as
    ``JARVIS_SPACK_ROOT`` (and ``JARVIS_SPACK_CONFIG``) only when the checkout
    has ``share/spack/setup-env.sh``, so a site without one keeps JARVIS's own
    lookup (``SPACK_ROOT``).
    """
    if not home:
        return {}
    kept = posixpath.join(home, BOOTSTRAP_DIR, PIPELINES_DIR, steps_digest(spec))
    return {
        FALLBACK_ENV: bootstrap_path(home),
        PIN_ENV: JARVIS_PIN,
        DECISIONS_ENV: posixpath.join(kept, "decisions.yaml"),
        SPACK_ENV_ENV: posixpath.join(kept, "spack-env"),
        SPACK_ROOT_ENV: posixpath.join(home, SPACK_DIR),
        SPACK_CONFIG_ENV: posixpath.join(home, SPACK_CONFIG_DIR),
    }


def jarvis_root(job_dir: str) -> str:
    """Return the job's private JARVIS root."""
    return posixpath.join(job_dir, JARVIS_DIR)


def execution_dir(job_dir: str, execution_id: str, *, pipeline: str | None = None) -> str:
    """Return the execution directory JARVIS seals for an execution.

    ``pipeline`` defaults to the operation's, ``pipeline_id(execution_id)``.
    """
    return posixpath.join(
        jarvis_root(job_dir),
        "shared",
        pipeline or pipeline_id(execution_id),
        "executions",
        execution_id,
    )


def record_path(job_dir: str, execution_id: str, *, pipeline: str | None = None) -> str:
    """Return the path of the execution's ``jarvis.execution.record.v1`` file."""
    return posixpath.join(execution_dir(job_dir, execution_id, pipeline=pipeline), RECORD_FILE)


def steps_of(spec: JobSpec) -> list[PackageRef]:
    """Return the pipeline's packages, refusing a job that has none."""
    steps = spec.steps()
    if not steps:
        raise ValueError("not a package job")
    return steps


def archive_inputs(spec: JobSpec) -> list[InputRef]:
    """Return the inputs that stage each reviewed package archive by its digest, once each."""
    found: dict[str, InputRef] = {}
    for step in steps_of(spec):
        digest = step.pinned()[1]
        found.setdefault(digest, InputRef(name=f"{ARCHIVE_DIR}/{digest}.tar.gz", sha256=digest))
    return list(found.values())


def with_archive(spec: JobSpec) -> JobSpec:
    """Return ``spec`` with every package archive among its inputs, once."""
    archives = archive_inputs(spec)
    names = {item.name for item in archives}
    inputs = [item for item in spec.inputs if item.name not in names]
    return spec.model_copy(update={"inputs": [*inputs, *archives]})


def steps_digest(spec: JobSpec) -> str:
    """Return the sha256 of the sorted package archive digests: the pipeline's software."""
    digests = sorted({step.pinned()[1] for step in steps_of(spec)})
    return hashlib.sha256(",".join(digests).encode()).hexdigest()


def package_names(steps: list[PackageRef]) -> dict[str, str]:
    """Map each step's ``pkg_name`` (:func:`step_names`) to its package's name."""
    return dict(zip(step_names(steps), (step.name for step in steps), strict=True))


def step_names(steps: list[PackageRef]) -> list[str]:
    """Return each step's ``pkg_name`` in the pipeline.

    One step is ``pkg``. Several are named by their package, with ``_2``, ``_3``
    for a package that appears again.
    """
    if len(steps) == 1:
        return ["pkg"]
    names: list[str] = []
    for step in steps:
        base = step.name.partition(".")[2] or step.name
        name, count = base, 1
        while name in names:
            count += 1
            name = f"{base}_{count}"
        names.append(name)
    return names


# The pipeline


def slurm_time(duration: timedelta) -> str:
    """Render a walltime as Slurm's ``[D-]HH:MM:SS``, rounding seconds up."""
    seconds = max(1, math.ceil(duration.total_seconds()))
    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, seconds = divmod(rest, 60)
    clock = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{days}-{clock}" if days else clock


def _token(key: str, value: object) -> object:
    if isinstance(value, str) and not _TOKEN.fullmatch(value):
        raise Refusal(
            RefusalReason.INVALID_SPEC,
            f"JARVIS's Slurm scheduler takes {key} as one word without spaces or '#', "
            f"not {value!r}",
            field=key,
        )
    return value


def scheduler_block(
    spec: JobSpec,
    *,
    job_name: str,
    stdout: str | None = None,
    stderr: str | None = None,
    job_dir: str | None = None,
) -> dict[str, object]:
    """Return the pipeline's ``scheduler:`` block for ``spec`` on Slurm.

    The resources and walltime are those the spend ask priced: ``nodes()``,
    ``ranks_per_node()`` and ``duration``. A ``slurm.<key>`` custom attribute
    adds ``--<key>``. With no duration, JARVIS writes no ``--time`` and the
    scheduler's default applies. With no ``stdout`` or ``stderr``, JARVIS
    writes the logs in its execution directory and registers them as the
    ``stdout`` and ``stderr`` artifacts.

    For a package service (:func:`is_service`) ``job_dir`` is required: the
    block's ``pre_cmds`` start relay's connector, ``serve/connect.sh``, beside
    the batch shell (:func:`connector_command`).
    """
    resources = spec.resources
    attributes = spec.attributes
    block: dict[str, object] = {"name": "slurm", "job_name": job_name, "nodes": resources.nodes()}
    ranks = resources.ranks_per_node()
    if ranks is not None:
        block["ntasks_per_node"] = ranks
    cores = resources.cpu_cores_per_process
    if cores is not None and cores > 1:
        block["cpus_per_task"] = cores
    if resources.memory is not None:
        block["mem"] = f"{math.ceil(resources.memory / 2**20)}M"
    if resources.gpu_cores_per_process is not None:
        block["gpus_per_node"] = resources.gpu_cores_per_process * (ranks or 1)
    if resources.exclusive_node_use:
        block["exclusive"] = True
    if attributes.duration is not None:
        block["time"] = slurm_time(attributes.duration)
    if attributes.queue_name:
        block["partition"] = attributes.queue_name
    account = attributes.account or attributes.project_name
    if account:
        block["account"] = account
    if attributes.reservation_id:
        block["reservation"] = attributes.reservation_id
    for name, value in sorted(attributes.custom_attributes.items()):
        if not name.startswith(_SLURM_PREFIX):
            continue
        key = name[len(_SLURM_PREFIX) :].replace("-", "_")
        if key in _OWN_KEYS or not _NAME.fullmatch(key):
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                f"the custom attribute {name} cannot set the scheduler's {key}",
                field=name,
            )
        block[key] = value
    if stdout is not None:
        block["output"] = stdout
    if stderr is not None:
        block["error"] = stderr
    checked: dict[str, object] = {key: _token(key, value) for key, value in block.items()}
    if is_service(spec):
        if job_dir is None:
            raise ValueError("a package service's scheduler block needs the job directory")
        checked["pre_cmds"] = [connector_command(spec, job_dir=job_dir)]
    return checked


CONNECTOR = "serve/connect.sh"
"""Relay's connector for a package service, relative to the job directory."""


def is_service(spec: JobSpec) -> bool:
    """Return whether ``spec`` is a package job run as a relay service."""
    return any(step.service for step in spec.steps())


def slice_start(step: PackageRef) -> int:
    """Return the first allocation host of a step's ``host_slice``, as JARVIS indexes it.

    No slice is the whole allocation, so 0. A negative start counts from the
    end, as in Python. JARVIS refuses a malformed slice when it loads the
    pipeline, so one here is taken as 0.
    """
    value = step.nodes()
    match = re.fullmatch(r"\s*(-?\d*):(-?\d*)\s*", value) if value is not None else None
    if match is None or not match.group(1):
        return 0
    return int(match.group(1))


def service_step(spec: JobSpec) -> PackageRef:
    """Return the step whose first host serves the ports: the service steps' first.

    Rank 0 of that step listens on the ports, on the first host of its
    ``host_slice``, wherever the slice sits in the allocation. Service steps
    that start on different hosts are refused: relay could not tell which one
    serves the ports.
    """
    steps = [step for step in steps_of(spec) if step.service]
    if not steps:
        raise ValueError("not a package service")
    starts = sorted({slice_start(step) for step in steps})
    if len(starts) > 1:
        raise Refusal(
            RefusalReason.INVALID_SPEC,
            "the pipeline's service steps start on different nodes "
            f"({', '.join(f'{step.name} at {slice_start(step)}' for step in steps)}); "
            "set service: true on the one step that serves the ports",
            field="packages",
        )
    return steps[0]


_SLICE_HOST = (
    'h=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | awk -v s={start} '
    "'{{ h[NR - 1] = $0 }} END {{ if (s < 0) s += NR; if (s >= 0 && s < NR) print h[s] }}'); "
    'if [ -z "$h" ]; then echo "no host {start} in the allocation $SLURM_JOB_NODELIST"; '
    "exit 1; fi; "
)


def connector_command(spec: JobSpec, *, job_dir: str) -> str:
    """Return the ``pre_cmds`` line that starts the connector on the service's first host.

    The connector reads ``serve/ports`` and ``serve/secrets.env``, runs frpc
    with restarts, writes ``serve/ready.json`` once the ports listen on its
    host, writes ``serve/idle`` after the idle release and ends with the job.
    Its settings are arguments, so they do not depend on the environment JARVIS
    gives the batch shell.

    It runs where the service step's rank 0 listens: the first host of its
    ``host_slice`` (:func:`service_step`). When that is the allocation's first
    host it runs beside the batch shell. Otherwise it is a one-task step of the
    job, ``srun --overlap`` on that host, which Slurm ends with the job. Either
    way it runs on a compute node, inside the allocation JARVIS submitted.
    """
    env = spec.environment
    job = shlex.quote(job_dir)
    arguments = shlex.join(
        [
            env.get("CLIO_RELAY_OPERATION", ""),
            env.get("CLIO_RELAY_IDLE_S", "0"),
            env.get("CLIO_RELAY_FRPC", "") or "-",
        ]
    )
    log = shlex.quote(posixpath.join(job_dir, "serve", "connect.log"))
    inner = f"cd {job} && exec sh {CONNECTOR} {arguments}"
    start = slice_start(service_step(spec))
    if start == 0:
        return f"({inner}) </dev/null >>{log} 2>&1 &"
    step = f'exec srun --overlap --nodes=1 --ntasks=1 --nodelist="$h" sh -c {shlex.quote(inner)}'
    return f"({_SLICE_HOST.format(start=start)}{step}) </dev/null >>{log} 2>&1 &"


def _parameter(key: str, value: CustomValue) -> CustomValue:
    if key in _RESERVED_PARAMETERS:
        raise Refusal(
            RefusalReason.INVALID_SPEC,
            f"{key} names the package in a JARVIS pipeline, so it is not a parameter",
            field=key,
        )
    return value


def input_paths(spec: JobSpec, job_dir: str) -> dict[str, str]:
    """Return each run input's absolute path on the site, by the input's name.

    The archive is left out: it is relay's, not the package's.
    """
    return {
        item.name: posixpath.join(job_dir, item.name)
        for item in spec.inputs
        if not item.name.startswith(f"{ARCHIVE_DIR}/")
    }


def _resolved(value: CustomValue, paths: Mapping[str, str]) -> CustomValue:
    if isinstance(value, str):
        return paths.get(value, value)
    return value


def pipeline_document(
    spec: JobSpec,
    *,
    pipeline: str,
    scheduler: Mapping[str, object] | None = None,
    job_dir: str | None = None,
) -> dict[str, object]:
    """Return the JARVIS pipeline for ``spec``'s packages, with typed parameters.

    Each package is one ``pkgs:`` entry, in order (:func:`step_names`), and all
    share the one ``scheduler:`` block. ``host_slice``, ``start_mode`` and the
    package's other parameters pass through as they are. The pipeline names no
    ``env``, so JARVIS captures the environment it is loaded in, which is where
    the site script exports the job's environment.

    With ``job_dir``, a parameter whose value is a run input's name becomes that
    input's absolute path in the job directory, so the person or agent never
    spells out where relay puts the job. JARVIS runs the package in its own
    execution directory, where a relative name would not resolve.
    """
    steps = steps_of(spec)
    if not _ID.fullmatch(pipeline):
        raise Refusal(RefusalReason.INVALID_SPEC, f"{pipeline!r} is not a JARVIS pipeline id")
    if spec.directory is not None or spec.stdin_path is not None:
        raise Refusal(
            RefusalReason.INVALID_SPEC,
            "JARVIS runs a package in its own execution directory with no standard input; "
            "leave directory and stdin_path out and set the package's own parameters",
            package=steps[0].name,
        )
    paths = input_paths(spec, job_dir) if job_dir is not None else {}
    entries: list[dict[str, object]] = []
    for step, name in zip(steps, step_names(steps), strict=True):
        entry: dict[str, object] = {"pkg_type": step.name, "pkg_name": name}
        entry.update(
            {
                key: _resolved(_parameter(key, value), paths)
                for key, value in sorted(step.parameters.items())
            }
        )
        entries.append(entry)
    document: dict[str, object] = {"name": pipeline}
    if scheduler is not None:
        document["scheduler"] = dict(scheduler)
    document["pkgs"] = entries
    return document


def render_pipeline(document: Mapping[str, object]) -> str:
    """Render a pipeline document as the YAML file JARVIS loads."""
    return yaml.safe_dump(dict(document), sort_keys=False, default_flow_style=False)


# The site script

SITE_SCRIPT = r"""set -eu
mode=$1 jarvis=$2 execution=$3 steps=$4
shift 4
job=$PWD
trap 'rc=$?; printf "%s\n" "$rc" > "$job/relay.exitcode"' EXIT
fail() { echo "clio-relay jarvis: $2" >&2; exit "$1"; }
case $mode in submit|run|install) ;; *) fail 64 "mode $mode is not submit, run or install" ;; esac
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
if [ "$mode" = install ]; then
  [ $# -ge 1 ] || fail 64 "an install needs the answers"
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
spack_env=$(home "${CLIO_RELAY_JARVIS_SPACK_ENV:-}")
spack_root=$(home "${CLIO_RELAY_SPACK_ROOT:-}")
spack_config=$(home "${CLIO_RELAY_SPACK_CONFIG:-}")
unset CLIO_RELAY_JARVIS_FALLBACK CLIO_RELAY_JARVIS_PIN CLIO_RELAY_JARVIS_DECISIONS
unset CLIO_RELAY_JARVIS_SPACK_ENV CLIO_RELAY_SPACK_ROOT CLIO_RELAY_SPACK_CONFIG
# Builds run only in a job JARVIS submits (ppl install +submit), never on this host.
unset JARVIS_ALLOW_LOCAL_BUILD
# The CLIO Spack, when the site has one: JARVIS installs and loads through it.
if [ -n "$spack_root" ] && [ -f "$spack_root/share/spack/setup-env.sh" ]; then
  export JARVIS_SPACK_ROOT="$spack_root"
  if [ -n "$spack_config" ] && [ -d "$spack_config" ]; then
    export JARVIS_SPACK_CONFIG="$spack_config"
  fi
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
      "$pip" install --disable-pip-version-check --no-input -r "$requirements" \
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
report() {
  rc=0
  out=$("$found" ppl install yaml "$pipeline" +check +json) || rc=$?
  printf '%s\n' "$out"
  printf '%s\n' "$out" | tail -n 1 > "$job/relay.install-report.json.tmp"
  mv -f "$job/relay.install-report.json.tmp" "$job/relay.install-report.json"
  [ "$rc" -eq 0 ] || [ "$rc" -eq 3 ] || exit "$rc"
  exit 3
}
if [ "$mode" = run ]; then
  if [ -n "$execution" ]; then
    "$found" ppl run yaml "$pipeline" --execution-id "$execution" +json
  else
    "$found" ppl run yaml "$pipeline" +json
  fi
  exit 0
fi
rc=0
if [ "$mode" = install ]; then
  out=$("$found" ppl install yaml "$pipeline" --answers "$answers" \
    +submit --execution-id "$execution" +json) || rc=$?
  saved=relay.install.json
else
  out=$(JARVIS_INSTALL_ON_LOAD=check "$found" ppl submit "$pipeline" \
    --execution-id "$execution" +json) || rc=$?
  saved=relay.submission.json
fi
printf '%s\n' "$out"
[ "$rc" -ne 3 ] || report
if [ "$rc" -ne 0 ] && printf '%s\n' "$out" | grep -q 'spack is not available'; then
  quote() { printf '%s' "$1" | tr -d '\r\n' | sed 's/[\\"]/\\&/g'; }
  # The site's own Spacks, as its login profile sets them up: SPACK_ROOT, a spack
  # on PATH, and the Spack module trees on MODULEPATH. They are reported, not used.
  found_spacks=$(bash -lc 'printf "SPACK_ROOT %s\n" "${SPACK_ROOT:-}"
p=$(command -v spack 2>/dev/null) && case $p in /*) echo "PATH ${p%/bin/spack}" ;; esac
printf "%s\n" "${MODULEPATH:-}" | tr ":" "\n" |
  sed -n "s:^\(/.*\)/share/spack/.*:MODULEPATH \1:p"' \
    </dev/null 2>/dev/null | tr -d '\r' || true)
  site_spacks="" seen=" "
  while read -r how where; do
    [ -n "$where" ] || continue
    where=${where%/}
    case $seen in *" $where "*) continue ;; esac
    seen="$seen$where "
    [ "$where" != "${spack_root%/}" ] || continue
    [ -x "$where/bin/spack" ] && [ -f "$where/share/spack/setup-env.sh" ] || continue
    version=$(sed -n 's/^__version__ = "\([^"]*\)".*/\1/p' \
      "$where/lib/spack/spack/__init__.py" 2>/dev/null | head -n 1)
    writable=false
    [ -w "$where" ] && writable=true
    site_spacks="${site_spacks:+$site_spacks,}{\"root\":\"$(quote "$where")\""
    site_spacks="$site_spacks,\"found_by\":\"$how\",\"version\":\"$(quote "$version")\""
    site_spacks="$site_spacks,\"writable\":$writable}"
  done <<EOF
$found_spacks
EOF
  printf '{"missing":"spack","reason":"adapter_unavailable","tried":"%s","config":"%s",' \
    "$(quote "$spack_root")" "$(quote "$spack_config")" > "$job/relay.refusal.json"
  printf '"site_spacks":[%s]}\n' "$site_spacks" >> "$job/relay.refusal.json"
  cat "$job/relay.refusal.json" >&2
  exit 69
fi
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
directory. ``$CLIO_RELAY_JARVIS_DECISIONS`` and ``$CLIO_RELAY_JARVIS_SPACK_ENV``
become ``JARVIS_SPACK_DECISIONS`` and ``JARVIS_SPACK_ENV_DIR``
(:func:`site_environment`). When the CLIO Spack is on the site,
``$CLIO_RELAY_SPACK_ROOT`` becomes ``JARVIS_SPACK_ROOT`` (only when its
``share/spack/setup-env.sh`` is there) and ``$CLIO_RELAY_SPACK_CONFIG`` becomes
``JARVIS_SPACK_CONFIG`` (only when that directory is there). The script unsets
``JARVIS_ALLOW_LOCAL_BUILD``: relay never builds outside a job. It unpacks the
archives into the job's private
JARVIS root and registers each repository once, once per set of archives. Before
it registers one whose archive lists Python requirements
(:data:`~clio_relay.jarvis_site.REQUIREMENTS_FILE`), it installs them with relay's
jarvis-cd ``pip`` (only for relay's own JARVIS, logged in
``relay.requirements.log``), and fails with exit 1 when that install fails. Then it
hands the pipeline to JARVIS:

- ``submit`` runs ``ppl submit`` under ``JARVIS_INSTALL_ON_LOAD=check`` and keeps
  the handle line as ``relay.submission.json``;
- ``install`` (the answers JSON comes first) keeps the answers as
  ``relay.answers.json``, runs ``ppl install ... +submit`` and keeps the handle
  line as ``relay.install.json``. A pipeline YAML given to ``install`` is the
  install job's own (:func:`install_pipeline`), kept as
  ``relay.install-pipeline.yaml``, so the run's pipeline stays as it was;
- ``run`` runs ``ppl run`` in place.

When JARVIS exits 3 (software to install, or a question), the script writes
the ``ppl install +check`` report to ``relay.install-report.json``, prints it
and exits 3. When JARVIS stops because it finds no Spack to install with, the
script writes ``relay.refusal.json`` with ``missing: spack``, the CLIO Spack
path it offered (``tried``) and its config directory, and ``site_spacks``: the
Spacks the site's login profile sets up (``SPACK_ROOT``, a ``spack`` on PATH, and
Spack module trees on ``MODULEPATH``), each with its version and whether it is
writable. Then it exits 69. Answers already given reach every check through
``JARVIS_INSTALL_ANSWERS``. Exit 4 (JARVIS refused to build outside an
allocation) passes through unchanged; relay reports it with
:func:`build_refused` and never retries it. The exit code is also written to
``relay.exitcode``.
"""

SiteMode = Literal["submit", "run", "install"]


def steps_argument(spec: JobSpec) -> str:
    """Return the site script's ``<repo>.<package>@<sha256>,...`` for the pipeline."""
    return ",".join(f"{step.name}@{step.pinned()[1]}" for step in steps_of(spec))


def site_arguments(
    spec: JobSpec,
    *,
    mode: SiteMode,
    jarvis: str,
    execution: str = "",
    answers: Mapping[str, Any] | None = None,
    pipeline: str | None = None,
) -> list[str]:
    """Return the site script's arguments after ``$0``.

    ``answers`` is for ``install`` only. ``pipeline`` is the YAML text, for an
    adapter that cannot place ``relay.pipeline.yaml`` in the job directory
    itself.
    """
    arguments = [mode, jarvis, execution, steps_argument(spec)]
    if mode == "install":
        arguments.append(json.dumps(dict(answers or {}), sort_keys=True, separators=(",", ":")))
    return arguments if pipeline is None else [*arguments, pipeline]


def _site_command(spec: JobSpec, arguments: list[str], *, script: str, home: str | None) -> str:
    settings = {**spec.environment, **site_environment(spec, home)}
    environment = [f"{key}={value}" for key, value in settings.items()]
    prefix = ["env", *environment] if environment else []
    return shlex.join([*prefix, "bash", script, *arguments])


def submit_command(
    spec: JobSpec,
    *,
    operation_id: str,
    jarvis: str,
    script: str = SCRIPT_FILE,
    home: str | None = None,
) -> str:
    """Return the shell command that submits ``spec`` from its job directory.

    The job's environment is exported first, so JARVIS captures it in the
    pipeline it seals. ``home`` is the CLIO site home, for
    :func:`site_environment`: the JARVIS relay installed there is used when
    ``jarvis`` points to nothing, and the person's install answers are kept there.
    """
    arguments = site_arguments(spec, mode="submit", jarvis=jarvis, execution=operation_id)
    return _site_command(spec, arguments, script=script, home=home)


INSTALL_WALLTIME_S = 4 * 3600
"""The default walltime of the job JARVIS builds a package's software in."""

INSTALL_WALLTIME_HELP = (
    "The walltime, in seconds, of the one-node job JARVIS builds a package's software in. "
    "A build outlasts most runs, so it does not take the run's walltime, unless the run's "
    "is longer. Keep it within the site's partition limit"
)

INSTALL_PIPELINE_FILE = "relay.install-pipeline.yaml"
"""The pipeline the install job builds with, beside the run's :data:`PIPELINE_FILE`."""


def install_walltime(spec: JobSpec, walltime_s: int) -> str:
    """Return the install job's Slurm walltime: ``walltime_s``, or the run's when longer."""
    walltime = timedelta(seconds=walltime_s)
    duration = spec.attributes.duration
    if duration is not None and duration > walltime:
        walltime = duration
    return slurm_time(walltime)


def install_pipeline(document: Mapping[str, object], spec: JobSpec, *, walltime_s: int) -> str:
    """Render the pipeline the install job builds with, from the run's ``document``.

    JARVIS's install job keeps the pipeline's scheduler walltime, so the run's
    (often minutes) would cut a build short. This one has
    :func:`install_walltime` instead, and no ``pre_cmds``: the install job
    serves nothing, so a package service's connector does not start in it.
    """
    found = document.get("scheduler")
    if not isinstance(found, Mapping):
        return render_pipeline(document)
    scheduler = {
        key: value for key, value in cast(Mapping[str, object], found).items() if key != "pre_cmds"
    }
    scheduler["time"] = install_walltime(spec, walltime_s)
    return render_pipeline({**document, "scheduler": scheduler})


def install_command(
    spec: JobSpec,
    *,
    operation_id: str,
    jarvis: str,
    answers: Mapping[str, Any],
    pipeline: str | None = None,
    script: str = SCRIPT_FILE,
    home: str | None = None,
) -> str:
    """Return the shell command that submits the install job for ``spec``'s software.

    It runs in the job directory after a submit exited 3, with the person's
    ``answers`` (:func:`answers_document`). The install job is
    ``install_execution_id(operation_id)`` in the operation's pipeline; ``home`` is
    as in :func:`submit_command`. ``pipeline`` is the YAML of
    :func:`install_pipeline`, which the site script keeps as
    :data:`INSTALL_PIPELINE_FILE`; without it the install builds with the run's.
    """
    arguments = site_arguments(
        spec,
        mode="install",
        jarvis=jarvis,
        execution=install_execution_id(operation_id),
        answers=answers,
        pipeline=pipeline,
    )
    return _site_command(spec, arguments, script=script, home=home)


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
log="$dir/install.log"
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
"""

PYTHON_SETTING_HELP = (
    "The Python 3.11 or newer relay builds its jarvis-cd from: a path, ~/..., a name on "
    "PATH, or module:<name> for an environment module. Leave empty: relay tries python3, "
    "then python3.<minor> on PATH, then the site's python modules, newest first"
)
"""The description of an adapter's ``jarvis_python`` setting (:func:`explicit_python`)."""

_MODULE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+/-]*")


def explicit_python(value: str) -> str:
    """Check an adapter's ``jarvis_python`` setting; empty means relay finds one."""
    value = value.strip()
    if not value:
        return ""
    if value.startswith("module:"):
        if _MODULE.fullmatch(value.removeprefix("module:")) is None:
            raise ValueError(f"jarvis_python {value!r} must name a module, as module:<name>")
        return value
    if value.startswith("-") or any(ch.isspace() for ch in value):
        raise ValueError(f"jarvis_python {value!r} must be one program, or module:<name>")
    return value


def bootstrap_arguments(home: str, python: str = "") -> list[str]:
    """Return :data:`BOOTSTRAP_SCRIPT`'s arguments after ``$0`` for the site home.

    ``python`` is the site's ``jarvis_python`` setting (:func:`explicit_python`).
    """
    return [home, JARVIS_PIN, JARVIS_SOURCE, python]


def direct_job(
    spec: JobSpec,
    *,
    jarvis: str = "",
    job_dir: str | None = None,
    home: str | None = None,
    site: str = "this site",
) -> JobSpec:
    """Lower a package job to the bash job that runs it with JARVIS's direct run.

    Only for a site with no batch scheduler, where the adapter starts the job
    on the machine itself: a package never runs this way inside an allocation,
    because there ``jarvis ppl submit`` owns the job. The archive becomes an
    input, so the adapter stages it by digest; the pipeline YAML travels as an
    argument. ``job_dir`` resolves input names as in :func:`pipeline_document`,
    and ``home`` is as in :func:`submit_command`. A package service is
    refused: it needs ``jarvis execution stop``, which only a submitted job has.
    """
    steps = spec.steps()
    if not steps:
        return spec
    if is_service(spec):
        raise service_refusal(site=site)
    pipeline = render_pipeline(
        pipeline_document(spec, pipeline=f"relay_{steps_digest(spec)[:12]}", job_dir=job_dir)
    )
    staged = with_archive(spec)
    data = staged.dump()
    for key in ("package", "packages", "executable", "arguments"):
        data.pop(key, None)
    data["executable"] = "/bin/bash"
    data["arguments"] = [
        "-c",
        SITE_SCRIPT,
        "clio-relay-jarvis",
        *site_arguments(spec, mode="run", jarvis=jarvis, pipeline=pipeline),
    ]
    settings = site_environment(spec, home)
    if settings:
        data["environment"] = {**spec.environment, **settings}
    data["name"] = spec.name or steps[0].name.replace(".", "-")
    return JobSpec.model_validate(data)


# JARVIS's execution commands


def execution_argv(
    verb: ExecutionVerb,
    execution_id: str,
    *,
    job_dir: str,
    jarvis: str,
    pipeline: str | None = None,
) -> list[str]:
    """Return the argv of ``jarvis execution <verb>`` for an execution, ``+json``.

    ``pipeline`` defaults to the operation's, ``pipeline_id(execution_id)``; the
    install job (:func:`install_execution_id`) names the operation's pipeline.
    ``cancel`` and ``stop`` print the execution record as ``get`` does.
    """
    if verb not in EXECUTION_VERBS:
        raise ValueError(f"unknown execution verb {verb!r}")
    return [
        "env",
        f"JARVIS_ROOT={jarvis_root(job_dir)}",
        jarvis,
        "execution",
        verb,
        execution_id,
        "--pipeline-id",
        pipeline or pipeline_id(execution_id),
        "+json",
    ]


def execution_shell(
    verb: ExecutionVerb, execution_id: str, *, job_dir: str, pipeline: str | None = None
) -> str:
    """Return ``jarvis execution <verb>`` as shell, with JARVIS from ``relay.jarvis``.

    The command runs in the job directory, after ``jarvis=$(cat relay.jarvis)``.
    """
    argv = execution_argv(verb, execution_id, job_dir=job_dir, jarvis="JARVIS", pipeline=pipeline)
    parts = [shlex.quote(part) for part in argv]
    parts[2] = '"$jarvis"'
    return " ".join(parts)


def handle_pipeline(data: Mapping[str, Any]) -> tuple[str, str]:
    """Return ``(execution_id, pipeline_id)`` from a JARVIS job handle's data."""
    execution_id = str(data["execution_id"])
    return execution_id, str(data.get("pipeline_id") or pipeline_id(execution_id))


# Reading JARVIS's documents


def parse_document(text: str, schema: str) -> dict[str, Any] | None:
    """Return the last line of ``text`` that is a JSON document of ``schema``."""
    for line in reversed(text.splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            found: object = json.loads(line)
        except ValueError:
            continue
        if isinstance(found, dict):
            document = cast(dict[str, Any], found)
            if document.get("schema_version") == schema:
                return document
    return None


def parse_handle(text: str) -> dict[str, Any] | None:
    """Return the ``jarvis.execution.handle.v1`` that ``ppl submit +json`` printed."""
    return parse_document(text, HANDLE_SCHEMA)


def handle_data(found: Mapping[str, Any], *, job_dir: str) -> tuple[str | None, dict[str, Any]]:
    """Return the native id and the job handle's data for a job ``ppl submit`` accepted.

    The data has ``mode: jarvis``, ``scheduler``, ``job_dir``, ``execution_id``,
    ``pipeline_id`` and ``execution_dir``; the adapter adds its own keys.
    """
    execution_id = str(found["execution_id"])
    pipeline = str(found["pipeline_id"])
    native = found.get("scheduler_native_id")
    data: dict[str, Any] = {
        "mode": "jarvis",
        "scheduler": str(found.get("scheduler_provider") or "slurm"),
        "job_dir": job_dir,
        "execution_id": execution_id,
        "pipeline_id": pipeline,
        "execution_dir": execution_dir(job_dir, execution_id, pipeline=pipeline),
    }
    return (str(native) if native else None), data


_STATES = {
    "preparing": JobState.QUEUED,
    "scripted": JobState.QUEUED,
    "submitting": JobState.QUEUED,
    "submitted": JobState.QUEUED,
    "running": JobState.RUNNING,
    "completed": JobState.COMPLETED,
    "failed": JobState.FAILED,
    "canceled": JobState.CANCELLED,
}


def control_of(record: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return a record's ``cancel_request``, ``stop_request`` and ``termination``, if any."""
    metadata = _dict(record.get("metadata"))
    control = {
        key: metadata[key]
        for key in ("cancel_request", "stop_request", "termination")
        if isinstance(metadata.get(key), dict)
    }
    return control or None


def _control_message(control: Mapping[str, Any] | None, state: JobState) -> str | None:
    if not control:
        return None
    cancel = _dict(control.get("cancel_request"))
    stop = _dict(control.get("stop_request"))
    if cancel.get("status") == "failed":
        return f"jarvis execution cancel failed: {cancel.get('error') or 'no reason given'}"
    if stop.get("status") == "failed":
        return f"jarvis execution stop failed: {stop.get('error') or 'no reason given'}"
    if stop.get("status") == "fallback_cancel":
        return "the job had not started, so jarvis execution stop cancelled it"
    if state.final:
        return None
    if cancel:
        return "cancelling: jarvis execution cancel asked the scheduler to end the job"
    if stop:
        return "stopping: jarvis execution stop sent TERM, and JARVIS runs ppl stop"
    return None


WAITING_ENV_LOCK = "waiting_env_lock"
"""The install phase while another install holds the shared Spack environment's lock."""
BUILD_REFUSED_PHASE = "refused"
"""The install phase when JARVIS refused to build outside an allocation (exit 4)."""


def install_progress(record: Mapping[str, Any]) -> JobProgress | None:
    """Return an install job's ``metadata.install`` (``jarvis.install.progress.v1``) as progress."""
    metadata = _dict(record.get("metadata"))
    if metadata.get("action") != "install":
        return None
    install = _dict(metadata.get("install"))
    if not install:
        return JobProgress(message="installing: waiting for the install job to start")
    phase = str(install.get("phase") or "installing")
    current = install.get("current_package")
    if phase == WAITING_ENV_LOCK:
        message = (
            "waiting for another install into the same Spack environment; "
            "it then reuses that install"
        )
    elif phase == BUILD_REFUSED_PHASE:
        message = "refused: JARVIS would not build outside an allocation; relay does not retry"
    elif isinstance(current, str) and current:
        message = f"{phase}: {current}"
    else:
        message = phase
    data = {
        key: install[key]
        for key in ("phase", "missing", "issue_count", "error", "log", "refused")
        if install.get(key) is not None
    }
    return JobProgress(
        step=_int(install.get("packages_done")),
        total=_int(install.get("packages_total")),
        message=message,
        data=data,
    )


def record_status(record: Mapping[str, Any], *, last: JobState | None = None) -> JobStatus:
    """Map a ``jarvis.execution.record.v1`` onto relay's job states.

    ``unknown`` (JARVIS cannot see the scheduler's job right now) keeps ``last``,
    the state relay knew before. ``control`` carries the record's cancel and
    stop requests and how the job was terminated; an install job's progress
    comes from ``metadata.install``.
    """
    native = str(record.get("state") or "unknown")
    code = record.get("return_code")
    exit_code = code if isinstance(code, int) and not isinstance(code, bool) else None
    error = record.get("error")
    control = control_of(record)
    progress = install_progress(record)
    if native == "unknown" or native not in _STATES:
        return JobStatus(
            state=last if last is not None and not last.final else JobState.RUNNING,
            native_state=native,
            message="jarvis can't see the Slurm job right now",
            control=control,
            progress=progress,
        )
    state = _STATES[native]
    message = _control_message(control, state) or (
        error if isinstance(error, str) and error else None
    )
    if state == JobState.COMPLETED and exit_code is None:
        exit_code = 0
    return JobStatus(
        state=state,
        exit_code=exit_code if state.final else None,
        native_state=native,
        message=message,
        control=control,
        progress=progress,
    )


def _dict(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def _int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return int(value)


def artifact_location(item: Mapping[str, Any], execution: str | None) -> str | None:
    """Return where an artifact is on the site: execution paths resolve under ``execution``."""
    location = _dict(item.get("location"))
    value = location.get("value")
    if not isinstance(value, str) or not value:
        return None
    kind = location.get("kind")
    if kind == "execution_path":
        return posixpath.join(execution, value) if execution else None
    if kind == "external_uri":
        return None
    return value


def artifact_output(item: Mapping[str, Any], execution: str | None) -> OutputRef | None:
    """Return one ``jarvis.artifact.v1`` as an output, with its view descriptor."""
    path = artifact_location(item, execution)
    if path is None:
        return None
    metadata: dict[str, Any] = {
        key: item[key] for key in ("state", "kind", "role", "format", "media_type") if item.get(key)
    }
    view = _dict(_dict(item.get("metadata")).get("clio")).get("view")
    if isinstance(view, dict):
        metadata[VIEW_METADATA] = view
    return OutputRef(
        name=str(item.get("logical_name") or item.get("artifact_id")),
        path=path,
        size=_int(item.get("size_bytes")),
        metadata=metadata,
    )


def artifact_outputs(snapshot: Mapping[str, Any], *, execution: str) -> list[OutputRef]:
    """Return the outputs of a ``jarvis.execution.artifacts.v1`` snapshot."""
    items = snapshot.get("artifacts")
    outputs: dict[str, OutputRef] = {}
    for raw in cast(list[object], items) if isinstance(items, list) else []:
        output = artifact_output(_dict(raw), execution)
        if output is not None and output.name not in outputs:
            outputs[output.name] = output
    return sorted(outputs.values(), key=lambda output: output.name)


# Refusals


def build_refused(text: str, *, site: str) -> Refusal:
    """The refusal for a JARVIS command that exited 4: it would have built on the login host.

    ``text`` is the command's output; its install report's ``refused`` says what
    and which packages. Relay builds only through ``ppl install +submit``, in a
    job, and never opts in to a local build, so this is final, not a retry.
    """
    refused: dict[str, Any] = {}
    for line in reversed(text.splitlines()):
        try:
            found: object = json.loads(line)
        except ValueError:
            continue
        if isinstance(found, dict):
            value = cast(dict[str, Any], found).get("refused")
            if isinstance(value, dict):
                refused = cast(dict[str, Any], value)
                break
    what = refused.get("what") if isinstance(refused.get("what"), str) else "build"
    packages = [str(item) for item in cast(list[Any], refused.get("packages") or [])]
    named = f" ({', '.join(packages)})" if packages else ""
    return Refusal(
        RefusalReason.POLICY_REFUSED,
        f"JARVIS on {site} refused to {what}{named} outside an allocation: the login "
        "node takes only light work. Relay builds only in a job JARVIS submits "
        "(ppl install +submit), so it does not retry this",
        kind=refused.get("kind") or "build_outside_allocation",
        what=what,
        packages=packages,
        exit_code=EXIT_BUILD_REFUSED,
        retry=False,
    )


def missing_spack(written: dict[str, Any], *, tried: str, site: str) -> Refusal:
    """The refusal for a site with no Spack for JARVIS to install with.

    Its ``remedy`` says what a generic agent does about it: prepare a Spack
    checkout at ``root`` that reuses the site's own Spack (``site_spacks``, the
    ones the site's login profile sets up) as an upstream. Relay never installs
    into a site's Spack: it is shared, usually not writable, and JARVIS writes
    its environments and install records into the Spack it uses.
    """
    raw_spacks: object = written.get("site_spacks")
    listed = cast(list[object], raw_spacks) if isinstance(raw_spacks, list) else []
    site_spacks: list[dict[str, Any]] = [
        {
            "root": str(item.get("root")),
            "found_by": str(item.get("found_by") or ""),
            "version": str(item.get("version") or "") or None,
            "writable": bool(item.get("writable")),
            "used": False,
        }
        for item in (cast(dict[str, Any], raw) for raw in listed if isinstance(raw, dict))
        if item.get("root")
    ]
    why = (
        "JARVIS installs only through a Spack of its own, which chains to the site's Spack as "
        "an upstream; relay never builds into a site's Spack"
    )
    for item in site_spacks:
        item["why_not_used"] = why + ("" if item["writable"] else ", and this one is not writable")
    config = str(written.get("config") or "") or None
    if site_spacks:
        first = site_spacks[0]
        found = (
            f" The site has its own Spack at {first['root']}"
            f"{' ' + first['version'] if first['version'] else ''} (from {first['found_by']}); "
            "relay does not build into it, so the new checkout reuses it as an upstream."
        )
        how = (
            f"Clone Spack {first['version'] or 'at the release of the site Spack'} into {tried}, "
            f"point its upstreams.yaml at the install tree of {first['root']}, turn on "
            "concretizer reuse, and run spack compiler find and spack external find"
        )
    else:
        found = " The site's login profile sets up no Spack of its own."
        how = (
            f"Clone a Spack release into {tried}, and run spack compiler find and spack "
            "external find"
        )
    remedy: dict[str, Any] = {
        "action": "prepare_spack",
        "root": tried,
        "setup_env": f"{tried.rstrip('/')}/share/spack/setup-env.sh",
        "config": config,
        "site_spacks": site_spacks,
        "steps": how,
        "where": (
            "the site's login node, through exec: a clone and configuration only, which is "
            "light. Install nothing there; JARVIS builds what is missing in its install job"
        ),
        "then": "call run again with the same spec",
    }
    return Refusal(
        RefusalReason.ADAPTER_UNAVAILABLE,
        f"{site} has no Spack for JARVIS to install the package with: relay looked for a "
        f"Spack checkout at {tried} (its share/spack/setup-env.sh) and JARVIS found no spack "
        f"on the site's PATH.{found} Prepare a Spack checkout at {tried} (remedy: "
        f"{remedy['action']}): {how}. That is light login-node work; then run again",
        missing="spack",
        tried=tried,
        remedy=remedy,
    )


def missing_jarvis(text: str, *, site: str) -> Refusal:
    """The refusal for a submit that exited 69: ``relay.refusal.json`` or stderr.

    Relay offers to install its pinned jarvis-cd when its own is missing or at
    another pin; a path the site's setting names is the site's to fix. A site
    with no Spack for JARVIS to install with (``missing: spack``) is the site's
    to fix too: relay offers no install, and its refusal carries the remedy
    (:func:`missing_spack`).
    """
    written: dict[str, Any] = {}
    for line in reversed(text.splitlines()):
        try:
            found: object = json.loads(line)
        except ValueError:
            continue
        if isinstance(found, dict) and isinstance(cast(dict[str, Any], found).get("tried"), str):
            written = cast(dict[str, Any], found)
            break
    tried = str(written.get("tried") or "the site home")
    if written.get("missing") == "spack":
        return missing_spack(written, tried=tried, site=site)
    if written.get("explicit"):
        return Refusal(
            RefusalReason.ADAPTER_UNAVAILABLE,
            f"the jarvis setting of {site} names {tried}, which is not a program there. Fix "
            f"the path, or clear the setting to use relay's pinned jarvis-cd {JARVIS_PIN[:8]}",
            missing="jarvis",
            tried=tried,
            explicit=True,
        )
    pin = written.get("pin")
    if isinstance(pin, str):
        now = f"at {pin[:8]}" if pin else "unpinned"
        message = (
            f"relay's jarvis-cd on {site} ({tried}) is {now}, not {JARVIS_PIN[:8]}. Relay "
            "can install the pinned one in the site's CLIO home once you agree"
        )
        return Refusal(
            RefusalReason.ADAPTER_UNAVAILABLE,
            message,
            missing="jarvis",
            tried=tried,
            pin=pin,
            installable=JARVIS_PIN,
        )
    return Refusal(
        RefusalReason.ADAPTER_UNAVAILABLE,
        f"{site} has no jarvis-cd of relay's at {tried}, so relay cannot hand the package to "
        f"JARVIS. Relay can install jarvis-cd {JARVIS_PIN[:8]} in the site's CLIO home once "
        "you agree, or the site's jarvis setting can name another by path",
        missing="jarvis",
        tried=tried,
        installable=JARVIS_PIN,
    )


def bootstrap_command(home: str, python: str = "") -> str:
    """Return the shell command that runs :data:`BOOTSTRAP_SCRIPT` for the site home.

    ``python`` is the site's ``jarvis_python`` setting, as in :func:`bootstrap_arguments`.
    """
    return shlex.join(
        ["bash", "-c", BOOTSTRAP_SCRIPT, "clio-relay-jarvis", *bootstrap_arguments(home, python)]
    )


def bootstrap_result(exit_code: int | None, output: str, *, site: str) -> str:
    """Return the ``jarvis`` path :data:`BOOTSTRAP_SCRIPT` linked, or raise its refusal."""
    if exit_code == 0:
        for line in reversed(output.splitlines()):
            try:
                found: object = json.loads(line)
            except ValueError:
                continue
            if isinstance(found, dict):
                path = cast(dict[str, Any], found).get("jarvis")
                if isinstance(path, str) and path:
                    return path
    tail = "\n".join(output.strip().splitlines()[-10:]) or "no output"
    reason = (
        RefusalReason.ADAPTER_UNAVAILABLE
        if exit_code == EXIT_NO_JARVIS
        else RefusalReason.SUBMIT_FAILED
    )
    raise Refusal(
        reason,
        f"relay could not install jarvis-cd {JARVIS_PIN[:8]} on {site}: {tail}",
        missing="jarvis",
        exit_code=exit_code,
    )


def scheduler_refusal(scheduler: str, *, site: str) -> Refusal:
    """A package job on a site whose batch scheduler JARVIS cannot submit to."""
    return Refusal(
        RefusalReason.ADAPTER_UNAVAILABLE,
        f"a package job on {site} goes through jarvis ppl submit, and JARVIS submits to "
        f"Slurm only, not {scheduler}",
        scheduler=scheduler,
        needs=f"a JARVIS {scheduler} scheduler",
    )


def not_run_here(adapter: str) -> Refusal:
    """A package job sent to an adapter that cannot hand it to ``jarvis ppl submit``."""
    return Refusal(
        RefusalReason.ADAPTER_UNAVAILABLE,
        f"the {adapter} adapter does not run JARVIS packages: a package job goes through "
        "jarvis ppl submit, and relay does not run a package inside an allocation itself",
        adapter=adapter,
        needs=f"jarvis ppl submit through the {adapter} adapter",
    )


def service_refusal(*, site: str) -> Refusal:
    """A package service on a site with no batch scheduler for ``jarvis ppl submit``."""
    return Refusal(
        RefusalReason.ADAPTER_UNAVAILABLE,
        f"a package service on {site} needs jarvis ppl submit, so jarvis execution stop "
        "can end it, and the site has no batch scheduler. Give the site a Slurm scheduler",
        needs="jarvis ppl submit",
    )


def execution_failed(verb: str, text: str) -> Refusal:
    """``jarvis execution cancel|stop`` exited non-zero; ``text`` is its output."""
    tail = "\n".join(text.strip().splitlines()[-5:]) or "no output"
    return Refusal(
        RefusalReason.NOT_CANCELLABLE,
        f"jarvis execution {verb} did not end the job: {tail}",
        needs=f"jarvis execution {verb}",
    )


# Installing the package's software


def parse_install_report(text: str) -> dict[str, Any] | None:
    """Return the last ``jarvis.install-report.v1`` line in ``text``."""
    return parse_document(text, INSTALL_REPORT_SCHEMA)


def report_digest(report: Mapping[str, Any]) -> str:
    """Return the sha256 of an install report's installers and issues.

    Paths and the mode are left out, so the digest binds what the person approves.
    """
    bound = {"installers": report.get("installers"), "issues": report.get("issues")}
    text = json.dumps(bound, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(text.encode()).hexdigest()


def _issues(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    found = report.get("issues")
    if not isinstance(found, list):
        return []
    return [_dict(item) for item in cast(list[object], found) if _dict(item)]


def choice_issues(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return the issues the person answers by picking one of JARVIS's candidates."""
    return [
        issue
        for issue in _issues(report)
        if issue.get("answerable")
        and isinstance(issue.get("candidates"), list)
        and issue["candidates"]
    ]


def blocking_issues(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return the issues no answer fixes: no recipe, or answerable with no candidates.

    ``not_installed`` is not blocking: the install itself fixes it.
    """
    blocking: list[dict[str, Any]] = []
    for issue in _issues(report):
        if issue.get("kind") == "not_installed":
            continue
        if issue in choice_issues(report):
            continue
        blocking.append(issue)
    return blocking


def install_summary(
    report: Mapping[str, Any], names: Mapping[str, str] | None = None
) -> dict[str, Any]:
    """Return what the install reuses and what it builds, per package.

    ``reuse`` names the packages the site already has; ``build`` lists the others,
    with the spec and how many of its dependencies are missing. JARVIS reports a
    package by its pipeline step (``pkg_name``); ``names`` (:func:`package_names`)
    turns that back into the package.
    """
    names = names or {}
    reuse: list[str] = []
    build: list[dict[str, Any]] = []
    for method, installer in _dict(report.get("installers")).items():
        for step, package in _dict(_dict(installer).get("packages")).items():
            name = names.get(step, step)
            entry = _dict(package)
            if entry.get("status") == "satisfied":
                reuse.append(name)
                continue
            missing = entry.get("missing")
            count = len(cast(list[object], missing)) if isinstance(missing, list) else None
            build.append(
                {
                    "package": name,
                    "method": method,
                    "status": entry.get("status"),
                    "spec": entry.get("spec"),
                    "missing": count,
                }
            )
    return {"reuse": sorted(reuse), "build": build, "issues": len(_issues(report))}


def install_message(
    report: Mapping[str, Any],
    *,
    site: str,
    package: str,
    names: Mapping[str, str] | None = None,
    walltime: str | None = None,
) -> str:
    """Return the approval question for installing the package's software on the site.

    ``walltime`` is the install job's, when the adapter reported it.
    """
    summary = install_summary(report, names)
    built: list[str] = []
    for item in summary["build"]:
        detail = item["spec"] or item["package"]
        if item["missing"]:
            detail = f"{detail} ({item['missing']} missing)"
        built.append(detail)
    if not built and choice_issues(report):
        return (
            f"JARVIS needs {len(choice_issues(report))} choice(s) from you before {package} "
            f"runs on {site}; nothing has to be built. It applies your answers in a short "
            "job on the site, then the run goes ahead. Go ahead?"
        )
    parts = [f"{package} needs software that {site} does not have yet."]
    if summary["reuse"]:
        parts.append("Reused: " + ", ".join(summary["reuse"]) + ".")
    if built:
        parts.append("Built: " + ", ".join(built) + ".")
    if summary["issues"]:
        parts.append(f"JARVIS has {summary['issues']} question(s) for you after this.")
    job = f"a one-node job of up to {walltime}" if walltime else "a job"
    parts.append(f"JARVIS installs it as {job} on the site before the run. Install it?")
    return " ".join(parts)


def choice_options(issue: Mapping[str, Any]) -> list[str]:
    """Return an issue's candidate constraints, in JARVIS's order."""
    options: list[str] = []
    for candidate in cast(list[object], issue.get("candidates") or []):
        constraint = _dict(candidate).get("constraint")
        if isinstance(constraint, str) and constraint and constraint not in options:
            options.append(constraint)
    return options


def _candidate_line(candidate: Mapping[str, Any]) -> str | None:
    constraint = candidate.get("constraint")
    if not isinstance(constraint, str) or not constraint:
        return None
    facts: list[str] = []
    source = candidate.get("source")
    if isinstance(source, str) and source:
        facts.append(source)
    modules = candidate.get("modules")
    if isinstance(modules, list) and modules:
        facts.append("modules " + ", ".join(str(item) for item in cast(list[object], modules)))
    prefix = candidate.get("prefix")
    if isinstance(prefix, str) and prefix:
        facts.append(prefix)
    implies = candidate.get("implies")
    if isinstance(implies, str) and implies:
        facts.append(implies)
    return f"{constraint} ({'; '.join(facts)})" if facts else constraint


def choice_message(issue: Mapping[str, Any]) -> str:
    """Return the question to ask for one of JARVIS's answerable issues.

    Each candidate is listed with where it comes from, its prefix and its modules
    (an ``ambiguous_provider`` MPI that is a site module carries ``modules``).
    """
    question = issue.get("question")
    subject = issue.get("package") or issue.get("subject") or "the package"
    text = (
        question
        if isinstance(question, str) and question
        else f"JARVIS needs a choice for {subject}: pick one"
    )
    lines = [
        line
        for candidate in cast(list[object], issue.get("candidates") or [])
        if (line := _candidate_line(_dict(candidate)))
    ]
    if issue.get("kind") == "ambiguous_provider" and issue.get("scope") == "site":
        text = f"{text} JARVIS keeps the answer for every later run on the site."
    return f"{text} Options: {'. '.join(lines)}." if lines else text


def install_needed(report: Mapping[str, Any], *, site: str, walltime: str | None = None) -> Refusal:
    """The refusal for a submit that exited 3: the software is missing or needs a choice.

    ``walltime`` is the install job's (:func:`install_walltime`), for the ask.
    """
    summary = install_summary(report)
    message = (
        f"the package's software is not installed on {site}: JARVIS builds "
        f"{len(summary['build'])} package(s) once you approve"
    )
    if not summary["build"] and choice_issues(report):
        kinds = sorted({str(issue.get("kind")) for issue in choice_issues(report)})
        message = f"JARVIS needs a choice on {site} before the run ({', '.join(kinds)})"
    extra: dict[str, Any] = {"install_walltime": walltime} if walltime else {}
    return Refusal(
        RefusalReason.NEEDS_APPROVAL,
        message,
        needs=NEEDS_INSTALL,
        install_report=dict(report),
        summary=summary,
        **extra,
    )


def install_blocked(issues: list[dict[str, Any]], *, site: str) -> Refusal:
    """The refusal when JARVIS reports an install issue no answer can fix."""
    first = issues[0]
    detail = first.get("detail") or first.get("question") or first.get("kind")
    hint = first.get("hint")
    message = f"JARVIS cannot install the package's software on {site}: {detail}"
    if isinstance(hint, str) and hint:
        message = f"{message}. {hint}"
    return Refusal(RefusalReason.SUBMIT_FAILED, message, issues=issues, needs=NEEDS_INSTALL)


def answers_document(choices: Mapping[str, tuple[str, str]]) -> dict[str, Any]:
    """Return JARVIS's answers file from ``{issue_id: (constraint, ask_id)}``."""
    return {
        issue_id: {"constraint": constraint, "answered_by": "person", "ask_id": ask_id}
        for issue_id, (constraint, ask_id) in choices.items()
    }
