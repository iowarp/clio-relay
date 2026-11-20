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

Either way the job's JARVIS root is ``<job dir>/.relay-jarvis``, private to the
job, so the person's own pipelines are never read or changed. Status, outputs
and services come from JARVIS's ``execution`` commands and its record, never
from relay's own tracking of the batch job.

JARVIS 1.8.1 has no ``execution cancel`` or ``execution stop``, so relay
refuses both after a submit (:func:`cancel_refusal`, :func:`stop_refusal`)
rather than calling the scheduler itself.
"""

from __future__ import annotations

import json
import math
import posixpath
import re
import shlex
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, Literal, cast

import yaml

from clio_relay.jobs.handles import VIEW_METADATA, JobState, JobStatus, OutputRef
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import CustomValue, InputRef, JobSpec

ARCHIVE_DIR = ".relay-package"
"""Where the site script finds the package archive, relative to the job directory."""

JARVIS_DIR = ".relay-jarvis"
"""The job's private JARVIS root, relative to the job directory."""

PIPELINE_FILE = "relay.pipeline.yaml"
SCRIPT_FILE = "relay-jarvis.sh"
SUBMISSION_FILE = "relay.submission.json"
"""The ``jarvis.execution.handle.v1`` line ``ppl submit`` printed."""
REFUSAL_FILE = "relay.refusal.json"
"""Written when the site has no JARVIS: ``{"reason", "missing", "tried"}``."""
EXITCODE_FILE = "relay.exitcode"
JARVIS_FILE = "relay.jarvis"
"""The JARVIS program the site script found, for the ``execution`` commands later."""

SITE_FILES = (PIPELINE_FILE, SCRIPT_FILE, SUBMISSION_FILE, REFUSAL_FILE, EXITCODE_FILE, JARVIS_FILE)
"""Files the site script keeps in the job directory. They are not outputs."""

HANDLE_SCHEMA = "jarvis.execution.handle.v1"
RECORD_SCHEMA = "jarvis.execution.record.v1"
ARTIFACTS_SCHEMA = "jarvis.execution.artifacts.v1"
SERVICE_RUNTIMES_SCHEMA = "jarvis.execution.service-runtimes.v1"
RECORD_FILE = ".jarvis-execution.json"

EXIT_USAGE = 64
"""A bad mode, package name or execution id."""
EXIT_DIGEST = 65
"""The archive's SHA-256 is not the reviewed one."""
EXIT_MISSING = 66
"""The archive, the pipeline or the archive's repository directory is missing."""
EXIT_NO_JARVIS = 69
"""The site has no JARVIS where the site's setting says."""

ExecutionVerb = Literal["get", "progress", "artifacts", "service-runtimes"]
EXECUTION_VERBS: tuple[ExecutionVerb, ...] = ("get", "progress", "artifacts", "service-runtimes")

NEEDS_CANCEL = "jarvis execution cancel"
NEEDS_STOP = "jarvis execution stop"

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


def jarvis_root(job_dir: str) -> str:
    """Return the job's private JARVIS root."""
    return posixpath.join(job_dir, JARVIS_DIR)


def execution_dir(job_dir: str, operation_id: str) -> str:
    """Return the execution directory JARVIS seals for the operation."""
    return posixpath.join(
        jarvis_root(job_dir), "shared", pipeline_id(operation_id), "executions", operation_id
    )


def record_path(job_dir: str, operation_id: str) -> str:
    """Return the path of the execution's ``jarvis.execution.record.v1`` file."""
    return posixpath.join(execution_dir(job_dir, operation_id), RECORD_FILE)


def archive_input(spec: JobSpec) -> InputRef:
    """Return the input that stages the reviewed package archive by its digest."""
    package = spec.package
    if package is None:
        raise ValueError("not a package job")
    digest = package.pinned()[1]
    return InputRef(name=f"{ARCHIVE_DIR}/{digest}.tar.gz", sha256=digest)


def with_archive(spec: JobSpec) -> JobSpec:
    """Return ``spec`` with the package archive among its inputs, once."""
    archive = archive_input(spec)
    inputs = [item for item in spec.inputs if item.name != archive.name]
    return spec.model_copy(update={"inputs": [*inputs, archive]})


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
    spec: JobSpec, *, job_name: str, stdout: str | None = None, stderr: str | None = None
) -> dict[str, object]:
    """Return the pipeline's ``scheduler:`` block for ``spec`` on Slurm.

    The resources and walltime are those the spend ask priced: ``nodes()``,
    ``ranks_per_node()`` and ``duration``. A ``slurm.<key>`` custom attribute
    adds ``--<key>``. With no duration, JARVIS writes no ``--time`` and the
    scheduler's default applies. With no ``stdout`` or ``stderr``, JARVIS
    writes the logs in its execution directory and registers them as the
    ``stdout`` and ``stderr`` artifacts.
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
    return {key: _token(key, value) for key, value in block.items()}


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
    """Return the JARVIS pipeline for ``spec``'s package, with typed parameters.

    The pipeline names no ``env``, so JARVIS captures the environment it is
    loaded in, which is where the site script exports the job's environment.

    With ``job_dir``, a parameter whose value is a run input's name becomes that
    input's absolute path in the job directory, so the person or agent never
    spells out where relay puts the job. JARVIS runs the package in its own
    execution directory, where a relative name would not resolve.
    """
    package = spec.package
    if package is None:
        raise ValueError("not a package job")
    if not _ID.fullmatch(pipeline):
        raise Refusal(RefusalReason.INVALID_SPEC, f"{pipeline!r} is not a JARVIS pipeline id")
    if spec.directory is not None or spec.stdin_path is not None:
        raise Refusal(
            RefusalReason.INVALID_SPEC,
            "JARVIS runs a package in its own execution directory with no standard input; "
            "leave directory and stdin_path out and set the package's own parameters",
            package=package.name,
        )
    paths = input_paths(spec, job_dir) if job_dir is not None else {}
    entry: dict[str, object] = {"pkg_type": package.name, "pkg_name": "pkg"}
    entry.update(
        {
            key: _resolved(_parameter(key, value), paths)
            for key, value in sorted(package.parameters.items())
        }
    )
    document: dict[str, object] = {"name": pipeline}
    if scheduler is not None:
        document["scheduler"] = dict(scheduler)
    document["pkgs"] = [entry]
    return document


def render_pipeline(document: Mapping[str, object]) -> str:
    """Render a pipeline document as the YAML file JARVIS loads."""
    return yaml.safe_dump(dict(document), sort_keys=False, default_flow_style=False)


# The site script

SITE_SCRIPT = r"""set -eu
mode=$1 digest=$2 name=$3 jarvis=$4 execution=$5
shift 5
job=$PWD
trap 'rc=$?; printf "%s\n" "$rc" > "$job/relay.exitcode"' EXIT
fail() { echo "clio-relay jarvis: $2" >&2; exit "$1"; }
case $mode in submit|run) ;; *) fail 64 "mode $mode is not submit or run" ;; esac
printf '%s\n' "$name" | grep -Eq '^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$' \
  || fail 64 "package name $name is not <repo>.<package>"
if [ -n "$execution" ]; then
  printf '%s\n' "$execution" | grep -Eq '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$' \
    || fail 64 "execution id $execution is not a JARVIS id"
elif [ "$mode" = submit ]; then
  fail 64 "a submit needs an execution id"
fi
pipeline="$job/relay.pipeline.yaml"
[ $# -eq 0 ] || printf '%s' "$1" > "$pipeline"
[ -f "$pipeline" ] || fail 66 "the pipeline $pipeline is missing"
archive="$job/.relay-package/$digest.tar.gz"
[ -f "$archive" ] || fail 66 "the package archive $archive is missing"
if command -v sha256sum >/dev/null 2>&1; then got=$(sha256sum "$archive" | cut -d' ' -f1)
else got=$(shasum -a 256 "$archive" | cut -d' ' -f1); fi
[ "$got" = "$digest" ] || fail 65 "the archive's SHA-256 is $got, not the reviewed $digest"
case $jarvis in "~/"*) jarvis="$HOME/${jarvis#??}" ;; esac
found=$(command -v "$jarvis" 2>/dev/null) || found=
if [ -z "$found" ]; then
  tried=$(printf '%s' "$jarvis" | sed 's/[\\"]/\\&/g')
  printf '{"missing":"jarvis","reason":"adapter_unavailable","tried":"%s"}\n' "$tried" \
    > "$job/relay.refusal.json"
  cat "$job/relay.refusal.json" >&2
  fail 69 "$jarvis is not on PATH on this site"
fi
printf '%s\n' "$found" > "$job/relay.jarvis"
root="$job/.relay-jarvis"
repo=${name%%.*}
rm -rf "$root"
mkdir -p "$root/src"
tar -xzf "$archive" -C "$root/src"
[ -d "$root/src/$repo/$repo" ] || fail 66 "the archive has no $repo/$repo directory"
export JARVIS_ROOT="$root"
"$found" init "$root/config" "$root/private" "$root/shared" >/dev/null
"$found" repo add "$root/src/$repo" >/dev/null
if [ "$mode" = run ]; then
  if [ -n "$execution" ]; then
    "$found" ppl run yaml "$pipeline" --execution-id "$execution" +json
  else
    "$found" ppl run yaml "$pipeline" +json
  fi
  exit 0
fi
rc=0
out=$("$found" ppl submit "$pipeline" --execution-id "$execution" +json) || rc=$?
printf '%s\n' "$out"
[ "$rc" -eq 0 ] || exit "$rc"
printf '%s\n' "$out" | tail -n 1 > "$job/relay.submission.json.tmp"
mv -f "$job/relay.submission.json.tmp" "$job/relay.submission.json"
"""
"""The site script. ``bash -c SITE_SCRIPT <$0> <mode> <digest> <name> <jarvis> <execution>
[<pipeline YAML>]``, run in the job directory.

It checks the archive's SHA-256 against the reviewed one, finds JARVIS (writing
``relay.refusal.json`` and exiting 69 when it is missing), unpacks the archive
into the job's private JARVIS root, registers its repository and hands the
pipeline to JARVIS. ``submit`` keeps the handle line as ``relay.submission.json``.
The exit code is also written to ``relay.exitcode``.
"""


def site_arguments(
    spec: JobSpec,
    *,
    mode: Literal["submit", "run"],
    jarvis: str,
    execution: str = "",
    pipeline: str | None = None,
) -> list[str]:
    """Return the site script's arguments after ``$0``.

    ``pipeline`` is the YAML text, for an adapter that cannot place
    ``relay.pipeline.yaml`` in the job directory itself.
    """
    package = spec.package
    if package is None:
        raise ValueError("not a package job")
    arguments = [mode, package.pinned()[1], package.name, jarvis, execution]
    return arguments if pipeline is None else [*arguments, pipeline]


def submit_command(
    spec: JobSpec, *, operation_id: str, jarvis: str, script: str = SCRIPT_FILE
) -> str:
    """Return the shell command that submits ``spec`` from its job directory.

    The job's environment is exported first, so JARVIS captures it in the
    pipeline it seals.
    """
    arguments = site_arguments(spec, mode="submit", jarvis=jarvis, execution=operation_id)
    environment = [f"{key}={value}" for key, value in spec.environment.items()]
    prefix = ["env", *environment] if environment else []
    return shlex.join([*prefix, "bash", script, *arguments])


def direct_job(spec: JobSpec, *, jarvis: str = "jarvis", job_dir: str | None = None) -> JobSpec:
    """Lower a package job to the bash job that runs it with JARVIS's direct run.

    Only for a site with no batch scheduler, where the adapter starts the job
    on the machine itself: a package never runs this way inside an allocation,
    because there ``jarvis ppl submit`` owns the job. The archive becomes an
    input, so the adapter stages it by digest; the pipeline YAML travels as an
    argument. ``job_dir`` resolves input names as in :func:`pipeline_document`.
    """
    package = spec.package
    if package is None:
        return spec
    digest = package.pinned()[1]
    pipeline = render_pipeline(
        pipeline_document(spec, pipeline=f"relay_{digest[:12]}", job_dir=job_dir)
    )
    staged = with_archive(spec)
    data = staged.model_dump(mode="json", exclude={"package", "executable", "arguments"})
    data["executable"] = "/bin/bash"
    data["arguments"] = [
        "-c",
        SITE_SCRIPT,
        "clio-relay-jarvis",
        *site_arguments(spec, mode="run", jarvis=jarvis, pipeline=pipeline),
    ]
    data["name"] = spec.name or package.name.replace(".", "-")
    return JobSpec.model_validate(data)


# JARVIS's execution commands


def execution_argv(
    verb: ExecutionVerb, operation_id: str, *, job_dir: str, jarvis: str
) -> list[str]:
    """Return the argv of ``jarvis execution <verb>`` for the operation, ``+json``."""
    if verb not in EXECUTION_VERBS:
        raise ValueError(f"unknown execution verb {verb!r}")
    return [
        "env",
        f"JARVIS_ROOT={jarvis_root(job_dir)}",
        jarvis,
        "execution",
        verb,
        operation_id,
        "--pipeline-id",
        pipeline_id(operation_id),
        "+json",
    ]


def execution_shell(verb: ExecutionVerb, operation_id: str, *, job_dir: str) -> str:
    """Return ``jarvis execution <verb>`` as shell, with JARVIS from ``relay.jarvis``.

    The command runs in the job directory, after ``jarvis=$(cat relay.jarvis)``.
    """
    argv = execution_argv(verb, operation_id, job_dir=job_dir, jarvis="JARVIS")
    parts = [shlex.quote(part) for part in argv]
    parts[2] = '"$jarvis"'
    return " ".join(parts)


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
    native = found.get("scheduler_native_id")
    data: dict[str, Any] = {
        "mode": "jarvis",
        "scheduler": str(found.get("scheduler_provider") or "slurm"),
        "job_dir": job_dir,
        "execution_id": execution_id,
        "pipeline_id": str(found["pipeline_id"]),
        "execution_dir": execution_dir(job_dir, execution_id),
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


def record_status(record: Mapping[str, Any], *, last: JobState | None = None) -> JobStatus:
    """Map a ``jarvis.execution.record.v1`` onto relay's job states.

    ``unknown`` (JARVIS cannot see the scheduler's job right now) keeps ``last``,
    the state relay knew before.
    """
    native = str(record.get("state") or "unknown")
    code = record.get("return_code")
    exit_code = code if isinstance(code, int) and not isinstance(code, bool) else None
    error = record.get("error")
    message = error if isinstance(error, str) and error else None
    if native == "unknown" or native not in _STATES:
        return JobStatus(
            state=last if last is not None and not last.final else JobState.RUNNING,
            native_state=native,
            message="jarvis can't see the Slurm job right now",
        )
    state = _STATES[native]
    if state == JobState.COMPLETED and exit_code is None:
        exit_code = 0
    if state.final:
        return JobStatus(state=state, exit_code=exit_code, native_state=native, message=message)
    return JobStatus(state=state, native_state=native, message=message)


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


def missing_jarvis(text: str, *, site: str) -> Refusal:
    """The refusal for a submit that exited 69: ``relay.refusal.json`` or stderr."""
    tried = "jarvis"
    for line in reversed(text.splitlines()):
        try:
            found: object = json.loads(line)
        except ValueError:
            continue
        if isinstance(found, dict) and isinstance(cast(dict[str, Any], found).get("tried"), str):
            tried = cast(str, cast(dict[str, Any], found)["tried"])
            break
    return Refusal(
        RefusalReason.ADAPTER_UNAVAILABLE,
        f"{site} has no JARVIS at {tried}, so relay cannot hand the package to it. Install "
        "jarvis-cd there, or set the site's jarvis setting to its path",
        missing="jarvis",
        tried=tried,
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


def cancel_refusal() -> Refusal:
    """Cancelling a submitted package job needs a JARVIS command 1.8.1 lacks."""
    return Refusal(
        RefusalReason.NOT_CANCELLABLE,
        "JARVIS owns this job, and cancelling it needs jarvis execution cancel, which this "
        "JARVIS does not have. Relay does not call the scheduler behind JARVIS's back",
        needs=NEEDS_CANCEL,
    )


def stop_refusal() -> Refusal:
    """Stopping a submitted package service needs a JARVIS command 1.8.1 lacks."""
    return Refusal(
        RefusalReason.ADAPTER_UNAVAILABLE,
        "a package service on this site is started by jarvis ppl submit, and stopping it "
        "needs jarvis execution stop, which this JARVIS does not have, nor a place for "
        "relay's connector in the job JARVIS writes",
        needs=NEEDS_STOP,
    )
