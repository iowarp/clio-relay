"""The POSIX ``sh`` scripts the ``ssh`` adapter runs on a site.

Each verb is one ``ssh`` call that runs one of these scripts with ``sh -s``.
They print sections that start with an ``@@name`` line, so one round trip
returns everything a verb needs. They use only POSIX ``sh`` and the tools every
Linux and macOS host has.

The site keeps relay's directory ``R`` (``remote_dir``, under the home
directory unless absolute)::

    R/content/cas/sha256/<2 hex>/<digest>   inputs, by content (RL16)
    R/psij/<hash>/                          PSI/J's launcher scripts
    R/incoming/<operation>/                 uploads on their way in
    R/jobs/<operation>/                     the job record (RL17)

The job directory holds ``spec.json``, ``inputs.json``, ``scheduler``,
``submit.sh`` or ``supervise.sh`` and ``child.sh``, ``submit.out``,
``submit.err``, ``submit.rc``, ``states`` (one ``<time> <state> <detail>`` line
per change), ``cancel`` once cancelled, and ``final`` once the job ended.

A package job on Slurm is JARVIS's instead (``scheduler`` says ``jarvis``): the
directory holds ``relay.pipeline.yaml`` and ``relay-jarvis.sh`` from
:mod:`clio_relay.jarvis_submit`, the handle ``ppl submit`` printed, and the
job's JARVIS root ``.relay-jarvis``, whose execution record is the job record.
"""

from __future__ import annotations

import posixpath
import shlex
from collections.abc import Iterable, Sequence
from pathlib import PurePosixPath

from clio_relay import jarvis_graph, jarvis_submit
from clio_relay.adapters.ssh import store
from clio_relay.login_load import BATCH_HINT_SH

SCHEDULER_COMMANDS: tuple[tuple[str, str], ...] = (
    ("slurm", "sbatch"),
    ("pbs", "qsub"),
    ("lsf", "bsub"),
    ("cobalt", "cqsub"),
)

# Files relay keeps in a job directory. They are not outputs.
INTERNAL_FILES = frozenset(
    {
        "spec.json",
        "inputs.json",
        "scheduler",
        "submit.sh",
        "submit.out",
        "submit.err",
        "submit.rc",
        "supervise.sh",
        "supervisor.log",
        "child.sh",
        "states",
        "cancel",
        "final",
        "exit_code",
        "pid",
        "child_pid",
        "signalled",
        "inputs.dropped",
        *jarvis_submit.SITE_FILES,
    }
)

q = shlex.quote


def root_expression(remote_dir: str) -> str:
    """Return a shell expression for relay's directory on the site."""
    path = remote_dir.rstrip("/") or "/"
    if path.startswith("/"):
        return q(path)
    if path.startswith("~/"):
        path = path[2:]
    return f'"$HOME"/{q(path)}'


def parse_sections(text: str) -> dict[str, list[str]]:
    """Split ``@@name value`` sections. The value and the lines after it are the body."""
    sections: dict[str, list[str]] = {}
    current: list[str] | None = None
    for line in text.splitlines():
        if line.startswith("@@"):
            key, _, rest = line[2:].partition(" ")
            current = sections.setdefault(key, [])
            if rest:
                current.append(rest)
            continue
        if current is not None:
            current.append(line)
    return sections


# On NFS a file another node wrote can stay invisible to ``[ -f ]``, ``cat`` and
# ``stat`` for the attribute cache's lifetime (two minutes on some sites). Reading
# the directory makes the client look again, so every check of a file another
# host may have written lists its directory first.
FRESH = 'fresh() { for d in "$@"; do ls -f "$d" >/dev/null 2>&1; done; }\n'

_PRELUDE = (
    FRESH
    + """\
ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
sha() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  elif command -v shasum >/dev/null 2>&1; then shasum -a 256 "$1" | cut -d' ' -f1
  fi
}
cas() { printf '%s/content/cas/sha256/%s/%s' "$R" "$(printf %s "$1" | cut -c1-2)" "$1"; }
"""
)

# Enter the job directory, looking again at its parent when it seems missing.
_ENTER_JOB = (
    FRESH
    + 'cd "$J" 2>/dev/null || { fresh "$(dirname "$J")"; cd "$J" 2>/dev/null; } '
    + '|| { echo "@@nodir"; exit 0; }\nfresh .\n'
)


# The job and upload directories, and their parents when the job seems missing.
_SEE_JOB = '[ -d "$J" ] || fresh "$R/jobs"\nfresh "$J" "$I"\n'


def _head(remote_dir: str, operation_id: str | None = None) -> str:
    lines = [f"R={root_expression(remote_dir)}"]
    if operation_id is not None:
        lines.append(f'J="$R/jobs/"{q(operation_id)}')
        lines.append(f'I="$R/incoming/"{q(operation_id)}')
    see = _SEE_JOB if operation_id is not None else ""
    return "\n".join(lines) + "\n" + _PRELUDE + see


def _detect(choice: str) -> str:
    if choice != "auto":
        return f'S={q(choice)}\necho "@@scheduler $S"\n'
    lines = ["S=none"]
    for name, command in reversed(SCHEDULER_COMMANDS):
        lines.append(f"command -v {command} >/dev/null 2>&1 && S={name}")
    lines.append('echo "@@scheduler $S"')
    return "\n".join(lines) + "\n" + BATCH_HINT_SH


def check_script(remote_dir: str, operation_id: str, scheduler: str, digests: Iterable[str]) -> str:
    """Before a submit: the root, the scheduler, which inputs the site has, any submission."""
    have = "\n".join(
        f'c=$(cas {q(digest)}); fresh "${{c%/*}}"; [ -f "$c" ] && echo "@@have {digest}"'
        for digest in sorted(set(digests))
    )
    return (
        _head(remote_dir, operation_id)
        + 'echo "@@root $R"\n'
        + _detect(scheduler)
        + f"{have}\n"
        + 'if [ -f "$J/submit.rc" ]; then echo "@@rc $(cat "$J/submit.rc")"; '
        + 'echo "@@out"; cat "$J/submit.out" 2>/dev/null; echo; '
        + 'echo "@@used $(cat "$J/scheduler" 2>/dev/null)"; fi\n'
        + 'mkdir -p "$I" && echo "@@ready"\n'
    )


def submit_script(
    remote_dir: str,
    operation_id: str,
    *,
    blobs: Sequence[str],
    inputs: Sequence[tuple[str, str]],
    job_files: Sequence[str],
    launcher_dir: str | None,
    launcher_files: Sequence[str],
    submit_command: str,
) -> str:
    """Move the uploads into place, link the inputs and submit, once (RL16, RL17)."""
    move_blobs = "\n".join(
        f"""f="$I"/{q(d)}
if [ -f "$f" ]; then
  got=$(sha "$f")
  if [ -n "$got" ] && [ "$got" != {q(d)} ]; then echo "@@corrupt {d}"; rm -f "$f"
  else c=$(cas {q(d)}); mkdir -p "$(dirname "$c")"; mv -f "$f" "$c"; chmod 444 "$c"; fi
fi"""
        for d in blobs
    )
    launcher = ""
    if launcher_dir is not None:
        copies = " && ".join(f'cp "$I"/{q(name)} "$L.$$"/' for name in launcher_files)
        launcher = f"""L="$R/psij"/{q(launcher_dir)}
if [ ! -d "$L" ]; then
  mkdir -p "$L.$$" && {copies} && chmod 755 "$L.$$"/* && mv "$L.$$" "$L" 2>/dev/null
  # If another submit installed it first, mv put this copy inside theirs.
  rm -rf "$L.$$" "$L/${{L##*/}}.$$"
fi
"""
    moves = "\n".join(f'  mv -f "$I"/{q(name)} "$J"/{q(name)}' for name in job_files)
    links = "\n".join(
        f"""  c=$(cas {q(digest)}); t="$J"/{q(name)}; fresh "${{c%/*}}"
  if [ ! -f "$c" ]; then echo "@@missing {digest}"; ok=0
  else mkdir -p "$(dirname "$t")"; rm -f "$t"; ln "$c" "$t" 2>/dev/null || cp "$c" "$t"; fi"""
        for name, digest in inputs
    )
    return (
        # A dropped connection must not stop a submit between the scheduler's answer
        # and the record of it.
        "trap '' HUP PIPE\n"
        + _head(remote_dir, operation_id)
        + 'umask 077\nmkdir -p "$J" "$R/content/cas/sha256" "$R/psij"\n'
        + f"{move_blobs}\n{launcher}"
        + 'if [ -f "$J/submit.rc" ] && [ "$(cat "$J/submit.rc")" != 0 ]; then\n'
        + '  rm -f "$J/submit.rc"; rmdir "$J/.submit.lock" 2>/dev/null\nfi\n'
        + 'if mkdir "$J/.submit.lock" 2>/dev/null; then\n'
        + "  ok=1\n"
        + f"{moves}\n{links}\n"
        + '  if [ "$ok" = 1 ]; then\n'
        + '    echo "$(ts) submitted" >> "$J/states"\n'
        + f'    (cd "$J" && {submit_command}) </dev/null >"$J/submit.out" 2>"$J/submit.err"\n'
        + '    echo $? > "$J/submit.rc.tmp" && mv -f "$J/submit.rc.tmp" "$J/submit.rc"\n'
        + '  else rmdir "$J/.submit.lock"; fi\n'
        + "else\n"
        + "  n=0\n"
        + '  while [ ! -f "$J/submit.rc" ] && [ $n -lt 600 ]; do\n'
        + '    sleep 1; fresh "$J"; n=$((n+1))\n  done\n'
        + "fi\n"
        + 'rm -rf "$I"\n'
        + 'if [ -f "$J/submit.rc" ]; then echo "@@rc $(cat "$J/submit.rc")"; fi\n'
        + 'echo "@@out"; cat "$J/submit.out" 2>/dev/null; echo\n'
        + 'echo "@@err"; cat "$J/submit.err" 2>/dev/null; echo\n'
    )


DETACHED_SUBMIT = (
    "if command -v setsid >/dev/null 2>&1; then "
    '(setsid sh "$J/supervise.sh" </dev/null >"$J/supervisor.log" 2>&1 &); '
    'else (nohup sh "$J/supervise.sh" </dev/null >"$J/supervisor.log" 2>&1 &); fi; '
    'n=0; while [ ! -s "$J/pid" ] && [ $n -lt 300 ]; do '
    "sleep 0.1 2>/dev/null || sleep 1; n=$((n+1)); done; "
    '[ -s "$J/pid" ] && echo "pid:$(cat "$J/pid")"'
)


def supervise_script(job_dir: PurePosixPath) -> str:
    """The supervisor of a job on a host with no scheduler (RL17)."""
    return f"""#!/bin/sh
J={q(str(job_dir))}
cd "$J" || exit 1
ts() {{ date -u +%Y-%m-%dT%H:%M:%SZ; }}
echo $$ > pid.tmp && mv -f pid.tmp pid
if [ -e cancel ]; then
  echo 143 > exit_code.tmp && mv -f exit_code.tmp exit_code
  exit 0
fi
if command -v setsid >/dev/null 2>&1; then setsid sh "$J/child.sh" & else sh "$J/child.sh" & fi
child=$!
echo $child > child_pid
echo "$(ts) running pid $child" >> states
wait $child
ec=$?
echo $ec > exit_code.tmp && mv -f exit_code.tmp exit_code
"""


def child_script(
    *,
    directory: PurePosixPath,
    executable: str,
    arguments: Sequence[str],
    environment: dict[str, str],
    inherit_environment: bool,
    stdin_path: PurePosixPath | None,
    stdout_path: PurePosixPath,
    stderr_path: PurePosixPath,
) -> str:
    """The job itself, for the supervisor to start."""
    command = shlex.join([executable, *arguments])
    if inherit_environment:
        exports = "".join(f"export {name}={q(value)}\n" for name, value in environment.items())
        launch = f"exec {command}"
    else:
        exports = ""
        pairs = " ".join(q(f"{name}={value}") for name, value in environment.items())
        launch = f"exec env -i {pairs} {command}"
    stdin = q(str(stdin_path)) if stdin_path is not None else "/dev/null"
    return f"""#!/bin/sh
cd {q(str(directory))} || exit 1
{exports}{launch} <{stdin} >{q(str(stdout_path))} 2>{q(str(stderr_path))}
"""


# Inputs are hard links into the content store, so an input the job left alone is
# not an output. One the job replaced has a single link and is listed.
_LISTING = 'echo "@@outputs"; find . -type f -links 1 -exec wc -c {} + 2>/dev/null\n'


def status_script(
    job_dir: str,
    *,
    mode: str,
    status_command: str | None,
    native_id: str | None,
    accounting_command: str | None = None,
) -> str:
    """One poll: the record, then the scheduler or the supervisor (RL17).

    ``accounting_command`` runs only when the queue no longer lists the job and
    the job wrote no exit code, as the last evidence of how it ended.
    """
    body = f"J={q(job_dir)}\n" + _ENTER_JOB
    body += 'if [ -f final ]; then echo "@@final"; cat final; echo\n' + _LISTING + "exit 0; fi\n"
    body += 'echo "@@last"; tail -n 1 states 2>/dev/null\n'
    body += 'echo "@@inputs"; cat inputs.json 2>/dev/null; echo\n'
    if mode == "scheduler" and status_command is not None and native_id is not None:
        ec = q(native_id + ".ec")
        body += (
            f"out=$({status_command} 2>&1 </dev/null); rc=$?\n"
            'echo "@@schedrc $rc"; echo "@@sched"; printf "%s\\n" "$out"\n'
            f'echo "@@ec"; cat {ec} 2>/dev/null; echo\n'
            '[ -s signalled ] && echo "@@signalled $(cat signalled)"\n'
        )
        if accounting_command is not None:
            body += (
                f"if {{ [ ! -s {ec} ] || [ -s signalled ]; }} && ! printf '%s\\n' \"$out\" | "
                f"awk '{{print $1}}' | grep -qxF {q(native_id)}; then\n"
                f'  echo "@@acct"; {accounting_command} 2>/dev/null </dev/null; echo\n'
                "fi\n"
            )
    else:
        body += (
            'echo "@@exit"; cat exit_code 2>/dev/null; echo\n'
            "pid=$(cat pid 2>/dev/null)\n"
            'if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then echo "@@alive"; fi\n'
            '[ -s child_pid ] && echo "@@started"\n'
        )
    body += '[ -e cancel ] && echo "@@cancel"\n'
    body += "exit 0\n"
    return body


def jarvis_status_script(
    job_dir: str,
    execution_id: str,
    pipeline: str | None = None,
    *,
    states: str | None = None,
) -> str:
    """One poll of a job JARVIS submitted: ``jarvis execution get``, and its artifacts.

    ``execution get`` reconciles the record with the scheduler and commits it,
    so relay keeps no state of its own: a pending job's place in the queue is
    the record's ``metadata.queue``, and a job that left the queue unseen is
    ended by JARVIS. The artifacts are read once the record is terminal.
    ``pipeline`` is as in :func:`jarvis_submit.execution_argv`. ``states`` is
    JARVIS's ``components.jsonl``, printed under ``@@components`` after ``get``.
    """
    get = jarvis_submit.execution_shell("get", execution_id, job_dir=job_dir, pipeline=pipeline)
    artifacts = jarvis_submit.execution_shell(
        "artifacts", execution_id, job_dir=job_dir, pipeline=pipeline
    )
    execution = q(jarvis_submit.execution_dir(job_dir, execution_id, pipeline=pipeline))
    return (
        f"J={q(job_dir)}\n"
        + _ENTER_JOB
        + 'echo "@@inputs"; cat inputs.json 2>/dev/null; echo\n'
        + '[ -e inputs.dropped ] && echo "@@dropped"\n'
        + f"fresh {execution}; jarvis=$(cat {jarvis_submit.JARVIS_FILE} 2>/dev/null)\n"
        + '[ -n "$jarvis" ] || { echo "@@nojarvis"; exit 0; }\n'
        + f"out=$({get} 2>&1 </dev/null); rc=$?\n"
        + 'echo "@@getrc $rc"; echo "@@get"; printf "%s\\n" "$out"\n'
        + (f'echo "@@components"; cat {q(states)} 2>/dev/null; echo\n' if states else "")
        + 'if [ "$rc" -eq 0 ] && printf \'%s\\n\' "$out" | tail -n 1 | grep -qF '
        + "'\"terminal\":true'; then\n"
        + f'  echo "@@artifacts"; {artifacts} 2>/dev/null </dev/null; echo\n'
        + "fi\n"
        + "exit 0\n"
    )


def jarvis_execution_script(
    job_dir: str,
    verb: jarvis_submit.ExecutionVerb,
    execution_id: str,
    pipeline: str,
    extra: Sequence[str] = (),
) -> str:
    """Run ``jarvis execution <verb>`` (cancel, stop, move or service-runtimes) in the job
    directory, with the verb's own options ``extra``.

    Prints ``@@rc`` and the command's output under ``@@out``; the output's last
    line is the document JARVIS printed: the execution record it committed, its
    service runtimes, or a typed refusal.
    """
    command = jarvis_submit.execution_shell(
        verb, execution_id, job_dir=job_dir, pipeline=pipeline, extra=extra
    )
    return (
        f"J={q(job_dir)}\n"
        + _ENTER_JOB
        + f"jarvis=$(cat {jarvis_submit.JARVIS_FILE} 2>/dev/null)\n"
        + '[ -n "$jarvis" ] || { echo "@@nojarvis"; exit 0; }\n'
        + f"out=$({command} 2>&1 </dev/null); rc=$?\n"
        + 'echo "@@rc $rc"; echo "@@out"; printf "%s\\n" "$out"\n'
        + "exit 0\n"
    )


def jarvis_install_script(job_dir: str, install_command: str) -> str:
    """Submit the install job from the job directory: the site script's ``install`` mode.

    Prints ``@@rc`` and the output under ``@@out``; the handle line JARVIS printed
    is kept as ``relay.install.json`` and printed under ``@@handle``.
    """
    return (
        f"J={q(job_dir)}\n"
        + _ENTER_JOB
        + f"out=$({install_command} 2>&1 </dev/null); rc=$?\n"
        + 'echo "@@rc $rc"; echo "@@out"; printf "%s\\n" "$out"\n'
        + f'echo "@@handle"; cat {jarvis_submit.INSTALL_FILE} 2>/dev/null; echo\n'
        + "exit 0\n"
    )


def drop_inputs_script(job_dir: str, root: str, inputs: Sequence[store.StagedInput]) -> str:
    """Once a JARVIS job ended: drop spent and transient inputs from the content store."""
    return (
        f"J={q(job_dir)}\n"
        + 'cd "$J" || exit 3\n'
        + FRESH
        + store.drop_script(root, inputs)
        + ": > inputs.dropped\n"
    )


def record_script(
    job_dir: str,
    line: str,
    *,
    final: bool,
    root: str | None = None,
    inputs: Sequence[store.StagedInput] = (),
) -> str:
    """Append a state change. With ``final``, standard input becomes the ``final`` file.

    A final record also drops spent and transient inputs from the content store
    (see :mod:`clio_relay.adapters.ssh.store`).
    """
    body = f"J={q(job_dir)}\n" + 'cd "$J" || exit 3\n'
    if final:
        body += "cat > final.tmp && mv -f final.tmp final\n"
    body += f"printf '%s %s\\n' \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\" {q(line)} >> states\n"
    if final and root is not None and inputs:
        body += FRESH + store.drop_script(root, inputs)
    if final:
        body += _LISTING
    return body


def cancel_script(job_dir: str, *, cancel_command: str | None) -> str:
    """Mark the job cancelled and stop it."""
    body = (
        f"J={q(job_dir)}\n"
        + _ENTER_JOB
        + '[ -f final ] && { echo "@@final"; exit 0; }\n'
        + ": > cancel\n"
        + "printf '%s cancel requested\\n' \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\" >> states\n"
    )
    if cancel_command is not None:
        body += f'out=$({cancel_command} 2>&1 </dev/null); echo "@@rc $?"; printf "%s\\n" "$out"\n'
    else:
        body += (
            "c=$(cat child_pid 2>/dev/null)\n"
            'if [ -n "$c" ]; then\n'
            '  kill -TERM -- "-$c" 2>/dev/null || kill -TERM "$c" 2>/dev/null\n'
            "fi\n"
            'echo "@@rc 0"\n'
        )
    return body


def find_script(remote_dir: str, operation_id: str) -> str:
    """Find a submission from its job directory, which is named by the operation (RL17)."""
    return (
        _head(remote_dir, operation_id)
        + 'echo "@@root $R"\n'
        + 'if [ -f "$J/submit.rc" ]; then echo "@@rc $(cat "$J/submit.rc")"; '
        + 'echo "@@used $(cat "$J/scheduler" 2>/dev/null)"; '
        + 'echo "@@out"; cat "$J/submit.out" 2>/dev/null; echo; fi\n'
    )


def graph_script(remote_dir: str, jarvis: str = "") -> str:
    """Print where JARVIS keeps the site's record, and its net report when JARVIS is placed.

    ``jarvis rg net show +json`` only reads the site graph, so it is safe on a
    login host. ``jarvis`` is the site's ``jarvis`` setting; empty is relay's pin.
    """
    home = remote_dir.rstrip("/") or "/"
    program = root_expression(jarvis or jarvis_submit.bootstrap_path(home))
    return (
        f"G={root_expression(jarvis_submit.site_graph(home))}\n"
        f"D={root_expression(jarvis_submit.site_decisions(home))}\n"
        f"R={root_expression(jarvis_submit.site_root(home))}\n"
        f"JB={program}\n"
        'echo "@@graph_path $G"; echo "@@decisions_path $D"\n'
        'if [ -x "$JB" ]; then\n'
        '  mkdir -p "$R" 2>/dev/null; T=; command -v timeout >/dev/null 2>&1 && T="timeout 60"\n'
        '  echo "@@graph"\n'
        '  JARVIS_ROOT="$R" JARVIS_RESOURCE_GRAPH="$G" $T "$JB" rg net show +json </dev/null 2>&1\n'
        "fi\n"
    )


def graph_collect_command(remote_dir: str, scheduler: str, jarvis: str = "") -> list[str]:
    """Return the ``exec`` argv that submits relay's network collection on ``scheduler``.

    It writes relay's collection package (:mod:`clio_relay.jarvis_graph`) and
    :func:`jarvis_submit.graph_pipeline` beside the site graph, registers the
    package in the site-level JARVIS root, and runs ``jarvis ppl submit`` (``ppl
    run`` with no scheduler), with the site graph and a site-level JARVIS root,
    printing JARVIS's handle as JSON. An argument after the argv's own (``$1``)
    is the queue the job goes to; with none, the scheduler's default.
    """
    home = remote_dir.rstrip("/") or "/"
    pipeline = jarvis_submit.render_pipeline(jarvis_submit.graph_pipeline(scheduler))
    verb = "run" if scheduler in ("", "none") else "submit"
    # ``ppl run`` names its source (``yaml <file>``); ``ppl submit`` takes the file.
    source = "yaml " if verb == "run" else ""
    # The scheduler block is the pipeline's last mapping, so a partition line ends it.
    queue = (
        'case "${1:-}" in\n'
        '  "") ;;\n'
        '  *[!A-Za-z0-9_.-]*) echo "not a queue name: $1" >&2; exit 64 ;;\n'
        '  *) printf \'  partition: %s\\n\' "$1" >> "$S/site_net.yaml" ;;\n'
        "esac\n"
        if verb == "submit"
        else ""
    )
    graph = jarvis_submit.site_graph(home)
    site = posixpath.dirname(graph)
    script = (
        f"S={root_expression(site)}; R={root_expression(jarvis_submit.site_root(home))}\n"
        f"JB={root_expression(jarvis or jarvis_submit.bootstrap_path(home))}\n"
        '[ -x "$JB" ] || { echo "relay has not placed JARVIS on this site yet" >&2; exit 69; }\n'
        + _INIT_SITE_ROOT
        + jarvis_graph.write_lines('"$S"')
        + f'printf %s {q(pipeline)} > "$S/site_net.yaml"\n'
        + queue
        + f'JARVIS_ROOT="$R" JARVIS_RESOURCE_GRAPH={root_expression(graph)} '
        f'exec "$JB" ppl {verb} {source}"$S/site_net.yaml" +json\n'
    )
    return ["sh", "-c", script, "site_net"]


def _jarvis_env(home: str, jarvis: str) -> str:
    """Shell lines that name relay's JARVIS (``JB``) and the site's JARVIS record."""
    return (
        f"G={root_expression(jarvis_submit.site_graph(home))}\n"
        f"D={root_expression(jarvis_submit.site_decisions(home))}\n"
        f"R={root_expression(jarvis_submit.site_root(home))}\n"
        f"SR={root_expression(posixpath.join(home, jarvis_submit.SPACK_DIR))}\n"
        f"JB={root_expression(jarvis or jarvis_submit.bootstrap_path(home))}\n"
    )


# The site-level JARVIS root (``$R``) is set up once, the first time a site-level
# call needs it; a run's own root is set up by the run script. It never keeps
# jarvis-cd's builtin repository.
_INIT_SITE_ROOT = (
    'mkdir -p "$R"\n'
    '[ -f "$R/jarvis_config.yaml" ] || JARVIS_ROOT="$R" "$JB" init '
    '"$R/config" "$R/private" "$R/shared" >/dev/null 2>&1\n' + jarvis_graph.DROP_BUILTIN
)


_JARVIS_EXPORTS = (
    'export JARVIS_ROOT="$R" JARVIS_RESOURCE_GRAPH="$G" JARVIS_SPACK_DECISIONS="$D"\n'
    'if [ -x "$SR/bin/spack" ]; then export JARVIS_SPACK_ROOT="$SR"; fi\n'
)


def jarvis_command(remote_dir: str, jarvis: str = "") -> list[str]:
    """Return the ``exec`` argv prefix that runs relay's JARVIS with the site's record.

    The caller appends JARVIS's own arguments (``sched info +json``, ``spack
    prepare ... +json``, ``rg skill show NAME +json``, ``rg net set ...``). It
    exports the site graph, the decisions file, a site-level JARVIS root and,
    when it has ``bin/spack``, the site's JARVIS Spack, as every relay job does.
    """
    home = remote_dir.rstrip("/") or "/"
    script = (
        _jarvis_env(home, jarvis)
        + '[ -x "$JB" ] || { echo "relay has not placed JARVIS on this site yet" >&2; exit 69; }\n'
        + _INIT_SITE_ROOT
        + _JARVIS_EXPORTS
        + 'exec "$JB" "$@"\n'
    )
    return ["sh", "-c", script, "jarvis"]


def sched_script(remote_dir: str, jarvis: str = "") -> str:
    """Print ``jarvis sched info +json`` under ``@@sched`` when JARVIS is placed.

    JARVIS reads the scheduler, the MPI candidates, the network and the site
    skills itself, with light reads only, so it is safe on a login host. Without
    JARVIS it prints ``@@nosched``.
    """
    home = remote_dir.rstrip("/") or "/"
    return (
        "(\n"
        + _jarvis_env(home, jarvis)
        + 'if [ -x "$JB" ]; then\n'
        + "  "
        + _INIT_SITE_ROOT.replace("\n[", "\n  [")
        + '  T=; command -v timeout >/dev/null 2>&1 && T="timeout 90"\n'
        + "  "
        + _JARVIS_EXPORTS.replace("\nif", "\n  if")
        + '  echo "@@sched"\n'
        '  $T "$JB" sched info +json </dev/null 2>&1\n'
        "else\n"
        '  echo "@@nosched"\n'
        "fi\n"
        ")\n"
    )


def describe_script(scheduler: str, remote_dir: str | None = None, jarvis: str = "") -> str:
    """Report the host, JARVIS's site graph and JARVIS's scheduler facts.

    Short reads only. The graph part is :func:`graph_script` and the scheduler
    part :func:`sched_script`, when ``remote_dir`` is given. Relay reads no
    partitions or MPI facts itself; before JARVIS is on the site it lists only
    the queue names.
    """
    queues = (
        'case "$S" in\n'
        'slurm) echo "@@queues"; sinfo -h -o %R 2>/dev/null ;;\n'
        "pbs) echo \"@@queues\"; qstat -Q 2>/dev/null | awk 'NR>2 {print $1}' ;;\n"
        "esac\n"
    )
    jarvis_part = ""
    if remote_dir is not None:
        jarvis_part = graph_script(remote_dir, jarvis) + sched_script(remote_dir, jarvis)
    return (
        jarvis_part + 'echo "@@host $(hostname 2>/dev/null)"\n'
        'echo "@@os $(uname -s 2>/dev/null)"\n'
        'echo "@@machine $(uname -m 2>/dev/null)"\n'
        'echo "@@cpus $(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null)"\n'
        'echo "@@home $HOME"\n' + _detect(scheduler) + queues
    )


def relay_jarvis(command: Sequence[str], remote_dir: str, jarvis: str = "") -> list[str]:
    """Return ``command`` with a bare ``jarvis`` program run as relay's JARVIS.

    ``jarvis`` in ``exec`` names relay's JARVIS with the site's record
    (:func:`jarvis_command`), at the start or after the login-load wrapper
    (:func:`clio_relay.login_load.light_argv`). The arguments after it reach
    JARVIS as ``"$@"``, never parsed by a shell. Any other command is unchanged.
    """
    argv = list(command)
    at = 4 if argv[:2] == ["sh", "-c"] and argv[3:4] == ["clio-light"] else 0
    if argv[at : at + 1] != ["jarvis"]:
        return argv
    return [*argv[:at], *jarvis_command(remote_dir, jarvis), *argv[at + 1 :]]


def exec_script(command: Sequence[str], remote_dir: str | None = None, jarvis: str = "") -> str:
    """Run one command from the home directory, stopped when relay lets go.

    Relay holds the session's standard input open. When it closes, because the
    call ended, timed out or was cancelled, or the connection dropped, the
    watchdog stops the command's process group.

    A program that is not a file on the ssh session's PATH, such as Lmod's
    ``module`` shell function, runs in a login shell, which defines it. The
    login shell gets the arguments as ``"$@"``, so it never parses them again.
    With ``remote_dir``, a bare ``jarvis`` is relay's JARVIS (:func:`relay_jarvis`).
    """
    if remote_dir is not None:
        command = relay_jarvis(command, remote_dir, jarvis)
    joined = shlex.join(list(command))
    program = shlex.quote(command[0])
    return (
        'cd "$HOME" 2>/dev/null\n'
        f"set -- {joined}\n"
        f"case {program} in */*) ;; *)\n"
        f'  case "$(command -v -- {program} 2>/dev/null)" in /*) ;; *)\n'
        "    if command -v bash >/dev/null 2>&1; then\n"
        '      set -- bash -lc \'"$@"\' relay-exec "$@"\n'
        "    fi ;;\n"
        "  esac ;;\n"
        "esac\n"
        "exec 3<&0\n"
        "if command -v setsid >/dev/null 2>&1; then\n"
        '  setsid "$@" </dev/null 3<&- &\n'
        "else\n"
        '  "$@" </dev/null 3<&- &\n'
        "fi\n"
        "c=$!\n"
        # The watchdog's cat holds the session's stdin; its trap takes the cat down
        # with it, so nothing of relay's lingers in the session after the command.
        "(\n"
        "  trap 'kill \"$k\" 2>/dev/null; exit 0' TERM\n"
        "  cat <&3 >/dev/null 2>&1 &\n"
        "  k=$!\n"
        '  wait "$k"\n'
        '  kill -TERM -"$c" 2>/dev/null || kill -TERM "$c" 2>/dev/null\n'
        '  sleep 2; kill -KILL -"$c" 2>/dev/null || kill -KILL "$c" 2>/dev/null\n'
        ") >/dev/null 2>&1 &\n"
        "w=$!\n"
        "exec 3<&-\n"
        'wait "$c"; rc=$?\n'
        'kill "$w" 2>/dev/null\n'
        'exit "$rc"\n'
    )


def listing_entries(lines: Iterable[str]) -> list[tuple[str, int]]:
    """Parse ``wc -c`` lines from ``find .`` into ``(relative path, size)``."""
    found: list[tuple[str, int]] = []
    for raw in lines:
        parts = raw.strip().split(None, 1)
        if len(parts) != 2 or not parts[1].startswith("./"):
            continue
        try:
            size = int(parts[0])
        except ValueError:
            continue
        found.append((parts[1][2:], size))
    return found
