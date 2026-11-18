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
"""

from __future__ import annotations

import shlex
from collections.abc import Iterable, Sequence
from pathlib import PurePosixPath

from clio_relay.adapters.ssh import store

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
    return "\n".join(lines) + "\n"


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


_FORWARD_SIGNALS = """# Some Slurm sites signal only this shell on a cancel and kill the rest after
# KillWait. Run the launch in its own process group, pass signals on to it and wait.
# Where the scheduler signals every process itself, the launch has already ended,
# so the signal is not sent twice.
_relay_job=
_relay_held=
_relay_live() {{
  command -v ps >/dev/null 2>&1 || return 0
  case "$(ps -o stat= -p "$1" 2>/dev/null)" in ''|Z*) return 1;; esac
}}
_relay_pass() {{
  case $1 in TERM|INT) echo "$1" > {marker};; esac
  if [ -z "$_relay_job" ]; then _relay_held=$1; return; fi
  sleep 1
  _relay_live "$_relay_job" && kill -s "$1" -- "-$_relay_job" 2>/dev/null
}}
trap '_relay_pass TERM' TERM
trap '_relay_pass INT' INT
trap '_relay_pass USR1' USR1
trap '_relay_pass USR2' USR2
set -m
{launch} &
_relay_job=$!
[ -n "$_relay_held" ] && _relay_pass "$_relay_held"
while :; do wait "$_relay_job"; _relay_rc=$?; kill -0 "$_relay_job" 2>/dev/null || break; done
set +m
_relay_return() {{ return "$1"; }}
_relay_return "$_relay_rc"
"""


def forward_signals(script: str, launcher: str, job_dir: str) -> str:
    """Wrap the launch line of a rendered batch script so a cancel reaches the command.

    A TERM or INT is also recorded in the job's ``signalled`` file. The command's
    exit code then says only that it was stopped, not whether by a cancel, the
    time limit or preemption.
    """
    marker = q(f"{job_dir.rstrip('/')}/signalled")
    lines = script.split("\n")
    for index, line in enumerate(lines):
        if launcher in line and not line.lstrip().startswith("#"):
            wrapped = _FORWARD_SIGNALS.format(launch=line.rstrip(), marker=marker)
            lines[index] = wrapped.rstrip("\n")
            return "\n".join(lines)
    raise ValueError("the rendered script has no launch line")


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


def record_script(
    job_dir: str,
    line: str,
    *,
    final: bool,
    root: str | None = None,
    inputs: Sequence[tuple[str, str]] = (),
) -> str:
    """Append a state change. With ``final``, standard input becomes the ``final`` file.

    A final record also drops the inputs the job deleted from the content store
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


def describe_script(scheduler: str) -> str:
    """Report the host."""
    return (
        'echo "@@host $(hostname 2>/dev/null)"\n'
        'echo "@@os $(uname -s 2>/dev/null)"\n'
        'echo "@@machine $(uname -m 2>/dev/null)"\n'
        'echo "@@cpus $(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null)"\n'
        'echo "@@home $HOME"\n'
        + _detect(scheduler)
        + 'case "$S" in\n'
        + 'slurm) echo "@@queues"; sinfo -h -o %R 2>/dev/null ;;\n'
        + "pbs) echo \"@@queues\"; qstat -Q 2>/dev/null | awk 'NR>2 {print $1}' ;;\n"
        + "esac\n"
    )


def exec_script(command: Sequence[str]) -> str:
    """Run one command from the home directory, stopped when relay lets go.

    Relay holds the session's standard input open. When it closes, because the
    call ended, timed out or was cancelled, or the connection dropped, the
    watchdog stops the command's process group.
    """
    joined = shlex.join(list(command))
    return (
        'cd "$HOME" 2>/dev/null\n'
        "exec 3<&0\n"
        "if command -v setsid >/dev/null 2>&1; then\n"
        f"  setsid {joined} </dev/null 3<&- &\n"
        "else\n"
        f"  {joined} </dev/null 3<&- &\n"
        "fi\n"
        "c=$!\n"
        '( cat <&3 >/dev/null 2>&1; kill -TERM -"$c" 2>/dev/null || kill -TERM "$c" 2>/dev/null\n'
        '  sleep 2; kill -KILL -"$c" 2>/dev/null || kill -KILL "$c" 2>/dev/null ) '
        ">/dev/null 2>&1 &\n"
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
