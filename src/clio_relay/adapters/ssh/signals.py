"""Passing a cancel's signal on to the command, in rendered Slurm scripts (RL18).

Some Slurm sites signal only the batch shell on a cancel and kill the rest after
``KillWait``, so a trap in the command would never run. The launch runs in its own
process group, and the batch shell passes signals on to it.
"""

from __future__ import annotations

import shlex

_FORWARD_SIGNALS = """# Some Slurm sites signal only this shell on a cancel and kill the rest after
# KillWait. Run the launch in its own process group and pass signals on to it.
# Where the scheduler signals every process itself, the launch has already ended,
# so the signal is not sent twice. After a TERM or INT either way, wait for the
# whole group, since the job ends when this shell does and the command's traps
# must finish first.
_relay_job=
_relay_held=
_relay_stop=
_relay_live() {{
  command -v ps >/dev/null 2>&1 || return 0
  case "$(ps -o stat= -p "$1" 2>/dev/null)" in ''|Z*) return 1;; esac
}}
_relay_pass() {{
  case $1 in TERM|INT) echo "$1" > {marker}; _relay_stop=1;; esac
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
if [ -n "$_relay_stop" ]; then
  while kill -0 -- "-$_relay_job" 2>/dev/null; do sleep 1; done
fi
set +m
_relay_return() {{ return "$1"; }}
_relay_return "$_relay_rc"
"""


def _command_end(lines: list[str], start: int) -> int:
    """Return the index of the last line of the command that starts at ``lines[start]``.

    An argument can hold newlines inside its quotes, such as a multi-line
    ``bash -c`` script, so the command ends on the first line where every quote
    is closed and no backslash continues it.
    """
    for end in range(start, len(lines)):
        text = "\n".join(lines[start : end + 1])
        if text.rstrip().endswith("\\"):
            continue
        try:
            shlex.split(text)
        except ValueError:
            continue
        return end
    raise ValueError("the launch command never ends")


def forward_signals(script: str, launcher: str, job_dir: str) -> str:
    """Wrap the launch command of a rendered batch script so a cancel reaches it.

    A TERM or INT is also recorded in the job's ``signalled`` file. The command's
    exit code then says only that it was stopped, not whether by a cancel, the
    time limit or preemption.
    """
    marker = shlex.quote(f"{job_dir.rstrip('/')}/signalled")
    lines = script.split("\n")
    for index, line in enumerate(lines):
        if launcher in line and not line.lstrip().startswith("#"):
            end = _command_end(lines, index)
            launch = "\n".join(lines[index : end + 1]).rstrip()
            wrapped = _FORWARD_SIGNALS.format(launch=launch, marker=marker)
            lines[index : end + 1] = [wrapped.rstrip("\n")]
            return "\n".join(lines)
    raise ValueError("the rendered script has no launch line")
