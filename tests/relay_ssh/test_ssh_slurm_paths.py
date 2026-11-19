"""Slurm's filename patterns in a job's paths, as sbatch would expand them."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path, PurePosixPath

import pytest

from clio_relay.adapters.ssh.slurm_paths import expand, mark, shell_word
from clio_relay.jobs.spec import JobSpec
from tests.relay_ssh.test_ssh_units import _adapter  # pyright: ignore[reportPrivateUsage]

pytestmark = pytest.mark.release_platform("posix")

_SLURM = {
    "SLURM_JOB_ID": "27873",
    "SLURM_JOB_NAME": "relay-op_abc",
    "SLURMD_NODENAME": "ares-comp-01",
}


def _words(command: str, **environment: str) -> list[str]:
    """What bash makes of ``command``'s words, in a Slurm job's environment."""
    result = subprocess.run(
        ["bash", "-c", f"printf '%s\\n' {command}"],
        env={"PATH": os.environ["PATH"], **_SLURM, **environment},
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.splitlines()


@pytest.mark.parametrize(
    ("path", "expanded"),
    [
        ("hostname-%j.out", "hostname-27873.out"),
        ("/scratch/my runs/%x/%j-%N.err", "/scratch/my runs/relay-op_abc/27873-ares-comp-01.err"),
        ("100%%-%j.out", "100%-27873.out"),
        ("%A_%a.out", "27873_4294967294.out"),
        ("it's-$HOME-%j", "it's-$HOME-27873"),
        ("left-%3a-%q", "left-%3a-%q"),
    ],
)
def test_a_path_expands_as_sbatch_would(path: str, expanded: str) -> None:
    assert _words(shell_word(path)) == [expanded]


def test_the_array_patterns_use_the_array_ids() -> None:
    word = shell_word("%A_%a.out")
    assert _words(word, SLURM_ARRAY_JOB_ID="500", SLURM_ARRAY_TASK_ID="3") == ["500_3.out"]


def test_only_paths_with_patterns_are_marked() -> None:
    command = ["/bin/bash", "launch.sh", "id", "", "", "", "/dev/null", "/j/out-%j", "/j/err"]
    marks = mark(command, range(6, 9))
    assert marks == {"__relay_path_7__": "/j/out-%j"}
    assert command[7] == "__relay_path_7__"
    assert command[8] == "/j/err"
    with pytest.raises(ValueError, match="__relay_path_7__ once"):
        expand("no placeholder here", marks)


def _launch_words(script: str) -> list[str]:
    launch = next(
        line
        for line in script.splitlines()
        if "single_launch.sh" in line and not line.lstrip().startswith("#")
    )
    words = launch.removesuffix(" &").split(" ", 1)[1]
    return _words(words)


@pytest.mark.parametrize(("scheduler", "stdout"), [("slurm", "hostname-27873.out"), ("pbs", None)])
def test_a_slurm_job_writes_to_a_file_named_for_its_id(
    tmp_path: Path, scheduler: str, stdout: str | None
) -> None:
    adapter = _adapter(tmp_path)
    root = PurePosixPath("/home/someone/.clio-relay")
    job_dir = root / "jobs" / "op_abc"
    spec = JobSpec(executable="hostname", stdout_path="hostname-%j.out", stderr_path="err")
    rendered = adapter._render(spec, scheduler, root, job_dir, "relay-op_abc")  # pyright: ignore[reportPrivateUsage, reportArgumentType]
    script = rendered.files["submit.sh"]
    assert "__relay_path_" not in script
    words = _launch_words(script)
    if stdout is None:
        assert f"{job_dir}/hostname-%j.out" in words
    else:
        assert words[6:8] == [f"{job_dir}/{stdout}", f"{job_dir}/err"]
        assert words[-1] == "hostname"
