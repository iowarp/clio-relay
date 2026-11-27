from __future__ import annotations

import pytest

from clio_relay.guard import execlist


@pytest.mark.parametrize(
    "command",
    [
        ["squeue", "-u", "relay"],
        ["ls", "-la", "/scratch"],
        ["cat", "job.out"],
        ["sacct", "-j", "12"],
        ["scontrol", "show", "job", "12"],
        ["scontrol", "-d", "show", "node"],
        ["find", ".", "-name", "*.out"],
        ["lfs", "quota", "-u", "relay", "/scratch"],
        ["sacctmgr", "show", "assoc", "-n", "-P", "format=account"],
        ["pbsnodes", "-a"],
        ["module", "avail"],
        ["jarvis", "ppl", "list"],
        ["jarvis", "repo", "list"],
        ["jarvis", "pkg", "readme", "demo.hello"],
        ["jarvis", "sched", "info", "+json"],
        ["jarvis", "rg", "facts", "show", "+json"],
        ["jarvis", "rg", "skill", "list", "+json"],
        ["jarvis", "rg", "skill", "show", "slurm-ares", "+json"],
    ],
)
def test_read_only_commands_pass(command: list[str]) -> None:
    assert execlist.refusal(command) is None


@pytest.mark.parametrize(
    "command",
    [
        [],
        ["rm", "-rf", "/scratch/relay"],
        ["bash", "-c", "ls"],
        ["sh", "-c", "squeue"],
        ["python3", "-c", "print(1)"],
        ["sbatch", "job.sh"],
        ["scancel", "12"],
        ["/tmp/evil/ls"],
        ["./squeue"],
        ["ls\0", "x"],
        ["find", ".", "-delete"],
        ["find", ".", "-exec", "rm", "{}", ";"],
        ["find", ".", "-fprint", "out"],
        ["scontrol", "update", "job=12", "timelimit=10"],
        ["scontrol", "requeue", "12"],
        ["sacctmgr", "add", "user", "x"],
        ["sacctmgr", "-i", "delete", "user", "x"],
        ["lfs", "setstripe", "-c", "4", "f"],
        ["pbsnodes", "-o", "node1"],
        ["file", "-C", "-m", "magic"],
        ["module", "load", "gcc"],
        ["jarvis", "ppl", "run"],
        ["jarvis", "repo", "add", "/tmp/x"],
        ["jarvis", "init", "a", "b", "c"],
        ["jarvis", "sched", "info"],
        ["jarvis", "sched", "info", "+json", "+submit"],
        ["jarvis", "rg", "facts", "answer", "sha256:x", "--value", "accept", "+json"],
        ["jarvis", "rg", "facts", "show", "--answers", "/tmp/a.json", "+json"],
        ["jarvis", "rg", "facts", "show", "+json", "+json"],
        ["jarvis", "rg", "skill", "set", "x", "+json"],
        ["jarvis", "rg", "skill", "show", "+json"],
        ["jarvis", "rg", "skill", "show", "../x", "+json"],
        ["jarvis", "rg", "skill", "show", "path=/tmp/x", "+json"],
        ["jarvis", "rg", "net", "set", "+json"],
        ["jarvis", "rg", "build", "+json"],
        ["srun", "hostname"],
        ["srun", "--jobid=12", "--overlap", "hostname"],
    ],
)
def test_everything_else_is_refused_with_a_reason(command: list[str]) -> None:
    reason = execlist.refusal(command)
    assert reason


def test_srun_reaches_only_a_held_warm_allocation() -> None:
    warm = {"12"}
    assert execlist.refusal(["srun", "--jobid=12", "--overlap", "hostname"], warm_jobs=warm) is None
    assert (
        execlist.refusal(
            ["srun", "--jobid", "12", "--overlap", "-N", "1", "-n", "1", "squeue"], warm_jobs=warm
        )
        is None
    )
    assert execlist.refusal(["srun", "--jobid=13", "--overlap", "hostname"], warm_jobs=warm)
    assert execlist.refusal(["srun", "--jobid=12", "hostname"], warm_jobs=warm), "needs --overlap"
    assert execlist.refusal(["srun", "--jobid=12", "--overlap", "rm", "x"], warm_jobs=warm)
    assert execlist.refusal(
        ["srun", "--jobid=12", "--overlap", "srun", "--jobid=12", "--overlap", "ls"],
        warm_jobs=warm,
    )
    assert execlist.refusal(
        ["srun", "--jobid=12", "--overlap", "--export=ALL", "ls"], warm_jobs=warm
    )


def test_a_site_can_add_its_own_read_only_programs() -> None:
    assert execlist.refusal(["qfree"])
    assert execlist.refusal(["qfree", "-a"], extra=["qfree"]) is None
    assert execlist.refusal(["/opt/site/bin/qfree"], extra=["/opt/site/bin/qfree"]) is None
    assert execlist.refusal(["/opt/other/qfree"], extra=["/opt/site/bin/qfree"])
    assert "qfree" in execlist.listed(["qfree"])


def test_the_configured_jarvis_path_is_read_only_too() -> None:
    jarvis = "~/w13-jv/bin/jarvis"
    assert execlist.refusal([jarvis, "ppl", "status"], jarvis=jarvis) is None
    assert execlist.refusal([jarvis, "ppl", "run"], jarvis=jarvis)
    assert execlist.refusal(["/other/jarvis", "ppl", "list"], jarvis=jarvis)
