"""The local adapter on a batch scheduler: job names, the job's own record, forgotten jobs.

A stand-in ``sbatch`` and ``squeue`` on ``PATH`` play a Slurm that has already
forgotten every job, the way Ares does with accounting off and a short
MinJobAge.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.local import LocalAdapter, LocalConfig, mark_script, name_submit
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import JobState
from clio_relay.jobs.spec import JobSpec

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]

NATIVE = "4242"


@pytest.fixture
def slurm(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Put a stand-in sbatch and squeue on PATH; return the file sbatch logs its argv to."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "sbatch.args"
    (bin_dir / "sbatch").write_text(
        f'#!/bin/sh\nprintf "%s\\n" "$@" > {calls}\necho "Submitted batch job {NATIVE}"\n',
        encoding="utf-8",
    )
    (bin_dir / "squeue").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (bin_dir / "scancel").write_text("#!/bin/sh\necho gone >&2\nexit 1\n", encoding="utf-8")
    for tool in bin_dir.iterdir():
        tool.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    return calls


def _adapter(tmp_path: Path) -> LocalAdapter:
    state = tmp_path / "state"
    context = AdapterContext(site_id="here", state_dir=state, content=ContentStore(state))
    return LocalAdapter(LocalConfig(scheduler="slurm"), context)


def test_the_submit_command_names_the_job() -> None:
    argv = ["sbatch", "/j/submit.sh"]
    assert name_submit("slurm", argv, "relay-op") == ["sbatch", "--job-name=relay-op", argv[1]]
    assert name_submit("pbs", ["qsub", "s"], "relay-op") == ["qsub", "-N", "relay-op", "s"]
    assert name_submit("lsf", ["bsub", "s"], "relay-op") == ["bsub", "-J", "relay-op", "s"]
    assert name_submit("cobalt", ["cqsub", "s"], "relay-op") == ["cqsub", "s"]


def test_the_job_records_its_own_id_after_the_directives() -> None:
    script = "#!/bin/bash\n#SBATCH --nodes=1\n\n#SBATCH -p x\nexec work\n"
    marked = mark_script(script, "slurm", Path("/jobs/op 1")).split("\n")
    assert marked[:4] == ["#!/bin/bash", "#SBATCH --nodes=1", "", "#SBATCH -p x"]
    assert marked[4] == "printf \"%s\\n\" \"${SLURM_JOB_ID:-}\" > '/jobs/op 1/scheduler_id'"
    assert marked[5] == "exec work"


async def test_a_forgotten_job_stays_open_until_its_record_says_how_it_ended(
    tmp_path: Path, slurm: Path
) -> None:
    adapter = _adapter(tmp_path)
    handle = await adapter.run(JobSpec(executable="true"), operation_id="op_named")
    assert handle.native_id == NATIVE
    assert f"--job-name={adapter.job_name('op_named')}" in slurm.read_text().split("\n")
    job_dir = Path(handle.data["job_dir"])
    assert "SLURM_JOB_ID" in (job_dir / "submit.sh").read_text(encoding="utf-8")
    assert "#SBATCH" in (job_dir / "submit.sh").read_text(encoding="utf-8")

    unknown = await adapter.status(handle)
    assert unknown.state == JobState.RUNNING
    assert unknown.native_state == "unknown"
    assert unknown.message is not None and "cancel" in unknown.message

    (Path(handle.data["script_dir"]) / f"{NATIVE}.ec").write_text("3", encoding="utf-8")
    ended = await adapter.status(handle)
    assert (ended.state, ended.exit_code) == (JobState.FAILED, 3)


async def test_cancel_ends_a_forgotten_job(tmp_path: Path, slurm: Path) -> None:
    del slurm
    adapter = _adapter(tmp_path)
    handle = await adapter.run(JobSpec(executable="true"), operation_id="op_cancel")
    cancelled = await adapter.cancel(handle)
    assert cancelled.state == JobState.CANCELLED


async def test_find_reads_the_id_the_job_wrote(tmp_path: Path, slurm: Path) -> None:
    """Relay died after sbatch and before keeping the id; the job kept it."""
    del slurm
    adapter = _adapter(tmp_path)
    handle = await adapter.run(JobSpec(executable="true"), operation_id="op_crash")
    job_dir = Path(handle.data["job_dir"])
    (job_dir / "native_id").unlink()
    (job_dir / "scheduler_id").write_text(f"{NATIVE}\n", encoding="utf-8")
    found = await adapter.find("op_crash")
    assert found is not None and found.native_id == NATIVE
    (job_dir / "scheduler_id").unlink()
    assert await adapter.find("op_crash") is None
