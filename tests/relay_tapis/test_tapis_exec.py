"""Relay's ``exec`` on a Tapis site is one launcher job: FORK on the login host when it can.

Setup, the package listing and ``packages(op="add")`` all run through it, as on
every adapter, so a package needs nothing shipped with its job.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.tapis import TapisAdapter, TapisConfig, mapping
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, PackageRef
from tests.fixtures.tapis_stub import TapisStub
from tests.fixtures.tapis_stub.support import (
    seed_host,
    sign_in,
    tapis_config,
    use_memory_keyring,
)

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]

SLURM: dict[str, Any] = {
    "canRunBatch": True,
    "batchScheduler": "SLURM",
    "batchDefaultLogicalQueue": "debug",
    "batchLogicalQueues": [{"name": "debug", "hpcQueueName": "debug", "maxMinutes": 30}],
}


def _adapter(stub: TapisStub, tmp_path: Path, site: str, **system: Any) -> TapisAdapter:
    seed_host(stub, site, tmp_path / "site", **system)
    context = AdapterContext(
        site_id=site, state_dir=tmp_path / "relay", content=ContentStore(tmp_path / "content")
    )
    return TapisAdapter(TapisConfig.model_validate(tapis_config(stub)), context)


async def test_exec_is_one_fork_job_whose_directory_goes_after(
    stub: TapisStub, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    adapter = _adapter(stub, tmp_path, "exec-fork")
    before = set(stub.state.jobs)
    result = await adapter.exec(
        ["sh", "-c", "echo said; echo warned >&2; exit 3"], max_output_bytes=1024
    )
    assert (result.exit_code, result.stdout, result.stderr) == (3, "said\n", "warned\n")
    assert not result.timed_out
    (made,) = set(stub.state.jobs) - before
    assert stub.state.jobs[made].request["jobType"] == "FORK"
    assert not list((tmp_path / "site").rglob("exec-*"))
    with pytest.raises(Refusal) as empty:
        await adapter.exec([], max_output_bytes=1)
    assert empty.value.reason == RefusalReason.INVALID_ARGUMENTS


async def test_exec_prefers_fork_and_falls_back_to_a_slurm_batch_job() -> None:
    assert mapping.exec_job_type(SLURM) == "FORK"
    assert mapping.exec_job_type({**SLURM, "canExec": False}) == "BATCH"
    assert mapping.exec_job_type({"canExec": False}) is None


async def test_a_package_on_a_slurm_system_is_jarvis_ppl_submit_through_exec(
    stub: TapisStub, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    adapter = _adapter(stub, tmp_path, "pkg-slurm", **SLURM)
    ran: list[str] = []
    line = (
        '{"execution_id":"op-pkg","mode":"scheduler","pipeline_id":"relay_op-pkg",'
        '"schema_version":"jarvis.execution.handle.v1","scheduler_native_id":"41",'
        '"scheduler_provider":"slurm"}'
    )

    async def sh(script: str, what: str) -> str:
        ran.append(script)
        return f"@@rc 0\n@@out\n{line}\n@@err\n"

    monkeypatch.setattr(adapter, "_sh", sh)
    spec = JobSpec(package=PackageRef(name="demo.hello", version="1", sha256="a" * 64))
    handle = await adapter.run(spec, operation_id="op-pkg")
    assert (handle.native_id, handle.data["mode"]) == ("41", "jarvis")
    assert "relay-jarvis.sh submit" in ran[0]
    (pipeline,) = (tmp_path / "site").rglob("relay.pipeline.yaml")
    assert "  name: slurm" in pipeline.read_text().splitlines()
    assert await adapter.find("op-pkg") == handle


async def test_a_batch_job_execs_the_launcher_only_where_the_system_allows_it() -> None:
    spec = JobSpec(executable="true")
    common: dict[str, Any] = {
        "operation_id": "op-prefix",
        "app_id": "relay",
        "app_version": "1",
        "system_id": "sys",
        "job_dir": "/w/jobs/op-prefix",
        "content_dir_host": "/w/content",
        "launcher_arg": "@/w/jobs/op-prefix/relay.record",
        "max_minutes": 30,
    }
    batch = mapping.job_request(spec, system_type="BATCH", exec_prefix=True, **common)
    fork = mapping.job_request(spec, system_type="FORK", exec_prefix=True, **common)
    plain = mapping.job_request(spec, system_type="BATCH", **common)
    assert batch["cmdPrefix"] == "exec"
    assert "cmdPrefix" not in fork
    assert "cmdPrefix" not in plain
